"""Reverse-direction stream control: telling a peer to stop sending a block.

Block-level deduplication used to save disk and parsing but no bandwidth: a
receiver that already held a block still had the whole block pushed at it and
dropped it on the floor (`save_chunks_to_block`'s drain branch). The missing
half was a way for the receiver to say "I have this one" and for the sender to
act on it mid-stream. `StreamControl` is that channel.

One object per call, shared by the two halves of a party's participation in it:

  * the **parse** side writes into it -- when `parse_from_buffer` meets the start
    marker of a block that `block_exists()` locally, it asks for a skip, which
    becomes a control Buffer queued for the opposite direction;
  * the **serialize** side reads from it -- it interleaves those queued Buffers
    into the stream it is producing, and it consults the set of hashes the peer
    has asked *it* to skip while it emits a block's chunks.

Both halves are touched from different threads (gRPC pulls a request iterator on
its own thread), so the set and the queue are lock-protected.

Nothing here is mandatory. Every entry point that accepts a control object
accepts `None`, which is the pre-existing behaviour exactly: no reverse-direction
markers are produced and none are honoured.
"""

import threading
import typing
from collections import deque

from bee_rpc import buffer_pb2
from bee_rpc.utils import Enviroment


def skip_buffer(block_id: str) -> buffer_pb2.Buffer:
    """The wire form of a skip request.

    `skip` is field 6, distinct from `block` (field 5). A peer built before the
    field existed parses it as an unknown field and ignores it -- which is why
    the carrier also sets an empty `chunk`: with no known field set at all, such
    a peer's `parser_iterator` would fall through to its "no chunk and no head"
    branch and end the iteration early. An empty chunk is instead inert
    everywhere it can land (appended to a buffer, written to a file: zero bytes).
    """
    return buffer_pb2.Buffer(
        skip=buffer_pb2.Buffer.Block(
            hashes=[buffer_pb2.Buffer.Block.Hash(
                type=Enviroment.hash_type,
                value=bytes.fromhex(block_id)
            )]
        ),
        chunk=b''
    )


def skipped_block_id(buffer: buffer_pb2.Buffer) -> typing.Optional[str]:
    if not buffer.HasField('skip'):
        return None
    for _hash in buffer.skip.hashes:
        if _hash.type == Enviroment.hash_type:
            return _hash.value.hex()
    return None


def _is_inert(buffer: buffer_pb2.Buffer) -> bool:
    """Whether this Buffer carries nothing but the skip request and its padding."""
    return not (
        buffer.HasField('head') or buffer.HasField('block')
        or (buffer.HasField('separator') and buffer.separator)
        or (buffer.HasField('signal') and buffer.signal)
        or (buffer.HasField('chunk') and len(buffer.chunk) > 0)
    )


class StreamControl:

    def __init__(self, enabled: bool = True) -> None:
        self.enabled: bool = enabled
        self._lock = threading.Lock()
        self._skip: typing.Set[str] = set()          # hashes the PEER asked us to skip
        self._asked: typing.Set[str] = set()         # hashes WE have already asked for
        self._outbound: typing.Deque[buffer_pb2.Buffer] = deque()
        self._outbound_ready = threading.Event()
        self._sending_done = threading.Event()
        self._watcher: typing.Optional[threading.Thread] = None
        self._source: typing.Optional[typing.Iterator] = None

    # ------------------------------------------------------------------ #
    # Receiver side                                                      #
    # ------------------------------------------------------------------ #

    def request_skip(self, block_id: str) -> None:
        """Ask the peer to stop sending `block_id`; we already hold it."""
        if not self.enabled or not block_id:
            return
        with self._lock:
            if block_id in self._asked:
                # The same block can legitimately appear more than once in a
                # stream. Asking once is enough: the peer's skip set is durable
                # for the life of the call.
                return
            self._asked.add(block_id)
            self._outbound.append(skip_buffer(block_id))
        self._outbound_ready.set()

    # ------------------------------------------------------------------ #
    # Sender side                                                        #
    # ------------------------------------------------------------------ #

    def should_skip(self, block_id: typing.Optional[str]) -> bool:
        if not self.enabled or not block_id:
            return False
        with self._lock:
            return block_id in self._skip

    def pending_outbound(self) -> typing.List[buffer_pb2.Buffer]:
        """Everything queued right now, without waiting. Called between yields."""
        if not self.enabled:
            return []
        with self._lock:
            if not self._outbound:
                return []
            out = list(self._outbound)
            self._outbound.clear()
        self._outbound_ready.clear()
        return out

    def next_outbound(self, timeout: float) -> typing.Optional[buffer_pb2.Buffer]:
        """Block up to `timeout` seconds for one queued Buffer.

        Used by a sender whose own payload is already exhausted but which must
        hold its direction of the stream open -- see `client_grpc`.
        """
        if not self.enabled:
            return None
        if self._outbound_ready.wait(timeout):
            with self._lock:
                if self._outbound:
                    buffer = self._outbound.popleft()
                    if not self._outbound:
                        self._outbound_ready.clear()
                    return buffer
            self._outbound_ready.clear()
        return None

    def finish_sending(self) -> None:
        """No more of our own output is coming; a holding generator may end."""
        self._sending_done.set()
        self._outbound_ready.set()  # wake a waiter immediately

    def sending_finished(self) -> bool:
        return self._sending_done.is_set()

    # ------------------------------------------------------------------ #
    # Plumbing                                                           #
    # ------------------------------------------------------------------ #

    def observe(self, buffer: buffer_pb2.Buffer) -> bool:
        """Record a skip request arriving from the peer. True if there was one."""
        if not self.enabled:
            return False
        block_id = skipped_block_id(buffer)
        if block_id is None:
            return False
        with self._lock:
            self._skip.add(block_id)
        return True

    def reader(self, iterator: typing.Iterator) -> typing.Generator[buffer_pb2.Buffer, None, None]:
        """Pass `iterator` through, picking off skip requests on the way.

        A Buffer that carries nothing but a skip request is consumed here; one
        that also carries payload (nothing produces that today, but the encoding
        allows it) is forwarded with the `skip` field cleared, so that no part of
        the parser ever has to know this field exists.
        """
        self._source = iterator
        if not self.enabled:
            yield from iterator
            return
        for buffer in iterator:
            if self.observe(buffer):
                if _is_inert(buffer):
                    continue
                buffer.ClearField('skip')
            yield buffer

    def watch(self, iterator: typing.Optional[typing.Iterator] = None) -> None:
        """Keep reading the peer's stream in the background, for skip requests only.

        A server handler has finished parsing its request long before it finishes
        streaming its response, but the client's skip requests arrive during that
        response. Call this once parsing is done and the remaining request
        direction is nobody else's business; it is a daemon thread, so it does
        not keep the process alive, and it swallows the teardown errors a
        cancelled or completed call raises on its request iterator.
        """
        if not self.enabled or self._watcher is not None:
            return
        source = iterator if iterator is not None else self._source
        if source is None:
            return

        def _drain():
            try:
                for buffer in source:
                    self.observe(buffer)
            except Exception:
                pass

        self._watcher = threading.Thread(target=_drain, daemon=True,
                                         name='bee-rpc-stream-control')
        self._watcher.start()
