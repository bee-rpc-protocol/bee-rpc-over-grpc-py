#!/usr/bin/env python3
"""The block stop signal actually stops the block (issue #8).

`signal_block_buffer_stream()` was a no-op, so a receiver that already held a
block still had the whole block pushed at it and dropped on the floor by
`save_chunks_to_block`'s drain branch: deduplication saved disk, never
bandwidth. These tests run both ends of a real bidirectional gRPC stream over a
localhost socket and assert on two things at once -- that the reconstruction is
byte-identical, and that the block's bytes did not cross the wire.

Run:  python -m pytest tests/test_block_skip_signal.py
"""
import os
import shutil
import tempfile
import threading
import time
import unittest

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.client import (client_grpc, parse_from_buffer, serialize_to_buffer,
                            skip_requested_blocks)
from bee_rpc.control import StreamControl, skip_buffer, skipped_block_id
from bee_rpc.reader import block_exists, read_multiblock_directory
from bee_rpc.utils import Dir, Enviroment, modify_env

from tests.wire_harness import Harness

BLOCK_MB = 6


def _flatten(path: str) -> bytes:
    """The bytes a Dir result stands for, blocks expanded."""
    if os.path.isdir(path):
        return b''.join(c for c in read_multiblock_directory(path, ignore_blocks=True)
                        if isinstance(c, bytes))
    with open(path, 'rb') as f:
        return f.read()


class BlockSkipBase(unittest.TestCase):

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix='blockskip-')
        self.blocks = os.path.join(self.root, 'blocks')
        os.makedirs(self.blocks)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.blocks + os.sep)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _shared_object(self, megabytes: int = BLOCK_MB):
        """A multiblock object whose body is one large block, held by both ends.

        Sender and receiver share a single `__block__` directory here, which is
        exactly the condition the feature is for: the receiver recognises the
        block on sight and has no reason to accept its body.
        """
        big = os.path.join(self.root, 'big-%d.bin' % megabytes)
        with open(big, 'wb') as f:
            f.write(os.urandom(megabytes * 1024 * 1024))
        block_hash, pointer = block_builder.create_block(file_path=big, copy=True)
        self.assertTrue(block_exists(block_hash.hex()))

        message = buffer_pb2.Buffer()
        message.chunk = pointer.SerializeToString()
        _obj_id, mdir = block_builder.build_multiblock(message, blocks=[block_hash])

        canonical = _flatten(mdir)
        return Dir(dir=mdir, _type=buffer_pb2.Buffer), canonical, block_hash.hex()

    def _exchange(self, source: Dir, server_skip: bool, client_skip: bool,
                  chunk_delay: float = 0.0, upstream_delay: float = 0.0):
        """Ship `source` from a server handler to a client, both this library.

        `server_skip` is whether the *sender* honours skip requests;
        `client_skip` whether the *receiver* emits them. Turning either off is
        how the compatibility cases are expressed.
        """
        def handler(request_iterator):
            control = StreamControl() if server_skip else None
            # A handler parses its request first and streams its response after
            # -- so the client's skip requests, which can only be written once
            # the response starts arriving, land on a request iterator nobody is
            # reading any more. watch() is what keeps reading it.
            for _ in parse_from_buffer(
                request_iterator=request_iterator,
                indices=buffer_pb2.Empty,
                partitions_message_mode=True,
                control=control,
            ):
                break
            if control is not None:
                control.watch()

            stream = serialize_to_buffer(
                message_iterator=iter([source]),
                indices={1: buffer_pb2.Buffer},
                control=control,
            )
            if chunk_delay:
                def slowly(inner):
                    for buffer in inner:
                        if buffer.HasField('chunk') and len(buffer.chunk) > 0:
                            time.sleep(chunk_delay)
                        yield buffer
                stream = slowly(stream)
            yield from stream

        with Harness(handler, upstream_delay=upstream_delay) as harness:
            results = list(client_grpc(
                method=harness.method(),
                indices_parser={1: buffer_pb2.Buffer},
                partitions_message_mode_parser=False,
                block_skip=client_skip,
            ))
            self.assertIsNone(harness.handler_error)
            self.assertEqual(len(results), 1, 'expected exactly one parsed result')
            return _flatten(results[0].dir), harness


class BlockSkipOverTheWire(BlockSkipBase):

    def test_held_block_is_not_transmitted(self):
        source, canonical, _ = self._shared_object()

        plain, baseline = self._exchange(source, server_skip=False, client_skip=False)
        skipped, measured = self._exchange(source, server_skip=True, client_skip=True)

        # Correctness first: skipping must not change the answer.
        self.assertEqual(plain, canonical, 'baseline transfer is not byte-correct')
        self.assertEqual(skipped, canonical,
                         'skipping the block changed the reconstructed bytes')

        # Then the point of the exercise.
        self.assertGreater(baseline.sent.chunk_bytes, BLOCK_MB * 1024 * 1024 - 1024,
                           'baseline should carry the whole block')
        self.assertLess(measured.sent.chunk_bytes, baseline.sent.chunk_bytes / 100,
                        'block body still crossing the wire: %r vs %r'
                        % (measured.sent, baseline.sent))

        # The markers themselves must still be there -- both of them. The
        # receiver's drain path scans for the terminator; a block that opens and
        # never closes desynchronises everything after it.
        self.assertEqual(measured.sent.block_markers, baseline.sent.block_markers,
                         'block start/end markers must survive the skip')

        # And the request that caused it went the other way.
        self.assertEqual(measured.upstream.skip_markers, 1)

    def test_skip_request_reaches_sender_mid_block(self):
        """The race in issue #8: chunks are already in flight when the request lands.

        The request cannot possibly arrive before the block starts -- the
        receiver only learns the hash from the start marker. Here the reverse
        direction is slowed so that the request lands well after it, with chunks
        genuinely in flight, which on plain loopback it does not (the round trip
        beats the sender's first 1 MB disk read). The sender must stop where it
        is and emit the terminator exactly once; the receiver must tolerate the
        chunks that overtook the request.
        """
        megabytes = 16
        source, canonical, _ = self._shared_object(megabytes=megabytes)
        # The sender emits roughly one chunk per 50 ms and the request takes
        # ~150 ms to come back, so it lands a few chunks into a 16-chunk block --
        # with a wide margin either side of "neither the first nor the last".
        content, harness = self._exchange(source, server_skip=True, client_skip=True,
                                          chunk_delay=0.05, upstream_delay=0.15)

        self.assertEqual(content, canonical,
                         'mid-block skip corrupted the reconstruction')
        self.assertEqual(harness.sent.block_markers, 2,
                         'exactly one start and one end marker, no duplicate terminator')

        # The overlap really happened: some of the block went out before the
        # request landed, but the sender stopped well short of the end.
        self.assertGreater(harness.sent.chunk_bytes, 0,
                           'no chunks in flight: this is not exercising the race')
        self.assertLess(harness.sent.chunk_bytes, megabytes * 1024 * 1024,
                        'sender did not stop mid-block: %r' % (harness.sent,))

    def test_sender_ignoring_the_signal_stays_correct(self):
        """An old sender: the receiver asks, nothing honours it, result unchanged."""
        source, canonical, _ = self._shared_object()
        content, harness = self._exchange(source, server_skip=False, client_skip=True)

        self.assertEqual(content, canonical)
        self.assertEqual(harness.upstream.skip_markers, 1, 'receiver still asked')
        self.assertGreater(harness.sent.chunk_bytes, BLOCK_MB * 1024 * 1024 - 1024,
                           'an unaware sender sends everything, as before')

    def test_receiver_not_asking_is_the_old_behaviour(self):
        """An old receiver: nothing is requested, so nothing is skipped."""
        source, canonical, _ = self._shared_object()
        content, harness = self._exchange(source, server_skip=True, client_skip=False)

        self.assertEqual(content, canonical)
        self.assertEqual(harness.upstream.skip_markers, 0)
        self.assertGreater(harness.sent.chunk_bytes, BLOCK_MB * 1024 * 1024 - 1024)

    def test_abandoned_response_generator_releases_the_request_direction(self):
        """`next(client_grpc(...))` and drop it -- how most of nodo calls this.

        With `block_skip` on, the request generator deliberately outlives the
        input so that skip requests still have a way out. A caller that stops
        consuming the response must still let it end, or that generator parks
        until the call is torn down.
        """
        source, canonical, _ = self._shared_object()

        def handler(request_iterator):
            control = StreamControl()
            for _ in parse_from_buffer(
                request_iterator=request_iterator,
                indices=buffer_pb2.Empty,
                partitions_message_mode=True,
                control=control,
            ):
                break
            control.watch()
            yield from serialize_to_buffer(
                message_iterator=iter([source]),
                indices={1: buffer_pb2.Buffer},
                control=control,
            )

        before = {t.ident for t in threading.enumerate() if not t.daemon}
        with Harness(handler) as harness:
            result = next(client_grpc(
                method=harness.method(),
                indices_parser={1: buffer_pb2.Buffer},
                partitions_message_mode_parser=False,
                block_skip=True,
            ))
            self.assertEqual(_flatten(result.dir), canonical)
            self.assertLess(harness.sent.chunk_bytes, 1024,
                            'the block was still sent: %r' % (harness.sent,))

        import gc
        gc.collect()
        time.sleep(0.2)
        leaked = {t.ident for t in threading.enumerate() if not t.daemon} - before
        self.assertEqual(leaked, set(),
                         'abandoning the response left a non-daemon thread behind')

    def test_no_control_object_is_untouched(self):
        """Neither side opted in: byte-for-byte the stream this library always sent."""
        source, canonical, _ = self._shared_object()
        content, harness = self._exchange(source, server_skip=False, client_skip=False)
        self.assertEqual(content, canonical)
        self.assertEqual(harness.sent.skip_markers, 0)
        self.assertEqual(harness.upstream.skip_markers, 0)


class SkipEncoding(unittest.TestCase):
    """The wire form, and what an unaware peer makes of it."""

    def test_skip_is_not_a_block(self):
        buffer = skip_buffer('ab' * 32)
        self.assertTrue(buffer.HasField('skip'))
        self.assertFalse(buffer.HasField('block'),
                         'a skip request must never look like a block marker')
        self.assertEqual(skipped_block_id(buffer), 'ab' * 32)

    def test_old_peer_sees_an_inert_empty_chunk(self):
        """Field 6 did not exist before this change; a peer built then parses the
        carrier as an unknown field plus the empty `chunk` it is padded with.

        That padding is load-bearing. `parser_iterator` ends its iteration on a
        buffer that has neither `chunk` nor `head`; without the empty chunk an
        old peer would treat a skip request as end-of-message rather than
        ignoring it.
        """
        wire = skip_buffer('cd' * 32).SerializeToString()

        legacy = buffer_pb2.Buffer()
        legacy.ParseFromString(wire)
        # Simulate the pre-change schema by looking only at the fields it had.
        self.assertTrue(legacy.HasField('chunk'))
        self.assertEqual(legacy.chunk, b'')
        self.assertFalse(legacy.HasField('block'))
        self.assertFalse(legacy.HasField('head'))
        self.assertFalse(legacy.HasField('separator') and legacy.separator)
        self.assertFalse(legacy.HasField('signal') and legacy.signal)


class SkipFiltering(unittest.TestCase):
    """`skip_requested_blocks` on hand-built streams -- nesting, and the exact
    point at which the sender stops."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix='blockskipunit-')
        modify_env(cache_dir=self.root + os.sep, block_dir=self.root + os.sep)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    @staticmethod
    def _marker(block_id: str) -> buffer_pb2.Buffer:
        return buffer_pb2.Buffer(block=buffer_pb2.Buffer.Block(
            hashes=[buffer_pb2.Buffer.Block.Hash(
                type=Enviroment.hash_type, value=bytes.fromhex(block_id))]))

    @classmethod
    def _stream(cls, spec):
        """`spec` is a nested description: (block_id, [children...]) or bytes."""
        for item in spec:
            if isinstance(item, bytes):
                yield buffer_pb2.Buffer(chunk=item)
            else:
                block_id, inner = item
                yield cls._marker(block_id)
                yield from cls._stream(inner)
                yield cls._marker(block_id)

    OUTER = 'aa' * 32
    INNER = 'bb' * 32

    def _nested(self):
        return [
            b'before',
            (self.OUTER, [b'outer-1', (self.INNER, [b'inner-1', b'inner-2']), b'outer-2']),
            b'after',
        ]

    def _run(self, spec, skip_ids):
        control = StreamControl()
        for block_id in skip_ids:
            control._skip.add(block_id)
        return list(skip_requested_blocks(self._stream(spec), control=control))

    @staticmethod
    def _summarise(buffers):
        out = []
        for b in buffers:
            if b.HasField('block'):
                out.append(('block', b.block.hashes[0].value.hex()))
            else:
                out.append(('chunk', b.chunk))
        return out

    def test_skipping_a_parent_skips_its_children(self):
        got = self._summarise(self._run(self._nested(), [self.OUTER]))
        self.assertEqual(got, [
            ('chunk', b'before'),
            ('block', self.OUTER),
            ('block', self.OUTER),   # straight to the terminator
            ('chunk', b'after'),
        ], 'a skipped parent must take its children with it')

    def test_skipping_a_child_keeps_the_parent(self):
        got = self._summarise(self._run(self._nested(), [self.INNER]))
        self.assertEqual(got, [
            ('chunk', b'before'),
            ('block', self.OUTER),
            ('chunk', b'outer-1'),
            ('block', self.INNER),
            ('block', self.INNER),
            ('chunk', b'outer-2'),
            ('block', self.OUTER),
            ('chunk', b'after'),
        ], "a skipped child must not disturb its parent's framing")

    def test_terminators_are_never_duplicated_or_dropped(self):
        for skip in ([], [self.OUTER], [self.INNER], [self.OUTER, self.INNER]):
            with self.subTest(skip=skip):
                got = self._summarise(self._run(self._nested(), skip))
                markers = [name for kind, name in got if kind == 'block']
                for block_id in (self.OUTER, self.INNER):
                    count = markers.count(block_id)
                    self.assertIn(count, (0, 2),
                                  'block %s appeared %d times; markers must come in '
                                  'start/end pairs' % (block_id[:8], count))
                self.assertEqual(markers.count(self.OUTER), 2,
                                 'the outer block is always framed')

    def test_request_arriving_mid_block_stops_at_the_next_chunk(self):
        """The sender re-checks per chunk, so it stops wherever the request lands."""
        control = StreamControl()
        spec = [(self.OUTER, [b'c%d' % i for i in range(6)])]

        emitted = []
        for buffer in skip_requested_blocks(self._stream(spec), control=control):
            emitted.append(buffer)
            # Ask for the skip only once two chunks have already gone out, which
            # is the in-flight overlap the issue describes.
            if len([b for b in emitted if b.HasField('chunk')]) == 2:
                control._skip.add(self.OUTER)

        got = self._summarise(emitted)
        self.assertEqual(got, [
            ('block', self.OUTER),
            ('chunk', b'c0'),
            ('chunk', b'c1'),
            ('block', self.OUTER),
        ], 'sender should stop at the first chunk after the request and terminate')

    def test_without_a_control_object_nothing_changes(self):
        spec = self._nested()
        self.assertEqual(
            self._summarise(list(skip_requested_blocks(self._stream(spec), control=None))),
            self._summarise(list(self._stream(spec))),
        )


class ControlObject(unittest.TestCase):

    def test_a_block_is_only_requested_once(self):
        control = StreamControl()
        control.request_skip('ee' * 32)
        control.request_skip('ee' * 32)
        self.assertEqual(len(control.pending_outbound()), 1)

    def test_disabled_control_is_inert(self):
        control = StreamControl(enabled=False)
        control.request_skip('ff' * 32)
        self.assertEqual(control.pending_outbound(), [])
        self.assertFalse(control.should_skip('ff' * 32))

    def test_reader_strips_requests_from_the_stream(self):
        control = StreamControl()
        block_id = '11' * 32
        stream = iter([
            buffer_pb2.Buffer(chunk=b'a'),
            skip_buffer(block_id),
            buffer_pb2.Buffer(chunk=b'b'),
        ])
        passed = list(control.reader(stream))
        self.assertEqual([b.chunk for b in passed], [b'a', b'b'],
                         'a skip request must not reach the parser')
        self.assertTrue(control.should_skip(block_id))

    def test_observe_is_thread_safe(self):
        control = StreamControl()
        ids = ['%064x' % i for i in range(200)]

        def half(subset):
            for block_id in subset:
                control.observe(skip_buffer(block_id))

        threads = [threading.Thread(target=half, args=(ids[:100],)),
                   threading.Thread(target=half, args=(ids[100:],))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertTrue(all(control.should_skip(i) for i in ids))


if __name__ == '__main__':
    unittest.main()
