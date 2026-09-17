"""A real bidirectional gRPC stream of Buffers, both ends using this library.

No generated stub: the service is registered with grpc's generic handler API, so
the tests can stand a server up on a localhost port without a .proto of their
own. Every Buffer crossing either direction is counted, which is the whole point
-- the block-skip tests assert on bytes on the wire, not only on the result.
"""
import threading
import time
import typing

import grpc

from bee_rpc import buffer_pb2

SERVICE = 'bee.WireHarness'
METHOD = 'Exchange'
FULL_METHOD = '/%s/%s' % (SERVICE, METHOD)


class Counter:
    """Buffers and bytes seen, by kind. Thread safe; both directions are counted
    from the thread that produces them."""

    def __init__(self, label: str = '') -> None:
        self.label = label
        self._lock = threading.Lock()
        self.buffers = 0
        self.chunk_buffers = 0
        self.chunk_bytes = 0
        self.block_markers = 0
        self.skip_markers = 0
        self.total_bytes = 0

    def observe(self, buffer: buffer_pb2.Buffer) -> None:
        size = buffer.ByteSize()
        with self._lock:
            self.buffers += 1
            self.total_bytes += size
            if buffer.HasField('chunk') and len(buffer.chunk) > 0:
                self.chunk_buffers += 1
                self.chunk_bytes += len(buffer.chunk)
            if buffer.HasField('block'):
                self.block_markers += 1
            if buffer.HasField('skip'):
                self.skip_markers += 1

    def wrap(self, iterator):
        for buffer in iterator:
            self.observe(buffer)
            yield buffer

    def __repr__(self) -> str:
        return ('<%s buffers=%d chunk_buffers=%d chunk_bytes=%d block_markers=%d '
                'skip_markers=%d>' % (self.label or 'counter', self.buffers,
                                      self.chunk_buffers, self.chunk_bytes,
                                      self.block_markers, self.skip_markers))


class Harness:
    """Runs `handler(request_iterator)` as a bidi-streaming RPC on a real socket.

    `handler` returns the generator of response Buffers. `sent` counts what the
    server actually pushes; `received` counts what reaches the client.
    """

    def __init__(self, handler: typing.Callable[[typing.Iterator], typing.Iterator],
                 upstream_delay: float = 0.0) -> None:
        self.handler = handler
        # Latency on the client->server direction. On loopback a skip request
        # completes the round trip faster than the sender can read the first
        # chunk off disk, so without this the overlap the feature has to tolerate
        # never actually happens.
        self.upstream_delay = upstream_delay
        self.sent = Counter('server->client')
        self.received = Counter('client(received)')
        self.upstream = Counter('client->server')
        self.handler_error: typing.Optional[BaseException] = None
        self._server: typing.Optional[grpc.Server] = None
        self._channel: typing.Optional[grpc.Channel] = None

    def _serve(self, request_iterator, context):
        try:
            yield from self.sent.wrap(self.handler(request_iterator))
        except BaseException as e:  # surfaced to the test rather than swallowed by grpc
            self.handler_error = e
            raise

    def __enter__(self) -> 'Harness':
        self._server = grpc.server(
            __import__('concurrent.futures', fromlist=['ThreadPoolExecutor'])
            .ThreadPoolExecutor(max_workers=8)
        )
        self._server.add_generic_rpc_handlers((
            grpc.method_handlers_generic_handler(SERVICE, {
                METHOD: grpc.stream_stream_rpc_method_handler(
                    self._serve,
                    request_deserializer=buffer_pb2.Buffer.FromString,
                    response_serializer=buffer_pb2.Buffer.SerializeToString,
                ),
            }),
        ))
        port = self._server.add_insecure_port('127.0.0.1:0')
        self._server.start()
        self._channel = grpc.insecure_channel(
            '127.0.0.1:%d' % port,
            options=[('grpc.max_receive_message_length', 64 * 1024 * 1024),
                     ('grpc.max_send_message_length', 64 * 1024 * 1024)],
        )
        return self

    def __exit__(self, *exc) -> None:
        if self._channel is not None:
            self._channel.close()
        if self._server is not None:
            self._server.stop(None)

    def method(self):
        """A callable with the shape `client_grpc` expects: (iterator, timeout=)."""
        raw = self._channel.stream_stream(
            FULL_METHOD,
            request_serializer=buffer_pb2.Buffer.SerializeToString,
            response_deserializer=buffer_pb2.Buffer.FromString,
        )

        def delayed(iterator):
            for buffer in iterator:
                if self.upstream_delay:
                    time.sleep(self.upstream_delay)
                yield buffer

        def call(request_iterator, timeout=None):
            return self.received.wrap(
                raw(delayed(self.upstream.wrap(request_iterator)), timeout=timeout))

        return call
