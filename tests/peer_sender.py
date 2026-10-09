#!/usr/bin/env python3
"""A sending peer in a process of its own, for tests that need two block stores.

`Enviroment` is process wide, so two ends in one process share one `__block__`
and the receiver always holds whatever is sent. Here the sender has its own.

    python tests/peer_sender.py <cache_dir> <block_dir> <block_depth> <object_dir>

Prints the port it listens on, then serves `<object_dir>` to every call until
stdin closes.
"""
import sys
from concurrent.futures import ThreadPoolExecutor

import grpc

from bee_rpc import buffer_pb2
from bee_rpc.client import parse_from_buffer, serialize_to_buffer
from bee_rpc.control import StreamControl
from bee_rpc.utils import Dir, modify_env

SERVICE, METHOD = 'bee.WireHarness', 'Exchange'


def main():
    cache_dir, block_dir, depth, object_dir = sys.argv[1:5]
    modify_env(cache_dir=cache_dir, block_dir=block_dir, block_depth=int(depth))

    def handler(request_iterator, context):
        control = StreamControl()
        for _ in parse_from_buffer(request_iterator=request_iterator,
                                   indices=buffer_pb2.Empty,
                                   partitions_message_mode=True, control=control):
            break
        control.watch()
        yield from serialize_to_buffer(
            message_iterator=iter([Dir(dir=object_dir, _type=buffer_pb2.Buffer)]),
            indices={1: buffer_pb2.Buffer}, control=control)

    server = grpc.server(ThreadPoolExecutor(max_workers=4))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(SERVICE, {
        METHOD: grpc.stream_stream_rpc_method_handler(
            handler,
            request_deserializer=buffer_pb2.Buffer.FromString,
            response_serializer=buffer_pb2.Buffer.SerializeToString),
    }),))
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    print(port, flush=True)
    sys.stdin.read()
    server.stop(None)


if __name__ == '__main__':
    main()
