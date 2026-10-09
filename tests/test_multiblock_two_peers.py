#!/usr/bin/env python3
"""A nested directory block between two peers with block stores of their own.

The sender runs in another process (tests/peer_sender.py), so what the receiver
holds is only what it held before plus what crossed the socket. It holds one of
the leaves already: that leaf is skipped on the wire, and the directory block
around it is still stored as a directory and still checked against its id --
with the leaf's bytes taken from the receiver's own copy.

Run:  python -m pytest tests/test_multiblock_two_peers.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

import grpc

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.client import client_grpc
from bee_rpc.reader import read_multiblock_directory
from bee_rpc.utils import Enviroment, modify_env, METADATA_FILE_NAME

from tests.wire_harness import Counter, FULL_METHOD

DEPTH = 8
LEAF = 4 * 1024 * 1024


def _expansion(path: str) -> bytes:
    return b''.join(c for c in read_multiblock_directory(path, ignore_blocks=True)
                    if isinstance(c, bytes))


class TwoPeers(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix='two-peers-')
        self.addCleanup(shutil.rmtree, self.root, True)
        self.addCleanup(modify_env, block_depth=1)
        self.dirs = {}
        for peer in ('sender', 'receiver'):
            cache = os.path.join(self.root, peer, 'cache') + os.sep
            blocks = os.path.join(self.root, peer, 'blocks') + os.sep
            os.makedirs(cache)
            os.makedirs(blocks)
            self.dirs[peer] = (cache, blocks)

    def _use(self, peer: str):
        cache, blocks = self.dirs[peer]
        modify_env(cache_dir=cache, block_dir=blocks, block_depth=DEPTH)

    def _file_block(self, name: str, size: int):
        path = os.path.join(self.root, name)
        with open(path, 'wb') as f:
            f.write(os.urandom(size))
        block_hash, pointer = block_builder.create_block(file_path=path, copy=True)
        return block_hash, pointer.SerializeToString()

    def _object(self):
        """outer -> inner (a directory block) -> two leaves."""
        self._use('sender')
        leaf_a, ptr_a = self._file_block('a', LEAF)
        leaf_b, ptr_b = self._file_block('b', LEAF)
        inner = buffer_pb2.Buffer()
        inner.block.hashes.add().value = ptr_a
        inner.block.hashes.add().value = ptr_b
        inner_id, inner_dir = block_builder.build_multiblock(inner, blocks=[leaf_a, leaf_b])
        shutil.move(inner_dir.rstrip(os.sep), Enviroment.block_dir + inner_id.hex())
        outer = buffer_pb2.Buffer()
        outer.block.hashes.add().value = buffer_pb2.Buffer.Block(
            hashes=[buffer_pb2.Buffer.Block.Hash(
                type=Enviroment.hash_type, value=inner_id)]).SerializeToString()
        _, outer_dir = block_builder.build_multiblock(outer, blocks=[inner_id])
        return outer_dir, inner_id.hex(), leaf_a.hex(), leaf_b.hex()

    def _serve(self, object_dir: str):
        cache, blocks = self.dirs['sender']
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join(
            [os.path.join(os.path.dirname(__file__), '..', 'src')] + sys.path)
        proc = subprocess.Popen(
            [sys.executable, os.path.join(os.path.dirname(__file__), 'peer_sender.py'),
             cache, blocks, str(DEPTH), object_dir],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=env, text=True)
        self.addCleanup(proc.wait, 10)
        self.addCleanup(proc.stdin.close)
        return int(proc.stdout.readline())

    def test_held_leaf_is_skipped_and_the_directory_block_is_kept_and_checked(self):
        outer_dir, inner, leaf_a, leaf_b = self._object()
        sender_blocks = self.dirs['sender'][1]
        port = self._serve(outer_dir)

        # The receiver already holds leaf a, and nothing else.
        receiver_blocks = self.dirs['receiver'][1]
        shutil.copy(sender_blocks + leaf_a, receiver_blocks + leaf_a)
        self._use('receiver')

        received = Counter('received')
        channel = grpc.insecure_channel('127.0.0.1:%d' % port)
        self.addCleanup(channel.close)
        raw = channel.stream_stream(
            FULL_METHOD,
            request_serializer=buffer_pb2.Buffer.SerializeToString,
            response_deserializer=buffer_pb2.Buffer.FromString)
        results = list(client_grpc(
            method=lambda it, timeout=None: received.wrap(raw(it, timeout=timeout)),
            indices_parser={1: buffer_pb2.Buffer},
            partitions_message_mode_parser=False,
            block_skip=True))
        self.assertEqual(len(results), 1)

        stored = receiver_blocks + inner
        self.assertTrue(os.path.isdir(stored), 'the directory block was flattened')
        with open(os.path.join(stored, METADATA_FILE_NAME)) as a, \
                open(os.path.join(sender_blocks + inner, METADATA_FILE_NAME)) as b:
            self.assertEqual(json.load(a), json.load(b))
        self.assertTrue(os.path.isfile(receiver_blocks + leaf_b))
        self.assertEqual(_expansion(results[0].dir), _expansion(outer_dir))
        # Leaf a was not sent again, or not much of it.
        self.assertLess(received.chunk_bytes, LEAF + LEAF // 2,
                        'the held leaf crossed the wire: %r' % received)


if __name__ == '__main__':
    unittest.main()
