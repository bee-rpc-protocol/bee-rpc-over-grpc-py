#!/usr/bin/env python3
"""A real nested directory block, skipped over the wire.

The two features meet here. Nested filesystem-as-block (#7) makes a directory
block stream as its expansion -- a block marker, the child's own marker and
chunks, then the parent's terminator -- and block skipping (#9) filters that same
stream, consulting the peer's skip set per chunk and suppressing a skipped
parent's children along with it.

`tests/test_block_skip_signal.py` covers the nesting cases of
`skip_requested_blocks` on *hand-built* streams, and covers the wire path with a
*flat* block. Neither exercises a genuine nested block produced by
`read_from_registry` going over a real socket, which is where the two changes
actually interact: the markers are built by the library rather than by the test,
and carry #7's typed-pointer encoding that `get_hash_from_block` has to resolve
for the skip filter to match them at all.

Run:  python -m pytest tests/test_nested_block_skip_combined.py
"""
import os
import shutil
import tempfile
import unittest

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.client import client_grpc, parse_from_buffer, serialize_to_buffer
from bee_rpc.control import StreamControl
from bee_rpc.reader import read_multiblock_directory
from bee_rpc.utils import Dir, modify_env

from tests.wire_harness import Harness

# `block_pointer` arrives with the nested filesystem-as-block change (#7), which
# is the other half of what this file tests. On a tree that has #9 but not yet
# #7 there is no nested directory block to skip, so there is nothing here to
# assert and the module skips itself rather than failing.
try:
    from bee_rpc.utils import block_pointer
except ImportError:  # pragma: no cover - depends on which PRs are merged
    block_pointer = None


def _flatten(directory) -> bytes:
    path = directory.dir if isinstance(directory, Dir) else directory
    return b''.join(read_multiblock_directory(directory=path, ignore_blocks=True))


@unittest.skipIf(block_pointer is None,
                 'needs nested filesystem-as-block (#7): no typed pointer encoder')
class NestedBlockSkip(unittest.TestCase):
    """Both ends share one `__block__`, so the receiver holds what is sent."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix='nested-skip-')
        self.blocks = os.path.join(self.root, 'blocks')
        os.makedirs(self.blocks)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.blocks + os.sep)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _file_block(self, name: str, payload: bytes):
        path = os.path.join(self.root, name)
        with open(path, 'wb') as f:
            f.write(payload)
        return block_builder.create_block(file_path=path, copy=True)

    def _nested_object(self):
        """An object pointing at a *directory* block that itself contains a block.

        The inner object is built, then moved into the registry under its own id
        so that it is a directory block -- the shape #7 added. The outer object
        points at it, so expanding the outer means expanding the inner too.
        """
        inner_hash, inner_pointer = self._file_block('leaf.bin', os.urandom(3 * 1024 * 1024))
        inner = buffer_pb2.Buffer()
        inner.chunk = inner_pointer.SerializeToString()
        inner_id, inner_dir = block_builder.build_multiblock(inner, blocks=[inner_hash])
        shutil.move(inner_dir.rstrip(os.sep), os.path.join(self.blocks, inner_id.hex()))

        # Typed, via the library's own encoder: at the top of an object there is
        # no ancestor to inherit a hash type from, so an untyped pointer would not
        # be recognised as one and the directory block would never expand.
        outer = buffer_pb2.Buffer()
        outer.chunk = block_pointer(block_id=inner_id).SerializeToString()
        _outer_id, outer_dir = block_builder.build_multiblock(outer, blocks=[inner_id])
        return Dir(dir=outer_dir, _type=buffer_pb2.Buffer), _flatten(outer_dir), inner_id.hex()

    def _exchange(self, source: Dir, block_skip: bool):
        def handler(request_iterator):
            control = StreamControl() if block_skip else None
            for _ in parse_from_buffer(
                request_iterator=request_iterator,
                indices=buffer_pb2.Empty,
                partitions_message_mode=True,
                control=control,
            ):
                break
            if control is not None:
                control.watch()
            yield from serialize_to_buffer(
                message_iterator=iter([source]),
                indices={1: buffer_pb2.Buffer},
                control=control,
            )

        with Harness(handler) as harness:
            results = list(client_grpc(
                method=harness.method(),
                indices_parser={1: buffer_pb2.Buffer},
                partitions_message_mode_parser=False,
                block_skip=block_skip,
            ))
            self.assertIsNone(harness.handler_error)
            self.assertEqual(len(results), 1)
            return _flatten(results[0].dir), harness

    def test_a_nested_directory_block_is_skipped_and_still_reconstructs(self):
        source, canonical, _inner_id = self._nested_object()

        baseline, base_h = self._exchange(source, block_skip=False)
        skipped, skip_h = self._exchange(source, block_skip=True)

        # The point of the feature: the same bytes come out either way.
        self.assertEqual(baseline, canonical,
                         'baseline reconstruction must match the canonical expansion')
        self.assertEqual(skipped, canonical,
                         'skipping a nested directory block must not change what is '
                         'reconstructed -- the receiver already holds the block')

        sent_baseline = base_h.sent.chunk_bytes
        sent_skipped = skip_h.sent.chunk_bytes
        self.assertLess(sent_skipped, sent_baseline // 2,
                        'the held nested block should not be transmitted: sent %d chunk '
                        'bytes with skipping vs %d without' % (sent_skipped, sent_baseline))

    def test_the_receiver_asks_for_the_nested_block(self):
        source, _canonical, inner_id = self._nested_object()
        _out, harness = self._exchange(source, block_skip=True)
        self.assertGreaterEqual(harness.upstream.skip_markers, 1,
                                'the receiver must emit a skip request for the block it holds')


if __name__ == '__main__':
    unittest.main()
