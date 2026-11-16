#!/usr/bin/env python3
"""Writing the wbp of a stored object measures each of its entries a bounded number of times.

`generate_wbp_file` looks up one position in the expanded stream per block pointer
(`validate_lengths_tree`, `compute_wbp_lengths`) and rewrites one varint per pointer
and per enclosing message (`regenerate_buffer`). Each of those lookups used to walk
the entries from the first one, measuring every entry before the position again --
a stat() per part, and for a multiblock directory a walk of its metadata. An object
with N entries cost O(N^2) stat() calls: receiving a 138 MB service with a few
hundred blocks spent 16 of its 20 seconds there, in about 2 million getsize() calls.

The entries are now measured once per object (`expanded_starts`) and a position is
found by bisection. The bytes written are the same.

Run:  python -m unittest tests.test_wbp_scales_linearly
"""
import os
import shutil
import tempfile
import unittest
from unittest import mock

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.block_driver import generate_wbp_file
from bee_rpc.utils import modify_env, expanded_starts, getsize, seek_expanded_position, \
    WITHOUT_BLOCK_POINTERS_FILE_NAME


def _linear_seek(position, file_list):
    """The lookup as it was: walk from the first entry, measuring each one."""
    remaining = position
    for path in file_list:
        length = getsize(path)
        if remaining < length:
            return path, remaining
        remaining -= length
    raise ValueError(position)


class WbpScalesLinearly(unittest.TestCase):
    BLOCKS = 300

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="wbp-linear-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.blocks = os.path.join(self.root, "blocks")
        os.makedirs(self.blocks)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.blocks + os.sep)

    def _object_with_many_blocks(self, count):
        obj = buffer_pb2.Buffer()
        hashes = []
        for i in range(count):
            path = os.path.join(self.root, "f%d.bin" % i)
            with open(path, "wb") as f:
                f.write(os.urandom(64 + i))
            block_hash, pointer = block_builder.create_block(file_path=path, copy=True)
            obj.block.hashes.add().value = pointer.SerializeToString()
            hashes.append(block_hash)
        _, directory = block_builder.build_multiblock(obj, blocks=hashes)
        return directory

    def test_the_wbp_is_the_same_file_and_each_entry_is_measured_a_few_times(self):
        directory = self._object_with_many_blocks(self.BLOCKS)
        wbp = os.path.join(directory, WITHOUT_BLOCK_POINTERS_FILE_NAME)
        with open(wbp, "rb") as f:
            from_builder = f.read()
        os.remove(wbp)

        real_getsize = os.path.getsize
        calls = []

        def counting_getsize(path):
            calls.append(path)
            return real_getsize(path)

        with mock.patch("os.path.getsize", counting_getsize):
            generate_wbp_file(directory)

        with open(wbp, "rb") as f:
            self.assertEqual(f.read(), from_builder)
        # One part between each pair of blocks: about 2N entries, each measured by
        # expanded_starts for the lookups and again for the rewrite, plus the wbp
        # length estimate. Walking from the first entry for every lookup is ~N^2/2
        # per pass: 45,000 calls a pass for N = 300.
        self.assertLess(len(calls), 10 * self.BLOCKS, f"{len(calls)} getsize() calls")

    def test_bisection_finds_the_entry_the_walk_finds(self):
        sizes = [0, 5, 0, 0, 1, 7, 0, 3, 0]          # empty entries hold no position
        file_list = []
        for i, size in enumerate(sizes):
            path = os.path.join(self.root, "part%d" % i)
            with open(path, "wb") as f:
                f.write(b"x" * size)
            file_list.append(path)
        starts = expanded_starts(file_list)
        self.assertEqual(starts[-1], sum(sizes))
        for position in range(sum(sizes)):
            expected = _linear_seek(position, file_list)
            self.assertEqual(seek_expanded_position(position, file_list), expected)
            self.assertEqual(seek_expanded_position(position, file_list, starts=starts), expected)
        for outside in (sum(sizes), sum(sizes) + 1, -1):
            with self.assertRaises(ValueError):
                seek_expanded_position(outside, file_list, starts=starts)

    def test_bytes_in_memory_measure_as_their_length(self):
        path = os.path.join(self.root, "block")
        with open(path, "wb") as f:
            f.write(b"y" * 11)
        self.assertEqual(expanded_starts([b"abc", path, b"", b"de"]), [0, 3, 14, 14, 16])


if __name__ == "__main__":
    unittest.main()
