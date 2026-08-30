#!/usr/bin/env python3
"""A built object's id must be the hash of the stream it expands to.

`build_multiblock` returns an id meant to content-address the object. It was
computed by zipping the object's parts against the caller's `blocks` list --
but that list is deduplicated and ordered however the caller happened to gather
it, while the expansion interleaves parts and blocks in the order the metadata
file records them. The two coincide often enough that the single-block case
always looked right; they diverge as soon as the caller's order differs from the
object's, or one block is referenced twice.

Nothing inside the library reads the id back, so this only ever surfaced in a
caller that content-addresses by it -- and there it names content that does not
exist.

Run:  python -m unittest tests.test_object_id_matches_expansion
"""
import hashlib
import os
import shutil
import tempfile
import unittest

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.reader import read_multiblock_directory
from bee_rpc.utils import modify_env


class ObjectIdMatchesExpansion(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objid-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.blocks = os.path.join(self.root, "blocks")
        os.makedirs(self.blocks)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.blocks + os.sep)
        self._n = 0

    def _block(self, data: bytes):
        self._n += 1
        path = os.path.join(self.root, "f%d.bin" % self._n)
        with open(path, "wb") as f:
            f.write(data)
        return block_builder.create_block(file_path=path, copy=True)

    def _assert_id_is_the_expansion(self, obj, blocks):
        object_id, directory = block_builder.build_multiblock(obj, blocks)
        digest = hashlib.sha3_256()
        for chunk in read_multiblock_directory(directory=directory, ignore_blocks=True):
            digest.update(chunk)
        self.assertEqual(object_id.hex(), digest.hexdigest())

    def _two_pointers(self, first: bytes, second: bytes):
        # Buffer.Block.hashes is a repeated message with bytes fields, so it can
        # hold two pointers -- the shape a real object with several blocks has.
        obj = buffer_pb2.Buffer()
        for pointer in (first, second):
            obj.block.hashes.add().value = pointer
        return obj

    def test_no_blocks_at_all(self):
        obj = buffer_pb2.Buffer()
        obj.chunk = b"nothing stored out of line"
        self._assert_id_is_the_expansion(obj, [])

    def test_a_single_block(self):
        block_hash, block = self._block(os.urandom(9000))
        obj = buffer_pb2.Buffer()
        obj.chunk = block.SerializeToString()
        self._assert_id_is_the_expansion(obj, [block_hash])

    def test_blocks_listed_in_the_order_they_appear(self):
        h1, b1 = self._block(os.urandom(9000))
        h2, b2 = self._block(os.urandom(5000))
        obj = self._two_pointers(b1.SerializeToString(), b2.SerializeToString())
        self._assert_id_is_the_expansion(obj, [h1, h2])

    def test_blocks_listed_in_a_different_order_than_they_appear(self):
        # The caller's list says nothing about where the blocks sit in the object.
        h1, b1 = self._block(os.urandom(9000))
        h2, b2 = self._block(os.urandom(5000))
        obj = self._two_pointers(b1.SerializeToString(), b2.SerializeToString())
        self._assert_id_is_the_expansion(obj, [h2, h1])

    def test_one_block_referenced_twice(self):
        # Deduplicated storage: the list holds it once, the object expands it twice.
        block_hash, block = self._block(os.urandom(9000))
        pointer = block.SerializeToString()
        obj = self._two_pointers(pointer, pointer)
        self._assert_id_is_the_expansion(obj, [block_hash])

    def test_a_directory_block_counts_as_its_expansion(self):
        inner_hash, inner_block = self._block(os.urandom(9000))
        inner = buffer_pb2.Buffer()
        inner.chunk = inner_block.SerializeToString()
        inner_id, inner_dir = block_builder.build_multiblock(inner, [inner_hash])
        shutil.move(inner_dir.rstrip(os.sep),
                    os.path.join(self.blocks, inner_id.hex()))

        outer = buffer_pb2.Buffer()
        outer.chunk = buffer_pb2.Buffer.Block(
            hashes=[buffer_pb2.Buffer.Block.Hash(value=inner_id)]
        ).SerializeToString()
        self._assert_id_is_the_expansion(outer, [inner_id])


if __name__ == "__main__":
    unittest.main()
