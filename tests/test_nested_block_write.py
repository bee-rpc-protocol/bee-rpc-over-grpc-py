#!/usr/bin/env python3
"""Write-side support for referencing a *multiblock directory* block.

Reading a block that is itself a multiblock directory has always worked --
`read_block` recurses into it. Writing an object that *references* one did not:
the builder measured every referenced block with `os.path.getsize`, which for a
directory reports the size of the dirent rather than the content, so
`get_block_length` refused the case outright ("multiblock blocks dont
supported") and `generate_id` raised IsADirectoryError trying to hash it.

That is what kept a large sub-object from being stored as one block of its own.
A container filesystem, say, had to be inlined whole into the object that holds
it, so every reader of that object paid for the entire filesystem just to reach
the fields beside it. With this, the object keeps a 36-byte pointer and the
filesystem becomes a block -- itself multiblock, holding its own per-file
sub-blocks.

The invariant under test is that none of this is visible downstream: an object
built with a nested block must expand to exactly the bytes it would have had
with everything inlined, because that expansion is what gets hashed, streamed
and content-addressed.

Run:  python -m unittest tests.test_nested_block_write
"""
import json
import os
import shutil
import tempfile
import unittest

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.reader import read_multiblock_directory, block_exists
from bee_rpc.utils import modify_env, Enviroment, getsize, get_expanded_block_length, \
    METADATA_FILE_NAME


class NestedBlockWrite(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="nested-write-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.blocks = os.path.join(self.root, "blocks")
        os.makedirs(self.blocks)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.blocks + os.sep)
        self._files = 0

    # -- helpers ---------------------------------------------------------
    def _file_block(self, data: bytes):
        """A single-file block, plus the pointer that stands in for it."""
        self._files += 1
        path = os.path.join(self.root, "f%d.bin" % self._files)
        with open(path, "wb") as f:
            f.write(data)
        block_hash, block = block_builder.create_block(file_path=path, copy=True)
        return block_hash, block.SerializeToString()

    def _pointer_to(self, block_id: bytes) -> bytes:
        return buffer_pb2.Buffer.Block(
            hashes=[buffer_pb2.Buffer.Block.Hash(
                type=Enviroment.hash_type, value=block_id)]
        ).SerializeToString()

    def _expand(self, directory: str) -> bytes:
        """The flat byte stream a reader sees. Deliberately unfiltered: a
        Buffer.Block leaking out of an ignore_blocks=True expansion must fail
        here, not be quietly dropped as every caller used to have to do."""
        return b"".join(read_multiblock_directory(
            directory=directory, ignore_blocks=True))

    def _install_as_block(self, block_id: bytes, directory: str) -> str:
        name = block_id.hex()
        shutil.move(directory.rstrip(os.sep), os.path.join(self.blocks, name))
        exists, is_dir = block_exists(block_id=name, is_dir=True)
        self.assertTrue(exists and is_dir, "fixture is not a multiblock (dir) block")
        return name

    # -- tests -----------------------------------------------------------
    def test_object_referencing_a_directory_block_round_trips(self):
        # An inner object with two sub-blocks of its own, stored as one block.
        h1, ptr1 = self._file_block(os.urandom(9000))
        h2, ptr2 = self._file_block(os.urandom(5000))
        inner = buffer_pb2.Buffer()
        inner.head.CopyFrom(buffer_pb2.Buffer.Head(index=1))
        inner.chunk = ptr1
        inner_id, inner_dir = block_builder.build_multiblock(inner, blocks=[h1])
        inner_expanded = self._expand(inner_dir)
        inner_name = self._install_as_block(inner_id, inner_dir)

        # The outer object holds a 36-byte pointer where the inner object's
        # whole content would otherwise sit.
        outer = buffer_pb2.Buffer()
        outer.head.CopyFrom(buffer_pb2.Buffer.Head(index=2))
        outer.chunk = self._pointer_to(inner_id)
        self.assertLess(outer.ByteSize(), len(inner_expanded),
                        "the pointer should be far smaller than what it stands for")

        outer_id, outer_dir = block_builder.build_multiblock(outer, blocks=[inner_id])

        # What the outer object would have been with the inner content inlined.
        inlined = buffer_pb2.Buffer()
        inlined.head.CopyFrom(buffer_pb2.Buffer.Head(index=2))
        inlined.chunk = inner_expanded
        self.assertEqual(self._expand(outer_dir), inlined.SerializeToString())

    def test_expansion_of_a_nested_block_yields_only_bytes(self):
        h1, ptr1 = self._file_block(os.urandom(9000))
        inner = buffer_pb2.Buffer()
        inner.chunk = ptr1
        inner_id, inner_dir = block_builder.build_multiblock(inner, blocks=[h1])
        inner_name = self._install_as_block(inner_id, inner_dir)

        pieces = list(read_multiblock_directory(
            directory=os.path.join(self.blocks, inner_name), ignore_blocks=True))
        self.assertTrue(all(isinstance(p, bytes) for p in pieces),
                        "ignore_blocks=True leaked a Buffer.Block from a nested level")

        # ...and the framed form still frames, at every level.
        framed = list(read_multiblock_directory(
            directory=os.path.join(self.blocks, inner_name), ignore_blocks=False))
        self.assertTrue(any(isinstance(p, buffer_pb2.Buffer.Block) for p in framed))

    def test_measured_length_matches_what_the_reader_emits(self):
        h1, ptr1 = self._file_block(os.urandom(9000))
        inner = buffer_pb2.Buffer()
        inner.chunk = ptr1
        inner_id, inner_dir = block_builder.build_multiblock(inner, blocks=[h1])
        inner_name = self._install_as_block(inner_id, inner_dir)
        block_path = os.path.join(self.blocks, inner_name)

        emitted = len(self._expand(block_path))
        self.assertEqual(get_expanded_block_length(block_name=inner_name), emitted)
        self.assertEqual(getsize(block_path), emitted)
        self.assertEqual(block_builder.get_block_length(inner_name), emitted)

    def test_one_block_referenced_twice_is_not_a_loop(self):
        # Deduplicated storage means one block is legitimately referenced many
        # times from the same object. Measuring must guard against a block that
        # contains *itself*, not against seeing the same id twice, so the guard
        # has to be the recursion stack rather than a set of everything visited.
        h1, ptr1 = self._file_block(os.urandom(9000))
        inner = buffer_pb2.Buffer()
        inner.chunk = ptr1
        inner_id, inner_dir = block_builder.build_multiblock(inner, blocks=[h1])
        inner_name = self._install_as_block(inner_id, inner_dir)
        once = len(self._expand(os.path.join(self.blocks, inner_name)))

        parent = os.path.join(self.root, "twice")
        os.makedirs(parent)
        for part in ("1", "2"):
            with open(os.path.join(parent, part), "wb") as f:
                f.write(b"x" * 10)
        with open(os.path.join(parent, METADATA_FILE_NAME), "w") as f:
            json.dump([1, [inner_name, [0]], 2, [inner_name, [0]]], f)

        self.assertEqual(getsize(parent), 2 * once + 20)
        self.assertEqual(len(self._expand(parent)), 2 * once + 20)

    def test_a_block_that_contains_itself_is_refused(self):
        # The guard the stack still has to provide: measuring must terminate.
        looping = os.path.join(self.blocks, "loop")
        os.makedirs(looping)
        with open(os.path.join(looping, METADATA_FILE_NAME), "w") as f:
            json.dump([["loop", [0]]], f)

        with self.assertRaises(Exception) as caught:
            get_expanded_block_length(block_name="loop")
        self.assertIn("recursive loop", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
