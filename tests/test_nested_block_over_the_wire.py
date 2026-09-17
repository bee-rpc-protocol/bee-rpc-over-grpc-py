#!/usr/bin/env python3
"""Streaming an object that references a *multiblock directory* block.

Storing a large sub-object as a block of its own (a container filesystem, say)
is only half the feature; the object then has to survive being streamed and
written back down. It did not.

`read_multiblock_directory(ignore_blocks=False)` frames each block it points at
with a `Buffer.Block` marker carrying that block's `previous_lengths_position`
-- offsets into *this* object's expanded stream, which is how a receiver
reconstructs the metadata. When the block was itself a multiblock directory,
the recursion into it kept ignore_blocks=False, so the nested level's markers
went out too, carrying offsets into the *nested* block's stream. A receiver
appends every marker it sees to its own `_.json`
(`client.save_chunks_to_block`), so the file ended up describing one stream in
two unrelated coordinate systems, and the length arithmetic over it read
varints out of the middle of file content -- which is what
`validate_lengths_tree` caught, far downstream of the cause.

Underneath that sat a second defect with the same shape: the length arithmetic
measured `file_list` entries with `os.path.getsize`, which for a directory
block reports the size of the dirent rather than the content it stands for, so
even a correctly framed stream put every position after such a block hundreds
of bytes off -- and a position landing inside one reached an `open()` on a
directory.

Run:  python -m unittest tests.test_nested_block_over_the_wire
"""
import json
import os
import shutil
import tempfile
import unittest

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.block_builder import get_hash
from bee_rpc.block_driver import generate_wbp_file
from bee_rpc.reader import read_multiblock_directory, block_exists
from bee_rpc.utils import modify_env, Enviroment, get_expanded_block_length, \
    get_varint_at_position, seek_expanded_position, block_pointer_length, \
    METADATA_FILE_NAME, WITHOUT_BLOCK_POINTERS_FILE_NAME
from bee_rpc.validate_lengths_tree import validate_lengths_tree


class NestedBlockOverTheWire(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="nested-wire-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.blocks = os.path.join(self.root, "blocks")
        os.makedirs(self.blocks)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.blocks + os.sep)
        self._files = 0

    # -- helpers ---------------------------------------------------------
    def _file_block(self, data: bytes):
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

    def _install_as_block(self, block_id: bytes, directory: str) -> str:
        name = block_id.hex()
        shutil.move(directory.rstrip(os.sep), os.path.join(self.blocks, name))
        exists, is_dir = block_exists(block_id=name, is_dir=True)
        self.assertTrue(exists and is_dir, "fixture is not a multiblock (dir) block")
        return name

    def _fixture(self):
        """An object pointing at a directory block, then at a plain one.

        The plain block *after* the directory one is the point: its own length
        varint sits past the directory block, so reaching it means measuring
        that block by what it expands to and not by its dirent.
        """
        inner_leaf, inner_ptr = self._file_block(os.urandom(9000))
        inner = buffer_pb2.Buffer()
        inner.chunk = inner_ptr
        inner_id, inner_dir = block_builder.build_multiblock(inner, blocks=[inner_leaf])
        inner_name = self._install_as_block(inner_id, inner_dir)

        tail_hash, tail_ptr = self._file_block(os.urandom(4000))
        outer = buffer_pb2.Buffer()
        outer.block.hashes.add().value = self._pointer_to(inner_id)
        outer.block.hashes.add().value = tail_ptr
        _, outer_dir = block_builder.build_multiblock(
            outer, blocks=[inner_id, tail_hash])
        return inner_name, tail_hash.hex(), outer_dir

    def _entries(self, directory: str):
        with open(os.path.join(directory, METADATA_FILE_NAME)) as f:
            return json.load(f)

    def _receive(self, framed, into: str):
        """What `client.save_to_dir` does with a framed stream, minus the gRPC.

        Parts are cut at every block boundary and the marker's positions are
        recorded verbatim -- copying the receiver exactly is the whole point,
        since the bug was that the sender handed it markers it could not place.
        """
        os.makedirs(into)
        _json, index, part, inside = [], 1, bytearray(), None
        for piece in framed:
            if isinstance(piece, buffer_pb2.Buffer.Block):
                if inside is None:
                    _json.append(index)
                    with open(os.path.join(into, str(index)), "wb") as f:
                        f.write(part)
                    part, index = bytearray(), index + 1
                    _json.append([get_hash(piece),
                                  list(piece.previous_lengths_position)])
                    inside = get_hash(piece)
                else:
                    inside = None
            elif inside is None:
                part += piece
        _json.append(index)
        with open(os.path.join(into, str(index)), "wb") as f:
            f.write(part)
        with open(os.path.join(into, METADATA_FILE_NAME), "w") as f:
            json.dump(_json, f)
        return _json

    # -- tests -----------------------------------------------------------
    def test_framed_stream_carries_only_this_objects_own_markers(self):
        inner_name, tail_name, outer_dir = self._fixture()

        framed = list(read_multiblock_directory(
            directory=outer_dir, ignore_blocks=False))
        markers = [p for p in framed if isinstance(p, buffer_pb2.Buffer.Block)]

        expected = {e[0]: tuple(e[1]) for e in self._entries(outer_dir)
                    if not isinstance(e, int)}
        self.assertEqual(set(expected), {inner_name, tail_name})

        # Every marker names a block this object points at, with the positions
        # this object recorded for it. A marker from the nested level would
        # fail both halves: an id nobody up here points at, and offsets into
        # the wrong stream.
        self.assertEqual([get_hash(m) for m in markers].count(inner_name), 2)
        for marker in markers:
            self.assertIn(get_hash(marker), expected)
            self.assertEqual(tuple(marker.previous_lengths_position),
                             expected[get_hash(marker)])

    def test_received_metadata_validates_and_a_wbp_can_be_written(self):
        inner_name, tail_name, outer_dir = self._fixture()
        received = os.path.join(self.root, "received")
        _json = self._receive(
            read_multiblock_directory(directory=outer_dir, ignore_blocks=False),
            into=received)

        # The receiver's metadata describes the same object the sender had.
        self.assertEqual([e[0] for e in _json if not isinstance(e, int)],
                         [inner_name, tail_name])

        blocks, file_list, pointer_lengths = {}, [], {}
        for e in _json:
            if isinstance(e, int):
                file_list.append(os.path.join(received, str(e)))
            else:
                file_list.append(os.path.join(self.blocks, e[0]))
                blocks.setdefault(e[0], []).append(e[1])
                pointer_lengths[e[1][-1]] = block_pointer_length(block_id=e[0])
        validate_lengths_tree(blocks=blocks, file_list=file_list,
                              pointer_lengths=pointer_lengths)   # raises if it does not

        generate_wbp_file(received)
        self.assertTrue(os.path.isfile(
            os.path.join(received, WITHOUT_BLOCK_POINTERS_FILE_NAME)))

    def test_positions_are_measured_past_and_inside_a_directory_block(self):
        inner_name, tail_name, outer_dir = self._fixture()
        entries = self._entries(outer_dir)
        file_list = [os.path.join(outer_dir, str(e)) if isinstance(e, int)
                     else os.path.join(self.blocks, e[0]) for e in entries]
        by_id = {e[0]: e[1] for e in entries if not isinstance(e, int)}

        # The varint for the block that comes *after* the directory one: only
        # reachable if the directory block measured as its expansion.
        self.assertEqual(get_varint_at_position(by_id[tail_name][-1], file_list),
                         get_expanded_block_length(block_name=tail_name))

        # And a position landing inside the directory block resolves against
        # that block's own entries instead of open()ing a directory.
        inside = by_id[inner_name][-1] + 1 + get_expanded_block_length(
            block_name=inner_name) // 2
        path, offset = seek_expanded_position(inside, file_list)
        self.assertTrue(os.path.isfile(path))
        self.assertLess(offset, os.path.getsize(path))


if __name__ == "__main__":
    unittest.main()
