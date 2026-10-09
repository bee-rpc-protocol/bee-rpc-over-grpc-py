#!/usr/bin/env python3
"""A block that is a multiblock directory keeps that form when it is sent.

An object can point at a block that is itself a multiblock directory (a container
filesystem, say, with one block per large file). Streamed *flat*, that block
reaches the receiver as one file holding its whole expansion: the sub-blocks are
inlined again. The receiver then holds an image it never needed to hold, and a
consumer that parses the block as a message fails once the expansion passes the
2 GiB protobuf limit.

With `block_depth` of 2 the sender frames the nested level too, and the receiver
writes the block as a directory of its own (`_.json`, the pointer form, the
sub-blocks as blocks). A receiver at depth 1 and a sender at depth 1 are
unchanged: still one flat file.

Run:  python -m unittest tests.test_multiblock_roundtrip
"""
import json
import typing
import os
import shutil
import tempfile
import unittest

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.block_driver import generate_wbp_file
from bee_rpc.client import parse_from_buffer, serialize_to_buffer
from bee_rpc.reader import block_exists, read_multiblock_directory
from bee_rpc.utils import Dir, Enviroment, modify_env, \
    METADATA_FILE_NAME, WITHOUT_BLOCK_POINTERS_FILE_NAME


def _expansion(path: str) -> bytes:
    if os.path.isdir(path):
        return b''.join(c for c in read_multiblock_directory(path, ignore_blocks=True)
                        if isinstance(c, bytes))
    with open(path, 'rb') as f:
        return f.read()


class MultiblockRoundTrip(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="mb-roundtrip-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self._files = 0
        self.sender = self._node("sender")
        self.receiver = self._node("receiver")
        self.addCleanup(modify_env, block_depth=1)  # a process-wide setting

    def _node(self, name: str) -> str:
        base = os.path.join(self.root, name)
        os.makedirs(os.path.join(base, "blocks"))
        os.makedirs(os.path.join(base, "cache"))
        return base

    def _use(self, node: str, depth: int):
        modify_env(cache_dir=os.path.join(node, "cache") + os.sep,
                   block_dir=os.path.join(node, "blocks") + os.sep,
                   block_depth=depth)

    # -- fixture, built on the sender -------------------------------------
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

    def _sender_object(self):
        """An object whose first field is a directory block with two sub-blocks."""
        leaf_a, ptr_a = self._file_block(os.urandom(40000))
        leaf_b, ptr_b = self._file_block(os.urandom(30000))
        inner = buffer_pb2.Buffer()
        inner.block.hashes.add().value = ptr_a
        inner.block.hashes.add().value = ptr_b
        inner_id, inner_dir = block_builder.build_multiblock(
            inner, blocks=[leaf_a, leaf_b])
        shutil.move(inner_dir.rstrip(os.sep),
                    os.path.join(Enviroment.block_dir, inner_id.hex()))
        tail_hash, tail_ptr = self._file_block(os.urandom(5000))
        outer = buffer_pb2.Buffer()
        outer.block.hashes.add().value = self._pointer_to(inner_id)
        outer.block.hashes.add().value = tail_ptr
        _, outer_dir = block_builder.build_multiblock(
            outer, blocks=[inner_id, tail_hash])
        return outer_dir, inner_id.hex(), (leaf_a.hex(), leaf_b.hex())

    def _wire(self, depth_sender: int):
        self._use(self.sender, depth_sender)
        outer_dir, inner, leaves = self._sender_object()
        wire = list(serialize_to_buffer(
            iter([Dir(dir=outer_dir, _type=buffer_pb2.Buffer)]),
            indices=buffer_pb2.Buffer))
        return wire, inner, leaves

    def _receive(self, wire, depth_receiver: int):
        self._use(self.receiver, depth_receiver)
        return list(parse_from_buffer(
            iter(wire), indices=buffer_pb2.Buffer, partitions_message_mode=False))

    def _tamper_inside(self, wire, block_id: str):
        """Flip one byte of the first chunk carried inside `block_id`."""
        inside = False
        for b in wire:
            if b.HasField('block') and b.block.hashes[0].value.hex() == block_id:
                inside = not inside
                continue
            if inside and b.HasField('chunk') and len(b.chunk) > 0:
                data = bytearray(b.chunk)
                data[len(data) // 2] ^= 0xFF
                b.chunk = bytes(data)
                return
        raise AssertionError("no chunk inside %s" % block_id)

    def _transfer(self, depth_sender: int, depth_receiver: int, receiver_holds_first_leaf=False):
        self._use(self.sender, depth_sender)
        outer_dir, inner, leaves = self._sender_object()
        if receiver_holds_first_leaf:
            shutil.copy(os.path.join(Enviroment.block_dir, leaves[0]),
                        os.path.join(self.receiver, "blocks", leaves[0]))
        wire = list(serialize_to_buffer(
            iter([Dir(dir=outer_dir, _type=buffer_pb2.Buffer)]),
            indices=buffer_pb2.Buffer))
        sender_inner = os.path.join(Enviroment.block_dir, inner)

        self._use(self.receiver, depth_receiver)
        received = list(parse_from_buffer(
            iter(wire), indices=buffer_pb2.Buffer, partitions_message_mode=False))
        self.assertEqual(len(received), 1)
        return outer_dir, received[0].dir, inner, leaves, sender_inner

    # -- tests ------------------------------------------------------------
    def test_directory_block_stays_a_directory_at_depth_two(self):
        outer_dir, got, inner, leaves, sender_inner = self._transfer(2, 2)
        stored = os.path.join(self.receiver, "blocks", inner)

        self.assertTrue(os.path.isdir(stored),
                        "the nested block arrived flattened into a single file")
        with open(os.path.join(stored, METADATA_FILE_NAME)) as f:
            got_entries = json.load(f)
        with open(os.path.join(sender_inner, METADATA_FILE_NAME)) as f:
            want_entries = json.load(f)
        self.assertEqual(got_entries, want_entries)

        # The pointer form is there, written the way a block nested in an object
        # writes it (hash types inherited from the pointer that names it), and the
        # large leaves are blocks of their own rather than inline bytes.
        expected = os.path.join(self.root, "expected")
        shutil.copytree(sender_inner, expected)
        generate_wbp_file(expected, inherited=(Enviroment.hash_type,))
        name = WITHOUT_BLOCK_POINTERS_FILE_NAME
        with open(os.path.join(stored, name), "rb") as a, \
                open(os.path.join(expected, name), "rb") as b:
            self.assertEqual(a.read(), b.read())
        for leaf in leaves:
            self.assertTrue(os.path.isfile(os.path.join(self.receiver, "blocks", leaf)))

        # And none of it changes what the object expands to.
        self.assertEqual(_expansion(got), _expansion(outer_dir))

    def test_flat_stream_is_still_accepted_by_a_depth_two_receiver(self):
        outer_dir, got, inner, leaves, _ = self._transfer(1, 2)
        stored = os.path.join(self.receiver, "blocks", inner)
        self.assertTrue(os.path.isfile(stored))
        self.assertEqual(_expansion(got), _expansion(outer_dir))

    def test_a_receiver_reads_nesting_deeper_than_it_sends(self):
        # block_depth is how deep a node frames what it sends; reading is not
        # limited by it, so receivers can be upgraded before senders.
        outer_dir, got, inner, leaves, sender_inner = self._transfer(2, 1)
        self.assertTrue(os.path.isdir(os.path.join(self.receiver, "blocks", inner)))
        self.assertEqual(_expansion(got), _expansion(outer_dir))

    def test_nesting_past_the_cap_is_refused(self):
        from bee_rpc.client import MAX_BLOCK_NESTING
        wire = [buffer_pb2.Buffer(head=buffer_pb2.Buffer.Head(index=1), chunk=b"x")]
        for level in range(MAX_BLOCK_NESTING + 1):
            wire.append(buffer_pb2.Buffer(block=buffer_pb2.Buffer.Block(hashes=[
                buffer_pb2.Buffer.Block.Hash(type=Enviroment.hash_type,
                                             value=level.to_bytes(32, "big"))])))
        with self.assertRaisesRegex(Exception, "nested more than"):
            self._receive(wire, 1)

    def test_depth_one_is_unchanged(self):
        outer_dir, got, inner, _, _ = self._transfer(1, 1)
        self.assertTrue(os.path.isfile(os.path.join(self.receiver, "blocks", inner)))
        self.assertEqual(_expansion(got), _expansion(outer_dir))

    def test_a_nested_block_the_receiver_holds_is_drained_not_rewritten(self):
        outer_dir, got, inner, leaves, sender_inner = self._transfer(
            2, 2, receiver_holds_first_leaf=True)
        stored = os.path.join(self.receiver, "blocks", inner)
        self.assertTrue(os.path.isdir(stored))
        with open(os.path.join(stored, METADATA_FILE_NAME)) as a, \
                open(os.path.join(sender_inner, METADATA_FILE_NAME)) as b:
            self.assertEqual(json.load(a), json.load(b))
        self.assertEqual(_expansion(got), _expansion(outer_dir))

    def test_no_scratch_is_left_in_the_block_dir(self):
        self._transfer(2, 2)
        left = [n for n in os.listdir(os.path.join(self.receiver, "blocks")) if ".tmp-" in n]
        self.assertEqual(left, [])

    def _dir_block(self, pointers_and_blocks):
        msg = buffer_pb2.Buffer()
        for ptr, _ in pointers_and_blocks:
            msg.block.hashes.add().value = ptr
        block_id, directory = block_builder.build_multiblock(
            msg, blocks=[b for _, b in pointers_and_blocks])
        shutil.move(directory.rstrip(os.sep),
                    os.path.join(Enviroment.block_dir, block_id.hex()))
        return block_id

    def test_every_level_of_a_deep_tree_crosses_the_wire_as_it_is_stored(self):
        self._use(self.sender, 8)
        leaf1, p1 = self._file_block(os.urandom(20000))
        leaf2, p2 = self._file_block(os.urandom(15000))
        leaf3, p3 = self._file_block(os.urandom(10000))
        inner = self._dir_block([(p1, leaf1), (p2, leaf2)])
        mid = self._dir_block([(self._pointer_to(inner), inner), (p3, leaf3)])
        top = buffer_pb2.Buffer()
        top.block.hashes.add().value = self._pointer_to(mid)
        top.block.hashes.add().value = p1  # a leaf again, outside the tree
        _, outer_dir = block_builder.build_multiblock(top, blocks=[mid, leaf1])
        sender_blocks = Enviroment.block_dir
        wire = list(serialize_to_buffer(
            iter([Dir(dir=outer_dir, _type=buffer_pb2.Buffer)]),
            indices=buffer_pb2.Buffer))

        got = self._receive(wire, 8)[0].dir
        for block in (mid, inner):
            stored = os.path.join(self.receiver, "blocks", block.hex())
            self.assertTrue(os.path.isdir(stored), "level %s was flattened" % block.hex())
            with open(os.path.join(stored, METADATA_FILE_NAME)) as a, \
                    open(os.path.join(sender_blocks, block.hex(), METADATA_FILE_NAME)) as b:
                self.assertEqual(json.load(a), json.load(b))
        for leaf in (leaf1, leaf2, leaf3):
            self.assertTrue(os.path.isfile(os.path.join(self.receiver, "blocks", leaf.hex())))
        self.assertEqual(_expansion(got), _expansion(outer_dir))

    # -- the id is checked on receipt ---------------------------------------
    def _assert_rejected(self, depth: int, pick):
        """Tamper inside the block `pick(inner, leaves)` names; every block that
        contains the tampered bytes must be refused."""
        wire, inner, leaves = self._wire(depth)
        tampered, rejected = pick(inner, leaves)
        self._tamper_inside(wire, tampered)
        with self.assertRaises(Exception) as caught:
            self._receive(wire, depth)
        self.assertIn("hashes to", str(caught.exception))
        stored = os.listdir(os.path.join(self.receiver, "blocks"))
        for block in rejected:
            self.assertNotIn(block, stored)
        self.assertEqual([n for n in stored if ".tmp-" in n], [])

    def test_a_flat_block_whose_content_is_not_its_id_is_refused(self):
        self._assert_rejected(1, lambda inner, leaves: (inner, [inner]))

    def test_a_nested_leaf_whose_content_is_not_its_id_is_refused(self):
        self._assert_rejected(2, lambda inner, leaves: (leaves[1], [leaves[1], inner]))

    def test_the_bytes_between_nested_blocks_are_checked_too(self):
        # A byte of the directory block's own content -- not of any leaf.
        wire, inner, leaves = self._wire(2)
        inside, depth_in = False, 0
        for b in wire:
            if b.HasField('block'):
                h = b.block.hashes[0].value.hex()
                if h == inner:
                    inside = not inside
                elif inside:
                    depth_in ^= 1  # entering or leaving a leaf
                continue
            if inside and not depth_in and b.HasField('chunk') and len(b.chunk) > 0:
                data = bytearray(b.chunk)
                data[0] ^= 0xFF
                b.chunk = bytes(data)
                break
        with self.assertRaises(Exception) as caught:
            self._receive(wire, 2)
        self.assertIn("hashes to", str(caught.exception))
        self.assertNotIn(inner, os.listdir(os.path.join(self.receiver, "blocks")))


if __name__ == "__main__":
    unittest.main()
