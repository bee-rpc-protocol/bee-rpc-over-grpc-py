#!/usr/bin/env python3
"""Where a block pointer's hash type comes from when it does not carry one.

A pointer names its block by hash, and `Buffer.Block.Hash.type` says which
algorithm produced it -- the digest of that algorithm over the empty input, so
the field describes itself without a registry.

On the wire the type is always written: a stream has no surrounding structure a
reader could consult. In storage writing it in every pointer of every block is
pure repetition -- a filesystem of a few thousand files pays for the same 32
bytes a few thousand times -- so a stored pointer may omit it and inherit
instead. Inheritance is positional and follows the block-containment chain:
hash `i` takes the type at index `i` from the nearest ancestor that has one,
each index resolved on its own. The top of a stored tree has no ancestor, so it
must carry its types; `Enviroment.hash_type` says what this node *packs* with,
which is not an answer to what somebody else's artefact was hashed with.

The library used to have this split without saying so: `regenerate_buffer` wrote
untyped pointers, everything else wrote typed ones, `BLOCK_LENGTH = 36` was the
length of the former, and `is_block` recognised only the latter -- so the object
a receiver stored and the object its builder produced were two different files,
and callers grew ad-hoc fallbacks to read both.

Run:  python -m unittest tests.test_hash_type_inheritance
"""
import hashlib
import json
import os
import shutil
import tempfile
import unittest

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.block_driver import generate_wbp_file
from bee_rpc.client import save_chunks_to_block
from bee_rpc.reader import read_multiblock_directory
from bee_rpc.utils import modify_env, Enviroment, HashTypeError, \
    block_id_from_pointer, block_pointer, block_pointer_length, \
    resolve_hash_types, inherit_hash_types, hash_types_for_packing, \
    METADATA_FILE_NAME, WITHOUT_BLOCK_POINTERS_FILE_NAME

SHA3_256 = hashlib.sha3_256(b"").digest()
BLAKE2B_256 = hashlib.blake2b(b"", digest_size=32).digest()
SHA2_256 = hashlib.sha256(b"").digest()


class TheRule(unittest.TestCase):
    """The resolution rule on its own, with no storage around it."""

    def _pointer(self, *types):
        block = buffer_pb2.Buffer.Block()
        for t in types:
            h = block.hashes.add()
            h.value = b"\x11" * 32
            if t is not None:
                h.type = t
        return block

    def test_an_explicit_type_answers_for_itself(self):
        self.assertEqual(resolve_hash_types(self._pointer(BLAKE2B_256)), (BLAKE2B_256,))

    def test_an_omitted_type_comes_from_the_ancestor_at_the_same_index(self):
        block = self._pointer(None, None)
        self.assertEqual(
            resolve_hash_types(block, inherited=(SHA3_256, BLAKE2B_256, SHA2_256)),
            (SHA3_256, BLAKE2B_256))

    def test_an_explicit_type_does_not_shift_the_ones_beside_it(self):
        # The fine case: hash 0 says sha2_256 -- which is index *2* of what it
        # inherits -- and hash 1 says nothing. Resolution is by position, so hash 1
        # is still blake2b_256. Realigning to "carry on after sha2_256" would need a
        # rule for what happens when a type appears twice in the ancestor list.
        block = self._pointer(SHA2_256, None)
        self.assertEqual(
            resolve_hash_types(block, inherited=(SHA3_256, BLAKE2B_256, SHA2_256)),
            (SHA2_256, BLAKE2B_256))

    def test_the_nearest_ancestor_wins_index_by_index(self):
        # Not just the parent: an index the nearer pointer does not reach is still
        # answered by whatever ancestor last spoke for it.
        grandparent = (SHA3_256, BLAKE2B_256, SHA2_256)
        parent = inherit_hash_types((BLAKE2B_256,), grandparent)
        self.assertEqual(parent, (BLAKE2B_256, BLAKE2B_256, SHA2_256))
        self.assertEqual(
            resolve_hash_types(self._pointer(None, None, None), inherited=parent),
            (BLAKE2B_256, BLAKE2B_256, SHA2_256))

    def test_undeducible_is_an_error_not_a_guess(self):
        with self.assertRaises(HashTypeError):
            resolve_hash_types(self._pointer(None))                       # no ancestor
        with self.assertRaises(HashTypeError):
            resolve_hash_types(self._pointer(None, None), inherited=(SHA3_256,))
        # Probing bytes that may simply be content answers None instead of raising.
        self.assertIsNone(block_id_from_pointer(self._pointer(None)))

    def test_the_registry_key_is_the_packing_type(self):
        self.assertEqual(hash_types_for_packing(), (Enviroment.hash_type,))
        # A pointer naming the same block under several algorithms still resolves to
        # the one the block registry is keyed by, whatever position it sits in.
        block = self._pointer(BLAKE2B_256, Enviroment.hash_type)
        block.hashes[1].value = b"\x22" * 32
        self.assertEqual(block_id_from_pointer(block), "22" * 32)


class StoredAndOnTheWire(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="hashtype-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.blocks = os.path.join(self.root, "blocks")
        os.makedirs(self.blocks)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.blocks + os.sep)
        # Both are global; whatever a test selects, put them back.
        self.addCleanup(setattr, Enviroment, "hash_type", Enviroment.hash_type)
        self.addCleanup(setattr, Enviroment, "hash_factory", Enviroment.hash_factory)
        self._n = 0

    def _file_block(self, data: bytes):
        self._n += 1
        path = os.path.join(self.root, "f%d.bin" % self._n)
        with open(path, "wb") as f:
            f.write(data)
        return block_builder.create_block(file_path=path, copy=True)

    def _object_with(self, pointer_bytes: bytes) -> buffer_pb2.Buffer:
        obj = buffer_pb2.Buffer()
        obj.chunk = pointer_bytes
        obj.head.CopyFrom(buffer_pb2.Buffer.Head(index=7))   # content after the pointer
        return obj

    def _inlined(self, payload: bytes) -> bytes:
        obj = buffer_pb2.Buffer()
        obj.chunk = payload
        obj.head.CopyFrom(buffer_pb2.Buffer.Head(index=7))
        return obj.SerializeToString()

    # -- the divergence this closes -------------------------------------
    def test_the_builders_wbp_and_the_drivers_are_the_same_file(self):
        payload = os.urandom(9000)
        block_hash, pointer = self._file_block(payload)
        _, directory = block_builder.build_multiblock(
            self._object_with(pointer.SerializeToString()), blocks=[block_hash])

        wbp = os.path.join(directory, WITHOUT_BLOCK_POINTERS_FILE_NAME)
        with open(wbp, "rb") as f:
            from_builder = f.read()
        generate_wbp_file(directory)                       # the receiver's path
        with open(wbp, "rb") as f:
            from_driver = f.read()
        self.assertEqual(from_builder, from_driver)

    def test_a_stored_root_carries_its_types_and_a_nested_one_omits_them(self):
        payload = os.urandom(9000)
        block_hash, pointer = self._file_block(payload)
        _, directory = block_builder.build_multiblock(
            self._object_with(pointer.SerializeToString()), blocks=[block_hash])
        wbp = os.path.join(directory, WITHOUT_BLOCK_POINTERS_FILE_NAME)

        generate_wbp_file(directory)                       # root: nothing above it
        root = buffer_pb2.Buffer(); root.ParseFromString(open(wbp, "rb").read())
        root_pointer = buffer_pb2.Buffer.Block(); root_pointer.ParseFromString(root.chunk)
        self.assertTrue(all(h.type for h in root_pointer.hashes))
        self.assertEqual(block_id_from_pointer(root_pointer), block_hash.hex())

        generate_wbp_file(directory, inherited=(Enviroment.hash_type,))
        nested = buffer_pb2.Buffer(); nested.ParseFromString(open(wbp, "rb").read())
        nested_pointer = buffer_pb2.Buffer.Block(); nested_pointer.ParseFromString(nested.chunk)
        self.assertFalse(any(h.type for h in nested_pointer.hashes))
        self.assertEqual(len(nested.chunk), len(root.chunk) - len(Enviroment.hash_type) - 2)

        # It names the same block -- but only to a reader that knows where it sits.
        self.assertEqual(
            block_id_from_pointer(nested_pointer, inherited=(Enviroment.hash_type,)),
            block_hash.hex())
        self.assertIsNone(block_id_from_pointer(nested_pointer))

    def test_an_object_whose_pointers_omit_their_types_still_expands_whole(self):
        # The regression the measured pointer length exists for. `is_block` now
        # recognises an untyped pointer, so it reaches arithmetic that used to size
        # every pointer by rebuilding it from its id -- 70 bytes for a 36-byte
        # pointer, skipping 34 bytes of the content that followed it.
        payload = os.urandom(9000)
        block_hash, _ = self._file_block(payload)
        untyped = block_pointer(block_id=block_hash, omit_types=True).SerializeToString()
        self.assertEqual(len(untyped), 36)

        _, directory = block_builder.build_multiblock(
            self._object_with(untyped), blocks=[block_hash],
            inherited=(Enviroment.hash_type,))
        expanded = b"".join(read_multiblock_directory(directory=directory, ignore_blocks=True))
        self.assertEqual(expanded, self._inlined(payload))

    def test_leaving_the_types_out_changes_the_bytes_but_not_the_id(self):
        """The property the compression rests on, and the reason it is safe to adopt.

        A pointer is replaced by its block's content in the expansion, so what the
        pointer itself looked like never reaches the hash. An object can therefore be
        re-stored with its types inherited without renaming itself or anything that
        points at it -- which for a service means the filesystem block and the service
        id are unchanged by the saving.
        """
        payloads = [os.urandom(9000 + i) for i in range(3)]
        hashes = [self._file_block(p)[0] for p in payloads]

        def built(omit_types):
            obj = buffer_pb2.Buffer()
            for block_hash in hashes:
                obj.block.hashes.add().value = block_pointer(
                    block_id=block_hash, omit_types=omit_types).SerializeToString()
            obj.chunk = b"tail content, after every pointer"
            object_id, directory = block_builder.build_multiblock(
                obj, blocks=hashes,
                inherited=(Enviroment.hash_type,) if omit_types else None)
            expansion = b"".join(read_multiblock_directory(
                directory=directory, ignore_blocks=True))
            return object_id, expansion, obj.ByteSize()

        typed_id, typed_expansion, typed_size = built(omit_types=False)
        inherited_id, inherited_expansion, inherited_size = built(omit_types=True)

        self.assertEqual(typed_expansion, inherited_expansion)
        self.assertEqual(typed_id, inherited_id)
        # 34 bytes of repeated hash type per pointer, plus the varints that shrink.
        self.assertLessEqual(inherited_size, typed_size - 3 * len(Enviroment.hash_type))

    def test_every_pointer_put_on_the_wire_carries_its_type(self):
        block_hash, pointer = self._file_block(os.urandom(9000))
        _, directory = block_builder.build_multiblock(
            self._object_with(pointer.SerializeToString()), blocks=[block_hash])
        # ...even when what is stored omits them.
        generate_wbp_file(directory, inherited=(Enviroment.hash_type,))

        markers = [p for p in read_multiblock_directory(directory=directory, ignore_blocks=False)
                   if isinstance(p, buffer_pb2.Buffer.Block)]
        self.assertTrue(markers)
        for marker in markers:
            self.assertTrue(all(h.type for h in marker.hashes))
            self.assertEqual(block_id_from_pointer(marker), block_hash.hex())

    def test_a_marker_without_a_type_is_refused_on_arrival(self):
        block_hash, _ = self._file_block(os.urandom(64))
        arriving = buffer_pb2.Buffer()
        arriving.block.CopyFrom(block_pointer(block_id=block_hash, omit_types=True))
        with self.assertRaises(Exception) as caught:
            save_chunks_to_block(block_buffer=arriving, buffer_iterator=iter(()), _json=[])
        self.assertIn("resolvable hash type", str(caught.exception))

    def test_the_wbp_arithmetic_no_longer_assumes_a_36_byte_pointer(self):
        # `BLOCK_LENGTH = 36` was the length of one encoding of one digest size.
        # Neither is fixed: the same block is 70 bytes of pointer with its type
        # spelled out, and a longer digest is longer again.
        modify_env(hash_factory=hashlib.sha3_512)
        os.makedirs(self.blocks, exist_ok=True)            # the switch drops the registry
        block_hash, pointer = self._file_block(os.urandom(9000))
        self.assertEqual(len(block_hash), 64)
        self.assertEqual(block_pointer_length(block_hash, omit_types=True), 68)
        self.assertEqual(block_pointer_length(block_hash), 135)

        _, directory = block_builder.build_multiblock(
            self._object_with(pointer.SerializeToString()), blocks=[block_hash])
        generate_wbp_file(directory)                       # used to raise here

        stored = buffer_pb2.Buffer()
        stored.ParseFromString(open(os.path.join(
            directory, WITHOUT_BLOCK_POINTERS_FILE_NAME), "rb").read())
        self.assertEqual(len(stored.chunk), 135)


if __name__ == "__main__":
    unittest.main()
