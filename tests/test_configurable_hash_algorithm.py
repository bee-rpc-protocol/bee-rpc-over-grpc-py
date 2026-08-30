#!/usr/bin/env python3
"""Which algorithm a node addresses its blocks by, and how a bad object is reported.

`Enviroment.hash_type` was a hex literal sitting beside four separate hardcoded
`hashlib.sha3_256()` calls -- in get_file_hash, in generate_id, and in the two
places that verify a block's content against its id. Changing it renamed what
the node claimed to be hashing with and changed nothing about what it computed,
so a node configured for blake2b would have written blake2b in every pointer and
sha3_256 in every block name. The algorithm is now the setting, and the type
follows from it: a hash type *is* that algorithm over the empty input.

The other half is how a stored object that turns out not to describe its own
parts gets reported. `generate_wbp_file` printed the numbers to stdout and called
`exit()` -- a library killing its caller's process, with no traceback, no chance
to clean up, and the diagnosis interleaved into whatever the caller was printing.

Run:  python -m unittest tests.test_configurable_hash_algorithm
"""
import contextlib
import functools
import hashlib
import io
import json
import os
import shutil
import tempfile
import unittest

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.block_driver import generate_wbp_file
from bee_rpc.reader import read_block, read_multiblock_directory
from bee_rpc.utils import modify_env, Enviroment, HashTypeError, LengthsValidationError, \
    hash_type_of, hasher_for, register_hash_algorithm, block_id_from_pointer, \
    METADATA_FILE_NAME


class ConfigurableHashAlgorithm(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="hashalg-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.blocks = os.path.join(self.root, "blocks")
        os.makedirs(self.blocks)
        # Global, so put it back whatever the test does.
        self.addCleanup(setattr, Enviroment, "hash_type", Enviroment.hash_type)
        self.addCleanup(setattr, Enviroment, "hash_factory", Enviroment.hash_factory)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.blocks + os.sep)

    def _use(self, factory):
        """Switch algorithm. Doing so drops the previous registry, by design: the
        block names in it were produced by an algorithm this node no longer speaks."""
        modify_env(hash_factory=factory)
        self.assertFalse(os.path.isdir(self.blocks), "the stale registry should be gone")
        os.makedirs(self.blocks, exist_ok=True)

    def _payload_block(self, data: bytes):
        path = os.path.join(self.root, "payload.bin")
        with open(path, "wb") as f:
            f.write(data)
        return block_builder.create_block(file_path=path, copy=True)

    # -- the setting --------------------------------------------------------
    def test_the_default_is_sha3_256_and_its_type_follows_from_it(self):
        self.assertIs(Enviroment.hash_factory, hashlib.sha3_256)
        self.assertEqual(Enviroment.hash_type, hashlib.sha3_256(b"").digest())
        # The literal the library used to carry.
        self.assertEqual(Enviroment.hash_type.hex(),
                         "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")

    def test_the_client_can_choose_and_everything_follows(self):
        blake2b_256 = functools.partial(hashlib.blake2b, digest_size=32)
        self._use(blake2b_256)
        self.assertEqual(Enviroment.hash_type, hashlib.blake2b(b"", digest_size=32).digest())

        payload = os.urandom(9000)
        block_hash, pointer = self._payload_block(payload)
        # The block is named by the chosen algorithm, not by sha3_256...
        self.assertEqual(block_hash.hex(), hashlib.blake2b(payload, digest_size=32).hexdigest())
        self.assertTrue(os.path.isfile(os.path.join(self.blocks, block_hash.hex())))
        # ...and the pointer written for it says so.
        self.assertEqual(pointer.hashes[0].type, Enviroment.hash_type)
        self.assertEqual(block_id_from_pointer(pointer), block_hash.hex())

    def test_an_object_round_trips_under_the_chosen_algorithm(self):
        self._use(hashlib.sha3_512)
        payload = os.urandom(9000)
        block_hash, pointer = self._payload_block(payload)
        self.assertEqual(len(block_hash), 64)

        obj = buffer_pb2.Buffer()
        obj.chunk = pointer.SerializeToString()
        obj.head.CopyFrom(buffer_pb2.Buffer.Head(index=7))
        object_id, directory = block_builder.build_multiblock(obj, blocks=[block_hash])

        inlined = buffer_pb2.Buffer()
        inlined.chunk = payload
        inlined.head.CopyFrom(buffer_pb2.Buffer.Head(index=7))
        expanded = b"".join(read_multiblock_directory(directory=directory, ignore_blocks=True))
        self.assertEqual(expanded, inlined.SerializeToString())
        # The object's id is its expansion under the same algorithm -- generate_id
        # used to reach for sha3_256 whatever the node was configured with.
        self.assertEqual(object_id, hashlib.sha3_512(expanded).digest())
        generate_wbp_file(directory)

    def test_content_is_verified_against_the_chosen_algorithm(self):
        self._use(hashlib.sha3_512)
        block_hash, _ = self._payload_block(os.urandom(9000))
        self.assertTrue(b"".join(read_block(block_id=block_hash.hex())))

        with open(os.path.join(self.blocks, block_hash.hex()), "r+b") as f:
            f.seek(0)
            f.write(b"\x00" * 64)                     # corrupt at rest
        with self.assertRaises(Exception) as caught:
            b"".join(read_block(block_id=block_hash.hex()))
        self.assertIn("hash mismatch", str(caught.exception))

    # -- the registry -------------------------------------------------------
    def test_an_unknown_type_is_refused_rather_than_guessed(self):
        with self.assertRaises(HashTypeError):
            hasher_for(b"\x00" * 32)

    def test_an_algorithm_can_be_registered_and_then_selected_by_its_type(self):
        exotic = functools.partial(hashlib.blake2s, digest_size=20)
        exotic_type = hash_type_of(exotic)
        with self.assertRaises(HashTypeError):
            hasher_for(exotic_type)

        register_hash_algorithm(exotic)
        self.assertIs(hasher_for(exotic_type), exotic)

        modify_env(hash_type=exotic_type)             # selected by identifier alone
        os.makedirs(self.blocks, exist_ok=True)
        self.assertEqual(Enviroment.hash_type, exotic_type)
        block_hash, _ = self._payload_block(b"content")
        self.assertEqual(block_hash, hashlib.blake2s(b"content", digest_size=20).digest())

    def test_selecting_an_unregistered_type_fails_instead_of_relabelling(self):
        # The failure this replaces: hash_type moved, the four hardcoded sha3_256
        # calls did not, and the node wrote one algorithm's name over another's work.
        with self.assertRaises(HashTypeError):
            modify_env(hash_type=hashlib.shake_128(b"").digest(32))
        self.assertIs(Enviroment.hash_factory, hashlib.sha3_256)


class BadObjectsAreReported(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="badobj-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.blocks = os.path.join(self.root, "blocks")
        os.makedirs(self.blocks)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.blocks + os.sep)

    def _object_with_a_lying_metadata(self) -> str:
        path = os.path.join(self.root, "payload.bin")
        with open(path, "wb") as f:
            f.write(os.urandom(9000))
        block_hash, pointer = block_builder.create_block(file_path=path, copy=True)
        obj = buffer_pb2.Buffer()
        obj.chunk = pointer.SerializeToString()
        obj.head.CopyFrom(buffer_pb2.Buffer.Head(index=7))
        _, directory = block_builder.build_multiblock(obj, blocks=[block_hash])

        # Point the block's varint somewhere that holds content rather than its
        # length -- the shape a receiver ends up with when the positions it was sent
        # belong to a different stream.
        metadata = os.path.join(directory, METADATA_FILE_NAME)
        with open(metadata) as f:
            _json = json.load(f)
        for entry in _json:
            if not isinstance(entry, int):
                entry[1][-1] = 0
        with open(metadata, "w") as f:
            json.dump(_json, f)
        return directory

    def test_it_raises_instead_of_killing_the_process(self):
        directory = self._object_with_a_lying_metadata()
        with self.assertRaises(LengthsValidationError) as caught:
            generate_wbp_file(directory)
        message = str(caught.exception)
        self.assertIn("does not describe its parts", message)
        self.assertIn("position 0", message)

    def test_it_says_nothing_on_stdout(self):
        directory = self._object_with_a_lying_metadata()
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            with self.assertRaises(LengthsValidationError):
                generate_wbp_file(directory)
        self.assertEqual(captured.getvalue(), "")

    def test_the_running_commentary_goes_to_the_debug_callback(self):
        directory = self._object_with_a_lying_metadata()
        lines = []
        with self.assertRaises(LengthsValidationError):
            generate_wbp_file(directory, debug=lines.append)
        self.assertTrue(any(line.startswith("Blocks:") for line in lines), lines)


if __name__ == "__main__":
    unittest.main()
