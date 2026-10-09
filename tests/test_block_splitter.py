#!/usr/bin/env python3
"""A block whose direct content is too large is given sub-blocks of its own.

`split_block` walks the block's expansion with the message type's descriptor and
moves the fields the policy names into blocks. The result has to be what a
builder would have written had it made those fields blocks to begin with: the
same `_.json`, the same expansion, the same id.

The limit is made small here; the 2 GiB case is the same code with a larger
number.

Run:  python -m pytest tests/test_block_splitter.py
"""
import json
import os
import shutil
import tempfile
import unittest

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.block_splitter import split_block, direct_content_size, SplitError
from bee_rpc.reader import read_block
from bee_rpc.utils import Enviroment, modify_env, METADATA_FILE_NAME, \
    WITHOUT_BLOCK_POINTERS_FILE_NAME, block_pointer

THRESHOLD = 50_000
LIMIT = 100_000


def _value_over_threshold(field, length):
    return field.name == 'value' and length >= THRESHOLD


class SplitBlock(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix='split-')
        self.addCleanup(shutil.rmtree, self.root, True)
        self.blocks = os.path.join(self.root, 'blocks') + os.sep
        os.makedirs(self.blocks)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.blocks)
        self.payloads = [os.urandom(120_000), os.urandom(3_000),
                         os.urandom(80_000), os.urandom(120_000)]  # a repeat below

    def _message(self, pointers: bool, threshold: int = THRESHOLD) -> buffer_pb2.Buffer:
        """Nested fields, some large; with `pointers`, the large ones as blocks."""
        msg = buffer_pb2.Buffer()
        for data in self.payloads + [self.payloads[0]]:
            h = msg.block.hashes.add()
            h.type = b'small-field'
            if pointers and len(data) >= threshold:
                h.value = block_pointer(self._as_block(data)).SerializeToString()
            else:
                h.value = data
        msg.block.previous_lengths_position.extend([7, 300, 70000])
        return msg

    def _as_block(self, data: bytes) -> bytes:
        path = os.path.join(self.root, 'payload')
        with open(path, 'wb') as f:
            f.write(data)
        block_hash, _ = block_builder.create_block(file_path=path, copy=True)
        return block_hash

    def _flat_block(self) -> str:
        data = self._message(pointers=False).SerializeToString()
        block_id = Enviroment.hash_factory(data).hexdigest()
        with open(self.blocks + block_id, 'wb') as f:
            f.write(data)
        return block_id

    def _expansion(self, block_id: str) -> bytes:
        return b''.join(read_block(block_id=block_id, ignore_blocks=True))

    def test_a_flat_block_becomes_what_the_builder_would_have_written(self):
        block_id = self._flat_block()
        before = self._expansion(block_id)

        self.assertTrue(split_block(block_id, buffer_pb2.Buffer, _value_over_threshold,
                                    limit=LIMIT))

        stored = self.blocks + block_id
        self.assertTrue(os.path.isdir(stored))
        self.assertEqual(self._expansion(block_id), before)
        self.assertLessEqual(direct_content_size(block_id), LIMIT)

        built_id, built_dir = block_builder.build_multiblock(
            self._message(pointers=True),
            blocks=[self._as_block(d) for d in self.payloads if len(d) >= THRESHOLD])
        self.assertEqual(built_id.hex(), block_id)
        with open(os.path.join(stored, METADATA_FILE_NAME)) as a, \
                open(os.path.join(built_dir, METADATA_FILE_NAME)) as b:
            self.assertEqual(json.load(a), json.load(b))

        # The pointer form parses, and is small.
        with open(os.path.join(stored, WITHOUT_BLOCK_POINTERS_FILE_NAME), 'rb') as f:
            wbp = f.read()
        self.assertLess(len(wbp), LIMIT)
        buffer_pb2.Buffer().ParseFromString(wbp)
        self.assertEqual([n for n in os.listdir(self.blocks) if '.tmp-' in n or '.old-' in n
                          or n.startswith('.split-')], [])

    def test_a_block_within_the_limit_is_left_alone(self):
        block_id = self._flat_block()
        self.assertFalse(split_block(block_id, buffer_pb2.Buffer, _value_over_threshold,
                                     limit=10 ** 9))
        self.assertTrue(os.path.isfile(self.blocks + block_id))

    def test_a_directory_block_with_too_much_inline_is_split_again(self):
        # Its sender made blocks of the 120 KB fields only: 80 KB stay direct.
        built_id, built_dir = block_builder.build_multiblock(
            self._message(pointers=True, threshold=100_000),
            blocks=[self._as_block(d) for d in self.payloads if len(d) >= 100_000])
        block_id = built_id.hex()
        shutil.move(built_dir.rstrip(os.sep), self.blocks + block_id)
        before = self._expansion(block_id)
        self.assertGreater(direct_content_size(block_id), 80_000)

        self.assertTrue(split_block(block_id, buffer_pb2.Buffer, _value_over_threshold,
                                    limit=50_000))
        self.assertLess(direct_content_size(block_id), 50_000)
        self.assertEqual(self._expansion(block_id), before)
        # Within the limit now: a second call leaves it.
        self.assertFalse(split_block(block_id, buffer_pb2.Buffer, _value_over_threshold,
                                     limit=50_000))

    def test_a_policy_that_cannot_bring_it_under_the_limit_leaves_it(self):
        block_id = self._flat_block()
        with self.assertRaises(SplitError):
            split_block(block_id, buffer_pb2.Buffer, lambda f, n: False, limit=LIMIT)
        self.assertTrue(os.path.isfile(self.blocks + block_id))
        self.assertEqual([n for n in os.listdir(self.blocks) if '.tmp-' in n], [])

    def test_a_stored_block_that_is_not_its_id_is_refused(self):
        block_id = self._flat_block()
        with open(self.blocks + block_id, 'r+b') as f:
            f.seek(10_000)
            f.write(b'\x00\x01\x02')
        # read_block refuses a corrupt single-file block before streaming it;
        # BlockIdMismatch is the same check on a directory block's expansion.
        with self.assertRaisesRegex(Exception, 'mismatch|hashes to'):
            split_block(block_id, buffer_pb2.Buffer, _value_over_threshold, limit=LIMIT)
        self.assertTrue(os.path.isfile(self.blocks + block_id))


if __name__ == '__main__':
    unittest.main()
