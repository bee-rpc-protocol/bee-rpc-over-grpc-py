#!/usr/bin/env python3
"""Backend-agnostic pack-with-a-block: full build + byte-exact reconstruct + guard.

Companion to test_backend_agnostic_block_detection.py (which pins `contain_blocks`
on a block reached through a repeated-message field). This file exercises the full
`build_multiblock` -> reconstruct path for the same shape, and pins the new
inconsistent-state guard.

Regression context — the packer 400 "Worker exception: object supporting the
buffer API required":

  A file >= MIN_BUFFER_BLOCK_SIZE is stored as a block POINTER in a bytes field
  reachable only by descending through a *repeated message* field (the real path
  is Service.Container.Filesystem.branch[*].file). Block detection used to test
  for repeated-message fields with
  `isinstance(value, google._upb._message.RepeatedCompositeContainer)` — a class
  that ONLY exists / matches under the C/upb backend. The packer forces the
  PURE-PYTHON backend (for reproducible ids), where that isinstance is always
  False, so the repeated field was skipped, `search_on_message`'s container came
  back empty while `blocks` held N hashes, and build_multiblock emitted metadata
  whose block markers did not match the real blocks. That inconsistency only blew
  up much later, cryptically, when a Buffer.Block marker object reached a stream
  hash instead of bytes: `TypeError: object supporting the buffer API required`.

  The fix detects repeated-message fields via the DESCRIPTOR
  (FieldDescriptor.LABEL_REPEATED + TYPE_MESSAGE), independent of the backend.

This uses only the generated buffer_pb2 messages (stable under both protobuf
backends). The block pointer is planted in the `value` bytes of a Hash inside the
repeated-message field Buffer.Block.hashes — reachable ONLY through a singular
message (Buffer.block) and then a repeated message (hashes), exactly the traversal
that regressed. Run under either/both backends:

    PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python -m unittest tests.test_block_detection_backends
    PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=upb    python -m unittest tests.test_block_detection_backends
"""
import hashlib
import os
import shutil
import tempfile
import unittest

from google.protobuf.internal import api_implementation

from bee_rpc import block_builder, buffer_pb2
from bee_rpc.reader import read_multiblock_directory
from bee_rpc.utils import modify_env, Enviroment


class PackWithBlockBackends(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="pwb-")
        self.block_dir = os.path.join(self.root, "blocks")
        os.makedirs(self.block_dir)
        modify_env(cache_dir=self.root + os.sep, block_dir=self.block_dir + os.sep)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _build_with_blocks(self, n=4):
        """Two parallel Buffers whose blocks sit in the value bytes of Hashes inside
        the repeated-message field Buffer.block.hashes:
          - ``ptr``: each Hash.value holds the block POINTER (serialized Block)
          - ``raw``: each Hash.value holds the RAW file content
        ``raw.SerializeToString()`` is exactly what a byte-exact flattened
        reconstruction of ``ptr`` must equal. Both are *built*, never parsed.
        """
        blocks = []
        ptr = buffer_pb2.Buffer()
        raw = buffer_pb2.Buffer()
        for i in range(n):
            content = ("block-%d-" % i).encode() * (500 + i)  # unique per block
            path = os.path.join(self.root, "src_%d.bin" % i)
            with open(path, "wb") as f:
                f.write(content)
            file_hash, block = block_builder.create_block(file_path=path, copy=True)
            blocks.append(file_hash)

            hp = ptr.block.hashes.add()
            hp.type = b"\x01"
            hp.value = block.SerializeToString()   # the block pointer

            hr = raw.block.hashes.add()
            hr.type = b"\x01"
            hr.value = content                     # expanded content
        return ptr, raw, blocks

    def test_container_detects_every_block(self):
        ptr, _raw, blocks = self._build_with_blocks()
        container = {}
        block_builder.search_on_message(
            message=ptr, pointers=[], initial_position=0, blocks=blocks, container=container,
        )
        self.assertEqual(
            set(container.keys()), {b.hex() for b in blocks},
            "search_on_message must detect every block reached through a "
            "repeated-message field under the %s backend" % api_implementation.Type(),
        )

    def test_build_and_reconstruct_byte_exact(self):
        ptr, raw, blocks = self._build_with_blocks()
        obj_id, cache_dir = block_builder.build_multiblock(
            pf_object_with_block_pointers=ptr, blocks=blocks,
        )
        import json
        with open(cache_dir + "_.json") as fh:
            meta = json.load(fh)
        markers = sum(1 for e in meta if isinstance(e, list))
        self.assertEqual(markers, len(blocks),
                         "metadata must carry one marker per block (got %d, want %d)"
                         % (markers, len(blocks)))

        # Flattened reconstruction expands every pointer to the block's raw bytes;
        # it must be byte-identical to the same message built with raw content.
        rebuilt = b"".join(read_multiblock_directory(cache_dir, ignore_blocks=True))
        self.assertEqual(
            rebuilt, raw.SerializeToString(),
            "flattened reconstruction must be byte-exact under the %s backend"
            % api_implementation.Type(),
        )

    def test_guard_raises_clear_error_on_inconsistent_state(self):
        """The old bug (repeated field skipped -> empty container) must now fail with
        a CLEAR error, not the cryptic downstream buffer-API TypeError."""
        ptr, _raw, blocks = self._build_with_blocks()
        original = block_builder.search_on_message

        def crippled_search(message, pointers, initial_position, blocks, container):
            # Reproduce the pre-fix miss: never descend into repeated-message fields.
            from google.protobuf.descriptor import FieldDescriptor
            for field, value in message.ListFields():
                if field.label == FieldDescriptor.LABEL_REPEATED and \
                        field.type == FieldDescriptor.TYPE_MESSAGE:
                    continue
            # container intentionally left empty

        block_builder.search_on_message = crippled_search
        try:
            with self.assertRaises(Exception) as ctx:
                block_builder.build_multiblock(
                    pf_object_with_block_pointers=ptr, blocks=blocks,
                )
            self.assertNotIsInstance(ctx.exception, TypeError)  # not the cryptic one
            self.assertIn("inconsistent block state", str(ctx.exception))
        finally:
            block_builder.search_on_message = original

    def test_guard_accepts_consistent_state(self):
        """Sanity: with correct detection the guard is a no-op and packing succeeds."""
        ptr, _raw, blocks = self._build_with_blocks()
        container = {}
        block_builder.search_on_message(
            message=ptr, pointers=[], initial_position=0, blocks=blocks, container=container,
        )
        # Should not raise.
        block_builder.assert_container_covers_blocks(container=container, blocks=blocks)


if __name__ == "__main__":
    print("backend:", api_implementation.Type())
    unittest.main()
