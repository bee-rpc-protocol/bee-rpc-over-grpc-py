#!/usr/bin/env python3
"""Regression tests for the protobuf FieldDescriptor API this package reads.

Block traversal has to know which fields are repeated messages, and it used to ask
`field.label == FieldDescriptor.LABEL_REPEATED`. protobuf deprecated `label` in 5.27
and removed it in 7.x, so on a current protobuf that attribute access raises

    AttributeError: 'google._upb._message.FieldDescriptor' object has no attribute 'label'

from inside `contain_blocks`, which every `serialize_to_buffer` call reaches — meaning
no message could be sent at all, not merely ones containing blocks.

`is_repeated_message_field` answers the same question through `is_repeated` where it
exists, falling back to `label` for protobuf < 5.27. These tests pin its answers against
a message that has one of each field kind, and pin that the answers do not depend on
which of the two attributes the installed protobuf provides.

Run:  python -m unittest tests.test_field_descriptor_compat
"""
import unittest

from google.protobuf.descriptor import FieldDescriptor

from bee_rpc import buffer_pb2
from bee_rpc.utils import is_repeated_message_field


class IsRepeatedMessageField(unittest.TestCase):
    """Buffer carries every field kind that matters, so no fixture .proto is needed."""

    def _field(self, message, name):
        return message.DESCRIPTOR.fields_by_name[name]

    def test_a_repeated_message_field_is_traversed(self):
        # Buffer.Block.hashes is `repeated buffer.Buffer.Block.Hash hashes`.
        self.assertTrue(is_repeated_message_field(self._field(buffer_pb2.Buffer.Block, "hashes")))

    def test_a_singular_message_field_is_not(self):
        # Recursed into elsewhere, by the `isinstance(value, Message)` branch.
        self.assertFalse(is_repeated_message_field(self._field(buffer_pb2.Buffer, "block")))

    def test_a_scalar_field_is_not(self):
        self.assertFalse(is_repeated_message_field(self._field(buffer_pb2.Buffer, "chunk")))

    def test_a_repeated_scalar_field_is_not(self):
        # Repeated, but not messages: nothing to recurse into.
        self.assertFalse(
            is_repeated_message_field(
                self._field(buffer_pb2.Buffer.Block, "previous_lengths_position")
            )
        )

    def test_a_map_field_is_not(self):
        # protobuf models a map as a repeated message of synthesized entries, which is
        # why the map_entry check exists (cfc527a). google.protobuf.Struct.fields is a
        # map<string, Value>, so it exercises that without a fixture .proto.
        from google.protobuf.struct_pb2 import Struct
        field = Struct.DESCRIPTOR.fields_by_name["fields"]
        self.assertTrue(field.message_type.GetOptions().map_entry)  # the shape is right
        self.assertFalse(is_repeated_message_field(field))


class FieldDescriptorApiTolerance(unittest.TestCase):
    """The point of the helper: the same answer whichever attribute protobuf offers."""

    class _OnlyIsRepeated:
        """protobuf >= 5.27, including 7.x, where `label` no longer exists."""
        def __init__(self, real):
            self.is_repeated = bool(getattr(real, "is_repeated", None)) if hasattr(real, "is_repeated") \
                else real.label == FieldDescriptor.LABEL_REPEATED
            self.type = real.type
            self.message_type = real.message_type

        def __getattr__(self, name):
            if name == "label":
                raise AttributeError(
                    "'google._upb._message.FieldDescriptor' object has no attribute 'label'"
                )
            raise AttributeError(name)

    class _OnlyLabel:
        """protobuf < 5.27, where `is_repeated` does not exist yet."""
        def __init__(self, real):
            repeated = bool(getattr(real, "is_repeated", None)) if hasattr(real, "is_repeated") \
                else real.label == FieldDescriptor.LABEL_REPEATED
            self.label = FieldDescriptor.LABEL_REPEATED if repeated else FieldDescriptor.LABEL_OPTIONAL
            self.type = real.type
            self.message_type = real.message_type

        def __getattr__(self, name):
            raise AttributeError(name)

    def test_both_descriptor_generations_give_the_same_answer(self):
        for name, expected in (("block", False), ("chunk", False), ("signal", False)):
            real = buffer_pb2.Buffer.DESCRIPTOR.fields_by_name[name]
            with self.subTest(field=name):
                self.assertEqual(is_repeated_message_field(self._OnlyIsRepeated(real)), expected)
                self.assertEqual(is_repeated_message_field(self._OnlyLabel(real)), expected)

    def test_a_repeated_message_field_survives_both_generations(self):
        real = buffer_pb2.Buffer.Block.DESCRIPTOR.fields_by_name["hashes"]
        self.assertTrue(is_repeated_message_field(self._OnlyIsRepeated(real)))
        self.assertTrue(is_repeated_message_field(self._OnlyLabel(real)))

    def test_the_new_api_is_not_reached_through_label(self):
        # Guards the regression directly: touching `label` on a modern descriptor raises,
        # so if the helper ever reads it first this fails with that AttributeError.
        real = buffer_pb2.Buffer.Block.DESCRIPTOR.fields_by_name["hashes"]
        self.assertTrue(is_repeated_message_field(self._OnlyIsRepeated(real)))


if __name__ == "__main__":
    unittest.main()
