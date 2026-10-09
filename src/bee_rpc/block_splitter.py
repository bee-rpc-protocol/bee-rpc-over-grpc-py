"""Give a stored block sub-blocks of its own when its direct content is too large.

A block arrives in the form its sender stored it in. A sender that inlined large
fields -- an old one, or one with another threshold -- leaves the receiver with a
block whose *direct* content (the bytes outside its sub-blocks: the whole file
for a flat block, the parts for a directory) passes what a protobuf message can be
parsed from (2 GiB). Nothing that reads it as a message can then use it.

`split_block` rewrites such a block as a multiblock directory, moving the fields
the caller's policy names into blocks of their own. It reads the block's
expansion once, as a stream: the message is too large to parse, so its wire
format is walked with the message type's descriptor instead. The expansion is
unchanged, so the block id is too; it is checked against the bytes read.

Only the caller knows what type a block holds and which of its fields may be a
block (a container filesystem's file contents, say, and not the sub-messages a
reader expects inline), so both are arguments.
"""
import json
import os
import shutil
import typing
from random import randint

from google.protobuf.descriptor import Descriptor, FieldDescriptor

from bee_rpc.block_driver import generate_wbp_file
from bee_rpc.reader import read_block, block_exists
from bee_rpc.utils import Enviroment, MAX_DIR, METADATA_FILE_NAME, CHUNK_SIZE, \
    BlockIdMismatch

# The largest message protobuf parses: its sizes are signed 32-bit.
PROTOBUF_LIMIT = 2 ** 31 - 1

ShouldSplit = typing.Callable[[FieldDescriptor, int], bool]


class SplitError(Exception):
    """The block cannot be given sub-blocks under the policy it was asked for."""


def direct_content_size(block_id: str) -> int:
    """The bytes of a block that are not inside one of its sub-blocks."""
    path = Enviroment.block_dir + block_id
    if not os.path.isdir(path):
        return os.path.getsize(path)
    with open(os.path.join(path, METADATA_FILE_NAME)) as f:
        return sum(os.path.getsize(os.path.join(path, str(e)))
                   for e in json.load(f) if isinstance(e, int))


class _Reader:
    """A byte stream over a chunk generator that knows its position."""

    def __init__(self, chunks: typing.Iterator[bytes], hasher):
        self._chunks = chunks
        self._hasher = hasher
        self._buffer = b''
        self._offset = 0
        self.position = 0

    def _fill(self) -> bool:
        for chunk in self._chunks:
            if not isinstance(chunk, bytes) or not chunk:
                continue
            self._hasher.update(chunk)
            self._buffer = self._buffer[self._offset:] + chunk
            self._offset = 0
            return True
        return False

    def at_end(self) -> bool:
        return self._offset >= len(self._buffer) and not self._fill()

    def read(self, n: int) -> bytes:
        out = b''
        while len(out) < n:
            if self._offset >= len(self._buffer) and not self._fill():
                raise SplitError('bee-rpc: the block ends inside a field.')
            take = self._buffer[self._offset:self._offset + n - len(out)]
            self._offset += len(take)
            out += take
        self.position += n
        return out

    def copy(self, n: int, write: typing.Callable[[bytes], None]):
        while n:
            if self._offset >= len(self._buffer) and not self._fill():
                raise SplitError('bee-rpc: the block ends inside a field.')
            take = self._buffer[self._offset:self._offset + min(n, CHUNK_SIZE)]
            self._offset += len(take)
            self.position += len(take)
            n -= len(take)
            write(take)

    def varint(self) -> typing.Tuple[bytes, int]:
        raw, value, shift = b'', 0, 0
        while True:
            byte = self.read(1)
            raw += byte
            value |= (byte[0] & 0x7F) << shift
            if not byte[0] & 0x80:
                return raw, value
            shift += 7
            if shift > 63:
                raise SplitError('bee-rpc: a varint longer than 10 bytes.')


class _Writer:
    """The parts and the `_.json` of the directory being written."""

    def __init__(self, directory: str):
        self.directory = directory
        self.entries: typing.List[typing.Union[int, list]] = []
        self.blocks = 0
        self._open_part()

    def _open_part(self):
        index = sum(1 for e in self.entries if isinstance(e, int)) + 1
        self.entries.append(index)
        self._part = open(os.path.join(self.directory, str(index)), 'wb')

    def write(self, data: bytes):
        self._part.write(data)

    def block(self, block_id: str, positions: typing.List[int]):
        self._part.close()
        self.entries.append([block_id, positions])
        self.blocks += 1
        self._open_part()

    def close(self):
        self._part.close()
        with open(os.path.join(self.directory, METADATA_FILE_NAME), 'w') as f:
            json.dump(self.entries, f)


def _store_payload(reader: _Reader, length: int) -> str:
    """Copy `length` bytes into a block of their own and return its id."""
    tmp = Enviroment.block_dir + '.split-' + str(randint(0, MAX_DIR))
    hasher = Enviroment.hash_factory()
    try:
        with open(tmp, 'wb') as f:
            def write(data: bytes):
                hasher.update(data)
                f.write(data)
            reader.copy(length, write)
        block_id = hasher.hexdigest()
        if block_exists(block_id):
            os.remove(tmp)
        else:
            os.replace(tmp, Enviroment.block_dir + block_id)
        return block_id
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def _walk(reader: _Reader, writer: _Writer, descriptor: typing.Optional[Descriptor],
          end: typing.Optional[int], chain: typing.List[int], should_split: ShouldSplit):
    while (reader.position < end) if end is not None else not reader.at_end():
        raw_tag, tag = reader.varint()
        writer.write(raw_tag)
        number, wire_type = tag >> 3, tag & 7
        if wire_type == 0:
            writer.write(reader.varint()[0])
        elif wire_type == 1:
            writer.write(reader.read(8))
        elif wire_type == 5:
            writer.write(reader.read(4))
        elif wire_type == 2:
            length_position = reader.position
            raw_length, length = reader.varint()
            writer.write(raw_length)
            field = descriptor.fields_by_number.get(number) if descriptor else None
            if field is not None and field.type == FieldDescriptor.TYPE_MESSAGE:
                _walk(reader, writer, field.message_type, reader.position + length,
                      chain + [length_position], should_split)
            elif field is not None and should_split(field, length):
                writer.block(_store_payload(reader, length), chain + [length_position])
            else:
                reader.copy(length, writer.write)
        else:
            raise SplitError('bee-rpc: wire type %d (groups) is not supported.' % wire_type)
    if end is not None and reader.position != end:
        raise SplitError('bee-rpc: a field runs past the end of its message.')


def split_block(
        block_id: str,
        message_type,
        should_split: ShouldSplit,
        inherited: typing.Optional[typing.Sequence[bytes]] = None,
        limit: int = PROTOBUF_LIMIT,
        debug: typing.Callable[[str], None] = lambda s: None,
) -> bool:
    """Rewrite `block_id` with sub-blocks if its direct content passes `limit`.

    `message_type` is what the block's content is a serialization of, and
    `should_split(field, length)` says which of its bytes or string fields become
    blocks. `inherited` is the hash-type context the block sits in, as for
    `generate_wbp_file`: pass the types of the pointer that names the block for the
    compressed pointer form a nested block stores.

    False when the block was within `limit` and was left as it was. Raises
    SplitError when the policy does not bring it within `limit`, and
    BlockIdMismatch when the stored block does not hash to its id; the block is
    left as it was in both cases.
    """
    size = direct_content_size(block_id)
    if size <= limit:
        return False
    debug(f"Block {block_id} holds {size} bytes outside its sub-blocks; splitting it.")

    final = Enviroment.block_dir + block_id
    tmp = final + '.tmp-' + str(randint(0, MAX_DIR))
    os.makedirs(tmp)
    try:
        hasher = Enviroment.hash_factory()
        reader = _Reader(read_block(block_id=block_id, ignore_blocks=True), hasher)
        writer = _Writer(tmp)
        try:
            _walk(reader, writer, message_type.DESCRIPTOR, None, [], should_split)
        finally:
            writer.close()
        if hasher.hexdigest() != block_id:
            raise BlockIdMismatch('bee-rpc: the stored block %s hashes to %s.'
                                  % (block_id, hasher.hexdigest()))

        remaining = sum(os.path.getsize(os.path.join(tmp, str(e)))
                        for e in writer.entries if isinstance(e, int))
        if remaining > limit:
            raise SplitError(
                'bee-rpc: block %s keeps %d bytes outside its sub-blocks after the split '
                '(%d made), over the limit of %d.' % (block_id, remaining, writer.blocks, limit))

        generate_wbp_file(tmp, inherited=inherited, debug=debug)

        # A directory cannot be renamed over a file, nor over a directory that is
        # not empty, so the old form steps aside first.
        old = final + '.old-' + str(randint(0, MAX_DIR))
        os.replace(final, old)
        os.replace(tmp, final)
        if os.path.isdir(old):
            shutil.rmtree(old)
        else:
            os.remove(old)
        debug(f"Block {block_id} split: {writer.blocks} sub-blocks, {remaining} bytes direct.")
        return True
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
