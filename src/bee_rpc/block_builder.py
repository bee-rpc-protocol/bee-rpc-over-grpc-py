import json
import os.path
import warnings
from hashlib import sha3_256
from io import BufferedReader
from itertools import zip_longest
from typing import Any, List, Dict, Union, Tuple
from bee_rpc import buffer_pb2
from google.protobuf.message import Message, DecodeError
from google.protobuf.descriptor import FieldDescriptor

from bee_rpc.client import generate_random_dir, block_exists, move_to_block_dir, copy_to_block_dir, \
    get_hash_from_block
from bee_rpc.utils import Enviroment, CHUNK_SIZE, METADATA_FILE_NAME, WITHOUT_BLOCK_POINTERS_FILE_NAME, \
    get_file_hash, create_lengths_tree, encode_bytes


def is_block(bytes_obj: bytes, blocks: List[bytes]) -> bool:
    try:
        block = buffer_pb2.Buffer.Block()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            block.ParseFromString(bytes_obj)
        return get_hash_from_block(block, internal_block=False, hexadecimal=False) in blocks
    except DecodeError:
        pass
    return False


def assert_container_covers_blocks(
        container: Dict[str, List[List[int]]],
        blocks: List[bytes],
) -> None:
    """Fail fast on an inconsistent block/pointer state.

    Every hash in ``blocks`` was created by ``create_block`` and embedded in the
    message as a block-pointer (``branch.file = block.SerializeToString()``), so
    ``search_on_message`` MUST have detected each one and recorded it in
    ``container``. If it did not, the message tree was traversed incompletely —
    historically because block detection descended into repeated message fields
    with a backend-specific ``isinstance(value, google._upb._message.
    RepeatedCompositeContainer)`` check that is only true under the C/upb backend
    and silently False under the pure-python backend the packer forces. The
    traversal then skipped the repeated ``Filesystem.branch`` field, ``container``
    came back ``{}`` while ``blocks`` held N hashes, and ``build_multiblock``
    produced a metadata file whose block markers did not match the real blocks.
    That inconsistency only surfaced far downstream as the cryptic
    ``TypeError: object supporting the buffer API required`` when a
    ``Buffer.Block`` marker object leaked into a stream hash instead of bytes.

    Raise a clear, actionable error here instead. Detection is now
    descriptor-based and backend-agnostic, so this guard should never trip; if it
    does, it points squarely at a traversal/detection regression rather than
    leaving a corrupt artifact to explode later.
    """
    detected = set(container.keys())
    expected = {block.hex() for block in blocks}
    missing = expected - detected
    if missing:
        preview = ', '.join(sorted(missing)[:4])
        if len(missing) > 4:
            preview += ', … (%d total)' % len(missing)
        raise Exception(
            'gRPCbb block_builder: inconsistent block state — %d block(s) were '
            'created and embedded as pointers but search_on_message detected only '
            '%d of them in the message tree. Missing: %s. This means block-pointer '
            'traversal did not reach a repeated/nested message field (a protobuf '
            'backend or detection mismatch). Refusing to build an inconsistent '
            'multiblock artifact that would later crash with "object supporting the '
            'buffer API required" during stream hashing.'
            % (len(expected), len(expected) - len(missing), preview)
        )


def get_position_length(varint_pos: int, buffer: bytes) -> int:
    """
    Returns the value of the varint at the given position in the Protobuf buffer.
    """
    value = 0
    shift = 0
    index = varint_pos
    while True:
        byte = buffer[index]
        value |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
        index += 1
    return value


def get_hash(block: buffer_pb2.Buffer.Block) -> str:
    for _hash in block.hashes:
        if _hash.type == Enviroment.hash_type:
            return _hash.value.hex()
    raise Exception('gRPCbb: any hash of type ' + Enviroment.hash_type.hex())


def get_block_length(block_id: str) -> int:
    if os.path.isfile(Enviroment.block_dir + block_id):
        return os.path.getsize(Enviroment.block_dir + block_id)
    elif os.path.isdir(Enviroment.block_dir + block_id):
        raise Exception('gRPCbb: error on compute_real_lengths, multiblock blocks dont supported.'
                        + Enviroment.block_dir + block_id)
    else:
        raise Exception('gRPCbb: error on compute_real_lengths, block does not in block registry. '
                        + Enviroment.block_dir + block_id)


def search_on_message_real(
        message: Message,
        pointers: List[int],
        initial_position: int,
        real_initial_position: int,
        blocks: List[bytes],
        container: List[Tuple[str, List[int]]],
        real_lengths: Dict[int, Tuple[int, int, bool]],
):
    position: int = initial_position
    real_position: int = real_initial_position
    for field, value in message.ListFields():
        if field.label == FieldDescriptor.LABEL_REPEATED and field.type == FieldDescriptor.TYPE_MESSAGE \
                and not field.message_type.GetOptions().map_entry:
            for element in value:
                position += 1
                if position not in real_lengths.keys():
                    position += len(encode_bytes(element.ByteSize())) + element.ByteSize()
                    real_position += 1 + len(encode_bytes(element.ByteSize())) + element.ByteSize()
                    continue
                try:
                    message_size = real_lengths[position][0]
                except KeyError:
                    raise Exception(
                        'gRPCbb block builder error, real lengths not in ' + str(position) + '. ' + str(real_lengths))
                search_on_message_real(
                    message=element,
                    pointers=pointers + [real_position + 1],
                    initial_position=position + len(encode_bytes(element.ByteSize())),
                    real_initial_position=real_position + 1 + len(encode_bytes(message_size)),
                    blocks=blocks,
                    container=container,
                    real_lengths=real_lengths
                )
                position += len(encode_bytes(element.ByteSize())) + element.ByteSize()
                real_position += 1 + len(encode_bytes(message_size)) + message_size

        elif isinstance(value, Message):
            position += 1
            if position not in real_lengths.keys():
                position += len(encode_bytes(value.ByteSize())) + value.ByteSize()
                real_position += 1 + len(encode_bytes(value.ByteSize())) + value.ByteSize()
                continue
            try:
                message_size = real_lengths[position][0]
            except KeyError:
                raise Exception(
                    'gRPCbb block builder error, real lengths not in ' + str(position) + '. ' + str(real_lengths))
            search_on_message_real(
                message=value,
                pointers=pointers + [real_position + 1],
                initial_position=position + len(encode_bytes(value.ByteSize())),
                real_initial_position=real_position + 1 + len(encode_bytes(message_size)),
                blocks=blocks,
                container=container,
                real_lengths=real_lengths
            )
            position += len(encode_bytes(value.ByteSize())) + value.ByteSize()
            real_position += 1 + len(encode_bytes(message_size)) + message_size

        elif type(value) == bytes and is_block(value, blocks):
            block = buffer_pb2.Buffer.Block()
            block.ParseFromString(value)
            container.append((get_hash(block), pointers + [real_position + 1]))
            block_length: int = get_block_length(get_hash(block))
            position += 1
            try:
                if (real_lengths[position][0] != block_length):
                    raise Exception('Error on gRPCbb: block_builder method computing real message positions. ',
                                    position, real_lengths[position], block_length)
            except KeyError:
                raise Exception(
                    'gRPCbb block builder error, real lengths not in ' + str(position) + '. ' + str(real_lengths))
            position += len(encode_bytes(block.ByteSize())) + block.ByteSize()
            real_position += 1 + len(encode_bytes(block_length)) + block_length

        elif type(value) == bytes or type(value) == str:
            position += 1 + len(encode_bytes(len(value))) + len(value)
            real_position += 1 + len(encode_bytes(len(value))) + len(value)

        else:
            try:
                temp_message = type(message)()
                temp_message.CopyFrom(message)
                for field_name, _ in temp_message.ListFields():
                    if field_name.index != field.index:
                        temp_message.ClearField(field_name.name)
                position += temp_message.ByteSize()
                real_position += temp_message.ByteSize()
            except Exception as e:
                raise Exception('gRPCbb block builder error obtaining the length of a primitive value :' + str(e))


def search_on_message(
        message: Message,
        pointers: List[int],
        initial_position: int,
        blocks: List[bytes],
        container: Dict[str, List[List[int]]]
):
    """
       Search_on_message makes a tree search of the protobuf object (attr. message) and stores all the buffer block
        identifier instances with its indexes (ascendant order) on the container dictionary.
        It allows to know where the buffer needs to be changed when the buffer block substitute the block identifier.
       """
    position: int = initial_position
    for field, value in message.ListFields():
        if field.label == FieldDescriptor.LABEL_REPEATED and field.type == FieldDescriptor.TYPE_MESSAGE \
                and not field.message_type.GetOptions().map_entry:
            for element in value:
                search_on_message(
                    message=element,
                    pointers=pointers + [position + 1],
                    initial_position=position + 1 + len(encode_bytes(element.ByteSize())),
                    blocks=blocks,
                    container=container
                )
                position += 1 + len(encode_bytes(element.ByteSize())) + element.ByteSize()

        elif isinstance(value, Message):
            search_on_message(
                message=value,
                pointers=pointers + [position + 1],
                initial_position=position + 1 + len(encode_bytes(value.ByteSize())),
                blocks=blocks,
                container=container
            )
            position += 1 + len(encode_bytes(value.ByteSize())) + value.ByteSize()

        elif type(value) == bytes and is_block(value, blocks):
            block = buffer_pb2.Buffer.Block()
            block.ParseFromString(value)

            _block_hash = get_hash(block)
            _list_of_pointers = pointers + [position + 1]
            if _block_hash not in container:
                container[_block_hash] = [_list_of_pointers]
            else:
                container[_block_hash].append(_list_of_pointers)

            position += 1 + len(encode_bytes(block.ByteSize())) + block.ByteSize()

        elif type(value) == bytes or type(value) == str:
            position += 1 + len(encode_bytes(len(value))) + len(value)

        else:
            try:
                temp_message = type(message)()
                temp_message.CopyFrom(message)
                for field_name, _ in temp_message.ListFields():
                    if field_name.index != field.index:
                        temp_message.ClearField(field_name.name)
                position += temp_message.ByteSize()
            except Exception as e:
                raise Exception('gRPCbb block builder error obtaining the length of a primitive value :' + str(e))


def compute_real_lengths(tree: Dict[int, Union[Dict, str]], buffer: bytes) -> Dict[int, Tuple[int, int, bool]]:
    """
    Given the pointer's tree with block id's as the leafs it will return a dict of pointers with its
    real length, compressed format (of the input buffer) length, and a boolean saying if it's the pointer
    of a block id message or not (tree's leaf or not).

    :param tree:Tree of pointers as nodes and block id's as leafs.
    :type tree: Dict[int, Union[Dict, str]]
    :param buffer:Buffer of the compressed object.
    :type buffer: bytes
    :return: A dict of pointers with its real and compressed lengths and if it's leaf or not.
    :rtype: Dict[int, Tuple[int, int, bool]]
    """
    def traverse_tree(internal_tree: Dict, internal_buffer: bytes, initial_total_length: int) \
            -> Tuple[int, Dict[int, Tuple[int, int, bool]]]:

        real_lengths: Dict[int, Tuple[int, int, bool]] = {}
        total_tree_length: int = 0
        total_block_length: int = 0
        for key, value in internal_tree.items():
            if isinstance(value, dict):
                initial_length: int = get_position_length(key, internal_buffer)
                real_length, internal_lengths = traverse_tree(
                    value, internal_buffer, initial_length
                )
                real_lengths[key] = (real_length, initial_length, False)
                real_lengths.update(internal_lengths)
                total_tree_length += real_length + len(encode_bytes(real_length)) + 1

                block_length: int = initial_length + len(encode_bytes(initial_length)) + 1
                total_block_length += block_length

            else:
                b = buffer_pb2.Buffer.Block()
                h = buffer_pb2.Buffer.Block.Hash()
                h.type = Enviroment.hash_type
                h.value = bytes.fromhex(value)
                b.hashes.append(h)
                b_length: int = len(b.SerializeToString())

                real_length: int = get_block_length(value)
                real_lengths[key] = (real_length, b_length, True)
                total_tree_length += real_length + len(encode_bytes(real_length)) + 1

                block_length: int = b_length + len(encode_bytes(b_length)) + 1
                total_block_length += block_length

        if initial_total_length < total_block_length:
            raise Exception('Error on compute real lengths, block length cant be greater than the total length',
                            initial_total_length, total_block_length)

        total_tree_length += initial_total_length - total_block_length

        return total_tree_length, real_lengths

    #  For the case when are duplicate blocks, the lengths tree needs to be sorted.
    return dict(sorted(traverse_tree(tree, buffer, len(buffer))[1].items()))


def generate_buffer(buffer: bytes, lengths: Dict[int, Tuple[int, int, bool]]) -> List[bytes]:
    """
    Iterates over the buffer, replacing the compressed buffer with the real buffer,
    replacing the compressed lengths of each pointer with its real length.
    It is returned in list format since it is not necessary to return the entire buffer,
    the content of the blocks does not need to be loaded into memory.

    :param buffer: Compressed buffer
    :type buffer: bytes
    :param lengths:  A dict of pointers with its real and compressed lengths and if it's leaf or not.
    :type lengths: Dict[int, Tuple[int, int, bool]]
    :return: The inter-block buffers list with the real lengths.
    :rtype: List[bytes]
    """
    list_of_bytes: List[bytes] = []
    new_buff: bytes = b''
    i: int = 0
    for key, value in lengths.items():
        new_buff += buffer[i:key] + encode_bytes(value[0])
        i = key + len(encode_bytes(value[1]))
        if value[2]:
            i += value[1]
            list_of_bytes.append(new_buff)
            new_buff = b''

    return list_of_bytes + [buffer[i:]]


def generate_id(buffers: List[bytes], blocks: List[bytes]) -> bytes:
    hash_id = sha3_256()
    for buffer, block in zip_longest(buffers, blocks):
        if buffer:
            hash_id.update(buffer)
        if block:
            with BufferedReader(open(Enviroment.block_dir + block.hex(), 'rb')) as f:
                while True:
                    f.flush()
                    piece: bytes = f.read(CHUNK_SIZE)
                    if len(piece) == 0:
                        break
                    hash_id.update(piece)
    return hash_id.digest()


def purify_buffer(buff: bytes) -> bytes:
    return buff


def build_multiblock(
        pf_object_with_block_pointers: Any,
        blocks: List[bytes]
) -> Tuple[bytes, str]:
    container: Dict[str, List[List[int]]] = {}
    search_on_message(
        message=pf_object_with_block_pointers,
        pointers=[],
        initial_position=0,
        blocks=blocks,
        container=container
    )

    assert_container_covers_blocks(container=container, blocks=blocks)

    tree: Dict[int, Union[Dict, str]] = create_lengths_tree(
        pointer_container=container
    )

    # no importa para duplicidad
    real_lengths: Dict[int, Tuple[int, int, bool]] = compute_real_lengths(
        tree=tree,
        buffer=pf_object_with_block_pointers.SerializeToString()
    )

    # no importa para duplicidad
    new_buff: List[bytes] = generate_buffer(
        buffer=pf_object_with_block_pointers.SerializeToString(),
        lengths=real_lengths
    )

    object_id: bytes = generate_id(
        buffers=new_buff,
        blocks=blocks
    )
    cache_dir: str = generate_random_dir() + '/'
    _json: List[Union[
        int,
        Tuple[str, List[int]]
    ]] = []

    container_real_lengths: List[Tuple[str, List[int]]] = []
    search_on_message_real(
        message=pf_object_with_block_pointers,
        pointers=[],
        initial_position=0,
        real_initial_position=0,
        blocks=blocks,
        container=container_real_lengths,
        real_lengths=real_lengths
    )

    for i, (b1, b2) in enumerate(zip_longest(new_buff, container_real_lengths)):
        _json.append(i + 1)
        with open(cache_dir + str(i + 1), 'wb') as f:
            f.write(b1)

        if b2:
            _json.append((b2[0], b2[1]))

    with open(cache_dir + METADATA_FILE_NAME, 'w') as f:
        json.dump(_json, f)

    with open(cache_dir + WITHOUT_BLOCK_POINTERS_FILE_NAME, 'wb') as f:
        f.write(pf_object_with_block_pointers.SerializeToString())

    return object_id, cache_dir


def get_recursive_block_length(block_id: str, cache: Dict[str, int]) -> int:
    if block_id in cache:
        if cache[block_id] == -1:
            raise Exception(f'Detected recursive loop when processing block {block_id}')
        return cache[block_id]

    cache[block_id] = -1

    block_path = Enviroment.block_dir + block_id
    if not os.path.exists(block_path):
        raise Exception(f'gRPCbb: Block not found: {block_path}')

    try:
        with open(block_path, 'rb') as f:
            content = f.read()

        inner_object = buffer_pb2.Buffer()
        inner_object.ParseFromString(content)

        container = {}
        search_on_message(inner_object, [], 0, [], container)
        tree = create_lengths_tree(container)

        inner_lengths = compute_real_lengths_recursive(tree, content, cache)
        total_real_length = sum(
            rl[0] + len(encode_bytes(rl[0])) + 1 for rl in inner_lengths.values()
        )

    except DecodeError:
        total_real_length = os.path.getsize(block_path)

    cache[block_id] = total_real_length
    return total_real_length


def compute_real_lengths_recursive(tree: Dict[int, Union[Dict, str]], buffer: bytes, cache: Dict[str, int]) -> Dict[int, Tuple[int, int, bool]]:
    def traverse_tree(internal_tree: Dict, internal_buffer: bytes, initial_total_length: int) -> Tuple[int, Dict[int, Tuple[int, int, bool]]]:
        real_lengths: Dict[int, Tuple[int, int, bool]] = {}
        total_tree_length: int = 0
        total_block_length: int = 0

        for key, value in internal_tree.items():
            if isinstance(value, dict):
                initial_length = get_position_length(key, internal_buffer)
                subtree_length, sub_lengths = traverse_tree(value, internal_buffer, initial_length)
                real_lengths[key] = (subtree_length, initial_length, False)
                real_lengths.update(sub_lengths)
                total_tree_length += subtree_length + len(encode_bytes(subtree_length)) + 1
                total_block_length += initial_length + len(encode_bytes(initial_length)) + 1
            else:
                b = buffer_pb2.Buffer.Block()
                h = buffer_pb2.Buffer.Block.Hash()
                h.type = Enviroment.hash_type
                h.value = bytes.fromhex(value)
                b.hashes.append(h)
                b_length = len(b.SerializeToString())

                real_length = get_recursive_block_length(value, cache)
                real_lengths[key] = (real_length, b_length, True)
                total_tree_length += real_length + len(encode_bytes(real_length)) + 1
                total_block_length += b_length + len(encode_bytes(b_length)) + 1

        if initial_total_length < total_block_length:
            raise Exception('Error on compute real lengths: block length cannot be greater than total length',
                            initial_total_length, total_block_length)

        total_tree_length += initial_total_length - total_block_length
        return total_tree_length, real_lengths

    return dict(sorted(traverse_tree(tree, buffer, len(buffer))[1].items()))


def build_multiblock_fractal(
        pf_object_with_block_pointers: Any,
        blocks: List[bytes]
) -> Tuple[bytes, str]:
    container: Dict[str, List[List[int]]] = {}
    search_on_message(
        message=pf_object_with_block_pointers,
        pointers=[],
        initial_position=0,
        blocks=blocks,
        container=container
    )

    assert_container_covers_blocks(container=container, blocks=blocks)

    tree: Dict[int, Union[Dict, str]] = create_lengths_tree(
        pointer_container=container
    )

    real_lengths: Dict[int, Tuple[int, int, bool]] = compute_real_lengths_recursive(
        tree=tree,
        buffer=pf_object_with_block_pointers.SerializeToString(),
        cache={}
    )

    new_buff: List[bytes] = generate_buffer(
        buffer=pf_object_with_block_pointers.SerializeToString(),
        lengths=real_lengths
    )

    object_id: bytes = generate_id(
        buffers=new_buff,
        blocks=blocks
    )

    cache_dir: str = generate_random_dir() + '/'
    _json: List[Union[int, Tuple[str, List[int]]]] = []

    container_real_lengths: List[Tuple[str, List[int]]] = []
    search_on_message_real(
        message=pf_object_with_block_pointers,
        pointers=[],
        initial_position=0,
        real_initial_position=0,
        blocks=blocks,
        container=container_real_lengths,
        real_lengths=real_lengths
    )

    for i, (b1, b2) in enumerate(zip_longest(new_buff, container_real_lengths)):
        _json.append(i + 1)
        with open(cache_dir + str(i + 1), 'wb') as f:
            f.write(b1)

        if b2:
            _json.append((b2[0], b2[1]))

    with open(cache_dir + METADATA_FILE_NAME, 'w') as f:
        json.dump(_json, f)

    with open(cache_dir + WITHOUT_BLOCK_POINTERS_FILE_NAME, 'wb') as f:
        f.write(pf_object_with_block_pointers.SerializeToString())

    return object_id, cache_dir


def create_block(file_path: str, copy: bool = False) -> Tuple[bytes, buffer_pb2.Buffer.Block]:
    file_hash: str = get_file_hash(file_path=file_path)
    if not block_exists(block_id=file_hash):
        if copy and not copy_to_block_dir(
                file_hash=file_hash,
                file_path=file_path
        ) or \
                not copy and not move_to_block_dir(
            file_hash=file_hash,
            file_path=file_path
        ):
            raise Exception('gRPCbb error creating block, file could not be moved.')

    file_hash: bytes = bytes.fromhex(file_hash)

    block = buffer_pb2.Buffer.Block()
    h = buffer_pb2.Buffer.Block.Hash()
    h.type = Enviroment.hash_type
    h.value = file_hash
    block.hashes.append(h)

    return file_hash, block
