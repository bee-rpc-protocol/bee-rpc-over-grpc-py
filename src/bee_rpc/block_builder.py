import json
import os.path
import warnings
from io import BufferedReader
from itertools import zip_longest
from typing import Any, List, Dict, Optional, Sequence, Union, Tuple
from bee_rpc import buffer_pb2
from google.protobuf.message import Message, DecodeError
from google.protobuf.descriptor import FieldDescriptor

from bee_rpc.client import generate_random_dir, block_exists, move_to_block_dir, copy_to_block_dir
from bee_rpc.reader import read_multiblock_directory
from bee_rpc.utils import Enviroment, CHUNK_SIZE, METADATA_FILE_NAME, WITHOUT_BLOCK_POINTERS_FILE_NAME, \
    get_file_hash, create_lengths_tree, encode_bytes, get_expanded_block_length, \
    block_id_from_pointer, block_pointer


def is_block(bytes_obj: bytes, blocks: List[bytes], inherited: Optional[Sequence[bytes]] = None) -> bool:
    """Whether this field value is a pointer to one of `blocks`.

    `inherited` is the hash-type context the pointer sits in -- None at the top of
    an object, where a pointer has to carry its own types. Probing content that was
    never a pointer is the ordinary case, so an undeducible type is answered with
    False here rather than raised.
    """
    try:
        block = buffer_pb2.Buffer.Block()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            block.ParseFromString(bytes_obj)
        return block_id_from_pointer(block, inherited=inherited, hexadecimal=False) in blocks
    except DecodeError:
        pass
    return False


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


def get_hash(block: buffer_pb2.Buffer.Block, inherited: Optional[Sequence[bytes]] = None) -> str:
    """The registry id this pointer names. Unlike is_block, a failure here is an error:
    the caller has already decided these bytes are a pointer."""
    block_id = block_id_from_pointer(block, inherited=inherited, hexadecimal=True)
    if block_id is None:
        raise Exception(
            'gRPCbb: this pointer names no hash of type ' + Enviroment.hash_type.hex()
            + ' -- either it carries none, or the type of one it does carry could not '
            'be deduced from its ancestors.')
    return block_id


def get_block_length(block_id: str) -> int:
    """The length a pointer to this block expands to, whichever shape it is stored in.

    A referenced block used to have to be a single file: a *multiblock directory*
    block was refused outright, which is what kept an object from referencing one
    (and so kept a large sub-object -- a whole container filesystem, say -- from
    being stored as one block of its own instead of inlined). Both shapes are
    measurable; the directory one is the sum of its own expansion.
    """
    if os.path.isfile(Enviroment.block_dir + block_id) or os.path.isdir(Enviroment.block_dir + block_id):
        return get_expanded_block_length(block_name=block_id)
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
        inherited: Optional[Sequence[bytes]] = None,
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
                    real_lengths=real_lengths,
                    inherited=inherited
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
                real_lengths=real_lengths,
                inherited=inherited
            )
            position += len(encode_bytes(value.ByteSize())) + value.ByteSize()
            real_position += 1 + len(encode_bytes(message_size)) + message_size

        elif type(value) == bytes and is_block(value, blocks, inherited=inherited):
            block = buffer_pb2.Buffer.Block()
            block.ParseFromString(value)
            block_id: str = get_hash(block, inherited=inherited)
            container.append((block_id, pointers + [real_position + 1]))
            block_length: int = get_block_length(block_id)
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
        container: Dict[str, List[List[int]]],
        pointer_lengths: Optional[Dict[int, int]] = None,
        inherited: Optional[Sequence[bytes]] = None
):
    """
       Search_on_message makes a tree search of the protobuf object (attr. message) and stores all the buffer block
        identifier instances with its indexes (ascendant order) on the container dictionary.
        It allows to know where the buffer needs to be changed when the buffer block substitute the block identifier.

       `pointer_lengths` collects, per pointer position, how many bytes that pointer
       *actually occupies in this buffer*. Every consumer downstream needs it, and
       rebuilding a pointer from its id to find out is not the same question: the
       same block is a 70-byte pointer where its hash types are written out and a
       36-byte one where they are inherited, so a rebuilt length is right only by
       coincidence and skips the wrong number of bytes when it is not.

       `inherited` is the hash-type context this object sits in; it does not change
       as the walk descends through the message tree, only when descending into a
       block, which is a different traversal.
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
                    container=container,
                    pointer_lengths=pointer_lengths,
                    inherited=inherited
                )
                position += 1 + len(encode_bytes(element.ByteSize())) + element.ByteSize()

        elif isinstance(value, Message):
            search_on_message(
                message=value,
                pointers=pointers + [position + 1],
                initial_position=position + 1 + len(encode_bytes(value.ByteSize())),
                blocks=blocks,
                container=container,
                pointer_lengths=pointer_lengths,
                inherited=inherited
            )
            position += 1 + len(encode_bytes(value.ByteSize())) + value.ByteSize()

        elif type(value) == bytes and is_block(value, blocks, inherited=inherited):
            block = buffer_pb2.Buffer.Block()
            block.ParseFromString(value)

            _block_hash = get_hash(block, inherited=inherited)
            _list_of_pointers = pointers + [position + 1]
            if _block_hash not in container:
                container[_block_hash] = [_list_of_pointers]
            else:
                container[_block_hash].append(_list_of_pointers)

            if pointer_lengths is not None:
                pointer_lengths[position + 1] = len(value)

            position += 1 + len(encode_bytes(len(value))) + len(value)

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


def compute_real_lengths(
        tree: Dict[int, Union[Dict, str]],
        buffer: bytes,
        pointer_lengths: Dict[int, int]
) -> Dict[int, Tuple[int, int, bool]]:
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
                # The pointer's length as measured in this very buffer. Rebuilding it
                # from the id would answer a different question -- what a pointer to
                # this block would look like if this node wrote it now -- and skip the
                # wrong number of bytes for any pointer written under another
                # encoding (hash types inherited rather than spelled out).
                try:
                    b_length: int = pointer_lengths[key]
                except KeyError:
                    raise Exception(
                        'gRPCbb block builder error, no measured pointer length at ' + str(key))

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


def generate_id(buffers: List[bytes], block_ids: List[str]) -> bytes:
    """The object's id: the hash of the stream it expands to.

    Parts and blocks alternate in the order the metadata file records them, so
    `block_ids` must arrive in that same order and with the multiplicity the
    object actually has -- which is what `search_on_message_real` collects.

    This used to hash the caller's `blocks` list instead. That list is
    deduplicated and ordered however the caller happened to gather it, so the id
    came out right only when it coincided with the order the blocks appear in
    the object and no block was referenced twice; otherwise it named content
    that does not exist. Nothing in the library reads the id back, so the
    mismatch surfaced only in a caller that content-addresses by it.
    """
    hash_id = Enviroment.hash_factory()
    for buffer, block_id in zip_longest(buffers, block_ids):
        if buffer:
            hash_id.update(buffer)
        if block_id:
            block_path: str = Enviroment.block_dir + block_id
            if os.path.isdir(block_path):
                # A multiblock directory block has no single file to read: what it
                # contributes to the id is the stream it expands to, the same bytes
                # a reader would see in its place.
                for piece in read_multiblock_directory(directory=block_path, ignore_blocks=True):
                    hash_id.update(piece)
            else:
                with BufferedReader(open(block_path, 'rb')) as f:
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
        blocks: List[bytes],
        inherited: Optional[Sequence[bytes]] = None
) -> Tuple[bytes, str]:
    """Build a multiblock directory from an object carrying block pointers.

    `inherited` is the hash-type context the object's pointers sit in. None -- the
    default -- is the top of a stored tree, where every pointer must spell out its
    own types. Pass the enclosing pointer's resolved types to build an object whose
    pointers omit theirs.
    """
    container: Dict[str, List[List[int]]] = {}
    pointer_lengths: Dict[int, int] = {}
    search_on_message(
        message=pf_object_with_block_pointers,
        pointers=[],
        initial_position=0,
        blocks=blocks,
        container=container,
        pointer_lengths=pointer_lengths,
        inherited=inherited
    )

    tree: Dict[int, Union[Dict, str]] = create_lengths_tree(
        pointer_container=container
    )

    # no importa para duplicidad
    real_lengths: Dict[int, Tuple[int, int, bool]] = compute_real_lengths(
        tree=tree,
        buffer=pf_object_with_block_pointers.SerializeToString(),
        pointer_lengths=pointer_lengths
    )

    # no importa para duplicidad
    new_buff: List[bytes] = generate_buffer(
        buffer=pf_object_with_block_pointers.SerializeToString(),
        lengths=real_lengths
    )

    container_real_lengths: List[Tuple[str, List[int]]] = []
    search_on_message_real(
        message=pf_object_with_block_pointers,
        pointers=[],
        initial_position=0,
        real_initial_position=0,
        blocks=blocks,
        container=container_real_lengths,
        real_lengths=real_lengths,
        inherited=inherited
    )

    # Hashed from the same two lists the metadata file below is written from, so
    # the id always describes the stream this object expands to.
    object_id: bytes = generate_id(
        buffers=new_buff,
        block_ids=[block_id for block_id, _ in container_real_lengths]
    )
    cache_dir: str = generate_random_dir() + '/'
    _json: List[Union[
        int,
        Tuple[str, List[int]]
    ]] = []

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

    if os.path.isdir(block_path):
        # A multiblock directory block states its own structure in its `_.json`;
        # there is nothing to open and re-parse, and open() would raise
        # IsADirectoryError past the DecodeError guard below.
        total_real_length = get_expanded_block_length(block_name=block_id)
        cache[block_id] = total_real_length
        return total_real_length

    try:
        with open(block_path, 'rb') as f:
            content = f.read()

        inner_object = buffer_pb2.Buffer()
        inner_object.ParseFromString(content)

        container = {}
        inner_pointer_lengths: Dict[int, int] = {}
        search_on_message(inner_object, [], 0, [], container,
                          pointer_lengths=inner_pointer_lengths)
        tree = create_lengths_tree(container)

        inner_lengths = compute_real_lengths_recursive(
            tree, content, cache, inner_pointer_lengths)
        total_real_length = sum(
            rl[0] + len(encode_bytes(rl[0])) + 1 for rl in inner_lengths.values()
        )

    except DecodeError:
        total_real_length = os.path.getsize(block_path)

    cache[block_id] = total_real_length
    return total_real_length


def compute_real_lengths_recursive(
        tree: Dict[int, Union[Dict, str]],
        buffer: bytes,
        cache: Dict[str, int],
        pointer_lengths: Dict[int, int]
) -> Dict[int, Tuple[int, int, bool]]:
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
                # Measured, not rebuilt -- see compute_real_lengths.
                try:
                    b_length = pointer_lengths[key]
                except KeyError:
                    raise Exception(
                        'gRPCbb block builder error, no measured pointer length at ' + str(key))

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
        blocks: List[bytes],
        inherited: Optional[Sequence[bytes]] = None
) -> Tuple[bytes, str]:
    container: Dict[str, List[List[int]]] = {}
    pointer_lengths: Dict[int, int] = {}
    search_on_message(
        message=pf_object_with_block_pointers,
        pointers=[],
        initial_position=0,
        blocks=blocks,
        container=container,
        pointer_lengths=pointer_lengths,
        inherited=inherited
    )

    tree: Dict[int, Union[Dict, str]] = create_lengths_tree(
        pointer_container=container
    )

    real_lengths: Dict[int, Tuple[int, int, bool]] = compute_real_lengths_recursive(
        tree=tree,
        buffer=pf_object_with_block_pointers.SerializeToString(),
        cache={},
        pointer_lengths=pointer_lengths
    )

    new_buff: List[bytes] = generate_buffer(
        buffer=pf_object_with_block_pointers.SerializeToString(),
        lengths=real_lengths
    )

    container_real_lengths: List[Tuple[str, List[int]]] = []
    search_on_message_real(
        message=pf_object_with_block_pointers,
        pointers=[],
        initial_position=0,
        real_initial_position=0,
        blocks=blocks,
        container=container_real_lengths,
        real_lengths=real_lengths,
        inherited=inherited
    )

    # Hashed from the same two lists the metadata file below is written from, so
    # the id always describes the stream this object expands to.
    object_id: bytes = generate_id(
        buffers=new_buff,
        block_ids=[block_id for block_id, _ in container_real_lengths]
    )

    cache_dir: str = generate_random_dir() + '/'
    _json: List[Union[int, Tuple[str, List[int]]]] = []

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

    return file_hash, block_pointer(block_id=file_hash)
