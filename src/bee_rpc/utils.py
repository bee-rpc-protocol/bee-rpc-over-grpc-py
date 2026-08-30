import hashlib
import json
import os
from shutil import rmtree
from threading import Condition

import typing

from bee_rpc import buffer_pb2

# GrpcBigBuffer.
CHUNK_SIZE = 1024 * 1024  # 1MB
MAX_DIR = 999999999
WITHOUT_BLOCK_POINTERS_FILE_NAME = 'wbp.bin'
METADATA_FILE_NAME = '_.json'

HashTypes = typing.Tuple[bytes, ...]


class EmptyBufferException(Exception):
    pass


class HashTypeError(Exception):
    """A hash type is unknown to this node, or none can be deduced for a hash's index."""


class LengthsValidationError(Exception):
    """A stored object's metadata does not describe the bytes it sits on."""


class Dir(object):
    def __init__(self, dir: str, _type: type):
        self.dir: str = dir
        self.type: type = _type


class MemManager(object):
    def __init__(self, len):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        pass


def get_file_hash(file_path: str) -> str:
    # Create a hash object, of whatever algorithm this node addresses blocks by.
    hash = Enviroment.hash_factory()
    # Open the file in binary mode
    with open(file_path, 'rb') as file:
        # Read the contents of the file in chunks
        chunk = file.read(1024)
        while chunk:
            # Update the hash with the chunk
            hash.update(chunk)
            # Read the next chunk
            chunk = file.read(1024)
        # Calculate the final hash
        file_hash: str = hash.hexdigest()
        # Return the hash
        return file_hash


class Signal():
    # The parser use change() when reads a signal on the buffer.
    # The serializer use wait() for stop to send the buffer if it've to do it.
    # It's thread safe because the open var is only used by one thread (the parser) with the change method.
    def __init__(self, exist: bool = True) -> None:
        self.exist = exist
        if exist: self.open = True
        if exist: self.condition = Condition()

    def change(self):
        if self.exist:
            if self.open:
                self.open = False  # Stop the input buffer.
            else:
                with self.condition:
                    self.condition.notify_all()
                self.open = True  # Continue the input buffer.

    def wait(self):
        if self.exist and not self.open:
            with self.condition:
                self.condition.wait()


## Hash algorithms ##

# A hash type *is* the algorithm applied to the empty input, so an algorithm and the
# identifier written into pointers for it are never two independent settings to keep
# in step. `Enviroment.hash_type` used to be a bare hex literal beside four separate
# hardcoded `hashlib.sha3_256()` calls, which made the type a label: changing it
# renamed what the node claimed to be hashing with, and changed nothing about what it
# actually computed.

HashFactory = typing.Callable[[], typing.Any]

_HASH_ALGORITHMS: typing.Dict[bytes, HashFactory] = {}


def hash_type_of(factory: HashFactory) -> bytes:
    """The identifier for a hash algorithm: that algorithm over the empty input."""
    return factory().digest()


def register_hash_algorithm(factory: HashFactory) -> HashFactory:
    """Make an algorithm resolvable by its type, so artefacts labelled with it can be read.

    A caller with an algorithm this library does not ship -- or a parameterised one
    like blake2b at a digest size of its own -- registers the factory it wants
    ( `functools.partial(hashlib.blake2b, digest_size=32)`, say ) and its type
    follows from it.
    """
    _HASH_ALGORITHMS[hash_type_of(factory)] = factory
    return factory


def hasher_for(hash_type: bytes) -> HashFactory:
    """The algorithm that produced `hash_type`, or an error.

    Never a fallback to whatever this node is configured with: hashing with one
    algorithm under another's name is the failure this registry exists to prevent.
    """
    try:
        return _HASH_ALGORITHMS[hash_type]
    except KeyError:
        raise HashTypeError(
            'bee-rpc: unknown hash type ' + hash_type.hex() + '. Register the algorithm '
            'that produces it with register_hash_algorithm() before reading content '
            'addressed by it.'
        )


for _factory in (
        hashlib.sha3_256,
        hashlib.sha3_512,
        hashlib.sha256,
        hashlib.sha512,
        lambda: hashlib.blake2b(digest_size=32),
        hashlib.blake2b,
):
    register_hash_algorithm(_factory)
del _factory


## Enviroment ##

class Enviroment(type):
    # Using singleton pattern
    _instances = {}
    cache_dir = os.path.abspath(os.curdir) + '/__cache__/'
    block_dir = os.path.abspath(os.curdir) + '/__block__/'
    block_depth = 1
    skip_wbp_generation = False
    mem_manager = lambda len: MemManager(len=len)
    # What this node hashes with, and the identifier that follows from it. SHA3_256
    # by default; `modify_env(hash_factory=...)` changes both together.
    hash_factory: HashFactory = hashlib.sha3_256
    hash_type: bytes = hash_type_of(hashlib.sha3_256)

    def __call__(cls):
        if cls not in cls._instances:
            os.makedirs(cls.cache_dir, exist_ok=True)
            os.makedirs(cls.block_dir, exist_ok=True)
            cls._instances[cls] = super(Enviroment, cls).__call__()
        return cls._instances[cls]


def modify_env(
        cache_dir:           typing.Optional[str]           = None,
        mem_manager:         typing.Optional[MemManager]    = None,
        hash_type:           typing.Optional[bytes]         = None,
        hash_factory:        typing.Optional[HashFactory]   = None,
        block_depth:         typing.Optional[int]           = None,
        block_dir:           typing.Optional[str]           = None,
        skip_wbp_generation: bool                           = False
):
    """Configure the environment. `hash_factory` selects the algorithm blocks are
    addressed by; `hash_type` selects one already registered, by its identifier."""
    if cache_dir: Enviroment.cache_dir = cache_dir + 'grpcbigbuffer/'
    if mem_manager: Enviroment.mem_manager = mem_manager

    if hash_factory:
        register_hash_algorithm(hash_factory)
    elif hash_type:
        hash_factory = hasher_for(hash_type)

    if hash_factory and hash_type_of(hash_factory) != Enviroment.hash_type:
        Enviroment.hash_factory = hash_factory
        Enviroment.hash_type = hash_type_of(hash_factory)
        # Si se modifica el algoritmo hash de los bloques, se pierde compatibilidad con el registro previo.
        if os.path.isdir(Enviroment.block_dir):
            rmtree(Enviroment.block_dir)

    if block_depth: Enviroment.block_depth = block_depth
    if block_dir: Enviroment.block_dir = block_dir
    Enviroment.skip_wbp_generation = skip_wbp_generation


## Hash types ##

# A block pointer names its block by hash. Which algorithm produced that hash is
# carried in `Buffer.Block.Hash.type` -- the digest of the algorithm applied to the
# empty input, so the field is self-describing without a registry.
#
# On the wire that type is always present: a stream has no surrounding structure to
# consult, so every pointer that goes out has to say what it is. In storage that
# would mean repeating the same 32 bytes in every pointer of every block -- a
# filesystem of a few thousand files pays it a few thousand times over for no
# information -- so a stored pointer below the top may omit it and inherit.
#
# Inheritance is positional and follows the block-containment chain: hash `i` of a
# pointer with no type of its own takes the type at index `i` from the nearest
# ancestor that has one. Nearest wins per index individually, so an ancestor list
# longer than the one below it still supplies the indices the nearer one does not
# reach. An explicit type replaces its own entry only; it never shifts the mapping
# for the entries beside it.
#
# The top of a stored tree has no ancestor, so it MUST carry its types. That is the
# one rule that keeps a stored artefact readable by a node whose own configuration
# differs -- `Enviroment.hash_type` says which algorithm this node *packs* with, and
# is not an answer to what some other node's artefact was hashed with.


def hash_types_for_packing() -> HashTypes:
    """The hash types this node writes into the pointers it creates."""
    return (Enviroment.hash_type,)


def resolve_hash_types(
        block: buffer_pb2.Buffer.Block,
        inherited: typing.Optional[typing.Sequence[bytes]] = None
) -> HashTypes:
    """The type of every hash in `block`, taking `inherited` where the block omits one.

    Raises HashTypeError rather than guessing: a pointer whose type cannot be
    deduced names a block under an unknown algorithm, and resolving it to whatever
    this node happens to be configured with would silently name different content.
    """
    inherited = tuple(inherited or ())
    resolved: typing.List[bytes] = []
    for index, _hash in enumerate(block.hashes):
        if _hash.type:
            resolved.append(_hash.type)
        elif index < len(inherited) and inherited[index]:
            resolved.append(inherited[index])
        else:
            raise HashTypeError(
                'bee-rpc: hash %d of a block pointer carries no type and none can be '
                'deduced from its ancestors (%d inherited type(s)). The top of a stored '
                'tree must carry its hash types.' % (index, len(inherited))
            )
    return tuple(resolved)


def inherit_hash_types(
        resolved: typing.Sequence[bytes],
        inherited: typing.Optional[typing.Sequence[bytes]] = None
) -> HashTypes:
    """The context that applies inside the block `resolved` points at.

    The chain, not just the parent: an index the nearer pointer does not reach is
    still answered by whatever ancestor last spoke for it.
    """
    inherited = tuple(inherited or ())
    resolved = tuple(resolved)
    return resolved + inherited[len(resolved):]


def block_id_from_pointer(
        block: buffer_pb2.Buffer.Block,
        inherited: typing.Optional[typing.Sequence[bytes]] = None,
        hexadecimal: bool = True
) -> typing.Optional[typing.Union[str, bytes]]:
    """The id this pointer names in the block registry, or None if it names none.

    The registry is keyed by one algorithm -- `Enviroment.hash_type`, the directory
    entries under `Enviroment.block_dir` -- so of however many names a pointer
    carries, this returns the one that is a storage key. None means "not a pointer
    to a block this node can address", which is the ordinary answer when probing
    bytes that may just be content.
    """
    try:
        types = resolve_hash_types(block=block, inherited=inherited)
    except HashTypeError:
        return None
    for _hash, _type in zip(block.hashes, types):
        if _type == Enviroment.hash_type:
            return _hash.value.hex() if hexadecimal else _hash.value
    return None


def block_pointer(
        block_id: typing.Union[str, bytes],
        omit_types: bool = False
) -> buffer_pb2.Buffer.Block:
    """The pointer that stands in for a block, in the one encoding the library writes.

    Single-hash, under the registry's own algorithm: a pointer built from an id
    alone can only name the digest that id *is*. Callers holding more digests for
    the same block build a longer pointer themselves; the first entry stays the
    storage key.

    `omit_types` writes the compressed form for a stored pointer that has an
    ancestor to inherit from. Never use it for the top of a stored tree, and never
    on the wire.
    """
    value = bytes.fromhex(block_id) if isinstance(block_id, str) else block_id
    _hash = buffer_pb2.Buffer.Block.Hash(value=value)
    if not omit_types:
        _hash.type = Enviroment.hash_type
    return buffer_pb2.Buffer.Block(hashes=[_hash])


def block_pointer_length(
        block_id: typing.Union[str, bytes],
        omit_types: bool = False
) -> int:
    """How many bytes the pointer for this block occupies.

    The one measure both sides of the wbp arithmetic must agree on: the length
    written into a field's varint and the bytes then emitted in its place. They used
    to be a constant (36) and a separately built message, free to drift -- and they
    did, in opposite directions, once the two encodings diverged.
    """
    return len(block_pointer(block_id=block_id, omit_types=omit_types).SerializeToString())


def create_lengths_tree(
        pointer_container: typing.Dict[str, typing.List[typing.List[int]]]
) -> typing.Dict[int, typing.Union[typing.Dict, str]]:
    """
        Create a tree of the pointers where the leafs are the block id's.
    """
    tree: typing.Dict[int, typing.Union[typing.Dict, str]] = {}
    for key, list_pointers in pointer_container.items():
        for pointers in list_pointers:
            current_level = tree
            for pointer in pointers[:-1]:
                if pointer not in current_level:
                    current_level[pointer] = {}
                current_level = current_level[pointer]
            current_level[pointers[-1]] = key
    return tree


def encode_bytes(n: int) -> bytes:
    # https://github.com/fmoo/python-varint/blob/master/varint.py
    def _byte(b):
        return bytes((b,))

    buf = b''
    while True:
        towrite = n & 0x7f
        n >>= 7
        if n:
            buf += _byte(towrite | 0x80)
        else:
            buf += _byte(towrite)
            break
    return buf


def entries_of_multiblock_directory(directory: str) -> typing.List[str]:
    """The ordered list of paths a multiblock directory expands to.

    The same shape as the `file_list` `block_driver.generate_wbp_file` builds:
    the directory's own parts, interleaved with the blocks it points at.
    """
    with open(os.path.join(directory, METADATA_FILE_NAME), 'r') as f:
        _json = json.load(f)
    return [
        os.path.join(directory, str(e)) if type(e) == int
        else os.path.join(Enviroment.block_dir, e[0])
        for e in _json
    ]


def seek_expanded_position(position: int, file_list: typing.List[str]) -> typing.Tuple[str, int]:
    """Map a position in the expanded stream onto (single file, offset within it).

    Every entry of `file_list` is measured by how much it contributes to the
    expansion, which for a multiblock *directory* block is the sum of its own
    expansion -- `os.path.getsize` would report the size of the dirent instead, a
    couple of hundred bytes standing in for however much content the block holds,
    silently shifting every position past it. And a position landing inside such a
    block resolves against that block's own entries, to any depth, rather than
    reaching an `open()` that would raise IsADirectoryError.
    """
    remaining: int = position
    for path in file_list:
        length: int = getsize(path)
        if remaining < length:
            if os.path.isdir(path):
                return seek_expanded_position(remaining, entries_of_multiblock_directory(path))
            return path, remaining
        remaining -= length
    raise ValueError(f"Position {position} is out of buffer range.")


def get_varint_at_position(position, file_list) -> int:
    path, offset = seek_expanded_position(position=position, file_list=file_list)
    with open(path, "rb") as file:
        file.seek(offset)
        result = 0
        shift = 0
        while True:
            byte = file.read(1)
            if not byte:
                break
            byte = ord(byte)
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
        return result


def get_expanded_block_length(block_name: str, _seen: typing.Optional[typing.Set[str]] = None) -> int:
    """How many bytes `reader.read_block` emits for this block.

    A block is stored in one of two shapes, and the length differs between them:
    a single file, whose content is streamed verbatim, or a *multiblock
    directory* with its own `_.json`, whose content is the expansion of that
    directory (its parts, plus its sub-blocks expanded the same way, to any
    depth). Measuring a directory block with `os.path.getsize` returns the size
    of the dirent -- a couple of hundred bytes standing in for however much
    content the block actually holds -- so every caller that has to know how far
    a pointer expands must come through here.
    """
    if _seen is None:
        _seen = set()
    if block_name in _seen:
        raise Exception(f'bee-rpc: detected recursive loop when measuring block {block_name}')

    path = os.path.join(Enviroment.block_dir, block_name)
    if not os.path.isdir(path):
        return os.path.getsize(path)

    # `_seen` is the recursion *stack*, not a set of everything already measured:
    # deduplicated storage means one block is legitimately referenced many times
    # from the same object, and only a block that contains itself is a loop.
    _seen.add(block_name)
    try:
        return getsize(path, _seen=_seen)
    finally:
        _seen.discard(block_name)


def get_pruned_block_length(block_name: str, pointer_length: int) -> int:
    """What a block pointer adds beyond the bytes of the pointer itself.

    `pointer_length` is the length of the pointer *as it will be written*, which
    depends on the digest and on whether the hash types are omitted -- so it is the
    caller's to supply, not a constant to assume.
    """
    return get_expanded_block_length(block_name=block_name) - pointer_length

def getsize(path: str, _seen: typing.Optional[typing.Set[str]] = None) -> int:
    if not os.path.exists(path): 
        return 0
    
    if os.path.isdir(path): 
        with open(os.path.join(path, METADATA_FILE_NAME), 'rb') as f:
            _json = json.load(f)
        
        total_size = 0
        for e in _json:
            if type(e) == int:
                total_size += os.path.getsize(os.path.join(path, str(e)))
            
            else:
                block_id: str = e[0]
                if type(block_id) != str:
                    _msg = f"'bee-rpc error on block metadata file ( _.json ).' for block {block_id} on utils.getsize"
                    raise Exception(_msg)
                
                # The pointer is not stored in the parts, so the whole expansion
                # of the sub-block is what this reference contributes -- not
                # `get_pruned_block_length`, which deliberately discounts the
                # BLOCK_LENGTH bytes a pointer occupies where one is present.
                total_size += get_expanded_block_length(block_name=block_id, _seen=_seen)
                
        return total_size
    
    else:
        return os.path.getsize(path)