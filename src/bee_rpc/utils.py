import hashlib
import json
import os
from shutil import rmtree
from threading import Condition

import typing

from google.protobuf.descriptor import FieldDescriptor

# GrpcBigBuffer.
CHUNK_SIZE = 1024 * 1024  # 1MB
MAX_DIR = 999999999
WITHOUT_BLOCK_POINTERS_FILE_NAME = 'wbp.bin'
METADATA_FILE_NAME = '_.json'
BLOCK_LENGTH = 36


def is_repeated_message_field(field: FieldDescriptor) -> bool:
    """Whether ``field`` is a repeated message field that block traversal must recurse into.

    Map fields are excluded: protobuf models them as a repeated message of synthesized
    entries, but their values are not part of the message tree a block can hide in.

    ``FieldDescriptor.label`` is not used to answer this. It was deprecated in protobuf
    5.27 in favour of ``is_repeated`` and removed in 7.x, so reading it raises
    ``AttributeError: 'google._upb._message.FieldDescriptor' object has no attribute
    'label'`` on a current protobuf -- from inside ``contain_blocks``, which every
    ``serialize_to_buffer`` call reaches, so nothing can be sent at all. ``is_repeated``
    is preferred where it exists and ``label`` is the fallback for protobuf < 5.27, which
    covers the whole range this package has ever been installed against.
    """
    is_repeated = getattr(field, "is_repeated", None)
    if is_repeated is None:
        is_repeated = field.label == FieldDescriptor.LABEL_REPEATED

    return bool(is_repeated) \
        and field.type == FieldDescriptor.TYPE_MESSAGE \
        and not field.message_type.GetOptions().map_entry


class EmptyBufferException(Exception):
    pass


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
    # Create a hash object
    hash = hashlib.sha3_256()
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


## Enviroment ##

class Enviroment(type):
    # Using singleton pattern
    _instances = {}
    cache_dir = os.path.abspath(os.curdir) + '/__cache__/'
    block_dir = os.path.abspath(os.curdir) + '/__block__/'
    block_depth = 1
    skip_wbp_generation = False
    mem_manager = lambda len: MemManager(len=len)
    hash_type: bytes = bytes.fromhex("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")  # SHA3_256

    def __call__(cls):
        if cls not in cls._instances:
            os.makedirs(cls.cache_dir, exist_ok=True)
            os.makedirs(cls.block_dir, exist_ok=True)
            cls._instances[cls] = super(Enviroment, cls).__call__()
        return cls._instances[cls]


def modify_env(
        cache_dir:           typing.Optional[str]        = None,
        mem_manager:         typing.Optional[MemManager] = None,
        hash_type:           typing.Optional[bytes]      = None,
        block_depth:         typing.Optional[int]        = None,
        block_dir:           typing.Optional[str]        = None,
        skip_wbp_generation: bool                        = False
):
    if cache_dir: Enviroment.cache_dir = cache_dir + 'grpcbigbuffer/'
    if mem_manager: Enviroment.mem_manager = mem_manager
    if hash_type and hash_type != Enviroment.hash_type:
        Enviroment.hash_type = hash_type
        # Si se modifica el algoritmo hash de los bloques, se pierde compatibilidad con el registro previo.
        rmtree(Enviroment.block_dir)
    if block_depth: Enviroment.block_depth = block_depth
    if block_dir: Enviroment.block_dir = block_dir
    Enviroment.skip_wbp_generation = skip_wbp_generation


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


def get_varint_at_position(position, file_list) -> int:
    file_size = sum(os.path.getsize(f) for f in file_list)
    if position > file_size:
        raise ValueError(f"Position {position} is out of buffer range.")
    file_index = 0
    while position > os.path.getsize(file_list[file_index]):
        position -= os.path.getsize(file_list[file_index])
        file_index += 1
    with open(file_list[file_index], "rb") as file:
        file.seek(position)
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


def get_pruned_block_length(block_name: str) -> int:
    return os.path.getsize(os.path.join(Enviroment.block_dir, block_name)) - BLOCK_LENGTH

def getsize(path: str) -> int:
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
                
                total_size += get_pruned_block_length(block_name=block_id)
                
        return total_size
    
    else:
        return os.path.getsize(path)