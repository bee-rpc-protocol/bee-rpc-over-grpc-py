import gc
import json
import os
import shutil
from io import BufferedReader
from typing import Callable, Generator, Union, Tuple

from google.protobuf.message import DecodeError
from bee_rpc import buffer_pb2
from bee_rpc.utils import Signal, CHUNK_SIZE, METADATA_FILE_NAME, Enviroment, block_pointer


def block_exists(block_id: str, is_dir: bool = False, debug: Callable[[str], None] = lambda s: None) -> bool|Tuple[bool, bool]:
    """
    This is a very bad pattern.    
    If is_dir is False, returns if if the file or directory block_id exists. 
    If is_dir is True, return if exists and if is a directory or not.
    
    TODO Should be two separate functions.
    """
    debug(f"Check if block exists {Enviroment.block_dir + block_id}")
    try:
        f: bool = os.path.isfile(Enviroment.block_dir + block_id)
        d: bool = os.path.isdir(Enviroment.block_dir + block_id)
    except Exception as e:
        raise Exception(
            'gRPCbb error checking block: ' + str(e) + " " + str(Enviroment.block_dir) + " " + str(
                block_id) + " " + str(is_dir)
        )
    return f or d if not is_dir else (f or d, d)


def read_file_by_chunks(filename: str, signal: Signal = None, debug: Callable[[str], None] = lambda s: None) -> Generator[bytes, None, None]:
    debug(f"Read file by chunks {filename}")
    if not signal: signal = Signal(exist=False)
    signal.wait()
    try:
        with BufferedReader(open(filename, 'rb')) as f:
            while True:
                f.flush()
                signal.wait()
                piece: bytes = f.read(CHUNK_SIZE)
                if len(piece) == 0: return
                yield piece
    except Exception as e:
        debug(f"Exception on read file by chunks: {e}")
    finally:
        debug(f"Finalized read file by chunks {filename}")
        gc.collect()

# TODO should be re-implemented like utils.getsize
def read_multiblock_directory(directory: str, delete_directory: bool = False, ignore_blocks: bool = True, debug: Callable[[str], None] = lambda s: None) \
        -> Generator[Union[bytes, buffer_pb2.Buffer.Block], None, None]:
    debug(f"Read multiblock directory {directory}. Delete dir: {delete_directory}.  Ignore blocks: {ignore_blocks}")
    if directory[-1] != '/':
        directory = directory + '/'
    for e in json.load(open(
            directory + METADATA_FILE_NAME,
    )):
        if type(e) == int:
            yield from read_file_by_chunks(filename=directory + str(e))
        else:
            block_id: str = e[0]
            if type(block_id) != str:
                debug(f"'gRPCbb error on block metadata file ( _.json ).' for block {block_id} on read_multiblock_directory")
                raise Exception('gRPCbb error on block metadata file ( _.json ).')
            if not ignore_blocks:
                # Fully typed, always: a stream has no surrounding structure a
                # reader could consult, so an omitted hash type on the wire is
                # simply unresolvable. Compression by inheritance is a property of
                # storage, where the containing block is there to be asked.
                block = block_pointer(block_id=block_id)
                block.previous_lengths_position.extend(e[1])
                debug("- yielding block init")
                yield block
                debug(f"yielded block init")
                # The marker already names the block; what follows it on the wire is
                # the block's *content*, always flat. A block that is itself a
                # multiblock directory used to be expanded with ignore_blocks=False
                # here, which emitted its sub-blocks' markers too -- carrying
                # `previous_lengths_position` values that are offsets into the nested
                # block's own stream, into a stream where they mean nothing. A
                # receiver writes those straight to its `_.json`
                # (client.save_chunks_to_block), producing metadata in two mixed
                # coordinate systems that no length arithmetic can make sense of.
                # Streaming flat keeps the nesting an implementation detail of
                # whoever stores the block: the bytes are the same either way, and a
                # directory block's id is the hash of exactly this expansion
                # (block_builder.generate_id), so the receiver can verify it.
                yield from read_block(block_id=block_id, debug=debug, ignore_blocks=True)
                debug("- yielding block end")
                yield block
                debug(f"yielded block end")
            else:
                yield from read_block(block_id=block_id, debug=debug, ignore_blocks=ignore_blocks)

    if delete_directory:
        shutil.rmtree(directory)


def _verify_single_file_block(path: str, block_id: str) -> None:
    """Fail closed before a block's bytes enter a serialized stream.

    A single-file block is content-addressed: its id is its bytes under the algorithm
    this node addresses blocks by (see block_builder.create_block /
    utils.get_file_hash and Enviroment.hash_factory). If the block file is
    present but truncated/corrupt at rest (torn write, interrupted copy, a race with
    the documented `rm -rf __block__` cleanup), read_file_by_chunks() would stream
    the short/empty content into the buffer with NO error — producing a `.bee`/stream
    that carries the Buffer.Block marker but little or no payload ("has Block() but no
    blocks"). Verify the content hash up front and raise instead of emitting garbage.
    """
    hasher = Enviroment.hash_factory()
    with open(path, 'rb') as f:
        while True:
            piece = f.read(CHUNK_SIZE)
            if not piece:
                break
            hasher.update(piece)
    if hasher.hexdigest() != block_id:
        raise Exception(
            'gRPCbb: block content hash mismatch for ' + block_id
            + ' (got ' + hasher.hexdigest() + ', ' + str(os.path.getsize(path))
            + ' bytes) — refusing to serialize a corrupt/truncated block.'
        )


def read_block(block_id: str, debug: Callable[[str], None] = lambda s: None, ignore_blocks: bool = True) -> Generator[Union[bytes, buffer_pb2.Buffer.Block], None, None]:
    """Stream a block's content, whichever shape it is stored in.

    `ignore_blocks` carries the caller's framing choice all the way down. It used
    to stop here: a block that is itself a multiblock directory was always
    expanded with ignore_blocks=False, so a caller asking for a flat byte stream
    still got `Buffer.Block` markers back from the nested level -- objects, not
    bytes -- and had to filter them out to write or hash the result. The bytes
    were right once filtered, but no caller could take the contract at its word.
    """
    b, d = block_exists(block_id=block_id, is_dir=True, debug=debug)
    debug(f"Reading block {block_id}. block exists -> {b, d}")
    if b and not d:
        # Verify integrity before yielding any bytes; a bad block must abort the
        # stream, not silently produce a payload-less Block() marker.
        _verify_single_file_block(Enviroment.block_dir + block_id, block_id)
        yield from read_file_by_chunks(filename=Enviroment.block_dir + block_id)

    elif d:
        yield from read_multiblock_directory(
            directory=Enviroment.block_dir + block_id,
            ignore_blocks=ignore_blocks
        )

    else:
        debug(f'gRPCbb: Error reading block {block_id}')
        raise Exception('gRPCbb: Error reading block.')


def read_from_registry(filename: str, signal: Signal = None, debug: Callable[[str], None] = lambda s: None) -> Generator[buffer_pb2.Buffer, None, None]:
    is_dir = os.path.isdir(filename)
    debug(f"Read from registry {filename}. Is dir: {is_dir}")
    for c in read_multiblock_directory(
            directory=filename,
            ignore_blocks=False,
            debug=debug
    ) if is_dir else \
            read_file_by_chunks(
                filename=filename,
                signal=signal,
                debug=debug
            ):
        yield buffer_pb2.Buffer(chunk=c) if type(c) is bytes else buffer_pb2.Buffer(block=c)


def read_bee_file(filename: str) -> Generator[buffer_pb2.Buffer, None, None]:
    """
    Reads a `.bee` file containing serialized buffer_pb2.Buffer objects with length-prefixed encoding.

    Each message is preceded by a 4-byte big-endian integer indicating its length. This function
    parses and yields each message as a buffer_pb2.Buffer object.

    Args:
        filename (str): Path to the `.bee` file.

    Yields:
        buffer_pb2.Buffer: Parsed protobuf message.

    Raises:
        ValueError: If a message cannot be fully read or deserialized.
    """
    try:
        with open(filename, 'rb') as f:
            while True:
                # Read the 4-byte length prefix
                size_bytes = f.read(4)
                if not size_bytes:
                    break  # End of file

                if len(size_bytes) != 4:
                    raise ValueError("Invalid file format: Could not read message size.")

                # Decode the length of the message
                message_size = int.from_bytes(size_bytes, byteorder='big')

                # Read the message content based on the length
                message_bytes = f.read(message_size)
                if len(message_bytes) != message_size:
                    raise ValueError("Invalid file format: Incomplete message data.")

                # Parse the message
                buff = buffer_pb2.Buffer()
                try:
                    buff.ParseFromString(message_bytes)
                except DecodeError as e:
                    raise ValueError(f"Failed to parse message: {e}")

                yield buff
    finally:
        gc.collect()