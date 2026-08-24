import hashlib
import inspect
import itertools
import json
import os
import shutil
import typing
import warnings
from random import randint
from typing import Callable, Generator, Union, List, Dict, Type

from google.protobuf.message import DecodeError, Message

from bee_rpc import buffer_pb2
from bee_rpc.block_driver import generate_wbp_file, WITHOUT_BLOCK_POINTERS_FILE_NAME, METADATA_FILE_NAME
from bee_rpc.reader import read_block, read_multiblock_directory, read_from_registry, block_exists, read_bee_file
from bee_rpc.utils import Enviroment, MAX_DIR, Signal, EmptyBufferException, Dir, CHUNK_SIZE, \
    is_repeated_message_field


## Block driver ##
def contain_blocks(message: Message) -> bool:
    for field, value in message.ListFields():
        if is_repeated_message_field(field):
            for element in value:
                if contain_blocks(element):
                    return True

        elif isinstance(value, Message) and contain_blocks(value):
            return True

        elif type(value) == bytes:
            try:
                block = buffer_pb2.Buffer.Block()
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=RuntimeWarning)
                    block.ParseFromString(value)
                    return True
            except DecodeError:
                pass

    return False


def copy_block_if_exists(buffer: bytes, directory: str) -> bool:
    # TODO support copy of multiblocks blocks. Now it will create a single file block.
    try:
        block = buffer_pb2.Buffer.Block()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            block.ParseFromString(buffer)
    except DecodeError:
        return False

    # Resolve the block id. Blocks produced by create_block()/build_multiblock()
    # carry a single hash of type Enviroment.hash_type (see block_builder.py), which
    # is what every other get_hash_from_block() call site resolves (internal_block=False).
    # Only this function used internal_block=True, which matches a single hash of the
    # empty type (b'') exclusively — a shape nothing in the library ever produces. As a
    # result copy_block_if_exists() always returned None here for real block pointers,
    # returned False, and callers silently wrote the 36-byte pointer as file content,
    # corrupting large binaries. Try the internal (type=b'') form first for backwards
    # compatibility, then fall back to the standard hash-typed form.
    block_id: typing.Optional[str] = (
        get_hash_from_block(block=block, internal_block=True)
        or get_hash_from_block(block=block, internal_block=False)
    )
    if not block_id:
        return False

    # Reconstruct the block to a temporary sibling file, verify its integrity,
    # then publish it atomically. Historically this streamed read_block() straight
    # into `directory` and returned True unconditionally, so a block that was
    # truncated/corrupt at rest (torn write, interrupted store, rm race) produced
    # a short file with NO error — silently corrupting large binaries (e.g. an ELF
    # whose body is truncated -> "invalid ELF header" at exec). Fail closed
    # instead: on any read error or hash mismatch, leave `directory` untouched and
    # return False so callers can raise rather than write garbage.
    #
    # Verification applies to single-file blocks, whose id is the sha3_256 of their
    # raw content (see block_builder.create_block / utils.get_file_hash). Multiblock
    # *directory* blocks have a composite id that is not the flat-content hash, so
    # they keep the previous stream-through behaviour (now atomic, still the
    # pre-existing "TODO support copy of multiblocks blocks" path).
    _exists, is_multiblock = block_exists(block_id=block_id, is_dir=True)

    # Reconstruct the block's flat content. Single-file blocks stream directly via
    # read_block(). Multiblock (directory) blocks are flattened with
    # read_multiblock_directory(ignore_blocks=True), which walks the block's _.json,
    # recursively rehydrates its sub-blocks (each single-file sub-block hash-verified
    # in read_block) and yields ONLY bytes. read_block() itself cannot flatten a
    # directory block — its dir branch uses ignore_blocks=False and emits
    # Buffer.Block marker objects, which aren't writable bytes; that is why
    # multiblock blocks previously fell into the except below and returned False
    # (the pre-existing "TODO support copy of multiblocks blocks").
    if is_multiblock:
        source = read_multiblock_directory(
            directory=Enviroment.block_dir + block_id,
            ignore_blocks=True,
        )
    else:
        source = read_block(block_id=block_id)

    tmp = directory + '.beeblk-' + str(randint(0, MAX_DIR))
    try:
        hasher = hashlib.sha3_256()
        with open(tmp, 'wb') as file:
            for data in source:
                file.write(data)
                hasher.update(data)
            file.flush()
            os.fsync(file.fileno())

        if not is_multiblock and hasher.hexdigest() != block_id:
            raise Exception(
                'gRPCbb: block reconstruction hash mismatch for ' + block_id
                + ' (got ' + hasher.hexdigest() + ') — refusing to write corrupt content.'
            )

        os.replace(tmp, directory)
        return True
    except Exception as e:  # TODO control only Exception('gRPCbb: Error reading block.')
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        return False


def move_to_block_dir(file_hash: str, file_path: str) -> bool:
    if not block_exists(block_id=file_hash) and os.path.isfile(file_path):
        try:
            # Use a filesystem-specific method to move the file without reading or writing the contents
            # (e.g. link() and unlink() on Unix-like systems) for improved performance.
            destination_path = os.path.join(Enviroment.block_dir, file_hash)
            os.rename(file_path, destination_path)
            return True
        except Exception as e:
            raise Exception('gRPCbb error creating block, file could not be moved: ' + str(e))
    return False


def copy_to_block_dir(file_hash: str, file_path: str) -> bool:
    if not block_exists(block_id=file_hash) and os.path.isfile(file_path):
        try:
            destination_path = os.path.join(Enviroment.block_dir, file_hash)
            # Copy to a temp sibling, fsync, then atomically rename into place. A
            # plain shutil.copyfile() straight to the content-addressed path leaves a
            # TRUNCATED block there if the process dies mid-copy (or the disk fills),
            # and a truncated block later serializes as a payload-less Block(). The
            # temp+fsync+os.replace makes the block appear only once it is complete
            # and durable.
            tmp = destination_path + '.tmp-' + str(randint(0, MAX_DIR))
            try:
                with open(file_path, 'rb') as src, open(tmp, 'wb') as dst:
                    shutil.copyfileobj(src, dst, CHUNK_SIZE)
                    dst.flush()
                    os.fsync(dst.fileno())
                os.replace(tmp, destination_path)
            except BaseException:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
                raise
            return True
        except Exception as e:
            raise Exception('gRPCbb error creating block, file could not be moved: ' + str(e))
    return False


def signal_block_buffer_stream(hash: str):
    # Receiver sends the Buffer with block attr. for stops the block buffer stream.
    pass  # Sends Buffer(block=Block())


def get_hash_from_block(block: buffer_pb2.Buffer.Block,
                        internal_block: bool = False,
                        hexadecimal: bool = True
                        ) -> typing.Optional[str]:
    if internal_block:
        if len(block.hashes) == 1 and block.hashes[0].type == b'':
            return block.hashes[0].value.hex() if hexadecimal else block.hashes[0].value
    else:
        for hash in block.hashes:
            if hash.type == Enviroment.hash_type:
                return hash.value.hex() if hexadecimal else hash.value
    return None


def generate_random_dir() -> str:
    cache_dir = Enviroment.cache_dir
    while True:
        try:
            new_dir: str = cache_dir + str(randint(1, MAX_DIR))
            os.makedirs(new_dir)
            return new_dir
        except FileExistsError:
            pass


def generate_random_file() -> str:
    cache_dir = Enviroment.cache_dir
    try:
        os.mkdir(cache_dir)
    except FileExistsError:
        pass
    while True:
        file = cache_dir + str(randint(1, MAX_DIR))
        if not os.path.isfile(file): return file


def message_to_bytes(message) -> bytes:
    if inspect.isclass(type(message)) and issubclass(type(message), Message):
        return message.SerializeToString()
    elif type(message) is str:
        return bytes(message, 'utf-8')
    else:
        try:
            return bytes(message)
        except TypeError:
            raise (
                    'gRPCbb error -> Serialize message error: some primitive type message not suported for contain partition ' + str(
                type(message)))


def remove_file(file: str):
    os.remove(file)  # TODO could be async.


def remove_dir(dir: str):
    shutil.rmtree(dir)


def i_read_multiblock_directory(directory: str, delete_directory: bool = False, ignore_blocks: bool = True) \
        -> Generator[Union[bytes, buffer_pb2.Buffer.Block], None, None]:
    for i in read_multiblock_directory(directory, delete_directory, ignore_blocks):
        yield i


def stop_generator(iterator, block_id):
    for b in iterator:
        if b.HasField('block') and get_hash_from_block(b.block) == block_id:
            b.ClearField('block')
            yield b
            break
        else:
            yield b


def save_chunks_to_block(
        block_buffer: buffer_pb2.Buffer,
        buffer_iterator,
        signal: Signal = None,
        _json: List[Union[
            int,
            typing.Tuple[str, List[int]]
        ]] = None,
        debug: Callable[[str], None] = lambda s: None,
):
    try:
        debug("Save chunks to block ...")
        block_id: str = get_hash_from_block(block_buffer.block)
        debug(f"Save chunks to block {block_id} start")
        if _json:
            _json.append(
                (block_id, list(block_buffer.block.previous_lengths_position))
            )
        if not block_exists(block_id):  # Second comprobation of that.
            debug(f"The block {block_id} does not exists, saving it.")
            save_chunks_to_file(
                prev=block_buffer.chunk if block_buffer.HasField('chunk') else None,
                buffer_iterator=stop_generator(buffer_iterator, block_id),
                filename=Enviroment.block_dir + block_id,
                signal=signal
            )
        else:
            debug(f"The block {block_id} does exists, skipping all the block buffer.")
            for buffer in buffer_iterator:
                if buffer.HasField('block') and \
                        get_hash_from_block(buffer.block) == block_id:
                    debug(f"Block {block_id} buffer moved.")
                    break
    except Exception as e:
        debug(f"Exception saving chunks to block {_json}: {e}")
        raise e


def save_chunks_to_file(
    buffer_iterator,
    filename: str,
    signal: Signal = None,
    _json: List[Union[
        int,
        typing.Tuple[str, List[int]]
    ]] = None,
    prev: typing.Optional[bytes] = None,
    debug: Callable[[str], None] = lambda s: None,
) -> bool:
    debug(f"Save chunks to file {filename} ...")
    if not signal: signal = Signal(exist=False)
    signal.wait()
    debug(f"Save chunks to the file {filename} start")
    try:
        with open(filename, 'wb') as f:
            signal.wait()
            if prev:
                f.write(prev)
                del prev

            for buffer in buffer_iterator:
                if buffer.HasField('block'):
                    save_chunks_to_block(
                        block_buffer=buffer,
                        buffer_iterator=buffer_iterator,
                        signal=signal,
                        _json=_json,
                        debug=debug
                    )
                    return False
                f.write(buffer.chunk)
            debug(f"Save chunks to the file {filename} ends")
            return True
    except Exception as e:
        debug(f"Exception saving chunks to file {filename}: {e}")
        raise e  # TODO Should be return False ??


def get_subclass(partition, object_cls):
    return get_subclass(
        object_cls=type(
            getattr(
                object_cls(),
                object_cls.DESCRIPTOR.fields_by_number[list(partition.index.keys())[0]].name
            )
        ),
        partition=list(partition.index.values())[0]
    ) if len(partition.index) == 1 else object_cls


def copy_message(obj, field_name, message):  # TODO for list too.
    e = getattr(obj, field_name) if field_name else obj
    if hasattr(message, 'CopyFrom'):
        e.CopyFrom(message)
    elif type(message) is bytes:
        e.ParseFromString(message)
    else:
        e = message
    return obj


def get_submessage(partition, obj, say_if_not_change=False):
    if len(partition.index) == 0:
        return False if say_if_not_change else obj
    if len(partition.index) == 1:
        for field in obj.DESCRIPTOR.fields:
            if field.index + 1 not in partition.index:
                obj.ClearField(field.name)
        return get_submessage(
            partition=list(partition.index.values())[0],
            obj=getattr(obj, obj.DESCRIPTOR.fields[list(partition.index.keys())[0] - 1].name)
        )
    for field in obj.DESCRIPTOR.fields:
        if field.index + 1 in partition.index:
            try:
                submessage = get_submessage(
                    partition=partition.index[field.index + 1],
                    obj=getattr(obj, field.name),
                    say_if_not_change=True
                )
                if not submessage: continue  # Anything to prune.
                copy_message(
                    obj=obj, field_name=field.name,
                    message=submessage
                )
            except:
                pass
        else:
            obj.ClearField(field.name)
    return obj


def put_submessage(partition, message, obj):
    if len(partition.index) == 0:
        return copy_message(
            obj=obj, field_name=None,
            message=message
        )
    if len(partition.index) == 1:
        p = list(partition.index.values())[0]
        if len(p.index) == 1:
            field_name = obj.DESCRIPTOR.fields[list(partition.index.keys())[0] - 1].name
            return copy_message(
                obj=obj, field_name=field_name,
                message=put_submessage(
                    partition=p,
                    obj=getattr(obj, field_name),
                    message=message,
                )
            )
        else:
            return copy_message(
                obj=obj, field_name=obj.DESCRIPTOR.fields[list(partition.index.keys())[0] - 1].name,
                message=message
            )


# TODO DELETE DEPRECATED
def combine_partitions(
        obj_cls: Message,
        partitions_model: tuple,
        partitions: typing.Tuple[str]
):
    obj = obj_cls()
    for i, partition in enumerate(partitions):
        if type(partition) is str and os.path.isfile(partition):
            with open(partition, 'rb') as f:
                partition: bytes = f.read()
        elif type(partition) is str and os.path.isdir(partition):
            with open(partition + '/' + WITHOUT_BLOCK_POINTERS_FILE_NAME, 'rb') as f:
                partition: bytes = f.read()
        elif not (hasattr(partition, 'SerializeToString') or not type(
                partition) is bytes):  # TODO check.   'not type(partition) is bytes' could affect on partitions to buffer()
            raise Exception('Partitions to buffer error.')
        obj = put_submessage(
            partition=partitions_model[i],
            message=partition,
            obj=obj
        )
    return obj


def parse_from_buffer(
        request_iterator,
        signal: Signal = None,
        indices: Union[Message, Dict[int, Union[Type[bytes], Message]]] = None,
        partitions_message_mode: Union[bool, Dict[int, bool]] = False,  # Write on disk by default.
        mem_manager=None,
        debug: Callable[[str], None] = lambda s: None,
):
    try:
        debug("Starting parse_from_buffer")
        if not indices:
            debug("Indices not provided, setting default value (buffer_pb2.Empty)")
            indices = buffer_pb2.Empty()
        if not signal:
            debug("Signal not provided, creating Signal with exist=False")
            signal = Signal(exist=False)
        if not mem_manager:
            debug("mem_manager not provided, using Enviroment.mem_manager")
            mem_manager = Enviroment.mem_manager
        if type(indices) is not dict:
            debug(f"Indices is not a dict, checking if it's a subclass of Message: {indices}")
            if issubclass(indices, Message):
                indices = {1: indices}
                debug(f"Converted indices to dict: {indices}")
            else:
                debug("Error: indices is neither a dict nor a subclass of Message")
                raise Exception

        debug("Updating indices with key 0: bytes")
        indices.update({0: bytes})
        debug(f"Updated indices: {indices}")

        if type(partitions_message_mode) is bool:
            debug(f"partitions_message_mode is bool, creating dict for all indices: {indices.keys()}")
            partitions_message_mode = {i: partitions_message_mode for i in indices}
        elif type(partitions_message_mode) is dict:
            debug("partitions_message_mode is dict, updating missing keys")
            partitions_message_mode.update(
                {i: [False] for i in indices if i not in partitions_message_mode})  # Check that it've all indices.
        else:
            debug(f"Error: partitions_message_mode has incorrect type: {type(partitions_message_mode)}")
            raise Exception("Incorrect partitions message mode type on parse_from_buffer.")

        debug("Validating partitions_message_mode and indices keys")
        if partitions_message_mode.keys() != indices.keys():
            debug(f"Error: partitions_message_mode keys {partitions_message_mode.keys()} != indices keys {indices.keys()}")
            raise Exception("Partitions message mode keys != indices keys on parse_from_buffer")

        debug("Initial configuration validated successfully")

    except Exception as e:
        debug(f"Exception during initial setup: {str(e)}")
        raise Exception(f'Parse from buffer error: Partitions or Indices are not correct. '
                        f'{partitions_message_mode} - {indices} - {str(e)}')

    def parser_iterator(
            request_iterator_obj,
            signal_obj: Signal = None,
            blocks: List[str] = None
    ) -> Generator[buffer_pb2.Buffer, None, None]:
        debug("Starting parser_iterator")
        if not signal_obj:
            debug("signal_obj not provided, creating new Signal")
            signal_obj = Signal(exist=False)
        while True:
            try:
                try:
                    buffer_obj = next(request_iterator_obj)
                except StopIteration:
                    raise StopIteration
                except Exception as e:
                    debug(f"Exception fetching next buffer object: {e}")
                    raise e
            except StopIteration:
                debug("StopIteration in parser_iterator")
                # raise Exception('AbortedIteration')
                break

            if buffer_obj.HasField('signal') and buffer_obj.signal:
                debug("Field 'signal' detected, changing signal_obj state")
                signal_obj.change()

            if not blocks and buffer_obj.HasField('block') or \
                    blocks and buffer_obj.HasField('block') and len(blocks) < Enviroment.block_depth:
                block_hash: str = get_hash_from_block(buffer_obj.block)

                if block_hash:
                    if blocks and block_hash in blocks:
                        if blocks.pop() == block_hash:
                            debug(f"Block {block_hash} removed from blocks")
                            break
                        else:
                            debug("Error: Block intersections are not allowed")
                            raise Exception('gRPCbb: IntersectionError: Intersections between blocks are not allowed.')
                    else:
                        if not blocks:
                            blocks = [block_hash]
                        else:
                            blocks.append(block_hash)

                        if block_exists(block_hash):
                            signal_block_buffer_stream(block_hash)  # Send the sub-buffer stop signal

                        yield buffer_obj
                        for block_chunk in parser_iterator(
                                request_iterator_obj=request_iterator_obj,
                                signal_obj=signal_obj,
                                blocks=blocks
                        ):
                            yield block_chunk

            if buffer_obj.HasField('chunk'):
                debug("Yielding normal chunk")
                yield buffer_obj
            elif not buffer_obj.HasField('head'):
                debug("Buffer has no 'head', ending iteration")
                break
            if buffer_obj.HasField('separator') and buffer_obj.separator:
                debug("Separator detected, ending iteration")
                break

    def parse_message(message_field, _request_iterator, _signal: Signal):
        debug(f"Starting parse_message for message_field: {message_field}")
        all_buffer: bytes = b''
        in_block: typing.Optional[str] = None
        for b in parser_iterator(
                request_iterator_obj=_request_iterator,
                signal_obj=_signal,
        ):
            debug(f"Processing element in parse_message: {b}")
            if b.HasField('block'):
                block_id: str = get_hash_from_block(block=b.block)
                debug(f"Block detected: {block_id}")
                if block_id == in_block:
                    debug(f"Exiting block {block_id}")
                    in_block = None
                elif not in_block and block_exists(block_id=block_id):
                    debug(f"Entering existing block {block_id}")
                    in_block = block_id
                    debug("Reading existing blocks")
                    all_buffer += b''.join([c for c in read_block(block_id=block_id) if type(c) is bytes])
                    continue

            if not in_block:
                debug(f"Adding chunk of size {len(b.chunk)}")
                all_buffer += b.chunk
                debug(f"Total buffer size: {len(all_buffer)}")

        debug(f"Finished accumulating buffer. Total size: {len(all_buffer)}")
        if len(all_buffer) == 0:
            debug("Empty buffer, raising EmptyBufferException")
            raise EmptyBufferException()
        if message_field is str:
            debug("Converting buffer to string")
            return all_buffer.decode('utf-8')
        elif inspect.isclass(message_field) and issubclass(message_field, Message):
            debug(f"Parsing protobuf message: {message_field}")
            message = message_field()
            message.ParseFromString(all_buffer)
            return message
        else:
            debug(f"Attempting to convert to primitive type: {message_field}")
            try:
                return message_field(all_buffer)
            except Exception as e:
                debug(f"Error converting buffer: {str(e)}")
                raise Exception(
                    'gRPCbb error -> Parse message error: some primitive type message not supported for contain '
                    'partition ' + str(
                        message_field) + str(e))

    def save_to_dir(_request_iterator, _signal) -> str:
        debug("Starting save_to_dir")
        dirname = generate_random_dir()
        debug(f"Temporary directory created: {dirname}")
        _i: int = 1
        _json: List[Union[int, typing.Tuple[str, List[int]]]] = []
        try:
            while True:
                debug(f"Saving part {_i}")
                _json.append(_i)
                debug(f"Calling save_chunks_to_file for part {_i}")
                if save_chunks_to_file(
                        filename=dirname + '/' + str(_i),
                        buffer_iterator=parser_iterator(
                            request_iterator_obj=_request_iterator,
                            signal_obj=_signal
                        ),
                        signal=_signal,
                        _json=_json,
                        debug=debug
                ):
                    debug(f"save_chunks_to_file signaled completion for part {_i}")
                    break
                _i += 1

        except StopIteration:
            debug("StopIteration in save_to_dir")
            pass

        except Exception as e:
            debug(f"Exception in save_to_dir: {str(e)}, removing directory {dirname}")
            remove_dir(dir=dirname)
            raise e

        if len(_json) < 2:
            debug("Single file detected, converting to standalone file")
            filename: str = generate_random_file()
            try:
                debug(f"Moving {dirname}/1 to {filename}")
                shutil.move(dirname + '/1', filename)
                return filename
            except FileNotFoundError:
                debug(f"Error: File {dirname}/1 not found")
                remove_file(file=filename)
                raise Exception('gRPCbb error: on save_to_dir function, the only file had no name 1')
        else:
            debug(f"Writing metadata to {dirname}/{METADATA_FILE_NAME}")
            with open(dirname + '/' + METADATA_FILE_NAME, 'w') as f:
                json.dump(_json, f)

            if not Enviroment.skip_wbp_generation:
                debug("Generating WBP file")
                generate_wbp_file(dirname, debug=debug)

            return dirname  # separator break.

    def iterate_message(message_field, mode: bool, _signal: Signal, _request_iterator):
        debug(f"Iterate_message: mode={'parse' if mode else 'save'}, message_field={message_field}")
        if mode:
            debug("Parse mode: parsing message in memory")
            return parse_message(
                message_field=message_field,
                _request_iterator=_request_iterator,
                _signal=_signal,
            )
        else:
            debug("Save mode: saving to directory")
            return Dir(
                dir=save_to_dir(
                    _request_iterator=_request_iterator,
                    _signal=_signal
                ),
                _type=message_field
            )

    debug("Starting main iteration over request_iterator")
    for buffer in request_iterator:
        debug(f"Processing buffer: {buffer}")
        if buffer.HasField('head'):
            debug(f"Field 'head' detected with index {buffer.head.index}")
            if buffer.head.index not in indices:
                debug(f"Error: index {buffer.head.index} not found in indices {indices.keys()}")
                raise Exception(
                    'Parse from buffer error: buffer head index is not correct ' + str(buffer.head.index) + str(
                        indices.keys()))
            try:
                debug(f"Processing index {buffer.head.index}")
                result = iterate_message(
                    message_field=indices[buffer.head.index],
                    mode=partitions_message_mode[buffer.head.index],
                    _signal=signal,
                    _request_iterator=itertools.chain([buffer], request_iterator),
                )
                debug(f"Yielding result for index {buffer.head.index}")
                yield result
            except EmptyBufferException:
                debug("EmptyBufferException caught")
                if indices[1] == buffer_pb2.Empty:
                    debug("Yielding buffer_pb2.Empty()")
                    yield buffer_pb2.Empty()
                else:
                    debug("Continuing without yield")
                    continue

        elif 1 in indices:  # Does not've more than one index and more than one partition too.
            debug("Processing default index 1")
            try:
                result = iterate_message(
                    message_field=indices[1],
                    mode=partitions_message_mode[1],
                    _signal=signal,
                    _request_iterator=itertools.chain([buffer], request_iterator),
                )
                debug("Yielding result for index 1")
                yield result
            except EmptyBufferException:
                debug("EmptyBufferException for index 1")
                if indices[1] == buffer_pb2.Empty:
                    yield buffer_pb2.Empty()
                else:
                    continue

        elif 0 in indices:  # always true
            debug("Processing default index 0")
            try:
                result = iterate_message(
                    message_field=indices[0],
                    mode=partitions_message_mode[0],
                    _signal=signal,
                    _request_iterator=itertools.chain([buffer], request_iterator),
                )
                debug("Yielding result for index 0")
                yield result
            except EmptyBufferException:
                debug("EmptyBufferException for index 0")
                if indices[0] == buffer_pb2.Empty:
                    yield buffer_pb2.Empty()
                else:
                    continue

        else:
            debug(f"Error: Invalid indices: {indices}")
            raise Exception('Parse from buffer error: index are not correct ' + str(indices))
    debug("Finalizing main iteration over request_iterator")

def serialize_to_buffer(
        message_iterator=None,  # Message, bytes or Dir
        signal=None,
        indices: Union[Message, Dict[int, Union[Type[bytes], Message]]] = None,
        mem_manager=None,
        debug: Callable[[str], None] = lambda s: None  # Debug function
) -> Generator[buffer_pb2.Buffer, None, None]:  # method: indice
    try:
        debug("Entering serialize_to_buffer")  # Log entry

        if not message_iterator:
            message_iterator = buffer_pb2.Empty()
            debug("message_iterator is None, initialized to Empty")
        if not indices:
            indices = {}
            debug("indices is None, initialized to {}")
        if not signal:
            signal = Signal(exist=False)
            debug("signal is None, initialized to Signal(exist=False)")
        if not mem_manager:
            mem_manager = Enviroment.mem_manager
            debug("mem_manager is None, initialized to Enviroment.mem_manager")

        debug(f"Initial indices: {indices}")

        if type(indices) is not dict:
            if issubclass(indices, Message):
                indices = {1: indices}
                debug(f"indices is a Message subclass, updated to: {indices}")
            else:
                raise Exception("Indices must be a dict or a Message subclass") 

        indices.update({0: bytes})
        debug(f"indices updated with 0: bytes: {indices}")

        if not hasattr(message_iterator, '__iter__'):
            message_iterator = itertools.chain([message_iterator])
            debug("message_iterator is not iterable, converted to itertools.chain")

        if len(indices) == 1:  # Only 've {0: bytes}
            first_message = next(message_iterator)  # Extract the first message to send.
            debug(f"First message: {first_message}")
            if type(first_message) is Dir and first_message.type != bytes:  # If the message is Dir and it's not bytes
                indices.update({1: first_message.type})
                debug(f"first_message is a Dir, indices updated: {indices}")
            elif issubclass(type(first_message), Message):  # If the message is a proto Message type
                indices.update({1: type(first_message)})
                debug(f"first_message is a Message subclass, indices updated: {indices}")
            message_iterator = itertools.chain([first_message], message_iterator)
            debug("message_iterator updated with first_message")

        indices = {e[1]: e[0] for e in indices.items()}
        debug(f"Final indices: {indices}")

    except Exception as e:
        error_message = f'Serialzie to buffer error: Indices are not correct {str(indices)} - {str(e)}'
        debug(error_message)  # Log the exception
        raise  # Re-raise the exception after logging

    def send_file(_head: buffer_pb2.Buffer.Head, filedir: str, _signal: Signal) -> Generator[buffer_pb2.Buffer, None, None]:
        debug(f"Sending file: {filedir}")
        yield buffer_pb2.Buffer(
            head=_head
        )
        for _b in read_from_registry(
                filename=filedir,
                signal=_signal,
                debug=debug
        ):
            _signal.wait()
            try:
                yield _b
            except Exception as e:
                debug(f"- exception {e}.")
            finally:
                _signal.wait()
        debug(f"Ends read from registry for {filedir}")
        yield buffer_pb2.Buffer(
            separator=True
        )

    def send_message(
            _signal: Signal,
            _message: Message | bytes,
            _head: buffer_pb2.Buffer.Head = None,
            _mem_manager=Enviroment.mem_manager,
    ) -> Generator[buffer_pb2.Buffer, None, None]:
        debug(f"Sending message: {_message}")
        message_bytes = message_to_bytes(message=_message)
        if len(message_bytes) < CHUNK_SIZE and (
                not isinstance(_message, Message) or
                isinstance(_message, Message) and not contain_blocks(message=_message)
        ):
            _signal.wait()
            try:
                yield buffer_pb2.Buffer(
                    chunk=bytes(message_bytes),
                    head=_head,
                    separator=True
                ) if _head else buffer_pb2.Buffer(
                    chunk=bytes(message_bytes),
                    separator=True
                )
            finally:
                _signal.wait()

        else:
            try:
                if _head:
                    yield buffer_pb2.Buffer(
                        head=_head
                    )
            finally:
                _signal.wait()

            _signal.wait()
            file = generate_random_file()
            with open(file, 'wb') as f, _mem_manager(len=len(message_bytes)):
                f.write(message_bytes)
            try:
                yield from read_from_registry(
                    filename=file,
                    signal=_signal
                )
            finally:
                remove_file(file)

            try:
                yield buffer_pb2.Buffer(
                    separator=True
                )
            finally:
                _signal.wait()

    for message in message_iterator:
        debug(f"Processing message: {message}") # Log each message being processed
        if type(message) is Dir:
            debug(f"Message is a Dir, sending file: {message.dir}")
            yield from send_file(
                _head=buffer_pb2.Buffer.Head(
                    index=indices[message.type]
                ),
                filedir=message.dir,
                _signal=signal
            )
        else:
            debug(f"Message is not a Dir, sending message: {message}")
            yield from send_message(
                _signal=signal,
                _message=message,
                _head=buffer_pb2.Buffer.Head(
                    index=indices[type(message)]
                ),
                _mem_manager=mem_manager,
            )
    debug("Exiting serialize_to_buffer") # Log exit


def client_grpc(
        method,
        input=None,
        timeout=None,
        indices_parser: Union[Message, Dict[int, Union[Type[bytes], Message]]] = None,
        partitions_message_mode_parser: Union[bool, list, dict] = None,
        indices_serializer: Union[Message, Dict[int, Union[Type[bytes], Message]]] = None,
        mem_manager=None,
        debug: Callable[[str], None]=lambda s: None
):  # indice: method
    if not indices_parser:
        indices_parser = buffer_pb2.Empty
        partitions_message_mode_parser = True
    if not partitions_message_mode_parser: partitions_message_mode_parser = False
    if not indices_serializer: indices_serializer = {}
    if not mem_manager: mem_manager = Enviroment.mem_manager
    signal = Signal()
    yield from parse_from_buffer(
        request_iterator=method(
            serialize_to_buffer(
                message_iterator=input if input else buffer_pb2.Empty(),
                signal=signal,
                indices=indices_serializer,
                mem_manager=mem_manager,
                debug=debug
            ),
            timeout=timeout
        ),
        signal=signal,
        indices=indices_parser,
        partitions_message_mode=partitions_message_mode_parser,
        debug=debug
    )


def write_to_file(
        path: str,
        file_name: str,
        input=None,
        indices: Union[Message, Dict[int, Union[Type[bytes], Message]]] = None,
        mem_manager=None,
        extension: str="bee"
) -> str:
    """
    Writes serialized data to a binary file with a `.bee` extension.
    Each serialized message is prefixed by its length (4 bytes, big-endian).
    Args:
        path (str): The directory path where the file will be created.
        file_name (str): The name of the output file (without the `.bee` extension).
        input (optional): The input data to be serialized. Defaults to `None`, in 
                          which case an empty message is used.
        indices (optional): A mapping or protocol buffer message for guiding the 
                             serialization. Defaults to `None`.
        mem_manager (optional): A memory manager for resource handling during 
                                 serialization. Defaults to `None`.
    Returns:
        str: The full path to the output `.bee` file that was created.
    """
    # Create the full path for the output file
    output_file = os.path.join(path, f"{file_name}.{extension}")  # bee-rpc file extension

    # Ensure the output directory exists
    os.makedirs(path, exist_ok=True)

    # Open the output file in write-binary mode
    with open(output_file, 'wb') as f:
        for buff in serialize_to_buffer(
                message_iterator=input if input else buffer_pb2.Empty(),
                indices=indices,
                mem_manager=mem_manager
            ):
            # Serialize the buffer
            serialized_data = buff.SerializeToString()
            
            # Get the size of the serialized data
            size = len(serialized_data)
            
            # Write the size as a 4-byte big-endian integer
            f.write(size.to_bytes(4, byteorder='big'))
            
            # Write the serialized message
            f.write(serialized_data)

    return output_file


def read_from_file(
        path: str,
        indices: Union[Message, Dict[int, Union[Type[bytes], Message]]] = None
) -> Generator[Dir, None, None]:        
    """
    Reads serialized data from a binary file with a `.bee` extension.

    This function opens a `.bee` file, deserializes its content, and yields 
    parsed `Dir` objects. It uses the provided indices for guiding deserialization 
    and ensures the correct parsing of the file.

    Args:
        path (str): The full path to the `.bee` file to read.
        indices (optional): A mapping or protocol buffer message for guiding 
                            the deserialization. Defaults to `None`.

    Returns:
        Generator[Dir, None, None]: A generator that yields `Dir` objects parsed 
                                    from the file content.

    Example:
        >>> for dir_obj in read_from_file("/path/to/myfile.bee"):
        ...     print(dir_obj)
    """

    yield from parse_from_buffer(
            request_iterator=read_bee_file(filename=path),
            indices=indices,
            partitions_message_mode=False  # Always false means always yield a Dir.
        )
