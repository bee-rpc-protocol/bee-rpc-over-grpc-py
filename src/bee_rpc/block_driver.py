import json
import os.path
from typing import Callable, Optional, Sequence, Union, List, Tuple, Dict, Generator

from bee_rpc.validate_lengths_tree import validate_lengths_tree
from bee_rpc.utils import METADATA_FILE_NAME, WITHOUT_BLOCK_POINTERS_FILE_NAME, Enviroment, \
    create_lengths_tree, encode_bytes, get_varint_at_position, get_pruned_block_length, getsize, \
    block_pointer, block_pointer_length


def compute_wbp_lengths(
        tree: Dict[int, Union[Dict, str]],
        file_list: List[str],
        pointer_lengths: Dict[int, int]
) -> Dict[int, int]:
    """The length each varint must state once blocks are replaced by their pointers.

    `pointer_lengths` says how long the pointer written at each position will be --
    the very lengths `regenerate_buffer` then emits, so the two cannot disagree.
    They used to be a constant here and a separately built message there, and drifted
    into two different encodings of the same object.
    """
    def __rec_compute_wbp_lengths(_tree: Dict[int, Union[Dict, str]], _file_list: List[str]) \
            -> Dict[int, Tuple[int, int]]:  # Tuple is wbp length and augmented pruned length.
        lengths: Dict[int, Tuple[int, int]] = {}
        for key, value in _tree.items():
            position_length: int = get_varint_at_position(key, _file_list)
            if isinstance(value, Dict):
                pruned_length: int = 0
                for k, v in __rec_compute_wbp_lengths(
                        _tree=value,
                        _file_list=_file_list
                ).items():
                    pruned_length += v[1]
                    lengths[k] = (v[0], 0)

            else:
                pruned_length: int = get_pruned_block_length(value, pointer_lengths[key])

            if pruned_length > position_length:
                raise Exception("gRPCbb on block_driver compute_wbp_lengths method, "
                                "the pruned_length can't be greater than the real length.")
            lengths[key] = (
                position_length - pruned_length,
                pruned_length + len(encode_bytes(position_length)) - len(encode_bytes(position_length - pruned_length))
            )
        return lengths

    return {k: v[0] for k, v in __rec_compute_wbp_lengths(_tree=tree, _file_list=file_list).items()}


def set_varint_value(varint_pos: int, buffer: List[Union[bytes, str]], new_value: int):
    def __set_varint_value(_varint_pos: int, _buffer: bytes, _new_value: int) -> bytes:
        """
        Sets the value of the varint at the given position in the Protobuf buffer to the given value and returns
        the modified buffer.
        """
        # Convert the given value to a varint and store it in a bytes object
        varint_bytes = []
        while True:
            byte = _new_value & 0x7F
            _new_value >>= 7
            varint_bytes.append(byte | 0x80 if _new_value > 0 else byte)
            if _new_value == 0:
                break
        varint_bytes = bytes(varint_bytes)

        # Calculate the number of bytes to remove from the original varint
        original_varint_bytes = _buffer[_varint_pos:]
        original_varint_length = 0
        while (original_varint_bytes[original_varint_length] & 0x80) != 0:
            original_varint_length += 1
        original_varint_length += 1

        # Remove the original varint and append the new one
        return _buffer[:_varint_pos] + varint_bytes + _buffer[_varint_pos + original_varint_length:]

    """
    Modify the varint at the given position in the buffer which is composed by a list of bytes and binary files. 
    The position is based on the concatenation of all the bytes and binary files in the buffer.
    The function modify the buffer.
    """
    offset: int = 0
    for index, value in enumerate(buffer):
        if type(value) == bytes:
            obj_size = len(value)
            if offset <= varint_pos < offset + obj_size:
                buffer[index] = __set_varint_value(
                        _varint_pos=varint_pos-offset,
                        _buffer=value,
                        _new_value=new_value
                    )
                return
            offset += obj_size
        else:
            # `varint_pos` is a position in the expanded stream, so a block entry
            # advances the cursor by its whole expansion. A multiblock directory
            # block measures as its dirent under `os.path.getsize`, which would put
            # every varint after it at the wrong offset.
            offset += getsize(value)

    raise Exception('gRPCbb block driver error on set varint value')


def regenerate_buffer(
        lengths: Dict[int, int],
        buffer: List[Union[bytes, str]],
        omit_types: bool = False
) -> Generator[bytes, None, None]:
    """
    Replace real lengths with wbp lengths.

    `omit_types` writes the compressed pointer form, for a stored object that has an
    ancestor to inherit its hash types from. False -- the default -- spells them out,
    which is what the top of a stored tree must do.
    """
    for varint_pos, new_value in sorted(lengths.items(), key=lambda x: x[0], reverse=True):
        set_varint_value(
                varint_pos=varint_pos,
                buffer=buffer,
                new_value=new_value,
            )

    for b in buffer:
        if type(b) == bytes:
            yield b
        else:
            yield block_pointer(
                block_id=str(b.split('/')[-1]),
                omit_types=omit_types
            ).SerializeToString()


def generate_wbp_file(
        dirname: str,
        inherited: Optional[Sequence[bytes]] = None,
        debug: Callable[[str], None] = lambda s: None,
):
    """Write the object with block pointers for an already-stored directory.

    `inherited` is the hash-type context this directory sits in. None -- the default
    -- means it is the top of a stored tree: its pointers spell out their own hash
    types, because there is nothing above them to ask. Pass the enclosing pointer's
    resolved types for a nested one, and its pointers are written in the compressed
    form that inherits them.
    """
    with open(dirname + '/' + METADATA_FILE_NAME, 'r') as f:
        _json: List[Union[
            int,
            List[str, List[int]]
        ]] = json.load(f)
           
    wbp_length = sum(os.path.getsize(os.path.join(dirname, str(e))) for e in _json if isinstance(e, int))     
    debug(f"Generate wbp file with length {(wbp_length / (1024 * 1024)):.2f} MB")
    
    omit_types: bool = inherited is not None
    blocks: Dict[str, List[List[int]]] = {}
    pointer_lengths: Dict[int, int] = {}
    buffer: List[Union[bytes, str]] = []
    file_list: List[str] = []
    for e in _json:
        if type(e) == int:
            file_list.append(dirname + '/' + str(e))
            with open(dirname + '/' + str(e), 'rb') as file:
                buffer.append(file.read())
                
        else:
            # This one-liner checks that the block 'e' meets the required structure:
            # 1. isinstance(e, list): 'e' must be a list.
            # 2. len(e) == 2: 'e' must have exactly two elements.
            # 3. isinstance(e[0], str): The first element must be a string.
            # 4. isinstance(e[1], list): The second element must be a list.
            # 5. len(e[1]) > 0: That inner list must not be empty.
            # 6. all(isinstance(x, int) for x in e[1]): Every item in the inner list must be an integer.
            # 7. all(e[1][i] <= e[1][i+1] for i in range(len(e[1]) - 1)): The numbers in the inner list must be sorted in ascending order.
            #
            # The entire condition is wrapped in a 'not' so that if any of these requirements are not met,
            # the condition becomes True and a Exception is raised.
            if not (
                isinstance(e, list) and
                len(e) == 2 and
                isinstance(e[0], str) and
                isinstance(e[1], list) and
                len(e[1]) > 0 and
                all(isinstance(x, int) for x in e[1]) and
                all(e[1][i] <= e[1][i+1] for i in range(len(e[1]) - 1))
            ):
                raise Exception('bee-rpc: Invalid block on _.json file.')
            
            block_name = e[0]
            block_lengths = e[1]
            file_list.append(Enviroment.block_dir + block_name)
            buffer.append(Enviroment.block_dir + block_name)

            if block_name not in blocks: blocks[block_name] = [block_lengths]
            else:                        blocks[block_name].append(block_lengths)

            # Measured once, here, and used by both the arithmetic below and the
            # bytes emitted at the end -- the same pointer, so they cannot diverge.
            pointer_lengths[block_lengths[-1]] = block_pointer_length(
                block_id=block_name, omit_types=omit_types)

    debug("Buffer loaded correctly")
    debug("Validate lenghts tree")

    # Raises rather than returning: a caller that cannot be told its object is
    # unusable has no way to stop, retry or report, and the failure surfaces
    # somewhere else entirely.
    validate_lengths_tree(blocks=blocks, file_list=file_list,
                          pointer_lengths=pointer_lengths, debug=debug)

    debug("Create lengths tree")
    tree: Dict[int, Union[Dict, str]] = create_lengths_tree(blocks)

    debug("Compute lengths tree")
    recalculated_lengths: Dict[int, int] = compute_wbp_lengths(
        tree=tree, file_list=file_list, pointer_lengths=pointer_lengths)

    debug("Write wbp file")
    with open(dirname + '/' + WITHOUT_BLOCK_POINTERS_FILE_NAME, 'wb') as f:
        for c in regenerate_buffer(recalculated_lengths, buffer, omit_types=omit_types):
            f.write(c)
