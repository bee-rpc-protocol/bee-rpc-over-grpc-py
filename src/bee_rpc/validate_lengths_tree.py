from typing import Callable, Dict, List

from bee_rpc.utils import LengthsValidationError, get_varint_at_position, get_pruned_block_length


def validate_lengths_tree(
        blocks: Dict[str, List[List[int]]],
        file_list: List[str],
        pointer_lengths: Dict[int, int],
        debug: Callable[[str], None] = lambda s: None,
) -> None:
    """Check that a stored object's metadata describes the bytes it sits on.

    Every block the metadata points at is reached through a varint that must state
    how far that block expands. Reading something else there means the metadata and
    the parts have drifted apart -- the position arithmetic is landing in the middle
    of file content -- and nothing built from them afterwards would be meaningful.

    Raises LengthsValidationError with what it found. This used to print the same
    numbers to stdout and then call `exit()`, which killed the caller's process from
    inside a library: no traceback, no chance to clean up, and for a caller reading
    that stdout, the diagnosis interleaved with whatever else it was printing.

    Args:
        blocks: block name -> the pointer chains that reach it.
        file_list: the parts and blocks the object expands to, in order.
        pointer_lengths: how long the pointer written at each position is.
        debug: where the running commentary goes, instead of stdout.
    """
    debug(f"Blocks: {blocks}")

    for block, pointer_lists in blocks.items():
        for pointer_list in pointer_lists:
            block_index_position = pointer_list[-1]
            position_length = get_varint_at_position(block_index_position, file_list=file_list)
            block_length = get_pruned_block_length(
                block_name=block, pointer_length=pointer_lengths[block_index_position])

            if block_length > position_length:
                raise LengthsValidationError(
                    f"bee-rpc: the metadata of this object does not describe its parts. "
                    f"Block '{block}' is reached through the varint at position "
                    f"{block_index_position}, which states {position_length}, but the block "
                    f"expands to {block_length} beyond its pointer. Pointer chain: "
                    f"{pointer_list}."
                )
