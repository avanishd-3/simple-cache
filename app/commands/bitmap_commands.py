import asyncio
import logging


# Internal imports
from app.format_response import (
    format_integer_success,
    format_simple_error,
)
from app.data_storage import DataStorage
from app.utils import write_and_drain, WRONG_TYPE_STRING

from app.data_storage import ValueWithExpiry



async def handle_bitmap_commands(
    writer: asyncio.StreamWriter, command: str, args: list, storage: DataStorage
) -> None:
    """
    Handles bitmap commands.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        command (str): The command to handle.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    commands_dict: dict = {
        "SETBIT": _handle_setbit,
        "GETBIT": _handle_getbit,
        "BITCOUNT": _handle_bitcount,
    }
    handler = commands_dict.get(command.upper())
    if handler:
        await handler(writer, args, storage)
    else:
        logging.info(f"Unknown bitmap command: {command}")
        await write_and_drain(
            writer, format_simple_error(f"ERR unknown bitmap command: {command}")
        )


async def _handle_setbit(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the SETBIT command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    offset: str = args[1] if len(args) > 1 else ""
    value: str = args[2] if len(args) > 2 else ""

    # If the key exists and is not a string, return an error
    if key in storage.storage_dict and not isinstance(storage.storage_dict[key].value, str):
        logging.info(f"Key {key} exists and is not a string")
        await write_and_drain(writer, format_simple_error(WRONG_TYPE_STRING))
        return
    
    # If key does not exist, create it as an 1 byte string (which is the minimum size for a bitmap)
    if key not in storage.storage_dict:
        storage.storage_dict[key] = ValueWithExpiry("\x00", None)  # Initialize with a single null byte
        logging.info(f"Created new key: {key} as an empty string")

    # Convert offset and value to integers
    try:
        offset_int: int = int(offset)
        value_int: int = int(value)

        # Validate the offset for SETBIT command
        if offset_int < 0:
            logging.info(f"Invalid offset for SETBIT: {offset_int}")
            await write_and_drain(writer, format_simple_error("ERR bit offset is not an integer or out of range"))
            return

        # Validate the value for SETBIT command
        if value_int not in (0, 1):
            logging.info(f"Invalid value for SETBIT: {value_int}")
            await write_and_drain(writer, format_simple_error("ERR bit is not an integer or out of range"))
            return
        
        # Calculate the byte index and bit position
        byte_index: int = offset_int // 8
        bit_position: int = offset_int % 8

        # Ensure the string is long enough to accommodate the offset
        current_length: int = len(storage.storage_dict[key].value)
        if byte_index >= current_length:
            # Extend the string with null bytes if necessary
            async with storage.lock:
                storage.storage_dict[key] = ValueWithExpiry(
                    storage.storage_dict[key].value + "\x00" * (byte_index - current_length + 1),
                    storage.storage_dict[key].expiry_time
                )
            logging.info(f"Extended key {key} to accommodate offset {offset_int}")

        # Get the current byte and modify the specific bit
        current_byte: int = ord(storage.storage_dict[key].value[byte_index])
        if value_int == 1:
            new_byte: int = current_byte | (1 << (7 - bit_position))  # Set the bit
        else:
            new_byte: int = current_byte & ~(1 << (7 - bit_position))  # Clear the bit
            logging.info(f"Clearing bit {bit_position} in key {key}")
        
        # Update the string with the new byte
        async with storage.lock:
            storage.storage_dict[key] = ValueWithExpiry(
                storage.storage_dict[key].value[:byte_index]
                + chr(new_byte)
                + storage.storage_dict[key].value[byte_index + 1:],
                storage.storage_dict[key].expiry_time
            )

        logging.info(f"SETBIT command executed: key={key}, offset={offset_int}, value={value_int}")

        await write_and_drain(writer, format_integer_success(str(current_byte >> (7 - bit_position) & 1)))
    except ValueError:
        # Base error based on if the offset or value is invalid

        try:
            int(offset)
        except ValueError:
            logging.info(f"Invalid offset: {offset}")
            await write_and_drain(writer, format_simple_error("ERR bit offset is not an integer or out of range"))
            return
        
        logging.info(f"Invalid value: {value}")
        await write_and_drain(writer, format_simple_error("ERR bit is not an integer or out of range"))
        return
    
async def _handle_getbit(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the GETBIT command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    offset: str = args[1] if len(args) > 1 else ""

    # If the key exists and is not a string, return an error
    if key in storage.storage_dict and not isinstance(storage.storage_dict[key].value, str):
        logging.info(f"Key {key} exists and is not a string")
        await write_and_drain(writer, format_simple_error(WRONG_TYPE_STRING))
        return

    # If the key does not exist, return 0
    if key not in storage.storage_dict:
        logging.info(f"Key {key} does not exist. Returning 0 for GETBIT.")
        await write_and_drain(writer, format_integer_success("0"))
        return

    # Convert offset to integer
    try:
        offset_int: int = int(offset)

        # Validate the offset for GETBIT command
        if offset_int < 0:
            logging.info(f"Invalid offset for GETBIT: {offset_int}")
            await write_and_drain(writer, format_simple_error("ERR bit offset is not an integer or out of range"))
            return
        
        # Calculate the byte index and bit position
        byte_index: int = offset_int // 8
        bit_position: int = offset_int % 8

        # Check if the byte index is within the bounds of the string
        current_length: int = len(storage.storage_dict[key].value)
        if byte_index >= current_length:
            logging.info(f"Offset {offset_int} exceeds length of key {key}. Returning 0 for GETBIT.")
            await write_and_drain(writer, format_integer_success("0"))
            return

        # Get the current byte and extract the specific bit
        current_byte: int = ord(storage.storage_dict[key].value[byte_index])
        bit_value: int = (current_byte >> (7 - bit_position)) & 1

        logging.info(f"GETBIT command executed: key={key}, offset={offset_int}, value={bit_value}")
        await write_and_drain(writer, format_integer_success(str(bit_value)))
    except ValueError:
        logging.info(f"Invalid offset: {offset}")
        await write_and_drain(writer, format_simple_error("ERR bit offset is not an integer or out of range"))
        return
    
async def _handle_bitcount(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the BITCOUNT command.

    Start and end indices are inclusive.

    Negative indices are supported and count from the end of the list. Ex: -1 is last element, -2 is second-last element, and
    so on.

    If negative index is >= length of list, it is treated as 0.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    start: str = args[1] if len(args) > 1 else ""
    end: str = args[2] if len(args) > 2 else ""
    bit = args[3] if len(args) > 3 else None

    # If the key exists and is not a string, return an error
    if key in storage.storage_dict and not isinstance(storage.storage_dict[key].value, str):
        logging.info(f"Key {key} exists and is not a string")
        await write_and_drain(writer, format_simple_error(WRONG_TYPE_STRING))
        return

    # If the key does not exist, return 0
    if key not in storage.storage_dict:
        logging.info(f"Key {key} does not exist. Returning 0 for BITCOUNT.")
        await write_and_drain(writer, format_integer_success("0"))
        return

    # Adjust negative indices
    if str(bit).upper() == "BIT":
        # start and end are bit offsets, so we need to convert them to byte offsets
        start = start // 8
        end = end // 8

    if start == "" and end == "":
        start = 0
        end = len(storage.storage_dict[key].value) - 1
    else:
        try:
            start = int(start)
            end = int(end)
        except ValueError:
            logging.info(f"Invalid start or end: {start}, {end}")
            await write_and_drain(writer, format_simple_error("ERR value is not an integer or out of range"))
            return

    if start < 0:
        start = max(0, len(storage.storage_dict[key].value) + start)
    if end == 0:
        end = len(storage.storage_dict[key].value) - 1
    if end < 0:
        end = max(0, len(storage.storage_dict[key].value) + end)

    if end < start:
        logging.info(f"End index {end} is less than start index {start}. Returning 0 for BITCOUNT.")
        await write_and_drain(writer, format_integer_success("0"))
        return
    
    # Binary 

    bit_count: int = sum(bin(ord(byte)).count('1') for byte in storage.storage_dict[key].value)

    logging.info(f"BITCOUNT command executed: key={key}, count={bit_count}")
    await write_and_drain(writer, format_integer_success(str(bit_count)))