import asyncio
import logging

# Internal imports
from app.format_response import (
    format_bulk_string_success,
    format_resp_array,
    format_null_bulk_string,
    format_simple_error,
)
from app.data_storage import DataStorage
from app.utils.writer_utils import write_and_drain


async def handle_stream_commands(
    writer: asyncio.StreamWriter, command: str, args: list, storage: DataStorage
) -> None:
    """
    Handles stream commands.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        command (str): The command to handle.
        args (list): The arguments provided with the command.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    commands_dict: dict = {
        "XADD": _handle_xadd,
        "XRANGE": _handle_xrange,
    }
    
    handler = commands_dict.get(command.upper())
    if handler:
        await handler(writer, args, storage)
    else:
        logging.info(f"Unknown stream command: {command}")
        await write_and_drain(writer, format_simple_error(f"ERR unknown stream command: {command}"))


async def _handle_xadd(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the XADD command.

    XADD appends one or more values to the end of a stream stored at key.
        If the key does not exist, it is created as an empty stream before performing the push operations.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    args_len: int = len(args)

    id: str = args[1] if args_len > 1 else ""

    # Get all field-value pairs

    field_value_pairs: dict = {}
    for j in range(2, args_len, 2):
        if j + 1 < args_len:
            field_value_pairs[args[j]] = args[j + 1]
        else:
            field_value_pairs[args[j]] = ""

    try:
        entry_id: str = await storage.xadd(key, id, field_value_pairs)
        logging.info(f"XADD: {key}, id: {id}, field-value pairs: {field_value_pairs}")
        writer.write(format_bulk_string_success(entry_id)) # Requires bulk string response
    except ValueError as e:
        logging.error(f"XADD: Error adding entry to stream {key}: {e}")
        writer.write(format_simple_error(e)) # Error response -> Should have ERR in it
        
    await writer.drain()  # Flush write buffer

async def _handle_xrange(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the XRANGE command.

    XRANGE returns the entries in a stream stored at key, with IDs between start and end.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    args_len: int = len(args)

    start: str = args[1] if args_len > 1 else "-"
    end: str = args[2] if args_len > 2 else "+"
    count: int | None = int(args[4]) if args_len > 4 and args[3].upper() == "COUNT" else None

    logging.info(f"XRANGE: {key}, start: {start}, end: {end}, count: {count}")

    # If count is <= 0, no need to query storage, just return null bulk string
    # Null bulk string is what Redis returns in this situation
    if count is not None and count <= 0:
        logging.info(f"XRANGE: Invalid count for {key}: {count}")
        write_and_drain(writer, format_null_bulk_string())
        return

    try:
        entries: list = await storage.xrange(key, start, end, count)

        # Need to return RESP array of arrays
        # Each inner array represents an entry in the stream
        # The first item in the inner array is the entry ID
        # The second item is a list of key values pairs (represented as list of strings)
        # Key value pairs in order they were added to the entry

        response: bytes = b""

        response += b"*" + str(len(entries)).encode("utf-8") + b"\r\n" # RESP array header


        for entry in entries:
            response += b"*" + str(len(entry)).encode("utf-8") + b"\r\n" # Inner array header
            for item in entry:
                if isinstance(item, list):
                    # List of field-value pairs
                    response += format_resp_array(item)
                else:
                    # Entry ID (string)
                    response += format_bulk_string_success(item)

        logging.info(f"XRANGE: Formatted RESP array response: {response}")
        writer.write(response) # RESP array response
        logging.info(f"XRANGE: Wrote array response for {key} with {len(entries)} entries")
    except ValueError as e:
        logging.error(f"XRANGE: Error retrieving entries from stream {key}: {e}")
        writer.write(format_simple_error(e)) # Error response -> Should have ERR in it

    await writer.drain()  # Flush write buffer