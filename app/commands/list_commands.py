import asyncio
import logging

# Internal imports
from app.format_response import (
    format_bulk_string_success,
    format_integer_success,
    format_resp_array,
    format_null_bulk_string,
    format_simple_error,
)
from app.data_storage import DataStorage
from app.utils.writer_utils import write_and_drain

async def handle_list_commands(
    writer: asyncio.StreamWriter, command: str, args: list, storage: DataStorage
) -> None:
    """
    Handles list commands.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        command (str): The command to handle.
        args (list): The arguments provided with the command.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    commands_dict: dict = {
        "RPUSH": _handle_rpush,
        "LPUSH": _handle_lpush,
        "LLEN": _handle_llen,
        "LRANGE": _handle_lrange,
        "LPOP": _handle_lpop,
        "BLPOP": _handle_blpop,
    }
    handler = commands_dict.get(command.upper())
    if handler:
        await handler(writer, args, storage)
    else:
        logging.info(f"Unknown list command: {command}")
        await write_and_drain(writer, format_simple_error(f"ERR unknown list command: {command}"))


async def _handle_rpush(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the RPUSH command.

    RPUSH appends one or more values to the end of a list stored at key.
        If the key does not exist, it is created as an empty list before performing the push operations.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""

    # Get all list elements to append
    list_elements: list = args[1:] # All args after key

    logging.info(f"RPUSH: {key} = {list_elements}")

    list_len = await storage.rpush(key, list_elements)

    await write_and_drain(writer, format_integer_success(list_len))

async def _handle_lpush(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the LPUSH command.

    LPUSH prepends one or more values to the beginning of a list stored at key.
        If the key does not exist, it is created as an empty list before performing the push operations.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""

    # Get all list elements to prepend
    list_elements: list = args[1:] # All args after key

    logging.info(f"LPUSH: {key} = {list_elements}")

    list_len = await storage.lpush(key, list_elements)

    await write_and_drain(writer, format_integer_success(list_len))

async def _handle_llen(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the LLEN command.

    LLEN returns the length of the list stored at key.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""

    logging.info(f"LLEN: {key}")

    length: int = await storage.llen(key)
    await write_and_drain(writer, format_integer_success(length))

async def _handle_lrange(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the LRANGE command.

    LRANGE returns a range of elements from a list stored at key.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    start: int = int(args[1]) if len(args) > 1 else 0
    end: int = int(args[2]) if len(args) > 2 else -1

    logging.info(f"LRANGE: {key}, start: {start}, end: {end}")

    elements = await storage.lrange(key, start, end)
    await write_and_drain(writer, format_resp_array(elements))

async def _handle_lpop(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the LPOP command.

    LPOP removes elements from the left.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""

    number_to_pop: int = int(args[1]) if len(args) > 1 else 1

    logging.info(f"LPOP: {key}, count: {number_to_pop}")

    value: list | None = await storage.lpop(key, number_to_pop)

    if value is None:
        writer.write(format_null_bulk_string())
    else:
        if len(value) == 1:
            # RESP expects bulk string for only 1 value popped
            writer.write(format_bulk_string_success(value[0]))
        else:
            # RESP expects array if multiple values popped
            writer.write(format_resp_array(value))
    await writer.drain()  # Flush write buffer

async def _handle_blpop(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the BLPOP command.

    BLPOP removes elements from the left with blocking.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    blocking_time: float = float(args[1]) if len(args) > 1 else 0

    logging.info(f"BLPOP: {key}, blocking time: {blocking_time}")

    # TODO -> Use Pydantic to validate input schema
    result: dict[str, list | None] = await storage.blpop(key, blocking_time)

    if result is None:
        # Unable to pop from specified list
        logging.info(f"BLPOP: {key} timed out after {blocking_time} seconds")
        writer.write(format_null_bulk_string())
    else:
        # List name and removed item are array of bulk strings
        writer.write(format_resp_array([result["list_name"], result["removed_item"]]))
        logging.info(f"BLPOP: Wrote array response for {key} -> [{result['list_name']}, {result['removed_item']}]")

    await writer.drain()  # Flush write buffer