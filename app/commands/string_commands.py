import asyncio
import logging
import time


# Internal imports
from app.format_response import (
    format_simple_string,
    format_bulk_string_success,
    format_null_bulk_string,
    format_simple_error,
)
from app.data_storage import DataStorage
from app.utils.writer_utils import write_and_drain

async def handle_string_commands(
    writer: asyncio.StreamWriter, command: str, args: list, storage: DataStorage
) -> None:
    """
    Handles string commands.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        command (str): The command to handle.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    commands_dict: dict = {
        "SET": _handle_set,
        "GET": _handle_get,
    }
    handler = commands_dict.get(command.upper())
    if handler:
        await handler(writer, args, storage)
    else:
        logging.info(f"Unknown string command: {command}")
        await write_and_drain(writer, format_simple_error(f"ERR unknown string command: {command}"))


async def _handle_set(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SET command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    value: str = args[1] if len(args) > 1 else ""

    upper_args: list = [arg.upper() for arg in args]

    # Expiry
    if "PX" in upper_args:
        expiry_amount: int = int(args[upper_args.index("PX") + 1]) if (upper_args.index("PX") + 1) < len(upper_args) else 0

        expiry_time: float = time.time() + (expiry_amount / 1000)  # Convert milliseconds to seconds

        await storage.set(key, value, expiry_time)

        logging.info(f"Set key with expiry: {key} = {value}, expiry = {expiry_time}")

    else:
        await storage.set(key, value)

        logging.info(f"Set key without expiry: {key} = {value}")

    await write_and_drain(writer, format_simple_string("OK"))

async def _handle_get(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the GET command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    value: str | None = await storage.get(key)

    if value is not None:
        writer.write(format_bulk_string_success(value))
        logging.info(f"Sent GET response: {key} = {value}")
    else:
        # Should return null bulk string -> $-1\r\n
        writer.write(format_null_bulk_string()) # Null bulk string (shows key doesn't exist)
        logging.info(f"Key {key} not found")

    await writer.drain()  # Flush write buffer