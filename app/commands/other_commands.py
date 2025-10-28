import asyncio
import logging
from typing import Literal

# Internal imports
from app.format_response import (
    format_simple_error,
    format_simple_string
)
from app.data_storage import DataStorage
from app.utils import write_and_drain


async def handle_other_commands(
    writer: asyncio.StreamWriter, command: str, args: list, storage: DataStorage
) -> None:
    """
    Handles other commands.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        command (str): The command to handle.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    commands_dict: dict = {
        "FLUSHDB": _handle_flushdb,
    }
    handler = commands_dict.get(command.upper())
    if handler:
        await handler(writer, args, storage)
    else:
        logging.info(f"Unknown other command: {command}")
        await write_and_drain(
            writer, format_simple_error(f"ERR unknown other command: {command}")
        )

async def _handle_flushdb(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the FLUSHDB command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)
    logging.info("Handling FLUSHDB command")

    # Flushing is sync by default for Redis, so copying this behaviour
    method: Literal["SYNC", "ASYNC", ""] | str = args[0] if args_len > 0 else ""

    if method == "": # So logs show when default method is used
        logging.info("FLUSHDB with default method SYNC")
    else:
        logging.info(f"FLUSHDB with method: {method}")

    if method == "ASYNC":
        await storage.flushdb_async()
    else:
        storage.flushdb_sync()

    writer.write(format_simple_string("OK"))

    try:
        await writer.drain()  # Flush write buffer
    except ConnectionError: # This happens in the integration tests (I haven't found it in normal usage yet)
        logging.info("Client disconnected before FLUSHDB response could be sent")