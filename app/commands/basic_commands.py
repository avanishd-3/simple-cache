import asyncio
import logging

from typing import Type

# Internal imports
from app.format_response import (
    format_simple_string,
    format_bulk_string_success,
    format_integer_success,
    format_simple_error,
)
from app.data_storage import DataStorage
from app.utils import write_and_drain
from app.utils import OrderedSet


async def handle_basic_commands(
    writer: asyncio.StreamWriter, command: str, args: list, storage: DataStorage
) -> None:
    """
    Handles basic commands.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        command (str): The command to handle.
        args (list): The arguments provided with the command.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    commands_dict: dict = {
        "PING": _handle_ping,
        "ECHO": _handle_echo,
        "TYPE": _handle_type,
        "EXISTS": _handle_exists,
        "DEL": _handle_del,
    }
    handler = commands_dict.get(command.upper())
    if handler:
        await handler(writer, args, storage)
    else:
        logging.info(f"Unknown basic command: {command}")
        await write_and_drain(
            writer, format_simple_error(f"ERR unknown basic command: {command}")
        )


async def _handle_ping(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the PING command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    if len(args) > 1:
        logging.info("Wrong number of arguments for PING command")
        writer.write(
            format_simple_error("ERR wrong number of arguments for 'ping' command")
        )
    elif len(args) == 1:
        message: str = args[0]
        logging.info(f"Sent PING response with message: {message}")
        writer.write(format_simple_string(message))
    else:
        logging.info("Sent PONG response")
        writer.write(format_simple_string("PONG"))

    await writer.drain()


async def _handle_echo(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the ECHO command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    if len(args) != 1:
        logging.info("Wrong number of arguments for ECHO command")
        writer.write(
            format_simple_error("ERR wrong number of arguments for 'echo' command")
        )
    else:
        message: str = args[0]
        logging.info(f"Sent ECHO response with message: {message}")
        writer.write(format_bulk_string_success(message))

    await writer.drain()


async def _handle_type(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the TYPE command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""

    key_type: Type[None | str | list | dict | OrderedSet] | None = await storage.key_type(key)

    logging.info(f"TYPE: {key} is of type {key_type}")

    if key_type is None:
        logging.info(f"Sent TYPE unknown for key {key}")
        writer.write(format_simple_string("unknown"))
    elif key_type is type(None):
        logging.info(f"Sent TYPE none for key {key}")
        writer.write(format_simple_string("none"))
    elif key_type is str:
        logging.info(f"Sent TYPE string for key {key}")
        writer.write(format_simple_string("string"))
    elif key_type is list:
        logging.info(f"Sent TYPE list for key {key}")
        writer.write(format_simple_string("list"))
    elif key_type is dict:
        logging.info(f"Sent TYPE stream for key {key}")
        writer.write(format_simple_string("stream"))
    elif key_type is OrderedSet:
        logging.info(f"Sent TYPE set for key {key}")
        writer.write(format_simple_string("set"))
    else:  # TODO: Remove this when type is fully implemented
        logging.info(f"Sent TYPE unknown for key {key}")
        writer.write(format_simple_string("unknown"))

    await writer.drain()  # Flush write buffer


async def _handle_exists(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the EXISTS command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """

    keys: list[str] = args if len(args) > 0 else []

    logging.info(f"EXISTS: keys {keys}")

    num_existing_keys: int = 0
    for key in keys:
        num_existing_keys += 1 if await storage.exists(key) else 0

    writer.write(format_integer_success(num_existing_keys))
    await writer.drain()  # Flush write buffer


async def _handle_del(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the DEL command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """

    keys: list[str] = args if len(args) > 0 else []

    logging.info(f"DEL: keys {keys}")

    num_deleted_keys: int = 0
    for key in keys:
        num_deleted_keys += 1 if await storage.delete(key) else 0

    writer.write(format_integer_success(num_deleted_keys))
    await writer.drain()  # Flush write buffer
