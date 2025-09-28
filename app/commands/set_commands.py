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

async def handle_set_commands(
    writer: asyncio.StreamWriter, command: str, args: list, storage: DataStorage
) -> None:
    """
    Handles set commands.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        command (str): The command to handle.
        args (list): The arguments provided with the command.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    commands_dict: dict = {
        "SADD": _handle_sadd,
        "SCARD": _handle_scard,
    }
    handler = commands_dict.get(command.upper())
    if handler:
        await handler(writer, args, storage)
    else:
        logging.info(f"Unknown set command: {command}")
        await write_and_drain(writer, format_simple_error(f"ERR unknown set command: {command}"))

async def _handle_sadd(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SADD command.

    SADD adds one or more members to a set stored at key.
        If the key does not exist, a new set is created before adding the members.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 2:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sadd' command"))
        return

    key: str = args[0]

    # Get all set members to add
    set_members: list = args[1:] # All args after key

    logging.info(f"SADD: {key} = {set_members}")

    added_count = await storage.sadd(key, set_members)

    await write_and_drain(writer, format_integer_success(added_count))

async def _handle_scard(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SCARD command.

    SCARD returns the set cardinality (number of elements) of the set stored at key.
        If the key does not exist, return0 is returned.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len != 1:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'scard' command"))
        return

    key: str = args[0]

    logging.info(f"SCARD: {key}")

    cardinality = await storage.scard(key)

    await write_and_drain(writer, format_integer_success(cardinality))