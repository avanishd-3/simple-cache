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
        "SDIFF": _handle_sdiff,
        "SDIFFSTORE": _handle_sdiff_store,
        "SINTER": _handle_sinter,
        "SINTERSTORE": _handle_sinter_store,
        "SUNION": _handle_sunion,
        # "SUNIONSTORE": _handle_sunionstore,
        # "SISMEMBER": _handle_sismember,
        # "SMEMBERS": _handle_smembers,
        # "SMOVE": _handle_smove,
        # "SREM": _handle_srem,
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

async def _handle_sdiff(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SDIFF command.

    SDIFF returns the members of the set resulting from the difference between the first set and all the successive sets.
        If the key does not exist, it is considered an empty set.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 1:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sdiff' command"))
        return

    # Get all keys to perform the difference operation on
    keys: list = args # All args

    logging.info(f"SDIFF: {keys}")

    difference_members = await storage.sdiff(keys)

    if not difference_members:
        await write_and_drain(writer, format_resp_array([])) # No members in set
    else:
        await write_and_drain(writer, format_resp_array(difference_members))

async def _handle_sdiff_store(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SDIFFSTORE command.

    SDIFFSTORE is SDIFF but stores the result in specified destination. If destination already exists, it is overwritten.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 2:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sdiffstore' command"))
        return

    # Get all keys to perform the difference operation on
    destination: str = args[0] # First arg is destination
    keys: list = args[1:] # All args after destination

    logging.info(f"SDIFFSTORE: {keys}")

    difference_members: set = await storage.sdiff(keys)
    await storage.set_overwrite(destination, difference_members)

    # RESP returns the number of members in the resulting set
    if not difference_members:
        await write_and_drain(writer, format_integer_success(0))
    else:
        await write_and_drain(writer, format_integer_success(len(difference_members)))

async def _handle_sinter(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SINTER command.

    SINTER returns the members of the set resulting from the intersection between all the sets.
        If the key does not exist, it is considered an empty set.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 1:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sinter' command"))
        return

    # Get all keys to perform the i operation on
    keys: list = args # All args

    logging.info(f"SINTER: {keys}")

    intersection_members = await storage.sinter(keys)

    if not intersection_members:
        await write_and_drain(writer, format_resp_array([])) # No members in set
    else:
        await write_and_drain(writer, format_resp_array(intersection_members))

async def _handle_sinter_store(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SINTERSTORE command.

    SINTERSTORE is SINTER but stores the result in specified destination. If destination already exists, it is overwritten.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 2:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sinterstore' command"))
        return

    # Get all keys to perform the difference operation on
    destination: str = args[0] # First arg is destination
    keys: list = args[1:] # All args after destination

    logging.info(f"SINTERSTORE: {keys}")

    intersection_members: set = await storage.sinter(keys)
    await storage.set_overwrite(destination, intersection_members)

    # RESP returns the number of members in the resulting set
    if not intersection_members:
        await write_and_drain(writer, format_integer_success(0))
    else:
        await write_and_drain(writer, format_integer_success(len(intersection_members)))

async def _handle_sunion(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SUNION command.

    SUNION returns the members of the set resulting from the union of all the sets.
        If the key does not exist, it is considered an empty set.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 1:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sunion' command"))
        return

    # Get all keys to perform the union operation on
    keys: list = args # All args

    logging.info(f"SUNION: {keys}")

    union_members = await storage.sunion(keys)

    if not union_members:
        await write_and_drain(writer, format_resp_array([])) # No members in set
    else:
        await write_and_drain(writer, format_resp_array(union_members))