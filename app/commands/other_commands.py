import asyncio
import logging
from typing import Literal
import time

# Internal imports
from app.format_response import (
    format_simple_error,
    format_simple_string,
    format_integer_success,
)
from app.data_storage import DataStorage
from app.utils import write_and_drain, NOT_AN_INTEGER


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
        "TTL": _handle_ttl,
        "EXPIRE": _handle_expire,
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

async def _handle_ttl(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the TTL command. TTL returns how many seconds until a key expires.

    Return values:
        - TTL in seconds if the key exists and has an expiry
        - -1 if the key exists but has no expiry
        - -2 if the key does not exist
    """
    if len(args) != 1:
        logging.info("TTL command received with incorrect number of arguments")
        await write_and_drain(
            writer, format_simple_error("ERR wrong number of arguments for 'ttl' command")
        )
        return

    key = args[0]
    item = await storage.get(key)
    expiry_time = await storage.get_expiry_time(key)
    if item is None: # Redis returns -2 if the key does not exist
        logging.info(f"TTL command: key '{key}' does not exist")
        await write_and_drain(writer, format_integer_success(-2))
    elif expiry_time is None: # Redis returns -1 if the key exists but has no expiry
        logging.info(f"TTL command: key '{key}' exists but has no expiry")
        await write_and_drain(writer, format_integer_success(-1))
    else: # Key exists and has an expiry
        ttl_seconds = int(expiry_time - time.time())
        logging.info(f"TTL command: key '{key}' has TTL of {ttl_seconds} seconds")
        await write_and_drain(writer, format_integer_success(ttl_seconds))

async def _handle_expire(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the EXPIRE command.

    Optional arguments:
        NX - Set expiry only if the key has no expiry
        XX - Set expiry only if the key has an existing expiry
        GT - Set expiry only if the new expiry is greater than current one
        LT - Set expiry only if the new expiry is less than current one

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    if len(args) != 2 and len(args) != 3: # 2 required args, 1 optional
        logging.info("EXPIRE command received with incorrect number of arguments")
        await write_and_drain(
            writer, format_simple_error("ERR wrong number of arguments for 'expire' command")
        )
        return

    key = args[0]
    try:
        seconds = int(args[1])
    except ValueError:
        logging.info("EXPIRE command received with non-integer expiration time")
        await write_and_drain(
            writer, format_simple_error(NOT_AN_INTEGER)
        )
        return
    
    upper_args = [arg.upper() for arg in args]

    item = await storage.get(key)
    if item is None:
        logging.info(f"EXPIRE command: key '{key}' does not exist")
        await write_and_drain(writer, format_integer_success(0))
        return
    elif item is not None:

        expiry_flags = {"NX", "XX", "GT", "LT"}
        if any(flag in upper_args for flag in expiry_flags):
            if "NX" in upper_args:  # Only expire when key has no expiry
                existing_expiry_time = await storage.get_expiry_time(key)
                if existing_expiry_time is None:
                    logging.info(f"EXPIRE command: key '{key}' has no expiry, NX flag present")
                    await storage.set_ttl(key, time.time() + seconds)
                    await write_and_drain(writer, format_integer_success(1))
                    return
                else:
                    logging.info(f"EXPIRE command: key '{key}' has existing expiry, NX flag present")
                    await write_and_drain(writer, format_integer_success(0))
                    return

            elif "XX" in upper_args:  # Only expire when key has existing expiry
                existing_expiry_time = await storage.get_expiry_time(key)
                if existing_expiry_time is not None:
                    logging.info(f"EXPIRE command: key '{key}' has existing expiry, XX flag present")
                    await storage.set_ttl(key, time.time() + seconds)
                    await write_and_drain(writer, format_integer_success(1))
                    return
                else:
                    logging.info(f"EXPIRE command: key '{key}' has no expiry, XX flag present")
                    await write_and_drain(writer, format_integer_success(0))
                    return
                
            elif "GT" in upper_args:  # Only expire when new expiry is greater than current one
                existing_expiry_time = await storage.get_expiry_time(key)
                # No TTL = infinite time, so any new expiry is less than infinite time
                if existing_expiry_time is not None and (time.time() + seconds) > existing_expiry_time:
                    logging.info(f"EXPIRE command: key '{key}' new expiry greater than current, GT flag present")
                    await storage.set_ttl(key, time.time() + seconds)
                    await write_and_drain(writer, format_integer_success(1))
                    return
                else:
                    logging.info(f"EXPIRE command: key '{key}' new expiry not greater than current, GT flag present")
                    await write_and_drain(writer, format_integer_success(0))
                    return
                
            elif "LT" in upper_args:  # Only expire when new expiry is less than current one
                existing_expiry_time = await storage.get_expiry_time(key)
                # No TTL = infinite time, so any new expiry is less than infinite time
                if existing_expiry_time is None or (time.time() + seconds) < existing_expiry_time:
                    logging.info(f"EXPIRE command: key '{key}' new expiry less than current, LT flag present")
                    await storage.set_ttl(key, time.time() + seconds)
                    await write_and_drain(writer, format_integer_success(1))
                    return
                else:
                    logging.info(f"EXPIRE command: key '{key}' new expiry not less than current, LT flag present")
                    await write_and_drain(writer, format_integer_success(0))
                    return
        else: # No flags, just set the expiry
            logging.info(f"EXPIRE command: setting expiry for key '{key}' without flags")
            await storage.set_ttl(key, time.time() + seconds)
            await write_and_drain(writer, format_integer_success(1))
            return