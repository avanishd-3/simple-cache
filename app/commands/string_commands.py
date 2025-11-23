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
from app.utils import write_and_drain, WRONG_TYPE_STRING


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
        await write_and_drain(
            writer, format_simple_error(f"ERR unknown string command: {command}")
        )


async def _handle_set(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
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

    expiry_flags = {"EX", "PX", "EXAT", "PXAT", "KEEPTTL"}
    if any(flag in upper_args for flag in expiry_flags):
        if "EX" in upper_args:  # Expiry in seconds
            expiry_amount: int = (
                int(args[upper_args.index("EX") + 1])
                if (upper_args.index("EX") + 1) < len(upper_args)
                else 0
            )

            expiry_time: float | None = time.time() + expiry_amount

        elif "PX" in upper_args:  # Expiry in milliseconds
            expiry_amount = (
                int(args[upper_args.index("PX") + 1])
                if (upper_args.index("PX") + 1) < len(upper_args)
                else 0
            )

            expiry_time = time.time() + (
                expiry_amount / 1000
            )  # Convert milliseconds to seconds

        elif "EXAT" in upper_args:  # Expiry at specific unix time in seconds
            expiry_time = (
                float(args[upper_args.index("EXAT") + 1])
                if (upper_args.index("EXAT") + 1) < len(upper_args)
                else 0.0
            )

        elif "PXAT" in upper_args:  # Expiry at specific unix time in milliseconds
            # Convert milliseconds to seconds to match the rest of the time code
            expiry_time = (
                float(args[upper_args.index("PXAT") + 1]) / 1000
                if (upper_args.index("PXAT") + 1) < len(upper_args)
                else 0.0
            )

        elif "KEEPTTL" in upper_args:  # Keep existing TTL, if any
            existing_ttl = await storage.get_expiry_time(key)

            if existing_ttl is not None:
                expiry_time = existing_ttl
            else:
                expiry_time = None

        # Common stuff
        await storage.set(key, value, expiry_time)

        logging.info(f"Set key with expiry: {key} = {value}, expiry = {expiry_time}")

        await write_and_drain(writer, format_simple_string("OK"))
    else:
        await storage.set(key, value)

        logging.info(f"Set key without expiry: {key} = {value}")

        await write_and_drain(writer, format_simple_string("OK"))


async def _handle_get(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the GET command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    value = await storage.get(key)

    if value is not None:
        if not isinstance(value, str):
            await write_and_drain(
                writer,
                format_simple_error(WRONG_TYPE_STRING),
            )
            logging.info(f"GET: Wrong type for key {key}")
            return
        else:
            await write_and_drain(writer, format_bulk_string_success(value))
            logging.info(f"Sent GET response: {key} = {value}")
    else:
        # Should return null bulk string -> $-1\r\n
        await write_and_drain(writer, format_null_bulk_string())
        logging.info(f"Key {key} not found")