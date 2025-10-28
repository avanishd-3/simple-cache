import asyncio
import logging

# Internal imports
from app.format_response import (
    format_simple_error,
    format_integer_success,
)
from app.data_storage import DataStorage
from app.utils import write_and_drain, WRONG_TYPE_STRING, INCR_NON_INTEGER


async def handle_transaction_commands(
    writer: asyncio.StreamWriter, command: str, args: list, storage: DataStorage
) -> None:
    """
    Handles transaction commands.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        command (str): The command to handle.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    commands_dict: dict = {
        "INCR": _handle_incr,
    }
    handler = commands_dict.get(command.upper())
    if handler:
        await handler(writer, args, storage)
    else:
        logging.info(f"Unknown transaction command: {command}")
        await write_and_drain(
            writer, format_simple_error(f"ERR unknown transaction command: {command}")
        )

async def _handle_incr(
    writer: asyncio.StreamWriter, args: list, storage: DataStorage
) -> None:
    """
    Handles the INCR command.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    key: str = args[0] if len(args) > 0 else ""
    value = await storage.get(key)

    if value is None:
        new_value = 1 # This is how Redis handles this
    else:
        if isinstance(value, str) and not value.isdigit():
            await write_and_drain(
                writer,
                format_simple_error(INCR_NON_INTEGER),
            )
            logging.info(f"INCR: Non-integer value for key {key}")
            return
        elif not isinstance(value, str) or not value.isdigit():
            await write_and_drain(
                writer,
                format_simple_error(WRONG_TYPE_STRING),
            )
            logging.info(f"INCR: Wrong type for key {key}")
            return
        new_value = int(value) + 1

    await storage.set(key, str(new_value))
    await write_and_drain(writer, format_integer_success(new_value))
    logging.info(f"INCR: {key} incremented to {new_value}")