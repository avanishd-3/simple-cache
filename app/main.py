import asyncio
import logging
import os
import sys
import signal
import argparse

# Type annotations
from typing import Literal

# Internal imports
from .format_response import (
    format_simple_string,
)

from .data_storage import DataStorage

from .utils.profiler import profile
from .utils.writer_utils import close_writer, write_and_drain
from .utils.conditional_decorator import conditional_decorator
from .utils.command_types import BASIC_COMMANDS, STRING_COMMANDS, LIST_COMMANDS, STREAM_COMMANDS

from .commands.basic_commands import handle_basic_commands
from .commands.string_commands import handle_string_commands
from .commands.list_commands import handle_list_commands
from .commands.stream_commands import handle_stream_commands

# Data
storage_data: DataStorage = DataStorage()

async def redis_parser(data: bytes) -> list[str]:
    # TODO: Make actual parser

    logging.debug(f"Raw data received: {data}")

    command_list = data.decode().strip().split("\r\n")

    # Remove commands with * and $
    # Do not uppercase commands, because some of them contain strings
    command_list = [cmd for cmd in command_list if not (cmd.startswith("*") or cmd.startswith("$"))]

    logging.info(f"Parsed commands: {command_list}")

    return command_list


async def handle_server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    data = None

    while True:
        try:
            data = await reader.read(1024)
        except BrokenPipeError: # Happens when client disconnects abruptly
            logging.info("Client disconnected")
            break
        
        if not data: # Client disconnected (I've not seen this happen, but just in case)
            break

        command_list = await redis_parser(data)

        operation: str = command_list[0].upper() if command_list else ""
        args: list[str] = command_list[1:] if len(command_list) > 1 else []
        args_len: int = len(args)

        logging.info(f"Operation: {operation}, Args: {args}")

        match operation:
            case "SHUTDOWN":
                # Initiate server shutdown
                # Do not close tasks here, as this would interfere with KeyboardInterrupt handling

                # No need to drain writer here, since previous command would have done that
                logging.info("Shutdown command received...")
                logging.info("Closing connection with client...")

                # Unblock any blocked clients before shutting down
                logging.info("Unblocking all blocked clients...")
                await storage_data.unblock_all_blocked_clients()

                # Cancel all other tasks except current one
                # So blpop does not raise exception when blocked info is removed
                logging.info("Cancelling all other tasks...")
                for task in asyncio.all_tasks():
                    logging.info(f"Current task: {task}")
                    if task is not asyncio.current_task():
                        logging.info("Cancelling task...")
                        task.cancel()

                # Close connection with client
                await close_writer(writer)

                logging.info("Killing self with SIGINT to stop the server gracefully...")
                # Send SIGINT to self to stop the server gracefully
                # This is kind of a hack, but raising asyncio.CancelledError here doesn't work properly
                # KeyboardInterrupt is handled properly in main, so this works fine
                os.kill(os.getpid(), signal.SIGINT)
                break # It sends the unknown command response here otherwise

            case cmd if cmd in BASIC_COMMANDS:
                logging.info(f"Handling basic command: {cmd}")
                await handle_basic_commands(writer, cmd, args, storage_data)

            case cmd if cmd in STRING_COMMANDS:
                logging.info(f"Handling string command: {cmd}")
                await handle_string_commands(writer, cmd, args, storage_data)

            case cmd if cmd in LIST_COMMANDS:
                logging.info(f"Handling list command: {cmd}")
                await handle_list_commands(writer, cmd, args, storage_data)

            case cmd if cmd in STREAM_COMMANDS:
                logging.info(f"Handling stream command: {cmd}")
                await handle_stream_commands(writer, cmd, args, storage_data)

            case "FLUSHDB":
                logging.info("Handling FLUSHDB command")

                # Flushing is sync by default for Redis, so copying this behaviour
                method: Literal["SYNC", "ASYNC", ""] = args[0] if args_len > 0 else ""

                if method == "": # So logs show when default method is used
                    logging.info("FLUSHDB with default method SYNC")
                else:
                    logging.info(f"FLUSHDB with method: {method}")

                if method == "ASYNC":
                    await storage_data.flushdb_async()
                else:
                    storage_data.flushdb_sync()
                writer.write(format_simple_string("OK"))
                try:
                    await writer.drain()  # Flush write buffer
                except ConnectionError: # This happens in the integration tests (I haven't found it in normal usage yet)
                    logging.info("Client disconnected before FLUSHDB response could be sent")
            case _:
                await write_and_drain(writer, b"-Error: Unknown command\r\n")
                logging.info(f"Sent error response for unknown command: {operation}")

    # This is just the client disconnecting -> do not shut down server if this happens
    logging.info("Closing connection with client")
    try:
        await close_writer(writer)
    except BrokenPipeError: # Happens when client disconnects abruptly
        logging.info("Client disconnected before connection could be closed")

def _parse_args(args):
    # Allow selecting port from cli arg
    parser = argparse.ArgumentParser(description="Start the simple-cache server.")
    parser.add_argument(
        "--port", type=int, default=6379, help="Port number to run the server on (default: 6379)"
    )
    parser.add_argument(
        "--debug", action="store_true", default=False, help="Enable debug logging (default: False)"
    )
    return parser.parse_args()

@conditional_decorator(profile(output_file="profile_stats.prof"), condition=False) # Change to True to enable profiling
async def main(port: int = 6379, debug: bool = False) -> None:
    """
    Starts the asyncio server on localhost:6379

    Notes: 
       1. uvloop performed worse than default asyncio event loop in benchmarks. Do not use it.
       2. Most of the runtime of the program is spent in asyncio selector, so rewrite to Rust or Go once API is stable
    """

    if debug:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug("Debug logging enabled")
    else: # Because info is being used for variable state (no trace level in Python)
        logging.basicConfig(level=logging.INFO)

    logging.critical(f"Starting server on localhost:{port}")
    logging.critical(f"Process ID: {os.getpid()}")

    server = await asyncio.start_server(handle_server, "localhost", port) # Client function called whenever client sends a message

    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        logging.info("Server serve_forever cancelled, shutting down server...")
        # Close server here to handle KeyboardInterrupt properly
        server.close()
        await server.wait_closed()
        
        logging.info("Server shut down successfully")

if __name__ == "__main__":
    parser = _parse_args(sys.argv[1:])
    asyncio.run(main(port=parser.port, debug=parser.debug))