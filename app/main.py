import asyncio
import logging
import time
import os
import signal

# Type annotations
from typing import Type, Literal

# Internal imports
from .format_response import (
    format_simple_string,
    format_bulk_string_success,
    format_integer_success,
    format_resp_array,
    format_null_bulk_string,
    format_simple_error,
)

from .data_storage import DataStorage

from .utils.profiler import profile
from .utils.writer_utils import close_writer
from .utils.conditional_decorator import conditional_decorator

logging.basicConfig(level=logging.INFO)

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

        if operation == "SHUTDOWN": 
            # Initiate server shutdown
            # Do not close tasks here, as this would interfere with KeyboardInterrupt handling

            # No need to drain writer here, since previous command would have done that
            logging.info("Shutdown command received...")
            logging.info("Closing connection with client...")
            await close_writer(writer)

            logging.info("Killing self with SIGINT to stop the server gracefully...")
            # Send SIGINT to self to stop the server gracefully
            # This is kind of a hack, but raising asyncio.CancelledError here doesn't work properly
            # KeyboardInterrupt is handled properly in main, so this works fine
            os.kill(os.getpid(), signal.SIGINT)
            break # It sends the unkown command response here otherwise

        command_list_len: int = len(command_list)
        i: int = 0

        while i < command_list_len:
            curr_command = command_list[i]

            logging.info(f"Received command: {curr_command}")

            match curr_command.upper():
                case "PING":
                    writer.write(format_simple_string("PONG"))
                    await writer.drain()  # Flush write buffer

                    logging.info("Sent PONG response")

                    i += 1  # Move to next command
                case "ECHO":
                    msg: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    writer.write(format_bulk_string_success(msg))
                    await writer.drain()  # Flush write buffer

                    logging.info(f"Sent ECHO response: {msg}")

                    i += 2  # Move to next command

                case "SET":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    value: str = command_list[i + 2] if i + 2 < command_list_len else ""

                    # Expiry
                    if i + 4 < command_list_len and command_list[i + 3].upper() == "PX":
                        expiry_amount: int = int(command_list[i + 4])

                        expiry_time: float = time.time() + (expiry_amount / 1000)  # Convert milliseconds to seconds

                        await storage_data.set(key, value, expiry_time)

                        logging.info(f"Set key with expiry: {key} = {value}, expiry = {expiry_time}")

                        i += 5

                    else:
                        await storage_data.set(key, value)
                        i += 3

                        logging.info(f"Set key without expiry: {key} = {value}")

                    writer.write(format_simple_string("OK"))
                    await writer.drain()  # Flush write buffer

                case "GET":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""

                    value: str | None = await storage_data.get(key)

                    if value is not None:
                        writer.write(format_bulk_string_success(value))
                        logging.info(f"Sent GET response: {key} = {value}")
                    else:
                        # Should return null bulk string -> $-1\r\n
                        writer.write(format_null_bulk_string()) # Null bulk string (shows key doesn't exist)
                        logging.info(f"Key {key} not found")

                    await writer.drain()  # Flush write buffer

                    i += 2  # Move to next command

                case "TYPE":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""

                    key_type: Type[None | str | list] = await storage_data.key_type(key)

                    logging.info(f"TYPE: {key} is of type {key_type}")

                    if key_type is Type[None]:
                        logging.info(f"Sent TYPE none for key {key}")
                        writer.write(format_simple_string("none"))
                    elif key_type is Type[str]:
                        logging.info(f"Sent TYPE string for key {key}")
                        writer.write(format_simple_string("string"))
                    elif key_type is Type[list]:
                        logging.info(f"Sent TYPE list for key {key}")
                        writer.write(format_simple_string("list"))
                    elif key_type is Type[dict]:
                        logging.info(f"Sent TYPE stream for key {key}")
                        writer.write(format_simple_string("stream"))
                    else: # TODO: Remove this when type is fully implemented
                        logging.info(f"Sent TYPE unknown for key {key}")
                        writer.write(format_simple_string("unknown"))

                    await writer.drain()  # Flush write buffer
                    i += 2  # Move to next command

                # Appends elements to a list
                # List is created first if it doesn't exist
                case "RPUSH":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""

                    increment_num: int = 2 # How much to move forward in commands list (based on number of elements)

                    # Get all list elements to append
                    list_elements: list = []
                    for j in range(i + 2, command_list_len):
                        list_elements.append(command_list[j])
                        increment_num += 1

                    logging.info(f"RPUSH: {key} = {list_elements}")

                    list_len = await storage_data.rpush(key, list_elements)

                    writer.write(format_integer_success(list_len))
                    await writer.drain()  # Flush write buffer

                    i += increment_num  # Move to next command

                # Prepends elements to a list
                # List is created first if it doesn't exist
                case "LPUSH":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""

                    increment_num: int = 2 # How much to move forward in commands list (based on number of elements)

                    # Get all list elements to append
                    list_elements: list = []
                    for j in range(i + 2, command_list_len):
                        list_elements.append(command_list[j])
                        increment_num += 1

                    logging.info(f"LPUSH: {key} = {list_elements}")

                    list_len = await storage_data.lpush(key, list_elements)

                    writer.write(format_integer_success(list_len))
                    await writer.drain()  # Flush write buffer

                    i += increment_num  # Move to next command

                # Get length of a list
                case "LLEN":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""

                    logging.info(f"LLEN: {key}")

                    length: int = await storage_data.llen(key)
                    writer.write(format_integer_success(length))
                    await writer.drain()  # Flush write buffer

                    i += 2

                # Retrieve a range of elements from a list
                case "LRANGE":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    start: int = int(command_list[i + 2]) if i + 2 < command_list_len else 0
                    end: int = int(command_list[i + 3]) if i + 3 < command_list_len else -1

                    logging.info(f"LRANGE: key {key}, start {start}, end {end}")

                    elements = await storage_data.lrange(key, start, end)
                    writer.write(format_resp_array(elements))
                    await writer.drain()  # Flush write buffer

                    i += 4

                # Remove elements from the left
                case "LPOP":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""

                    number_to_pop: int = int(command_list[i + 2]) if i + 2 < command_list_len else 1

                    logging.info(f"LPOP: {key}, count: {number_to_pop}")

                    value: list | None = await storage_data.lpop(key, number_to_pop)

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

                    if i + 2 < command_list_len:
                        # There is optional count specified
                        i += 3
                    else:
                        i += 2

                # Remove elements from the left with blocking
                case "BLPOP":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    blocking_time: float = float(command_list[i + 2]) if i + 2 < command_list_len else 0

                    logging.info(f"BLPOP: {key}, blocking time: {blocking_time}")

                    # TODO -> Use Pydantic to validate input schema
                    result: dict[str, list | None] = await storage_data.blpop(key, blocking_time)

                    if result is None:
                        # Unable to pop from specified list
                        logging.info(f"BLPOP: {key} timed out after {blocking_time} seconds")
                        writer.write(format_null_bulk_string())
                    else:
                        # List name and removed item are array of bulk strings
                        writer.write(format_resp_array([result["list_name"], result["removed_item"]]))
                        logging.info(f"BLPOP: Wrote array response for {key} -> [{result['list_name']}, {result['removed_item']}]")

                    await writer.drain()  # Flush write buffer

                    if i + 2 < command_list_len:
                        # Blocking time is specified
                        i += 3
                    else:
                        i += 2

                # Add an entry to a stream
                # Stream is created first if it doesn't exist
                case "XADD":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    id: str = command_list[i + 2] if i + 2 < command_list_len else ""

                    # Get all field-value pairs
                    field_value_pairs: dict = {}
                    for j in range(i + 3, command_list_len, 2):
                        if j + 1 < command_list_len:
                            field_value_pairs[command_list[j]] = command_list[j + 1]
                        else:
                            field_value_pairs[command_list[j]] = ""

                    try:
                        entry_id: str = await storage_data.xadd(key, id, field_value_pairs)
                        logging.info(f"XADD: {key}, id: {id}, field-value pairs: {field_value_pairs}")
                        writer.write(format_bulk_string_success(entry_id)) # Requires bulk string response
                    except ValueError as e:
                        logging.error(f"XADD: Error adding entry to stream {key}: {e}")
                        writer.write(format_simple_error(e)) # Error response -> Should have ERR in it
                        
                    await writer.drain()  # Flush write buffer

                    # Move to next command
                    i += 3 + (2 * len(field_value_pairs))

                case "XRANGE":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    start: str = command_list[i + 2] if i + 2 < command_list_len else "-"
                    end: str = command_list[i + 3] if i + 3 < command_list_len else "+"
                    count: int | None = int(command_list[i + 5]) if i + 4 < command_list_len and command_list[i + 4].upper() == "COUNT" and i + 5 < command_list_len else None

                    logging.info(f"XRANGE: {key}, start: {start}, end: {end}, count: {count}")

                    # If count is <= 0, no need to query storage, just return null bulk string
                    # Null bulk string is what Redis returns in this situation
                    if count is not None and count <= 0:
                        logging.info(f"XRANGE: Invalid count for {key}: {count}")
                        writer.write(format_null_bulk_string())

                        await writer.drain()  # Flush write buffer
                        i += 6
                        continue

                    try:
                        entries: list = await storage_data.xrange(key, start, end, count)

                        # Need to return RESP array of arrays
                        # Each inner array represents an entry in the stream
                        # The first item in the inner array is the entry ID
                        # The second item is a list of key values pairs (represented as list of strings)
                        # Key value pairs in order they were added to the entry

                        response: bytes = b""

                        response += b"*" + str(len(entries)).encode("utf-8") + b"\r\n" # RESP array header


                        for entry in entries:
                            response += b"*" + str(len(entry)).encode("utf-8") + b"\r\n" # Inner array header
                            for item in entry:
                                if isinstance(item, list):
                                    # List of field-value pairs
                                    response += format_resp_array(item)
                                else:
                                    # Entry ID (string)
                                    response += format_bulk_string_success(item)

                        logging.info(f"XRANGE: Formatted RESP array response: {response}")
                        writer.write(response) # RESP array response
                        logging.info(f"XRANGE: Wrote array response for {key} with {len(entries)} entries")
                    except ValueError as e:
                        logging.error(f"XRANGE: Error retrieving entries from stream {key}: {e}")
                        writer.write(format_simple_error(e)) # Error response -> Should have ERR in it

                    await writer.drain()  # Flush write buffer

                    # Move to next command
                    if count is not None:
                        i += 6
                    else:
                        i += 4

                case "EXISTS":
                    keys: list[str] = command_list[i + 1 : command_list_len]

                    logging.info(f"EXISTS: keys {keys}")

                    num_existing_keys: int = 0
                    for key in keys:
                        num_existing_keys += 1 if await storage_data.exists(key) else 0

                    writer.write(format_integer_success(num_existing_keys))
                    await writer.drain()  # Flush write buffer

                    i += 1 + len(keys)  # Move to next command

                case "DEL":
                    keys: list[str] = command_list[i + 1 : command_list_len]

                    logging.info(f"DEL: keys {keys}")

                    num_deleted_keys: int = 0
                    for key in keys:
                        num_deleted_keys += 1 if await storage_data.delete(key) else 0

                    writer.write(format_integer_success(num_deleted_keys))
                    await writer.drain()  # Flush write buffer

                    i += 1 + len(keys)  # Move to next command

                case "FLUSHDB":
                    # Flushing is sync by default for Redis, so copying this behaviour
                    method: Literal["SYNC", "ASYNC", ""] = command_list[i + 1] if i + 1 < command_list_len else ""

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

                    # Move to next command
                    if method == "":  # No method specified
                        i += 1
                    else:
                        i += 2

                case _:
                    # Keep this for now, change/remove when done
                    writer.write(b"-Error: Unknown command\r\n")
                    logging.info(f"Sent error response for unknown command: {curr_command}")
                    await writer.drain()  # Flush write buffer

                    i += 1  # Move to next command

    # This is just the client disconnecting -> do not shut down server if this happens
    logging.info("Closing connection with client")
    try:
        await close_writer(writer)
    except BrokenPipeError: # Happens when client disconnects abruptly
        logging.info("Client disconnected before connection could be closed")

@conditional_decorator(profile(output_file="profile_stats.prof"), condition=False) # Change to True to enable profiling
async def main() -> None:
    """
    Starts the asyncio server on localhost:6379

    Notes: 
       1. uvloop performed worse than default asyncio event loop in benchmarks. Do not use it.
       2. Most of the runtime of the program is spent in asyncio selector, so rewrite to Rust or Go once API is stable
    """
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    logging.info("Starting server on localhost:6379")
    logging.info(f"Process ID: {os.getpid()}")

    server = await asyncio.start_server(handle_server, "localhost", 6379) # Client function called whenever client sends a message

    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        logging.info("Server serve_forever cancelled, shutting down server...")
        # Close tasks here instead of in handle_server to handle KeyboardInterrupt properly
        for task in asyncio.all_tasks():
            logging.info(f"Current task: {task}")
            if task is not asyncio.current_task():
                logging.info("Cancelling task...")
                task.cancel()
        server.close()
        await server.wait_closed()
        
        logging.info("Server shut down successfully")

if __name__ == "__main__":
    asyncio.run(main())