import asyncio
import logging
import time

# Internal imports
from .format_response import (
    format_simple_success,
    format_bulk_success,
    format_integer_success,
    format_resp_array,
    format_bulk_error,
)

from .data_storage import DataStorage

logging.basicConfig(level=logging.INFO)

# Data
storage_data: DataStorage = DataStorage()

async def redis_parser(data: bytes) -> list[str]:
    # TODO: Make actual parser

    command_list = data.decode().strip().split("\r\n")

    # Remove commands with * and $
    # Do not uppercase commands, because some of them contain strings
    command_list = [cmd for cmd in command_list if not (cmd.startswith("*") or cmd.startswith("$"))]

    logging.info(f"Parsed commands: {command_list}")

    return command_list


async def handle_server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    data = None

    while data != b"QUIT\r\n":
        data = await reader.read(1024)
        
        if not data:
            break

        command_list = await redis_parser(data)

        command_list_len: int = len(command_list)
        i: int = 0

        while i < command_list_len:
            curr_command = command_list[i]

            logging.info(f"Received command: {curr_command}")

            match curr_command.upper():
                case "PING":
                    writer.write(format_simple_success("PONG"))
                    await writer.drain()  # Flush write buffer

                    logging.info("Sent PONG response")

                    i += 1  # Move to next command
                case "ECHO":
                    msg: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    writer.write(format_simple_success(msg))
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

                    writer.write(format_simple_success("OK"))
                    await writer.drain()  # Flush write buffer

                case "GET":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""

                    value: str | None = await storage_data.get(key)

                    if value is not None:
                        writer.write(format_bulk_success(value))
                        logging.info(f"Sent GET response: {key} = {value}")
                    else:
                        # Should return null bulk string -> $-1\r\n
                        writer.write(format_bulk_error()) # Null bulk string (shows key doesn't exist)
                        logging.info(f"Key {key} not found")

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

                # Retrieve a range of elements from a list
                # TODO: Add support for negative indices
                case "LRANGE":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    start: int = int(command_list[i + 2]) if i + 2 < command_list_len else 0
                    end: int = int(command_list[i + 3]) if i + 3 < command_list_len else -1

                    logging.info(f"LRANGE: key {key}, start {start}, end {end}")

                    elements = await storage_data.lrange(key, start, end)
                    writer.write(format_resp_array(elements))
                    await writer.drain()  # Flush write buffer

                    i += 4

                case _:
                    # Keep this for now, change/remove when done
                    writer.write(b"-Error: Unknown command\r\n")
                    logging.info(f"Sent error response for unknown command: {curr_command}")
                    await writer.drain()  # Flush write buffer

                    i += 1  # Move to next command

    writer.close()
    await writer.wait_closed()



async def main() -> None:
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server = await asyncio.start_server(handle_server, "localhost", 6379) # Client function called whenever client sends a message

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
