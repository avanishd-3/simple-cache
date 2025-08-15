import asyncio
import logging

# Internal imports
from .format_response import format_success_response, format_error_response, format_bulk_response
from .data_storage import DataStorage

# Data
storage_data: DataStorage = DataStorage()

async def redis_parser(data: bytes) -> list[str]:
    command_list = data.decode().strip().split("\r\n")

    # Remove commands with * and $
    # Do not uppercase commands, because some of them contain strings
    command_list = [cmd for cmd in command_list if not (cmd.startswith("*") or cmd.startswith("$"))]

    print(command_list)

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
            print(f"Received command: {curr_command}")

            match curr_command.upper():
                case "PING":
                    writer.write(format_success_response("PONG"))
                    await writer.drain()  # Flush write buffer

                    logging.info("Sent PONG response")

                    i += 1  # Move to next command
                case "ECHO":
                    msg: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    writer.write(format_success_response(msg))
                    await writer.drain()  # Flush write buffer

                    logging.info(f"Sent ECHO response: {msg}")

                    i += 2  # Move to next command

                case "SET":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    value: str = command_list[i + 2] if i + 2 < command_list_len else ""

                    await storage_data.set(key, value)

                    writer.write(format_success_response("OK"))
                    await writer.drain()  # Flush write buffer

                    logging.info(f"Sent SET response: {key} = {value}")

                    i += 3  # Move to next command

                case "GET":
                    key: str = command_list[i + 1] if i + 1 < command_list_len else ""

                    value: str | None = await storage_data.get(key)

                    if value is not None:
                        writer.write(format_bulk_response(value))
                    else:
                        writer.write(format_error_response("-1")) # Null bulk string (shows key doesn't exist)

                    await writer.drain()  # Flush write buffer

                    logging.info(f"Sent GET response: {key} = {value}")

                    i += 2  # Move to next command

                case _:
                    # Keep this for now, change/remove when done
                    writer.write(b"-Error: Unknown command\r\n")
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
