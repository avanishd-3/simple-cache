import asyncio
import logging

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
                    writer.write(b"+PONG\r\n")
                    await writer.drain()  # Flush write buffer

                    i += 1  # Move to next command
                case "ECHO":
                    msg: str = command_list[i + 1] if i + 1 < command_list_len else ""
                    writer.write(f"+{msg}\r\n".encode())
                    await writer.drain()  # Flush write buffer

                    i += 2 # Move to next command
                case _:
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
