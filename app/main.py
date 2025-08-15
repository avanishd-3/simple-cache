import asyncio


async def handle_echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    data = None

    while data != b"QUIT\r\n":
        data = await reader.read(1024)
        
        if not data:
            break

        writer.write(b"+PONG\r\n")
        await writer.drain() # Flush write buffer

    writer.close()
    await writer.wait_closed()



async def main() -> None:
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server = await asyncio.start_server(handle_echo, "localhost", 6379) # Client function called whenever client sends a message

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
