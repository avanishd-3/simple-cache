import asyncio

async def close_writer(writer: asyncio.StreamWriter) -> None:
    """
    Closes the given StreamWriter.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to close.

    # See: https://docs.python.org/3/library/asyncio-stream.html#examples
    """
    writer.close()
    await writer.wait_closed()

async def write_and_drain(writer: asyncio.StreamWriter, data: bytes):
    """
    Helper function to write data to the writer and drain it immediately.
    """
    writer.write(data)
    await writer.drain()