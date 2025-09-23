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