import unittest
import asyncio

from app.main import main

def _write_and_drain(writer: asyncio.StreamWriter, data: bytes):
    """
    Helper function to write data to the writer and drain it immediately.
    """
    writer.write(data)
    return writer.drain()


class TestServer(unittest.IsolatedAsyncioTestCase):
    """
    Base class for server tests.

    Ensures a fresh db for each test and handles server startup/shutdown.
    """
    async def asyncSetUp(self):
        self.server_port = 6379
        # Start the server
        self.server_task = asyncio.create_task(main())
        await asyncio.sleep(0.1)  # Give the server a moment to start (will get connect call failed without this)

        self.reader, self.writer = await asyncio.open_connection('localhost', self.server_port)


    async def asyncTearDown(self):

        # Flush the db to ensure clean state
        await _write_and_drain(self.writer, b'*1\r\n$5\r\nFLUSHDB\r\n')

        # Cancel server tasks so no interference between tests
        self.server_task.cancel()

        # See: https://docs.python.org/3/library/asyncio-stream.html#examples
        self.writer.close()
        await self.writer.wait_closed()


class BasicCommandsTests(TestServer):
    """
    Test PING, ECHO, SET, GET, TYPE, EXISTS, DEL commands
    """

    async def test_single_ping(self):
        await _write_and_drain(self.writer, b'PING')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+PONG\r\n')

    async def test_multiple_pings(self):
        for _ in range(5):
            await _write_and_drain(self.writer, b'PING')
            response = await self.reader.read(100)
            self.assertEqual(response, b'+PONG\r\n')

    async def test_multiple_clients(self):
        clients = []
        for _ in range(3):
            reader, writer = await asyncio.open_connection('localhost', self.server_port)
            clients.append((reader, writer))

        for reader, writer in clients:
            await _write_and_drain(writer, b'PING')

        for reader, writer in clients:
            response = await reader.read(100)
            self.assertEqual(response, b'+PONG\r\n')
            writer.close()

    async def test_echo(self):
        await _write_and_drain(self.writer, b'PING')
        response = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'$3\r\nhey\r\n')

    async def test_non_uppercase_commands_work(self):
        await _write_and_drain(self.writer, b'*1\r\n$4\r\nping\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+PONG\r\n')

    async def test_set(self):
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')

    async def test_get(self):
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$5\r\nvalue\r\n')

    async def test_get_key_not_found(self):
        await _write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nnon_existing_key\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')

    async def test_set_with_expiry(self):
        await _write_and_drain(self.writer, b'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n100\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')
        await asyncio.sleep(0.1)  # Wait for key to expire
        await _write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')  # Key should be expired

    async def test_type_string(self):
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*2\r\n$4\r\nTYPE\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+string\r\n')

    async def test_type_key_not_found(self):
        await _write_and_drain(self.writer, b'*2\r\n$4\r\nTYPE\r\n$3\r\nnon_existing_key\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+none\r\n')

    async def test_type_list(self):
        await _write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$4\r\nshould_be_list\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*2\r\n$4\r\nTYPE\r\n$4\r\nshould_be_list\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+list\r\n')

    async def test_type_stream(self):
        await _write_and_drain(self.writer, b'*5\r\n$4\r\nXADD\r\n$16\r\nshould_be_stream\r\n$3\r\n0-1\r\n$4\r\ntemp\r\n$2\r\n36\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*2\r\n$4\r\nTYPE\r\n$16\r\nshould_be_stream\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+stream\r\n')

    async def test_exists_with_existing_key(self):
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nexistent\r\n$3\r\nyes\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*2\r\n$6\r\nEXISTS\r\n$8\r\nexistent\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_exists_with_nonexistent_key(self):
        await _write_and_drain(self.writer, b'*2\r\n$6\r\nEXISTS\r\n$4\r\nnope\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':0\r\n')

    async def test_exists_with_multiple_keys(self):
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey1\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey2\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*4\r\n$6\r\nEXISTS\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':2\r\n')

    async def test_del_with_existing_key(self):
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nexistent\r\n$3\r\nyes\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*2\r\n$3\r\nDEL\r\n$8\r\nexistent\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_del_with_nonexistent_key(self):
        await _write_and_drain(self.writer, b'*2\r\n$6\r\nDEL\r\n$4\r\nnope\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':0\r\n')

    async def test_del_with_multiple_keys(self):
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey1\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey2\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*4\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':2\r\n')


class ListTests(TestServer):
    """
    Test RPUSH, LPUSH, LRANGE, LLEN, LPOP, BLPOP commands
    """

    async def test_create_list_rpush(self):
        await _write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$5\r\nvalue\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_rpush_append_single_element(self):
        await _write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$5\r\nvalue1\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$5\r\nvalue2\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':2\r\n')

    async def test_rpush_with_multiple_elements_in_single_command(self):
        await _write_and_drain(self.writer, b'*6\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$5\r\nvalue3\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':3\r\n')

    async def test_lrange_positive_indices(self):
        await _write_and_drain(self.writer, b'*6\r\n$5\r\nRPUSH\r\n$8\r\nlist_key\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n0\r\n$1\r\n2\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n')
        await _write_and_drain(self.writer, b'*4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n2\r\n$1\r\n4\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n')

    async def test_lrange_negative_indices(self):
        await _write_and_drain(self.writer, b'*6\r\n$5\r\nRPUSH\r\n$8\r\nlist_key\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n-3\r\n$1\r\n-1\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n')
        await _write_and_drain(self.writer, b'*4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n-2\r\n$1\r\n-1\r\n')

    async def test_lpush(self):
        await _write_and_drain(self.writer, b'*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\nc\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')
        await _write_and_drain(self.writer, b'*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\nb\r\n$1\r\na\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':3\r\n')
        await _write_and_drain(self.writer, b'*4\r\n$6\r\nlrange\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n')

    async def test_llen(self):
        await _write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*3\r\n$5\r\nLLEN\r\n$6\r\nmylist\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':3\r\n')

    async def test_lpop_single_element(self):
        await _write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$1\r\na\r\n')
        await _write_and_drain(self.writer, b'*3\r\n$5\r\nLLEN\r\n$6\r\nmylist\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':2\r\n')

    async def test_lpop_list_does_not_exist(self):
        await _write_and_drain(self.writer, b'*3\r\n$4\r\nLPOP\r\n$10\r\nnonexistent\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')

    async def test_lpop_empty_list(self):
        await _write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$9\r\nemptylist\r\n$0\r\n\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*3\r\n$4\r\nLPOP\r\n$9\r\nemptylist\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')

    async def test_lpop_multiple_elements_single_command(self):
        await _write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*4\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$2\r\n2\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*2\r\n$1\r\na\r\n$1\r\nb\r\n')
        await _write_and_drain(self.writer, b'*3\r\n$5\r\nLLEN\r\n$6\r\nmylist\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_blpop_with_timeout_simple(self):
        await _write_and_drain(self.writer, b'*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n1\r\n')
        
        # Need to open new connection to rpush to the list since blpop is blocking
        new_reader, new_writer = await asyncio.open_connection('localhost', self.server_port)
        await _write_and_drain(new_writer, b'*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\nfoo\r\n')
        int_response = await new_reader.read(100)
        self.assertEqual(int_response, b':1\r\n')

        # Check response from blpop client
        response = await self.reader.read(300)
        self.assertEqual(response, b'*2\r\n$6\r\nmylist\r\n$3\r\nfoo\r\n')
        
        # Clean up
        new_writer.close()
        await new_writer.wait_closed()


class StreamTests(TestServer):
    """
    Test XADD and XRANGE commands
    """

    async def test_xadd(self):
        await _write_and_drain(self.writer, b'*5\r\n$4\r\nXADD\r\n$9\r\ntest_xadd\r\n$3\r\n0-1\r\n$4\r\ntemp\r\n$2\r\n36\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$3\r\n0-1\r\n')

    async def test_xrange(self):
        await _write_and_drain(self.writer, b'*7\r\n$4\r\nXADD\r\n$11\r\ntest_xrange\r\n$15\r\n1526985054069-0\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*7\r\n$4\r\nXADD\r\n$11\r\ntest_xrange\r\n$15\r\n1526985054079-0\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n')
        _ = await self.reader.read(100)
        await _write_and_drain(self.writer, b'*4\r\n$6\r\nXRANGE\r\n$11\r\ntest_xrange\r\n$13\r\n1526985054069\r\n$13\r\n1526985054079\r\n')
        response = await self.reader.read(500)
        
        should_be = b'*2\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n'

        self.assertEqual(response, should_be)


class OtherCommandsTests(TestServer):
    """
    Test FLUSHDB command
    """

    async def test_flushdb_sync(self):
        # Set a key
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)

        # Flush the database
        await _write_and_drain(self.writer, b'*1\r\n$5\r\nFLUSHDB\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')

        # Try to get the key, should return nil
        await _write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')  # Key should not be found

    async def test_flushdb_async(self):
        # Set a key
        await _write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)

        # Flush the database asynchronously
        await _write_and_drain(self.writer, b'*2\r\n$7\r\nFLUSHDB\r\n$5\r\nASYNC\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')

        # Wait a moment to allow async flush to complete
        await asyncio.sleep(0.1)

        # Try to get the key, should return nil
        await _write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')  # Key should not be found


if __name__ == "__main__":
    unittest.main()