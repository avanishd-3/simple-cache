import unittest
import asyncio
import time

from app.main import main
from app.utils.writer_utils import close_writer, write_and_drain




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
        await write_and_drain(self.writer, b'*1\r\n$5\r\nFLUSHDB\r\n')

        # Shutdown the server
        # This is just here to be a test of the SHUTDOWN command
        await write_and_drain(self.writer, b'*1\r\n$8\r\nSHUTDOWN\r\n')

        # Cancel server task so no interference between tests
        self.server_task.cancel()

        await close_writer(self.writer)

class StringCommandsTests(TestServer):
    """
    Test SET and GET commands
    """

    async def test_set(self):
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')

    async def test_set_with_expiry_milliseconds(self):
        await write_and_drain(self.writer, b'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n100\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')
        await asyncio.sleep(0.1)  # Wait for key to expire
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')  # Key should be expired

    async def test_set_with_expiry_seconds(self):
        await write_and_drain(self.writer, b'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')
        await asyncio.sleep(1)  # Wait for key to expire
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')  # Key should be expired

    async def test_set_with_expiry_at_unix_time_seconds(self):
        future_time = int(time.time()) + 1  # 1 second in the future
        await write_and_drain(self.writer, f'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$4\r\nEXAT\r\n${len(str(future_time))}\r\n{future_time}\r\n'.encode())
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')
        await asyncio.sleep(1)  # Wait for key to expire
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')  # Key should be expired

    async def test_set_with_expiry_at_unix_time_milliseconds(self):
        future_time = int(time.time() * 1000) + 100  # 100 ms in the future
        await write_and_drain(self.writer, f'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$4\r\nPXAT\r\n${len(str(future_time))}\r\n{future_time}\r\n'.encode())
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')
        await asyncio.sleep(0.1)  # Wait for key to expire
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')  # Key should be expired

    async def test_set_with_keep_ttl_new_key(self):
        await write_and_drain(self.writer, b'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$7\r\nKEEPTTL\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$5\r\nvalue\r\n')  # Key should exist without expiry

    async def test_set_with_keep_ttl_existing_key_no_expiry(self):
        # Set key with expiry
        await write_and_drain(self.writer, b'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')

        # Update key with KEEPTTL
        await write_and_drain(self.writer, b'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nnew_val\r\n$7\r\nKEEPTTL\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')

        # Check if key still exists
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$7\r\nnew_val\r\n')  # Key should still exist with new value

    async def test_set_with_keep_ttl_existing_key_with_expiry(self):
        # Set key with expiry
        await write_and_drain(self.writer, b'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n100\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')

        # Update key with KEEPTTL
        await write_and_drain(self.writer, b'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nnew_val\r\n$7\r\nKEEPTTL\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')

        await asyncio.sleep(0.1)  # Wait for key to expire

        # Check if key still exists
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')  # Key should be expired

    async def test_get(self):
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$5\r\nvalue\r\n')

    async def test_get_key_not_found(self):
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nnon_existing_key\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')


class BasicCommandsTests(TestServer):
    """
    Test PING, ECHO, TYPE, EXISTS, DEL commands
    """

    async def test_single_ping(self):
        await write_and_drain(self.writer, b'PING')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+PONG\r\n')

    async def test_multiple_pings(self):
        for _ in range(5):
            await write_and_drain(self.writer, b'PING')
            response = await self.reader.read(100)
            self.assertEqual(response, b'+PONG\r\n')

    async def test_multiple_clients(self):
        clients = []
        for _ in range(3):
            reader, writer = await asyncio.open_connection('localhost', self.server_port)
            clients.append((reader, writer))

        for reader, writer in clients:
            await write_and_drain(writer, b'PING')

        for reader, writer in clients:
            response = await reader.read(100)
            self.assertEqual(response, b'+PONG\r\n')
            writer.close()

    async def test_echo(self):
        await write_and_drain(self.writer, b'PING')
        response = await self.reader.read(100)
        await write_and_drain(self.writer, b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'$3\r\nhey\r\n')

    async def test_non_uppercase_commands_work(self):
        await write_and_drain(self.writer, b'*1\r\n$4\r\nping\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+PONG\r\n')

    async def test_type_string(self):
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*2\r\n$4\r\nTYPE\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+string\r\n')

    async def test_type_key_not_found(self):
        await write_and_drain(self.writer, b'*2\r\n$4\r\nTYPE\r\n$3\r\nnon_existing_key\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+none\r\n')

    async def test_type_list(self):
        await write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$4\r\nshould_be_list\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*2\r\n$4\r\nTYPE\r\n$4\r\nshould_be_list\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+list\r\n')

    async def test_type_stream(self):
        await write_and_drain(self.writer, b'*5\r\n$4\r\nXADD\r\n$16\r\nshould_be_stream\r\n$3\r\n0-1\r\n$4\r\ntemp\r\n$2\r\n36\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*2\r\n$4\r\nTYPE\r\n$16\r\nshould_be_stream\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+stream\r\n')

    async def test_type_set(self):
        await write_and_drain(self.writer, b'*4\r\n$4\r\nSADD\r\n$6\r\nmyset\r\n$7\r\nmember1\r\n$7\r\nmember2\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*2\r\n$4\r\nTYPE\r\n$5\r\nmyset\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+set\r\n')

    async def test_exists_with_existing_key(self):
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nexistent\r\n$3\r\nyes\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*2\r\n$6\r\nEXISTS\r\n$8\r\nexistent\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_exists_with_nonexistent_key(self):
        await write_and_drain(self.writer, b'*2\r\n$6\r\nEXISTS\r\n$4\r\nnope\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':0\r\n')

    async def test_exists_with_multiple_keys(self):
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey1\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey2\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$6\r\nEXISTS\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':2\r\n')

    async def test_del_with_existing_key(self):
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nexistent\r\n$3\r\nyes\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*2\r\n$3\r\nDEL\r\n$8\r\nexistent\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_del_with_nonexistent_key(self):
        await write_and_drain(self.writer, b'*2\r\n$6\r\nDEL\r\n$4\r\nnope\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':0\r\n')

    async def test_del_with_multiple_keys(self):
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey1\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey2\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':2\r\n')


class ListTests(TestServer):
    """
    Test RPUSH, LPUSH, LRANGE, LLEN, LPOP, BLPOP commands
    """

    async def test_create_list_rpush(self):
        await write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$5\r\nvalue\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_rpush_append_single_element(self):
        await write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$5\r\nvalue1\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$5\r\nvalue2\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':2\r\n')

    async def test_rpush_with_multiple_elements_in_single_command(self):
        await write_and_drain(self.writer, b'*6\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$5\r\nvalue3\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':3\r\n')

    async def test_lrange_positive_indices(self):
        await write_and_drain(self.writer, b'*6\r\n$5\r\nRPUSH\r\n$8\r\nlist_key\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n0\r\n$1\r\n2\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n')
        await write_and_drain(self.writer, b'*4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n2\r\n$1\r\n4\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n')

    async def test_lrange_negative_indices(self):
        await write_and_drain(self.writer, b'*6\r\n$5\r\nRPUSH\r\n$8\r\nlist_key\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n-3\r\n$1\r\n-1\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n')
        await write_and_drain(self.writer, b'*4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n-2\r\n$1\r\n-1\r\n')

    async def test_lpush(self):
        await write_and_drain(self.writer, b'*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\nc\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')
        await write_and_drain(self.writer, b'*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\nb\r\n$1\r\na\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':3\r\n')
        await write_and_drain(self.writer, b'*4\r\n$6\r\nlrange\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n')

    async def test_llen(self):
        await write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*3\r\n$5\r\nLLEN\r\n$6\r\nmylist\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':3\r\n')

    async def test_lpop_single_element(self):
        await write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$1\r\na\r\n')
        await write_and_drain(self.writer, b'*3\r\n$5\r\nLLEN\r\n$6\r\nmylist\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':2\r\n')

    async def test_lpop_list_does_not_exist(self):
        await write_and_drain(self.writer, b'*3\r\n$4\r\nLPOP\r\n$10\r\nnonexistent\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')

    async def test_lpop_empty_list(self):
        await write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$9\r\nemptylist\r\n$0\r\n\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*3\r\n$4\r\nLPOP\r\n$9\r\nemptylist\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')

    async def test_lpop_multiple_elements_single_command(self):
        await write_and_drain(self.writer, b'*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$2\r\n2\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*2\r\n$1\r\na\r\n$1\r\nb\r\n')
        await write_and_drain(self.writer, b'*3\r\n$5\r\nLLEN\r\n$6\r\nmylist\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_blpop_with_timeout_simple(self):
        await write_and_drain(self.writer, b'*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n1\r\n')
        
        # Need to open new connection to rpush to the list since blpop is blocking
        new_reader, new_writer = await asyncio.open_connection('localhost', self.server_port)
        await write_and_drain(new_writer, b'*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\nfoo\r\n')
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
        await write_and_drain(self.writer, b'*5\r\n$4\r\nXADD\r\n$9\r\ntest_xadd\r\n$3\r\n0-1\r\n$4\r\ntemp\r\n$2\r\n36\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$3\r\n0-1\r\n')

    async def test_xrange(self):
        await write_and_drain(self.writer, b'*7\r\n$4\r\nXADD\r\n$11\r\ntest_xrange\r\n$15\r\n1526985054069-0\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*7\r\n$4\r\nXADD\r\n$11\r\ntest_xrange\r\n$15\r\n1526985054079-0\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$6\r\nXRANGE\r\n$11\r\ntest_xrange\r\n$13\r\n1526985054069\r\n$13\r\n1526985054079\r\n')
        response = await self.reader.read(500)
        
        should_be = b'*2\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n'

        self.assertEqual(response, should_be)

class SetTests(TestServer):
    """
    Test SADD, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SUNION commands
    """

    async def test_sadd_new_set(self):
        await write_and_drain(self.writer, b'*4\r\n$4\r\nSADD\r\n$7\r\nmyset\r\n$5\r\nvalue1\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_sadd_existing_set_new_member(self):
        await write_and_drain(self.writer, b'*4\r\n$4\r\nSADD\r\n$7\r\nmyset\r\n$5\r\nvalue1\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$4\r\nSADD\r\n$7\r\nmyset\r\n$5\r\nvalue2\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_sadd_existing_set_existing_member(self):
        await write_and_drain(self.writer, b'*4\r\n$4\r\nSADD\r\n$7\r\nmyset\r\n$5\r\nvalue1\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$4\r\nSADD\r\n$7\r\nmyset\r\n$5\r\nvalue1\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':0\r\n')

    async def test_sadd_multiple_members(self):
        await write_and_drain(self.writer, b'*6\r\n$4\r\nSADD\r\n$7\r\nmyset\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$5\r\nvalue3\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':3\r\n')

    async def test_sadd_error_when_no_members(self):
        await write_and_drain(self.writer, b'*2\r\n$4\r\nSADD\r\n$7\r\nmyset\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'-ERR wrong number of arguments for \'sadd\' command\r\n')

    async def test_sadd_error_when_no_key(self):
        await write_and_drain(self.writer, b'*3\r\n$4\r\nSADD\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'-ERR wrong number of arguments for \'sadd\' command\r\n')

    async def test_scard_non_existent_set(self):
        await write_and_drain(self.writer, b'*3\r\n$5\r\nSCARD\r\n$7\r\nnoset\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':0\r\n')

    async def test_scard_existing_set(self):
        await write_and_drain(self.writer, b'*6\r\n$4\r\nSADD\r\n$7\r\nmyset\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$5\r\nvalue3\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*3\r\n$5\r\nSCARD\r\n$7\r\nmyset\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':3\r\n')

    async def test_scard_error_when_no_key(self):
        await write_and_drain(self.writer, b'*2\r\n$5\r\nSCARD\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'-ERR wrong number of arguments for \'scard\' command\r\n')

    async def test_sdiff_error_when_no_keys(self):
        await write_and_drain(self.writer, b'*1\r\n$5\r\nSDIFF\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'-ERR wrong number of arguments for \'sdiff\' command\r\n')

    async def test_sdiff_non_existent_keys(self):
        await write_and_drain(self.writer, b'*3\r\n$5\r\nSDIFF\r\n$3\r\nkey1\r\n$3\r\nkey2\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'*0\r\n')

    async def test_sdiff_existing_keys(self):
        await write_and_drain(self.writer, b'*6\r\n$4\r\nSADD\r\n$4\r\nkey1\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$5\r\nvalue3\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$4\r\nSADD\r\n$4\r\nkey2\r\n$5\r\nvalue2\r\n$5\r\nvalue4\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*3\r\n$5\r\nSDIFF\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*2\r\n$6\r\nvalue1\r\n$6\r\nvalue3\r\n')

    async def test_sdiff_store_error_when_no_keys(self):
        await write_and_drain(self.writer, b'*2\r\n$5\r\nSDIFFSTORE\r\n$7\r\ndestset\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'-ERR wrong number of arguments for \'sdiffstore\' command\r\n')

    async def test_sdiff_store_new_set(self):
        await write_and_drain(self.writer, b'*6\r\n$4\r\nSADD\r\n$4\r\nkey1\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$5\r\nvalue3\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$4\r\nSADD\r\n$4\r\nkey2\r\n$5\r\nvalue2\r\n$5\r\nvalue4\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$10\r\nSDIFFSTORE\r\n$7\r\ndestset\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':2\r\n')  # 2 members in the resulting set
        # Verify contents of destset
        await write_and_drain(self.writer, b'*3\r\n$5\r\nSCARD\r\n$7\r\ndestset\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':2\r\n')

    async def test_sinter_error_when_no_keys(self):
        await write_and_drain(self.writer, b'*1\r\n$5\r\nSINTER\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'-ERR wrong number of arguments for \'sinter\' command\r\n')

    async def test_sinter_non_existent_keys(self):
        await write_and_drain(self.writer, b'*3\r\n$5\r\nSINTER\r\n$3\r\nkey1\r\n$3\r\nkey2\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'*0\r\n')

    async def test_sinter_existing_keys(self):
        await write_and_drain(self.writer, b'*6\r\n$4\r\nSADD\r\n$4\r\nkey1\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$5\r\nvalue3\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$4\r\nSADD\r\n$4\r\nkey2\r\n$5\r\nvalue2\r\n$5\r\nvalue4\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*3\r\n$5\r\nSINTER\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*1\r\n$6\r\nvalue2\r\n')

    async def test_sinter_store_error_when_no_keys(self):
        await write_and_drain(self.writer, b'*2\r\n$5\r\nSINTERSTORE\r\n$7\r\ndestset\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'-ERR wrong number of arguments for \'sinterstore\' command\r\n')

    async def test_sinter_store_new_set(self):
        await write_and_drain(self.writer, b'*6\r\n$4\r\nSADD\r\n$4\r\nkey1\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$5\r\nvalue3\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$4\r\nSADD\r\n$4\r\nkey2\r\n$5\r\nvalue2\r\n$5\r\nvalue4\r\n')
        _ = await self.reader.read(100)
        await write_and_drain(self.writer, b'*4\r\n$10\r\nSINTERSTORE\r\n$7\r\ndestset\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')  # 1 member in the resulting set
        # Verify contents of destset
        await write_and_drain(self.writer, b'*3\r\n$5\r\nSCARD\r\n$7\r\ndestset\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b':1\r\n')

    async def test_sunion_error_when_no_keys(self):
        await write_and_drain(self.writer, b'*1\r\n$5\r\nSUNION\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'-ERR wrong number of arguments for \'sunion\' command\r\n')

    async def test_sunion_existent_and_non_existent_keys(self):
        await write_and_drain(self.writer, b'*6\r\n$4\r\nSADD\r\n$4\r\nkey1\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$5\r\nvalue3\r\n')
        await self.reader.read(100)
        await write_and_drain(self.writer, b'*3\r\n$5\r\nSUNION\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n')
        response = await self.reader.read(300)
        self.assertEqual(response, b'*3\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n$6\r\nvalue3\r\n')

class OtherCommandsTests(TestServer):
    """
    Test FLUSHDB and SHUTDOWN commands
    """

    async def test_flushdb_sync(self):
        # Set a key
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)

        # Flush the database
        await write_and_drain(self.writer, b'*1\r\n$5\r\nFLUSHDB\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')

        # Try to get the key, should return nil
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')  # Key should not be found

    async def test_flushdb_async(self):
        # Set a key
        await write_and_drain(self.writer, b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        _ = await self.reader.read(100)

        # Flush the database asynchronously
        await write_and_drain(self.writer, b'*2\r\n$7\r\nFLUSHDB\r\n$5\r\nASYNC\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'+OK\r\n')

        # Wait a moment to allow async flush to complete
        await asyncio.sleep(0.1)

        # Try to get the key, should return nil
        await write_and_drain(self.writer, b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
        response = await self.reader.read(100)
        self.assertEqual(response, b'$-1\r\n')  # Key should not be found


if __name__ == "__main__":
    unittest.main()