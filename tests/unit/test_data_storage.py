import unittest
import asyncio
import time

from unittest.mock import Mock, patch

from app.data_storage import DataStorage

from typing import Type

mock_time = Mock()
mock_time.return_value = 1234567890.0


class BaseDataStorageTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.storage = DataStorage()

    async def asyncTearDown(self):
        await self.storage.flushdb_async()


class BasicDataStorageTests(BaseDataStorageTest):
    """
    SET, GET, TYPE, EXISTS, DEL tests
    """

    async def test_set_and_get(self):
        await self.storage.set("foo", "bar")
        value = await self.storage.get("foo")
        self.assertEqual(value, "bar")

    async def test_get_nonexistent_key(self):
        value = await self.storage.get("doesnotexist")
        self.assertIsNone(value)

    async def test_set_with_expiry(self):
        await self.storage.set("expiring", "soon", expiry_time=time.time() + 0.1)
        value = await self.storage.get("expiring")
        self.assertEqual(value, "soon")
        await asyncio.sleep(0.2)
        value = await self.storage.get("expiring")
        self.assertIsNone(value)

    async def test_type_of_nonexistent_key(self):
        key_type = await self.storage.key_type("nope")
        self.assertEqual(key_type, Type[None])

    async def test_type_of_string_key(self):
        await self.storage.set("mystring", "hello")
        key_type = await self.storage.key_type("mystring")
        self.assertEqual(key_type, Type[str])

    async def test_type_of_list_key(self):
        await self.storage.rpush("mylist", ["a", "b", "c"])
        key_type = await self.storage.key_type("mylist")
        self.assertEqual(key_type, Type[list])

    async def test_type_of_stream_key(self):
        await self.storage.xadd("mystream", "1-0", {"field1": "value1"})
        key_type = await self.storage.key_type("mystream")
        self.assertEqual(key_type, Type[dict])

    async def test_exists_with_existing_key(self):
        await self.storage.set("existent", "yes")
        exists = await self.storage.exists("existent")
        self.assertTrue(exists)

    async def test_exists_with_nonexistent_key(self):
        exists = await self.storage.exists("nope")
        self.assertFalse(exists)

    async def test_del_with_existing_key(self):
        await self.storage.set("to_delete", "yes")
        deleted = await self.storage.delete("to_delete")
        self.assertTrue(deleted)
        self.assertTrue("to_delete" not in self.storage.storage_dict)
        self.assertEqual(await self.storage.exists("to_delete"), False)

    async def test_del_with_nonexistent_key(self):
        deleted = await self.storage.delete("nope")
        self.assertFalse(deleted)


class ListDataStorageTests(BaseDataStorageTest):
    """
    RPUSH, LPUSH, LRANGE, LLEN, LPOP, BLPOP tests
    """

    async def test_rpush_creates_list_if_it_doesnt_exist(self):
        length = await self.storage.rpush("numbers", [1, 2])
        self.assertEqual(length, 2)

    async def test_rpush_appends_to_existing_list(self):
        await self.storage.rpush("numbers", [1, 2])
        length = await self.storage.rpush("numbers", [3])
        self.assertEqual(length, 3)

    async def test_lpush_creates_list_if_it_doesnt_exist(self):
        length = await self.storage.lpush("letters", ["a", "b"])
        self.assertEqual(length, 2)

    async def test_lpush_appends_to_existing_list(self):
        await self.storage.lpush("numbers", [1, 2])
        length = await self.storage.lpush("numbers", [3])
        self.assertEqual(length, 3)

    async def test_rpush_and_lrange_basic(self):
        await self.storage.rpush("mylist", ["a", "b", "c"])
        result = await self.storage.lrange("mylist", 0, 2)
        self.assertEqual(result, ["a", "b", "c"])

    async def test_lpush_and_lrange_basic(self):
        await self.storage.lpush("mylist", ["c"])
        await self.storage.lpush("mylist", ["b", "a"])
        
        result = await self.storage.lrange("mylist", 0, -1)
        self.assertEqual(result, ["a", "b", "c"])

    async def test_lrange_with_nonexistent_key(self):
        result = await self.storage.lrange("nope", 0, 1)
        self.assertEqual(result, [])

    async def test_lrange_start_greater_than_length(self):
        await self.storage.rpush("nums", [1, 2, 3])
        result = await self.storage.lrange("nums", 5, 10)
        self.assertEqual(result, [])

    async def test_lrange_start_greater_than_end(self):
        await self.storage.rpush("nums", [1, 2, 3])
        result = await self.storage.lrange("nums", 2, 1)
        self.assertEqual(result, [])

    async def test_lrange_end_greater_than_length(self):
        await self.storage.rpush("nums", [1, 2, 3])
        result = await self.storage.lrange("nums", 0, 10)
        self.assertEqual(result, [1, 2, 3])

    async def test_lrange_on_non_list_value(self):
        await self.storage.set("notalist", "value")
        result = await self.storage.lrange("notalist", 0, 1)
        self.assertEqual(result, [])

    async def test_lrange_with_negative_indices_end_is_last_element(self):
        await self.storage.rpush("letters", ["x", "y", "z"])
        result = await self.storage.lrange("letters", -2, -1)
        self.assertEqual(result, ["y", "z"])

    async def test_lrange_with_negative_indices_start_is_last_element(self):
        await self.storage.rpush("letters", ["x", "y", "z"])
        result = await self.storage.lrange("letters", -1, -1)
        self.assertEqual(result, ["z"])

    async def test_lrange_start_is_zero_end_is_negative(self):
        await self.storage.rpush("letters", ["a", "b", "c", "d", "e"])
        result = await self.storage.lrange("letters", 0, -3)
        self.assertEqual(result, ["a", "b", "c"])

    async def test_lrange_negative_number_start_negative_greater_than_end(self):
        await self.storage.rpush("testlist", ['raspberry', 'grape', 'pineapple', 'mango', 'blueberry', 'pear'])
        result = await self.storage.lrange("testlist", -1, -2)
        self.assertEqual(result, [])

    async def test_llen_with_existing_key(self):
        await self.storage.rpush("mylist", ["a", "b", "c"])
        length = await self.storage.llen("mylist")
        self.assertEqual(length, 3)

    async def test_llen_with_nonexistent_key(self):
        length = await self.storage.llen("nope")
        self.assertEqual(length, 0)

    async def test_lpop_with_one_element_removal(self):
        await self.storage.rpush("mylist", ["a", "b", "c", "d"])
        result: str = await self.storage.lpop("mylist", 1)
        self.assertEqual(result, ["a"])
        self.assertEqual(await self.storage.llen("mylist"), 3)

    async def test_lpop_with_multiple_elements_removal(self):
        await self.storage.rpush("mylist", ["one", "two", "three", "four"])
        result: str = await self.storage.lpop("mylist", 2)
        self.assertEqual(result, ["one", "two"])
        self.assertEqual(await self.storage.llen("mylist"), 2)

    async def test_lpop_with_nonexistent_key(self):
        result: str = await self.storage.lpop("nope", 1)
        self.assertEqual(result, None)

    async def test_lpop_with_empty_list(self):
        await self.storage.rpush("mylist", [])
        result: str = await self.storage.lpop("mylist", 1)
        self.assertEqual(result, None)

    async def test_blpop_no_timeout(self):
        async def blpop_task():
            result = await self.storage.blpop("mylist")
            return result

        task = asyncio.create_task(blpop_task())
        await asyncio.sleep(0.01)
        await self.storage.rpush("mylist", ["first"])
        result = await task

        should_be: dict = {"list_name": "mylist", "removed_item": "first"}
        self.assertEqual(result, should_be)
        self.assertEqual(await self.storage.llen("mylist"), 0)

    async def test_blpop_key_appended_before_timeout(self):
        async def blpop_task():
            result = await self.storage.blpop("mylist", timeout=1)
            return result

        task = asyncio.create_task(blpop_task())
        await asyncio.sleep(0.01)
        await self.storage.rpush("mylist", ["item"])
        result = await task

        should_be: dict = {"list_name": "mylist", "removed_item": "item"}
        self.assertEqual(result, should_be)
        self.assertEqual(await self.storage.llen("mylist"), 0)

    async def test_blpop_timeout_occurs(self):
        async def blpop_task():
            result = await self.storage.blpop("mylist", timeout=0.1)
            return result

        task = asyncio.create_task(blpop_task())
        await asyncio.sleep(0.1)
        await self.storage.rpush("mylist", ["item"]) # Need this here to make sure None is returning due to timeout, not because list was never appended to
        result = await task
        self.assertIsNone(result, None)

    async def test_blpop_list_has_items_before_call(self):
        await self.storage.rpush("mylist", ["a", "b", "c"])

        result = await self.storage.blpop("mylist")
        should_be: dict = {"list_name": "mylist", "removed_item": "a"}
        self.assertEqual(result, should_be)
        self.assertEqual(await self.storage.llen("mylist"), 2)

    async def test_xadd_creates_stream_if_not_exists(self):
        entry_id = await self.storage.xadd("mystream", "1-0", {"field1": "value1"})
        self.assertEqual(entry_id, "1-0")
        key_len: float = len(self.storage.storage_dict["mystream"].value)
        self.assertEqual(key_len, 1)


class StreamDataStorageTests(BaseDataStorageTest):
    async def test_xadd_appends_to_existing_stream(self):
        await self.storage.xadd("mystream", "1-0", {"field1": "value1"})
        entry_id = await self.storage.xadd("mystream", "1-1", {"field2": "value2"})
        self.assertEqual(entry_id, "1-1")
        key_len: float = len(self.storage.storage_dict["mystream"].value)
        self.assertEqual(key_len, 2)

    async def test_xadd_errors_with_id_equal_to_last(self):
        await self.storage.xadd("some_key", "1-1", {"foo": "bar"})
        with self.assertRaises(ValueError) as context:
            await self.storage.xadd("some_key", "1-1", {"bar": "baz"})
        self.assertIn("ERR The ID specified in XADD is equal or smaller than the target stream top item", str(context.exception))

    async def test_xadd_errors_with_sequence_number_less_than_last(self):
        await self.storage.xadd("another_key", "2-5", {"a": "b"})
        with self.assertRaises(ValueError) as context:
            await self.storage.xadd("another_key", "2-4", {"c": "d"})
        self.assertIn("ERR The ID specified in XADD is equal or smaller than the target stream top item", str(context.exception))

    async def test_xadd_errors_with_milliseconds_less_than_last(self):
        await self.storage.xadd("stream_key", "3-10", {"x": "y"})
        with self.assertRaises(ValueError) as context:
            await self.storage.xadd("stream_key", "2-20", {"y": "z"})
        self.assertIn("ERR The ID specified in XADD is equal or smaller than the target stream top item", str(context.exception))

    async def test_xadd_errors_with_0_0_id_on_new_stream(self):
        with self.assertRaises(ValueError) as context:
            await self.storage.xadd("newstream", "0-0", {"field": "value"})
        self.assertIn("ERR The ID specified in XADD must be greater than 0-0", str(context.exception))

    async def test_xadd_has_0_0_error_id_0_0_on_existing_stream(self):
        await self.storage.xadd("stream_key", "1-0", {"a": "b"})
        with self.assertRaises(ValueError) as context:
            await self.storage.xadd("stream_key", "0-0", {"c": "d"})
        self.assertIn("ERR The ID specified in XADD must be greater than 0-0", str(context.exception))

    async def test_xadd_errors_with_negative_numbers_in_id(self):
        with self.assertRaises(ValueError) as context:
            await self.storage.xadd("badstream", "-1--1", {"field": "value"})
        self.assertIn("ERR Invalid stream ID specified as stream command argument", str(context.exception))

    async def test_xadd_errors_with_invalid_id(self):
        with self.assertRaises(ValueError) as context:
            await self.storage.xadd("badstream", "not-id", {"field": "value"})
        self.assertIn("ERR Invalid stream ID specified as stream command argument", str(context.exception))

    async def test_xadd_auto_generate_sequence_number_new_stream_with_time_0(self):
        entry_id = await self.storage.xadd("autostream", "0-*", {"field": "value"})
        self.assertEqual(entry_id, "0-1")
        key_len: float = len(self.storage.storage_dict["autostream"].value)
        self.assertEqual(key_len, 1)

    async def test_xadd_auto_generate_sequence_number_new_stream_with_time_non_zero(self):
        entry_id = await self.storage.xadd("autostream", "5-*", {"field": "value"})
        self.assertEqual(entry_id, "5-0")
        key_len: float = len(self.storage.storage_dict["autostream"].value)
        self.assertEqual(key_len, 1)

    async def test_xadd_auto_generate_sequence_number_existing_stream_time_non_zero(self):
        await self.storage.xadd("autostream", "0-*", {"field": "value"})
        entry_id = await self.storage.xadd("autostream", "5-*", {"field2": "value2"})
        self.assertEqual(entry_id, "5-0")
        key_len: float = len(self.storage.storage_dict["autostream"].value)
        self.assertEqual(key_len, 2)

    async def test_xadd_auto_generate_sequence_number_existing_stream_time_same_as_last(self):
        await self.storage.xadd("autostream", "1-*", {"field": "value"})
        entry_id = await self.storage.xadd("autostream", "1-*", {"field2": "value2"})
        self.assertEqual(entry_id, "1-1")
        key_len: float = len(self.storage.storage_dict["autostream"].value)
        self.assertEqual(key_len, 2)

    async def test_xadd_fully_auto_generated_id_new_stream(self):
        entry_id = await self.storage.xadd("autostream", "*", {"field": "value"})
        # ID should be current time in milliseconds-0
        current_millis = int(time.time() * 1000)

        # Time sometimes changes between calls, so allow for that
        expected_id_1 = f"{current_millis}-0"
        expected_id_2 = f"{current_millis+1}-0"

        should_be_true = entry_id == expected_id_1 or entry_id == expected_id_2
        self.assertTrue(should_be_true)
        key_len: float = len(self.storage.storage_dict["autostream"].value)
        self.assertEqual(key_len, 1)

    @patch('time.time', mock_time)
    async def test_xadd_fully_auto_generated_id_same_time_as_previous_entry(self):
        await self.storage.xadd("autostream", "*", {"field": "value"})
        entry_id = await self.storage.xadd("autostream", "*", {"field2": "value2"})
        
        # ID should be mock time in milliseconds-1
        expected_id = f"{int(time.time() * 1000)}-1"
        self.assertEqual(entry_id, expected_id)
        key_len: float = len(self.storage.storage_dict["autostream"].value)
        self.assertEqual(key_len, 2)

    async def test_xrange_basic(self):
        await self.storage.xadd("stream_key", "0-1", {"foo": "bar"})
        await self.storage.xadd("stream_key", "0-2", {"bar": "baz"})
        await self.storage.xadd("stream_key", "0-3", {"baz": "foo"})

        result = await self.storage.xrange("stream_key", "0-2", "0-3")

        expected = [
            [
                "0-2",
                [
                    "bar", "baz",
                ]
            ],
            [
                "0-3",
                [
                    "baz", "foo"
                ]
            ]
        ]

        self.assertEqual(result, expected)

    async def test_xrange_sequence_number_not_provided(self):
        await self.storage.xadd("some_key", "1526985054069-0", {"temperature": "36", "humidity": "95"})
        await self.storage.xadd("some_key", "1526985054079-0", {"temperature": "37", "humidity": "94"})
        

        result = await self.storage.xrange("some_key", "1526985054069", "1526985054079")

        expected = [
            [
                "1526985054069-0",
                [
                    "temperature", "36",
                    "humidity", "95"
                ]
            ],
            [
                "1526985054079-0",
                [
                    "temperature", "37",
                    "humidity", "94"
                ]
            ]
        ]

        self.assertEqual(result, expected)

    async def test_xrange_non_existent_stream(self):
        result = await self.storage.xrange("nope", "0-0", "9999-9999")
        self.assertEqual(result, [])

    async def test_xrange_start_greater_than_end(self):
        await self.storage.xadd("stream_key", "0-1", {"foo": "bar"})
        await self.storage.xadd("stream_key", "0-2", {"bar": "baz"})

        result = await self.storage.xrange("stream_key", "0-2", "0-1")
        self.assertEqual(result, [])

    async def test_xrange_errors_when_sequence_is_not_a_number(self):
        await self.storage.xadd("stream_key", "0-1", {"foo": "bar"})
        await self.storage.xadd("stream_key", "0-2", {"bar": "baz"})

        with self.assertRaises(ValueError) as context:
            await self.storage.xrange("stream_key", "0-one", "0-2")
        self.assertIn("ERR Invalid stream ID specified as stream command argument", str(context.exception))

    async def test_xrange_errors_when_milliseconds_is_not_a_number(self):
        await self.storage.xadd("stream_key", "0-1", {"foo": "bar"})
        await self.storage.xadd("stream_key", "0-2", {"bar": "baz"})

        with self.assertRaises(ValueError) as context:
            await self.storage.xrange("stream_key", "zero-1", "0-2")
        self.assertIn("ERR Invalid stream ID specified as stream command argument", str(context.exception))

    async def test_xrange_errors_when_sequence_is_negative(self):
        await self.storage.xadd("stream_key", "0-1", {"foo": "bar"})
        await self.storage.xadd("stream_key", "0-2", {"bar": "baz"})

        with self.assertRaises(ValueError) as context:
            await self.storage.xrange("stream_key", "-1-0", "0-2")
        self.assertIn("ERR Invalid stream ID specified as stream command argument", str(context.exception))

        with self.assertRaises(ValueError) as context:
            await self.storage.xrange("stream_key", "0-1", "0--2")
        self.assertIn("ERR Invalid stream ID specified as stream command argument", str(context.exception))

    async def test_xrange_errors_with_milliseconds_is_negative(self):
        await self.storage.xadd("stream_key", "0-1", {"foo": "bar"})
        await self.storage.xadd("stream_key", "0-2", {"bar": "baz"})

        with self.assertRaises(ValueError) as context:
            await self.storage.xrange("stream_key", "-5-0", "0-2")
        self.assertIn("ERR Invalid stream ID specified as stream command argument", str(context.exception))

    async def test_xrange_with_count(self):
        await self.storage.xadd("stream_key", "0-1", {"foo": "bar"})
        await self.storage.xadd("stream_key", "0-2", {"bar": "baz"})
        await self.storage.xadd("stream_key", "0-3", {"baz": "foo"})

        result = await self.storage.xrange("stream_key", "0-1", "0-3", count=2)

        expected = [
            [
                "0-1",
                [
                    "foo", "bar",
                ]
            ],
            [
                "0-2",
                [
                    "bar", "baz"
                ]
            ]
        ]

        self.assertEqual(result, expected)

    async def test_xrange_with_minus_as_start(self):
        await self.storage.xadd("stream_key", "0-1", {"foo": "bar"})
        await self.storage.xadd("stream_key", "0-2", {"bar": "baz"})
        await self.storage.xadd("stream_key", "0-3", {"baz": "foo"})

        result = await self.storage.xrange("stream_key", "-", "0-2")

        expected = [
            [
                "0-1",
                [
                    "foo", "bar",
                ]
            ],
            [
                "0-2",
                [
                    "bar", "baz"
                ]
            ]
        ]

        self.assertEqual(result, expected)

    async def test_xrange_with_plus_as_end(self):
        await self.storage.xadd("stream_key", "0-1", {"foo": "bar"})
        await self.storage.xadd("stream_key", "0-2", {"bar": "baz"})
        await self.storage.xadd("stream_key", "0-3", {"baz": "foo"})

        result = await self.storage.xrange("stream_key", "0-2", "+")

        expected = [
            [
                "0-2",
                [
                    "bar", "baz",
                ]
            ],
            [
                "0-3",
                [
                    "baz", "foo"
                ]
            ]
        ]

        self.assertEqual(result, expected)


class OtherDataStorageTests(BaseDataStorageTest):
    """
    FLUSHDB tests
    """
    
    async def test_flushdb_sync(self):
        await self.storage.set("key1", "value1")
        self.storage.flushdb_sync()
        self.assertEqual(len(self.storage.storage_dict), 0)


    async def test_flushdb_async(self):
        await self.storage.set("key2", "value2")
        await self.storage.flushdb_async()
        self.assertEqual(len(self.storage.storage_dict), 0)


if __name__ == "__main__":
    unittest.main()
