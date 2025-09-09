import unittest
import asyncio
import time

from app.data_storage import DataStorage

from typing import Type

class TestDataStorage(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.storage = DataStorage()

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

    async def test_xadd_appends_to_existing_stream(self):
        await self.storage.xadd("mystream", "1-0", {"field1": "value1"})
        entry_id = await self.storage.xadd("mystream", "1-1", {"field2": "value2"})
        self.assertEqual(entry_id, "1-1")
        key_len: float = len(self.storage.storage_dict["mystream"].value)
        self.assertEqual(key_len, 2)


if __name__ == "__main__":
    unittest.main()
