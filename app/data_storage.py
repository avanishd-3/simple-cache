import asyncio
from collections import namedtuple
import logging

import time

logging.basicConfig(level=logging.INFO)

ValueWithExpiry = namedtuple("ValueWithExpiry", ["value", "expiry_time"])

# TODO -> Turn lists into linked list from array (See: https://redis.io/docs/latest/develop/data-types/lists/)
class DataStorage():
    """
    Stores all data and provides concurrent-safe data access
    """

    def __init__(self):
        self.storage_dict = {}
        self.lock = asyncio.Lock()

    async def set(self, key: str, value: str, expiry_time: float | None = None) -> None:
        async with self.lock:
            self.storage_dict[key] = ValueWithExpiry(value, expiry_time)

    async def get(self, key: str) -> str | None:
        async with self.lock:
            # Do passive check: Delete expired keys when they are accessed

            logging.info(f"Retrieving value for key: {key}")

            item = self.storage_dict.get(key, None)
            curr_time = time.time()
            if item is not None and item.expiry_time is not None and curr_time > item.expiry_time:
                logging.info(f"Difference b/n curr time and expiry time: {curr_time - item.expiry_time}")
                logging.info(f"Deleting expired key: {key}")
                del self.storage_dict[key]
                return None

            if item is not None:
                logging.info(f"Retrieved value for key '{key}': {item.value}")
                return item.value
            else:
                logging.info(f"Key not found: {key}")
                return None
            
    async def rpush(self, key: str, items: list) -> int:
        """
        Add items to the end of a list stored at the specified key.

        Create the list with these items if it doesn't exist.

        Return length of list
        """
        async with self.lock:
            if key not in self.storage_dict:
                self.storage_dict[key] = ValueWithExpiry([], None)
                logging.info(f"Created new list for key: {key}")


            accessed_list: list = self.storage_dict[key].value
            accessed_list.extend(items) # Append but for an entire list
            logging.info(f"Appended {items} to list {key}")

        # Return number of elements in list
        return len(accessed_list)
    
    async def lpush(self, key: str, items: list) -> int:
        """
        Add items to the beginning of a list stored at the specified key in reverse order.

        Create the list with these items if it doesn't exist.

        Return length of the list
        """
        async with self.lock:
            if key not in self.storage_dict:
                self.storage_dict[key] = ValueWithExpiry([], None)
                logging.info(f"Created new list for key: {key}")

            accessed_list: list = self.storage_dict[key].value

            # Insert items at the start of the list
            # This is better than += b/c it avoids creating a new list
            # A new list would mean needing to update the storage dict, which harms cache performance for no reason

            item_len: int = len(items)
            for i in range(item_len):
                # Insert in reverse order
                # Subtract by 1 accounts for zero-indexing
                # Doing this is faster than reversing the list
                accessed_list.insert(i, items[item_len - i - 1])
            
            logging.info(f"Prepended {items} to list {key}")

        # Return number of elements in list
        return len(accessed_list)
    
    async def llen(self, key: str) -> int:
        """
        Return length of key

        Return 0 for non-existent key
        """
        async with self.lock:
            item = self.storage_dict.get(key, None)
            if item is not None and isinstance(item.value, list):
                logging.info(f"Retrieved length for key '{key}': {len(item.value)}")
                return len(item.value)
            else:
                logging.info(f"Key not found or not a list: {key}")
                return 0

    async def lrange(self, key: str, start: int, end: int) -> list:
        """
        Retrieve a range of elements from a list stored at the specified key.

        Start and end indices are inclusive.

        Negative indices are supported and count from the end of the list. Ex: -1 is last element, -2 is second-last element, and 
        so on.

        If negative index is >= length of list, it is treated as 0.

        Return an empty list if:
          - List does not exist
          - Start index is >= list length
          - Start index > stop index
        
        If stop index is >= list length, stop index is last element
        """

        if (start > end) and ((start > 0 and end > 0) or (start < 0 and end < 0)):
            logging.info(f"Start index {start} > end index {end} in search for {key}")
            return []


        async with self.lock:
            item = self.storage_dict.get(key, None)
            if item is not None and isinstance(item.value, list):
                list_len: int = len(item.value)

                logging.info(f"List is: {item.value}")

                if start >= list_len:
                    logging.info(f"Start index {start} >= list length {list_len} in search for {key}")
                    return []
                if end >= list_len:
                    logging.info(f"End index {end} >= list length {list_len} in search for {key}, treating last item as end")
                    end: int = list_len - 1 # Otherwise will overflow on last element
                
                if end == -1:
                    # Prevents empty list when we want to include the last element and using negative indexing
                    # Empty list will happen b/c index will be [start:0] -> makes empty list
                    logging.info(f"Negative end index {end} includes last element")
                    items_to_return: list = item.value[start:]
                elif start == -1:
                    # This must be the last element
                    logging.info(f"Negative start index {start} includes last element")
                    items_to_return: list = item.value[start:]
                else:
                    items_to_return: list = item.value[start:end+1]  # Redis treats end as inclusive

                logging.info(f"Retrieved elements from {key} from index {start} to {end}: {items_to_return}")
                return items_to_return
            else:
                logging.info(f"Key not found or not a list: {key}")
                return [] # RESP specification returns empty array for this
            
    async def lpop(self, key: str, count: int = 1) -> list | None:
        """
        Remove and return the specified number of elements from the left side of the list stored at the specified key.

        If the list does not exist or is empty, an empty list is returned.
        """
        async with self.lock:
            item = self.storage_dict.get(key, None)
            
            if item is not None and isinstance(item.value, list):
                if (len(item.value) == 0):
                    logging.info(f"List is empty: {key}")
                    return None  # RESP specification returns null bulk string for this
                else:
                    removed_items: list = item.value[:count]

                    # namedtuples are immutable by default, so need to create a new one
                    new_item = ValueWithExpiry(item.value[count:], item.expiry_time)
                    self.storage_dict[key] = new_item # Update value in storage

                    logging.info(f"Removed items from {key}: {removed_items}")
                    return removed_items
            
            else:
                logging.info(f"Key not found or not a list: {key}")
                return None # RESP specification returns null bulk string for this
            
    async def blpop(self, key: str, blocking_time: int = 0) -> tuple[str, list | None]:
        """
        Block for specified blocking time (in seconds) until an element is available in the list.

        If blocking time is 0, block indefinitely.
        """

        curr_time: float = time.time()
        stop_blocking_time: float = curr_time + blocking_time if blocking_time > 0 else float("inf")

        while curr_time < stop_blocking_time:
            async with self.lock:
                # Fetch item every time so we get the latest state
                item = self.storage_dict.get(key, None)
                if item is not None and isinstance(item.value, list) and len(item.value) > 0:
                    # Found an item to pop
                    logging.info(f"No longer blocked: found item to pop from {key}")
                    # Release lock so lpop can access the storage
                    self.lock.release()
                    # Get removed item
                    removed_item: str = (await self.lpop(key, count=1))[0]  # List will be only 1 element
                    # Acquire lock again so context manager works
                    await self.lock.acquire()
                    logging.info(f"Removed item from {key}: {removed_item}")
                    return {"list_name": key, "removed_item": removed_item}

            # So blocking times of 100 ms or less don't timeout immediately
            # This doesn't work for 101 ms or 102ms but these times are uncommon
            time_to_sleep: float = min(blocking_time / 10, 0.1) 
            await asyncio.sleep(time_to_sleep)
            curr_time = time.time()

        logging.info(f"Blocking pop timed out for {key}")
        return None  # RESP specification returns null bulk string for this