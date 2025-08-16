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
                return []