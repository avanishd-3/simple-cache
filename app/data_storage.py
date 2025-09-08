import asyncio
from collections import namedtuple
import logging

from typing import Any

import time

import heapq

logging.basicConfig(level=logging.INFO)

ValueWithExpiry = namedtuple("ValueWithExpiry", ["value", "expiry_time"])
BlockedClientFutureResult = namedtuple("BlockedClientFutureResult", ["key", "removed_item", "timestamp"])

# TODO -> Turn lists into linked list from array (See: https://redis.io/docs/latest/develop/data-types/lists/)
class DataStorage():
    """
    Stores all data and provides concurrent-safe data access
    """

    def __init__(self):
        self.storage_dict = {}
        self.lock = asyncio.Lock()
        # Is a heap
        self.blocked_clients = {}  # key: list blocking for, value: (timestamp, future, key)

    def _unblock_clients_and_pop(self, key: str, accessed_list: list) -> None:
        """
        Unblock any clients that used BLPOP to wait on this list.
        Pop the first item from the list for each unblocked client.
        """

        # Need to make sure futures result are set based on their timestamp
        # This is done by default, since dicts are ordered based on insertion order in Python 3.7+
        futures_to_set: dict[asyncio.Future[Any], BlockedClientFutureResult] = {}
        item_to_remove = accessed_list[0] if len(accessed_list) > 0 else None

        # Unblock any clients waiting on this list
        if key in self.blocked_clients and len(self.blocked_clients[key]) > 0:
            num_blocked_clients: int = len(self.blocked_clients[key])
            logging.info(f" Unblocking {num_blocked_clients} clients blocked on list: {key}")

            while len(self.blocked_clients[key]) > 0:
                client_timestamp: float = self.blocked_clients[key][0][0]
                logging.info(f"Unblocking client with timestamp: {client_timestamp}")

                _, future, list_key = heapq.heappop(self.blocked_clients[key])
                if not future.done():
                    # namedtuples are immutable by default, so need to create a new one
                    # TODO: Add expiry time support for lists
                    new_item = ValueWithExpiry(accessed_list, None)
                    logging.info(f"List after unblocking client w/ timestamp {client_timestamp}: {new_item.value}")
                    self.storage_dict[key] = new_item # Update value in storage
                    futures_to_set[future] = BlockedClientFutureResult(list_key, item_to_remove, client_timestamp)
                else:
                    logging.info(f"Future already done for blocked client on list: {key}")

        # Set results here so async doesn't take over and continue w/ BLPOP
        if len(futures_to_set) > 0:
            removed_item = accessed_list.pop(0)
        for future, blocked_info in futures_to_set.items():
            new_blocked_info = BlockedClientFutureResult(blocked_info.key, removed_item, blocked_info.timestamp)
            future.set_result(new_blocked_info)

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

        # Need to get it here b/c list length may have changed after unblocking clients
        list_len: int = len(accessed_list)

        # Unblock any clients waiting on this list
        self._unblock_clients_and_pop(key, accessed_list)

        # Return number of elements in list
        return list_len

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

        # Need to get it here b/c list length may have changed after unblocking clients
        list_len: int = len(accessed_list)

        # Unblock any clients waiting on this list
        self._unblock_clients_and_pop(key, accessed_list)

        # Return number of elements in list
        return list_len
    
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
            
    async def blpop(self, key: str, timeout: int = 0) -> tuple[str, list | None]:
        """
        Block for specified blocking time (in seconds) until an element is available in the list.

        If blocking time is 0, block indefinitely.
        """

        future = asyncio.get_event_loop().create_future()
        curr_time: float = time.time()

        if key not in self.blocked_clients:
            self.blocked_clients[key] = []
        heapq.heappush(self.blocked_clients[key], (curr_time, future, key))

        try:
            await asyncio.wait_for(future, timeout=timeout if timeout > 0 else None)
            blocked_info: BlockedClientFutureResult = future.result()
            logging.info(f"BLPOP -> Removed {blocked_info.removed_item} from {blocked_info.key} for client w/ timestamp {blocked_info.timestamp}")
            return {"list_name": blocked_info.key, "removed_item": blocked_info.removed_item}
        except asyncio.TimeoutError:
            # Remove from queue if timed out
            logging.info(f"TimeoutError in blpop for key: {key}")

            # Remove blocked client from queue
            if key in self.blocked_clients:
                # Rebuild heap without the timed-out future
                self.blocked_clients[key] = [(t, f, k) for (t, f, k) in self.blocked_clients[key] if f != future]
                heapq.heapify(self.blocked_clients[key])

            return None # RESP specification returns null bulk string for this