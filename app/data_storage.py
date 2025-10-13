import asyncio
from collections import namedtuple
import logging

from typing import Any, Type

import time

import heapq
from copy import copy

# Internal imports
from app.utils.ordered_set import OrderedSet

ValueWithExpiry = namedtuple("ValueWithExpiry", ["value", "expiry_time"])
BlockedClientFutureResult = namedtuple("BlockedClientFutureResult", ["key", "removed_item", "timestamp"])

class WrongTypeError(TypeError):
    def __init__(self):
        super().__init__("ERR WRONGTYPE Operation against a key holding the wrong kind of value")

class DataStorage():
    """
    Stores all data and provides concurrent-safe data access

    Note: 
    1. Using a per-key lock led to worse performance, probably due to the overhead of managing many locks.
    2. Lists are stored as Python lists instead of deque to improve cache performance and make indexing faster.
    """

    def __init__(self):
        self.storage_dict: dict[str, ValueWithExpiry] = {}
        self.lock = asyncio.Lock()
        # Is a heap
        self.blocked_clients = {}  # key: list blocking for, value: (timestamp, future, key)

    ############################################### Helpers ####################################################
        
    async def unblock_all_blocked_clients(self) -> None:
        """
        Unblock all blocked clients by setting their futures to None.
        This is used during server shutdown to ensure no clients remain blocked.
        """

        for key, blocked_list in self.blocked_clients.items():
            logging.info(f" Unblocking {len(blocked_list)} clients blocked on list: {key}")

            while len(blocked_list) > 0:
                client_timestamp: float = blocked_list[0][0]
                logging.info(f"Unblocking client with timestamp: {client_timestamp}")

                _, future, list_key = heapq.heappop(blocked_list)
                if not future.done():
                    future.set_result(None)
                else:
                    logging.info(f"Future already done for blocked client on list: {key}")

        self.blocked_clients.clear()

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

    async def get_ttl(self, key: str) -> float | None:
        """
        Get the time-to-live (TTL) for a key.

        This is only used for set KEEPTTL option.
        """
        async with self.lock:
            # Do passive check: Delete expired keys when they are accessed
            logging.info(f"Retrieving TTL for key: {key}")

            item = self.storage_dict.get(key, None)
            curr_time = time.time()
            
            if item is not None and item.expiry_time is not None and curr_time > item.expiry_time:
                logging.info(f"Difference b/n curr time and expiry time: {curr_time - item.expiry_time}")
                logging.info(f"Deleting expired key: {key}")
                return None
            
            if item is not None:
                logging.info(f"Retrieved TTL for key '{key}': {item.expiry_time}")
                return item.expiry_time
            else:
                logging.info(f"Key not found when retrieving TTL: {key}")
                return None

    ############################################### General ####################################################

    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in the storage.

        Return True if the key exists, False otherwise.
        """

        async with self.lock:
            return key in self.storage_dict
        
    # TODO: Add support for set, zset, hash, stream
    async def key_type(self, key: str) -> Type[None | str | list | dict | OrderedSet]:
        """
        Return type of key
        """
        async with self.lock:
            item = self.storage_dict.get(key, None)
            if item is None:
                logging.info(f"Key not found: {key}")
                return Type[None]
            elif isinstance(item.value, str):
                logging.info(f"Key '{key}' is of type string")
                return Type[str]
            elif isinstance(item.value, list):
                logging.info(f"Key '{key}' is of type list")
                return Type[list]
            elif isinstance(item.value, dict):
                logging.info(f"Key '{key}' is of type stream")
                return Type[dict]
            elif isinstance(item.value, OrderedSet):
                logging.info(f"Key '{key}' is of type set")
                return Type[OrderedSet]
            else:
                logging.info(f"Key '{key}' is of unknown type")
                return Type[None]
        
    async def delete(self, key: str) -> bool:
        """
        Remove the specified key.

        Return True if the key was removed, False if the key did not exist.
        """
        async with self.lock:
            if key in self.storage_dict:
                del self.storage_dict[key]
                logging.info(f"Deleted key: {key}")
                return True
            else:
                logging.info(f"Key not found for deletion: {key}")
                return False
        
    async def flushdb_async(self) -> None:
        """
        Remove all keys from the current database.
        """
        async with self.lock:
            self.storage_dict.clear()
            logging.info("Flushed all data from the database (async)")

    def flushdb_sync(self) -> None:
        """
        Synchronous version of flushdb
        """
        self.storage_dict.clear()
        logging.info("Flushed all data from the database (sync)")
        
    ############################################### Strings ####################################################

    async def set(self, key: str, value: str, expiry_time: float | None = None) -> None:
        async with self.lock:
            self.storage_dict[key] = ValueWithExpiry(value, expiry_time)

    async def get(self, key: str) -> str | list | dict | OrderedSet | None:
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
            
    ############################################### Lists ####################################################
            
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

        If the list exists and has elements, pop the first element and return immediately.
        """

        # Check if list exists and has elements
        # This works b/c lpop doesn't do anything if list is empty or doesn't exist
        # So we can just call it and see if it returns something
        lpop_result = await self.lpop(key, 1)
        if lpop_result is not None:
            logging.info(f"List {key} has items before BLPOP call, returning immediately")
            return {"list_name": key, "removed_item": lpop_result[0]}
        
        # Block if list does not exist or is empty
        logging.info(f"Blocking on list: {key} with timeout: {timeout}")

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
        
    ############################################### Streams ####################################################
        
    # TODO: Implement stream as radix trie instead of dict
    async def xadd(self, key: str, id: str, field_value_pairs: dict) -> str:
        """
        Add an entry to a stream stored at the specified key.

        Create the stream if it doesn't exist.

        The id can be specified in 3 formats: 
           1. Explicitly specified as <milliseconds>-<sequence number>
           2. Partially auto-generated as <milliseconds>-*
           3. Fully auto-generated as *
        """

        # Validate entry ID before adding entry to the stream
        # Entry ID must be in the format <milliseconds>-<sequence number>

        auto_generate_milliseconds: bool = False
        auto_generate_sequence_number: bool = False

        if id == "*":
            auto_generate_milliseconds = True
            auto_generate_sequence_number = True
            logging.info(f"Need to auto-generate milliseconds and sequence number in stream with key {key}")

            # Use current Unix time in milliseconds for time and 0 for sequence number
            # Needs to be int for RESP response
            milliseconds = int(time.time() * 1000)
            sequence_number = 0 # Will be updated below if time already exists in stream

        else:
            id_parts = id.split("-")
            if len(id_parts) != 2:
                # Will catch negative milliseconds or sequence numbers
                logging.info(f"Failed to add entry to stream with key {key} b/c ID {id} is not in correct format")
                raise ValueError("ERR Invalid stream ID specified as stream command argument")
       
            try:
                milliseconds = int(id_parts[0])
                sequence_number = int(id_parts[1])
            except ValueError:
                # Check if sequence number needs to be auto-generated
                if id_parts[1] == "*":
                    logging.info(f"Need to auto-generate sequence number for ID {id} in stream with key {key}")
                    auto_generate_sequence_number = True
                    
                else:
                    logging.info(f"Failed to add entry to stream with key {key} b/c ID {id} is not in correct format")
                    raise ValueError("ERR Invalid stream ID specified as stream command argument")
        
        # Check that ID is greater than 0-0 for explicitly specified IDs
        if not auto_generate_milliseconds and not auto_generate_sequence_number and milliseconds == 0 and sequence_number == 0:
            logging.info(f"Failed to create stream with key {key} b/c ID was 0-0")
            raise ValueError("ERR The ID specified in XADD must be greater than 0-0")
        
        async with self.lock:
            # Check that milliseconds is >= last entry's milliseconds
            if key in self.storage_dict:
                stream: dict = self.storage_dict[key].value
                if len(stream) > 0:
                    last_entry_id = list(stream.keys())[-1]
                    last_id_parts = last_entry_id.split("-")
                    last_milliseconds = int(last_id_parts[0])
                    last_sequence_number = int(last_id_parts[1])

                    if auto_generate_sequence_number:
                        # By definition, if the stream contains the same timestamp, it must be in the last entry
                        # Default sequence number is 0 except when the time part is also 0
                        # If time part is 0, then default sequence number is 1
                        if milliseconds == 0:
                            sequence_number = last_sequence_number + 1 if milliseconds == last_milliseconds else 1
                        else:
                            sequence_number = last_sequence_number + 1 if milliseconds == last_milliseconds else 0

                        id = f"{milliseconds}-{sequence_number}"
                        logging.info(f"Auto-generated sequence number, new ID is {id} for existing stream with key {key}")

                    elif auto_generate_milliseconds:
                        if milliseconds == last_milliseconds:
                            sequence_number = last_sequence_number + 1

                        id = f"{milliseconds}-{sequence_number}"
                        logging.info(f"Auto-generated id, new ID is {id} for existing stream with key {key}")
                    
                    else:
                        if milliseconds < last_milliseconds or (milliseconds == last_milliseconds and sequence_number <= last_sequence_number):
                            logging.info(f"Failed to add entry to stream with key {key} b/c ID {id} is not greater than last entry ID {last_entry_id}")
                            raise ValueError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
                    
            # Add entry / create stream if it doesn't exist
            if key not in self.storage_dict:
                if auto_generate_sequence_number:
                    # If stream doesn't exist, then this must be the first entry
                    # Default sequence number is 0 except when the time part is also 0
                    # If time part is 0, then default sequence number must be 1
                    sequence_number = 1 if milliseconds == 0 else 0

                    id = f"{milliseconds}-{sequence_number}"
                    logging.info(f"Auto-generated sequence number, new ID is {id} for new stream with key {key}")

                # Add entry
                self.storage_dict[key] = ValueWithExpiry({}, None)
                logging.info(f"Created new stream for key: {key}")

            accessed_stream: dict = self.storage_dict[key].value
            accessed_stream[id] = field_value_pairs
            logging.info(f"Appended {field_value_pairs} to stream {key}")

            logging.info(f"Stream {key} after XADD: {accessed_stream}")

        # RESP specification returns the ID of the entry created for this
        return id
    
    async def xrange(self, key: str, start: str, end: str, count: int | None = None) -> list:
        """
        Retrieve a range of entries from a stream stored at the specified key.

        Start and end IDs are inclusive.

        If the sequence number is not specified, default to 0 for start and max sequence number for end.

        The special ID "-" can be used to specify the smallest ID in the stream.

        The special ID "+" can be used to specify the largest ID in the stream.

        If count is specified, return at most count entries.
        """

        def id_less_than_equal(id1: str, id2: str) -> bool:
            """
            Return True if id1 <= id2
            """
            if id1 == "-" or id2 == "+":
                return True
            if id1 == "+" or id2 == "-":
                return False

            id1_parts = id1.split("-")
            id2_parts = id2.split("-")

            # Handle negative sequence numbers or milliseconds
            if len(id1_parts) > 2 or len(id2_parts) > 2:
                raise ValueError("ERR Invalid stream ID specified as stream command argument")

            # Guaranteed to have at least 1 part
            try:
                milliseconds1 = int(id1_parts[0])
                milliseconds2 = int(id2_parts[0])
            except ValueError:
                raise ValueError("ERR Invalid stream ID specified as stream command argument")
           
            try:
                sequence_number1 = int(id1_parts[1])
                sequence_number2 = int(id2_parts[1])
            except IndexError:
                sequence_number1 = 0

                # Get max sequence number for milliseconds2
                sequence_number2 = max(
                    (int(entry_id.split("-")[1]) for entry_id in stream.keys() if entry_id.startswith(f"{milliseconds2}-")),
                    default=0
                )
            except ValueError: # If sequence number is not specified as an integer
                raise ValueError("ERR Invalid stream ID specified as stream command argument")

            if milliseconds1 < milliseconds2:
                return True
            if milliseconds1 > milliseconds2:
                return False
            return sequence_number1 <= sequence_number2

        async with self.lock:
            item = self.storage_dict.get(key, None)
            if item is not None and isinstance(item.value, dict):
                stream: dict = item.value
                entries: list = []

                for entry_id, field_value_pairs in stream.items():
                    if id_less_than_equal(start, entry_id) and id_less_than_equal(entry_id, end):
                        entries.append([entry_id, [str(k) for pair in field_value_pairs.items() for k in pair]])
                        if count is not None and len(entries) >= count:
                            break

                logging.info(f"Retrieved entries from {key} from ID {start} to {end}: {entries}")
                return entries
            else:
                logging.info(f"Key not found or not a stream: {key}")
                return []

    ############################################### Sets ####################################################

    async def set_overwrite(self, key: str, members: set) -> None:
        """
        Overwrite the set at the specified key with the provided members.

        Create the set with these members if it doesn't exist.

        Note: This is only used for sdiffstore to overwrite the destination set.
        """
        async with self.lock:
            self.storage_dict[key] = ValueWithExpiry(members, None)
            logging.info(f"Overwrote set for key {key} with members {members}")

    async def sadd(self, key: str, members: list) -> int:
        """
        Add one or more members to a set stored at the specified key.

        Create the set with these members if it doesn't exist.

        Return the number of elements that were added to the set, not including all the elements already present in the set.
        """
        async with self.lock:
            if key not in self.storage_dict:
                self.storage_dict[key] = ValueWithExpiry(OrderedSet(), None)
                logging.info(f"Created new set for key: {key}")

            accessed_set: OrderedSet = self.storage_dict[key].value
            initial_size: int = len(accessed_set)
            accessed_set.update(members) # Duplicate members are ignored
            logging.info(f"Added {members} to set {key}")

            # Return number of new elements added to the set
            return len(accessed_set) - initial_size
        
    async def scard(self, key: str) -> int:
        """
        Return the set cardinality (number of elements) of the set stored at key.

        If the key does not exist, return 0.
        """
        async with self.lock:
            item = self.storage_dict.get(key, None)
            if item is not None and isinstance(item.value, OrderedSet):
                logging.info(f"Retrieved cardinality for key '{key}': {len(item.value)}")
                return len(item.value)
            else:
                logging.info(f"Key not found or not a set: {key}")
                return 0
            
    async def sdiff(self, keys: list) -> OrderedSet:
        """
        Return the members of the set resulting from the difference between the first set and all the successive sets.

        If the first set does not exist, return an empty set.

        Assumes keys has at least one element b/c command handler checks for this
        """

        async with self.lock:
            first_key = keys[0]
            first_set_item = self.storage_dict.get(first_key, None)
            if first_set_item is None or not isinstance(first_set_item.value, OrderedSet):
                logging.info(f"First key not found or not a set: {first_key}")
                return OrderedSet()  # RESP specification returns empty array for this

            result_set: OrderedSet = copy(first_set_item.value)

            for key in keys[1:]:
                item = self.storage_dict.get(key, None)
                if item is not None and isinstance(item.value, OrderedSet):
                    result_set.difference_update(item.value)

            logging.info(f"Set difference for keys {keys}: {result_set}")
            return result_set
        
    async def sinter(self, keys: list) -> OrderedSet:
        """
        Return intersection of all sets.

        If the first set does not exist, return an empty set.

        Assumes keys has at least one element b/c command handler checks for this
        """

        async with self.lock:

            first_key = keys[0]
            first_set_item = self.storage_dict.get(first_key, None)
            if first_set_item is None or not isinstance(first_set_item.value, OrderedSet):
                logging.info(f"First key not found or not a set: {first_key}")
                return OrderedSet()  # RESP specification returns empty array for this

            result_set: OrderedSet = copy(first_set_item.value)

            for key in keys[1:]:
                item = self.storage_dict.get(key, None)
                if item is not None and isinstance(item.value, OrderedSet):
                    result_set.intersection_update(item.value)
                else:
                    # If any set doesn't exist, intersection is empty set
                    logging.info(f"Key not found or not a set: {key}, intersection is empty set")
                    return OrderedSet()

            logging.info(f"Set intersection for keys {keys}: {result_set}")
            return result_set
        
    async def sunion(self, keys: list) -> OrderedSet:
        """
        Return union of all sets. Non-existent keys are treated as empty sets (so they are ignored).
        """

        async with self.lock:

            result_set: OrderedSet = OrderedSet()

            for key in keys:
                item = self.storage_dict.get(key, None)
                if item is not None and isinstance(item.value, OrderedSet):
                    result_set.update(item.value)

            logging.info(f"Set union for keys {keys}: {result_set}")
            return result_set
        
    async def smove(self, source: str, destination: str, member: str) -> bool:
        """
        Move a member from the source set to the destination set.

        Return True if the element was moved, False if the element was not found in the source set.
        """

        async with self.lock:
            try:
                source_item = self.storage_dict.get(source, None).value
            except AttributeError: 
                logging.info(f"Source key not found: {source}")
                source_item = None

            try:
                destination_item = self.storage_dict.get(destination, None).value
            except AttributeError:
                destination_item = None
                logging.info(f"Destination key not found: {destination}")

            # If source is not a set or doesn't exist or destination exists and is not a set, return False
            if not isinstance(source_item, OrderedSet) or (destination_item is not None and not isinstance(destination_item, OrderedSet)):
                 logging.info("Source or destination is not a set, cannot perform SMOVE")
                 return False
        
            if member in source_item:
                source_item.remove(member)
                if destination_item is None:
                    destination_item = OrderedSet()
                    self.storage_dict[destination] = ValueWithExpiry(destination_item, None)
                destination_item.add(member)
                logging.info(f"Moved member {member} from source set to destination set")
                return True
            else:
                logging.info(f"Member {member} not found in source set, not moved")
                return False
            
    async def srem(self, key: str, members: list) -> int:
        """
        Remove one or more members from a set stored at the specified key.

        Return the number of members that were removed from the set, not including non-existing members.
        """
        async with self.lock:
            item = self.storage_dict.get(key, None)
            if item is None:
                logging.info(f"Key not found: {key}")
                return 0  # RESP specification returns 0 for this
            elif not isinstance(item.value, OrderedSet):
                logging.info(f"Key not a set: {key}")
                raise WrongTypeError()  # RESP specification returns error for this

            accessed_set: OrderedSet = item.value
            initial_size: int = len(accessed_set)
            for member in members:
                accessed_set.remove(member)
            logging.info(f"Removed {members} from set {key}")

            # Return number of elements removed from the set
            return initial_size - len(accessed_set)