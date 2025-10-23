import asyncio
import logging

# Internal imports
from app.format_response import (
    format_integer_success,
    format_resp_array,
    format_simple_error,
)
from app.data_storage import DataStorage
from app.utils import write_and_drain
from app.utils import OrderedSet
from app.utils import WRONG_TYPE_STRING
from app.data_storage import WrongTypeError

async def handle_set_commands(
    writer: asyncio.StreamWriter, command: str, args: list, storage: DataStorage
) -> None:
    """
    Handles set commands.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        command (str): The command to handle.
        args (list): The arguments provided with the command.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    commands_dict: dict = {
        "SADD": _handle_sadd,
        "SCARD": _handle_scard,
        "SDIFF": _handle_sdiff,
        "SDIFFSTORE": _handle_sdiff_store,
        "SINTER": _handle_sinter,
        "SINTERSTORE": _handle_sinter_store,
        "SUNION": _handle_sunion,
        "SUNIONSTORE": _handle_sunion_store,
        "SISMEMBER": _handle_sismember,
        "SMEMBERS": _handle_smembers,
        "SMOVE": _handle_smove,
        "SREM": _handle_srem,
    }
    handler = commands_dict.get(command.upper())
    if handler:
        await handler(writer, args, storage)
    else:
        logging.info(f"Unknown set command: {command}")
        await write_and_drain(writer, format_simple_error(f"ERR unknown set command: {command}"))

async def _handle_sadd(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SADD command.

    SADD adds one or more members to a set stored at key.
        If the key does not exist, a new set is created before adding the members.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 2:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sadd' command"))
        return

    key: str = args[0]

    # Get all set members to add
    set_members: list = args[1:] # All args after key

    logging.info(f"SADD: {key} = {set_members}")

    added_count: int = await storage.sadd(key, set_members)

    await write_and_drain(writer, format_integer_success(added_count))

async def _handle_scard(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SCARD command.

    SCARD returns the set cardinality (number of elements) of the set stored at key.
        If the key does not exist, 0 is returned.
        If the key exists but does not hold a set, an error is returned.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len != 1:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'scard' command"))
        return

    key: str = args[0]

    logging.info(f"SCARD: {key}")

    try:
        cardinality: int = await storage.scard(key)
    except WrongTypeError as e:
       await write_and_drain(writer, format_simple_error(str(e)))
       return

    await write_and_drain(writer, format_integer_success(cardinality))

async def _handle_sdiff(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SDIFF command.

    SDIFF returns the members of the set resulting from the difference between the first set and all the successive sets.
        If the key does not exist, it is considered an empty set.
        If the key exists but does not hold a set, an error is returned.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 1:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sdiff' command"))
        return

    # Get all keys to perform the difference operation on
    keys: list = args # All args

    logging.info(f"SDIFF: {keys}")

    try:
        difference_members: OrderedSet = await storage.sdiff(keys)
    except WrongTypeError as e:
       await write_and_drain(writer, format_simple_error(str(e)))
       return

    if not difference_members:
        await write_and_drain(writer, format_resp_array([])) # No members in set
    else:
        await write_and_drain(writer, format_resp_array(difference_members))

async def _handle_sdiff_store(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SDIFFSTORE command.

    SDIFFSTORE is SDIFF but stores the result in specified destination. If destination already exists, it is overwritten.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 2:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sdiffstore' command"))
        return

    # Get all keys to perform the difference operation on
    destination: str = args[0] # First arg is destination
    keys: list = args[1:] # All args after destination

    logging.info(f"SDIFFSTORE: {keys}")

    try:
        difference_members: OrderedSet = await storage.sdiff(keys)
    except WrongTypeError as e:
       await write_and_drain(writer, format_simple_error(str(e)))
       return
    
    await storage.set_overwrite(destination, difference_members)

    # RESP returns the number of members in the resulting set
    if not difference_members:
        await write_and_drain(writer, format_integer_success(0))
    else:
        await write_and_drain(writer, format_integer_success(len(difference_members)))

async def _handle_sinter(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SINTER command.

    SINTER returns the members of the set resulting from the intersection between all the sets.
        If the key does not exist, it is considered an empty set.
        If the key exists but does not hold a set, an error is returned.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 1:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sinter' command"))
        return

    # Get all keys to perform the i operation on
    keys: list = args # All args

    logging.info(f"SINTER: {keys}")

    try:
        intersection_members: OrderedSet = await storage.sinter(keys)
    except WrongTypeError as e:
       await write_and_drain(writer, format_simple_error(str(e)))
       return

    if not intersection_members:
        await write_and_drain(writer, format_resp_array([])) # No members in set
    else:
        await write_and_drain(writer, format_resp_array(intersection_members))

async def _handle_sinter_store(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SINTERSTORE command.

    SINTERSTORE is SINTER but stores the result in specified destination. If destination already exists, it is overwritten.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 2:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sinterstore' command"))
        return

    # Get all keys to perform the difference operation on
    destination: str = args[0] # First arg is destination
    keys: list = args[1:] # All args after destination

    logging.info(f"SINTERSTORE: {keys}")

    try:
        intersection_members: OrderedSet = await storage.sinter(keys)
    except WrongTypeError as e:
       await write_and_drain(writer, format_simple_error(str(e)))
       return
    
    await storage.set_overwrite(destination, intersection_members)

    # RESP returns the number of members in the resulting set
    if not intersection_members:
        await write_and_drain(writer, format_integer_success(0))
    else:
        await write_and_drain(writer, format_integer_success(len(intersection_members)))

async def _handle_sunion(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SUNION command.

    SUNION returns the members of the set resulting from the union of all the sets.
        If the key does not exist, it is considered an empty set.
        If the key exists but does not hold a set, an error is returned.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 1:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sunion' command"))
        return

    # Get all keys to perform the union operation on
    keys: list = args # All args

    logging.info(f"SUNION: {keys}")

    try:
        union_members: OrderedSet = await storage.sunion(keys)
    except WrongTypeError as e:
       await write_and_drain(writer, format_simple_error(str(e)))
       return
    
    if not union_members:
        await write_and_drain(writer, format_resp_array([])) # No members in set
    else:
        await write_and_drain(writer, format_resp_array(union_members))

async def _handle_sunion_store(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SUNIONSTORE command.

    SUNIONSTORE is SUNION but stores the result in specified destination. If destination already exists, it is overwritten.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len < 2:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sunionstore' command"))
        return

    # Get all keys to perform the union operation on
    destination: str = args[0] # First arg is destination
    keys: list = args[1:] # All args after destination

    logging.info(f"SUNIONSTORE: {keys}")

    try:
        union_members: OrderedSet = await storage.sunion(keys)
    except WrongTypeError as e:
       await write_and_drain(writer, format_simple_error(str(e)))
       return

    await storage.set_overwrite(destination, union_members)

    if not union_members:
        await write_and_drain(writer, format_integer_success(0))
    else:
        await write_and_drain(writer, format_integer_success(len(union_members)))

async def _handle_sismember(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SISMEMBER command.

    SISMEMBER returns if member is a member of the set stored at key.
        Returns 1 if the element is a member of the set.
        Returns 0 if the element is not a member of the set, or if key does not exist or is not a set.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len != 2:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'sismember' command"))
        return

    key: str = args[0]
    member: str = args[1]

    logging.info(f"SISMEMBER: {key}, {member}")

    set: OrderedSet = await storage.get(key)

    if set and isinstance(set, OrderedSet) and member in set:
        await write_and_drain(writer, format_integer_success(1))
    else:
        await write_and_drain(writer, format_integer_success(0))

async def _handle_smembers(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SMEMBERS command.

    SMEMBERS returns all the members of the set value stored at key.
        If the key does not exist, an empty set is returned.
        If the key exists but does not hold a set, an error is returned.

    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len != 1:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'smembers' command"))
        return

    key: str = args[0]

    logging.info(f"SMEMBERS: {key}")

    set_members: OrderedSet = await storage.get(key)

    if not set_members:
        await write_and_drain(writer, format_resp_array([])) # No members in set or key does not exist/is not a set
    elif not isinstance(set_members, OrderedSet): # What Redis does
        await write_and_drain(writer, format_simple_error(WRONG_TYPE_STRING))
    else:
        await write_and_drain(writer, format_resp_array(set_members))

async def _handle_smove(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SMOVE command.

    SMOVE moves member from the set at source to the set at destination. This operation is atomic for the other clients.
        If the source set does not exist, no operation is performed and 0 is returned.
        If the source exists but does not hold a set, an error is returned.
        If the destination set does not exist, it is created before the operation is performed.
        
    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len != 3:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'smove' command"))
        return

    source: str = args[0]
    destination: str = args[1]
    member: str = args[2]

    logging.info(f"SMOVE: {source}, {destination}, {member}")

    try:
        moved: bool = await storage.smove(source, destination, member)
    except WrongTypeError as e:
        await write_and_drain(writer, format_simple_error(str(e)))
        return

    if moved:
        await write_and_drain(writer, format_integer_success(1))
    else:
        await write_and_drain(writer, format_integer_success(0))

async def _handle_srem(writer: asyncio.StreamWriter, args: list, storage: DataStorage) -> None:
    """
    Handles the SREM command.

    SREM removes the specified members from the set stored at key.
        If the member is not a member of the set, it is ignored.
        If key does not exist, it is treated as an empty set and 0 is returned.
        Return error when value stored at key is not a set.
        
    Args:
        writer (asyncio.StreamWriter): The StreamWriter to write the response to.
        args (list): The arguments provided.
        storage (DataStorage): The DataStorage instance to interact with.
    """
    args_len: int = len(args)

    if args_len != 2:
        await write_and_drain(writer, format_simple_error("ERR wrong number of arguments for 'srem' command"))
        return

    key: str = args[0]
    members: list[str] = args[1:]

    logging.info(f"SREM: {key}, {members}")

    try:
        removed_count: int = await storage.srem(key, members)
    except WrongTypeError as e:
       await write_and_drain(writer, format_simple_error(str(e)))
       return

    await write_and_drain(writer, format_integer_success(removed_count))