import asyncio

class DataStorage():
    """
    Stores all data and provides concurrent-safe data access
    """
    
    def __init__(self):
        self.storage_dict = {}
        self.lock = asyncio.Lock()

    async def set(self, key: str, value: str) -> None:
        async with self.lock:
            self.storage_dict[key] = value

    async def get(self, key: str) -> str | None:
        async with self.lock:
            return self.storage_dict.get(key, None)
