from collections.abc import Iterable

class OrderedSet:
    """
    A simple implementation of an ordered set using a dictionary to maintain order and a set for O(1) lookups.

    This works because dictionaries in Python 3.7+ maintain insertion order, so ignore the values and just use the keys.

    The item set exists for fast membership testing.
    """
    def __init__(self):
        self.items: dict = dict()
        self.item_set = set()

    def add(self, item):
        """
        Add an item to the ordered set if it's not already present.
        """
        if item not in self.item_set:
            self.items[item] = None
            self.item_set.add(item)

    def remove(self, item):
        """
        Remove an item from the ordered set if it exists."""
        if item in self.item_set:
            del self.items[item]
            self.item_set.remove(item)

    def update(self, items: Iterable):
        """
        Same as regular set update, adds all items from the iterable to the set.
        """
        for item in items:
            self.add(item)

    def difference_update(self, items: Iterable):
        """
        Same as regular set difference_update, removes all items in the iterable from the set.
        """
        for item in items:
            self.remove(item)

    def intersection_update(self, items: Iterable):
        """
        Same as regular set intersection_update, keeps only items that are also in the iterable.
        """
        items_to_keep = set(items)
        for item in list(self.items.keys()):
            if item not in items_to_keep:
                self.remove(item)

    def __contains__(self, item):
        return item in self.item_set

    def __iter__(self):
        return iter(self.items)

    def __len__(self):
        return len(self.items)
    
    def __eq__(self, other):
        """
        This is equal to other sets if they have the same items
        """
        
        if isinstance(other, OrderedSet):
            return self.item_set == other.item_set
        elif isinstance(other, set):
            try:
                return set(self.items.keys()) == other
            except TypeError:
                return False
            
    def __repr__(self):
        """
        Same as regular set

        Note: This is needed for tests to work properly
        """
        return f"{set(self.items.keys())}"