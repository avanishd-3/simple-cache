from typing import Callable

def conditional_decorator(dec: Callable, condition: bool):
    """
    Apply the decorator `dec` to a function only if `condition` is True.
    """
    def decorator(func: Callable):
        if not condition:
            return func
        return dec(func)
    
    return decorator