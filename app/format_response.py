def format_simple_success(message: str) -> bytes:
    """
    Format a simple Redis success response.
    """
    return f"+{message}\r\n".encode("utf-8")

def format_bulk_success(message: str) -> bytes:
    """
    Format a Redis bulk success response.
    """
    return f"${len(message)}\r\n{message}\r\n".encode("utf-8")

def format_integer_success(value: int) -> bytes:
    """
    Return a RESP integer
    """
    return f":{value}\r\n".encode("utf-8")

def format_bulk_error() -> bytes:
    """
    Format a Redis error response.
    """
    return "$-1\r\n".encode("utf-8")