def format_simple_string(message: str) -> bytes:
    """
    Format a simple RESP string.
    """
    return f"+{message}\r\n".encode("utf-8")

def format_bulk_string_success(message: str) -> bytes:
    """
    Format a RESP bulk success response.
    """
    return f"${len(message)}\r\n{message}\r\n".encode("utf-8")

def format_integer_success(value: int) -> bytes:
    """
    Return a RESP integer
    """
    return f":{value}\r\n".encode("utf-8")

def format_resp_array(elements: list[str]) -> bytes:
    """
    Return a RESP array
    """
    if len(elements) == 0:
        # Empty RESP array
        return "*0\r\n".encode("utf-8")
    else:
        array = "".join(f"${len(el)}\r\n{el}\r\n" for el in elements)
        return f"*{len(elements)}\r\n{array}".encode("utf-8")

def format_null_bulk_string() -> bytes:
    """
    Format a null bulk string RESP response.
    """
    return "$-1\r\n".encode("utf-8")

def format_simple_error(message: str) -> bytes:
    """
    Format a simple RESP error response.
    """
    return f"-{message}\r\n".encode("utf-8")
