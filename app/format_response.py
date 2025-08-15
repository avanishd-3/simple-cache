def format_success_response(message: str) -> bytes:
    """
    Format a Redis response.
    """
    return f"+{message}\r\n".encode("utf-8")


def format_error_response(message: str) -> bytes:
    """
    Format a Redis error response.
    """
    return f"-{message}\r\n".encode("utf-8")

def format_bulk_response(message: str) -> bytes:
    """
    Format a Redis bulk response. Bulk responses are always successes.
    """
    return f"${len(message)}\r\n{message}\r\n".encode("utf-8")