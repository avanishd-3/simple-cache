import unittest

from app.format_response import (
    format_simple_string,
    format_bulk_string_success,
    format_integer_success,
    format_resp_array,
    format_null_bulk_string,
)

class TestFormatResponse(unittest.IsolatedAsyncioTestCase):
    def test_format_simple_success(self):
        response: bytes = format_simple_string("PONG")
        self.assertEqual(response, b"+PONG\r\n")

    def test_format_bulk_success(self):
        response: bytes = format_bulk_string_success("bar")
        self.assertEqual(response, b"$3\r\nbar\r\n")

    def test_format_integer_success(self):
        response: bytes = format_integer_success(1)
        self.assertEqual(response, b":1\r\n")

    def test_format_resp_array_empty_array(self):
        response: bytes = format_resp_array([])
        self.assertEqual(response, b"*0\r\n")

    def test_format_resp_array_non_empty_array(self):
        response: bytes = format_resp_array(["a", "b", "c"])

        should_be: bytes = b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
        self.assertEqual(response, should_be)

    def test_format_null_bulk_string(self):
        response: bytes = format_null_bulk_string()
        self.assertEqual(response, b"$-1\r\n")

if __name__ == "__main__":
    unittest.main()
