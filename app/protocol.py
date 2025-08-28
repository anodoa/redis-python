"""
protocol.py - module that contains prtotocol constants, functions for bulk-string encoding, sending responses and errors.

Functions:
- encode_bulk_string(data: bytes) -> bytes - encoding data into bulk-string RESP2.
- encode_integer(data: bytes) -> bytes - encoding data into integer RESP2.
- send_response(data: bytes, writer: asyncio.StreamWriter) - asynchronously sending response to client.
- send_error(message: str, writer: asyncio.StreamWriter) - asynchronously sending error to client.
"""


import asyncio
from collections import deque
from typing import Deque


def encode_bulk_string(data: bytes) -> bytes:
    """Encoding data into bulk string"""

    return b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"


def encode_integer(data: bytes) -> bytes:
    """Encoding data into integer"""

    return f":{data}\r\n".encode()

def encode_array(data: Deque[bytes]) -> bytes:
    """Encoding deque of bytes in RESP2-array"""

    result = [f"*{len(data)}\r\n".encode()]
    for element in data:
        result.append(f"${len(element)}\r\n".encode())
        result.append(element + b"\r\n")
    return b"".join(result)


async def send_response(data: bytes, writer: asyncio.StreamWriter) -> None:
    """Writes answer to client"""

    writer.write(data)
    await writer.drain()


async def send_error(message: str, writer: asyncio.StreamWriter) -> None:
    """Writes error to client"""

    writer.write(f"-ERR {message}\r\n".encode())
    await writer.drain()