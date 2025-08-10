"""
protocol.py - module that contains prtotocol constants, functions for bulk-string encoding, sending responses and errors.

Functions:
- encode_bulk_string(data: bytes) -> bytes - encoding data into bulk-string RESP2.
- send_response(data: bytes, writer: asyncio.StreamWriter) - asynchronously sending response to client.
- send_error(message: str, writer: asyncio.StreamWriter) - asynchronously sending error to client.
"""


import asyncio


async def send_response(data: bytes, writer: asyncio.StreamWriter) -> None:
    """Function that writes answer"""

    writer.write(data)
    await writer.drain()


def encode_bulk_string(data: bytes) -> bytes:
    """Encoding data into bulk string"""

    return b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"


async def send_error(message: str, writer: asyncio.StreamWriter) -> None:
    """Writes error to client"""

    writer.write(f"-ERR {message}\r\n".encode())
    await writer.drain()