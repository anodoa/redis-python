"""
parser.py - module for parsing incoming commands by Redis protocol RESP2.

Contains functions for reading bulk-strings and arrays of commands from stream asyncio.StreamReader.

Functions:
- read_bulk_string(reader: asyncio.StreamReader) -> bytes | None —
  reads one bulk-string from incoming stream, returns its content or None if error.
- parser(reader: asyncio.StreamReader) -> list[bytes] | None —
  parses arguments array of command and returns list of bytes or None if data dont corresponds to RESP2 format.

Used in main cycle of clients processing for transformation incoming data into list of arguments before executing a command.
""" 


from collections import deque
from typing import Deque, Optional
import asyncio


async def read_bulk_string(reader: asyncio.StreamReader) -> bytes | None:
    """Reads clients bulk string for parser()"""

    try:
        len_line = await reader.readuntil(b"\r\n")
        
        if not len_line.startswith(b"$"):
            return None
        
        length = int(len_line[1:].strip())
        data = await reader.readexactly(length)
        await reader.readexactly(2) # reads \r\n after data
        
        if length == len(data):
            return data
    except asyncio.IncompleteReadError:
        return None
    
    return None


async def parser(reader: asyncio.StreamReader) -> Optional[Deque[bytes]]:
    """Function that parsing clients requests"""

    try:
        line = await reader.readuntil(b"\r\n")
    except asyncio.IncompleteReadError:
        return None

    if not line.startswith(b"*"):
        return None

    n_arr = int(line[1:-2])
    parts = deque()

    for _ in range(n_arr):
        data = await read_bulk_string(reader)
        if data is None:
            return None
        else:
            parts.append(data)
    
    return parts
