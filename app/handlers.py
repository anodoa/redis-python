"""
handlers.py — module with implementations of handlers for supported commands.

Each function takes in list of command arguments (list[bytes])
and asyncio.StreamWriter object for sending response to client.

Function:
- handle_ping — responds with a PONG message to a command PING.
- handle_echo — responds to the ECHO command by returning the passed string.
- handle_set — saves key and value into database, supports PX flag.
- handle_get — returns value based on the key or an empty response if there is no key.
- handle_rpush - saves key and list of values into database.

Constants:
- PONG, OK, NBS — typical server responses.
- *_MESSAGE — messages about errors.

Used in execute_command() to call the appropriate handler,
depending on the received command.
"""


import asyncio
import time
from app.db import db
from app.protocol import send_response, send_error, encode_bulk_string, encode_integer


PONG = b"+PONG\r\n"
OK = b"+OK\r\n"
NBS = b"$-1\r\n" # Null bulk string
UNKNOWN_COMMAND_MESSAGE = "Unknown command"
WRONG_VALUE_MESSAGE = "Operation against a key holding the wrong kind of value"
CMD_PING = b"PING"
CMD_ECHO = b"ECHO"
CMD_SET = b"SET"
CMD_GET = b"GET"
ARG_PX = b"PX"
CMD_RPUSH = b"RPUSH"


async def handle_ping(parts: list[bytes], writer: asyncio.StreamWriter) -> None:
    """Handles ping command"""

    if len(parts) == 1:
        await send_response(PONG, writer)
    else:
        await send_error(UNKNOWN_COMMAND_MESSAGE, writer)


async def handle_echo(parts: list[bytes], writer: asyncio.StreamWriter) -> None:
    """Handles echo command"""

    if len(parts) == 2:
        await send_response(encode_bulk_string(parts[1]), writer)
    else:
        await send_error(UNKNOWN_COMMAND_MESSAGE, writer)


async def handle_set(parts: list[bytes], writer: asyncio.StreamWriter) -> None:
    """Handles set command"""

    key = parts[1]
    if len(parts) > 4 and parts[3].upper() == ARG_PX:
        expiry_sec = float(parts[4]) / 1000 # Parts[4] - expiry in ms
        db[key] = (parts[2], time.monotonic() + expiry_sec)
    else:
        db[key] = (parts[2], None)
    await send_response(OK, writer)


async def handle_get(parts: list[bytes], writer: asyncio.StreamWriter) -> None:
    """Handles get command"""

    key = parts[1]
    if key in db:
        value, expiry = db[key]
        if expiry is None or expiry > time.monotonic():
            await send_response(encode_bulk_string(value), writer)
        else:
            del db[key]
            await send_response(NBS, writer)
    else:
        await send_response(NBS, writer)


async def handle_rpush(parts: list[bytes], writer: asyncio.StreamWriter) -> None:
    """Handles rpush command"""

    key = parts[1]
    if key in db:
        value, expiry = db[key]
        
        if not isinstance(value, list):
            await send_error(WRONG_VALUE_MESSAGE, writer)
            return
        
        if expiry is None or expiry > time.monotonic():
            value.extend(parts[2:])
            db[key] = (value, expiry)
        else:
            del db[key]
            db[key] = (list(parts[2:]), None)
    
    else:
        db[key] = (list(parts[2:]), None)
    
    length_lst = len(db[key][0])
    await send_response(encode_integer(length_lst), writer)
        

handlers = {
    CMD_PING: handle_ping,
    CMD_ECHO: handle_echo,
    CMD_SET: handle_set,
    CMD_GET: handle_get,
    CMD_RPUSH: handle_rpush
}