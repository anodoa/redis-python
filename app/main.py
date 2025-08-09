"""main.py - the main module of application which creates TCP-server, 
allow to parse clients requests and execute their commands"""

from typing import Optional, Tuple
import asyncio
import time


HOST = "localhost"
PORT = 6379
PONG = b"+PONG\r\n"
OK = b"+OK\r\n"
NBS = b"$-1\r\n"
UNKNOWN_COMMAND_MESSAGE = "unknown command"
CMD_PING = b"PING"
CMD_ECHO = b"ECHO"
CMD_SET = b"SET"
CMD_GET = b"GET"
ARG_PX = b"PX"


db: dict[bytes, Tuple[bytes, float | None]] = {}


async def read_bulk_string(reader: asyncio.StreamReader) -> bytes | None:
    """reads clients bulk string for parser()"""

    try:
        len_line = await reader.readuntil(b"\r\n")
        
        if not len_line.startswith(b"$"):
            return None
        
        length = int(len_line[1:-2])
        data = await reader.readexactly(length)
        await reader.readexactly(2) # reads \r\n after data
        
        if length == len(data):
            return data
    except asyncio.IncompleteReadError:
        return None
    
    return None


async def parser(reader: asyncio.StreamReader) -> Optional[list[bytes]]:
    """Function that parsing clients requests"""

    try:
        line = await reader.readuntil(b"\r\n")
    except asyncio.IncompleteReadError:
        return None

    if not line.startswith(b"*"):
        return None

    n_arr = int(line[1:-2])
    parts = []

    for _ in range(n_arr):
        data = await read_bulk_string(reader)
        if data is None:
            return None
        else:
            parts.append(data)
    
    return parts


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

    if len(parts) > 4 and parts[3].upper() == ARG_PX:
        expiry_sec = float(parts[4]) / 1000 # Parts[4] - expiry in ms
        db[parts[1]] = (parts[2], time.monotonic() + expiry_sec)
    else:
        db[parts[1]] = (parts[2], None)
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


handlers = {
    CMD_PING: handle_ping,
    CMD_ECHO: handle_echo,
    CMD_SET: handle_set,
    CMD_GET: handle_get,
}


async def execute_command(parts: list[bytes], writer: asyncio.StreamWriter) -> None:
    """Function that executes clients commands"""

    if not parts:
        return

    cmd = parts[0].upper()
    handler = handlers.get(cmd)
    if handler:
        await handler(parts, writer)
    else:
        await send_error(UNKNOWN_COMMAND_MESSAGE, writer)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Function that answer to clients"""
    while True:
        parts = await parser(reader)
        if not parts:
            break
        await execute_command(parts, writer)
    writer.close()
    await writer.wait_closed()


async def start_server() -> None:
    """Function that creates server"""

    server = await asyncio.start_server(handle_client, HOST, PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(start_server())
