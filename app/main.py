"""main.py - the main module of application which creates TCP-server, 
allow to parse clients requests and execute their commands"""

from typing import Optional
import asyncio


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


db: dict[bytes, bytes] = {}


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
        try:
            len_line = await reader.readuntil(b"\r\n")
        except asyncio.IncompleteReadError:
            return None

        if len_line.startswith(b"$"):
            length = int(len_line[1:-2])
            try:
                data = await reader.readexactly(length)
                await reader.readexactly(2)
            except asyncio.IncompleteReadError:
                return None
            if length == len(data):
                parts.append(data)
            else:
                return None

    return parts


async def send_response(writer: asyncio.StreamWriter, data: bytes) -> None:
    """Function that writes answer"""

    writer.write(data)
    await writer.drain()


async def encode_bulk_string(data: bytes) -> bytes:
    """Encoding data into bulk string"""

    return b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"


async def send_error(writer: asyncio.StreamWriter, message: str) -> None:
    """Writes error to client"""

    writer.write(f"-ERR {message}\r\n".encode())
    await writer.drain()


async def execute_command(
    parts: list[bytes], writer: asyncio.StreamWriter
) -> None:
    """Function that executes clients commands"""

    if not parts:
        return

    if parts[0].upper() == CMD_PING:
        await send_response(writer, PONG)
    elif parts[0].upper() == CMD_ECHO and len(parts) == 2:
        await send_response(writer, encode_bulk_string(parts[1]))
    elif parts[0].upper() == CMD_SET and len(parts) == 3:
        db[parts[1]] = parts[2]
        await send_response(writer, OK)
    elif parts[0].upper() == CMD_GET and len(parts) == 2:
        key = parts[1]
        if key in db:
            await send_response(writer, encode_bulk_string(db[key]))
        else:
            await send_response(writer, NBS)
    else:
        await send_error(writer, UNKNOWN_COMMAND_MESSAGE)


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
    loop = asyncio.new_event_loop()
    loop.run_until_complete(start_server())
