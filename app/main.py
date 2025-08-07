"""main.py - the main module of application which creates TCP-server, 
allow to parse clients requests and answer on them"""

from typing import Optional
import asyncio


HOST = "localhost"
PORT = 6379


async def parser(reader: asyncio.StreamReader) -> Optional[list[bytes]]:
    """parser - function that parsing clients request"""

    try:
        line = await reader.readuntil(b"\r\n")
    except asyncio.IncompleteReadError:
        return

    if not line.startswith(b"*"):
        return

    n_arr = int(line[1:-2])
    parts = []

    for _ in range(n_arr):
        try:
            len_line = await reader.readuntil(b"\r\n")
        except asyncio.IncompleteReadError:
            return

        if len_line.startswith(b"$"):
            length = int(len_line[1:-2])
            try:
                data = await reader.readexactly(length)
                await reader.readexactly(2)
            except asyncio.IncompleteReadError:
                return
            parts.append(data)

    return parts


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """Function that answer to clients request"""

    while True:
        parts = await parser(reader)

        if not parts:
            return

        if parts[0].upper() == b"PING":
            writer.write(b"+PONG\r\n")
            await writer.drain()
        else:
            continue

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