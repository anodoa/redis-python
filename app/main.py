"""
main.py - the main module of the application.

Creates and starts a TCP server that accepts client connections
and processes commands in the Redis RESP2 format.

Functions:
- handle_client - command processing cycle for one client.
- execute_command - calling the appropriate handler by the command name.
- start_server - initialization of the TCP server on the specified HOST:PORT

Launch:
    python main.py

At startup, an asyncio server is created that accepts commands
and returns responses in accordance with the implementation of commands in handlers.py.
"""


import asyncio
from app.protocol import send_error
from app.handlers import handlers, UNKNOWN_COMMAND_MESSAGE
from app.parser import parser


HOST = "localhost"
PORT = 6379


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
