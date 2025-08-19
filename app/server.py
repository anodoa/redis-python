"""
server.py - module that implement asyncronous TCP-server of Redis-like database.

Contains:
- class Redis_Server - main server that accepts connections and processes commands.
- method handle_client - command processing cycle for one client.
- method execute_command - calling the appropriate handler by the command name.
- method start_server - initialization of the TCP server on the specified HOST:PORT.

Constants:
- PONG, OK, NBS — typical server responses.
- *_MESSAGE — messages about errors.

Used in execute_command() to call the appropriate handler,
depending on the received command:
- method handle_ping — responds with a PONG message to a command PING.
- method handle_echo — responds to the ECHO command by returning the passed string.
- method handle_set — saves key and value into database, supports PX flag.
- method handle_get — returns value based on the key or an empty response if there is no key.
- method handle_rpush - saves key and list of values into database.
- method handle_lrange - returns values of the array based on the key or an emtpy array if there is no key.

Server works in RAM: all keys and values stored in RAM and cleared when the programm ends.
"""


import asyncio 
import time
from typing import Tuple, Union, List, Optional 
from app.protocol import encode_array, send_response, send_error, encode_bulk_string, encode_integer, encode_array
from app.parser import parser


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
CMD_LRANGE = b"LRANGE"


class Redis_Server:

    def __init__(self, host="localhost", port=6379):
        self.host = host
        self.port = port
        self.db: dict[bytes, Tuple[Union[bytes, List[bytes]], Optional[float]]] = {} # database in RAM
        self.handlers = {
            CMD_PING: self.handle_ping,
            CMD_ECHO: self.handle_echo,
            CMD_SET: self.handle_set,
            CMD_GET: self.handle_get,
            CMD_RPUSH: self.handle_rpush,
            CMD_LRANGE: self.handle_lrange
        }


    async def start_server(self) -> None:
        """Function that creates server"""

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        async with server:
            await server.serve_forever()


    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Function that answer to clients"""
        while True:
            parts = await parser(reader)
            if not parts:
                break
            await self.execute_command(parts, writer)
        writer.close()
        await writer.wait_closed()


    async def execute_command(self, parts: list[bytes], writer: asyncio.StreamWriter) -> None:
        """Function that executes clients commands"""

        if not parts:
            return

        cmd = parts[0].upper()
        handler = self.handlers.get(cmd)
        
        if handler:
            await handler(parts, writer)
        else:
            await send_error(UNKNOWN_COMMAND_MESSAGE, writer)


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


    async def handle_set(self, parts: list[bytes], writer: asyncio.StreamWriter) -> None:
        """Handles set command"""

        key = parts[1]
        if len(parts) > 4 and parts[3].upper() == ARG_PX:
            expiry_sec = float(parts[4]) / 1000 # Parts[4] - expiry in ms
            self.db[key] = (parts[2], time.monotonic() + expiry_sec)
        else:
            self.db[key] = (parts[2], None)
        await send_response(OK, writer)


    async def handle_get(self, parts: list[bytes], writer: asyncio.StreamWriter) -> None:
        """Handles get command"""

        key = parts[1]
        if key in self.db:
            value, expiry = self.db[key]
            if expiry is None or expiry > time.monotonic():
                await send_response(encode_bulk_string(value), writer)
            else:
                del self.db[key]
                await send_response(NBS, writer)
        else:
            await send_response(NBS, writer)


    async def handle_rpush(self, parts: list[bytes], writer: asyncio.StreamWriter) -> None:
        """Handles rpush command"""

        key = parts[1]
        if key in self.db:
            value, expiry = self.db[key]
            
            if not isinstance(value, list):
                await send_error(WRONG_VALUE_MESSAGE, writer)
                return
            
            if expiry is None or expiry > time.monotonic():
                value.extend(parts[2:])
                self.db[key] = (value, expiry)
            else:
                del self.db[key]
                self.db[key] = (list(parts[2:]), None)
        
        else:
            self.db[key] = (list(parts[2:]), None)
        
        length_lst = len(self.db[key][0])
        await send_response(encode_integer(length_lst), writer)


    async def handle_lrange(self, parts: list[bytes], writer: asyncio.StreamWriter) -> None:
        """Handles lrange command"""

        key = parts[1]
        if key in self.db:
            value, expiry = self.db[key]

            if not isinstance(value, list):
                await send_error(WRONG_VALUE_MESSAGE, writer)
                return

            if expiry is None or expiry > time.monotonic():
                start = int(parts[2])
                end = int(parts[3])

                if start < 0:
                    start += len(value)
                    start = max(start, 0)

                if end < 0:
                    end += len(value)
                    end = min(end, len(value) - 1)

                if start > end:
                    await send_response(encode_array([]), writer)
                elif start <= end:
                    result = value[start:end+1]
                    await send_response(encode_array(result), writer)
            else:
                del self.db[key]
                await send_response(encode_array([]), writer)
        else:
            await send_response(encode_array([]), writer)
