"""
server.py - module that implement asyncronous TCP-server of Redis-like database.

Contains:
- class Redis_Server - main server that accepts connections and processes commands.
- method handle_client - command processing cycle for one client.
- method execute_command - calling the appropriate handler by the command name.
- method start_server - initialization of the TCP server on the specified HOST:PORT.

Used in execute_command() to call the appropriate handler,
depending on the received command:
- method handle_ping — responds with a PONG message to a command PING.
- method handle_echo — responds to the ECHO command by returning the passed string.
- method handle_set — saves key and value into database, supports PX flag.
- method handle_get — returns value based on the key or an empty response if there is no key.
- method handle_rpush - saves key and list of values into database.
- method handle_lrange - returns values of the array based on the key or an emtpy array if there is no key.
- method handle_lpush - saves key and list of values into database like rpush, but in reversed order.
- method handle_llen - used to query a list's length. It returns a RESP-encoded integer.

Constants:
- PONG, OK, NBS — typical server responses.
- *_MESSAGE — messages about errors.

Server works in RAM: all keys and values stored in RAM and cleared when the programm ends.
"""

import asyncio
import time
from typing import Tuple, Union, List, Optional, Dict
from abc import ABC, abstractmethod
from app.protocol import (
    encode_array,
    send_response,
    send_error,
    encode_bulk_string,
    encode_integer,
)
from app.parser import parser


PONG = b"+PONG\r\n"
OK = b"+OK\r\n"
NBS = b"$-1\r\n"  # Null bulk string
UNKNOWN_COMMAND_MESSAGE = "Unknown command"
WRONG_VALUE_MESSAGE = "Operation against a key holding the wrong kind of value"
CMD_PING = b"PING"
CMD_ECHO = b"ECHO"
CMD_SET = b"SET"
CMD_GET = b"GET"
ARG_PX = b"PX"
CMD_RPUSH = b"RPUSH"
CMD_LRANGE = b"LRANGE"
CMD_LPUSH = b"LPUSH"
CMD_LLEN = b"LLEN"


class AbstractDatabase(ABC):
    @abstractmethod
    def set_(
        self,
        key: bytes,
        value: Union[bytes, List[bytes]],
        expire: Optional[float] = None,
    ) -> None: ...

    @abstractmethod
    def delete(self, key: bytes): ...

    @abstractmethod
    def get_(
        self, key: bytes
    ) -> Optional[Tuple[Union[bytes, List[bytes]], Optional[float]]]: ...

    @abstractmethod
    def rpush(self, key: bytes, values: List[bytes]) -> int: ...

    @abstractmethod
    def lrange(self, key: bytes, start: int, end: int) -> List[bytes]: ...

    @abstractmethod
    def lpush(self, key: bytes, values: List[bytes]) -> int: ...

    @abstractmethod
    def llen(self, key: bytes) -> int: ...


class InMemoryDB(AbstractDatabase):
    def __init__(self):
        self.db: Dict[bytes, Tuple[Union[bytes, List[bytes]], Optional[float]]] = {}

    def __repr__(self):
        return f"{self.__class__.__name__}(keys={len(self.db)})"

    def set_(
        self,
        key: bytes,
        value: Union[bytes, List[bytes]],
        expire: Optional[float] = None,
    ):
        expiry_time = time.monotonic() + expire if expire else None
        self.db[key] = (value, expiry_time)

    def delete(self, key: bytes):
        self.db.pop(key, None)

    def get_(self, key: bytes):
        item = self.db.get(key)
        if item is None:
            return None
        value, expiry = item
        if expiry is None or expiry > time.monotonic():
            return value, expiry
        self.delete(key)
        return None

    def rpush(self, key: bytes, values: List[bytes]) -> int:
        current = self.get_(key)
        if current and not isinstance(current[0], list):
            raise ValueError(WRONG_VALUE_MESSAGE)
        new_list = current[0] if current else []
        new_list.extend(values)
        expiry = current[1] if current else None
        self.set_(key, new_list, expire=(expiry - time.monotonic()) if expiry else None)
        return len(new_list)

    def lrange(self, key: bytes, start: int, end: int) -> List[bytes]:
        current = self.get_(key)
        if not current:
            return []
        value, _ = current
        if not isinstance(value, list):
            raise ValueError(WRONG_VALUE_MESSAGE)
        length = len(value)
        if start < 0:
            start += length
            start = max(start, 0)
        if end < 0:
            end += length
            end = min(end, length - 1)
        if start > end:
            return []
        return value[start : end + 1]

    def lpush(self, key: bytes, values: List[bytes]) -> int:
        current = self.get_(key)
        if current and not isinstance(current[0], list):
            raise ValueError(WRONG_VALUE_MESSAGE)
        old_list = current[0] if current else []
        new_list = values[::-1] + old_list
        expiry = current[1] if current else None
        self.set_(key, new_list, expire=(expiry - time.monotonic()) if expiry else None)
        return len(new_list)

    def llen(self, key:bytes) -> int:
        current = self.get_(key)
        if not current:
            return 0
        if not isinstance(current[0], list):
            raise ValueError(WRONG_VALUE_MESSAGE)
        return len(current[0])


class RedisServer:

    def __init__(self, host="localhost", port=6379, db: AbstractDatabase = None):
        self.host = host
        self.port = port
        self.db: AbstractDatabase = db or InMemoryDB()
        self.handlers = {
            CMD_PING: self.handle_ping,
            CMD_ECHO: self.handle_echo,
            CMD_SET: self.handle_set,
            CMD_GET: self.handle_get,
            CMD_RPUSH: self.handle_rpush,
            CMD_LRANGE: self.handle_lrange,
            CMD_LPUSH: self.handle_lpush,
            CMD_LLEN: self.handle_llen,
        }

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(host = {self.host!r}, port = {self.port!r},"
            f"keys = {len(self.db.db) if isinstance(self.db, InMemoryDB) else '?'},"
            f"handlers = {list(self.handlers.keys())})"
        )

    async def start_server(self) -> None:
        """Function that creates server"""

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        async with server:
            await server.serve_forever()

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Function that answer to clients"""
        while True:
            parts = await parser(reader)
            if not parts:
                break
            await self.execute_command(parts, writer)
        writer.close()
        await writer.wait_closed()

    async def execute_command(
        self, parts: list[bytes], writer: asyncio.StreamWriter
    ) -> None:
        """Function that executes clients commands"""

        if not parts:
            return

        cmd = parts[0].upper()
        handler = self.handlers.get(cmd)

        if handler:
            await handler(parts, writer)
        else:
            await send_error(UNKNOWN_COMMAND_MESSAGE, writer)

    async def handle_ping(
        self, parts: list[bytes], writer: asyncio.StreamWriter
    ) -> None:
        """Handles ping command"""

        if len(parts) == 1:
            await send_response(PONG, writer)
        else:
            await send_error(UNKNOWN_COMMAND_MESSAGE, writer)

    async def handle_echo(
        self, parts: list[bytes], writer: asyncio.StreamWriter
    ) -> None:
        """Handles echo command"""

        if len(parts) == 2:
            await send_response(encode_bulk_string(parts[1]), writer)
        else:
            await send_error(UNKNOWN_COMMAND_MESSAGE, writer)

    async def handle_set(
        self, parts: list[bytes], writer: asyncio.StreamWriter
    ) -> None:
        """Handles set command"""

        key = parts[1]
        if len(parts) > 4 and parts[3].upper() == ARG_PX:
            expiry_sec = float(parts[4]) / 1000  # Parts[4] - expiry in ms
            self.db.set_(key, parts[2], expire=expiry_sec)
        else:
            self.db.set_(key, parts[2])
        await send_response(OK, writer)

    async def handle_get(
        self, parts: list[bytes], writer: asyncio.StreamWriter
    ) -> None:
        """Handles get command"""

        key = parts[1]
        result = self.db.get_(key)
        if result:
            await send_response(encode_bulk_string(result[0]), writer)
        else:
            await send_response(NBS, writer)

    async def handle_rpush(
        self, parts: list[bytes], writer: asyncio.StreamWriter
    ) -> None:
        """Handles rpush command"""

        key = parts[1]
        try:
            length_lst = self.db.rpush(key, parts[2:])
            await send_response(encode_integer(length_lst), writer)
        except ValueError:
            await send_error(WRONG_VALUE_MESSAGE, writer)

    async def handle_lrange(
        self, parts: list[bytes], writer: asyncio.StreamWriter
    ) -> None:
        """Handles lrange command"""

        key = parts[1]
        try:
            start = int(parts[2])
            end = int(parts[3])
            result = self.db.lrange(key, start, end)
            await send_response(encode_array(result), writer)
        except ValueError as e:
            await send_error(str(e), writer)

    async def handle_lpush(
        self, parts: list[bytes], writer: asyncio.StreamWriter
    ) -> None:
        """Handles lpush command"""

        key = parts[1]
        try:
            length_lst = self.db.lpush(key, parts[2:])
            await send_response(encode_integer(length_lst), writer)
        except ValueError:
            await send_error(WRONG_VALUE_MESSAGE, writer)

    async def handle_llen(self, parts: list[bytes], writer: asyncio.StreamWriter) -> None:
        """Handles llen command"""

        key = parts[1]
        try:
            length_lst = self.db.llen(key)
            await send_response(encode_integer(length_lst), writer)
        except ValueError:
            await send_error(WRONG_VALUE_MESSAGE, writer)
