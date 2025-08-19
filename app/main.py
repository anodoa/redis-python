"""
main.py - the application's entry point.

Appointment:
- Creating an instance of Redis_Server and launching of asynchronous TCP-server.
- The launch is performed by calling 'python main.py'

Using:
    $ python main.py

After startup, the server starts accepting commands from clients in the RESP2 format.
"""


import asyncio
from app.server import Redis_Server


if __name__ == "__main__":
    server = Redis_Server()
    asyncio.run(server.start_server())
