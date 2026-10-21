"""
The asyncio TCP server: accepts client connections and runs their commands.

Everything runs on one thread in a single event loop, like Redis. Only one command runs
at a time, so the store needs no locks, while asyncio still lets that one thread serve
many clients at once by switching to another client whenever one is waiting on the network.
"""

import asyncio
import contextlib
import logging

from .aof import AppendOnlyFile, replay
from .commands import CommandHandler
from .protocol import OK, ErrorReply, ProtocolError, encode, read_command
from .store import Store

log = logging.getLogger("pykv")

EXPIRY_INTERVAL = 0.1  # seconds between active-expiry sweeps


class Server:
    def __init__(self, host="127.0.0.1", port=6380, aof_path=None, fsync="everysec", max_keys=None):
        self.host = host
        self.port = port
        self.aof_path = aof_path
        self.fsync = fsync
        self.store = Store(max_keys=max_keys)
        self.handler = CommandHandler(self.store)
        self._server = None
        self._background = None

    async def start(self):
        if self.aof_path:
            loaded, discarded = await replay(self.aof_path, self.handler)
            log.info("Loaded %d commands from %s", loaded, self.aof_path)
            if discarded:
                log.warning("Discarded %d bytes of incomplete data at the end of the AOF", discarded)
            self.handler.aof = AppendOnlyFile(self.aof_path, self.fsync)

        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]  # the real port if 0 was requested
        self._background = asyncio.create_task(self._background_jobs())
        log.info("PyKV listening on %s:%d", self.host, self.port)

    async def serve_forever(self):
        await self.start()
        try:
            await self._server.serve_forever()
        finally:
            await self.stop()

    async def stop(self):
        if self._background:
            self._background.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._background
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        if self.handler.aof:
            self.handler.aof.close()

    async def _handle_client(self, reader, writer):
        self.handler.connected_clients += 1
        try:
            while True:
                try:
                    args = await read_command(reader)
                except ProtocolError as error:
                    writer.write(encode(ErrorReply(f"ERR Protocol error: {error}")))
                    break
                except asyncio.IncompleteReadError:
                    break  # client disconnected mid-command
                if args is None:
                    break  # client closed the connection
                if not args:
                    continue  # blank inline line
                if args[0].upper() == b"QUIT":
                    writer.write(encode(OK))
                    break
                writer.write(encode(self.handler.execute(args)))
                await writer.drain()  # back-pressure: wait if the client is reading slowly
        except ConnectionError:
            pass
        finally:
            self.handler.connected_clients -= 1
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    async def _background_jobs(self):
        """Delete expired keys every 100 ms and fsync the AOF every second."""
        ticks = 0
        while True:
            await asyncio.sleep(EXPIRY_INTERVAL)
            self.store.purge_expired()
            ticks += 1
            if ticks % 10 == 0 and self.handler.aof:
                self.handler.aof.sync()
