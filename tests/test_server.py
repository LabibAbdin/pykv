"""End-to-end tests: start a real server on a free port and talk to it over TCP."""

import asyncio
import os
import tempfile
import unittest

from pykv.protocol import encode_command
from pykv.server import Server

from tests.test_commands import cmd


class Client:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer

    @classmethod
    async def connect(cls, port):
        return cls(*await asyncio.open_connection("127.0.0.1", port))

    async def send(self, *parts):
        self.writer.write(encode_command(cmd(*parts)))
        await self.writer.drain()
        return await self.read_reply()

    async def read_reply(self):
        line = await self.reader.readline()
        kind, body = line[:1], line[1:-2]
        if kind in (b"+", b"-"):
            return body.decode()
        if kind == b":":
            return int(body)
        if kind == b"$":
            if body == b"-1":
                return None
            data = await self.reader.readexactly(int(body) + 2)
            return data[:-2]
        if kind == b"*":
            return [await self.read_reply() for _ in range(int(body))]
        raise ValueError(f"unexpected reply {line!r}")

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()


class TestServer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.aof_path = os.path.join(self.dir.name, "server.aof")
        self.server = await self.start_server()

    async def asyncTearDown(self):
        await self.server.stop()
        self.dir.cleanup()

    async def start_server(self, **kwargs):
        server = Server(port=0, aof_path=self.aof_path, **kwargs)
        await server.start()
        return server

    async def test_basic_commands(self):
        client = await Client.connect(self.server.port)
        self.assertEqual(await client.send("PING"), "PONG")
        self.assertEqual(await client.send("SET", "greeting", "hello"), "OK")
        self.assertEqual(await client.send("GET", "greeting"), b"hello")
        self.assertIsNone(await client.send("GET", "nope"))
        self.assertTrue((await client.send("NOPE")).startswith("ERR unknown command"))
        await client.close()

    async def test_many_concurrent_clients(self):
        async def worker(n):
            client = await Client.connect(self.server.port)
            for _ in range(50):
                await client.send("INCR", "shared")
            await client.send("SET", f"client:{n}", n)
            await client.close()

        await asyncio.gather(*(worker(n) for n in range(20)))
        client = await Client.connect(self.server.port)
        self.assertEqual(await client.send("GET", "shared"), b"1000")  # no lost updates
        self.assertEqual(await client.send("DBSIZE"), 21)
        await client.close()

    async def test_pipelining(self):
        client = await Client.connect(self.server.port)
        client.writer.write(b"".join(encode_command(cmd("INCR", "p")) for _ in range(100)))
        await client.writer.drain()
        replies = [await client.read_reply() for _ in range(100)]
        self.assertEqual(replies, list(range(1, 101)))
        await client.close()

    async def test_inline_commands(self):
        client = await Client.connect(self.server.port)
        client.writer.write(b"SET inline works\r\nGET inline\r\n")
        await client.writer.drain()
        self.assertEqual(await client.read_reply(), "OK")
        self.assertEqual(await client.read_reply(), b"works")
        await client.close()

    async def test_protocol_error_closes_connection(self):
        client = await Client.connect(self.server.port)
        client.writer.write(b"*x\r\n")
        await client.writer.drain()
        self.assertTrue((await client.read_reply()).startswith("ERR Protocol error"))
        self.assertEqual(await client.reader.read(), b"")  # server hung up
        await client.close()

    async def test_data_persists_across_restart(self):
        client = await Client.connect(self.server.port)
        await client.send("SET", "durable", "yes")
        await client.send("RPUSH", "queue", "job1", "job2")
        await client.close()
        await self.server.stop()

        self.server = await self.start_server()
        client = await Client.connect(self.server.port)
        self.assertEqual(await client.send("GET", "durable"), b"yes")
        self.assertEqual(await client.send("LRANGE", "queue", "0", "-1"), [b"job1", b"job2"])
        await client.close()

    async def test_background_expiry(self):
        client = await Client.connect(self.server.port)
        await client.send("SET", "short", "lived", "PX", "50")
        await asyncio.sleep(0.3)  # longer than the key's life plus one sweep
        self.assertEqual(len(self.server.store), 0)  # removed without anyone reading it
        await client.close()


if __name__ == "__main__":
    unittest.main()
