import os
import tempfile
import unittest

from pykv.aof import AppendOnlyFile, replay
from pykv.commands import CommandHandler
from pykv.protocol import encode_command
from pykv.store import Store

from tests.test_commands import cmd
from tests.test_store import FakeClock


class TestAof(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "test.aof")
        self.clock = FakeClock()

    def tearDown(self):
        self.dir.cleanup()

    def new_handler(self, with_aof=True):
        handler = CommandHandler(Store(clock=self.clock))
        if with_aof:
            handler.aof = AppendOnlyFile(self.path, fsync="always")
        return handler

    async def restart(self):
        """Simulate a server restart: a fresh, empty store rebuilt from the log."""
        handler = self.new_handler(with_aof=False)
        loaded, discarded = await replay(self.path, handler)
        return handler, loaded, discarded

    async def test_data_survives_restart(self):
        handler = self.new_handler()
        for parts in (("SET", "a", "1"), ("INCRBY", "n", "5"), ("RPUSH", "l", "x", "y"),
                      ("DEL", "a"), ("LPOP", "l")):
            handler.execute(cmd(*parts))
        handler.aof.close()

        restored, loaded, discarded = await self.restart()
        self.assertEqual(loaded, 5)
        self.assertEqual(discarded, 0)
        self.assertIsNone(restored.execute(cmd("GET", "a")))
        self.assertEqual(restored.execute(cmd("GET", "n")), b"5")
        self.assertEqual(restored.execute(cmd("LRANGE", "l", "0", "-1")), [b"y"])

    async def test_expiry_is_absolute_after_restart(self):
        handler = self.new_handler()
        handler.execute(cmd("SET", "k", "v", "EX", "10"))
        handler.aof.close()

        self.clock.now += 8  # the server was down for 8 seconds
        restored, _, _ = await self.restart()
        self.assertEqual(restored.execute(cmd("TTL", "k")), 2)  # not 10 again

    async def test_truncated_last_command_is_discarded(self):
        handler = self.new_handler()
        handler.execute(cmd("SET", "a", "1"))
        handler.aof.close()
        complete_size = os.path.getsize(self.path)
        with open(self.path, "ab") as f:
            f.write(encode_command(cmd("SET", "b", "2"))[:-5])  # crash mid-write

        restored, loaded, discarded = await self.restart()
        self.assertEqual(loaded, 1)
        self.assertGreater(discarded, 0)
        self.assertEqual(restored.execute(cmd("GET", "a")), b"1")
        self.assertEqual(os.path.getsize(self.path), complete_size)

    async def test_rewrite_compacts_log(self):
        handler = self.new_handler()
        for _ in range(100):
            handler.execute(cmd("INCR", "counter"))
        handler.execute(cmd("SET", "temp", "x", "EX", "60"))
        size_before = os.path.getsize(self.path)

        handler.execute(cmd("REWRITEAOF"))
        handler.execute(cmd("SET", "after", "rewrite"))
        handler.aof.close()
        self.assertLess(os.path.getsize(self.path), size_before)

        restored, loaded, _ = await self.restart()
        self.assertEqual(loaded, 4)  # SET counter, SET temp, PEXPIREAT temp, SET after
        self.assertEqual(restored.execute(cmd("GET", "counter")), b"100")
        self.assertEqual(restored.execute(cmd("TTL", "temp")), 60)
        self.assertEqual(restored.execute(cmd("GET", "after")), b"rewrite")

    async def test_missing_file(self):
        _, loaded, discarded = await self.restart()
        self.assertEqual((loaded, discarded), (0, 0))


if __name__ == "__main__":
    unittest.main()
