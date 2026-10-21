import unittest

from pykv.commands import CommandHandler
from pykv.protocol import OK, ErrorReply
from pykv.store import Store

from tests.test_store import FakeClock


class RecordingAof:
    """Stands in for AppendOnlyFile and remembers what was logged."""

    def __init__(self):
        self.entries = []

    def append(self, args):
        self.entries.append(list(args))


def cmd(*parts):
    return [part if isinstance(part, bytes) else str(part).encode() for part in parts]


class TestCommands(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.handler = CommandHandler(Store(clock=self.clock))

    def run_cmd(self, *parts):
        return self.handler.execute(cmd(*parts))

    def test_ping_and_echo(self):
        self.assertEqual(self.run_cmd("PING"), "PONG")
        self.assertEqual(self.run_cmd("ECHO", "hi"), b"hi")

    def test_commands_are_case_insensitive(self):
        self.assertEqual(self.run_cmd("set", "k", "v"), OK)
        self.assertEqual(self.run_cmd("Get", "k"), b"v")

    def test_unknown_command(self):
        reply = self.run_cmd("FLY")
        self.assertIsInstance(reply, ErrorReply)
        self.assertIn("unknown command", reply)

    def test_wrong_number_of_arguments(self):
        self.assertIn("wrong number of arguments", self.run_cmd("GET"))
        self.assertIn("wrong number of arguments", self.run_cmd("GET", "a", "b"))

    def test_set_options(self):
        self.assertEqual(self.run_cmd("SET", "k", "v", "NX"), OK)
        self.assertIsNone(self.run_cmd("SET", "k", "v2", "NX"))
        self.assertIsNone(self.run_cmd("SET", "missing", "v", "XX"))
        self.assertEqual(self.run_cmd("SET", "k", "v3", "XX", "EX", "10"), OK)
        self.assertEqual(self.run_cmd("TTL", "k"), 10)
        self.assertIn("syntax error", self.run_cmd("SET", "k", "v", "BOGUS"))
        self.assertIn("invalid expire", self.run_cmd("SET", "k", "v", "EX", "0"))

    def test_counters(self):
        self.assertEqual(self.run_cmd("INCR", "n"), 1)
        self.assertEqual(self.run_cmd("INCRBY", "n", "10"), 11)
        self.assertEqual(self.run_cmd("DECR", "n"), 10)
        self.run_cmd("SET", "s", "abc")
        self.assertIn("not an integer", self.run_cmd("INCR", "s"))

    def test_expire_and_ttl(self):
        self.run_cmd("SET", "k", "v")
        self.assertEqual(self.run_cmd("EXPIRE", "k", "5"), 1)
        self.assertEqual(self.run_cmd("PTTL", "k"), 5000)
        self.clock.now += 5
        self.assertIsNone(self.run_cmd("GET", "k"))
        self.assertEqual(self.run_cmd("TTL", "k"), -2)
        self.assertEqual(self.run_cmd("EXPIRE", "missing", "5"), 0)

    def test_wrongtype_error(self):
        self.run_cmd("RPUSH", "l", "a")
        self.assertTrue(self.run_cmd("GET", "l").startswith("WRONGTYPE"))

    def test_lists(self):
        self.assertEqual(self.run_cmd("RPUSH", "l", "b", "c"), 2)
        self.assertEqual(self.run_cmd("LPUSH", "l", "a"), 3)
        self.assertEqual(self.run_cmd("LRANGE", "l", "0", "-1"), [b"a", b"b", b"c"])
        self.assertEqual(self.run_cmd("LLEN", "l"), 3)
        self.assertEqual(self.run_cmd("LPOP", "l"), b"a")
        self.assertEqual(self.run_cmd("RPOP", "l"), b"c")

    def test_del_exists_keys_dbsize_flushall(self):
        self.run_cmd("SET", "a", "1")
        self.run_cmd("SET", "b", "2")
        self.assertEqual(self.run_cmd("EXISTS", "a", "b", "c"), 2)
        self.assertEqual(sorted(self.run_cmd("KEYS", "*")), [b"a", b"b"])
        self.assertEqual(self.run_cmd("DEL", "a", "c"), 1)
        self.assertEqual(self.run_cmd("DBSIZE"), 1)
        self.assertEqual(self.run_cmd("FLUSHALL"), OK)
        self.assertEqual(self.run_cmd("DBSIZE"), 0)

    def test_info(self):
        self.run_cmd("SET", "a", "1")
        self.assertIn("keys:1", self.run_cmd("INFO"))


class TestAofLogging(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.aof = RecordingAof()
        self.handler = CommandHandler(Store(clock=self.clock), self.aof)

    def test_only_successful_writes_are_logged(self):
        self.handler.execute(cmd("SET", "k", "v"))
        self.handler.execute(cmd("GET", "k"))
        self.handler.execute(cmd("DEL", "missing"))
        self.handler.execute(cmd("SET", "k", "v", "NX"))
        self.assertEqual(self.aof.entries, [cmd("SET", "k", "v")])

    def test_relative_expiry_logged_as_absolute(self):
        self.handler.execute(cmd("SET", "k", "v", "EX", "10"))
        self.handler.execute(cmd("EXPIRE", "k", "20"))
        expected_set = int((self.clock.now + 10) * 1000)
        expected_expire = int((self.clock.now + 20) * 1000)
        self.assertEqual(self.aof.entries, [
            cmd("SET", "k", "v", "PXAT", expected_set),
            cmd("PEXPIREAT", "k", expected_expire),
        ])

    def test_nothing_logged_while_loading(self):
        self.handler.loading = True
        self.handler.execute(cmd("SET", "k", "v"))
        self.assertEqual(self.aof.entries, [])


if __name__ == "__main__":
    unittest.main()
