import asyncio
import unittest

from pykv.protocol import OK, ErrorReply, ProtocolError, encode, encode_command, read_command


def reader_for(data):
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


class TestReadCommand(unittest.IsolatedAsyncioTestCase):
    async def test_array_of_bulk_strings(self):
        reader = reader_for(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        self.assertEqual(await read_command(reader), [b"SET", b"foo", b"bar"])

    async def test_binary_safe_values(self):
        # bulk strings are length-prefixed, so values may contain \r\n
        reader = reader_for(b"*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n")
        self.assertEqual(await read_command(reader), [b"ECHO", b"a\r\nb"])

    async def test_inline_command(self):
        self.assertEqual(await read_command(reader_for(b"PING hello\r\n")), [b"PING", b"hello"])

    async def test_multiple_commands_in_one_buffer(self):
        reader = reader_for(encode_command([b"GET", b"a"]) + encode_command([b"GET", b"b"]))
        self.assertEqual(await read_command(reader), [b"GET", b"a"])
        self.assertEqual(await read_command(reader), [b"GET", b"b"])
        self.assertIsNone(await read_command(reader))

    async def test_end_of_stream(self):
        self.assertIsNone(await read_command(reader_for(b"")))

    async def test_truncated_command(self):
        with self.assertRaises(asyncio.IncompleteReadError):
            await read_command(reader_for(b"*2\r\n$3\r\nGET\r\n$3\r\nfo"))

    async def test_invalid_length(self):
        with self.assertRaises(ProtocolError):
            await read_command(reader_for(b"*abc\r\n"))

    async def test_missing_bulk_prefix(self):
        with self.assertRaises(ProtocolError):
            await read_command(reader_for(b"*1\r\n+GET\r\n"))


class TestEncode(unittest.TestCase):
    def test_reply_types(self):
        self.assertEqual(encode(OK), b"+OK\r\n")
        self.assertEqual(encode(ErrorReply("ERR bad")), b"-ERR bad\r\n")
        self.assertEqual(encode(42), b":42\r\n")
        self.assertEqual(encode(b"hi"), b"$2\r\nhi\r\n")
        self.assertEqual(encode(None), b"$-1\r\n")
        self.assertEqual(encode([b"a", 1]), b"*2\r\n$1\r\na\r\n:1\r\n")
        self.assertEqual(encode([]), b"*0\r\n")

    def test_unsupported_type(self):
        with self.assertRaises(TypeError):
            encode(1.5)


if __name__ == "__main__":
    unittest.main()
