"""
RESP (REdis Serialization Protocol) parsing and encoding.

Clients send each command as an array of bulk strings. For example, SET foo bar is sent as:
    *3\\r\\n$3\\r\\nSET\\r\\n$3\\r\\nfoo\\r\\n$3\\r\\nbar\\r\\n
Replies start with a type byte: + simple string, - error, : integer, $ bulk string, * array.
Because the server speaks real RESP, the standard redis-cli and Redis client libraries work with it.
"""

MAX_ARGS = 1024 * 1024
MAX_BULK_LENGTH = 64 * 1024 * 1024  # 64 MB per argument


class ProtocolError(Exception):
    """Raised when a client sends bytes that are not valid RESP."""


class SimpleString(str):
    """A short status reply such as OK or PONG, sent as +OK."""


class ErrorReply(str):
    """An error reply, sent as -ERR message."""


OK = SimpleString("OK")


async def read_command(reader):
    """Read one command from an asyncio StreamReader.

    Returns a list of byte strings, e.g. [b"SET", b"foo", b"bar"], or None when the
    stream has ended cleanly. Raises asyncio.IncompleteReadError if it ends mid-command.
    """
    line = await reader.readline()
    if not line:
        return None
    if not line.endswith(b"\r\n"):
        raise ProtocolError("unexpected end of stream")

    if line.startswith(b"*"):
        count = _parse_length(line, MAX_ARGS)
        args = []
        for _ in range(count):
            header = await reader.readline()
            if not header.startswith(b"$"):
                raise ProtocolError("expected '$' bulk string")
            length = _parse_length(header, MAX_BULK_LENGTH)
            data = await reader.readexactly(length + 2)  # +2 for the trailing \r\n
            if data[-2:] != b"\r\n":
                raise ProtocolError("bulk string not terminated by CRLF")
            args.append(data[:-2])
        return args

    # Inline command, e.g. typed by hand in telnet: "PING\r\n"
    return line.split()


def _parse_length(line, limit):
    try:
        value = int(line[1:-2])
    except ValueError:
        raise ProtocolError(f"invalid length in {line!r}")
    if not 0 <= value <= limit:
        raise ProtocolError(f"length {value} out of range")
    return value


def encode(value):
    """Encode a Python value as a RESP reply."""
    if value is None:
        return b"$-1\r\n"  # null bulk string, e.g. GET on a missing key
    if isinstance(value, SimpleString):
        return b"+" + value.encode() + b"\r\n"
    if isinstance(value, ErrorReply):
        return b"-" + value.encode() + b"\r\n"
    if isinstance(value, int):
        return b":%d\r\n" % value
    if isinstance(value, str):
        value = value.encode()
    if isinstance(value, bytes):
        return b"$%d\r\n%s\r\n" % (len(value), value)
    if isinstance(value, (list, tuple)):
        return b"*%d\r\n" % len(value) + b"".join(encode(item) for item in value)
    raise TypeError(f"cannot encode {type(value).__name__} as RESP")


def encode_command(args):
    """Encode a command (a list of byte strings) the way a client would send it."""
    return encode(list(args))
