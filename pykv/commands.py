"""
Command dispatch: validates arguments, runs each command against the Store and logs
write commands to the append-only file (AOF).

Writes with relative expiry (EXPIRE, SET ... EX) are logged with an absolute timestamp
(PEXPIREAT, SET ... PXAT). Otherwise replaying the log after a restart would give keys
a fresh TTL and they would live longer than they should.
"""

import math

from .protocol import OK, ErrorReply, SimpleString
from .store import WrongTypeError

WRONGTYPE = ErrorReply("WRONGTYPE Operation against a key holding the wrong kind of value")


class CommandError(Exception):
    """A user error that is sent back to the client as -ERR <message>."""


def _to_int(raw):
    try:
        return int(raw)
    except ValueError:
        raise CommandError("value is not an integer or out of range") from None


class CommandHandler:
    def __init__(self, store, aof=None):
        self.store = store
        self.aof = aof
        self.loading = False  # True while replaying the AOF, so replayed commands are not logged again
        self.commands_processed = 0
        self.connected_clients = 0

        # name -> (method, arity). Arity counts the command name: a positive number means
        # exactly that many arguments, a negative number means at least that many.
        self.commands = {
            b"PING": (self.ping, -1),
            b"ECHO": (self.echo, 2),
            b"GET": (self.get, 2),
            b"SET": (self.set, -3),
            b"DEL": (self.delete, -2),
            b"EXISTS": (self.exists, -2),
            b"INCR": (self.incr, 2),
            b"DECR": (self.decr, 2),
            b"INCRBY": (self.incrby, 3),
            b"EXPIRE": (self.expire, 3),
            b"PEXPIREAT": (self.pexpireat, 3),
            b"TTL": (self.ttl, 2),
            b"PTTL": (self.pttl, 2),
            b"KEYS": (self.keys, 2),
            b"LPUSH": (self.lpush, -3),
            b"RPUSH": (self.rpush, -3),
            b"LPOP": (self.lpop, 2),
            b"RPOP": (self.rpop, 2),
            b"LRANGE": (self.lrange, 4),
            b"LLEN": (self.llen, 2),
            b"DBSIZE": (self.dbsize, 1),
            b"FLUSHALL": (self.flushall, 1),
            b"REWRITEAOF": (self.rewriteaof, 1),
            b"INFO": (self.info, -1),
            b"COMMAND": (self.command, -1),
        }

    def execute(self, args):
        """Run one command and return its reply value (see protocol.encode)."""
        if not args:
            return ErrorReply("ERR empty command")
        name = args[0].upper()
        entry = self.commands.get(name)
        if entry is None:
            return ErrorReply(f"ERR unknown command '{args[0].decode(errors='replace')}'")

        method, arity = entry
        if (arity > 0 and len(args) != arity) or (arity < 0 and len(args) < -arity):
            return ErrorReply(f"ERR wrong number of arguments for '{name.decode().lower()}' command")

        self.commands_processed += 1
        try:
            return method(*args[1:])
        except WrongTypeError:
            return WRONGTYPE
        except (CommandError, ValueError) as error:
            return ErrorReply(f"ERR {error}")

    def _log(self, *args):
        if self.aof is not None and not self.loading:
            self.aof.append(args)

    # ------------------------------------------------------------ connection

    def ping(self, *args):
        if len(args) > 1:
            raise CommandError("wrong number of arguments for 'ping' command")
        return args[0] if args else SimpleString("PONG")

    def echo(self, message):
        return message

    def command(self, *args):
        return []  # redis-cli asks for command docs on connect; an empty list is fine

    # ------------------------------------------------------------ strings

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, *options):
        """SET key value [EX seconds | PX milliseconds | PXAT unix-ms] [NX | XX]"""
        expire_at = None
        only_if_missing = only_if_exists = False
        i = 0
        while i < len(options):
            option = options[i].upper()
            if option in (b"EX", b"PX", b"PXAT") and i + 1 < len(options):
                amount = _to_int(options[i + 1])
                if amount <= 0:
                    raise CommandError("invalid expire time in 'set' command")
                if option == b"EX":
                    expire_at = self.store.clock() + amount
                elif option == b"PX":
                    expire_at = self.store.clock() + amount / 1000
                else:
                    expire_at = amount / 1000
                i += 2
            elif option == b"NX":
                only_if_missing = True
                i += 1
            elif option == b"XX":
                only_if_exists = True
                i += 1
            else:
                raise CommandError("syntax error")

        if only_if_missing and only_if_exists:
            raise CommandError("syntax error")
        exists = self.store.exists([key]) > 0
        if (only_if_missing and exists) or (only_if_exists and not exists):
            return None

        self.store.set(key, value, expire_at)
        if expire_at is None:
            self._log(b"SET", key, value)
        else:
            self._log(b"SET", key, value, b"PXAT", str(int(expire_at * 1000)).encode())
        return OK

    def incr(self, key):
        return self.incrby(key, b"1")

    def decr(self, key):
        return self.incrby(key, b"-1")

    def incrby(self, key, amount):
        result = self.store.incr_by(key, _to_int(amount))
        self._log(b"INCRBY", key, amount)
        return result

    # ------------------------------------------------------------ keys

    def delete(self, *keys):
        removed = self.store.delete(keys)
        if removed:
            self._log(b"DEL", *keys)
        return removed

    def exists(self, *keys):
        return self.store.exists(keys)

    def expire(self, key, seconds):
        deadline = self.store.clock() + _to_int(seconds)
        return self._expire_at(key, deadline)

    def pexpireat(self, key, timestamp_ms):
        return self._expire_at(key, _to_int(timestamp_ms) / 1000)

    def _expire_at(self, key, deadline):
        if not self.store.expire_at(key, deadline):
            return 0
        self._log(b"PEXPIREAT", key, str(int(deadline * 1000)).encode())
        return 1

    def ttl(self, key):
        remaining = self.store.ttl(key)
        return remaining if remaining < 0 else math.ceil(remaining)

    def pttl(self, key):
        remaining = self.store.ttl(key)
        return remaining if remaining < 0 else math.ceil(remaining * 1000)

    def keys(self, pattern):
        return self.store.keys(pattern)

    def dbsize(self):
        return len(self.store)

    def flushall(self):
        self.store.flush()
        self._log(b"FLUSHALL")
        return OK

    # ------------------------------------------------------------ lists

    def lpush(self, key, *values):
        length = self.store.push(key, values, left=True)
        self._log(b"LPUSH", key, *values)
        return length

    def rpush(self, key, *values):
        length = self.store.push(key, values, left=False)
        self._log(b"RPUSH", key, *values)
        return length

    def lpop(self, key):
        value = self.store.pop(key, left=True)
        if value is not None:
            self._log(b"LPOP", key)
        return value

    def rpop(self, key):
        value = self.store.pop(key, left=False)
        if value is not None:
            self._log(b"RPOP", key)
        return value

    def lrange(self, key, start, stop):
        return self.store.lrange(key, _to_int(start), _to_int(stop))

    def llen(self, key):
        return self.store.llen(key)

    # ------------------------------------------------------------ server

    def rewriteaof(self):
        if self.aof is None:
            raise CommandError("append-only file is not enabled")
        self.aof.rewrite(self.store.snapshot_commands())
        return OK

    def info(self, *sections):
        lines = [
            "# Stats",
            f"connected_clients:{self.connected_clients}",
            f"total_commands_processed:{self.commands_processed}",
            f"keys:{len(self.store)}",
            f"expired_keys:{self.store.expired_keys}",
            f"evicted_keys:{self.store.evicted_keys}",
            f"max_keys:{self.store.max_keys or 0}",
            f"aof_enabled:{int(self.aof is not None)}",
        ]
        return "\r\n".join(lines) + "\r\n"
