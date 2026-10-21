"""
The in-memory data store: string and list values, key expiry (TTL) and LRU eviction.

Values are either bytes (strings) or deques (lists). Keys live in an OrderedDict kept in
least-recently-used order, so when the store is full the oldest key is evicted in O(1).
"""

import fnmatch
import itertools
import time
from collections import OrderedDict, deque

INT64_MIN, INT64_MAX = -(2 ** 63), 2 ** 63 - 1


class WrongTypeError(Exception):
    """Raised when a command is used on a key holding the wrong kind of value."""


class Store:
    def __init__(self, max_keys=None, clock=time.time):
        self.data = OrderedDict()  # key -> bytes | deque, least recently used first
        self.expires = {}          # key -> absolute expiry time in seconds since the epoch
        self.max_keys = max_keys   # None means unlimited
        self.clock = clock         # injectable so tests can control time
        self.expired_keys = 0
        self.evicted_keys = 0

    def __len__(self):
        return len(self.data)

    # ------------------------------------------------------------ internals

    def _lookup(self, key):
        """Return the live value for key or None. Expired keys are deleted on access (lazy expiry)."""
        if key not in self.data:
            return None
        if self._is_expired(key):
            self._remove(key)
            self.expired_keys += 1
            return None
        self.data.move_to_end(key)  # mark as most recently used
        return self.data[key]

    def _is_expired(self, key):
        deadline = self.expires.get(key)
        return deadline is not None and deadline <= self.clock()

    def _insert(self, key, value):
        self.data[key] = value
        self.data.move_to_end(key)
        while self.max_keys is not None and len(self.data) > self.max_keys:
            evicted, _ = self.data.popitem(last=False)  # least recently used
            self.expires.pop(evicted, None)
            self.evicted_keys += 1

    def _remove(self, key):
        del self.data[key]
        self.expires.pop(key, None)

    # ------------------------------------------------------------ keys

    def exists(self, keys):
        return sum(1 for key in keys if self._lookup(key) is not None)

    def delete(self, keys):
        removed = 0
        for key in keys:
            if self._lookup(key) is not None:
                self._remove(key)
                removed += 1
        return removed

    def keys(self, pattern):
        """Return live keys matching a glob pattern such as b"user:*". Does not affect LRU order."""
        return [key for key in self.data
                if not self._is_expired(key) and fnmatch.fnmatchcase(key, pattern)]

    def expire_at(self, key, deadline):
        """Set an absolute expiry time. Returns False if the key does not exist."""
        if self._lookup(key) is None:
            return False
        self.expires[key] = deadline
        return True

    def ttl(self, key):
        """Seconds until key expires, -1 if it has no expiry, or -2 if it does not exist."""
        if self._lookup(key) is None:
            return -2
        if key not in self.expires:
            return -1
        return max(self.expires[key] - self.clock(), 0)

    def purge_expired(self):
        """Active expiry: delete every key whose deadline has passed. Returns the number removed."""
        now = self.clock()
        expired = [key for key, deadline in self.expires.items() if deadline <= now]
        for key in expired:
            self._remove(key)
        self.expired_keys += len(expired)
        return len(expired)

    def flush(self):
        self.data.clear()
        self.expires.clear()

    # ------------------------------------------------------------ strings

    def get(self, key):
        value = self._lookup(key)
        if value is not None and not isinstance(value, bytes):
            raise WrongTypeError
        return value

    def set(self, key, value, expire_at=None):
        """Store a string, replacing any old value and its expiry."""
        self.expires.pop(key, None)
        self._insert(key, value)
        if expire_at is not None:
            self.expires[key] = expire_at

    def incr_by(self, key, amount):
        current = self.get(key)
        try:
            number = int(current) if current is not None else 0
        except ValueError:
            raise ValueError("value is not an integer or out of range") from None
        number += amount
        if not INT64_MIN <= number <= INT64_MAX:
            raise ValueError("increment or decrement would overflow")
        encoded = str(number).encode()
        if current is None:
            self._insert(key, encoded)
        else:
            self.data[key] = encoded  # keep the existing expiry, like Redis
        return number

    # ------------------------------------------------------------ lists

    def _get_list(self, key, create=False):
        value = self._lookup(key)
        if value is None:
            if not create:
                return None
            value = deque()
            self._insert(key, value)
        elif not isinstance(value, deque):
            raise WrongTypeError
        return value

    def push(self, key, values, left):
        items = self._get_list(key, create=True)
        for value in values:
            if left:
                items.appendleft(value)
            else:
                items.append(value)
        return len(items)

    def pop(self, key, left):
        items = self._get_list(key)
        if items is None:
            return None
        value = items.popleft() if left else items.pop()
        if not items:
            self._remove(key)  # empty lists are deleted, like Redis
        return value

    def lrange(self, key, start, stop):
        """Elements from start to stop inclusive. Negative indexes count from the end."""
        items = self._get_list(key)
        if items is None:
            return []
        length = len(items)
        if start < 0:
            start = max(length + start, 0)
        if stop < 0:
            stop = length + stop
        stop = min(stop, length - 1)
        if start > stop:
            return []
        return list(itertools.islice(items, start, stop + 1))

    def llen(self, key):
        items = self._get_list(key)
        return len(items) if items is not None else 0

    # ------------------------------------------------------------ persistence

    def snapshot_commands(self):
        """Yield the minimal commands that rebuild the current data. Used to compact the AOF."""
        now = self.clock()
        for key, value in self.data.items():
            deadline = self.expires.get(key)
            if deadline is not None and deadline <= now:
                continue
            if isinstance(value, bytes):
                yield [b"SET", key, value]
            else:
                yield [b"RPUSH", key, *value]
            if deadline is not None:
                yield [b"PEXPIREAT", key, str(int(deadline * 1000)).encode()]
