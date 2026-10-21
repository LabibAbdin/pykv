import unittest

from pykv.store import Store, WrongTypeError


class FakeClock:
    """Lets tests move time forward instantly instead of sleeping."""

    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


class TestStrings(unittest.TestCase):
    def setUp(self):
        self.store = Store()

    def test_set_and_get(self):
        self.store.set(b"k", b"v")
        self.assertEqual(self.store.get(b"k"), b"v")
        self.assertIsNone(self.store.get(b"missing"))

    def test_incr_by(self):
        self.assertEqual(self.store.incr_by(b"n", 5), 5)
        self.assertEqual(self.store.incr_by(b"n", -2), 3)
        self.assertEqual(self.store.get(b"n"), b"3")

    def test_incr_non_integer(self):
        self.store.set(b"k", b"abc")
        with self.assertRaises(ValueError):
            self.store.incr_by(b"k", 1)

    def test_incr_overflow(self):
        self.store.set(b"k", str(2 ** 63 - 1).encode())
        with self.assertRaises(ValueError):
            self.store.incr_by(b"k", 1)

    def test_wrong_type(self):
        self.store.push(b"list", [b"a"], left=False)
        with self.assertRaises(WrongTypeError):
            self.store.get(b"list")
        self.store.set(b"str", b"x")
        with self.assertRaises(WrongTypeError):
            self.store.push(b"str", [b"a"], left=False)

    def test_delete_and_exists(self):
        self.store.set(b"a", b"1")
        self.store.set(b"b", b"2")
        self.assertEqual(self.store.exists([b"a", b"b", b"c"]), 2)
        self.assertEqual(self.store.delete([b"a", b"c"]), 1)
        self.assertEqual(self.store.exists([b"a"]), 0)

    def test_keys_pattern(self):
        for key in (b"user:1", b"user:2", b"order:1"):
            self.store.set(key, b"x")
        self.assertEqual(sorted(self.store.keys(b"user:*")), [b"user:1", b"user:2"])


class TestExpiry(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.store = Store(clock=self.clock)

    def test_key_expires(self):
        self.store.set(b"k", b"v", expire_at=self.clock.now + 10)
        self.clock.now += 9
        self.assertEqual(self.store.get(b"k"), b"v")
        self.clock.now += 1
        self.assertIsNone(self.store.get(b"k"))
        self.assertEqual(self.store.expired_keys, 1)

    def test_ttl(self):
        self.assertEqual(self.store.ttl(b"missing"), -2)
        self.store.set(b"k", b"v")
        self.assertEqual(self.store.ttl(b"k"), -1)
        self.store.expire_at(b"k", self.clock.now + 30)
        self.assertEqual(self.store.ttl(b"k"), 30)

    def test_set_clears_old_expiry(self):
        self.store.set(b"k", b"v", expire_at=self.clock.now + 10)
        self.store.set(b"k", b"v2")
        self.assertEqual(self.store.ttl(b"k"), -1)

    def test_active_purge(self):
        for i in range(5):
            self.store.set(b"k%d" % i, b"v", expire_at=self.clock.now + i + 1)
        self.clock.now += 3
        self.assertEqual(self.store.purge_expired(), 3)
        self.assertEqual(len(self.store), 2)

    def test_expired_keys_hidden_from_keys(self):
        self.store.set(b"k", b"v", expire_at=self.clock.now + 1)
        self.clock.now += 2
        self.assertEqual(self.store.keys(b"*"), [])


class TestLruEviction(unittest.TestCase):
    def test_evicts_least_recently_used(self):
        store = Store(max_keys=3)
        for key in (b"a", b"b", b"c"):
            store.set(key, b"x")
        store.get(b"a")          # a is now the most recently used
        store.set(b"d", b"x")    # over the limit, so b (least recently used) is evicted
        self.assertEqual(sorted(store.data), [b"a", b"c", b"d"])
        self.assertEqual(store.evicted_keys, 1)

    def test_overwrite_does_not_evict(self):
        store = Store(max_keys=2)
        store.set(b"a", b"1")
        store.set(b"b", b"2")
        store.set(b"a", b"3")
        self.assertEqual(len(store), 2)
        self.assertEqual(store.evicted_keys, 0)


class TestLists(unittest.TestCase):
    def setUp(self):
        self.store = Store()

    def test_push_and_range(self):
        self.store.push(b"l", [b"b", b"c"], left=False)
        self.store.push(b"l", [b"a"], left=True)
        self.assertEqual(self.store.lrange(b"l", 0, -1), [b"a", b"b", b"c"])
        self.assertEqual(self.store.lrange(b"l", -2, 10), [b"b", b"c"])
        self.assertEqual(self.store.lrange(b"l", 5, 10), [])

    def test_pop_deletes_empty_list(self):
        self.store.push(b"l", [b"x"], left=False)
        self.assertEqual(self.store.pop(b"l", left=True), b"x")
        self.assertEqual(self.store.exists([b"l"]), 0)
        self.assertIsNone(self.store.pop(b"l", left=True))


if __name__ == "__main__":
    unittest.main()
