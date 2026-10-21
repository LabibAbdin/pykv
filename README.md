# PyKV

A **Redis-compatible in-memory key-value database server** written from scratch in Python, using only the standard library.

It uses Redis's wire protocol (RESP), so the official `redis-cli`, `redis-benchmark` and Redis client libraries work with it
unchanged. It supports strings, counters and lists, key expiry (TTL), LRU eviction, and crash-safe persistence through an
append-only log.

```
$ python -m pykv --aof data.aof
2026-10-21 12:00:00 INFO PyKV listening on 127.0.0.1:6380

$ redis-cli -p 6380
127.0.0.1:6380> SET session:42 "labib" EX 60
OK
127.0.0.1:6380> TTL session:42
(integer) 60
127.0.0.1:6380> RPUSH jobs build test deploy
(integer) 3
127.0.0.1:6380> LPOP jobs
"build"
127.0.0.1:6380> INCR page_views
(integer) 1
```

## Features

- **asyncio TCP server**: one thread and one event loop serve many concurrent clients. It supports
  **pipelining** and applies **back-pressure** to slow readers.
- **RESP protocol parser and encoder**: binary-safe, with length limits and protocol-error handling. It also accepts
  plain-text "inline" commands typed in telnet.
- **Commands:** `GET SET (EX/PX/PXAT/NX/XX) DEL EXISTS INCR DECR INCRBY EXPIRE PEXPIREAT TTL PTTL KEYS LPUSH RPUSH LPOP
  RPOP LRANGE LLEN DBSIZE FLUSHALL INFO PING ECHO REWRITEAOF`
- **Key expiry:** keys are removed either *lazily*, when an expired key is read, or *actively*, by a background sweep
  every 100 ms.
- **LRU eviction:** with `--max-keys N`, the least recently used key is evicted in O(1) when the store is full.
- **Append-only file (AOF) persistence:**
  - Every write is logged, and the log is replayed on startup so the data survives restarts.
  - Configurable `fsync` policy: `always`, `everysec` or `no`.
  - **Crash recovery:** if the server died halfway through writing a command, the partial command is detected and
    truncated on startup.
  - **Log compaction:** `REWRITEAOF` shrinks the log to the minimum commands needed to recreate the data, written
    atomically through a temporary file and `os.replace`.
- **52 unit and end-to-end tests**, including concurrent clients, pipelining, restarts and crash recovery.

## Architecture

```
                    ┌──────────────────────── PyKV process (single thread) ────────────────────────┐
  redis-cli ──┐     │                                                                              │
  client lib ─┼─TCP─►  server.py          protocol.py        commands.py          store.py         │
  bench.py ───┘     │  asyncio event  ──► RESP parser   ──►  validate args  ──►   OrderedDict      │
                    │  loop, one task     & encoder          dispatch by name     (LRU order)      │
                    │  per client                                  │              + expiry table   │
                    │       ▲                                      ▼                    ▲          │
                    │       │ every 100 ms: purge expired keys   aof.py ──append──► data.aof       │
                    │       └──────────── every 1 s: fsync AOF   (replayed on startup)             │
                    └──────────────────────────────────────────────────────────────────────────────┘
```

| File | Responsibility |
|---|---|
| `pykv/protocol.py` | Parse RESP commands from a stream; encode replies |
| `pykv/server.py` | Accept connections, run the read → execute → reply loop, background jobs |
| `pykv/commands.py` | Command table, argument checking, error replies, deciding what to log |
| `pykv/store.py` | The data: strings and lists, TTLs, LRU eviction |
| `pykv/aof.py` | Append-only log: write, fsync, replay, crash recovery, compaction |

## Design decisions

- **Single-threaded event loop, like Redis.** Only one command runs at a time, so there are no locks and no race
  conditions (20 clients incrementing the same counter 1,000 times always gives exactly 1,000). asyncio switches
  between clients while they wait on the network, which is where most of the time goes.
- **`OrderedDict` for O(1) LRU.** Reading a key moves it to the end, and eviction pops from the front, so there's no
  scanning. (Redis uses an *approximate* LRU by sampling keys, to save memory.)
- **Two kinds of expiry.** Lazy expiry alone would leave never-read keys in memory forever. Active sweeps alone would
  let clients briefly see expired keys. Using both gives correct reads and bounded memory.
- **Relative expiry is logged as absolute time.** `SET k v EX 10` is written to the AOF as `SET k v PXAT <timestamp>`.
  If the log stored "10 seconds", every restart would give the key a fresh 10 seconds of life.
- **Durability versus speed:** `fsync=always` can't lose an acknowledged write but is slowest. `everysec` (the default,
  as in Redis) risks at most about 1 second of writes on a power failure.

## Performance

Measured with the official `redis-benchmark` tool on a 4-vCPU cloud VM (50 concurrent clients, 100,000 requests):

| Configuration | SET | GET | Median latency |
|---|---|---|---|
| In-memory | ~24,000 req/s | ~24,000 req/s | ~2 ms |
| With AOF (`fsync=everysec`) | ~20,000 req/s | — | ~2.4 ms |
| Pipelined (16 commands per round trip) | ~27,000 req/s | ~34,000 req/s | — |

Results depend on hardware. Real Redis (written in C) is roughly 5–10× faster. The aim of this project was to
understand how such a system works, not to beat it.

Reproduce:
```bash
python -m pykv &                          # start the server
redis-benchmark -p 6380 -t set,get -n 100000 -c 50 -q
python bench.py --clients 50 --requests 1000 --pipeline 16   # built-in benchmark, no Redis tools needed
```

## Usage

Requires Python 3.9+. No dependencies.

```bash
python -m pykv                                   # in-memory only, port 6380
python -m pykv --aof data.aof                    # with persistence
python -m pykv --aof data.aof --fsync always     # maximum durability
python -m pykv --max-keys 10000                  # LRU eviction beyond 10,000 keys
```

Connect with `redis-cli -p 6380`, any Redis client library, or even `telnet localhost 6380`.

Run the tests:
```bash
python -m unittest discover -s tests -t . -v
```

## Future improvements

- Hashes and sets, and `SCAN` as a non-blocking alternative to `KEYS`
- Approximate LRU by sampling, plus memory-based limits (`maxmemory`) instead of a key count
- Run AOF rewrites in the background so they don't pause other clients
- Pub/Sub messaging
- Leader–follower replication
