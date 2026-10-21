"""
Throughput benchmark: many concurrent clients sending SET and GET commands.

    python -m pykv &               # start the server first
    python bench.py --clients 50 --requests 2000 --pipeline 1
"""

import argparse
import asyncio
import time

from pykv.protocol import encode_command


async def run_client(host, port, client_id, requests, pipeline):
    reader, writer = await asyncio.open_connection(host, port)
    for start in range(0, requests, pipeline):
        batch = range(start, min(start + pipeline, requests))
        # send a batch of commands, then read all their replies
        writer.write(b"".join(
            encode_command([b"SET", f"key:{client_id}:{i}".encode(), b"value"]) for i in batch))
        await writer.drain()
        for _ in batch:
            await reader.readline()  # +OK
        writer.write(b"".join(
            encode_command([b"GET", f"key:{client_id}:{i}".encode()]) for i in batch))
        await writer.drain()
        for _ in batch:
            await reader.readline()  # $5
            await reader.readline()  # value
    writer.close()
    await writer.wait_closed()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--requests", type=int, default=2000, help="SET+GET pairs per client")
    parser.add_argument("--pipeline", type=int, default=1, help="commands sent per round trip")
    args = parser.parse_args()

    started = time.perf_counter()
    await asyncio.gather(*(run_client(args.host, args.port, n, args.requests, args.pipeline)
                           for n in range(args.clients)))
    elapsed = time.perf_counter() - started

    total = args.clients * args.requests * 2
    print(f"{total:,} commands from {args.clients} clients (pipeline={args.pipeline}) "
          f"in {elapsed:.2f}s -> {total / elapsed:,.0f} ops/sec")


if __name__ == "__main__":
    asyncio.run(main())
