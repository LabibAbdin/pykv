"""Command-line entry point: python -m pykv [--port 6380] [--aof data.aof]"""

import argparse
import asyncio
import logging

from .aof import FSYNC_POLICIES
from .server import Server


def main():
    parser = argparse.ArgumentParser(description="PyKV: a Redis-compatible key-value server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--aof", metavar="PATH", help="enable persistence to this append-only file")
    parser.add_argument("--fsync", choices=FSYNC_POLICIES, default="everysec")
    parser.add_argument("--max-keys", type=int, help="evict least recently used keys beyond this limit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    server = Server(args.host, args.port, args.aof, args.fsync, args.max_keys)
    try:
        asyncio.run(server.serve_forever())
    except KeyboardInterrupt:
        logging.info("Shutting down")


if __name__ == "__main__":
    main()
