"""
Append-only file (AOF) persistence.

Every write command is appended to a log file in RESP format. On startup the log is
replayed from the beginning to rebuild the data, so nothing is lost when the server
restarts. REWRITEAOF compacts the log down to the minimum commands needed to recreate
the current data (e.g. 1,000 INCRs on one key become a single SET).
"""

import asyncio
import os

from .protocol import ProtocolError, encode_command, read_command

FSYNC_POLICIES = ("always", "everysec", "no")


class AppendOnlyFile:
    """
    fsync policies trade speed for durability, like Redis:
      always   - fsync after every write: safest, slowest
      everysec - fsync once a second: lose at most ~1s of writes on a power cut (default)
      no       - leave flushing to the OS: fastest
    """

    def __init__(self, path, fsync="everysec"):
        if fsync not in FSYNC_POLICIES:
            raise ValueError(f"fsync must be one of {FSYNC_POLICIES}")
        self.path = path
        self.fsync = fsync
        self.file = open(path, "ab")
        self.dirty = False

    def append(self, args):
        self.file.write(encode_command(args))
        self.file.flush()  # hand the data to the operating system
        if self.fsync == "always":
            os.fsync(self.file.fileno())  # force it onto the disk
        else:
            self.dirty = True

    def sync(self):
        """Called once a second by the server when the policy is 'everysec'."""
        if self.dirty and self.fsync == "everysec":
            os.fsync(self.file.fileno())
            self.dirty = False

    def rewrite(self, commands):
        """Replace the log with a compact one. Writes a temp file first so a crash can't corrupt the log."""
        temp_path = self.path + ".rewrite"
        with open(temp_path, "wb") as temp:
            for args in commands:
                temp.write(encode_command(args))
            temp.flush()
            os.fsync(temp.fileno())
        self.file.close()
        os.replace(temp_path, self.path)  # atomic on POSIX and Windows
        self.file = open(self.path, "ab")
        self.dirty = False

    def close(self):
        if not self.file.closed:
            self.file.flush()
            os.fsync(self.file.fileno())
            self.file.close()


async def replay(path, handler):
    """Re-run every command in the AOF through handler.

    Returns (commands_loaded, bytes_discarded). If the server crashed halfway through
    writing the last command, that partial command is discarded and the file is
    truncated so new writes are appended after the last complete command.
    """
    if not os.path.exists(path):
        return 0, 0

    with open(path, "rb") as f:
        data = f.read()

    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()

    loaded = 0
    valid_bytes = 0  # length of the log up to the end of the last complete command
    handler.loading = True
    try:
        while True:
            try:
                args = await read_command(reader)
            except (asyncio.IncompleteReadError, ProtocolError):
                break
            if args is None:
                break
            handler.execute(args)
            loaded += 1
            valid_bytes += len(encode_command(args))
    finally:
        handler.loading = False

    discarded = len(data) - valid_bytes
    if discarded:
        with open(path, "r+b") as f:
            f.truncate(valid_bytes)
    return loaded, discarded
