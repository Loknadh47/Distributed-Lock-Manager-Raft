"""Durable Raft state: (currentTerm, votedFor) and the log.

Raft requires these to be persisted *before* responding to any RPC. The file
backend uses a single append-only write-ahead log of JSON lines:

    {"k": "hs", "t": <term>, "v": <voted_for|null>}
    {"k": "w",  "i": <start_index>, "e": [[term, command], ...]}

A ``w`` record means "truncate the log at ``start_index`` and append these
entries", which is the only log mutation Raft ever needs. A torn final line
(crash mid-write) is detected and discarded on replay.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
from typing import Any, Protocol

log = logging.getLogger("dlm.storage")

LogTuple = tuple[int, Any]  # (term, command)


class Storage(Protocol):
    def load(self) -> tuple[int, str | None, list[LogTuple]]: ...
    def save_hard_state(self, term: int, voted_for: str | None) -> None: ...
    def write_entries(self, start_index: int, entries: list[LogTuple]) -> None: ...


class CorruptWAL(Exception):
    pass


class MemoryStorage:
    """Volatile storage that survives *node* restarts inside one process."""

    def __init__(self) -> None:
        self._term = 0
        self._voted: str | None = None
        self._entries: list[LogTuple] = []

    def load(self):
        return self._term, self._voted, list(self._entries)

    def save_hard_state(self, term, voted_for):
        self._term, self._voted = term, voted_for

    def write_entries(self, start_index, entries):
        del self._entries[start_index - 1 :]
        self._entries.extend(entries)


class FileStorage:
    def __init__(self, directory: str, fsync: bool = True):
        os.makedirs(directory, exist_ok=True)
        self._path = os.path.join(directory, "raft.wal")
        self._fsync = fsync
        self._term = 0
        self._voted: str | None = None
        self._entries: list[LogTuple] = []
        self._replay()
        self._fh = open(self._path, "ab", buffering=0)  # noqa: SIM115 (kept open for appends)

    # -- replay -----------------------------------------------------------
    def _replay(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path, "rb") as fh:
            data = fh.read()
        last_nl = data.rfind(b"\n")
        good_len = last_nl + 1
        if good_len != len(data):
            log.warning("discarding %d torn bytes at WAL tail", len(data) - good_len)
            with open(self._path, "r+b") as fh:
                fh.truncate(good_len)
        for lineno, line in enumerate(data[:good_len].split(b"\n")[:-1], start=1):
            try:
                rec = json.loads(line)
                if rec["k"] == "hs":
                    self._term, self._voted = rec["t"], rec["v"]
                elif rec["k"] == "w":
                    start = rec["i"]
                    if start < 1 or start - 1 > len(self._entries):
                        raise CorruptWAL(f"line {lineno}: write at {start}, log length {len(self._entries)}")
                    del self._entries[start - 1 :]
                    self._entries.extend((t, c) for t, c in rec["e"])
                else:
                    raise CorruptWAL(f"line {lineno}: unknown record kind {rec['k']!r}")
            except (ValueError, KeyError, TypeError) as exc:
                raise CorruptWAL(f"line {lineno}: {exc}") from exc

    # -- Storage API ------------------------------------------------------
    def load(self):
        return self._term, self._voted, list(self._entries)

    def _write(self, record: dict) -> None:
        self._fh.write((json.dumps(record, separators=(",", ":")) + "\n").encode())
        if self._fsync:
            os.fsync(self._fh.fileno())

    def save_hard_state(self, term, voted_for):
        self._write({"k": "hs", "t": term, "v": voted_for})

    def write_entries(self, start_index, entries):
        self._write({"k": "w", "i": start_index, "e": [[t, c] for t, c in entries]})

    def close(self) -> None:
        with contextlib.suppress(OSError):
            self._fh.close()
