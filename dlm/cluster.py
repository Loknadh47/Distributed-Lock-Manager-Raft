"""In-process cluster used by the tests and the local demo (no gRPC needed)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

from .lock_fsm import LockStateMachine
from .raft import (
    LEADER,
    InMemoryNetwork,
    InMemoryTransport,
    MemoryStorage,
    NodeStopped,
    NotLeaderError,
    ProposalDropped,
    RaftNode,
)
from .service import LockService, ProposalTimeout

# Short timers so tests converge quickly; production defaults live in RaftNode.
FAST = {"election_timeout": (0.08, 0.16), "heartbeat_interval": 0.02, "tick": 0.002}
RETRYABLE = (NotLeaderError, ProposalDropped, ProposalTimeout, NodeStopped)


def wait_for(predicate: Callable[[], Any], timeout: float = 5.0, interval: float = 0.01) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


class LocalCluster:
    def __init__(
        self,
        size: int = 3,
        *,
        storage_factory: Callable[[str], Any] | None = None,
        seed: int = 1,
        request_timeout: float = 1.0,
        **node_kwargs: Any,
    ):
        self.ids = [f"n{i + 1}" for i in range(size)]
        self.network = InMemoryNetwork(seed=seed)
        self._seed = seed
        self._request_timeout = request_timeout
        self._node_kwargs = {**FAST, **node_kwargs}
        if storage_factory is None:
            cache: dict[str, MemoryStorage] = {}
            storage_factory = lambda nid: cache.setdefault(nid, MemoryStorage())  # noqa: E731
        self._storage_factory = storage_factory
        self._storages: dict[str, Any] = {}
        self.nodes: dict[str, RaftNode] = {}
        self.fsms: dict[str, LockStateMachine] = {}
        self.services: dict[str, LockService] = {}
        self.applied: dict[str, list[dict]] = {}
        self._leaders_by_term: dict[int, set[str]] = {}
        self._mu = threading.Lock()
        for nid in self.ids:
            self._build(nid)

    # -- lifecycle --------------------------------------------------------
    def _record_leader(self, node_id: str, term: int) -> None:
        with self._mu:
            self._leaders_by_term.setdefault(term, set()).add(node_id)

    def _build(self, nid: str) -> None:
        fsm = LockStateMachine()
        applied: list[dict] = []

        def apply(cmd: dict, fsm=fsm, applied=applied) -> dict:
            applied.append(cmd)
            return fsm.apply(cmd)

        storage = self._storage_factory(nid)
        node = RaftNode(
            nid,
            self.ids,
            InMemoryTransport(self.network, nid),
            storage,
            apply,
            seed=self._seed * 100 + self.ids.index(nid),
            on_become_leader=self._record_leader,
            **self._node_kwargs,
        )
        self._storages[nid] = storage
        self.fsms[nid], self.nodes[nid], self.applied[nid] = fsm, node, applied
        self.services[nid] = LockService(node, fsm, request_timeout=self._request_timeout)
        self.network.register(nid, node)

    def start(self) -> None:
        for node in self.nodes.values():
            node.start()

    def stop(self, nid: str) -> None:
        self.nodes[nid].stop()
        close = getattr(self._storages[nid], "close", None)
        if close:
            close()

    def restart(self, nid: str) -> None:
        """Crash-restart: new node + new state machine, same durable storage."""
        self.stop(nid)
        self._build(nid)
        self.nodes[nid].start()

    def shutdown(self) -> None:
        for nid in self.ids:
            self.stop(nid)

    # -- queries ----------------------------------------------------------
    def running_ids(self) -> list[str]:
        return [n for n in self.ids if self.nodes[n].is_running]

    def leader(self, among: list[str] | None = None) -> str | None:
        best: tuple[int, str] | None = None
        for nid in among or self.running_ids():
            st = self.nodes[nid].status()
            if st["role"] == LEADER and self.nodes[nid].is_running and (best is None or st["term"] > best[0]):
                best = (st["term"], nid)
        return best[1] if best else None

    def wait_for_leader(self, timeout: float = 5.0, among: list[str] | None = None) -> str:
        found: list[str] = []

        def check() -> bool:
            leader = self.leader(among)
            if leader:
                found[:] = [leader]
            return bool(leader)

        if not wait_for(check, timeout):
            raise TimeoutError("no leader elected")
        return found[0]

    def leaders_by_term(self) -> dict[int, set[str]]:
        with self._mu:
            return {t: set(s) for t, s in self._leaders_by_term.items()}

    def call(self, fn: Callable[[LockService], dict], timeout: float = 10.0) -> dict:
        """Run ``fn(service)`` against the current leader, retrying across failovers."""
        deadline = time.monotonic() + timeout
        last: Exception | None = None
        while time.monotonic() < deadline:
            leader = self.leader()
            if leader:
                try:
                    return fn(self.services[leader])
                except RETRYABLE as exc:
                    last = exc
            time.sleep(0.02)
        raise TimeoutError(f"no successful call within {timeout}s (last error: {last!r})")
