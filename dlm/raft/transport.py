"""Transport abstraction plus an in-memory network with fault injection.

The Raft node only knows about ``Transport.call``; production uses gRPC
(``dlm.grpc_transport``), tests use :class:`InMemoryNetwork`, which can
partition, isolate, delay and drop messages deterministically.
"""

from __future__ import annotations

import copy
import random
import threading
import time
from typing import Protocol

from .errors import NodeStopped, TransportError


class Transport(Protocol):
    def call(self, peer_id: str, method: str, request: dict, timeout: float) -> dict:
        """Send ``method`` ("RequestVote" | "AppendEntries") to ``peer_id``.

        Must raise :class:`TransportError` on any failure.
        """
        ...


class InMemoryNetwork:
    """A simulated network connecting :class:`RaftNode` instances in-process."""

    def __init__(self, seed: int = 0):
        self._nodes: dict[str, object] = {}
        self._blocked: set[tuple[str, str]] = set()
        self._isolated: set[str] = set()
        self._mu = threading.Lock()
        self._rng = random.Random(seed)
        self.drop_rate = 0.0
        self.max_delay = 0.0

    # -- topology ---------------------------------------------------------
    def register(self, node_id: str, node: object) -> None:
        with self._mu:
            self._nodes[node_id] = node

    def isolate(self, node_id: str) -> None:
        """Cut every link to and from ``node_id``."""
        with self._mu:
            self._isolated.add(node_id)

    def partition(self, *groups: list[str]) -> None:
        """Only nodes within the same group can talk to each other."""
        with self._mu:
            self._blocked.clear()
            for i, g in enumerate(groups):
                for h in groups[i + 1 :]:
                    for a in g:
                        for b in h:
                            self._blocked.add((a, b))
                            self._blocked.add((b, a))

    def heal(self) -> None:
        with self._mu:
            self._blocked.clear()
            self._isolated.clear()

    # -- delivery ---------------------------------------------------------
    def deliver(self, src: str, dst: str, method: str, request: dict) -> dict:
        with self._mu:
            node = self._nodes.get(dst)
            blocked = src in self._isolated or dst in self._isolated or (src, dst) in self._blocked
            dropped = self.drop_rate > 0 and self._rng.random() < self.drop_rate
            delay = self._rng.uniform(0, self.max_delay) if self.max_delay else 0.0
        if node is None or blocked or dropped or not node.is_running:
            raise TransportError(f"{src}->{dst} unreachable")
        if delay:
            time.sleep(delay)
        handlers = {
            "RequestVote": node.handle_request_vote,
            "AppendEntries": node.handle_append_entries,
        }
        try:
            # deepcopy emulates serialisation: no shared mutable state between nodes.
            response = handlers[method](copy.deepcopy(request))
        except NodeStopped as exc:
            raise TransportError(str(exc)) from exc
        return copy.deepcopy(response)


class InMemoryTransport:
    """Per-node view of an :class:`InMemoryNetwork`."""

    def __init__(self, network: InMemoryNetwork, src_id: str):
        self._network = network
        self._src = src_id

    def call(self, peer_id: str, method: str, request: dict, timeout: float) -> dict:
        return self._network.deliver(self._src, peer_id, method, request)
