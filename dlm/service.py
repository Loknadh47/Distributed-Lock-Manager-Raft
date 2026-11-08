"""Lock service: turns client requests into Raft proposals.

Transport-agnostic (the gRPC servicer in ``dlm.server`` is a thin adapter), so
it is fully testable without a network.
"""

from __future__ import annotations

import time
from concurrent.futures import TimeoutError as FutureTimeout

from .lock_fsm import LockStateMachine
from .raft import RaftNode

MAX_TTL_MS = 3_600_000
MAX_NAME_LEN = 256


class ProposalTimeout(Exception):
    """The command was proposed but not committed in time. Outcome is unknown."""


class LockService:
    def __init__(self, node: RaftNode, fsm: LockStateMachine, *, request_timeout: float = 3.0):
        self.node = node
        self.fsm = fsm
        self._timeout = request_timeout

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _check_name(label: str, value: str) -> None:
        if not value or len(value) > MAX_NAME_LEN:
            raise ValueError(f"{label} must be 1..{MAX_NAME_LEN} characters")

    @staticmethod
    def _check_ttl(ttl_ms: int) -> None:
        if not 1 <= ttl_ms <= MAX_TTL_MS:
            raise ValueError(f"ttl_ms must be in 1..{MAX_TTL_MS}")

    def _submit(self, command: dict) -> dict:
        command["ts"] = self._now_ms()
        fut = self.node.propose(command)  # raises NotLeaderError on followers
        try:
            return fut.result(timeout=self._timeout)
        except FutureTimeout as exc:
            raise ProposalTimeout(f"{command['op']} not committed within {self._timeout}s") from exc

    def acquire(self, resource: str, owner: str, ttl_ms: int) -> dict:
        self._check_name("resource", resource)
        self._check_name("owner", owner)
        self._check_ttl(ttl_ms)
        return self._submit({"op": "acquire", "resource": resource, "owner": owner, "ttl_ms": ttl_ms})

    def release(self, resource: str, owner: str, token: int) -> dict:
        self._check_name("resource", resource)
        self._check_name("owner", owner)
        return self._submit({"op": "release", "resource": resource, "owner": owner, "token": token})

    def renew(self, resource: str, owner: str, token: int, ttl_ms: int) -> dict:
        self._check_name("resource", resource)
        self._check_name("owner", owner)
        self._check_ttl(ttl_ms)
        return self._submit(
            {"op": "renew", "resource": resource, "owner": owner, "token": token, "ttl_ms": ttl_ms}
        )

    def status(self, resource: str) -> dict:
        """Local (possibly stale) read; served by any node."""
        self._check_name("resource", resource)
        return self.fsm.query(resource)
