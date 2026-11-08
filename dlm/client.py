"""Client library: leader discovery, retries with backoff, and auto-renewing locks."""

from __future__ import annotations

import contextlib
import random
import threading
import time
import uuid
from dataclasses import dataclass, field

import grpc

from .v1 import dlm_pb2 as pb
from .v1 import dlm_pb2_grpc as pb_grpc

_RETRY_CODES = {"NOT_LEADER", "RETRY", "TIMEOUT", "UNAVAILABLE"}


class DLMError(Exception):
    pass


class DLMUnavailable(DLMError):
    """No node produced a definitive answer before the deadline."""


@dataclass
class Lease:
    resource: str
    owner: str
    token: int
    expires_at_ms: int
    lost: threading.Event = field(default_factory=threading.Event, repr=False)


class DLMClient:
    def __init__(self, addresses: list[str], *, request_timeout: float = 3.0, overall_timeout: float = 15.0):
        if not addresses:
            raise ValueError("at least one address required")
        self._addrs = list(addresses)
        self._channels = [grpc.insecure_channel(a) for a in self._addrs]
        self._stubs = [pb_grpc.LockServiceStub(c) for c in self._channels]
        self._cur = 0
        self._request_timeout = request_timeout
        self._overall_timeout = overall_timeout
        self.owner_id = f"client-{uuid.uuid4().hex[:12]}"

    def close(self) -> None:
        for c in self._channels:
            c.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- transport with leader discovery ----------------------------------
    def _rpc(self, method: str, request) -> pb.LockResponse:
        deadline = time.monotonic() + self._overall_timeout
        attempt = 0
        while True:
            try:
                resp = getattr(self._stubs[self._cur], method)(request, timeout=self._request_timeout)
            except grpc.RpcError:
                self._cur = (self._cur + 1) % len(self._addrs)
            else:
                if resp.code not in _RETRY_CODES:
                    return resp
                if resp.code == "NOT_LEADER" and resp.leader_hint in self._addrs:
                    self._cur = self._addrs.index(resp.leader_hint)
                else:
                    self._cur = (self._cur + 1) % len(self._addrs)
            if time.monotonic() >= deadline:
                raise DLMUnavailable(f"{method}: no definitive answer within {self._overall_timeout}s")
            time.sleep(min(0.5, 0.05 * 2**attempt) * random.uniform(0.5, 1.0))
            attempt += 1

    # -- lock API ---------------------------------------------------------
    def acquire(
        self, resource: str, *, ttl_ms: int = 10_000, owner: str | None = None, wait_timeout: float = 0.0
    ) -> Lease | None:
        """Try to take the lock, polling up to ``wait_timeout`` seconds. None if still held."""
        owner = owner or self.owner_id
        deadline = time.monotonic() + wait_timeout
        while True:
            r = self._rpc("Acquire", pb.AcquireRequest(resource=resource, owner=owner, ttl_ms=ttl_ms))
            if r.ok:
                return Lease(resource, owner, r.fencing_token, r.expires_at_ms)
            if r.code != "HELD":
                raise DLMError(f"acquire failed: {r.code} {r.message}")
            if time.monotonic() >= deadline:
                return None
            time.sleep(random.uniform(0.02, 0.1))

    def release(self, lease: Lease) -> bool:
        """True if released now; False if the lease was already lost/released."""
        lease.lost.set()  # stops any auto-renewer
        r = self._rpc("Release", pb.ReleaseRequest(resource=lease.resource, owner=lease.owner, fencing_token=lease.token))
        return r.ok

    def renew(self, lease: Lease, ttl_ms: int = 10_000) -> bool:
        r = self._rpc(
            "Renew", pb.RenewRequest(resource=lease.resource, owner=lease.owner, fencing_token=lease.token, ttl_ms=ttl_ms)
        )
        if r.ok:
            lease.expires_at_ms = r.expires_at_ms
        return r.ok

    def status(self, resource: str) -> dict:
        r = self._rpc("Status", pb.StatusRequest(resource=resource))
        return {"holder": r.holder, "fencing_token": r.fencing_token, "expires_at_ms": r.expires_at_ms}

    @contextlib.contextmanager
    def lock(self, resource: str, *, ttl_ms: int = 10_000, wait_timeout: float = 30.0):
        """Hold a lock for the ``with`` body, renewing the lease in the background.

        Check ``lease.lost.is_set()`` in long critical sections, and pass
        ``lease.token`` to downstream systems so they can fence stale holders.
        """
        lease = self.acquire(resource, ttl_ms=ttl_ms, wait_timeout=wait_timeout)
        if lease is None:
            raise DLMError(f"could not acquire {resource!r} within {wait_timeout}s")

        def keepalive() -> None:
            while not lease.lost.wait(ttl_ms / 3000.0):
                try:
                    if not self.renew(lease, ttl_ms):
                        lease.lost.set()  # lease expired or was taken over
                except DLMError:
                    pass  # transient; retry on the next tick

        t = threading.Thread(target=keepalive, daemon=True)
        t.start()
        try:
            yield lease
        finally:
            if not lease.lost.is_set():
                with contextlib.suppress(DLMError):
                    self.release(lease)
            lease.lost.set()
