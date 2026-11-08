"""Replicated lock state machine: leases + fencing tokens.

Determinism is what makes replication safe: every replica applies the same
commands in the same order and must reach the same state. So the FSM never
reads a wall clock. Instead the *leader stamps each command with a timestamp*
(``ts``, ms) when it is proposed, and the FSM's notion of "now" is the running
maximum of those stamps (monotonic even across leader changes).

Semantics
---------
* ``acquire``  -- grants the lock if free or its lease expired. A new grant
  bumps a per-resource **fencing token** that never decreases. Re-acquiring as
  the current holder is idempotent: it extends the lease and keeps the token.
* ``renew``    -- extends a *live* lease held by (owner, token). A lease that
  already expired cannot be revived.
* ``release``  -- frees the lock if (owner, token) match the live holder.

Fencing tokens let a downstream resource reject writes from a stale holder
(e.g. one paused by GC past its lease expiry) -- see README.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass
class _Lock:
    holder: str | None = None
    token: int = 0  # last token issued for this resource
    expires_at_ms: int = 0


def _result(ok: bool, code: str, lk: _Lock | None = None, holder: str | None = None) -> dict:
    out = {"ok": ok, "code": code, "holder": "", "token": 0, "expires_at_ms": 0}
    if lk is not None:
        out["holder"] = holder if holder is not None else (lk.holder or "")
        out["token"] = lk.token
        out["expires_at_ms"] = lk.expires_at_ms
    return out


class LockStateMachine:
    def __init__(self) -> None:
        self._locks: dict[str, _Lock] = {}
        self._clock_ms = 0
        self._mu = threading.Lock()  # guards concurrent local reads vs. apply

    # -- replicated writes ------------------------------------------------
    def apply(self, command: dict) -> dict:
        with self._mu:
            self._clock_ms = max(self._clock_ms, int(command["ts"]))
            now = self._clock_ms
            op = command["op"]
            if op == "acquire":
                return self._acquire(command, now)
            if op == "release":
                return self._release(command, now)
            if op == "renew":
                return self._renew(command, now)
            raise ValueError(f"unknown op {op!r}")

    def _acquire(self, c: dict, now: int) -> dict:
        lk = self._locks.setdefault(c["resource"], _Lock())
        live = lk.holder is not None and lk.expires_at_ms > now
        if live and lk.holder != c["owner"]:
            return _result(False, "HELD", lk)
        if live:  # same owner: idempotent re-acquire, keep token, extend lease
            lk.expires_at_ms = now + c["ttl_ms"]
            return _result(True, "OK", lk)
        lk.token += 1
        lk.holder = c["owner"]
        lk.expires_at_ms = now + c["ttl_ms"]
        return _result(True, "OK", lk)

    def _check_holder(self, c: dict, now: int) -> tuple[_Lock | None, dict | None]:
        lk = self._locks.get(c["resource"])
        if lk is None or lk.holder != c["owner"] or lk.token != c["token"]:
            return None, _result(False, "NOT_HOLDER", lk)
        if lk.expires_at_ms <= now:
            return None, _result(False, "EXPIRED", lk)
        return lk, None

    def _release(self, c: dict, now: int) -> dict:
        lk, err = self._check_holder(c, now)
        if err:
            return err
        lk.holder = None
        lk.expires_at_ms = 0
        return _result(True, "OK", lk, holder="")

    def _renew(self, c: dict, now: int) -> dict:
        lk, err = self._check_holder(c, now)
        if err:
            return err
        lk.expires_at_ms = now + c["ttl_ms"]
        return _result(True, "OK", lk)

    # -- local reads (may be stale on a follower) -------------------------
    def query(self, resource: str) -> dict:
        with self._mu:
            lk = self._locks.get(resource)
            if lk is None:
                return _result(True, "OK")
            now = max(self._clock_ms, int(time.time() * 1000))
            if lk.holder is None or lk.expires_at_ms <= now:
                return _result(True, "OK", lk, holder="")
            return _result(True, "OK", lk)

    def snapshot(self) -> dict[str, tuple[str | None, int, int]]:
        """Raw replicated state; identical on all replicas at the same log index."""
        with self._mu:
            return {r: (lk.holder, lk.token, lk.expires_at_ms) for r, lk in self._locks.items()}
