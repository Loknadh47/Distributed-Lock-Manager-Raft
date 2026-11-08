"""A compact, transport-agnostic implementation of the Raft consensus algorithm.

Implements leader election, log replication, commit/apply, persistence of
hard state, a no-op entry on election (so entries from earlier terms can be
committed), fast log backtracking on conflicts, and check-quorum (a leader that
cannot reach a majority steps down).

Not implemented (see README roadmap): log compaction/snapshots, membership
changes, pre-vote, ReadIndex.

Concurrency model: one re-entrant lock guards all state. RPCs are always sent
*outside* the lock from a small thread pool, so a slow peer never blocks the
node. The state machine is applied under the lock, in log order.
"""

from __future__ import annotations

import contextlib
import logging
import random
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from .errors import NodeStopped, NotLeaderError, ProposalDropped
from .storage import Storage
from .transport import Transport

log = logging.getLogger("dlm.raft")

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


@dataclass(frozen=True)
class Entry:
    term: int
    command: Any  # None marks a no-op


class RaftNode:
    MAX_BATCH = 64  # entries per AppendEntries

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        transport: Transport,
        storage: Storage,
        apply_fn: Callable[[Any], Any],
        *,
        election_timeout: tuple[float, float] = (0.15, 0.30),
        heartbeat_interval: float = 0.05,
        rpc_timeout: float = 0.25,
        tick: float = 0.005,
        seed: int | None = None,
        on_become_leader: Callable[[str, int], None] | None = None,
    ):
        self.id = node_id
        self.peers = [p for p in peers if p != node_id]
        self._quorum = (len(self.peers) + 1) // 2 + 1
        self._transport = transport
        self._storage = storage
        self._apply_fn = apply_fn
        self._election_timeout = election_timeout
        self._hb = heartbeat_interval
        self._rpc_timeout = rpc_timeout
        self._tick = tick
        self._rng = random.Random(seed)
        self._on_become_leader = on_become_leader
        self._clock = time.monotonic

        # Persistent state (restored from storage).
        term, voted_for, entries = storage.load()
        self.current_term: int = term
        self.voted_for: str | None = voted_for
        self.log: list[Entry] = [Entry(0, None)] + [Entry(t, c) for t, c in entries]

        # Volatile state.
        self.commit_index = 0
        self.last_applied = 0
        self.role = FOLLOWER
        self.leader_id: str | None = None
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._inflight: dict[str, bool] = {}
        self._last_ack: dict[str, float] = {}
        self._votes: set[str] = set()
        self._pending: dict[int, tuple[int, Future]] = {}
        self._election_deadline = 0.0
        self._next_heartbeat = 0.0

        self._lock = threading.RLock()
        self._stop_evt = threading.Event()
        self._stopped = False
        self._started = False
        self._executor = ThreadPoolExecutor(
            max_workers=max(2, 2 * len(self.peers)), thread_name_prefix=f"raft-{node_id}"
        )
        self._thread = threading.Thread(target=self._run, name=f"raft-ticker-{node_id}", daemon=True)

    # ------------------------------------------------------------------ API
    @property
    def is_running(self) -> bool:
        return self._started and not self._stopped

    def start(self) -> None:
        with self._lock:
            self._reset_election_timer()
            self._started = True
        self._thread.start()

    def stop(self) -> None:
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            for _, fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(NodeStopped(self.id))
            self._pending.clear()
        self._stop_evt.set()
        if self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(timeout=1.0)
        self._executor.shutdown(wait=False, cancel_futures=True)

    def propose(self, command: Any) -> Future:
        """Append ``command`` to the log (leader only).

        The returned future resolves with the state machine's result once the
        entry is committed and applied, or fails with :class:`ProposalDropped`
        if a new leader overwrites it. If the caller times out the outcome is
        *unknown* (the entry may still commit), so commands should be idempotent.
        """
        if command is None:
            raise ValueError("command must not be None")
        with self._lock:
            if self._stopped:
                raise NodeStopped(self.id)
            if self.role != LEADER:
                raise NotLeaderError(self.leader_id)
            idx = self._append_local(command)
            fut: Future = Future()
            self._pending[idx] = (self.current_term, fut)
            self._advance_commit()  # commits immediately in a single-node cluster
            self._broadcast()
            return fut

    def status(self) -> dict:
        with self._lock:
            return {
                "id": self.id,
                "role": self.role,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "last_index": self._last_index(),
            }

    # ------------------------------------------------------------- helpers
    def _last_index(self) -> int:
        return len(self.log) - 1

    def _term_at(self, index: int) -> int:
        return self.log[index].term

    def _submit(self, fn, *args) -> None:
        with contextlib.suppress(RuntimeError):  # executor already shut down
            self._executor.submit(fn, *args)

    def _reset_election_timer(self) -> None:
        self._election_deadline = self._clock() + self._rng.uniform(*self._election_timeout)

    def _persist_hard_state(self) -> None:
        self._storage.save_hard_state(self.current_term, self.voted_for)

    def _append_local(self, command: Any) -> int:
        idx = self._last_index() + 1
        self.log.append(Entry(self.current_term, command))
        self._storage.write_entries(idx, [(self.current_term, command)])
        return idx

    def _truncate_from(self, index: int) -> None:
        del self.log[index:]
        for i in [i for i in self._pending if i >= index]:
            _, fut = self._pending.pop(i)
            if not fut.done():
                fut.set_exception(ProposalDropped(f"entry {i} overwritten by a newer leader"))

    def _become_follower(self, term: int, leader_id: str | None = None) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._persist_hard_state()
        if self.role != FOLLOWER:
            log.info("%s -> follower (term %d)", self.id, self.current_term)
        self.role = FOLLOWER
        self.leader_id = leader_id
        self._votes = set()
        self._reset_election_timer()

    # ------------------------------------------------------------- ticker
    def _run(self) -> None:
        while not self._stop_evt.wait(self._tick):
            with self._lock:
                if self._stopped:
                    return
                now = self._clock()
                if self.role == LEADER:
                    self._check_quorum(now)
                    if self.role == LEADER and now >= self._next_heartbeat:
                        self._next_heartbeat = now + self._hb
                        self._broadcast()
                elif now >= self._election_deadline:
                    self._start_election()

    def _check_quorum(self, now: float) -> None:
        if not self.peers:
            return
        window = self._election_timeout[1]
        alive = 1 + sum(1 for p in self.peers if now - self._last_ack.get(p, 0.0) < window)
        if alive < self._quorum:
            log.info("%s lost contact with a quorum; stepping down", self.id)
            self._become_follower(self.current_term)

    # ----------------------------------------------------------- election
    def _start_election(self) -> None:
        self.current_term += 1
        self.role = CANDIDATE
        self.voted_for = self.id
        self.leader_id = None
        self._persist_hard_state()
        self._reset_election_timer()
        self._votes = {self.id}
        term = self.current_term
        log.info("%s starts election for term %d", self.id, term)
        if len(self._votes) >= self._quorum:
            self._become_leader()
            return
        last = self._last_index()
        req = {
            "term": term,
            "candidate_id": self.id,
            "last_log_index": last,
            "last_log_term": self._term_at(last),
        }
        for p in self.peers:
            self._submit(self._send_vote_request, p, term, req)

    def _send_vote_request(self, peer: str, term: int, req: dict) -> None:
        try:
            resp = self._transport.call(peer, "RequestVote", req, self._rpc_timeout)
        except Exception:
            return
        with self._lock:
            if self._stopped:
                return
            if resp["term"] > self.current_term:
                self._become_follower(resp["term"])
                return
            if self.role != CANDIDATE or self.current_term != term or not resp["vote_granted"]:
                return
            self._votes.add(peer)
            if len(self._votes) >= self._quorum:
                self._become_leader()

    def _become_leader(self) -> None:
        now = self._clock()
        self.role = LEADER
        self.leader_id = self.id
        last = self._last_index()
        self.next_index = {p: last + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self._inflight = {p: False for p in self.peers}
        self._last_ack = {p: now for p in self.peers}
        log.info("%s became leader for term %d", self.id, self.current_term)
        # No-op in the new term: lets entries from earlier terms commit (Raft §5.4.2).
        self._append_local(None)
        self._next_heartbeat = now + self._hb
        self._advance_commit()
        self._broadcast()
        if self._on_become_leader:
            self._on_become_leader(self.id, self.current_term)

    def handle_request_vote(self, req: dict) -> dict:
        with self._lock:
            self._check_running()
            if req["term"] < self.current_term:
                return {"term": self.current_term, "vote_granted": False}
            if req["term"] > self.current_term:
                self._become_follower(req["term"])
            last = self._last_index()
            up_to_date = (req["last_log_term"], req["last_log_index"]) >= (self._term_at(last), last)
            granted = up_to_date and self.voted_for in (None, req["candidate_id"])
            if granted:
                self.voted_for = req["candidate_id"]
                self._persist_hard_state()  # durable before the reply leaves
                self._reset_election_timer()
            return {"term": self.current_term, "vote_granted": granted}

    # -------------------------------------------------------- replication
    def _broadcast(self) -> None:
        for p in self.peers:
            self._submit(self._replicate_to, p)

    def _replicate_to(self, peer: str) -> None:
        with self._lock:
            if self._stopped or self.role != LEADER or self._inflight.get(peer):
                return
            term = self.current_term
            ni = self.next_index[peer]
            prev = ni - 1
            req = {
                "term": term,
                "leader_id": self.id,
                "prev_log_index": prev,
                "prev_log_term": self._term_at(prev),
                "entries": [{"term": e.term, "command": e.command} for e in self.log[ni : ni + self.MAX_BATCH]],
                "leader_commit": self.commit_index,
            }
            self._inflight[peer] = True
        try:
            resp = self._transport.call(peer, "AppendEntries", req, self._rpc_timeout)
        except Exception:
            with self._lock:
                self._inflight[peer] = False
            return
        with self._lock:
            self._inflight[peer] = False
            if self._stopped:
                return
            if resp["term"] > self.current_term:
                self._become_follower(resp["term"])
                return
            if self.role != LEADER or self.current_term != term:
                return
            self._last_ack[peer] = self._clock()
            if resp["success"]:
                self.match_index[peer] = max(self.match_index[peer], resp["match_index"])
                self.next_index[peer] = self.match_index[peer] + 1
                self._advance_commit()
                if self.next_index[peer] <= self._last_index():
                    self._submit(self._replicate_to, peer)
            else:
                self.next_index[peer] = self._next_after_conflict(resp, ni)
                self._submit(self._replicate_to, peer)

    def _next_after_conflict(self, resp: dict, ni: int) -> int:
        """Fast backtracking (Raft §5.3 optimisation); always makes progress."""
        ci, ct = resp.get("conflict_index", 0), resp.get("conflict_term", 0)
        candidate = ci if ci else ni - 1
        if ct:
            for i in range(self._last_index(), 0, -1):
                t = self._term_at(i)
                if t == ct:
                    candidate = i + 1
                    break
                if t < ct:
                    break
        return max(1, min(candidate, ni - 1))

    def handle_append_entries(self, req: dict) -> dict:
        with self._lock:
            self._check_running()

            def reply(success: bool, match=0, conflict_index=0, conflict_term=0) -> dict:
                return {
                    "term": self.current_term,
                    "success": success,
                    "match_index": match,
                    "conflict_index": conflict_index,
                    "conflict_term": conflict_term,
                }

            if req["term"] < self.current_term:
                return reply(False)
            if req["term"] > self.current_term or self.role != FOLLOWER:
                self._become_follower(req["term"], req["leader_id"])
            self.leader_id = req["leader_id"]
            self._reset_election_timer()

            prev_idx, prev_term = req["prev_log_index"], req["prev_log_term"]
            last = self._last_index()
            if prev_idx > last:
                return reply(False, conflict_index=last + 1)
            if self._term_at(prev_idx) != prev_term:
                ct = self._term_at(prev_idx)
                ci = prev_idx
                while ci > 1 and self._term_at(ci - 1) == ct:
                    ci -= 1
                return reply(False, conflict_index=ci, conflict_term=ct)

            entries = [Entry(e["term"], e["command"]) for e in req["entries"]]
            write_from: int | None = None
            to_write: list[tuple[int, Any]] = []
            for offset, e in enumerate(entries):
                idx = prev_idx + 1 + offset
                if write_from is None:
                    if idx <= self._last_index():
                        if self._term_at(idx) == e.term:
                            continue  # already have it
                        self._truncate_from(idx)  # conflict: drop it and everything after
                    write_from = idx
                self.log.append(e)
                to_write.append((e.term, e.command))
            if to_write:
                self._storage.write_entries(write_from, to_write)  # durable before ack

            match = prev_idx + len(entries)
            new_commit = min(req["leader_commit"], match)
            if new_commit > self.commit_index:
                self.commit_index = new_commit
                self._apply_committed()
            return reply(True, match=match)

    # ------------------------------------------------------------- commit
    def _advance_commit(self) -> None:
        if self.role == LEADER:
            for n in range(self._last_index(), self.commit_index, -1):
                if self._term_at(n) != self.current_term:
                    break  # only entries from the current term commit by counting
                replicas = 1 + sum(1 for p in self.peers if self.match_index[p] >= n)
                if replicas >= self._quorum:
                    self.commit_index = n
                    break
        self._apply_committed()

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            result, error = None, None
            if entry.command is not None:
                try:
                    result = self._apply_fn(entry.command)
                except Exception as exc:  # state machine bug; surface to caller
                    log.exception("state machine failed on entry %d", self.last_applied)
                    error = exc
            pending = self._pending.pop(self.last_applied, None)
            if pending:
                term, fut = pending
                if fut.done():
                    continue
                if term != entry.term:
                    fut.set_exception(ProposalDropped(f"entry {self.last_applied} replaced"))
                elif error is not None:
                    fut.set_exception(error)
                else:
                    fut.set_result(result)

    def _check_running(self) -> None:
        if self._stopped:
            raise NodeStopped(self.id)
