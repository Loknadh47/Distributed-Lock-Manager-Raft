"""Exceptions shared by the Raft core, transports and the lock service."""

from __future__ import annotations


class NotLeaderError(Exception):
    """The node is not the leader; ``leader_id`` is a hint (may be None)."""

    def __init__(self, leader_id: str | None = None):
        super().__init__(f"not the leader (leader hint: {leader_id})")
        self.leader_id = leader_id


class ProposalDropped(Exception):
    """The proposed entry was overwritten by a new leader and will never commit."""


class NodeStopped(Exception):
    """The node was stopped while the request was in flight."""


class TransportError(Exception):
    """An RPC to a peer failed (unreachable, timed out, dropped, ...)."""
