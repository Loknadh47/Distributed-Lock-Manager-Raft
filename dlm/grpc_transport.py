"""gRPC implementation of the Raft ``Transport`` (peer-to-peer RPCs)."""

from __future__ import annotations

import json

import grpc

from .raft.errors import TransportError
from .v1 import dlm_pb2 as pb
from .v1 import dlm_pb2_grpc as pb_grpc


class GrpcTransport:
    def __init__(self, peer_addrs: dict[str, str]):
        """``peer_addrs`` maps node id -> ``host:port``."""
        self._channels = {nid: grpc.insecure_channel(addr) for nid, addr in peer_addrs.items()}
        self._stubs = {nid: pb_grpc.RaftStub(ch) for nid, ch in self._channels.items()}

    def call(self, peer_id: str, method: str, request: dict, timeout: float) -> dict:
        stub = self._stubs.get(peer_id)
        if stub is None:
            raise TransportError(f"unknown peer {peer_id}")
        try:
            if method == "RequestVote":
                resp = stub.RequestVote(
                    pb.RequestVoteRequest(
                        term=request["term"],
                        candidate_id=request["candidate_id"],
                        last_log_index=request["last_log_index"],
                        last_log_term=request["last_log_term"],
                    ),
                    timeout=timeout,
                )
                return {"term": resp.term, "vote_granted": resp.vote_granted}
            if method == "AppendEntries":
                resp = stub.AppendEntries(
                    pb.AppendEntriesRequest(
                        term=request["term"],
                        leader_id=request["leader_id"],
                        prev_log_index=request["prev_log_index"],
                        prev_log_term=request["prev_log_term"],
                        entries=[
                            pb.LogEntry(term=e["term"], command=json.dumps(e["command"]).encode())
                            for e in request["entries"]
                        ],
                        leader_commit=request["leader_commit"],
                    ),
                    timeout=timeout,
                )
                return {
                    "term": resp.term,
                    "success": resp.success,
                    "match_index": resp.match_index,
                    "conflict_index": resp.conflict_index,
                    "conflict_term": resp.conflict_term,
                }
        except grpc.RpcError as exc:
            raise TransportError(f"{peer_id}.{method}: {exc.code()}") from exc
        raise TransportError(f"unknown method {method}")

    def close(self) -> None:
        for ch in self._channels.values():
            ch.close()
