"""gRPC server: exposes the Raft peer API and the client-facing lock API on one port.

Configuration (flags or environment variables):
    --id / NODE_ID        this node's id, e.g. n1
    --peers / PEERS       comma list of id=host:port for ALL nodes (including self)
    --listen / LISTEN     bind address, default 0.0.0.0:50051
    --data-dir / DATA_DIR directory for the write-ahead log, default ./data
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import threading
from concurrent.futures import ThreadPoolExecutor

import grpc

from .grpc_transport import GrpcTransport
from .lock_fsm import LockStateMachine
from .raft import FileStorage, NodeStopped, NotLeaderError, ProposalDropped, RaftNode
from .service import LockService, ProposalTimeout
from .v1 import dlm_pb2 as pb
from .v1 import dlm_pb2_grpc as pb_grpc

log = logging.getLogger("dlm.server")


class _RaftServicer(pb_grpc.RaftServicer):
    def __init__(self, node: RaftNode):
        self._node = node

    def RequestVote(self, request, context):  # noqa: N802 (gRPC naming)
        try:
            r = self._node.handle_request_vote(
                {
                    "term": request.term,
                    "candidate_id": request.candidate_id,
                    "last_log_index": request.last_log_index,
                    "last_log_term": request.last_log_term,
                }
            )
        except NodeStopped:
            context.abort(grpc.StatusCode.UNAVAILABLE, "node stopped")
        return pb.RequestVoteResponse(term=r["term"], vote_granted=r["vote_granted"])

    def AppendEntries(self, request, context):  # noqa: N802
        try:
            r = self._node.handle_append_entries(
                {
                    "term": request.term,
                    "leader_id": request.leader_id,
                    "prev_log_index": request.prev_log_index,
                    "prev_log_term": request.prev_log_term,
                    "entries": [{"term": e.term, "command": json.loads(e.command)} for e in request.entries],
                    "leader_commit": request.leader_commit,
                }
            )
        except NodeStopped:
            context.abort(grpc.StatusCode.UNAVAILABLE, "node stopped")
        return pb.AppendEntriesResponse(
            term=r["term"],
            success=r["success"],
            match_index=r["match_index"],
            conflict_index=r["conflict_index"],
            conflict_term=r["conflict_term"],
        )


class _LockServicer(pb_grpc.LockServiceServicer):
    def __init__(self, service: LockService, peer_addrs: dict[str, str]):
        self._svc = service
        self._addrs = peer_addrs

    def _respond(self, fn) -> pb.LockResponse:
        try:
            r = fn()
        except NotLeaderError as exc:
            return pb.LockResponse(
                ok=False, code="NOT_LEADER", leader_hint=self._addrs.get(exc.leader_id or "", ""), message=str(exc)
            )
        except ProposalDropped as exc:
            return pb.LockResponse(ok=False, code="RETRY", message=str(exc))  # definitely not applied
        except ProposalTimeout as exc:
            return pb.LockResponse(ok=False, code="TIMEOUT", message=str(exc))  # outcome unknown
        except NodeStopped:
            return pb.LockResponse(ok=False, code="UNAVAILABLE", message="node stopping")
        except ValueError as exc:
            return pb.LockResponse(ok=False, code="INVALID_ARGUMENT", message=str(exc))
        return pb.LockResponse(
            ok=r["ok"],
            code=r["code"],
            holder=r["holder"],
            fencing_token=r["token"],
            expires_at_ms=r["expires_at_ms"],
        )

    def Acquire(self, request, context):  # noqa: N802
        return self._respond(lambda: self._svc.acquire(request.resource, request.owner, request.ttl_ms))

    def Release(self, request, context):  # noqa: N802
        return self._respond(lambda: self._svc.release(request.resource, request.owner, request.fencing_token))

    def Renew(self, request, context):  # noqa: N802
        return self._respond(
            lambda: self._svc.renew(request.resource, request.owner, request.fencing_token, request.ttl_ms)
        )

    def Status(self, request, context):  # noqa: N802
        return self._respond(lambda: self._svc.status(request.resource))


class ServerHandle:
    def __init__(self, server: grpc.Server, node: RaftNode, transport: GrpcTransport, storage: FileStorage):
        self.server, self.node, self._transport, self._storage = server, node, transport, storage

    def start(self) -> None:
        self.server.start()
        self.node.start()

    def stop(self, grace: float = 2.0) -> None:
        self.node.stop()
        self.server.stop(grace).wait()
        self._transport.close()
        self._storage.close()


def build_server(
    node_id: str,
    listen: str,
    peer_addrs: dict[str, str],
    data_dir: str,
    *,
    fsync: bool = True,
    request_timeout: float = 3.0,
    **node_kwargs,
) -> ServerHandle:
    storage = FileStorage(os.path.join(data_dir, node_id), fsync=fsync)
    fsm = LockStateMachine()
    transport = GrpcTransport({p: a for p, a in peer_addrs.items() if p != node_id})
    node = RaftNode(node_id, list(peer_addrs), transport, storage, fsm.apply, **node_kwargs)
    service = LockService(node, fsm, request_timeout=request_timeout)
    server = grpc.server(ThreadPoolExecutor(max_workers=32, thread_name_prefix="grpc"))
    pb_grpc.add_RaftServicer_to_server(_RaftServicer(node), server)
    pb_grpc.add_LockServiceServicer_to_server(_LockServicer(service, peer_addrs), server)
    if server.add_insecure_port(listen) == 0:
        raise RuntimeError(f"could not bind {listen}")
    return ServerHandle(server, node, transport, storage)


def parse_peers(spec: str) -> dict[str, str]:
    peers: dict[str, str] = {}
    for item in filter(None, (s.strip() for s in spec.split(","))):
        nid, _, addr = item.partition("=")
        if not nid or not addr:
            raise ValueError(f"bad peer spec {item!r}; expected id=host:port")
        peers[nid] = addr
    return peers


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Raft-backed distributed lock manager node")
    p.add_argument("--id", default=os.environ.get("NODE_ID"), required="NODE_ID" not in os.environ)
    p.add_argument("--peers", default=os.environ.get("PEERS"), required="PEERS" not in os.environ)
    p.add_argument("--listen", default=os.environ.get("LISTEN", "0.0.0.0:50051"))
    p.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "./data"))
    p.add_argument("--no-fsync", action="store_true", help="unsafe: skip fsync (benchmarks only)")
    p.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    args = p.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    peers = parse_peers(args.peers)
    if args.id not in peers:
        p.error(f"--id {args.id!r} must appear in --peers")

    handle = build_server(args.id, args.listen, peers, args.data_dir, fsync=not args.no_fsync)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    handle.start()
    log.info("node %s listening on %s (cluster: %s)", args.id, args.listen, ", ".join(sorted(peers)))
    stop.wait()
    log.info("shutting down")
    handle.stop()


if __name__ == "__main__":
    main()
