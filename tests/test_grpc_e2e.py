"""End-to-end test over real gRPC. Skipped unless grpcio is installed and `make proto` has run."""

import socket
import time

import pytest

pytest.importorskip("grpc")
pytest.importorskip("dlm.v1.dlm_pb2_grpc")

from dlm.client import DLMClient  # noqa: E402
from dlm.server import build_server  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def cluster(tmp_path):
    ids = ["n1", "n2", "n3"]
    addrs = {i: f"127.0.0.1:{_free_port()}" for i in ids}
    handles = {
        i: build_server(
            i, addrs[i], addrs, str(tmp_path), fsync=False,
            election_timeout=(0.15, 0.3), heartbeat_interval=0.05,
        )
        for i in ids
    }
    for h in handles.values():
        h.start()
    yield addrs, handles
    for h in handles.values():
        h.stop(grace=0.5)


def _wait_leader(handles, timeout=10.0, exclude=()):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for nid, h in handles.items():
            if nid not in exclude and h.node.status()["role"] == "leader":
                return nid
        time.sleep(0.05)
    raise TimeoutError("no leader")


def test_lock_lifecycle_over_grpc_with_failover(cluster):
    addrs, handles = cluster
    _wait_leader(handles)
    with DLMClient(list(addrs.values()), request_timeout=1.0, overall_timeout=20.0) as client:
        lease = client.acquire("job", ttl_ms=30_000, owner="alice")
        assert lease and lease.token == 1
        assert client.acquire("job", ttl_ms=30_000, owner="bob") is None  # held

        leader = _wait_leader(handles)
        handles[leader].stop(grace=0.2)  # kill the leader
        _wait_leader(handles, exclude=(leader,))

        assert client.acquire("job", ttl_ms=30_000, owner="bob") is None  # state survived failover
        assert client.release(lease)
        bob = client.acquire("job", ttl_ms=30_000, owner="bob")
        assert bob and bob.token == 2
