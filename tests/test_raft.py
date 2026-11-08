import random
import threading
import time

import pytest

from dlm.cluster import LocalCluster, wait_for
from dlm.raft import FileStorage, NotLeaderError, ProposalDropped


def acquire(owner, resource="r", ttl_ms=60_000):
    return {"op": "acquire", "ts": int(time.time() * 1000), "resource": resource, "owner": owner, "ttl_ms": ttl_ms}


def converged(cluster, ids=None):
    ids = ids or cluster.running_ids()
    snaps = [cluster.fsms[i].snapshot() for i in ids]
    applied = [len(cluster.applied[i]) for i in ids]
    return all(s == snaps[0] for s in snaps) and len(set(applied)) == 1


@pytest.fixture
def cluster():
    c = LocalCluster(3)
    c.start()
    yield c
    c.shutdown()


def test_elects_exactly_one_leader_and_stays_stable(cluster):
    leader = cluster.wait_for_leader()
    term = cluster.nodes[leader].status()["term"]
    time.sleep(0.5)  # many election timeouts: heartbeats must suppress new elections
    assert cluster.leader() == leader
    assert cluster.nodes[leader].status()["term"] == term
    roles = [cluster.nodes[i].status()["role"] for i in cluster.ids]
    assert roles.count("leader") == 1


def test_command_replicates_and_applies_on_every_node(cluster):
    leader = cluster.wait_for_leader()
    result = cluster.nodes[leader].propose(acquire("alice")).result(timeout=2)
    assert result["ok"] and result["token"] == 1
    assert wait_for(lambda: all(cluster.fsms[i].snapshot().get("r", (None,))[0] == "alice" for i in cluster.ids))


def test_follower_rejects_proposals_with_leader_hint(cluster):
    leader = cluster.wait_for_leader()
    follower = next(i for i in cluster.ids if i != leader)
    assert wait_for(lambda: cluster.nodes[follower].status()["leader_id"] == leader)
    with pytest.raises(NotLeaderError) as exc:
        cluster.nodes[follower].propose(acquire("x"))
    assert exc.value.leader_id == leader


def test_committed_state_survives_leader_failover(cluster):
    leader = cluster.wait_for_leader()
    cluster.nodes[leader].propose(acquire("alice")).result(timeout=2)
    cluster.stop(leader)
    new_leader = cluster.wait_for_leader(among=[i for i in cluster.ids if i != leader])
    assert new_leader != leader
    r = cluster.nodes[new_leader].propose(acquire("bob")).result(timeout=2)
    assert not r["ok"] and r["code"] == "HELD" and r["holder"] == "alice"


def test_minority_leader_cannot_commit_and_its_entry_is_dropped_after_heal(cluster):
    old = cluster.wait_for_leader()
    cluster.network.isolate(old)
    doomed = cluster.nodes[old].propose(acquire("ghost"))  # accepted locally, can never reach a quorum
    majority = [i for i in cluster.ids if i != old]
    new = cluster.wait_for_leader(among=majority)
    cluster.nodes[new].propose(acquire("real")).result(timeout=2)
    assert not doomed.done()
    cluster.network.heal()
    with pytest.raises(ProposalDropped):
        doomed.result(timeout=3)
    assert wait_for(lambda: converged(cluster))
    for i in cluster.ids:
        assert cluster.fsms[i].snapshot()["r"][0] == "real"
        assert all(c["owner"] != "ghost" for c in cluster.applied[i])


def test_isolated_leader_steps_down_via_check_quorum(cluster):
    old = cluster.wait_for_leader()
    cluster.network.isolate(old)
    assert wait_for(lambda: cluster.nodes[old].status()["role"] != "leader", timeout=2)


def test_restarted_follower_catches_up(cluster):
    leader = cluster.wait_for_leader()
    follower = next(i for i in cluster.ids if i != leader)
    cluster.stop(follower)
    for n in range(5):
        cluster.nodes[leader].propose(acquire(f"o{n}", resource=f"res{n}")).result(timeout=2)
    cluster.restart(follower)
    assert wait_for(lambda: converged(cluster), timeout=5)
    assert len(cluster.fsms[follower].snapshot()) == 5


def test_five_node_cluster_tolerates_two_failures():
    c = LocalCluster(5)
    c.start()
    try:
        leader = c.wait_for_leader()
        victims = [i for i in c.ids if i != leader][:2]
        for v in victims:
            c.stop(v)
        assert c.nodes[leader].propose(acquire("alice")).result(timeout=3)["ok"]
    finally:
        c.shutdown()


def test_full_cluster_restart_recovers_from_disk(tmp_path):
    c = LocalCluster(3, storage_factory=lambda nid: FileStorage(str(tmp_path / nid), fsync=False))
    c.start()
    try:
        leader = c.wait_for_leader()
        assert c.nodes[leader].propose(acquire("alice")).result(timeout=2)["token"] == 1
        for nid in c.ids:
            c.stop(nid)
        for nid in c.ids:
            c._build(nid)  # brand-new nodes + state machines, same WAL directories
            c.nodes[nid].start()
        leader = c.wait_for_leader()
        r = c.nodes[leader].propose(acquire("bob")).result(timeout=3)
        assert not r["ok"] and r["holder"] == "alice"  # committed state came back from the log
    finally:
        c.shutdown()


def test_chaos_safety_invariants():
    """Random partitions/crashes under concurrent load; invariants must always hold."""
    rng = random.Random(7)
    c = LocalCluster(5, seed=3, request_timeout=0.5)
    c.start()
    stop = threading.Event()
    results = []

    def client(tid):
        n = 0
        while not stop.is_set():
            n += 1
            try:
                c.call(lambda s, n=n: s.acquire(f"res{n % 3}", f"t{tid}-{n}", 20), timeout=2)
                results.append(1)
            except TimeoutError:
                pass

    threads = [threading.Thread(target=client, args=(t,), daemon=True) for t in range(3)]
    try:
        c.wait_for_leader()
        for t in threads:
            t.start()
        end = time.monotonic() + 3.0
        while time.monotonic() < end:
            action = rng.choice(["isolate", "partition", "crash", "heal"])
            victim = rng.choice(c.ids)
            if action == "isolate":
                c.network.isolate(victim)
            elif action == "partition":
                others = [i for i in c.ids if i != victim]
                c.network.partition([victim], others)
            elif action == "crash" and len(c.running_ids()) > 3:
                c.stop(victim)
            else:
                c.network.heal()
                for nid in c.ids:
                    if not c.nodes[nid].is_running:
                        c.restart(nid)
            time.sleep(rng.uniform(0.05, 0.25))
        stop.set()
        for t in threads:
            t.join(timeout=3)
        c.network.heal()
        for nid in c.ids:
            if not c.nodes[nid].is_running:
                c.restart(nid)
        c.wait_for_leader()
        assert wait_for(lambda: converged(c), timeout=10), "replicas did not converge"

        # Election safety: at most one leader per term, ever.
        for term, leaders in c.leaders_by_term().items():
            assert len(leaders) == 1, f"two leaders in term {term}: {leaders}"
        # State machine safety: every replica applied the identical command sequence.
        seqs = [c.applied[i] for i in c.ids]
        assert all(s == seqs[0] for s in seqs)
        # Liveness sanity: the cluster made real progress despite the faults.
        assert len(results) > 0
    finally:
        stop.set()
        c.shutdown()
