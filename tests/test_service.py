import threading
import time

from dlm.cluster import LocalCluster


def test_contended_lock_gives_mutual_exclusion_across_a_leader_failover():
    c = LocalCluster(3, request_timeout=1.0)
    c.start()
    state = {"inside": 0, "max_inside": 0}
    tokens: list[int] = []
    guard = threading.Lock()
    errors: list[Exception] = []
    TTL = 30_000  # long vs. the critical section, so safety here is about consensus, not lease expiry

    def worker(wid):
        owner = f"w{wid}"
        try:
            for _ in range(8):
                while True:
                    r = c.call(lambda s: s.acquire("critical", owner, TTL), timeout=15)
                    if r["ok"]:
                        break
                    time.sleep(0.002)
                with guard:
                    state["inside"] += 1
                    state["max_inside"] = max(state["max_inside"], state["inside"])
                    tokens.append(r["token"])
                time.sleep(0.003)
                with guard:
                    state["inside"] -= 1
                rel = c.call(lambda s, tok=r["token"]: s.release("critical", owner, tok), timeout=15)
                assert rel["code"] in ("OK", "NOT_HOLDER", "EXPIRED")
        except Exception as exc:  # surfaced after join
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    try:
        c.wait_for_leader()
        for t in threads:
            t.start()
        time.sleep(0.15)
        c.network.isolate(c.leader())  # kill the leader mid-run
        for t in threads:
            t.join(timeout=60)
        assert not errors, errors
        assert all(not t.is_alive() for t in threads), "workers did not finish"
        assert state["max_inside"] == 1, "two clients were inside the critical section at once"
        assert len(tokens) == 32
        assert tokens == sorted(tokens) and len(set(tokens)) == len(tokens), "fencing tokens must be strictly increasing"
    finally:
        c.shutdown()


def test_lease_expires_and_stale_holder_is_fenced_out():
    c = LocalCluster(3)
    c.start()
    try:
        c.wait_for_leader()
        a = c.call(lambda s: s.acquire("db", "alice", 100))
        time.sleep(0.25)  # alice "pauses" past her lease
        b = c.call(lambda s: s.acquire("db", "bob", 10_000))
        assert b["ok"] and b["token"] == a["token"] + 1  # a resource can reject token < b.token
        late = c.call(lambda s: s.release("db", "alice", a["token"]))
        assert late["code"] == "NOT_HOLDER"
        renew = c.call(lambda s: s.renew("db", "alice", a["token"], 1000))
        assert renew["code"] == "NOT_HOLDER"
    finally:
        c.shutdown()


def test_invalid_arguments_are_rejected():
    import pytest

    c = LocalCluster(1)
    c.start()
    try:
        leader = c.wait_for_leader()
        svc = c.services[leader]
        with pytest.raises(ValueError):
            svc.acquire("", "alice", 100)
        with pytest.raises(ValueError):
            svc.acquire("r", "alice", 0)
        assert svc.acquire("r", "alice", 100)["ok"]  # single-node cluster commits on its own
    finally:
        c.shutdown()
