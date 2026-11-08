"""Watch Raft in action without Docker or gRPC:  python examples/local_demo.py"""

import time

from dlm.cluster import LocalCluster


def main() -> None:
    cluster = LocalCluster(3)
    cluster.start()
    try:
        leader = cluster.wait_for_leader()
        print(f"leader elected: {leader} (term {cluster.nodes[leader].status()['term']})")

        r = cluster.call(lambda s: s.acquire("payments-job", "worker-A", 30_000))
        print(f"worker-A acquire -> ok={r['ok']} fencing_token={r['token']}")
        r = cluster.call(lambda s: s.acquire("payments-job", "worker-B", 30_000))
        print(f"worker-B acquire -> ok={r['ok']} code={r['code']} held_by={r['holder']}")

        print(f"\ncrashing leader {leader} ...")
        cluster.stop(leader)
        new_leader = cluster.wait_for_leader()
        print(f"new leader: {new_leader} (term {cluster.nodes[new_leader].status()['term']})")

        r = cluster.call(lambda s: s.acquire("payments-job", "worker-B", 30_000))
        print(f"worker-B acquire after failover -> ok={r['ok']} code={r['code']} (lock state survived)")

        print("\nshort lease + a paused client (fencing token demo):")
        a = cluster.call(lambda s: s.acquire("db", "slow-client", 100))
        time.sleep(0.25)  # slow-client stalls past its lease
        b = cluster.call(lambda s: s.acquire("db", "fast-client", 30_000))
        print(f"slow-client held token {a['token']}; fast-client now holds token {b['token']}")
        print(f"a storage layer that remembers the highest token seen rejects slow-client's late write ({a['token']} < {b['token']})")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
