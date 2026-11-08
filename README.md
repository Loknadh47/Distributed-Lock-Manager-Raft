<<<<<<< HEAD
# Distributed Lock Manager on a from-scratch Raft

[![CI](https://github.com/Loknadh47/Distributed-Lock-Manager-Raft/actions/workflows/ci.yml/badge.svg)](https://github.com/Loknadh47/Distributed-Lock-Manager-Raft/actions)

A fault-tolerant distributed lock service written in pure Python. Locks are
replicated across a cluster with a **Raft consensus implementation written from
scratch** (no Raft libraries), served over **gRPC**, and deployable as a 3-node
cluster with **Docker Compose**.

- **Mutual exclusion that survives node failures** — a lock granted by the cluster stays granted across leader crashes and partitions.
- **Leases + fencing tokens** — holders can't block the system forever, and a stale holder (e.g. paused by a GC stall) is detectably fenced out.
- **Durable** — Raft's persistent state lives in a write-ahead log with `fsync` and torn-write recovery.
- **Testable by design** — the Raft core is transport-agnostic; tests run whole clusters in-process on a simulated network with partitions, crashes and message loss.

```
=======
Distributed Lock Manager on a from-scratch Raft

CI

A fault-tolerant distributed lock service written in pure Python. Locks are replicated across a cluster with a Raft consensus implementation written from scratch (no Raft libraries), served over gRPC, and deployable as a 3-node cluster with Docker Compose.

Mutual exclusion that survives node failures — a lock granted by the cluster stays granted across leader crashes and partitions.
Leases + fencing tokens — holders can't block the system forever, and a stale holder (e.g. paused by a GC stall) is detectably fenced out.
Durable — Raft's persistent state lives in a write-ahead log with fsync and torn-write recovery.
Testable by design — the Raft core is transport-agnostic; tests run whole clusters in-process on a simulated network with partitions, crashes and message loss.
>>>>>>> 3e63bee3445f188c11e61dedc628d26b5752bed8
                     ┌───────────────────────── cluster ─────────────────────────┐
  client ──gRPC──►   │  node n1 (leader) ◄──AppendEntries/RequestVote──► n2, n3   │
  (retry, leader     │   ├─ LockService   propose(cmd) ─► Raft log ─► commit      │
   discovery,        │   ├─ RaftNode      election · replication · commit index   │
   auto-renew)       │   ├─ WAL           term, vote, log entries (fsync)         │
                     │   └─ LockStateMachine   deterministic apply (leases/tokens)│
                     └────────────────────────────────────────────────────────────┘
<<<<<<< HEAD
```

## Quick start

### Docker (3-node cluster)

```bash
=======
Quick start
Docker (3-node cluster)
bash
>>>>>>> 3e63bee3445f188c11e61dedc628d26b5752bed8
docker compose up --build -d

# take a lock (the CLI discovers the leader and retries automatically)
docker compose --profile tools run --rm cli acquire my-lock --owner alice --ttl-ms 30000
docker compose --profile tools run --rm cli acquire my-lock --owner bob      # -> HELD by 'alice'
docker compose --profile tools run --rm cli status  my-lock

# kill the leader and watch the cluster recover with the lock state intact
docker compose logs | grep "became leader"
docker compose stop n1            # (or whichever node is leader)
docker compose --profile tools run --rm cli status my-lock
<<<<<<< HEAD
```

### Local development

```bash
=======
Local development
bash
>>>>>>> 3e63bee3445f188c11e61dedc628d26b5752bed8
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt pytest ruff
make proto      # generates dlm/v1/dlm_pb2*.py
make test
make demo       # in-process cluster: election, failover, fencing — no gRPC needed
<<<<<<< HEAD
```

### Windows (cmd), no `make` needed

```bat
=======
Windows (cmd), no make needed
bat
>>>>>>> 3e63bee3445f188c11e61dedc628d26b5752bed8
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt pytest
pip install -e .
python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dlm/v1/dlm.proto
python -m pytest
python examples\local_demo.py
<<<<<<< HEAD
```

Run a 3-node cluster without Docker (one terminal each, from the repo root):

```bat
python -m dlm.server --id n1 --listen 127.0.0.1:50051 --peers n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053 --data-dir data
python -m dlm.server --id n2 --listen 127.0.0.1:50052 --peers n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053 --data-dir data
python -m dlm.server --id n3 --listen 127.0.0.1:50053 --peers n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053 --data-dir data
```

### Python client

```python
=======

Run a 3-node cluster without Docker (one terminal each, from the repo root):

bat
python -m dlm.server --id n1 --listen 127.0.0.1:50051 --peers n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053 --data-dir data
python -m dlm.server --id n2 --listen 127.0.0.1:50052 --peers n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053 --data-dir data
python -m dlm.server --id n3 --listen 127.0.0.1:50053 --peers n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053 --data-dir data
Python client
python
>>>>>>> 3e63bee3445f188c11e61dedc628d26b5752bed8
from dlm.client import DLMClient

with DLMClient(["localhost:50051", "localhost:50052", "localhost:50053"]) as client:
    with client.lock("nightly-report", ttl_ms=10_000) as lease:   # lease auto-renews in background
        write_to_storage(data, fencing_token=lease.token)           # downstream rejects stale tokens
<<<<<<< HEAD
```

## How it works

### Raft (`dlm/raft/`)
Leader election with randomized timeouts, log replication with fast conflict
backtracking, commit-by-majority restricted to current-term entries (Raft §5.4.2),
a no-op entry on election so earlier-term entries commit, durable
`(currentTerm, votedFor, log)` persisted **before** replying to any RPC,
and check-quorum (a leader that loses its majority steps down so clients get a fast redirect instead of hanging).
Proposals return futures that resolve when the entry is *applied*, or fail with
`ProposalDropped` if a newer leader overwrites them.

### Lock semantics (`dlm/lock_fsm.py`)
| op | behavior |
|---|---|
| `acquire` | grants if free/expired and bumps a per-resource **fencing token** (never decreases); same-owner re-acquire is idempotent |
| `renew` | extends a live lease; an expired lease **cannot** be revived |
| `release` | frees the lock only if `(owner, token)` match the live holder |

**Deterministic time.** Replicas must compute identical state, so the state machine never reads a clock.
The leader stamps each command with a timestamp; the FSM's "now" is the running maximum of those stamps (monotonic across leader changes).

**Why fencing tokens.** A lease alone can't stop a client that was paused past its expiry from writing after another client took over.
Passing `lease.token` to the protected resource lets it reject any write with a token lower than the highest it has seen.

### Guarantees and assumptions
- **Safety (mutual exclusion in the replicated state)** comes from consensus and holds regardless of clock behavior or network delays.
- **Lease expiry timing** assumes bounded clock drift between the leader and clients. Fencing tokens protect resources if that assumption is violated.
- Commands that time out have an **unknown outcome** (they may still commit); the API is designed so retries are safe (idempotent acquire; token-checked release/renew).
- Reads: `Status` is a local read and may be stale on followers. All state changes are linearized through the Raft log.

## Testing

```bash
python -m pytest
```

| Area | What is verified |
|---|---|
| Election | exactly one leader, stable under heartbeats; follower redirects with leader hint |
| Replication | commands apply on every replica; restarted follower catches up |
| Failover | committed lock state survives leader crash; 5-node cluster tolerates 2 failures |
| Partitions | a minority-side leader cannot commit; its uncommitted entry is dropped after healing; isolated leader steps down |
| Durability | full-cluster restart recovers from the WAL; torn tail discarded; mid-file corruption detected |
| Chaos | random partitions/crashes under concurrent load; asserts **one leader per term**, **identical applied command sequences on all replicas**, and convergence |
| Service | 4 contending clients + leader killed mid-run: never two in the critical section, fencing tokens strictly increasing |
| gRPC e2e | lock lifecycle with leader failover over real sockets (runs once gRPC stubs are generated) |

## Benchmarks

With a cluster running (Docker or local processes):

```bash
python bench/benchmark.py throughput --clients 8 --seconds 15   # ops/sec and p50/p95/p99 latency
python bench/benchmark.py failover --seconds 40                 # then stop the leader; reports the longest unavailability gap
```

Every operation is a replicated, `fsync`'d Raft commit, so results depend on hardware, Docker networking and `fsync` speed.

## Project layout

```
=======
How it works
Raft (dlm/raft/)

Leader election with randomized timeouts, log replication with fast conflict backtracking, commit-by-majority restricted to current-term entries (Raft §5.4.2), a no-op entry on election so earlier-term entries commit, durable (currentTerm, votedFor, log) persisted before replying to any RPC, and check-quorum (a leader that loses its majority steps down so clients get a fast redirect instead of hanging). Proposals return futures that resolve when the entry is applied, or fail with ProposalDropped if a newer leader overwrites them.

Lock semantics (dlm/lock_fsm.py)
op	behavior
acquire	grants if free/expired and bumps a per-resource fencing token (never decreases); same-owner re-acquire is idempotent
renew	extends a live lease; an expired lease cannot be revived
release	frees the lock only if (owner, token) match the live holder

Deterministic time. Replicas must compute identical state, so the state machine never reads a clock. The leader stamps each command with a timestamp; the FSM's "now" is the running maximum of those stamps (monotonic across leader changes).

Why fencing tokens. A lease alone can't stop a client that was paused past its expiry from writing after another client took over. Passing lease.token to the protected resource lets it reject any write with a token lower than the highest it has seen.

Guarantees and assumptions
Safety (mutual exclusion in the replicated state) comes from consensus and holds regardless of clock behavior or network delays.
Lease expiry timing assumes bounded clock drift between the leader and clients. Fencing tokens protect resources if that assumption is violated.
Commands that time out have an unknown outcome (they may still commit); the API is designed so retries are safe (idempotent acquire; token-checked release/renew).
Reads: Status is a local read and may be stale on followers. All state changes are linearized through the Raft log.
Testing
bash
python -m pytest
Area	What is verified
Election	exactly one leader, stable under heartbeats; follower redirects with leader hint
Replication	commands apply on every replica; restarted follower catches up
Failover	committed lock state survives leader crash; 5-node cluster tolerates 2 failures
Partitions	a minority-side leader cannot commit; its uncommitted entry is dropped after healing; isolated leader steps down
Durability	full-cluster restart recovers from the WAL; torn tail discarded; mid-file corruption detected
Chaos	random partitions/crashes under concurrent load; asserts one leader per term, identical applied command sequences on all replicas, and convergence
Service	4 contending clients + leader killed mid-run: never two in the critical section, fencing tokens strictly increasing
gRPC e2e	lock lifecycle with leader failover over real sockets (runs once gRPC stubs are generated)
Benchmarks

With a cluster running (Docker or local processes):

bash
python bench/benchmark.py throughput --clients 8 --seconds 15   # ops/sec and p50/p95/p99 latency
python bench/benchmark.py failover --seconds 40                 # then stop the leader; reports the longest unavailability gap

Every operation is a replicated, fsync'd Raft commit, so results depend on hardware, Docker networking and fsync speed.

Project layout
>>>>>>> 3e63bee3445f188c11e61dedc628d26b5752bed8
dlm/raft/        node.py (consensus) · storage.py (WAL) · transport.py (+ in-memory fault-injecting network)
dlm/lock_fsm.py  replicated state machine          dlm/service.py   request → Raft proposal
dlm/server.py    gRPC server (Raft + Lock API)     dlm/client.py    leader discovery, retries, auto-renew
dlm/cluster.py   in-process cluster for tests      proto/dlm/v1/    wire protocol
<<<<<<< HEAD
```

## Limitations / roadmap
Built to demonstrate the algorithm, not as a production lock service.
- No log compaction/snapshots (WAL and in-memory log grow without bound)
- No dynamic membership changes (cluster is static)
- No pre-vote; reads aren't linearizable via ReadIndex
- No TLS/authentication on the gRPC ports
- No fair wait-queue (clients poll with jittered backoff)

## License
MIT
=======
Limitations / roadmap

Built to demonstrate the algorithm, not as a production lock service.

No log compaction/snapshots (WAL and in-memory log grow without bound)
No dynamic membership changes (cluster is static)
No pre-vote; reads aren't linearizable via ReadIndex
No TLS/authentication on the gRPC ports
No fair wait-queue (clients poll with jittered backoff)
>>>>>>> 3e63bee3445f188c11e61dedc628d26b5752bed8
