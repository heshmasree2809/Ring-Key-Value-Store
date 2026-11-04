# Ring Key-Value Store

A small **Dynamo-style distributed key-value store** written in Python (standard library only, no dependencies).
Keys are partitioned with **consistent hashing**, replicated to **N nodes**, and read/written at a
**tunable consistency level** (`one`, `quorum`, `all`). It keeps working when nodes crash, and repairs
itself afterwards with **hinted handoff** and **read repair**. Every node persists data in a
**CRC-protected write-ahead log** and recovers it after `kill -9`.

```
                      client (any node can coordinate)
                              │  PUT /kv/user:42?consistency=quorum
                              ▼
                       ┌─────────────┐   hash(key) → position on the ring
                       │ coordinator │   preference list = next N distinct nodes
                       └──┬───┬───┬──┘
              parallel RPC│   │   │               ring (virtual nodes)
                 ┌────────┘   │   └────────┐        n1 ─ n3 ─ n2 ─ n5 ─ n4 ─ n1 …
                 ▼            ▼            ▼
            ┌────────┐   ┌────────┐   ┌────────┐   respond as soon as the required
            │replica │   │replica │   │replica │   number of acks (W) / answers (R)
            │ WAL +  │   │ WAL +  │   │ WAL +  │   arrive; replicas that missed a write
            │ index  │   │ index  │   │ index  │   get a hint, stale ones get repaired
            └────────┘   └────────┘   └────────┘
```

## Features

| Area | What is implemented |
|---|---|
| Partitioning | Consistent hash ring (MD5), 128 virtual nodes per node, preference list of N distinct nodes |
| Replication | Replication factor N (default 3); any node can coordinate any request |
| Consistency | Per-request `one` / `quorum` (N//2+1) / `all`; `quorum` reads + `quorum` writes give R + W > N |
| Versioning | Hybrid logical clock timestamps, last-write-wins, idempotent writes |
| Durability | Append-only WAL with per-record CRC32, replay on start, torn-tail repair, compaction |
| Deletes | Tombstones (so deleted data is never resurrected by a stale replica) with grace-period GC |
| Fault tolerance | Failure detector (heartbeats), **hinted handoff** (persisted, TTL, de-duplicated), **read repair** |
| Security | Optional shared-secret token (`X-Auth-Token`) for client and node-to-node traffic |
| Ops | `/health`, `/metrics`, structured logs, graceful SIGTERM shutdown, Docker + Compose, CI |

## Quick start (no Docker)

Requires Python 3.10+.

```bash
python -m ringkv.cluster --nodes 5          # starts 5 nodes on random localhost ports, prints their URLs
# in another terminal (use a URL printed above):
python -m ringkv.cli --node http://127.0.0.1:PORT put city Hyderabad
python -m ringkv.cli --node http://127.0.0.1:OTHER_PORT get city
python -m ringkv.cli --node http://127.0.0.1:PORT lookup city        # which nodes own this key
python -m ringkv.cli --node http://127.0.0.1:PORT metrics
```

Run a single node by hand:

```bash
python -m ringkv.node --id node1 --data-dir ./data/node1 \
  --peers node1=127.0.0.1:8001,node2=127.0.0.1:8002,node3=127.0.0.1:8003
```

## Docker (5-node cluster)

```bash
docker compose up -d --build
curl -X PUT 'localhost:8001/kv/hello?consistency=quorum' -d '{"value":"world"}'
curl 'localhost:8003/kv/hello'                       # read from a different node
docker compose stop node2                            # simulate a crash
curl -X PUT 'localhost:8001/kv/during-outage' -d '{"value":"still works"}'
docker compose start node2                           # hinted handoff delivers the missed write
curl localhost:8001/metrics                          # see hints_stored / hints_delivered
docker compose down -v
```

Set a shared secret with `RINGKV_TOKEN=changeme docker compose up -d`; clients then send `-H 'X-Auth-Token: changeme'`.

## Deploying on AWS EC2 (single-host demo)

1. Launch an Amazon Linux / Ubuntu instance (t3.small is plenty). Security group: allow SSH from your IP and
   ports 8001-8005 **only from your IP**. The API is unauthenticated unless you set `RINGKV_TOKEN`
   and uses plain HTTP, so don't expose it to the whole internet.
2. Install Docker and the compose plugin, clone this repo.
3. `RINGKV_TOKEN=<long random string> docker compose up -d --build`
4. Test from your laptop: `curl -H 'X-Auth-Token: …' http://<public-ip>:8001/kv/hello`.

For a real multi-machine cluster, run one node per host with `--peers` pointing at the hosts' private IPs
(all nodes must be given the same peer list).

## HTTP API

| Request | Meaning |
|---|---|
| `PUT /kv/<key>?consistency=quorum` body `{"value": <any JSON>}` | write; `200` with `acks`, `required`, `replicas`; `503` if not enough replicas acked |
| `GET /kv/<key>?consistency=quorum` | `200 {"value", "ts"}`, `404` if missing/deleted, `503` if not enough replicas answered |
| `DELETE /kv/<key>?consistency=quorum` | writes a tombstone |
| `GET /ring/lookup?key=<key>` | replicas responsible for a key |
| `GET /metrics`, `GET /health` | counters, store stats, pending hints, peer liveness |
| `POST /internal/put`, `GET /internal/get` | node-to-node replication (not for clients) |

Keys containing `/`, `?`, spaces etc. must be URL-encoded.

## Design decisions and trade-offs

* **Strict quorums, no sloppy quorum.** A write succeeds only if `required` *real* replicas acknowledge it.
  Hints are stored for replicas that missed it, but a hint never counts as an ack. This keeps the
  `R + W > N` guarantee simple to reason about at the cost of lower availability than Dynamo's sloppy quorum.
* **Last-write-wins with hybrid logical clocks, not vector clocks.** Concurrent writes to the same key from different
  coordinators are resolved by timestamp, so one of them is silently lost. This is the same choice as Cassandra.
  The HLC prevents timestamps from going backwards when wall clocks drift, but it does not make LWW safe for
  workloads that need conflict detection.
* **A failed quorum write is not rolled back.** If only 1 of 3 replicas acked and the client gets a `503`, that
  replica still holds the new value, and hints will propagate it. Clients should treat a `503` as "outcome unknown"
  and retry; writes are idempotent so retrying is safe.
* **Read path.** The coordinator asks all N replicas in parallel, answers after `R` responses, then repairs stale
  replicas in the background.
* **Tombstone GC** happens only during compaction and only after a grace period (default 10 days). A replica that
  is down longer than that and then returns with an old value could resurrect deleted data; hint TTL (3 h) and
  read repair make this unlikely but it is the same limitation real systems document.
* **WAL `fsync` is per write by default** (`--fsync always`). With `--fsync none` data still survives process
  crashes (`kill -9`) because the OS already has it, but not power loss.

## Known limitations (deliberate scope cuts)

* Static membership: the node list is fixed at start-up. Adding or removing a node needs a rolling restart with a new
  peer list and does **not** move existing data.
* No background anti-entropy (Merkle trees); replicas converge via hints and read repair only, so a key that is never
  read again after a missed write and whose hint expired can stay inconsistent.
* One in-memory index per node: the dataset must fit in RAM.
* Compaction blocks writes on that node while it runs.
* HTTP/JSON is used for simplicity, not speed. Plain HTTP; use a TLS-terminating proxy for anything non-local.

## Tests

```bash
python -m unittest discover -s tests -t . -v        # also works with pytest
```

* `tests/test_ring.py`: even key distribution, minimal key movement when a node is added or removed
* `tests/test_storage.py`: last-write-wins, tombstones, WAL recovery, torn tail, corrupt record, compaction
* `tests/test_cluster.py`: real multi-process cluster with failure injection:
  quorum survives one crash; quorum fails (503) but `one` succeeds with two replicas down;
  hinted handoff after restart; `kill -9` + WAL recovery; read repair; deletes are not resurrected; auth

## Benchmarks

```bash
python bench.py --spawn --ops 20000 --consistency one
python bench.py --spawn --ops 20000 --consistency quorum
python bench.py --spawn --ops 20000 --consistency quorum --kill-at 0.5   # SIGKILL a node halfway through
python bench.py --spawn --ops 20000 --consistency quorum --fsync         # durable writes
```

| Config | ops/sec | p50 (get / put) | p99 (get / put) | Errors |
|---|---|---|---|---|
| quorum, 16 threads | 1354 | 9.5 / 11.6 ms | 27.8 / 32.0 ms | 0 (0.00%) |
| quorum, node killed at 50% (SIGKILL) | 623 | 9.3 / 10.9 ms | 483 / 505 ms | 0 (0.00%) |

## Project layout

```
ringkv/ring.py      consistent-hash ring with virtual nodes
ringkv/clock.py     hybrid logical clock
ringkv/storage.py   WAL + in-memory index, recovery, compaction
ringkv/node.py      HTTP server, coordinator, replication, hinted handoff, read repair, failure detector
ringkv/cluster.py   spawn/kill local clusters (tests, benchmarks, demos)
ringkv/cli.py       command-line client
bench.py            load generator with a chaos (kill a node) mode
tests/              unit + multi-process fault-injection tests
Dockerfile, docker-compose.yml, .github/workflows/ci.yml
```
