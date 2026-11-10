#!/usr/bin/env python3
"""Load generator for ringkv.

Against a running cluster:
    python bench.py --nodes http://127.0.0.1:8001,http://127.0.0.1:8002 --ops 20000

Self-contained (spawns a local 5-node cluster, optionally kills a node mid-run):
    python bench.py --spawn --ops 10000 --consistency quorum
    python bench.py --spawn --ops 10000 --consistency quorum --kill-at 0.5
"""
import argparse
import http.client
import json
import random
import statistics
import tempfile
import threading
import time
from urllib.parse import quote, urlsplit


def percentile(sorted_values, p):
    if not sorted_values:
        return float("nan")
    idx = min(len(sorted_values) - 1, int(round(p / 100 * (len(sorted_values) - 1))))
    return sorted_values[idx]


def worker(urls, ops, keys, read_ratio, consistency, value, token, results, progress, seed):
    rng = random.Random(seed)
    parts = [urlsplit(u) for u in urls]
    conns = [http.client.HTTPConnection(p.hostname, p.port, timeout=10) for p in parts]
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Auth-Token"] = token
    lat = {"get": [], "put": []}
    errors = 0
    for _ in range(ops):
        i = rng.randrange(len(conns))
        key = quote("key-%d" % rng.randrange(keys), safe="")
        is_read = rng.random() < read_ratio
        path = "/kv/%s?consistency=%s" % (key, consistency)
        started = time.perf_counter()
        try:
            if is_read:
                conns[i].request("GET", path, headers=headers)
            else:
                conns[i].request("PUT", path, body=json.dumps({"value": value}), headers=headers)
            resp = conns[i].getresponse()
            resp.read()
            ok = resp.status in (200, 404)
        except OSError:
            ok = False
            conns[i].close()
            conns[i] = http.client.HTTPConnection(parts[i].hostname, parts[i].port, timeout=10)
        elapsed = (time.perf_counter() - started) * 1000
        if ok:
            lat["get" if is_read else "put"].append(elapsed)
        else:
            errors += 1
        progress.append(1)
    results.append((lat, errors))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", help="comma-separated node URLs to send traffic to")
    ap.add_argument("--spawn", action="store_true", help="start a local 5-node cluster")
    ap.add_argument("--ops", type=int, default=10000, help="total operations")
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--keys", type=int, default=2000)
    ap.add_argument("--read-ratio", type=float, default=0.5)
    ap.add_argument("--consistency", default="quorum", choices=["one", "quorum", "all"])
    ap.add_argument("--value-size", type=int, default=128)
    ap.add_argument("--token", default=None)
    ap.add_argument("--fsync", action="store_true", help="(spawn mode) fsync every write")
    ap.add_argument("--kill-at", type=float, default=None,
                    help="(spawn mode) SIGKILL one node when this fraction of ops is done")
    args = ap.parse_args()

    cluster = None
    victim = None
    urls = (args.nodes or "").split(",") if args.nodes else []
    if args.spawn:
        from ringkv.cluster import LocalCluster
        cluster = LocalCluster(tempfile.mkdtemp(prefix="ringkv-bench-"), nodes=5, fsync=args.fsync)
        cluster.start_all()
        victim = cluster.ids[-1]
        # When killing a node, clients only talk to the survivors.
        targets = [n for n in cluster.ids if not (args.kill_at is not None and n == victim)]
        urls = [cluster.url(n) for n in targets]
    if not urls:
        ap.error("give --nodes or --spawn")

    value = "x" * args.value_size
    results, progress = [], []
    per_thread = args.ops // args.threads
    total = per_thread * args.threads
    threads = [threading.Thread(target=worker, args=(urls, per_thread, args.keys, args.read_ratio,
                                                      args.consistency, value, args.token,
                                                      results, progress, t))
               for t in range(args.threads)]
    started = time.perf_counter()
    for t in threads:
        t.start()
    killed_at = None
    while any(t.is_alive() for t in threads):
        if cluster and args.kill_at is not None and killed_at is None \
                and len(progress) >= args.kill_at * total:
            cluster.kill(victim)
            killed_at = time.perf_counter() - started
            print("  >> killed %s (SIGKILL) at %.1fs, %d ops done" % (victim, killed_at, len(progress)))
        time.sleep(0.05)
    elapsed = time.perf_counter() - started

    merged = {"get": [], "put": []}
    errors = 0
    for lat, err in results:
        merged["get"] += lat["get"]
        merged["put"] += lat["put"]
        errors = err
    done = len(merged["get"]) + len(merged["put"])
    print("\nconsistency=%s threads=%d ops=%d value=%dB%s" % (
        args.consistency, args.threads, total, args.value_size,
        " fsync" if args.fsync else ""))
    print("throughput: %.0f ops/sec   errors: %d (%.2f%%)" % (done / elapsed, errors, 100 * errors / total))
    print("%-5s %8s %8s %8s %8s %8s" % ("op", "count", "mean", "p50", "p95", "p99"))
    for op in ("get", "put"):
        vals = sorted(merged[op])
        if vals:
            print("%-5s %8d %7.2fms %7.2fms %7.2fms %7.2fms" % (
                op, len(vals), statistics.mean(vals), percentile(vals, 50),
                percentile(vals, 95), percentile(vals, 99)))
    if cluster:
        cluster.shutdown()


if __name__ == "__main__":
    main()
