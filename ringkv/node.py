"""A ringkv node: HTTP API + request coordinator + replica, in one process.

Any node can coordinate any request. The coordinator hashes the key onto the
ring, picks the N replicas (preference list), fans the operation out in
parallel and answers once the requested consistency level is satisfied:

    ONE     1 replica must answer
    QUORUM  N // 2 + 1 replicas (R + W > N  =>  reads see the latest quorum write)
    ALL     every replica

Failure handling:
    * hinted handoff - writes a replica missed are stored by the coordinator
      and replayed when that replica is reachable again
    * read repair    - after a read, replicas holding stale data are updated
    * WAL recovery   - each replica replays its write-ahead log on restart
"""
import argparse
import http.client
import json
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout, as_completed
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, unquote, urlsplit

from .clock import HybridClock
from .ring import HashRing
from .storage import Store

log = logging.getLogger("ringkv.node")

MAX_BODY = 1024 * 1024        # 1 MiB per request
MAX_KEY_LEN = 512
MAX_HINTS_PER_TARGET = 100000


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._counts = {}

    def inc(self, name, amount=1):
        with self._lock:
            self._counts[name] = self._counts.get(name, 0) + amount

    def snapshot(self):
        with self._lock:
            return dict(self._counts)


class PeerClient:
    """HTTP client with a per-thread keep-alive connection to each peer."""

    def __init__(self, peers, timeout, token=None):
        self.peers = peers
        self.timeout = timeout
        self.token = token
        self._local = threading.local()

    def _conn(self, peer):
        conns = getattr(self._local, "conns", None)
        if conns is None:
            conns = self._local.conns = {}
        conn = conns.get(peer)
        if conn is None:
            host, port = self.peers[peer]
            conn = conns[peer] = http.client.HTTPConnection(host, port, timeout=self.timeout)
            conn.used = False
        return conn

    def _drop(self, peer):
        conn = getattr(self._local, "conns", {}).pop(peer, None)
        if conn is not None:
            conn.close()

    def request(self, peer, method, path, body=None):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["X-Auth-Token"] = self.token
        data = json.dumps(body).encode("utf-8") if body is not None else None
        for attempt in (0, 1):
            conn = self._conn(peer)
            was_used = conn.used
            try:
                conn.request(method, path, body=data, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                conn.used = True
                return resp.status, (json.loads(raw) if raw else None)
            except (OSError, http.client.HTTPException):
                self._drop(peer)
                # A reused keep-alive connection may simply have gone stale: retry once.
                if attempt == 0 and was_used:
                    continue
                raise

    def ping(self, peer, timeout):
        host, port = self.peers[peer]
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        try:
            conn.request("GET", "/health")
            return conn.getresponse().status == 200
        except (OSError, http.client.HTTPException):
            return False
        finally:
            conn.close()


class HintStore:
    """Writes that a replica missed, kept (and persisted) by the coordinator.

    Only the newest hint per (target, key) is kept, so memory stays bounded by
    the number of distinct keys written while a node was down.
    """

    def __init__(self, path, ttl_s):
        self.path = path
        self.ttl_s = ttl_s
        self._lock = threading.Lock()
        self._hints = {}   # target -> {key: (rec_dict, added_at)}
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    obj = json.loads(line)
                    self._put(obj["t"], obj["rec"], obj["at"])
                except (ValueError, KeyError, TypeError):
                    continue

    def _put(self, target, rec, added_at):
        bucket = self._hints.setdefault(target, {})
        cur = bucket.get(rec["key"])
        if cur is None or tuple(cur[0]["ts"]) < tuple(rec["ts"]):
            if cur is None and len(bucket) >= MAX_HINTS_PER_TARGET:
                return False
            bucket[rec["key"]] = (rec, added_at)
        return True

    def add(self, target, rec):
        now = time.time()
        with self._lock:
            if not self._put(target, rec, now):
                return False
            with open(self.path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps({"t": target, "rec": rec, "at": now}) + "\n")
            return True

    def targets(self):
        with self._lock:
            return [t for t, bucket in self._hints.items() if bucket]

    def snapshot(self, target):
        with self._lock:
            return [(k, rec, at) for k, (rec, at) in self._hints.get(target, {}).items()]

    def discard(self, target, key, rec):
        """Forget a hint, unless a newer one has replaced it meanwhile."""
        with self._lock:
            cur = self._hints.get(target, {}).get(key)
            if cur is not None and tuple(cur[0]["ts"]) <= tuple(rec["ts"]):
                del self._hints[target][key]

    def expired(self, added_at):
        return time.time() - added_at > self.ttl_s

    def persist(self):
        """Rewrite the hint file from memory (atomic)."""
        with self._lock:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                for target, bucket in self._hints.items():
                    for rec, at in bucket.values():
                        handle.write(json.dumps({"t": target, "rec": rec, "at": at}) + "\n")
            os.replace(tmp, self.path)

    def pending(self):
        with self._lock:
            return sum(len(b) for b in self._hints.values())


# --------------------------------------------------------------------------- #
# The node
# --------------------------------------------------------------------------- #
class Node:
    def __init__(self, node_id, peers, data_dir, replication=3, vnodes=128,
                 timeout=1.0, heartbeat=1.0, fsync=True, token=None,
                 hint_ttl_s=3 * 3600):
        if node_id not in peers:
            raise ValueError("node id %r is not in the peer list" % node_id)
        self.id = node_id
        self.peers = peers
        self.n = replication
        self.timeout = timeout
        self.heartbeat = heartbeat
        self.token = token

        self.ring = HashRing(peers.keys(), vnodes=vnodes)
        self.clock = HybridClock(node_id)
        self.store = Store(os.path.join(data_dir, "store"), fsync=fsync)
        self.clock.observe(self.store.max_ts)
        self.hints = HintStore(os.path.join(data_dir, "hints.log"), hint_ttl_s)
        self.rpc = PeerClient(peers, timeout, token)
        self.metrics = Metrics()
        self.alive = {p: False for p in peers if p != node_id}

        self.pool = ThreadPoolExecutor(max_workers=64, thread_name_prefix="rpc")
        self.maint = ThreadPoolExecutor(max_workers=8, thread_name_prefix="maint")
        self._stop = threading.Event()
        self.server = None

    # ----- consistency levels ----------------------------------------------
    @staticmethod
    def required_acks(level, replicas):
        if level == "one":
            return 1
        if level == "quorum":
            return replicas // 2 + 1
        if level == "all":
            return replicas
        raise ValueError("consistency must be one, quorum or all")

    # ----- local replica operations ----------------------------------------
    def apply_local(self, rec):
        ts = tuple(rec["ts"])
        self.clock.observe(ts)
        return self.store.put(rec["key"], rec["value"], ts, rec.get("del", False))

    def read_local(self, key):
        return self.store.get(key)

    # ----- coordinator: write ----------------------------------------------
    def _write_replica(self, replica, rec):
        try:
            if replica == self.id:
                self.apply_local(rec)
                return True
            status, _ = self.rpc.request(replica, "POST", "/internal/put", rec)
            return status == 200
        except Exception:   # any failure means "this replica did not ack"
            return False

    def _after_write(self, replica, rec, future):
        if not future.result():
            if self.hints.add(replica, rec):
                self.metrics.inc("hints_stored")
            else:
                self.metrics.inc("hints_dropped")

    def coordinate_write(self, key, value, deleted, level):
        replicas = self.ring.preference_list(key, self.n)
        required = self.required_acks(level, len(replicas))
        ts = self.clock.now()
        rec = {"key": key, "value": value, "ts": list(ts), "del": deleted}

        futures = {self.pool.submit(self._write_replica, r, rec): r for r in replicas}
        for fut, replica in futures.items():
            fut.add_done_callback(partial(self._after_write, replica, rec))

        acks = failures = 0
        try:
            for fut in as_completed(futures, timeout=self.timeout + 0.5):
                if fut.result():
                    acks += 1
                else:
                    failures += 1
                if acks >= required or len(replicas) - failures < required:
                    break
        except FutureTimeout:
            pass

        self.metrics.inc("deletes" if deleted else "puts")
        info = {"key": key, "ts": list(ts), "acks": acks, "required": required,
                "replicas": replicas}
        if acks >= required:
            return 200, info
        self.metrics.inc("write_quorum_failures")
        info["error"] = "not enough replicas acknowledged the write"
        return 503, info

    # ----- coordinator: read -----------------------------------------------
    def _read_replica(self, replica, key):
        """Returns (ok, record_or_None)."""
        try:
            if replica == self.id:
                return True, self.read_local(key)
            status, obj = self.rpc.request(
                replica, "GET", "/internal/get?key=" + quote(key, safe=""))
            if status != 200:
                return False, None
            if not obj["found"]:
                return True, None
            return True, (obj["value"], tuple(obj["ts"]), bool(obj["del"]))
        except Exception:
            return False, None

    def coordinate_read(self, key, level):
        replicas = self.ring.preference_list(key, self.n)
        required = self.required_acks(level, len(replicas))
        futures = {self.pool.submit(self._read_replica, r, key): r for r in replicas}

        responses = {}   # replica -> record (or None if it has nothing)
        failures = 0
        try:
            for fut in as_completed(futures, timeout=self.timeout + 0.5):
                ok, rec = fut.result()
                if ok:
                    responses[futures[fut]] = rec
                else:
                    failures += 1
                if len(responses) >= required or len(replicas) - failures < required:
                    break
        except FutureTimeout:
            pass

        self.metrics.inc("gets")
        if len(responses) < required:
            self.metrics.inc("read_quorum_failures")
            return 503, {"error": "not enough replicas responded", "key": key,
                         "responses": len(responses), "required": required,
                         "replicas": replicas}

        self.maint.submit(self._read_repair, key, futures, dict(responses))

        found = [r for r in responses.values() if r is not None]
        best = max(found, key=lambda r: r[1]) if found else None
        if best is None or best[2]:   # nothing stored, or newest version is a tombstone
            return 404, {"error": "not found", "key": key}
        return 200, {"key": key, "value": best[0], "ts": list(best[1])}

    def _read_repair(self, key, futures, responses):
        """Collect the remaining replica answers, then fix any stale replica."""
        try:
            for fut, replica in futures.items():
                if replica in responses:
                    continue
                try:
                    ok, rec = fut.result(timeout=self.timeout + 0.5)
                except Exception:
                    continue
                if ok:
                    responses[replica] = rec
            found = [r for r in responses.values() if r is not None]
            if not found:
                return
            best = max(found, key=lambda r: r[1])
            payload = {"key": key, "value": best[0], "ts": list(best[1]), "del": best[2]}
            for replica, rec in responses.items():
                if rec is None or rec[1] < best[1]:
                    if self._write_replica(replica, payload):
                        self.metrics.inc("read_repairs")
        except Exception:
            log.exception("read repair failed for key %r", key)

    # ----- background: failure detector + hint replay ----------------------
    def _heartbeat_loop(self):
        while not self._stop.is_set():
            for peer in list(self.alive):
                self.alive[peer] = self.rpc.ping(peer, min(0.5, self.timeout))
            self._stop.wait(self.heartbeat)

    def _hint_loop(self):
        while not self._stop.is_set():
            for target in self.hints.targets():
                if self.alive.get(target):
                    self._deliver_hints(target)
            self._stop.wait(self.heartbeat)

    def _deliver_hints(self, target):
        changed = False
        for key, rec, added_at in self.hints.snapshot(target):
            if self.hints.expired(added_at):
                self.hints.discard(target, key, rec)
                self.metrics.inc("hints_expired")
                changed = True
                continue
            try:
                status, _ = self.rpc.request(target, "POST", "/internal/put", rec)
            except Exception:
                break
            if status != 200:
                break
            self.hints.discard(target, key, rec)
            self.metrics.inc("hints_delivered")
            changed = True
        if changed:
            self.hints.persist()

    # ----- request routing ---------------------------------------------------
    def handle(self, method, path, query, body):
        """Returns (status, json_object)."""
        if path.startswith("/kv/"):
            key = unquote(path[len("/kv/"):])
            if not key or len(key) > MAX_KEY_LEN:
                raise ValueError("key must be 1-%d characters" % MAX_KEY_LEN)
            level = query.get("consistency", "quorum").lower()
            if method == "GET":
                return self.coordinate_read(key, level)
            if method == "PUT":
                if not isinstance(body, dict) or "value" not in body:
                    raise ValueError('body must be JSON like {"value": ...}')
                return self.coordinate_write(key, body["value"], False, level)
            if method == "DELETE":
                return self.coordinate_write(key, None, True, level)
        elif path == "/internal/put" and method == "POST":
            if not isinstance(body, dict) or "key" not in body or "ts" not in body:
                raise ValueError("malformed replica write")
            return 200, {"applied": self.apply_local(body)}
        elif path == "/internal/get" and method == "GET":
            rec = self.read_local(query.get("key", ""))
            if rec is None:
                return 200, {"found": False}
            return 200, {"found": True, "value": rec[0], "ts": list(rec[1]), "del": rec[2]}
        elif path == "/ring/lookup" and method == "GET":
            key = query.get("key", "")
            return 200, {"key": key, "replicas": self.ring.preference_list(key, self.n)}
        elif path == "/metrics" and method == "GET":
            return 200, {"node": self.id, "counters": self.metrics.snapshot(),
                         "store": self.store.stats(), "hints_pending": self.hints.pending(),
                         "alive": dict(self.alive), "ring_nodes": self.ring.nodes,
                         "replication": self.n}
        return 404, {"error": "unknown route"}

    # ----- lifecycle -----------------------------------------------------------
    def serve(self, bind_host="0.0.0.0"):
        host, port = self.peers[self.id]

        class Server(ThreadingHTTPServer):
            daemon_threads = True
            request_queue_size = 128

        self.server = Server((bind_host, port), _make_handler(self))
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._hint_loop, daemon=True).start()

        def shutdown(*_):
            threading.Thread(target=self.server.shutdown, daemon=True).start()

        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)
        log.info("node %s listening on %s:%d (N=%d, ring=%s)",
                 self.id, bind_host, port, self.n, self.ring.nodes)
        try:
            self.server.serve_forever()
        finally:
            self._stop.set()
            self.server.server_close()
            self.store.close()
            log.info("node %s stopped", self.id)


def _make_handler(node):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"      # keep-alive between nodes
        server_version = "ringkv/1.0"

        def log_message(self, fmt, *args):
            log.debug("%s - %s", self.address_string(), fmt % args)

        def _send(self, status, obj):
            body = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _dispatch(self, method):
            try:
                length = int(self.headers.get("Content-Length") or 0)
                if length > MAX_BODY:
                    self.close_connection = True
                    return self._send(413, {"error": "request body too large"})
                raw = self.rfile.read(length) if length else b""

                url = urlsplit(self.path)
                if url.path == "/health":
                    return self._send(200, {"id": node.id, "status": "ok"})
                if node.token and self.headers.get("X-Auth-Token") != node.token:
                    return self._send(401, {"error": "missing or invalid X-Auth-Token"})

                try:
                    body = json.loads(raw) if raw else None
                except ValueError:
                    raise ValueError("request body is not valid JSON")
                query = {k: v[0] for k, v in parse_qs(url.query).items()}
                status, obj = node.handle(method, url.path, query, body)
                self._send(status, obj)
            except ValueError as exc:
                self._send(400, {"error": str(exc)})
            except Exception:
                log.exception("unhandled error")
                self._send(500, {"error": "internal error"})

        def do_GET(self):
            self._dispatch("GET")

        def do_PUT(self):
            self._dispatch("PUT")

        def do_POST(self):
            self._dispatch("POST")

        def do_DELETE(self):
            self._dispatch("DELETE")

    return Handler


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_peers(text):
    peers = {}
    for item in text.split(","):
        name, addr = item.split("=", 1)
        host, port = addr.rsplit(":", 1)
        peers[name.strip()] = (host.strip(), int(port))
    return peers


def main(argv=None):
    env = os.environ.get
    ap = argparse.ArgumentParser(description="Run a ringkv node")
    ap.add_argument("--id", default=env("RINGKV_ID"), required=env("RINGKV_ID") is None)
    ap.add_argument("--peers", default=env("RINGKV_PEERS"), required=env("RINGKV_PEERS") is None,
                    help="all nodes incl. this one: id=host:port,id=host:port,...")
    ap.add_argument("--data-dir", default=env("RINGKV_DATA_DIR", "./data"))
    ap.add_argument("--bind", default=env("RINGKV_BIND", "0.0.0.0"))
    ap.add_argument("--replication", type=int, default=int(env("RINGKV_REPLICATION", 3)))
    ap.add_argument("--vnodes", type=int, default=int(env("RINGKV_VNODES", 128)))
    ap.add_argument("--timeout", type=float, default=float(env("RINGKV_TIMEOUT", 1.0)),
                    help="per-replica RPC timeout in seconds")
    ap.add_argument("--heartbeat", type=float, default=float(env("RINGKV_HEARTBEAT", 1.0)))
    ap.add_argument("--fsync", choices=["always", "none"], default=env("RINGKV_FSYNC", "always"))
    ap.add_argument("--token", default=env("RINGKV_TOKEN") or None,
                    help="shared secret required in the X-Auth-Token header")
    ap.add_argument("--log-level", default=env("RINGKV_LOG_LEVEL", "INFO"))
    args = ap.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper(), stream=sys.stdout,
                        format="%(asctime)s %(levelname)s [" + args.id + "] %(name)s: %(message)s")
    node = Node(args.id, parse_peers(args.peers), args.data_dir,
                replication=args.replication, vnodes=args.vnodes, timeout=args.timeout,
                heartbeat=args.heartbeat, fsync=(args.fsync == "always"), token=args.token)
    node.serve(args.bind)


if __name__ == "__main__":
    main()
