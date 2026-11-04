"""Run a throw-away multi-node cluster on localhost (used by tests, bench and demos).

    python -m ringkv.cluster --nodes 5
"""
import argparse
import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent.parent


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class LocalCluster:
    def __init__(self, base_dir, nodes=5, replication=3, fsync=False, heartbeat=0.3,
                 timeout=1.0, token=None):
        self.base_dir = Path(base_dir)
        self.replication = replication
        self.fsync = fsync
        self.heartbeat = heartbeat
        self.timeout = timeout
        self.token = token
        self.ids = ["node%d" % (i + 1) for i in range(nodes)]
        self.ports = {nid: _free_port() for nid in self.ids}
        self.procs = {}
        self._logs = {}

    # ----- process control --------------------------------------------------
    @property
    def peers_arg(self):
        return ",".join("%s=127.0.0.1:%d" % (n, self.ports[n]) for n in self.ids)

    def start(self, node_id):
        if node_id in self.procs and self.procs[node_id].poll() is None:
            return
        self.base_dir.mkdir(parents=True, exist_ok=True)
        old = self._logs.pop(node_id, None)
        if old:
            old.close()
        log = open(self.base_dir / ("%s.log" % node_id), "ab")
        self._logs[node_id] = log
        cmd = [sys.executable, "-m", "ringkv.node", "--id", node_id,
               "--peers", self.peers_arg, "--bind", "127.0.0.1",
               "--data-dir", str(self.base_dir / node_id),
               "--replication", str(self.replication),
               "--heartbeat", str(self.heartbeat), "--timeout", str(self.timeout),
               "--fsync", "always" if self.fsync else "none"]
        if self.token:
            cmd += ["--token", self.token]
        env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONUNBUFFERED="1")
        self.procs[node_id] = subprocess.Popen(cmd, cwd=str(ROOT), env=env,
                                               stdout=log, stderr=subprocess.STDOUT)

    def start_all(self):
        for nid in self.ids:
            self.start(nid)
        for nid in self.ids:
            self.wait_healthy(nid)

    def is_running(self, node_id):
        proc = self.procs.get(node_id)
        return proc is not None and proc.poll() is None

    def kill(self, node_id):
        """Simulate a crash (SIGKILL: no shutdown hooks run)."""
        proc = self.procs.get(node_id)
        if proc and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)

    def stop(self, node_id):
        """Graceful shutdown (SIGTERM)."""
        proc = self.procs.get(node_id)
        if proc and proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()

    def wait_healthy(self, node_id, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self.request(node_id, "GET", "/health", auth=False)[0] == 200:
                    return
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("%s did not become healthy; see %s" %
                           (node_id, self.base_dir / ("%s.log" % node_id)))

    def shutdown(self):
        for nid in self.ids:
            self.stop(nid)
        for log in self._logs.values():
            log.close()

    # ----- client helpers ------------------------------------------------------
    def url(self, node_id):
        return "http://127.0.0.1:%d" % self.ports[node_id]

    def request(self, node_id, method, path, body=None, auth=True):
        conn = http.client.HTTPConnection("127.0.0.1", self.ports[node_id], timeout=10)
        try:
            headers = {"Content-Type": "application/json"}
            if self.token and auth:
                headers["X-Auth-Token"] = self.token
            data = json.dumps(body).encode() if body is not None else None
            conn.request(method, path, body=data, headers=headers)
            resp = conn.getresponse()
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
        finally:
            conn.close()

    def put(self, node, key, value, consistency="quorum"):
        return self.request(node, "PUT", "/kv/%s?consistency=%s" % (quote(key, safe=""), consistency),
                            {"value": value})

    def get(self, node, key, consistency="quorum"):
        return self.request(node, "GET", "/kv/%s?consistency=%s" % (quote(key, safe=""), consistency))

    def delete(self, node, key, consistency="quorum"):
        return self.request(node, "DELETE", "/kv/%s?consistency=%s" % (quote(key, safe=""), consistency))

    def internal_get(self, node, key):
        return self.request(node, "GET", "/internal/get?key=" + quote(key, safe=""))[1]

    def replicas(self, key):
        return self.request(self.ids[0], "GET", "/ring/lookup?key=" + quote(key, safe=""))[1]["replicas"]


def main():
    ap = argparse.ArgumentParser(description="Start a local ringkv cluster")
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--replication", type=int, default=3)
    ap.add_argument("--fsync", action="store_true", help="fsync every write")
    ap.add_argument("--dir", default=None, help="data directory (default: temp dir)")
    args = ap.parse_args()

    base = args.dir or tempfile.mkdtemp(prefix="ringkv-")
    cluster = LocalCluster(base, nodes=args.nodes, replication=args.replication, fsync=args.fsync)
    cluster.start_all()
    print("cluster up. data/logs in", base)
    for nid in cluster.ids:
        print("  %s  %s" % (nid, cluster.url(nid)))
    print("try:  python -m ringkv.cli --node %s put hello world" % cluster.url(cluster.ids[0]))
    print("Ctrl-C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
