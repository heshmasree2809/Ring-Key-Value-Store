"""Tiny command-line client.

    python -m ringkv.cli --node http://127.0.0.1:8001 put city Hyderabad
    python -m ringkv.cli --node http://127.0.0.1:8001 get city --consistency one
"""
import argparse
import http.client
import json
import os
import sys
from urllib.parse import quote, urlsplit


def call(node_url, method, path, body=None, token=None):
    url = urlsplit(node_url)
    conn = http.client.HTTPConnection(url.hostname, url.port or 80, timeout=10)
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Auth-Token"] = token
    try:
        conn.request(method, path, body=json.dumps(body) if body is not None else None,
                     headers=headers)
        resp = conn.getresponse()
        raw = resp.read()
        return resp.status, (json.loads(raw) if raw else None)
    finally:
        conn.close()


def main(argv=None):
    ap = argparse.ArgumentParser(description="ringkv client")
    ap.add_argument("--node", default=os.environ.get("RINGKV_NODE", "http://127.0.0.1:8001"))
    ap.add_argument("--token", default=os.environ.get("RINGKV_TOKEN"))
    ap.add_argument("--consistency", choices=["one", "quorum", "all"], default="quorum")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("put"); p.add_argument("key"); p.add_argument("value")
    g = sub.add_parser("get"); g.add_argument("key")
    d = sub.add_parser("delete"); d.add_argument("key")
    l = sub.add_parser("lookup"); l.add_argument("key")
    sub.add_parser("metrics")
    a = ap.parse_args(argv)

    q = "consistency=" + a.consistency
    if a.cmd == "put":
        status, obj = call(a.node, "PUT", "/kv/%s?%s" % (quote(a.key, safe=""), q), {"value": a.value}, a.token)
    elif a.cmd == "get":
        status, obj = call(a.node, "GET", "/kv/%s?%s" % (quote(a.key, safe=""), q), token=a.token)
    elif a.cmd == "delete":
        status, obj = call(a.node, "DELETE", "/kv/%s?%s" % (quote(a.key, safe=""), q), token=a.token)
    elif a.cmd == "lookup":
        status, obj = call(a.node, "GET", "/ring/lookup?key=" + quote(a.key, safe=""), token=a.token)
    else:
        status, obj = call(a.node, "GET", "/metrics", token=a.token)
    print(status, json.dumps(obj, indent=2))
    return 0 if status < 400 else 1


if __name__ == "__main__":
    sys.exit(main())
