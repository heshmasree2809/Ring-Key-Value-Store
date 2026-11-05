""""Integration tests:real node processes on localhost, with real crashes.""""
import tempfile
import time
import unittest

from ringkv.cluster import LocalCluster


def wait_for(predicate, timeout=10.0, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class ClusterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cluster = LocalCluster(tempfile.mkdtemp(prefix="ringkv-test-"), nodes=5)
        cls.cluster.start_all()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.shutdown()

    def tearDown(self):
        # Bring any node a test killed back so tests stay independent.
        for nid in self.cluster.ids:
            if not self.cluster.is_running(nid):
                self.cluster.start(nid)
                self.cluster.wait_healthy(nid)
        time.sleep(0.8)   # let failure detectors notice

    # ----- basic behaviour ------------------------------------------------
    def test_write_via_one_node_read_via_another(self):
        c = self.cluster
        status, _ = c.put("node1", "basic", {"a": [1, 2, 3]})
        self.assertEqual(status, 200)
        for nid in c.ids:
            status, body = c.get(nid, "basic")
            self.assertEqual(status, 200)
            self.assertEqual(body["value"], {"a": [1, 2, 3]})

    def test_overwrite_returns_latest(self):
        c = self.cluster
        for i in range(10):
            c.put(c.ids[i % 5], "ow", "v%d" % i)
        self.assertEqual(c.get("node2", "ow")[1]["value"], "v9")

    def test_delete_and_missing_keys(self):
        c = self.cluster
        c.put("node1", "del-me", "x")
        self.assertEqual(c.delete("node3", "del-me")[0], 200)
        self.assertEqual(c.get("node5", "del-me")[0], 404)
        self.assertEqual(c.get("node2", "never-written")[0], 404)

    def test_each_key_is_stored_on_exactly_n_nodes(self):
        c = self.cluster
        c.put("node1", "replicated", "v")
        self.assertTrue(wait_for(lambda: sum(
            c.internal_get(n, "replicated")["found"] for n in c.ids) == 3))
        holders = {n for n in c.ids if c.internal_get(n, "replicated")["found"]}
        self.assertEqual(holders, set(c.replicas("replicated")))

    def test_special_characters_in_keys(self):
        c = self.cluster
        key = "users/42?name=a b&x=%"
        self.assertEqual(c.put("node1", key, "ok")[0], 200)
        self.assertEqual(c.get("node4", key)[1]["value"], "ok")

    def test_bad_requests(self):
        c = self.cluster
        self.assertEqual(c.request("node1", "PUT", "/kv/x", {"nope": 1})[0], 400)
        self.assertEqual(c.request("node1", "GET", "/kv/x?consistency=bogus")[0], 400)
        self.assertEqual(c.request("node1", "GET", "/nothing")[0], 404)

    # ----- fault tolerance --------------------------------------------------
    def test_quorum_survives_one_replica_crash(self):
        c = self.cluster
        key = "one-down"
        reps = c.replicas(key)
        coordinator, victim = reps[0], reps[2]
        c.kill(victim)
        status, body = c.put(coordinator, key, "v")
        self.assertEqual(status, 200, body)
        self.assertEqual(c.get(coordinator, key)[1]["value"], "v")

    def test_quorum_fails_but_one_succeeds_when_two_replicas_are_down(self):
        c = self.cluster
        key = "two-down"
        reps = c.replicas(key)
        coordinator = reps[0]
        c.kill(reps[1])
        c.kill(reps[2])
        status, body = c.put(coordinator, key, "v")
        self.assertEqual(status, 503)
        self.assertEqual((body["acks"], body["required"]), (1, 2))
        self.assertEqual(c.put(coordinator, key + "-b", "v", consistency="one")[0], 200)
        self.assertEqual(c.get(coordinator, key + "-b", consistency="quorum")[0], 503)

    def test_hinted_handoff_delivers_missed_write_after_restart(self):
        c = self.cluster
        key = "hinted"
        reps = c.replicas(key)
        coordinator, victim = reps[0], reps[1]
        c.kill(victim)
        self.assertEqual(c.put(coordinator, key, "while-down")[0], 200)
        self.assertFalse(c.internal_get(coordinator, "missing-sanity")["found"])
        c.start(victim)
        c.wait_healthy(victim)
        self.assertTrue(wait_for(lambda: c.internal_get(victim, key)["found"], timeout=15),
                        "hint was never delivered")
        self.assertEqual(c.internal_get(victim, key)["value"], "while-down")
        counters = c.request(coordinator, "GET", "/metrics")[1]["counters"]
        self.assertGreaterEqual(counters.get("hints_delivered", 0), 1)

    def test_data_survives_kill_9_via_write_ahead_log(self):
        c = self.cluster
        key = "durable"
        reps = c.replicas(key)
        c.put(reps[0], key, "persisted")
        self.assertTrue(wait_for(lambda: all(c.internal_get(r, key)["found"] for r in reps)))
        victim = reps[1]
        c.kill(victim)                       # SIGKILL: no shutdown code runs
        c.start(victim)
        c.wait_healthy(victim)
        rec = c.internal_get(victim, key)    # read the replica's own store directly
        self.assertTrue(rec["found"])
        self.assertEqual(rec["value"], "persisted")

    def test_read_repair_fixes_a_stale_replica(self):
        c = self.cluster
        key = "repair-me"
        reps = c.replicas(key)
        stale = reps[2]
        rec = {"key": key, "value": "fresh", "ts": [int(time.time() * 1000), 0, "test"], "del": False}
        # Write directly to two replicas only (bypassing the coordinator).
        for r in reps[:2]:
            self.assertEqual(c.request(r, "POST", "/internal/put", rec)[0], 200)
        self.assertFalse(c.internal_get(stale, key)["found"])
        status, body = c.get(reps[0], key)
        self.assertEqual((status, body["value"]), (200, "fresh"))
        self.assertTrue(wait_for(lambda: c.internal_get(stale, key)["found"]),
                        "stale replica was not repaired")
        self.assertEqual(c.internal_get(stale, key)["value"], "fresh")

    def test_delete_propagates_via_read_repair_and_is_not_resurrected(self):
        c = self.cluster
        key = "tombstone"
        reps = c.replicas(key)
        c.put(reps[0], key, "alive")
        self.assertTrue(wait_for(lambda: all(c.internal_get(r, key)["found"] for r in reps)))
        c.kill(reps[2])
        self.assertEqual(c.delete(reps[0], key)[0], 200)
        c.start(reps[2])
        c.wait_healthy(reps[2])
        for r in c.ids:   # old value on the restarted replica must never win
            self.assertEqual(c.get(r, key)[0], 404)


class AuthTests(unittest.TestCase):
    def test_token_is_enforced_but_health_is_open(self):
        cluster = LocalCluster(tempfile.mkdtemp(prefix="ringkv-auth-"), nodes=3, token="s3cret")
        cluster.start_all()
        try:
            self.assertEqual(cluster.put("node1", "k", "v")[0], 200)          # sends token
            self.assertEqual(cluster.get("node2", "k")[1]["value"], "v")      # internal RPC authed too
            self.assertEqual(cluster.request("node1", "GET", "/health", auth=False)[0], 200)
            self.assertEqual(cluster.request("node1", "GET", "/kv/k", auth=False)[0], 401)
        finally:
            cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
