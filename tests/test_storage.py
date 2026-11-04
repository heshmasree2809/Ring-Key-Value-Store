import os
import tempfile
import time
import unittest

from ringkv.clock import HybridClock
from ringkv.storage import Store


class ClockTests(unittest.TestCase):
    def test_timestamps_strictly_increase(self):
        clock = HybridClock("n1")
        stamps = [clock.now() for _ in range(1000)]
        self.assertEqual(stamps, sorted(stamps))
        self.assertEqual(len(set(stamps)), 1000)

    def test_observe_moves_clock_past_a_remote_timestamp(self):
        clock = HybridClock("n1")
        future = (int(time.time() * 1000) + 60_000, 5, "n2")
        clock.observe(future)
        self.assertGreater(clock.now(), future)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="store-test-")

    def open(self, **kw):
        store = Store(self.dir, fsync=False, **kw)
        self.addCleanup(store.close)
        return store

    def test_put_get_and_last_write_wins(self):
        s = self.open()
        self.assertTrue(s.put("k", "v1", (10, 0, "a")))
        self.assertTrue(s.put("k", "v2", (20, 0, "a")))
        self.assertFalse(s.put("k", "old", (15, 0, "a")))   # older write ignored
        self.assertFalse(s.put("k", "v2", (20, 0, "a")))    # duplicate ignored
        self.assertEqual(s.get("k")[0], "v2")

    def test_tie_on_time_is_broken_by_node_id(self):
        s = self.open()
        s.put("k", "from-a", (10, 0, "a"))
        s.put("k", "from-b", (10, 0, "b"))
        self.assertEqual(s.get("k")[0], "from-b")

    def test_tombstone_hides_older_value_and_blocks_resurrection(self):
        s = self.open()
        s.put("k", "v", (10, 0, "a"))
        s.put("k", None, (20, 0, "a"), deleted=True)
        self.assertTrue(s.get("k")[2])
        s.put("k", "stale", (15, 0, "a"))
        self.assertTrue(s.get("k")[2])

    def test_recovery_after_restart(self):
        s = self.open()
        for i in range(100):
            s.put("k%d" % i, "v%d" % i, (i + 1, 0, "a"))
        s.put("k5", None, (999, 0, "a"), deleted=True)
        s.close()
        s2 = self.open()
        self.assertEqual(s2.get("k7")[0], "v7")
        self.assertTrue(s2.get("k5")[2])
        self.assertEqual(s2.max_ts, (999, 0, "a"))

    def test_truncated_tail_is_ignored_and_trimmed(self):
        s = self.open()
        s.put("a", "1", (1, 0, "n"))
        s.put("b", "2", (2, 0, "n"))
        s.close()
        with open(os.path.join(self.dir, "wal.log"), "ab") as f:
            f.write(b'deadbeef {"k":"c","v":"3","ts":[3,0')   # crash mid-write
        s2 = self.open()
        self.assertEqual(s2.get("b")[0], "2")
        self.assertIsNone(s2.get("c"))
        s2.put("d", "4", (4, 0, "n"))                          # log still appendable
        s2.close()
        self.assertEqual(self.open().get("d")[0], "4")

    def test_corrupt_record_stops_replay(self):
        s = self.open()
        s.put("a", "1", (1, 0, "n"))
        s.put("b", "2", (2, 0, "n"))
        s.close()
        path = os.path.join(self.dir, "wal.log")
        data = open(path, "rb").read().replace(b'"v":"2"', b'"v":"X"')   # CRC now wrong
        open(path, "wb").write(data)
        s2 = self.open()
        self.assertEqual(s2.get("a")[0], "1")
        self.assertIsNone(s2.get("b"))

    def test_compaction_keeps_latest_and_drops_old_tombstones(self):
        s = self.open(tombstone_gc_ms=0)
        for i in range(50):
            s.put("hot", "v%d" % i, (i + 1, 0, "a"))
        s.put("gone", None, (1, 0, "a"), deleted=True)        # ancient tombstone
        s.put("fresh", "x", (int(time.time() * 1000) + 10**9, 0, "a"))
        before = s.stats()["wal_records"]
        s.compact()
        self.assertLess(s.stats()["wal_records"], before)
        self.assertEqual(s.get("hot")[0], "v49")
        self.assertIsNone(s.get("gone"))
        s.close()
        s2 = self.open()
        self.assertEqual(s2.get("hot")[0], "v49")
        self.assertEqual(s2.get("fresh")[0], "x")


if __name__ == "__main__":
    unittest.main()
