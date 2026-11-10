import unittest

from ringkv.ring import HashRing

class RingTests(unittest.TestCase):
    def test_preference_list_is_distinct_and_deterministic(self):
        ring = HashRing(["a", "b", "c", "d", "e"])
        for i in range(500):
            reps = ring.preference_list("key-%d" % i, 3)
            self.assertEqual(len(reps), 3)
            self.assertEqual(len(set(reps)), 3)
            self.assertEqual(reps, ring.preference_list("key-%d" % i, 3))

    def test_n_larger_than_cluster_is_capped(self):
        ring = HashRing(["a", "b"])
        self.assertEqual(len(ring.preference_list("k", 5)), 2)

    def test_empty_ring(self):
        self.assertEqual(HashRing().preference_list("k", 3), [])

    def test_keys_are_spread_evenly(self):
        ring = HashRing(["a", "b", "c", "d", "e"], vnodes=128)
        counts = {}
        total = 20000
        for i in range(total):
            owner = ring.preference_list("k%d" % i, 1)[0]
            counts[owner] = counts.get(owner, 0) + 1
        for node, count in counts.items():
            share = count / total
            self.assertTrue(0.12 < share < 0.30, "%s owns %.2f of keys" % (node, share))

    def test_adding_a_node_moves_only_a_fraction_and_only_to_the_new_node(self):
        keys = ["k%d" % i for i in range(10000)]
        ring = HashRing(["a", "b", "c", "d", "e"])
        before = {k: ring.preference_list(k, 1)[0] for k in keys}
        ring.add_node("f")
        moved = [k for k in keys if ring.preference_list(k, 1)[0] != before[k]]
        self.assertLess(len(moved) / len(keys), 0.30)   # ideal is 1/6
        self.assertTrue(all(ring.preference_list(k, 1)[0] == "f" for k in moved))

    def test_removing_a_node_reassigns_its_keys_only(self):
        keys = ["k%d" % i for i in range(5000)]
        ring = HashRing(["a", "b", "c", "d"])
        before = {k: ring.preference_list(k, 1)[0] for k in keys}
        ring.remove_node("d")
        
        for k in keys:
            if before[k] != "d":
                self.assertEqual(ring.preference_list(k, 1)[0], before[k])
            else:
                self.assertNotEqual(ring.preference_list(k, 1)[0], "d")


if __name__ == "__main__":
    unittest.main()
