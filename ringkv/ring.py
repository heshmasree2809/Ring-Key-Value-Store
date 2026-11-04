"""Consistent-hash ring with virtual nodes."""
import bisect
import hashlib


def _hash(value):
    return int.from_bytes(hashlib.md5(value.encode("utf-8")).digest()[:8], "big")


class HashRing:
    """Maps keys to an ordered preference list of distinct physical nodes.

    Each physical node owns ``vnodes`` points on the ring, which smooths the
    key distribution and means adding or removing a node only moves the keys
    adjacent to that node's points.
    """

    def __init__(self, nodes=(), vnodes=128):
        self.vnodes = vnodes
        self._points = []   # sorted hash positions
        self._owner = {}    # hash position -> node id
        self._nodes = set()
        for node in nodes:
            self.add_node(node)

    @property
    def nodes(self):
        return sorted(self._nodes)

    def add_node(self, node):
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self.vnodes):
            point = _hash("%s#%d" % (node, i))
            if point in self._owner:
                continue
            self._owner[point] = node
            bisect.insort(self._points, point)

    def remove_node(self, node):
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        self._owner = {p: o for p, o in self._owner.items() if o != node}
        self._points = sorted(self._owner)

    def preference_list(self, key, n):
        """First ``n`` distinct nodes found walking clockwise from the key."""
        if not self._points:
            return []
        n = min(n, len(self._nodes))
        start = bisect.bisect(self._points, _hash(key))
        result = []
        for step in range(len(self._points)):
            owner = self._owner[self._points[(start + step) % len(self._points)]]
            if owner not in result:
                result.append(owner)
                if len(result) == n:
                    break
        return result
