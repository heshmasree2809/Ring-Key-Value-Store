"""Hybrid logical clock used to version every write.

A timestamp is the tuple ``(wall_ms, counter, node_id)``. Tuples compare
lexicographically, so any two writes have a total order and the newest write
wins (last-write-wins). The counter and ``observe()`` keep timestamps moving
forward even when a node's wall clock is slightly behind another node's.
"""
import threading
import time


class HybridClock:
    def __init__(self, node_id):
        self.node_id = node_id
        self._ms = 0
        self._counter = 0
        self._lock = threading.Lock()

    def now(self):
        """Timestamp for a new local event (always strictly increasing)."""
        wall = int(time.time() * 1000)
        with self._lock:
            if wall > self._ms:
                self._ms, self._counter = wall, 0
            else:
                self._counter += 1
            return (self._ms, self._counter, self.node_id)

    def observe(self, ts):
        """Fold a timestamp seen from another node into the local clock."""
        ms, counter = int(ts[0]), int(ts[1])
        with self._lock:
            if (ms, counter) > (self._ms, self._counter):
                self._ms, self._counter = ms, counter
