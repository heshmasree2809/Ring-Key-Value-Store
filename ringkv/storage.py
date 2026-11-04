"""Durable local storage: an in-memory index backed by an append-only write-ahead log.

Every accepted write is appended to the log (CRC-protected) *before* it is
applied in memory. On startup the log is replayed. A truncated or corrupt tail
(e.g. a crash mid-write) is detected by the CRC, ignored and trimmed.

Records are ``(value, ts, deleted)`` where ``ts`` is a ``(ms, counter, node)``
tuple. A write is applied only if its timestamp is newer than the stored one
(last-write-wins), which also makes every write idempotent.
"""
import json
import logging
import os
import threading
import time
import zlib

log = logging.getLogger("ringkv.storage")


def _fsync_dir(path):
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:  # not supported on every platform
        pass


class Store:
    def __init__(self, data_dir, fsync=True, compact_min_records=10000,
                 tombstone_gc_ms=10 * 24 * 3600 * 1000):
        os.makedirs(data_dir, exist_ok=True)
        self.data_dir = data_dir
        self.path = os.path.join(data_dir, "wal.log")
        self.fsync = fsync
        self.compact_min_records = compact_min_records
        self.tombstone_gc_ms = tombstone_gc_ms
        self._lock = threading.RLock()
        self._data = {}
        self._wal_records = 0
        self.max_ts = (0, 0, "")
        self._replay()
        self._wal = open(self.path, "ab")

    # ---- log format -------------------------------------------------------
    @staticmethod
    def _encode(key, rec):
        payload = json.dumps(
            {"k": key, "v": rec[0], "ts": list(rec[1]), "d": rec[2]},
            separators=(",", ":"), ensure_ascii=True).encode("ascii")
        return b"%08x %s\n" % (zlib.crc32(payload) & 0xFFFFFFFF, payload)

    def _replay(self):
        if not os.path.exists(self.path):
            return
        good = 0
        with open(self.path, "rb") as handle:
            for line in handle:
                if not line.endswith(b"\n"):
                    log.warning("truncated record at end of WAL; ignoring it")
                    break
                try:
                    crc_hex, payload = line[:-1].split(b" ", 1)
                    if int(crc_hex, 16) != (zlib.crc32(payload) & 0xFFFFFFFF):
                        raise ValueError("crc mismatch")
                    obj = json.loads(payload)
                    key, rec = obj["k"], (obj["v"], tuple(obj["ts"]), bool(obj["d"]))
                except (ValueError, KeyError, TypeError):
                    log.warning("corrupt WAL record at offset %d; stopping replay", good)
                    break
                self._apply(key, rec)
                self._wal_records += 1
                good += len(line)
        if good < os.path.getsize(self.path):
            with open(self.path, "r+b") as handle:   # drop the bad tail
                handle.truncate(good)
        log.info("replayed %d WAL records, %d live keys", self._wal_records, len(self._data))

    # ---- core operations --------------------------------------------------
    def _apply(self, key, rec):
        cur = self._data.get(key)
        if cur is not None and cur[1] >= rec[1]:
            return False
        self._data[key] = rec
        if rec[1] > self.max_ts:
            self.max_ts = rec[1]
        return True

    def put(self, key, value, ts, deleted=False):
        """Apply a write. Returns True if applied, False if it was older/duplicate."""
        ts = tuple(ts)
        rec = (value, ts, bool(deleted))
        with self._lock:
            cur = self._data.get(key)
            if cur is not None and cur[1] >= ts:
                return False
            self._wal.write(self._encode(key, rec))
            self._wal.flush()
            if self.fsync:
                os.fsync(self._wal.fileno())
            self._apply(key, rec)
            self._wal_records += 1
            if (self._wal_records >= self.compact_min_records
                    and self._wal_records > 2 * len(self._data)):
                self._compact()
            return True

    def get(self, key):
        """Return ``(value, ts, deleted)`` or None. Tombstones are returned as-is."""
        with self._lock:
            return self._data.get(key)

    def compact(self):
        with self._lock:
            self._compact()

    def _compact(self):
        """Rewrite the log with one record per key, dropping old tombstones."""
        cutoff = int(time.time() * 1000) - self.tombstone_gc_ms
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as handle:
            for key, rec in list(self._data.items()):
                if rec[2] and rec[1][0] < cutoff:
                    del self._data[key]
                    continue
                handle.write(self._encode(key, rec))
            handle.flush()
            os.fsync(handle.fileno())
        self._wal.close()
        os.replace(tmp, self.path)
        _fsync_dir(self.data_dir)
        self._wal = open(self.path, "ab")
        self._wal_records = len(self._data)
        log.info("compacted WAL to %d records", self._wal_records)

    def stats(self):
        with self._lock:
            return {"keys": len(self._data), "wal_records": self._wal_records}

    def close(self):
        with self._lock:
            try:
                self._wal.flush()
                os.fsync(self._wal.fileno())
            except (OSError, ValueError):
                pass
            self._wal.close()
