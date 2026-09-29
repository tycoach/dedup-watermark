import hashlib
import math
from datetime import datetime, timedelta

from .base import DedupBackend


class _BloomBitset:
    """A single Bloom filter: a bit array plus k independent hash functions."""

    def __init__(self, expected_items: int, false_positive_rate: float):
        # Standard Bloom filter sizing formulas.
        self.size = max(8, int(-(expected_items * math.log(false_positive_rate)) / (math.log(2) ** 2)))
        self.num_hashes = max(1, round((self.size / expected_items) * math.log(2))) if expected_items else 1
        self.bits = bytearray((self.size + 7) // 8)

    def _positions(self, key: str):
        # Double hashing (Kirsch-Mitzenmacher): derive k hash positions from two
        # independent hashes instead of computing k separate hash functions.
        h1 = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        h2 = int(hashlib.md5(key.encode()).hexdigest(), 16)
        for i in range(self.num_hashes):
            yield (h1 + i * h2) % self.size

    def add(self, key: str) -> None:
        for pos in self._positions(key):
            self.bits[pos // 8] |= 1 << (pos % 8)

    def __contains__(self, key: str) -> bool:
        return all(self.bits[pos // 8] & (1 << (pos % 8)) for pos in self._positions(key))


def _bucket_key(dt: datetime, bucket_width: timedelta) -> int:
    return int(dt.timestamp() // bucket_width.total_seconds())


class BloomFilterBackend(DedupBackend):
    """
    Approximate dedup using a time-bucketed Bloom filter.
    """

    def __init__(
        self,
        bucket_width: timedelta = timedelta(hours=1),
        expected_items_per_bucket: int = 1_000_000,
        false_positive_rate: float = 0.001,
    ):
        self.bucket_width = bucket_width
        self.expected_items_per_bucket = expected_items_per_bucket
        self.false_positive_rate = false_positive_rate
        self._buckets: dict[int, _BloomBitset] = {}

    def _get_or_create_bucket(self, bucket_id: int) -> _BloomBitset:
        if bucket_id not in self._buckets:
            self._buckets[bucket_id] = _BloomBitset(
                self.expected_items_per_bucket, self.false_positive_rate
            )
        return self._buckets[bucket_id]

    def seen(self, key: str) -> bool:
        return any(key in bucket for bucket in self._buckets.values())

    def add(self, key: str, event_time: datetime) -> None:
        bucket_id = _bucket_key(event_time, self.bucket_width)
        self._get_or_create_bucket(bucket_id).add(key)

    def evict_before(self, watermark: datetime) -> None:
        cutoff_bucket = _bucket_key(watermark, self.bucket_width)
        stale = [b for b in self._buckets if b < cutoff_bucket]
        for b in stale:
            del self._buckets[b]

    def approximate_size(self) -> int:
        # Bloom filters don't track exact membership count; report bit-array
        # bytes in use instead, which is the actual memory cost that matters.
        return sum(len(bucket.bits) for bucket in self._buckets.values())
