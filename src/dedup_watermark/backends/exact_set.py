from collections import defaultdict
from datetime import datetime, timedelta

from .base import DedupBackend


def _bucket_key(dt: datetime, bucket_width: timedelta) -> int:
    """Map a datetime to an integer bucket id (epoch seconds // bucket width)."""
    epoch_seconds = dt.timestamp()
    width_seconds = bucket_width.total_seconds()
    return int(epoch_seconds // width_seconds)


class ExactSetBackend(DedupBackend):
    """
    Exact dedup using time-bucketed hash sets.

    """

    def __init__(self, bucket_width: timedelta = timedelta(hours=1)):
        self.bucket_width = bucket_width
        self._buckets: dict[int, set[str]] = defaultdict(set)

    def seen(self, key: str) -> bool:
        return any(key in bucket for bucket in self._buckets.values())

    def add(self, key: str, event_time: datetime) -> None:
        bucket_id = _bucket_key(event_time, self.bucket_width)
        self._buckets[bucket_id].add(key)

    def evict_before(self, watermark: datetime) -> None:
        cutoff_bucket = _bucket_key(watermark, self.bucket_width)
        stale = [b for b in self._buckets if b < cutoff_bucket]
        for b in stale:
            del self._buckets[b]

    def approximate_size(self) -> int:
        return sum(len(bucket) for bucket in self._buckets.values())
