from abc import ABC, abstractmethod
from datetime import datetime


class DedupBackend(ABC):
    """
    Contract every dedup backend must satisfy.

    A backend only ever answers "have I seen this key" and "remember this key" —
    it knows nothing about watermarks, decisions, or record shape. That split is
    what lets DedupWatermarkProcessor swap an exact hash-set for a Bloom filter
    (or a Redis-backed set later) without touching the watermark logic at all.

    Backends are expected to bucket keys by event time internally so that
    evict_before() can drop state for time windows that have fallen behind the
    watermark, keeping memory bounded regardless of stream volume.
    """

    @abstractmethod
    def seen(self, key: str) -> bool:
        """Return True if this key has already been recorded."""
        raise NotImplementedError

    @abstractmethod
    def add(self, key: str, event_time: datetime) -> None:
        """Record this key as seen, bucketed under its event time."""
        raise NotImplementedError

    @abstractmethod
    def evict_before(self, watermark: datetime) -> None:
        """Drop internal state for time buckets entirely behind the watermark."""
        raise NotImplementedError

    @abstractmethod
    def approximate_size(self) -> int:
        """Return a rough count of keys currently held, for metrics/benchmarking."""
        raise NotImplementedError
