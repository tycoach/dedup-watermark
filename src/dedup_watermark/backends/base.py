from abc import ABC, abstractmethod
from datetime import datetime


class DedupBackend(ABC):
    """
    Contract every dedup backend must satisfy.

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
