from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

from .backends import DedupBackend, create_backend
from .decisions import RecordDecision


@dataclass
class ProcessorMetrics:
    accepted: int = 0
    duplicates: int = 0
    late_dropped: int = 0
    late_accepted: int = 0

    def as_dict(self) -> dict:
        return {
            "accepted": self.accepted,
            "duplicates": self.duplicates,
            "late_dropped": self.late_dropped,
            "late_accepted": self.late_accepted,
            "total": self.accepted + self.duplicates + self.late_dropped + self.late_accepted,
        }


class DedupWatermarkProcessor:
    """
    Schema-agnostic streaming deduplication + late-arrival watermarking.
    """

    def __init__(
        self,
        key_fn: Callable[[Any], str],
        event_time_fn: Callable[[Any], datetime],
        watermark_delay: timedelta,
        dedup_backend: str = "exact",
        late_policy: str = "drop",
        backend_kwargs: Optional[dict] = None,
    ):
        if late_policy not in ("drop", "accept_flagged"):
            raise ValueError("late_policy must be 'drop' or 'accept_flagged'")

        self.key_fn = key_fn
        self.event_time_fn = event_time_fn
        self.watermark_delay = watermark_delay
        self.late_policy = late_policy

        self._backend: DedupBackend = create_backend(dedup_backend, **(backend_kwargs or {}))
        self._max_event_time: Optional[datetime] = None
        self.metrics = ProcessorMetrics()

    @property
    def current_watermark(self) -> Optional[datetime]:
        if self._max_event_time is None:
            return None
        return self._max_event_time - self.watermark_delay

    def process(self, record: Any) -> RecordDecision:
        key = self.key_fn(record)
        event_time = self.event_time_fn(record)

        # Advance the high-water mark first, so the watermark reflects the
        # newest event time seen so far, then evict backend state that has
        # fallen behind it — this is what keeps memory bounded over a long
        # running stream.
        if self._max_event_time is None or event_time > self._max_event_time:
            self._max_event_time = event_time
            self._backend.evict_before(self.current_watermark)

        watermark = self.current_watermark

        if event_time < watermark:
            if self.late_policy == "drop":
                self.metrics.late_dropped += 1
                return RecordDecision.LATE_DROPPED
            # accept_flagged: still check dedup below, but let a late record through
            decision_if_new = RecordDecision.LATE_ACCEPTED
        else:
            decision_if_new = RecordDecision.ACCEPT

        if self._backend.seen(key):
            self.metrics.duplicates += 1
            return RecordDecision.DUPLICATE

        self._backend.add(key, event_time)
        if decision_if_new is RecordDecision.LATE_ACCEPTED:
            self.metrics.late_accepted += 1
        else:
            self.metrics.accepted += 1
        return decision_if_new