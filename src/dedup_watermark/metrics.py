from collections import Counter


class RunMetrics:
    """
    Aggregates decisions across a full run (batch or streaming session) for
    benchmarking and reporting — separate from ProcessorMetrics, which is a
    live running total kept on the processor itself. Use this when you want
    to compare multiple runs (e.g. exact vs. bloom backend) side by side.
    """

    def __init__(self):
        self.counts: Counter = Counter()

    def record(self, decision) -> None:
        self.counts[decision.value] += 1

    def summary(self) -> dict:
        total = sum(self.counts.values())
        return {
            "total": total,
            **dict(self.counts),
            "duplicate_rate": self.counts.get("DUPLICATE", 0) / total if total else 0.0,
            "late_rate": (
                (self.counts.get("LATE_DROPPED", 0) + self.counts.get("LATE_ACCEPTED", 0)) / total
                if total else 0.0
            ),
        }
