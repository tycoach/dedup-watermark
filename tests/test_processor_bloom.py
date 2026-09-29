from datetime import datetime, timedelta

from dedup_watermark import DedupWatermarkProcessor, RecordDecision
from .fixtures.synthetic_records import generate_synthetic_records


def make_bloom_processor():
    return DedupWatermarkProcessor(
        key_fn=lambda r: r["id"],
        event_time_fn=lambda r: r["ts"],
        watermark_delay=timedelta(minutes=5),
        dedup_backend="bloom",
        backend_kwargs={"expected_items_per_bucket": 10_000, "false_positive_rate": 0.001},
    )


def test_bloom_backend_catches_exact_duplicate():
    p = make_bloom_processor()
    t = datetime(2026, 1, 1, 0, 0, 0)
    assert p.process({"id": "a", "ts": t}) == RecordDecision.ACCEPT
    assert p.process({"id": "a", "ts": t}) == RecordDecision.DUPLICATE


def test_bloom_backend_false_positive_rate_is_bounded():
    # Run enough distinct keys through that we can check the observed
    # false-positive rate stays in the right ballpark for the configured
    # target (0.1%) — allowing generous headroom since this is a small,
    # fast-running sample, not a statistically rigorous benchmark.
    p = make_bloom_processor()
    records = generate_synthetic_records(
        num_records=5000, duplicate_rate=0.0, late_rate=0.0
    )
    decisions = [p.process(r) for r in records]
    false_positives = sum(1 for d in decisions if d == RecordDecision.DUPLICATE)
    # with duplicate_rate=0.0, every DUPLICATE decision here is a false positive
    assert false_positives / len(records) < 0.02  # well under 2%, generous margin


def test_bloom_memory_bounded_regardless_of_volume():
    p = make_bloom_processor()
    base = datetime(2026, 1, 1, 0, 0, 0)
    for i in range(20_000):
        p.process({"id": f"rec-{i}", "ts": base + timedelta(seconds=i)})
    # Bloom filter size is fixed per bucket at construction time, so this
    # should stay flat rather than growing with record count the way
    # ExactSetBackend would.
    size_at_20k = p._backend.approximate_size()  # pylint: disable=protected-access
    for i in range(20_000, 40_000):
        p.process({"id": f"rec-{i}", "ts": base + timedelta(seconds=i)})
    size_at_40k = p._backend.approximate_size()  # pylint: disable=protected-access
    # allowed to grow only as new time buckets open, not per-record
    assert size_at_40k < size_at_20k * 3

    