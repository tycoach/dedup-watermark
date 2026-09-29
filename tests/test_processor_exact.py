from datetime import datetime, timedelta

from dedup_watermark import DedupWatermarkProcessor, RecordDecision


def make_processor(backend="exact"):
    return DedupWatermarkProcessor(
        key_fn=lambda r: r["id"],
        event_time_fn=lambda r: r["ts"],
        watermark_delay=timedelta(minutes=5),
        dedup_backend=backend,
    )


def test_first_record_is_accepted():
    p = make_processor()
    decision = p.process({"id": "a", "ts": datetime(2026, 1, 1, 0, 0, 0)})
    assert decision == RecordDecision.ACCEPT


def test_exact_duplicate_is_flagged():
    p = make_processor()
    t = datetime(2026, 1, 1, 0, 0, 0)
    assert p.process({"id": "a", "ts": t}) == RecordDecision.ACCEPT
    assert p.process({"id": "a", "ts": t}) == RecordDecision.DUPLICATE


def test_late_record_dropped_by_default():
    p = make_processor()
    base = datetime(2026, 1, 1, 0, 0, 0)
    p.process({"id": "a", "ts": base})
    # advance watermark well past base
    p.process({"id": "b", "ts": base + timedelta(minutes=10)})
    # this record is older than (max_seen - watermark_delay)
    decision = p.process({"id": "c", "ts": base - timedelta(minutes=1)})
    assert decision == RecordDecision.LATE_DROPPED


def test_late_record_accepted_when_policy_set():
    p = DedupWatermarkProcessor(
        key_fn=lambda r: r["id"],
        event_time_fn=lambda r: r["ts"],
        watermark_delay=timedelta(minutes=5),
        late_policy="accept_flagged",
    )
    base = datetime(2026, 1, 1, 0, 0, 0)
    p.process({"id": "a", "ts": base})
    p.process({"id": "b", "ts": base + timedelta(minutes=10)})
    decision = p.process({"id": "c", "ts": base - timedelta(minutes=1)})
    assert decision == RecordDecision.LATE_ACCEPTED


def test_metrics_track_all_decision_types():
    p = make_processor()
    base = datetime(2026, 1, 1, 0, 0, 0)
    p.process({"id": "a", "ts": base})          # ACCEPT
    p.process({"id": "a", "ts": base})          # DUPLICATE
    p.process({"id": "b", "ts": base + timedelta(minutes=10)})  # ACCEPT, advances watermark
    p.process({"id": "c", "ts": base - timedelta(minutes=1)})   # LATE_DROPPED

    m = p.metrics.as_dict()
    assert m["accepted"] == 2
    assert m["duplicates"] == 1
    assert m["late_dropped"] == 1
    assert m["total"] == 4


def test_eviction_keeps_memory_bounded():
    p = make_processor()
    base = datetime(2026, 1, 1, 0, 0, 0)
    for i in range(500):
        p.process({"id": f"rec-{i}", "ts": base + timedelta(seconds=i)})
    # far in the future — everything before should be evicted from the backend
    p.process({"id": "future", "ts": base + timedelta(hours=5)})
    assert p._backend.approximate_size() < 500  # pylint: disable=protected-access