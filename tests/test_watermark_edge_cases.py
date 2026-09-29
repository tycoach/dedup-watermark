from datetime import datetime, timedelta

from dedup_watermark import DedupWatermarkProcessor, RecordDecision


def test_out_of_order_burst_within_watermark_still_dedups():
    p = DedupWatermarkProcessor(
        key_fn=lambda r: r["id"],
        event_time_fn=lambda r: r["ts"],
        watermark_delay=timedelta(minutes=5),
    )
    base = datetime(2026, 1, 1, 0, 0, 0)
    # records arrive out of order but all within the watermark window
    order = [2, 0, 1, 0, 3]  # index 0 repeated -> should be caught as duplicate
    times = [base, base + timedelta(seconds=1), base + timedelta(seconds=2),
             base + timedelta(seconds=3), base + timedelta(seconds=4)]
    seen_decisions = []
    for idx in order:
        seen_decisions.append(p.process({"id": f"rec-{idx}", "ts": times[idx]}))
    assert seen_decisions.count(RecordDecision.DUPLICATE) == 1


def test_clock_skew_does_not_break_watermark_monotonicity():
    p = DedupWatermarkProcessor(
        key_fn=lambda r: r["id"],
        event_time_fn=lambda r: r["ts"],
        watermark_delay=timedelta(minutes=5),
    )
    base = datetime(2026, 1, 1, 0, 0, 0)
    p.process({"id": "a", "ts": base + timedelta(hours=1)})
    watermark_after_future_record = p.current_watermark
    # a record with a slightly earlier timestamp than the last one should
    # not move the watermark backwards
    p.process({"id": "b", "ts": base + timedelta(minutes=30)})
    assert p.current_watermark == watermark_after_future_record


def test_empty_stream_has_no_watermark():
    p = DedupWatermarkProcessor(
        key_fn=lambda r: r["id"],
        event_time_fn=lambda r: r["ts"],
        watermark_delay=timedelta(minutes=5),
    )
    assert p.current_watermark is None


def test_invalid_late_policy_raises():
    import pytest
    with pytest.raises(ValueError):
        DedupWatermarkProcessor(
            key_fn=lambda r: r["id"],
            event_time_fn=lambda r: r["ts"],
            watermark_delay=timedelta(minutes=5),
            late_policy="not_a_real_policy",
        )
