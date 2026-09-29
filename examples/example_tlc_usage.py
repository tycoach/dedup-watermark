"""
Example: wiring dedup_watermark into an NYC TLC trip-record ingestion step.

This shows the ONLY thing a caller needs to do to use the library: define
two small functions that know how to pull a key and an event time out of
their specific record shape. Everything else (watermark math, dedup
backend, eviction) is handled by the library.
"""

from datetime import datetime, timedelta

from dedup_watermark import DedupWatermarkProcessor, RecordDecision

# A real pipeline would read these rows from a parquet file landed in MinIO;
# here we inline a couple of rows just to show the shape.
tlc_rows = [
    {"trip_id": "t-1001", "pickup_datetime": "2026-01-01T08:00:00"},
    {"trip_id": "t-1002", "pickup_datetime": "2026-01-01T08:00:05"},
    {"trip_id": "t-1001", "pickup_datetime": "2026-01-01T08:00:00"},  # duplicate ingest
]


def parse_ts(row):
    return datetime.fromisoformat(row["pickup_datetime"])


processor = DedupWatermarkProcessor(
    key_fn=lambda row: row["trip_id"],
    event_time_fn=parse_ts,
    watermark_delay=timedelta(hours=2),
    dedup_backend="bloom",
    backend_kwargs={"expected_items_per_bucket": 500_000, "false_positive_rate": 0.001},
)

for row in tlc_rows:
    decision = processor.process(row)
    print(f"{row['trip_id']} -> {decision.value}")

print(processor.metrics.as_dict())
