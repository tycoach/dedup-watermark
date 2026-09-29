"""
Example: the same library wired into a completely different, made-up schema
(an insurance-claims-style record) to prove the algorithm has no hidden
dependency on the NYC TLC shape — this is the actual test of reusability
for something like the ERAC-Hertz MOM pipeline.
"""

from datetime import datetime, timedelta

from dedup_watermark import DedupWatermarkProcessor

claim_records = [
    {"claim_ref": "CLM-2026-001", "submitted_at": datetime(2026, 3, 1, 9, 0, 0)},
    {"claim_ref": "CLM-2026-002", "submitted_at": datetime(2026, 3, 1, 9, 5, 0)},
    {"claim_ref": "CLM-2026-001", "submitted_at": datetime(2026, 3, 1, 9, 0, 0)},  # resent
]

processor = DedupWatermarkProcessor(
    key_fn=lambda r: r["claim_ref"],
    event_time_fn=lambda r: r["submitted_at"],
    watermark_delay=timedelta(hours=6),   # claims pipelines tolerate longer lateness
    dedup_backend="exact",                # lower volume, exact correctness matters more here
    late_policy="accept_flagged",         # late claims still get processed, just flagged
)

for record in claim_records:
    decision = processor.process(record)
    print(f"{record['claim_ref']} -> {decision.value}")

print(processor.metrics.as_dict())
