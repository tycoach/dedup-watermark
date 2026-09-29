"""
Compare ExactSetBackend vs BloomFilterBackend on the same synthetic stream:
throughput, memory footprint, and (for bloom) observed false-positive rate.

Run: python benchmarks/bench_exact_vs_bloom.py
"""

import sys
import time
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))

from dedup_watermark import DedupWatermarkProcessor, RecordDecision  # noqa: E402
from fixtures.synthetic_records import generate_synthetic_records  # noqa: E402


def run_backend(name, records, **backend_kwargs):
    processor = DedupWatermarkProcessor(
        key_fn=lambda r: r["id"],
        event_time_fn=lambda r: r["ts"],
        watermark_delay=timedelta(minutes=5),
        dedup_backend=name,
        backend_kwargs=backend_kwargs,
    )
    start = time.perf_counter()
    for r in records:
        processor.process(r)
    elapsed = time.perf_counter() - start

    return {
        "backend": name,
        "elapsed_sec": round(elapsed, 4),
        "records_per_sec": round(len(records) / elapsed, 1),
        "approx_memory_units": processor._backend.approximate_size(),
        **processor.metrics.as_dict(),
    }


def main():
    num_records = 200_000
    records = generate_synthetic_records(
        num_records=num_records, duplicate_rate=0.08, late_rate=0.03
    )

    print(f"Benchmarking on {num_records} synthetic records "
          f"(8% injected duplicates, 3% injected late arrivals)\n")

    exact_result = run_backend("exact", records)
    bloom_result = run_backend(
        "bloom", records, expected_items_per_bucket=50_000, false_positive_rate=0.001
    )

    for result in (exact_result, bloom_result):
        print(result)


if __name__ == "__main__":
    main()
