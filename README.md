# streamdedup

Reusable streaming deduplication and late-arrival watermarking for data pipelines.

```bash
pip install streamdedup
```

Given a stream of records, `streamdedup` decides — for each one — whether it's
a new record, a duplicate, or arriving too late to trust:

```python
from datetime import timedelta
from dedup_watermark import DedupWatermarkProcessor

processor = DedupWatermarkProcessor(
    key_fn=lambda r: r["trip_id"],
    event_time_fn=lambda r: r["pickup_datetime"],
    watermark_delay=timedelta(hours=2),
    dedup_backend="bloom",   # or "exact"
)

decision = processor.process(record)  # ACCEPT / DUPLICATE / LATE_DROPPED / LATE_ACCEPTED
```

> **Note on naming:** the PyPI distribution is `streamdedup`
> (`pip install streamdedup`), but the importable module is
> `dedup_watermark` (`from dedup_watermark import ...`) — matching the
> `dedup-watermark` GitHub repo this package lives in. Same split as
> `pip install scikit-learn` / `import sklearn`.

## Where this fits in a pipeline

`streamdedup` sits at the ingestion boundary — the point where raw records
first enter your system, before they're written to your landing zone,
warehouse, or downstream consumers:

```
Source (Kafka, an API, a batch file, a webhook)
   │
   ▼
DedupWatermarkProcessor.process(record)
   │
   ├── ACCEPT / LATE_ACCEPTED  → write to your landing zone / warehouse
   ├── DUPLICATE               → drop, or log for audit
   └── LATE_DROPPED            → drop, or route to a backfill/dead-letter path
```

It's a pure decision function — it doesn't read from or write to anything
itself. You call `.process(record)` for each record as it arrives, and
branch on the `RecordDecision` it returns. That's the entire integration
surface, whether the caller is a Kafka consumer loop, a Spark
`mapPartitions`, or a plain batch script reading a file.

## Why this exists

Every pipeline that ingests events or records eventually has to answer the
same two questions: *have I seen this before?* and *is this arriving too
late to still count?* Retries, at-least-once delivery, multiple producers,
and network delays make both questions unavoidable at any real scale.

This library answers them once, generically, so it can be dropped into any
pipeline rather than reimplemented per project.

## The algorithm

The reusable part of this project is not the Python package scaffolding —
it's the algorithm inside `processor.py` and `backends/bloom_filter.py`.
The package is just how that algorithm gets distributed and reused across
pipelines.

**Watermarking.** The processor tracks the newest event time it has seen
(`max_event_time_seen`) and derives a moving watermark:

```
watermark = max_event_time_seen - watermark_delay
```

Any record with an event time older than the current watermark is
classified as late. This is the same core idea used by real stream
processors (Flink, Spark Structured Streaming, Kafka Streams) to decide
when a window of data can be considered "final" despite out-of-order
arrival — implemented here from first principles rather than by calling
into one of those frameworks.

**Deduplication, two ways, same interface.** A key is checked for
membership in a set of already-seen keys before being accepted:
- *Exact* — a hash set, bucketed by time window, evicted once a bucket
  falls behind the watermark. Zero false positives; memory scales with
  the number of distinct keys in the retention window.
- *Approximate* — a Bloom filter built from scratch: a bit array plus
  **double hashing** (the Kirsch–Mitzenmacher technique), which derives
  *k* independent hash positions from just two underlying hashes (SHA-256
  and MD5) instead of computing *k* separate hash functions. Filter size
  and hash count are derived from the standard Bloom sizing formulas
  (`m = -(n·ln p)/(ln 2)²`, `k = (m/n)·ln 2`) given a target false-positive
  rate. Memory is fixed per time bucket regardless of how many keys
  actually arrive — the trade-off is a small, tunable rate of false
  positives (a genuinely new record occasionally misclassified as a
  duplicate), with no false negatives ever.

**Bounded memory under continuous load.** Both backends bucket their
state by event time and evict buckets that have fully fallen behind the
watermark, so a stream that runs forever doesn't grow memory forever —
this is what makes either backend viable in a long-running pipeline
rather than just a batch script.

The `benchmarks/bench_exact_vs_bloom.py` script measures the actual
throughput/memory trade-off between the two implementations on the same
synthetic stream, rather than assuming one is "better."

## Design

- **Schema-agnostic**: the processor never touches your record's shape
  directly — you supply `key_fn` and `event_time_fn`, two small functions.
  No pipeline-specific code lives inside the library.
- **No I/O inside the library**: it doesn't know about Kafka, files, or
  databases — feed it records from anywhere, get decisions back.

## Contributing / local development setup

Clone the repo and install in editable mode with dev dependencies:

```bash
git clone https://github.com/tycoach/dedup-watermark.git
cd dedup-watermark
pip install -e ".[dev]"
```

## Run tests

```bash
pytest tests/ -v
```

## Run the benchmark

```bash
python benchmarks/bench_exact_vs_bloom.py
```

## Examples

- `examples/example_tlc_usage.py` — wiring against NYC TLC trip records
- `examples/example_generic_usage.py` — the same library against a
  completely different (insurance-claims-shaped) schema, to demonstrate
  portability

## Upgrade path

Dedup state currently lives in process memory. For a pipeline that needs
dedup state to survive a restart (e.g. a production claims pipeline), a
Redis-backed `DedupBackend` implementation can be dropped in without
changing `DedupWatermarkProcessor` at all — see `state/redis_store.py` for
the sketch.