import random
from datetime import datetime, timedelta


def generate_synthetic_records(
    num_records: int = 1000,
    start_time: datetime = datetime(2026, 1, 1),
    time_step: timedelta = timedelta(seconds=1),
    duplicate_rate: float = 0.1,
    late_rate: float = 0.05,
    max_lateness: timedelta = timedelta(minutes=10),
    seed: int = 42,
):
    """
    Generate a synthetic stream of {"id": ..., "ts": ...} records with
    injected duplicates and late arrivals, so processor behavior can be
    verified deterministically without depending on any real dataset
    happening to contain these cases naturally.

    Returns a list of dicts in emission order (i.e. the order a consumer
    would actually see them, including late/out-of-order records).
    """
    rng = random.Random(seed)
    records = []
    seen_ids = []

    for i in range(num_records):
        event_time = start_time + i * time_step
        record_id = f"rec-{i}"

        is_duplicate = seen_ids and rng.random() < duplicate_rate
        if is_duplicate:
            dup_of = rng.choice(seen_ids)
            records.append({"id": dup_of["id"], "ts": dup_of["ts"]})
            continue

        is_late = rng.random() < late_rate
        if is_late:
            lateness = timedelta(seconds=rng.randint(1, int(max_lateness.total_seconds())))
            event_time = event_time - lateness

        record = {"id": record_id, "ts": event_time}
        records.append(record)
        seen_ids.append(record)

    return records
