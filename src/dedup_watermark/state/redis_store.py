"""
Sketch for a Redis-backed dedup backend — not wired into the registry yet.

Why this exists: ExactSetBackend and BloomFilterBackend both hold state in
process memory, which means a pipeline restart loses everything it has
"seen" so far and could re-accept genuine duplicates across a deploy/crash.
For a pipeline where that matters (e.g. ERAC-Hertz claims processing),
swap in a backend like this one without touching DedupWatermarkProcessor —
that's the whole point of the DedupBackend interface.

Sketch:

    import redis

    class RedisSetBackend(DedupBackend):
        def __init__(self, redis_client, bucket_width, key_prefix="dedup_watermark"):
            self.r = redis_client
            self.bucket_width = bucket_width
            self.key_prefix = key_prefix

        def seen(self, key):
            # SISMEMBER across recent bucket keys, or a single rolling SET
            # with per-member TTL via a sorted set (ZADD score=event_time).
            ...

        def add(self, key, event_time):
            # ZADD to a sorted set keyed by bucket; score = event_time.timestamp()
            ...

        def evict_before(self, watermark):
            # ZREMRANGEBYSCORE to drop entries older than the watermark
            ...

Left unimplemented intentionally until a real pipeline needs restart-safe
dedup state — implement against that pipeline's actual Redis instance rather
than guessing at the interface in isolation.
"""
