# In-memory is the default and lives inside each backend directly (see
# backends/exact_set.py and backends/bloom_filter.py) — there is no separate
# state object to instantiate for the common case.
#
# This module is a placeholder for pluggable external state (Redis, RocksDB)
# for pipelines that need dedup state to survive a process restart. See
# state/redis_store.py for the shape that upgrade takes.
