from .base import DedupBackend
from .exact_set import ExactSetBackend
from .bloom_filter import BloomFilterBackend

_REGISTRY = {
    "exact": ExactSetBackend,
    "bloom": BloomFilterBackend,
}


def create_backend(name: str, **kwargs) -> DedupBackend:
    """Factory so callers can select a backend by string name (e.g. from config)."""
    if name not in _REGISTRY:
        raise ValueError(f"Unknown backend '{name}'. Available: {list(_REGISTRY)}")
    return _REGISTRY[name](**kwargs)


__all__ = ["DedupBackend", "ExactSetBackend", "BloomFilterBackend", "create_backend"]
