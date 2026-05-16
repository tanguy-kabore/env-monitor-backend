"""
In-memory TTL cache with:
  - LRU eviction when MAX_SIZE is reached
  - Hit/miss/eviction statistics
  - Tag-based bulk invalidation
  - Thread-safe via a simple lock (asyncio-compatible)
"""
import time
import threading
import logging
from collections import OrderedDict
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

FOREVER = float("inf")
MAX_SIZE = 2048  # maximum entries before eviction

# Each entry: {"value": Any, "expires_at": float, "tags": frozenset[str]}
_store: OrderedDict[str, dict] = OrderedDict()
_tags: dict[str, set[str]] = {}  # tag → set of keys
_lock = threading.Lock()

# Stats
_hits = 0
_misses = 0
_evictions = 0


# ── Internal helpers ──────────────────────────────────────────────────────────

def _is_expired(entry: dict) -> bool:
    exp = entry["expires_at"]
    return exp != FOREVER and time.monotonic() > exp


def _evict_lru() -> None:
    """Remove an expired entry if one exists, otherwise evict the least-recently-used entry."""
    global _evictions
    # Prefer evicting an already-expired entry over a live one
    expired_key = next((k for k, v in _store.items() if _is_expired(v)), None)
    if expired_key is not None:
        entry = _store.pop(expired_key)
        for tag in entry.get("tags", frozenset()):
            _tags.get(tag, set()).discard(expired_key)
        _evictions += 1
        logger.debug("Expired evicted: %s", expired_key)
        return
    if _store:
        key, entry = _store.popitem(last=False)
        for tag in entry.get("tags", frozenset()):
            _tags.get(tag, set()).discard(key)
        _evictions += 1
        logger.debug("LRU evicted: %s", key)


def _register_tags(key: str, tags: frozenset) -> None:
    for tag in tags:
        if tag not in _tags:
            _tags[tag] = set()
        _tags[tag].add(key)


def _unregister_key(key: str, tags: frozenset) -> None:
    for tag in tags:
        _tags.get(tag, set()).discard(key)


# ── Public API ────────────────────────────────────────────────────────────────

def get(key: str) -> Optional[Any]:
    global _hits, _misses
    with _lock:
        entry = _store.get(key)
        if entry is None:
            _misses += 1
            return None
        if _is_expired(entry):
            _unregister_key(key, entry.get("tags", frozenset()))
            del _store[key]
            _misses += 1
            return None
        # Move to end (most recently used)
        _store.move_to_end(key)
        _hits += 1
        return entry["value"]


def set(key: str, value: Any, ttl: float = 300, tags: list[str] | None = None) -> None:
    tag_set = frozenset(tags or [])
    expires_at = FOREVER if ttl == FOREVER else time.monotonic() + ttl

    with _lock:
        if key in _store:
            _unregister_key(key, _store[key].get("tags", frozenset()))

        _store[key] = {"value": value, "expires_at": expires_at, "tags": tag_set}
        _store.move_to_end(key)
        _register_tags(key, tag_set)

        while len(_store) > MAX_SIZE:
            _evict_lru()


def delete(key: str) -> None:
    with _lock:
        entry = _store.pop(key, None)
        if entry:
            _unregister_key(key, entry.get("tags", frozenset()))


def delete_prefix(prefix: str) -> int:
    with _lock:
        keys = [k for k in list(_store.keys()) if k.startswith(prefix)]
        for k in keys:
            entry = _store.pop(k)
            _unregister_key(k, entry.get("tags", frozenset()))
        return len(keys)


def invalidate_tag(tag: str) -> int:
    """Invalidate all entries that carry the given tag."""
    with _lock:
        keys = list(_tags.get(tag, set()))
        for k in keys:
            entry = _store.pop(k, None)
            if entry:
                _unregister_key(k, entry.get("tags", frozenset()))
        if tag in _tags:
            del _tags[tag]
        return len(keys)


def invalidate_tags(*tags: str) -> int:
    """Invalidate all entries matching any of the given tags."""
    total = 0
    for tag in tags:
        total += invalidate_tag(tag)
    return total


def clear() -> None:
    with _lock:
        _store.clear()
        _tags.clear()


def stats() -> dict:
    with _lock:
        total = _hits + _misses
        return {
            "entries": len(_store),
            "max_size": MAX_SIZE,
            "hits": _hits,
            "misses": _misses,
            "hit_rate": round(_hits / total, 3) if total else 0.0,
            "evictions": _evictions,
            "tags": {t: len(keys) for t, keys in _tags.items()},
        }


def cached(key_fn: Callable, ttl: float = 300, tags: list[str] | None = None):
    """Decorator for async functions: @cached(lambda *a, **kw: f"key:{a[0]}", ttl=60)"""
    from functools import wraps

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key = key_fn(*args, **kwargs)
            cached_val = get(key)
            if cached_val is not None:
                return cached_val
            result = await func(*args, **kwargs)
            set(key, result, ttl, tags=tags)
            return result
        return wrapper
    return decorator
