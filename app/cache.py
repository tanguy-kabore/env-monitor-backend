import time
import asyncio
import logging
from typing import Any, Optional, Callable
from functools import wraps

logger = logging.getLogger(__name__)

_store: dict[str, tuple[Any, float]] = {}  # key -> (value, expires_at)

FOREVER = float("inf")


def get(key: str) -> Optional[Any]:
    entry = _store.get(key)
    if entry is None:
        return None
    value, expires_at = entry
    if expires_at != FOREVER and time.monotonic() > expires_at:
        del _store[key]
        return None
    return value


def set(key: str, value: Any, ttl: float = 300) -> None:
    expires_at = FOREVER if ttl == FOREVER else time.monotonic() + ttl
    _store[key] = (value, expires_at)


def delete(key: str) -> None:
    _store.pop(key, None)


def delete_prefix(prefix: str) -> int:
    keys = [k for k in list(_store.keys()) if k.startswith(prefix)]
    for k in keys:
        del _store[k]
    return len(keys)


def clear() -> None:
    _store.clear()


def cached(key_fn: Callable, ttl: float = 300):
    """Decorator for async functions: @cached(lambda *a, **kw: f"key:{a[0]}", ttl=60)"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key = key_fn(*args, **kwargs)
            cached_val = get(key)
            if cached_val is not None:
                return cached_val
            result = await func(*args, **kwargs)
            set(key, result, ttl)
            return result
        return wrapper
    return decorator
