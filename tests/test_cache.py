"""Unit tests for the in-memory cache module."""
import time
import pytest
from app import cache


@pytest.fixture(autouse=True)
def reset_cache():
    cache.clear()
    yield
    cache.clear()


# ── Basic get/set/delete ──────────────────────────────────────────────────────

def test_set_and_get():
    cache.set("k1", {"x": 1})
    assert cache.get("k1") == {"x": 1}


def test_get_missing_returns_none():
    assert cache.get("nonexistent") is None


def test_delete():
    cache.set("k2", "value")
    cache.delete("k2")
    assert cache.get("k2") is None


def test_delete_missing_is_noop():
    cache.delete("ghost")  # must not raise


# ── TTL expiry ────────────────────────────────────────────────────────────────

def test_ttl_expiry(monkeypatch):
    import app.cache as _mod
    fake_now = [0.0]
    monkeypatch.setattr(_mod.time, "monotonic", lambda: fake_now[0])

    cache.set("ttl_key", "data", ttl=10)
    assert cache.get("ttl_key") == "data"

    fake_now[0] = 11.0  # advance past TTL
    assert cache.get("ttl_key") is None


def test_forever_ttl_never_expires(monkeypatch):
    import app.cache as _mod
    fake_now = [0.0]
    monkeypatch.setattr(_mod.time, "monotonic", lambda: fake_now[0])

    cache.set("forever_key", "alive", ttl=cache.FOREVER)
    fake_now[0] = 999_999.0
    assert cache.get("forever_key") == "alive"


# ── Prefix deletion ───────────────────────────────────────────────────────────

def test_delete_prefix():
    cache.set("weather:city1", "w1")
    cache.set("weather:city2", "w2")
    cache.set("flood:city1", "f1")

    removed = cache.delete_prefix("weather:")
    assert removed == 2
    assert cache.get("weather:city1") is None
    assert cache.get("weather:city2") is None
    assert cache.get("flood:city1") == "f1"


# ── Tag-based invalidation ────────────────────────────────────────────────────

def test_invalidate_tag():
    cache.set("a", 1, tags=["weather"])
    cache.set("b", 2, tags=["weather", "city"])
    cache.set("c", 3, tags=["flood"])

    removed = cache.invalidate_tag("weather")
    assert removed == 2
    assert cache.get("a") is None
    assert cache.get("b") is None
    assert cache.get("c") == 3


def test_invalidate_tags_multiple():
    cache.set("x", 10, tags=["weather"])
    cache.set("y", 20, tags=["flood"])
    cache.set("z", 30, tags=["drought"])

    removed = cache.invalidate_tags("weather", "flood")
    assert removed == 2
    assert cache.get("z") == 30


def test_invalidate_nonexistent_tag_is_noop():
    removed = cache.invalidate_tag("does_not_exist")
    assert removed == 0


# ── LRU eviction ─────────────────────────────────────────────────────────────

def test_lru_eviction():
    original_max = cache.MAX_SIZE
    cache.MAX_SIZE = 3

    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)
    # Access "a" so "b" becomes the LRU
    cache.get("a")
    cache.set("d", 4)  # triggers eviction of "b"

    assert cache.get("b") is None  # evicted
    assert cache.get("a") is not None
    assert cache.get("c") is not None
    assert cache.get("d") == 4

    cache.MAX_SIZE = original_max


# ── Stats ─────────────────────────────────────────────────────────────────────

def test_stats_counts_hits_and_misses():
    # Reset global counters via clear (they are module-level)
    import app.cache as _mod
    _mod._hits = 0
    _mod._misses = 0

    cache.set("s1", "v1")
    cache.get("s1")   # hit
    cache.get("s1")   # hit
    cache.get("nope") # miss

    s = cache.stats()
    assert s["hits"] == 2
    assert s["misses"] == 1
    assert s["hit_rate"] == pytest.approx(2 / 3, abs=0.01)


def test_stats_includes_tags():
    cache.set("t1", 1, tags=["foo"])
    cache.set("t2", 2, tags=["foo", "bar"])

    s = cache.stats()
    assert s["tags"]["foo"] == 2
    assert s["tags"]["bar"] == 1


# ── cached() decorator ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cached_decorator():
    call_count = [0]

    @cache.cached(key_fn=lambda n: f"dec:{n}", ttl=60)
    async def expensive(n: int):
        call_count[0] += 1
        return n * 2

    result1 = await expensive(5)
    result2 = await expensive(5)

    assert result1 == 10
    assert result2 == 10
    assert call_count[0] == 1  # called only once; second hit from cache
