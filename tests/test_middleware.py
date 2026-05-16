"""Tests for auth and rate-limit middleware in isolation."""
import pytest
from unittest.mock import AsyncMock, MagicMock
from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route


# ── Auth middleware ───────────────────────────────────────────────────────────

def _make_app_with_auth(api_key: str | None):
    import os
    os.environ["API_KEY"] = api_key or ""

    from app.middleware.auth import AuthMiddleware

    async def protected(request):
        return JSONResponse({"ok": True})

    async def public(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[
        Route("/api/v1/system/reset-all", protected, methods=["POST"]),
        Route("/api/v1/weather/summary", public, methods=["GET"]),
    ])
    app.add_middleware(AuthMiddleware)
    return app


def test_auth_blocks_unauthenticated_mutation():
    app = _make_app_with_auth("secret123")
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/v1/system/reset-all")
    assert r.status_code == 401
    assert "X-API-Key" in r.json()["detail"]


def test_auth_allows_valid_api_key():
    app = _make_app_with_auth("secret123")
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/v1/system/reset-all", headers={"X-API-Key": "secret123"})
    assert r.status_code == 200


def test_auth_allows_get_without_key():
    app = _make_app_with_auth("secret123")
    c = TestClient(app, raise_server_exceptions=False)
    r = c.get("/api/v1/weather/summary")
    assert r.status_code == 200


def test_auth_disabled_when_no_key_configured():
    app = _make_app_with_auth(None)
    c = TestClient(app, raise_server_exceptions=False)
    # No key configured → no protection
    r = c.post("/api/v1/system/reset-all")
    assert r.status_code == 200


# ── Rate limiter ──────────────────────────────────────────────────────────────

def test_rate_limit_headers_on_get():
    from app.middleware.rate_limiter import RateLimitMiddleware, _buckets
    _buckets.clear()

    async def endpoint(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/api/v1/weather/summary", endpoint)])
    app.add_middleware(RateLimitMiddleware)
    c = TestClient(app)

    r = c.get("/api/v1/weather/summary")
    assert r.status_code == 200
    assert "X-RateLimit-Limit" in r.headers
    assert int(r.headers["X-RateLimit-Limit"]) == 60


def test_rate_limit_blocks_after_exceeding():
    from app.middleware.rate_limiter import RateLimitMiddleware, _buckets, _WINDOW
    _buckets.clear()

    import time
    # Fill the bucket to the limit artificially
    ip_key = "testclient:GET+HEAD+OPTIONS:all"
    from collections import deque
    now = time.monotonic()
    _buckets[ip_key] = deque([now] * 60)  # 60 fake timestamps

    async def endpoint(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/api/v1/weather/summary", endpoint)])
    app.add_middleware(RateLimitMiddleware)
    c = TestClient(app, raise_server_exceptions=False)

    r = c.get("/api/v1/weather/summary")
    assert r.status_code == 429
    assert "Retry-After" in r.headers
    _buckets.clear()
