"""Integration tests for /api/v1/system/* endpoints."""
import pytest
from unittest.mock import MagicMock, patch


# ── /health ───────────────────────────────────────────────────────────────────

def test_health_ok(client):
    with patch("app.routers.system.check_connection", return_value=True):
        r = client.get("/api/v1/system/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "healthy"
    assert body["database"] == "connected"


def test_health_degraded(client):
    with patch("app.routers.system.check_connection", return_value=False):
        r = client.get("/api/v1/system/health")
    assert r.status_code == 200
    assert r.json()["status"] == "degraded"


# ── /config ───────────────────────────────────────────────────────────────────

def test_get_config(client):
    r = client.get("/api/v1/system/config")
    assert r.status_code == 200
    body = r.json()
    assert "country" in body
    assert "ml" in body
    assert "alert_thresholds" in body


# ── /cache-stats ──────────────────────────────────────────────────────────────

def test_cache_stats(client):
    r = client.get("/api/v1/system/cache-stats")
    assert r.status_code == 200
    body = r.json()
    assert "hits" in body
    assert "misses" in body
    assert "entries" in body
    assert "hit_rate" in body


# ── /collection-log (pagination) ─────────────────────────────────────────────

def _make_supabase_page(data, count):
    result = MagicMock()
    result.data = data
    result.count = count
    q = MagicMock()
    q.execute.return_value = result
    q.eq.return_value = q
    q.order.return_value = q
    q.range.return_value = q
    q.select.return_value = q
    return q


def test_collection_log_default_pagination(client, mock_supabase):
    rows = [{"id": str(i), "status": "success"} for i in range(5)]
    mock_q = _make_supabase_page(rows, 5)
    mock_supabase.table.return_value = mock_q

    r = client.get("/api/v1/system/collection-log")
    assert r.status_code == 200
    body = r.json()
    assert "data" in body
    assert "pagination" in body
    p = body["pagination"]
    assert "total" in p
    assert "limit" in p
    assert "offset" in p
    assert "has_more" in p


def test_collection_log_custom_pagination(client, mock_supabase):
    mock_q = _make_supabase_page([], 100)
    mock_supabase.table.return_value = mock_q

    r = client.get("/api/v1/system/collection-log?limit=10&offset=20")
    assert r.status_code == 200
    p = r.json()["pagination"]
    assert p["limit"] == 10
    assert p["offset"] == 20


def test_collection_log_limit_validation(client):
    r = client.get("/api/v1/system/collection-log?limit=9999")
    assert r.status_code == 422  # FastAPI validation error


# ── Auth middleware (admin endpoints) ─────────────────────────────────────────

def test_initialize_requires_api_key(client):
    r = client.post("/api/v1/system/initialize")
    assert r.status_code == 401


def test_initialize_with_valid_api_key(client, admin_headers, mock_supabase):
    cfg_result = MagicMock(data=[{"value": "true"}])
    mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = cfg_result

    with patch("app.routers.system.get_system_config", return_value="true"):
        r = client.post("/api/v1/system/initialize", headers=admin_headers)
    # Either 200 (skipped/started) or 500 (db not fully mocked) — not 401
    assert r.status_code != 401


def test_reset_all_requires_api_key(client):
    r = client.post("/api/v1/system/reset-all")
    assert r.status_code == 401


# ── Rate limiting headers ─────────────────────────────────────────────────────

def test_rate_limit_headers_present(client):
    r = client.get("/api/v1/system/health")
    assert r.status_code == 200
    assert "X-RateLimit-Limit" in r.headers
    assert "X-RateLimit-Remaining" in r.headers


# ── Legacy redirect ───────────────────────────────────────────────────────────

def test_legacy_redirect(client):
    r = client.get("/api/system/health", follow_redirects=False)
    assert r.status_code == 308
    assert "/api/v1/system/health" in r.headers["location"]
