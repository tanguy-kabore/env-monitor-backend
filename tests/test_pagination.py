"""Tests verifying pagination behaviour on heavy endpoints."""
import pytest
from unittest.mock import MagicMock, patch


def _mock_paginated(data, count):
    result = MagicMock()
    result.data = data
    result.count = count
    chain = MagicMock()
    chain.execute.return_value = result
    for method in ("select", "eq", "order", "range", "in_", "limit", "not_"):
        getattr(chain, method).return_value = chain
    return chain


# ── Alerts pagination ─────────────────────────────────────────────────────────

def test_alerts_pagination_default(client, mock_supabase):
    rows = [{"id": str(i)} for i in range(10)]
    mock_supabase.table.return_value = _mock_paginated(rows, 10)

    r = client.get("/api/v1/alerts")
    assert r.status_code == 200
    body = r.json()
    assert "pagination" in body
    assert body["pagination"]["limit"] == 50   # default
    assert body["pagination"]["offset"] == 0


def test_alerts_pagination_params(client, mock_supabase):
    mock_supabase.table.return_value = _mock_paginated([], 200)

    r = client.get("/api/v1/alerts?limit=25&offset=50")
    assert r.status_code == 200
    p = r.json()["pagination"]
    assert p["limit"] == 25
    assert p["offset"] == 50
    assert p["has_more"] is True  # 200 total, offset+limit=75 < 200


def test_alerts_has_more_false_on_last_page(client, mock_supabase):
    mock_supabase.table.return_value = _mock_paginated([], 30)

    r = client.get("/api/v1/alerts?limit=50&offset=0")
    assert r.status_code == 200
    assert r.json()["pagination"]["has_more"] is False  # 30 < 50


def test_alerts_limit_too_large(client):
    r = client.get("/api/v1/alerts?limit=999")
    assert r.status_code == 422


# ── Models pagination ─────────────────────────────────────────────────────────

def test_models_pagination_default(client, mock_supabase):
    mock_supabase.table.return_value = _mock_paginated([], 0)

    r = client.get("/api/v1/system/models")
    assert r.status_code == 200
    p = r.json()["pagination"]
    assert p["limit"] == 50
    assert p["offset"] == 0


def test_models_filter_by_type(client, mock_supabase):
    mock_supabase.table.return_value = _mock_paginated([], 0)

    r = client.get("/api/v1/system/models?model_type=weather")
    assert r.status_code == 200


# ── Collection-log pagination ─────────────────────────────────────────────────

def test_collection_log_has_more(client, mock_supabase):
    mock_supabase.table.return_value = _mock_paginated([], 100)

    r = client.get("/api/v1/system/collection-log?limit=20&offset=0")
    assert r.status_code == 200
    assert r.json()["pagination"]["has_more"] is True  # 100 > 20


def test_collection_log_filter_by_status(client, mock_supabase):
    mock_supabase.table.return_value = _mock_paginated([], 0)

    r = client.get("/api/v1/system/collection-log?status=failed")
    assert r.status_code == 200
