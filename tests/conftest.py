"""
Pytest fixtures shared across all test modules.

The test client patches Supabase so tests run without a live database.
Set SUPABASE_URL and SUPABASE_KEY to dummy values so pydantic-settings
does not raise a validation error at import time.
"""
import os
import pytest
from unittest.mock import MagicMock, patch

# Provide dummy env vars before any app import
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-service-role-key")
os.environ.setdefault("API_KEY", "test-api-key")

from fastapi.testclient import TestClient


@pytest.fixture(scope="session")
def mock_supabase():
    """Return a MagicMock that mimics the Supabase client."""
    client = MagicMock()
    # Default .execute() returns empty result
    execute_result = MagicMock(data=[], count=0)
    client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = execute_result
    return client


@pytest.fixture(scope="session")
def app(mock_supabase):
    """FastAPI test app with Supabase and scheduler stubbed out."""
    with (
        patch("app.database.get_supabase", return_value=mock_supabase),
        patch("app.database.check_connection", return_value=True),
        patch("app.services.scheduler.setup_scheduler"),
    ):
        from main import app as fastapi_app
        yield fastapi_app


@pytest.fixture(scope="session")
def client(app):
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def admin_headers():
    """Headers required for admin / mutation endpoints."""
    return {"X-API-Key": os.environ["API_KEY"]}
