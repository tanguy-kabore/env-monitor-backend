"""
API key authentication middleware.

Rules:
- All GET requests are public (read-only environmental data).
- POST / PUT / DELETE / PATCH on /api/v1/system/* or mutation endpoints require
  the X-API-Key header to match the API_KEY env variable.
- If API_KEY is not configured, admin protection is disabled (dev mode).
- /api/v1/alerts/generate and /api/v1/alerts/resolve-all are also protected.
"""
import os
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

# Endpoints that require an API key regardless of HTTP method
_PROTECTED_PREFIXES = (
    "/api/v1/system/initialize",
    "/api/v1/system/reset",
    "/api/v1/system/collect",
    "/api/v1/system/train",
    "/api/v1/system/models/cleanup",
    "/api/v1/alerts/generate",
    "/api/v1/alerts/resolve-all",
    "/api/v1/alerts/archive-daily",
)

# Methods that mutate state — require API key on any matched route
_MUTATION_METHODS = {"POST", "PUT", "DELETE", "PATCH"}


def _requires_auth(method: str, path: str) -> bool:
    if method in _MUTATION_METHODS:
        if any(path.startswith(p) for p in _PROTECTED_PREFIXES):
            return True
        # Protect all system mutations
        if path.startswith("/api/v1/system/"):
            return True
    return False


class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, **kwargs):
        super().__init__(app, **kwargs)
        self._api_key: str | None = os.getenv("API_KEY")
        if not self._api_key:
            logger.warning(
                "API_KEY not set — admin endpoints are unprotected (development mode)"
            )

    async def dispatch(self, request: Request, call_next):
        if not self._api_key:
            return await call_next(request)

        if _requires_auth(request.method, request.url.path):
            provided = request.headers.get("X-API-Key", "")
            if provided != self._api_key:
                logger.warning(
                    "Unauthorized admin request: %s %s from %s",
                    request.method,
                    request.url.path,
                    request.client.host if request.client else "unknown",
                )
                return JSONResponse(
                    status_code=401,
                    content={
                        "error": "Unauthorized",
                        "detail": "X-API-Key header manquant ou invalide.",
                    },
                    headers={"WWW-Authenticate": "ApiKey"},
                )

        return await call_next(request)
