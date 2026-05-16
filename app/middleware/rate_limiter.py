"""
Sliding-window rate limiter middleware.

Limits per client IP (or X-Forwarded-For):
  - GET  requests : 200 req / minute
  - POST/PUT/DELETE : 20 req / minute
  - /api/v1/system/initialize : 2 req / minute  (heavy background task)

Responses include standard rate-limit headers.
"""
import time
import logging
from collections import deque
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

_WINDOW = 60  # seconds

# (method_group, path_prefix) → max requests per window
_LIMITS: list[tuple[set[str], str, int]] = [
    ({"GET", "HEAD", "OPTIONS"}, "", 200),      # all GETs
    ({"POST", "PUT", "DELETE", "PATCH"}, "/api/v1/system/initialize", 2),
    ({"POST", "PUT", "DELETE", "PATCH"}, "", 20),  # all mutations
]

# ip → deque of timestamps, keyed by bucket
_buckets: dict[str, deque] = {}


def _get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _bucket_key(ip: str, group: str) -> str:
    return f"{ip}:{group}"


def _check_limit(ip: str, method: str, path: str) -> tuple[bool, int, int]:
    """
    Returns (allowed, limit, remaining).
    Mutates the in-memory bucket.
    """
    for methods, prefix, limit in _LIMITS:
        if method not in methods:
            continue
        if prefix and not path.startswith(prefix):
            continue

        group = f"{'+'.join(sorted(methods))}:{prefix or 'all'}"
        key = _bucket_key(ip, group)
        now = time.monotonic()
        window_start = now - _WINDOW

        if key not in _buckets:
            _buckets[key] = deque()
        dq = _buckets[key]

        # Evict expired timestamps
        while dq and dq[0] < window_start:
            dq.popleft()

        remaining = max(0, limit - len(dq))
        if len(dq) >= limit:
            return False, limit, 0

        dq.append(now)
        return True, limit, remaining - 1

    return True, 0, 0


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        ip = _get_client_ip(request)
        allowed, limit, remaining = _check_limit(ip, request.method, request.url.path)

        if not allowed:
            logger.warning("Rate limit exceeded: %s %s from %s", request.method, request.url.path, ip)
            headers = {
                "X-RateLimit-Limit": str(limit),
                "X-RateLimit-Remaining": "0",
                "Retry-After": str(_WINDOW),
            }
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Too Many Requests",
                    "detail": f"Limite de {limit} requêtes par minute dépassée. Réessayez dans {_WINDOW}s.",
                },
                headers=headers,
            )

        response = await call_next(request)

        if limit:
            response.headers["X-RateLimit-Limit"] = str(limit)
            response.headers["X-RateLimit-Remaining"] = str(remaining)

        return response
