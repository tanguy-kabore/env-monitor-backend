"""
Proactive cache warmer.

Populates per-city forecast caches by calling external APIs directly,
bypassing HTTP overhead. Runs at startup (after DB check) and every
few hours via the scheduler so users are never the first to hit a
cold cache entry.
"""
import asyncio
import logging

logger = logging.getLogger(__name__)

_FORECAST_DAYS = 7          # default days param used by the weather page
_BATCH_SIZE    = 4          # concurrent cities per batch (avoid API rate limits)
_BATCH_PAUSE   = 1.5        # seconds between batches
_FORECAST_TTL  = 14_400     # 4 h  — matches router TTL
_MAP_TTL       = 1_800      # 30 min — matches router TTL


async def warm_caches() -> dict:
    """
    Pre-populate cache for the top cities and map endpoints.
    Returns a summary dict with counts for logging.
    """
    try:
        from app.database import get_supabase
        from app import cache as _cache
        from app.services import open_meteo

        from app.database import db_exec
        client = get_supabase()
        cities_res = await db_exec(lambda: client.table("locations")
            .select("id,external_id,latitude,longitude,name")
            .eq("type", "city")
            .eq("is_active", True)
            .order("population", desc=True)
            .limit(20)
            .execute())
        cities = cities_res.data or []
        if not cities:
            logger.info("Cache warmer: no active cities found")
            return {"warmed": 0, "errors": 0}

        ok = 0
        errors = 0

        async def warm_city(city: dict):
            nonlocal ok, errors
            lat  = city["latitude"]
            lon  = city["longitude"]
            eid  = city["external_id"]
            name = city["name"]

            # ── Weather forecast ────────────────────────────────────────────
            try:
                live = await open_meteo.fetch_current_weather(lat, lon)
                _cache.set(f"weather:forecast:{eid}:{_FORECAST_DAYS}", {
                    "data": live,
                    "location": name,
                    "coordinates": {"latitude": lat, "longitude": lon},
                }, ttl=_FORECAST_TTL)
                ok += 1
            except Exception as exc:
                logger.debug("Weather forecast warm failed %s: %s", eid, exc)
                errors += 1

            # ── Flood forecast ──────────────────────────────────────────────
            try:
                live = await open_meteo.fetch_flood_data(lat, lon)
                _cache.set(f"flood:forecast:{eid}", {
                    "data": live,
                    "location": name,
                }, ttl=_FORECAST_TTL)
                ok += 1
            except Exception as exc:
                logger.debug("Flood forecast warm failed %s: %s", eid, exc)
                errors += 1

            # ── Air-quality forecast ────────────────────────────────────────
            try:
                live = await open_meteo.fetch_air_quality_forecast(lat, lon)
                _cache.set(f"aq:forecast:{eid}", {
                    "data": live,
                    "location": name,
                }, ttl=_FORECAST_TTL)
                ok += 1
            except Exception as exc:
                logger.debug("AQ forecast warm failed %s: %s", eid, exc)
                errors += 1

        # Process in small batches to avoid hammering external APIs
        for i in range(0, len(cities), _BATCH_SIZE):
            batch = cities[i : i + _BATCH_SIZE]
            await asyncio.gather(*[warm_city(c) for c in batch], return_exceptions=True)
            if i + _BATCH_SIZE < len(cities):
                await asyncio.sleep(_BATCH_PAUSE)

        # ── Warm shared map / summary endpoints via internal HTTP ───────────
        await _warm_global_endpoints()

        summary = {"warmed": ok, "errors": errors, "cities": len(cities)}
        logger.info("Cache warmer complete: %s", summary)
        return summary

    except Exception as exc:
        logger.warning("Cache warmer failed: %s", exc)
        return {"warmed": 0, "errors": 1}


async def _warm_global_endpoints():
    """Hit the aggregate map/summary endpoints so their caches are populated."""
    try:
        import httpx
        endpoints = [
            "/api/v1/dashboard",
            "/api/v1/weather/summary",
            "/api/v1/air-quality/map",
            "/api/v1/floods/risk-map",
            "/api/v1/drought/map",
        ]
        async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=30) as client:
            await asyncio.gather(
                *[client.get(ep) for ep in endpoints],
                return_exceptions=True,
            )
    except Exception as exc:
        logger.debug("Global endpoint warm failed: %s", exc)
