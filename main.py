import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import RedirectResponse
from dotenv import load_dotenv

load_dotenv()

from app.config import get_app_config
from app.routers import locations, weather, floods, air_quality, drought, climate, alerts, system, report, export
from app.services.scheduler import setup_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Legacy /api/* → /api/v1/* prefixes that changed with versioning
_LEGACY_PREFIXES = [
    "/api/system", "/api/weather", "/api/floods", "/api/air-quality",
    "/api/drought", "/api/climate", "/api/locations", "/api/report",
    "/api/alerts", "/api/export", "/api/dashboard",
]


async def _pre_warm_cache():
    """Pre-populate expensive cache entries so first user request is fast."""
    import asyncio
    await asyncio.sleep(8)
    try:
        import httpx
        base = "http://localhost:8000"
        endpoints = [
            "/api/v1/dashboard",
            "/api/v1/weather/summary",
            "/api/v1/air-quality/map",
            "/api/v1/floods/risk-map",
            "/api/v1/drought/map",
        ]
        async with httpx.AsyncClient(timeout=60) as client:
            for ep in endpoints:
                try:
                    await client.get(f"{base}{ep}")
                    logger.info(f"Cache pre-warmed: {ep}")
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"Cache pre-warm failed: {e}")


async def _boot_warm_caches():
    """Warm per-city forecast caches shortly after startup."""
    import asyncio
    await asyncio.sleep(12)   # let the server fully bind first
    from app.services.cache_warmer import warm_caches
    logger.info("Boot cache warm starting...")
    result = await warm_caches()
    logger.info(f"Boot cache warm done: {result}")


async def _warm_up_models():
    # Model training uses sync Supabase calls throughout ml_engine.py and would block
    # the event loop if awaited directly. The APScheduler job handles periodic retraining.
    logger.info("Model warm-up skipped at startup — scheduler handles periodic retraining")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    config = get_app_config()
    logger.info(f"Starting {config.app_name} v{config.app_version}")
    logger.info(f"Country: {config.country.get('name', 'Unknown')}")

    try:
        from app.database import check_connection, prime_location_cache
        if check_connection():
            logger.info("Database connection verified")
            setup_scheduler()
            import asyncio
            asyncio.ensure_future(prime_location_cache())
            asyncio.ensure_future(_pre_warm_cache())
            asyncio.ensure_future(_warm_up_models())
            asyncio.ensure_future(_boot_warm_caches())
        else:
            logger.warning("Database not connected - scheduler not started")
    except Exception as e:
        logger.warning(f"Could not connect to database: {e}")

    yield
    logger.info("Shutting down...")


config = get_app_config()

app = FastAPI(
    title=config.app_name,
    description=config.get("app.description", "Environmental Monitoring System"),
    version=config.app_version,
    lifespan=lifespan,
    # Expose both /docs (v1) and legacy /redoc
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/api/v1/openapi.json",
)

app.add_middleware(GZipMiddleware, minimum_size=1024)

# ── Import custom middleware ──────────────────────────────────────────────────
from app.middleware.auth import AuthMiddleware
from app.middleware.rate_limiter import RateLimitMiddleware

app.add_middleware(RateLimitMiddleware)
app.add_middleware(AuthMiddleware)

# CORSMiddleware must be outermost so it adds headers to ALL responses,
# including 429 / 401 returned by inner middlewares before reaching the routes.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers (v1) ─────────────────────────────────────────────────────────────
app.include_router(system.router)
app.include_router(locations.router)
app.include_router(weather.router)
app.include_router(floods.router)
app.include_router(air_quality.router)
app.include_router(drought.router)
app.include_router(climate.router)
app.include_router(alerts.router)
app.include_router(report.router)
app.include_router(export.router)


# ── Root & dashboard ─────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "name": config.app_name,
        "version": config.app_version,
        "description": config.get("app.description"),
        "country": config.country.get("name"),
        "api": "/api/v1",
        "docs": "/docs",
    }


@app.get("/api/v1/dashboard")
async def get_dashboard():
    import asyncio
    from app.database import get_supabase
    from app import cache as _cache

    cache_key = "dashboard:main"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()

    # Run the locations query first (needed to build loc_ids for the batch queries)
    locations_result = await asyncio.to_thread(
        lambda: client.table("locations")
        .select("id, external_id, name, latitude, longitude, population")
        .eq("type", "city")
        .eq("is_active", True)
        .order("population", desc=True)
        .limit(100)
        .execute()
    )
    all_locs = locations_result.data or []
    loc_ids = [loc["id"] for loc in all_locs]

    if not loc_ids:
        return {"all_cities": [], "top_cities": [], "total_cities": 0, "active_alerts_count": 0}

    # Use asyncio.to_thread() so each Supabase query runs in a worker thread —
    # the synchronous SDK would otherwise block the event loop and serialize what
    # looks like parallel gather() calls into sequential execution.
    w_all, fl_all, aq_all, alerts_result, log = await asyncio.gather(
        asyncio.to_thread(lambda: client.table("weather_data").select(
            "location_id,temperature,temperature_max,humidity,precipitation,wind_speed,observed_at"
        ).in_("location_id", loc_ids).not_.is_("temperature", "null").order(
            "observed_at", desc=True
        ).limit(500).execute()),
        asyncio.to_thread(lambda: client.table("flood_data").select(
            "location_id,river_discharge,flood_risk_level,observed_at"
        ).in_("location_id", loc_ids).order("observed_at", desc=True).limit(500).execute()),
        asyncio.to_thread(lambda: client.table("air_quality_data").select(
            "location_id,pm2_5,pm10,dust,aqi,observed_at"
        ).in_("location_id", loc_ids).order("observed_at", desc=True).limit(500).execute()),
        asyncio.to_thread(lambda: client.table("alerts").select("id", count="exact").eq(
            "is_active", True
        ).limit(0).execute()),
        asyncio.to_thread(lambda: client.table("collection_log").select(
            "id,source,data_type,status,records_inserted,duration_seconds,created_at"
        ).order("created_at", desc=True).limit(1).execute()),
    )

    w_by_loc = {}
    for r in (w_all.data or []):
        if r["location_id"] not in w_by_loc:
            w_by_loc[r["location_id"]] = r

    fl_by_loc = {}
    for r in (fl_all.data or []):
        if r["location_id"] not in fl_by_loc:
            fl_by_loc[r["location_id"]] = r

    aq_by_loc = {}
    for r in (aq_all.data or []):
        if r["location_id"] not in aq_by_loc:
            aq_by_loc[r["location_id"]] = r

    all_cities = [
        {
            "location": loc,
            "weather": w_by_loc.get(loc["id"]),
            "flood": fl_by_loc.get(loc["id"]),
        }
        for loc in all_locs
    ]
    top_cities = [
        {
            "location": loc,
            "weather": w_by_loc.get(loc["id"]),
            "air_quality": aq_by_loc.get(loc["id"]),
            "flood": fl_by_loc.get(loc["id"]),
        }
        for loc in all_locs
    ]

    dashboard = {
        "app": {
            "name": config.app_name,
            "version": config.app_version,
            "country": config.country.get("name"),
        },
        "all_cities": all_cities,
        "top_cities": top_cities,
        "total_cities": len(all_locs),
        "active_alerts_count": alerts_result.count or 0,
        "latest_collection": log.data[0] if log.data else None,
    }
    _cache.set(cache_key, dashboard, ttl=300)   # 5 min
    return dashboard


# ── Backward-compat redirects /api/<path> → /api/v1/<path> ───────────────────
# Must be declared AFTER all /api/v1/* routes so FastAPI matches those first.
# The guard prevents infinite redirect loops when the path already starts with v1/.
@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def legacy_redirect(request: Request, path: str):
    if path.startswith("v1"):
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not found")
    new_url = request.url.replace(path=f"/api/v1/{path}")
    return RedirectResponse(url=str(new_url), status_code=308)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
