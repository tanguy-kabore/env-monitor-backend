import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

from app.config import get_app_config, get_settings
from app.routers import locations, weather, floods, air_quality, drought, climate, alerts, system, report, export
from app.services.scheduler import setup_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def _pre_warm_cache():
    """Pre-populate expensive cache entries so first user request is fast."""
    import asyncio
    await asyncio.sleep(8)
    try:
        import httpx
        base = "http://localhost:8000"
        endpoints = [
            "/api/dashboard",
            "/api/weather/summary",
            "/api/air-quality/map",
            "/api/floods/risk-map",
            "/api/drought/map",
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


async def _warm_up_models():
    import asyncio
    await asyncio.sleep(5)  # wait for DB to be fully ready
    try:
        from app.database import get_supabase
        from app.services.ml_engine import MODEL_CACHE, train_all_models, generate_predictions
        client = get_supabase()
        r = client.table("ml_models").select("id", count="exact").eq("status", "active").limit(0).execute()
        has_models = (r.count or 0) > 0
        if has_models and not MODEL_CACHE:
            logger.info("Warm-up: retraining models to repopulate in-memory cache...")
            await train_all_models()
            logger.info("Warm-up complete — predictions regenerated")
        elif not has_models:
            logger.info("Warm-up: no active models in DB, skipping")
    except Exception as e:
        logger.warning(f"Warm-up failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = get_app_config()
    logger.info(f"Starting {config.app_name} v{config.app_version}")
    logger.info(f"Country: {config.country.get('name', 'Unknown')}")

    try:
        from app.database import check_connection
        if check_connection():
            logger.info("Database connection verified")
            setup_scheduler()
            import asyncio
            asyncio.ensure_future(_pre_warm_cache())
            asyncio.ensure_future(_warm_up_models())
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
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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


@app.get("/")
async def root():
    return {
        "name": config.app_name,
        "version": config.app_version,
        "description": config.get("app.description"),
        "country": config.country.get("name"),
        "docs": "/docs",
    }


@app.get("/api/dashboard")
async def get_dashboard():
    from app.database import get_supabase, get_system_config
    from app import cache as _cache

    cache_key = "dashboard:main"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()

    # Fetch ALL cities for map display
    locations_result = (
        client.table("locations")
        .select("id, external_id, name, latitude, longitude, type, population")
        .eq("type", "city")
        .eq("is_active", True)
        .order("population", desc=True)
        .limit(500)
        .execute()
    )
    all_locs = locations_result.data or []
    loc_ids = [loc["id"] for loc in all_locs]

    # Batch queries for all cities
    w_all = (
        client.table("weather_data")
        .select("location_id,temperature,temperature_max,humidity,precipitation,wind_speed,observed_at")
        .in_("location_id", loc_ids)
        .not_.is_("temperature", "null")
        .order("observed_at", desc=True)
        .limit(1000)
        .execute()
    )
    fl_all = (
        client.table("flood_data")
        .select("location_id,river_discharge,flood_risk_level,observed_at")
        .in_("location_id", loc_ids)
        .order("observed_at", desc=True)
        .limit(1000)
        .execute()
    )
    # Air quality for all cities
    aq_all = (
        client.table("air_quality_data")
        .select("location_id,pm2_5,pm10,dust,aqi,observed_at")
        .in_("location_id", loc_ids)
        .order("observed_at", desc=True)
        .limit(1000)
        .execute()
    )
    aq_by_loc = {}
    for r in (aq_all.data or []):
        if r["location_id"] not in aq_by_loc:
            aq_by_loc[r["location_id"]] = r

    def _latest(rows, lid):
        for r in rows:
            if r["location_id"] == lid:
                return r
        return None

    # Build latest weather/flood per location (for map)
    w_by_loc = {}
    for r in (w_all.data or []):
        if r["location_id"] not in w_by_loc:
            w_by_loc[r["location_id"]] = r
    fl_by_loc = {}
    for r in (fl_all.data or []):
        if r["location_id"] not in fl_by_loc:
            fl_by_loc[r["location_id"]] = r

    # All cities with weather+flood (for map)
    all_cities = [
        {
            "location": loc,
            "weather": w_by_loc.get(loc["id"]),
            "flood": fl_by_loc.get(loc["id"]),
        }
        for loc in all_locs
    ]
    # All cities with full data (for detail cards)
    top_cities = [
        {
            "location": loc,
            "weather": w_by_loc.get(loc["id"]),
            "air_quality": aq_by_loc.get(loc["id"]),
            "flood": fl_by_loc.get(loc["id"]),
        }
        for loc in all_locs
    ]

    alerts_result = (
        client.table("alerts")
        .select("id", count="exact")
        .eq("is_active", True)
        .limit(0)
        .execute()
    )
    log = (
        client.table("collection_log")
        .select("*")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )

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
    _cache.set(cache_key, dashboard, ttl=120)
    return dashboard


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
