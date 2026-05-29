from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from app.database import get_supabase, get_system_config, set_system_config, check_connection, db_exec
from app.config import get_app_config
from app.services.data_collector import (
    initialize_locations,
    collect_current_weather,
    collect_historical_weather,
    collect_air_quality,
    collect_flood_data,
    collect_climate_data,
    compute_drought_indicators,
)
from app.services.ml_engine import train_all_models, generate_predictions
from app.services.scheduler import get_scheduler_jobs, scheduler, get_job_details, update_job_interval, pause_job, resume_job, run_job_now

router = APIRouter(prefix="/api/v1/system", tags=["System"])


@router.get("/health")
async def health_check():
    db_ok = check_connection()
    config = get_app_config()
    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "disconnected",
        "app_name": config.app_name,
        "version": config.app_version,
    }


@router.get("/config")
async def get_config():
    config = get_app_config()
    return {
        "app": config.get("app"),
        "country": config.country,
        "theme": config.get("app.theme"),
        "data_collection": {
            "weather_frequency_min": config.data_collection["weather"]["frequency_minutes"],
            "air_quality_frequency_min": config.data_collection["air_quality"]["frequency_minutes"],
            "flood_frequency_min": config.data_collection["flood"]["frequency_minutes"],
        },
        "ml": {
            "retrain_frequency_hours": config.ml_config["training"]["retrain_frequency_hours"],
        },
        "alert_thresholds": config.alert_thresholds,
    }


@router.get("/status")
async def get_system_status():
    import asyncio
    from app import cache as _cache

    # Serve from cache (60s) so cold-start Supabase latency never hits the client
    cache_key = "system:status"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    def _cfg(key):
        val = get_system_config(key)
        if not val or val in ("never", "false", "null", ""):
            return None
        return val.strip('"')

    initialized_val = get_system_config("app_initialized")
    initialized = initialized_val == "true"
    last_hist = _cfg("last_historical_load")
    last_train = _cfg("last_model_training")
    jobs = get_scheduler_jobs()

    client = get_supabase()
    tables = ["weather_data", "flood_data", "air_quality_data", "drought_data", "climate_data",
              "weather_predictions", "flood_predictions", "air_quality_predictions"]

    def _count(table: str):
        try:
            r = client.table(table).select("id", count="exact").limit(0).execute()
            return table, r.count if r.count is not None else 0
        except Exception:
            return table, 0

    pairs = await asyncio.gather(*[
        asyncio.to_thread(_count, t) for t in tables
    ])
    counts = dict(pairs)

    result = {
        "initialized": initialized,
        "last_historical_load": last_hist,
        "last_model_training": last_train,
        "data_counts": counts,
        "scheduled_jobs": jobs,
    }
    _cache.set(cache_key, result, ttl=60)
    return result


def _invalidate_status_cache():
    from app import cache as _cache
    _cache.delete("system:status")


@router.post("/reset-status")
async def reset_system_status():
    """Force-update initialized status based on actual data in DB."""
    from datetime import datetime
    client = get_supabase()
    try:
        r = client.table("weather_data").select("id", count="exact").limit(0).execute()
        has_data = (r.count or 0) > 0
    except:
        has_data = False
    if has_data:
        set_system_config("app_initialized", True)
        last_hist = get_system_config("last_historical_load")
        if not last_hist or last_hist in ("never", "false", "null", ""):
            set_system_config("last_historical_load", datetime.utcnow().isoformat())
        last_train = get_system_config("last_model_training")
        if not last_train or last_train in ("never", "false", "null", ""):
            r2 = client.table("ml_models").select("trained_at").order("trained_at", desc=True).limit(1).execute()
            if r2.data:
                set_system_config("last_model_training", r2.data[0]["trained_at"])
    _invalidate_status_cache()
    return {"initialized": has_data, "message": "Status updated from actual DB counts"}


@router.post("/initialize")
async def initialize_system(background_tasks: BackgroundTasks):
    initialized = get_system_config("app_initialized")
    if initialized == "true":
        return {"message": "System already initialized", "status": "skipped"}

    _invalidate_status_cache()
    background_tasks.add_task(_run_initialization)
    return {"message": "Initialization started in background", "status": "started"}


@router.post("/collect/weather")
async def trigger_weather_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_async, collect_current_weather)
    return {"message": "Weather collection triggered"}


@router.post("/collect/air-quality")
async def trigger_air_quality_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_async, collect_air_quality)
    return {"message": "Air quality collection triggered"}


@router.post("/collect/flood")
async def trigger_flood_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_async, collect_flood_data)
    return {"message": "Flood data collection triggered"}


@router.post("/collect/climate")
async def trigger_climate_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_async, collect_climate_data)
    return {"message": "Climate data collection triggered"}


@router.post("/collect/drought")
async def trigger_drought_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_async, compute_drought_indicators)
    return {"message": "Drought indicators computation triggered"}


@router.post("/collect/all")
async def trigger_all_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_all_collection)
    return {"message": "Full data collection triggered"}


@router.post("/train")
async def trigger_training(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_async, train_all_models)
    return {"message": "Model training triggered"}


@router.post("/full-cycle")
async def trigger_full_cycle(background_tasks: BackgroundTasks):
    """Collect all → train models → generate alerts (sequential background task)."""
    async def _full_cycle():
        import logging as _log
        _logger = _log.getLogger(__name__)
        try:
            from app.routers.alerts import generate_alerts as _generate_alerts
            _logger.info("Full cycle [1/3]: collecting data...")
            await _run_all_collection()
            _logger.info("Full cycle [2/3]: training models...")
            await train_all_models()
            _logger.info("Full cycle [3/3]: generating alerts...")
            await _generate_alerts()
            _logger.info("Full cycle: complete")
        except Exception as e:
            _log.getLogger(__name__).error("Full cycle failed: %s", e, exc_info=True)

    background_tasks.add_task(_full_cycle)
    return {"message": "Cycle complet démarré (collecte → entraînement → alertes)"}


@router.get("/collection-log")
async def get_collection_log(
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: str = Query(None),
):
    client = get_supabase()

    def _fetch():
        q = client.table("collection_log").select("*", count="exact").order("created_at", desc=True)
        if status:
            q = q.eq("status", status)
        return q.range(offset, offset + limit - 1).execute()

    result = await db_exec(_fetch)
    total = result.count if result.count is not None else len(result.data)
    return {
        "data": result.data,
        "pagination": {"total": total, "limit": limit, "offset": offset, "has_more": offset + limit < total},
    }


@router.get("/cache-stats")
async def get_cache_stats():
    """Return in-memory cache statistics (hits, misses, entries, tags)."""
    from app import cache as _cache
    return _cache.stats()


@router.delete("/cache")
async def clear_cache():
    """Flush the entire in-memory cache."""
    from app import cache as _cache
    _cache.clear()
    return {"message": "Cache vidé."}


@router.post("/collection-log/cleanup")
async def cleanup_partial_logs():
    """
    For each partial/failed collection log entry, delete all data rows
    that were inserted during that run window (observed_at between
    created_at - duration_seconds and created_at), then mark the log entry
    as 'cleaned'.
    """
    from datetime import timedelta
    import logging as _log
    _logger = _log.getLogger(__name__)

    DATA_TYPE_TABLE = {
        "weather":            "weather_data",
        "weather_historical": "weather_data",
        "air_quality":        "air_quality_data",
        "flood":              "flood_data",
        "climate":            "climate_data",
    }

    client = get_supabase()

    logs_res = (
        client.table("collection_log")
        .select("*")
        .in_("status", ["partial", "failed"])
        .execute()
    )
    logs = logs_res.data or []

    if not logs:
        return {"message": "Aucun enregistrement partiel ou échoué trouvé.", "deleted": 0, "cleaned_logs": 0}

    total_deleted = 0
    cleaned_log_ids = []

    for entry in logs:
        data_type = entry.get("data_type", "")
        table = DATA_TYPE_TABLE.get(data_type)
        if not table:
            _logger.warning(f"Unknown data_type '{data_type}' — skipping log {entry['id']}")
            continue

        created_at = entry.get("created_at")
        duration_s = entry.get("duration_seconds") or 120
        if not created_at:
            continue

        from datetime import datetime
        try:
            end_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except Exception:
            continue
        start_dt = end_dt - timedelta(seconds=float(duration_s) + 60)

        try:
            del_res = (
                client.table(table)
                .delete()
                .gte("observed_at", start_dt.isoformat())
                .lte("observed_at", end_dt.isoformat())
                .execute()
            )
            deleted = len(del_res.data) if del_res.data else 0
            total_deleted += deleted
            _logger.info(f"Deleted {deleted} rows from {table} for log {entry['id']}")
        except Exception as e:
            _logger.warning(f"Could not delete from {table}: {e}")
            continue

        cleaned_log_ids.append(entry["id"])

    if cleaned_log_ids:
        client.table("collection_log").delete().in_("id", cleaned_log_ids).execute()

    return {
        "message": f"{total_deleted} enregistrements supprimés, {len(cleaned_log_ids)} entrées de log nettoyées.",
        "deleted": total_deleted,
        "cleaned_logs": len(cleaned_log_ids),
    }


@router.get("/models")
async def get_ml_models(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    status: str = Query("active"),
    model_type: str = Query(None),
):
    client = get_supabase()

    def _fetch():
        q = (client.table("ml_models")
            .select("*", count="exact")
            .eq("status", status)
            .order("model_type")
            .order("trained_at", desc=True))
        if model_type:
            q = q.eq("model_type", model_type)
        return q.range(offset, offset + limit - 1).execute()

    result = await db_exec(_fetch)
    total = result.count if result.count is not None else len(result.data)
    return {
        "data": result.data,
        "pagination": {"total": total, "limit": limit, "offset": offset, "has_more": offset + limit < total},
    }


@router.post("/models/cleanup")
async def cleanup_ml_models():
    """Keep only the best model (highest r2) per (model_type, location_id). Archive the rest."""
    import logging as _log
    _logger = _log.getLogger(__name__)
    client = get_supabase()

    # Paginate to fetch all active models (Supabase max 1000/page)
    all_rows = []
    offset = 0
    page_size = 1000
    while True:
        page = (
            client.table("ml_models")
            .select("id,model_type,location_id,metrics,trained_at")
            .eq("status", "active")
            .order("trained_at", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch_data = page.data or []
        all_rows.extend(batch_data)
        if len(batch_data) < page_size:
            break
        offset += page_size

    _logger.info(f"Cleanup: fetched {len(all_rows)} active models")

    from collections import defaultdict
    from fastapi import HTTPException as _HTTPException
    try:
        groups: dict = defaultdict(list)
        for m in all_rows:
            key = (m["model_type"], m["location_id"])
            groups[key].append(m)

        def _r2(m):
            return (m.get("metrics") or {}).get("r2") or -999

        to_archive = []
        for _key, _models in groups.items():
            if len(_models) <= 1:
                continue
            best = max(_models, key=_r2)
            to_archive.extend(m["id"] for m in _models if m["id"] != best["id"])

        archived = 0
        page_size_upd = 100
        for i in range(0, len(to_archive), page_size_upd):
            chunk = to_archive[i:i + page_size_upd]
            client.table("ml_models").update({"status": "retired"}).in_("id", chunk).execute()
            archived += len(chunk)

        from app import cache as _cache
        _cache.delete("ml:models")

        return {
            "groups_processed": len(groups),
            "duplicates_archived": archived,
            "active_remaining": len(all_rows) - archived,
        }
    except Exception as exc:
        _logger.exception(f"Cleanup failed: {exc}")
        raise _HTTPException(status_code=500, detail=str(exc))


@router.post("/reset-all")
async def reset_all_data():
    """
    DANGER — delete ALL collected data, ML models, alerts and collection logs.
    Resets system config so the app is back to uninitialized state.
    """
    import logging as _log
    _logger = _log.getLogger(__name__)
    client = get_supabase()

    TABLES = [
        "weather_data",
        "air_quality_data",
        "flood_data",
        "climate_data",
        "drought_indicators",
        "ml_models",
        "ml_predictions",
        "alerts",
        "collection_log",
    ]

    deleted: dict = {}
    errors: list = []

    for table in TABLES:
        try:
            # Delete all rows — Supabase requires a filter; use gte on a common field
            # We use a broad filter that matches every row
            res = client.table(table).delete().gte("id", "00000000-0000-0000-0000-000000000000").execute()
            deleted[table] = len(res.data) if res.data else 0
            _logger.info(f"reset-all: deleted {deleted[table]} rows from {table}")
        except Exception as e:
            errors.append(f"{table}: {e}")
            _logger.warning(f"reset-all: could not delete from {table}: {e}")

    # Reset system config flags
    set_system_config("app_initialized", False)
    set_system_config("last_historical_load", "never")
    set_system_config("last_model_training", "never")

    # Clear ML model cache if present
    try:
        from app import cache as _cache
        _cache.delete("ml:models")
    except Exception:
        pass

    return {
        "success": len(errors) == 0,
        "deleted": deleted,
        "errors": errors,
        "message": "Base de données réinitialisée. Relancez l'initialisation pour recollecterles données.",
    }


async def _run_initialization():
    import logging
    from datetime import datetime
    logger = logging.getLogger(__name__)
    logger.info("Starting system initialization...")

    def _step(name, coro):
        import asyncio
        async def run():
            try:
                result = await coro
                logger.info(f"{name}: OK -> {result}")
                return result
            except Exception as e:
                logger.error(f"{name} FAILED: {e}")
                return None
        return run()

    set_system_config("app_initialized", True)
    logger.info("Initialization marked as started")

    await _step("Locations", initialize_locations())
    hist = await _step("Historical weather", collect_historical_weather())
    if hist is not None:
        set_system_config("last_historical_load", datetime.utcnow().isoformat())
    await _step("Current weather", collect_current_weather())
    await _step("Air quality", collect_air_quality())
    await _step("Flood data", collect_flood_data())
    await _step("Climate data", collect_climate_data())
    await _step("Drought indicators", compute_drought_indicators())
    await _step("ML training", train_all_models())

    logger.info("System initialization complete!")


async def _run_all_collection():
    await collect_current_weather()
    await collect_air_quality()
    await collect_flood_data()
    await compute_drought_indicators()
    await collect_climate_data()


async def _run_async(func):
    await func()


# ── Scheduler Control Endpoints ───────────────────────────────────────────────

from pydantic import BaseModel
from typing import Optional

class JobIntervalUpdate(BaseModel):
    minutes: Optional[int] = None
    hours: Optional[int] = None
    days: Optional[int] = None
    cron: Optional[dict] = None


@router.get("/scheduler/jobs/{job_id}")
async def get_job_detail(job_id: str):
    """Get detailed info about a specific scheduled job."""
    from app.services.scheduler import get_job_details
    details = get_job_details(job_id)
    if not details:
        raise HTTPException(status_code=404, detail="Job not found")
    return details


@router.post("/scheduler/jobs/{job_id}/run")
async def run_job_manual(job_id: str, background_tasks: BackgroundTasks):
    """Manually trigger a scheduled job to run now."""
    from app.services.scheduler import run_job_now
    success = run_job_now(job_id)
    if not success:
        raise HTTPException(status_code=404, detail="Job not found or failed to run")
    return {"success": True, "message": f"Job {job_id} triggered"}


@router.post("/scheduler/jobs/{job_id}/pause")
async def pause_scheduled_job(job_id: str):
    """Pause a scheduled job."""
    from app.services.scheduler import pause_job
    success = pause_job(job_id)
    if not success:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"success": True, "message": f"Job {job_id} paused"}


@router.post("/scheduler/jobs/{job_id}/resume")
async def resume_scheduled_job(job_id: str):
    """Resume a paused job."""
    from app.services.scheduler import resume_job
    success = resume_job(job_id)
    if not success:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"success": True, "message": f"Job {job_id} resumed"}


@router.put("/scheduler/jobs/{job_id}/interval")
async def update_job_schedule(job_id: str, update: JobIntervalUpdate):
    """Update a job's schedule interval."""
    from app.services.scheduler import update_job_interval
    success = update_job_interval(
        job_id,
        minutes=update.minutes,
        hours=update.hours,
        days=update.days,
        cron=update.cron
    )
    if not success:
        raise HTTPException(status_code=400, detail="Invalid parameters or job not found")
    return {"success": True, "message": f"Job {job_id} schedule updated"}
