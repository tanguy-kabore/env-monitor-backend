import logging
import asyncio
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.config import get_app_config

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(
    # If the server was sleeping and missed a fire time by up to 1 hour, run it
    # immediately on wake-up instead of skipping. Critical for Render free tier.
    job_defaults={"misfire_grace_time": 3600},
)


def _delay(minutes: int):
    """Return a UTC-aware datetime offset from now, used to stagger first runs."""
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def _training_startup_delay() -> int:
    """Return startup delay (minutes) for the training job.
    If training last ran over 20 hours ago (or never), run it sooner (10 min).
    Otherwise use 120 min to avoid hammering on normal restarts."""
    try:
        from app.database import get_system_config
        last = get_system_config("last_model_training")
        if not last or last in ("never", "false", "null", ""):
            return 10
        from datetime import datetime, timezone
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        elapsed_h = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
        return 10 if elapsed_h >= 20 else 120
    except Exception:
        return 10


def setup_scheduler():
    config = get_app_config()
    dc = config.data_collection
    ml = config.ml_config

    from app.services.data_collector import (
        collect_current_weather,
        collect_air_quality,
        collect_flood_data,
        collect_climate_data,
        compute_drought_indicators,
    )
    from app.services.ml_engine import train_all_models, generate_predictions
    from app.routers.alerts import generate_alerts as _generate_alerts_endpoint
    from app.routers.alerts import archive_daily_alerts as _archive_daily_alerts
    from apscheduler.triggers.cron import CronTrigger

    # Stagger first runs so all jobs don't fire simultaneously at boot.
    # Offsets are chosen so heavier jobs start after lighter ones finish.
    scheduler.add_job(
        _run_async(collect_current_weather),
        IntervalTrigger(minutes=dc["weather"]["frequency_minutes"]),
        id="collect_weather",
        name="Collect current weather data",
        replace_existing=True,
        next_run_time=_delay(5),
    )

    scheduler.add_job(
        _run_async(collect_air_quality),
        IntervalTrigger(minutes=dc["air_quality"]["frequency_minutes"]),
        id="collect_air_quality",
        name="Collect air quality data",
        replace_existing=True,
        next_run_time=_delay(8),
    )

    scheduler.add_job(
        _run_async(collect_flood_data),
        IntervalTrigger(minutes=dc["flood"]["frequency_minutes"]),
        id="collect_flood",
        name="Collect flood data",
        replace_existing=True,
        next_run_time=_delay(11),
    )

    scheduler.add_job(
        _run_async(collect_climate_data),
        IntervalTrigger(hours=dc["climate"]["frequency_hours"]),
        id="collect_climate",
        name="Collect climate data",
        replace_existing=True,
        next_run_time=_delay(60),
    )

    scheduler.add_job(
        _run_async(compute_drought_indicators),
        IntervalTrigger(hours=6),
        id="compute_drought",
        name="Compute drought indicators",
        replace_existing=True,
        next_run_time=_delay(90),
    )

    retrain_hours = ml["training"]["retrain_frequency_hours"]
    train_delay = _training_startup_delay()
    scheduler.add_job(
        _run_ml_in_thread(train_all_models),
        IntervalTrigger(hours=retrain_hours),
        id="retrain_models",
        name="Retrain ML models",
        replace_existing=True,
        next_run_time=_delay(train_delay),
    )
    logger.info("retrain_models scheduled: first run in %d min", train_delay)

    scheduler.add_job(
        _run_ml_in_thread(generate_predictions),
        IntervalTrigger(hours=6),
        id="generate_predictions",
        name="Generate predictions",
        replace_existing=True,
        next_run_time=_delay(30),
    )

    # Alert generation — runs every 60 min, after weather + air quality have updated
    async def _auto_generate_alerts():
        try:
            result = await _generate_alerts_endpoint()
            logger.info(f"Auto alert generation: {result.get('created',0)} created, {result.get('resolved',0)} resolved")
        except Exception as e:
            logger.error(f"Auto alert generation failed: {e}")

    scheduler.add_job(
        _auto_generate_alerts,
        IntervalTrigger(minutes=dc["weather"]["frequency_minutes"]),
        id="generate_alerts",
        name="Auto-generate environmental alerts",
        replace_existing=True,
        next_run_time=_delay(15),
    )

    # Daily archive — 23:59 every day: snapshot persisting alerts for clean history
    async def _auto_archive_daily():
        try:
            result = await _archive_daily_alerts()
            logger.info(f"Daily archive: {result.get('archived',0)} archived, {result.get('reopened',0)} reopened")
        except Exception as e:
            logger.error(f"Daily archive failed: {e}")

    scheduler.add_job(
        _auto_archive_daily,
        CronTrigger(hour=23, minute=59),
        id="archive_daily_alerts",
        name="Archive daily alert snapshots",
        replace_existing=True,
    )

    # Cache warmer — runs every 3 h 30 min to refresh before 4-h TTLs expire
    from app.services.cache_warmer import warm_caches as _warm_caches

    async def _run_warm_caches():
        try:
            result = await _warm_caches()
            logger.info(f"Scheduled cache warm: {result}")
        except Exception as e:
            logger.error(f"Scheduled cache warm failed: {e}")

    scheduler.add_job(
        _run_warm_caches,
        IntervalTrigger(minutes=210),   # 3 h 30 min
        id="warm_caches",
        name="Proactive cache warmer",
        replace_existing=True,
        next_run_time=_delay(20),
    )

    # Keep-alive: ping own public URL every 10 min to prevent Render free-tier sleep.
    # Set SELF_BASE_URL=https://your-app.onrender.com in Render environment variables.
    import os as _os
    _self_url = _os.getenv("SELF_BASE_URL", "").rstrip("/")

    async def _keep_alive():
        if not _self_url:
            return
        try:
            import httpx
            async with httpx.AsyncClient(timeout=8) as _c:
                await _c.get(f"{_self_url}/api/v1/system/health")
        except Exception:
            pass

    scheduler.add_job(
        _keep_alive,
        IntervalTrigger(minutes=10),
        id="keep_alive",
        name="Keep-alive ping",
        replace_existing=True,
        next_run_time=_delay(2),
    )

    scheduler.start()
    logger.info("Scheduler started with periodic data collection, model training and alert generation")


def _run_async(coro_func):
    async def wrapper():
        try:
            result = await coro_func()
            logger.info(f"{coro_func.__name__} completed: {result}")
        except Exception as e:
            logger.error(f"{coro_func.__name__} failed: {e}")
    return wrapper


def _run_ml_in_thread(async_func):
    """Run an async ML function (which contains sync Supabase + sklearn calls) in a
    dedicated thread with its own event loop so it never blocks the server event loop."""
    def _sync_runner():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(async_func())
        finally:
            loop.close()

    async def wrapper():
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, _sync_runner)
            logger.info(f"{async_func.__name__} completed in thread: {result}")
        except Exception as e:
            logger.error(f"{async_func.__name__} failed: {e}")

    return wrapper


def _interval_label(trigger) -> str:
    """Convert a trigger to a human-readable French string."""
    trigger_str = str(trigger)
    # CronTrigger
    if hasattr(trigger, 'fields'):
        return f"Chaque jour à {trigger_str.split('(')[-1].rstrip(')')}" if 'cron' in trigger_str.lower() else trigger_str
    # IntervalTrigger
    try:
        td = trigger.interval
        total_seconds = int(td.total_seconds())
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60

        if days >= 1 and hours == 0 and minutes == 0:
            return "Quotidien (1× par jour)"
        if hours >= 1 and minutes == 0:
            return f"Toutes les {hours} h"
        if hours >= 1 and minutes > 0:
            return f"Toutes les {hours} h {minutes} min"
        return f"Toutes les {minutes} min"
    except Exception:
        return trigger_str


JOB_ICONS = {
    "collect_weather":       "🌤️",
    "collect_air_quality":   "💨",
    "collect_flood":         "💧",
    "collect_climate":       "🌍",
    "compute_drought":       "☀️",
    "retrain_models":        "🤖",
    "generate_predictions":  "📈",
    "generate_alerts":       "🔔",
    "archive_daily_alerts":  "🗄️",
    "keep_alive":            "💓",
}

JOB_DESC = {
    "collect_weather":       "Récupère température, pluie, vent depuis Open-Meteo",
    "collect_air_quality":   "Récupère PM2.5, PM10, AQI depuis Open-Meteo",
    "collect_flood":         "Récupère débits des rivières depuis Open-Meteo",
    "collect_climate":       "Récupère données climatiques NASA POWER",
    "compute_drought":       "Calcule les indices de sécheresse (SPI, anomalies)",
    "retrain_models":        "Réentraîne les modèles ML de prédiction",
    "generate_predictions":  "Génère les prévisions ML pour chaque ville",
    "generate_alerts":       "Analyse les données et crée/résout les alertes automatiquement",
    "archive_daily_alerts":  "Chaque soir à 23h59 : archive les alertes persistantes du jour",
    "keep_alive":            "Ping toutes les 10 min pour maintenir le serveur actif (Render)",
}


def get_scheduler_jobs():
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "icon": JOB_ICONS.get(job.id, "⚙️"),
            "description": JOB_DESC.get(job.id, ""),
            "interval_label": _interval_label(job.trigger),
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
            "paused": job.next_run_time is None,
        })
    return jobs


def get_job_details(job_id: str):
    """Get detailed info about a specific job including editable trigger params."""
    job = scheduler.get_job(job_id)
    if not job:
        return None
    
    trigger = job.trigger
    editable = {"minutes": None, "hours": None, "days": None, "cron": None}
    
    # Extract current interval values
    if hasattr(trigger, 'interval'):
        td = trigger.interval
        total_seconds = int(td.total_seconds())
        editable["days"] = total_seconds // 86400
        editable["hours"] = (total_seconds % 86400) // 3600
        editable["minutes"] = (total_seconds % 3600) // 60
    elif hasattr(trigger, 'fields'):
        # Cron trigger - extract hour/minute
        fields = trigger.fields
        editable["cron"] = {
            "hour": str(fields[5]) if len(fields) > 5 else "*",
            "minute": str(fields[0]) if len(fields) > 0 else "*",
        }
    
    return {
        "id": job.id,
        "name": job.name,
        "description": JOB_DESC.get(job.id, ""),
        "editable": editable,
        "paused": job.next_run_time is None,
        "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
    }


def update_job_interval(job_id: str, minutes: int = None, hours: int = None, days: int = None, cron: dict = None):
    """Update a job's trigger interval."""
    job = scheduler.get_job(job_id)
    if not job:
        return False
    
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.triggers.cron import CronTrigger
    
    if cron:
        # Update to cron trigger
        new_trigger = CronTrigger(hour=cron.get("hour", 0), minute=cron.get("minute", 0))
    elif days or hours or minutes:
        # Update interval
        total_seconds = (days or 0) * 86400 + (hours or 0) * 3600 + (minutes or 0) * 60
        if total_seconds < 60:
            total_seconds = 60  # Minimum 1 minute
        new_trigger = IntervalTrigger(seconds=total_seconds)
    else:
        return False
    
    scheduler.reschedule_job(job_id, trigger=new_trigger)
    logger.info(f"Updated schedule for job {job_id}: {new_trigger}")
    return True


def pause_job(job_id: str):
    """Pause a scheduled job."""
    job = scheduler.get_job(job_id)
    if not job:
        return False
    scheduler.pause_job(job_id)
    logger.info(f"Paused job {job_id}")
    return True


def resume_job(job_id: str):
    """Resume a paused job."""
    job = scheduler.get_job(job_id)
    if not job:
        return False
    scheduler.resume_job(job_id)
    logger.info(f"Resumed job {job_id}")
    return True


def run_job_now(job_id: str):
    """Execute a job immediately (doesn't wait for next scheduled run)."""
    job = scheduler.get_job(job_id)
    if not job:
        return False
    
    # Get the job's function and run it
    if hasattr(job, 'func') and job.func:
        try:
            if asyncio.iscoroutinefunction(job.func):
                # Schedule immediate execution
                asyncio.create_task(job.func())
            else:
                job.func()
            logger.info(f"Manually triggered job {job_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to run job {job_id}: {e}")
            return False
    return False
