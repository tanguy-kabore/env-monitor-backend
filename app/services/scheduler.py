import logging
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.config import get_app_config

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


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

    scheduler.add_job(
        _run_async(collect_current_weather),
        IntervalTrigger(minutes=dc["weather"]["frequency_minutes"]),
        id="collect_weather",
        name="Collect current weather data",
        replace_existing=True,
    )

    scheduler.add_job(
        _run_async(collect_air_quality),
        IntervalTrigger(minutes=dc["air_quality"]["frequency_minutes"]),
        id="collect_air_quality",
        name="Collect air quality data",
        replace_existing=True,
    )

    scheduler.add_job(
        _run_async(collect_flood_data),
        IntervalTrigger(minutes=dc["flood"]["frequency_minutes"]),
        id="collect_flood",
        name="Collect flood data",
        replace_existing=True,
    )

    scheduler.add_job(
        _run_async(collect_climate_data),
        IntervalTrigger(hours=dc["climate"]["frequency_hours"]),
        id="collect_climate",
        name="Collect climate data",
        replace_existing=True,
    )

    scheduler.add_job(
        _run_async(compute_drought_indicators),
        IntervalTrigger(hours=6),
        id="compute_drought",
        name="Compute drought indicators",
        replace_existing=True,
    )

    retrain_hours = ml["training"]["retrain_frequency_hours"]
    scheduler.add_job(
        _run_async(train_all_models),
        IntervalTrigger(hours=retrain_hours),
        id="retrain_models",
        name="Retrain ML models",
        replace_existing=True,
    )

    scheduler.add_job(
        _run_async(generate_predictions),
        IntervalTrigger(hours=6),
        id="generate_predictions",
        name="Generate predictions",
        replace_existing=True,
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
        })
    return jobs
