import httpx
import logging
from datetime import datetime, timedelta
from typing import Optional
from app.config import get_app_config

logger = logging.getLogger(__name__)

TIMEOUT = 15.0


async def fetch_current_weather(latitude: float, longitude: float) -> dict:
    config = get_app_config()
    url = config.apis["open_meteo"]["weather_forecast"]
    variables = ",".join(config.data_collection["weather"]["current_variables"])
    daily_vars = ",".join(config.data_collection["weather"]["daily_variables"])
    forecast_days = config.data_collection["weather"]["forecast_days"]
    tz = config.country.get("timezone", "GMT")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": variables,
        "daily": daily_vars,
        "forecast_days": forecast_days,
        "timezone": tz,
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


async def fetch_historical_weather(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
) -> dict:
    import asyncio
    config = get_app_config()
    url = config.apis["open_meteo"]["weather_archive"]
    variables = ",".join(config.data_collection["weather"]["archive_variables"])
    tz = config.country.get("timezone", "GMT")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": variables,
        "timezone": tz,
    }

    for attempt in range(5):
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 429:
                wait = 2 ** attempt * 5
                logger.warning(f"Rate limited by Open-Meteo archive, retrying in {wait}s (attempt {attempt+1})")
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
    raise Exception("Open-Meteo archive API rate limit exceeded after 5 retries")


async def fetch_air_quality(latitude: float, longitude: float) -> dict:
    import asyncio
    config = get_app_config()
    url = config.apis["open_meteo"]["air_quality"]
    variables = ",".join(config.data_collection["air_quality"]["variables"])

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": variables,
    }

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                resp = await client.get(url, params=params)
                if resp.status_code == 429:
                    wait = 2 ** attempt * 5
                    logger.warning(f"AQ rate limited, retrying in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
        except httpx.TimeoutException:
            logger.warning(f"AQ fetch timeout (attempt {attempt+1})")
            if attempt < 2:
                await asyncio.sleep(3)
    raise Exception("Air quality API failed after 3 attempts")


async def fetch_air_quality_forecast(latitude: float, longitude: float) -> dict:
    config = get_app_config()
    url = config.apis["open_meteo"]["air_quality"]

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "pm10,pm2_5,dust,european_aqi",
        "forecast_days": 5,
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


async def fetch_flood_data(latitude: float, longitude: float) -> dict:
    config = get_app_config()
    url = config.apis["open_meteo"]["flood"]
    forecast_days = config.data_collection["flood"]["forecast_days"]

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "river_discharge",
        "forecast_days": forecast_days,
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


async def fetch_flood_history(latitude: float, longitude: float, past_days: int = 90) -> dict:
    config = get_app_config()
    url = config.apis["open_meteo"]["flood"]

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "river_discharge",
        "past_days": past_days,
        "forecast_days": 0,
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


async def fetch_historical_air_quality(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
) -> dict:
    config = get_app_config()
    url = config.apis["open_meteo"]["air_quality"]

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "pm10,pm2_5,dust,european_aqi,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone",
        "start_date": start_date,
        "end_date": end_date,
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
