import httpx
import logging
from app.config import get_app_config

logger = logging.getLogger(__name__)

TIMEOUT = 60.0


async def fetch_monthly_climate(
    latitude: float,
    longitude: float,
    start_year: int,
    end_year: int,
) -> dict:
    config = get_app_config()
    base_url = config.apis["nasa_power"]["base_url"]
    params_list = ",".join(config.data_collection["climate"]["nasa_parameters"])

    url = f"{base_url}/monthly/point"
    params = {
        "parameters": params_list,
        "community": "RE",
        "longitude": longitude,
        "latitude": latitude,
        "start": start_year,
        "end": end_year,
        "format": "JSON",
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


async def fetch_daily_climate(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
) -> dict:
    config = get_app_config()
    base_url = config.apis["nasa_power"]["base_url"]
    params_list = ",".join(config.data_collection["climate"]["nasa_parameters"])

    url = f"{base_url}/daily/point"
    start_fmt = start_date.replace("-", "")
    end_fmt = end_date.replace("-", "")

    params = {
        "parameters": params_list,
        "community": "RE",
        "longitude": longitude,
        "latitude": latitude,
        "start": start_fmt,
        "end": end_fmt,
        "format": "JSON",
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
