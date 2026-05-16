from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase, resolve_location_uuid_async, db_exec
from app.services import open_meteo
from app import cache as _cache
from typing import Optional

router = APIRouter(prefix="/api/v1/weather", tags=["Weather"])


@router.get("/current/{location_id}")
async def get_current_weather(location_id: str):
    cache_key = f"weather:current:{location_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    result = await db_exec(lambda: client.table("weather_data")
        .select("id,location_id,observed_at,temperature,temperature_max,temperature_min,temperature_mean,humidity,precipitation,wind_speed,wind_direction,pressure,cloud_cover,uv_index,evapotranspiration,source")
        .eq("location_id", uuid)
        .not_.is_("temperature", "null")
        .order("observed_at", desc=True)
        .limit(1)
        .execute())

    if not result.data:
        loc = await db_exec(lambda: client.table("locations")
            .select("latitude, longitude, name")
            .eq("id", uuid)
            .limit(1)
            .execute())
        if not loc.data:
            raise HTTPException(status_code=404, detail="Location not found")
        live = await open_meteo.fetch_current_weather(
            loc.data[0]["latitude"], loc.data[0]["longitude"]
        )
        response = {"data": live, "source": "live_api", "location": loc.data[0]["name"]}
        _cache.set(cache_key, response, ttl=300)
        return response

    response = {"data": result.data[0], "source": "database"}
    _cache.set(cache_key, response, ttl=300)
    return response


@router.get("/forecast/{location_id}")
async def get_weather_forecast(location_id: str, days: int = Query(7, ge=1, le=16)):
    cache_key = f"weather:forecast:{location_id}:{days}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    loc = await db_exec(lambda: client.table("locations")
        .select("latitude, longitude, name")
        .eq("id", uuid)
        .limit(1)
        .execute())
    if not loc.data:
        raise HTTPException(status_code=404, detail="Location not found")

    live = await open_meteo.fetch_current_weather(
        loc.data[0]["latitude"], loc.data[0]["longitude"]
    )
    response = {
        "data": live,
        "location": loc.data[0]["name"],
        "coordinates": {
            "latitude": loc.data[0]["latitude"],
            "longitude": loc.data[0]["longitude"],
        },
    }
    _cache.set(cache_key, response, ttl=14_400)
    return response


@router.get("/history/{location_id}")
async def get_weather_history(
    location_id: str,
    days: int = Query(30, ge=1, le=365),
):
    cache_key = f"weather:history:{location_id}:{days}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    from datetime import datetime, timedelta
    start = (datetime.utcnow() - timedelta(days=days)).isoformat()

    result = await db_exec(lambda: client.table("weather_data")
        .select("observed_at,temperature_max,temperature_min,temperature_mean,humidity,precipitation,wind_speed")
        .eq("location_id", uuid)
        .gte("observed_at", start)
        .not_.is_("temperature_max", "null")
        .order("observed_at", desc=False)
        .limit(days * 4 + 50)
        .execute())

    response = {"data": result.data, "count": len(result.data)}
    _cache.set(cache_key, response, ttl=7_200)
    return response


@router.get("/predictions/{location_id}")
async def get_weather_predictions(location_id: str):
    cache_key = f"weather:predictions:{location_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    from datetime import datetime
    now = datetime.utcnow().isoformat()

    result = await db_exec(lambda: client.table("weather_predictions")
        .select("id,location_id,target_date,predicted_at,temperature_max,temperature_min,temperature_mean,humidity,precipitation,wind_speed,model_version,confidence")
        .eq("location_id", uuid)
        .gte("target_date", now)
        .order("target_date", desc=False)
        .limit(30)
        .execute())

    response = {"data": result.data, "count": len(result.data)}
    _cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/summary")
async def get_weather_summary():
    cache_key = "weather:summary"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    locations = await db_exec(lambda: client.table("locations")
        .select("id, external_id, name, latitude, longitude")
        .eq("type", "city")
        .order("name")
        .execute())
    loc_by_id = {loc["id"]: loc for loc in locations.data}

    all_w = await db_exec(lambda: client.table("weather_data")
        .select("location_id,temperature,temperature_max,humidity,precipitation,wind_speed,observed_at")
        .in_("location_id", list(loc_by_id.keys()))
        .not_.is_("temperature_max", "null")
        .order("observed_at", desc=True)
        .limit(500)
        .execute())
    latest_by_loc: dict = {}
    for row in (all_w.data or []):
        lid = row["location_id"]
        if lid not in latest_by_loc:
            if row.get("temperature") is None and row.get("temperature_max") is not None:
                row["temperature"] = row["temperature_max"]
            latest_by_loc[lid] = row

    summaries = [
        {"location": loc, "current": latest_by_loc.get(loc["id"])}
        for loc in locations.data
    ]
    response = {"data": summaries}
    _cache.set(cache_key, response, ttl=1_800)
    return response
