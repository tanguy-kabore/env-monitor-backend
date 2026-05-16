from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase, resolve_location_uuid_async, db_exec
from app.services import open_meteo
from app import cache as _cache
from typing import Optional

router = APIRouter(prefix="/api/v1/air-quality", tags=["Air Quality"])


@router.get("/current/{location_id}")
async def get_current_air_quality(location_id: str):
    cache_key = f"aq:current:{location_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    result = await db_exec(lambda: client.table("air_quality_data")
        .select("id,location_id,observed_at,pm2_5,pm10,no2,so2,o3,co,dust,aqi,source")
        .eq("location_id", uuid)
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
        live = await open_meteo.fetch_air_quality(
            loc.data[0]["latitude"], loc.data[0]["longitude"]
        )
        response = {"data": live, "source": "live_api", "location": loc.data[0]["name"]}
        _cache.set(cache_key, response, ttl=300)
        return response

    response = {"data": result.data[0], "source": "database"}
    _cache.set(cache_key, response, ttl=300)
    return response


@router.get("/forecast/{location_id}")
async def get_air_quality_forecast(location_id: str):
    cache_key = f"aq:forecast:{location_id}"
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

    live = await open_meteo.fetch_air_quality_forecast(
        loc.data[0]["latitude"], loc.data[0]["longitude"]
    )
    response = {
        "data": live,
        "location": loc.data[0]["name"],
    }
    _cache.set(cache_key, response, ttl=14_400)
    return response


@router.get("/history/{location_id}")
async def get_air_quality_history(
    location_id: str,
    days: int = Query(30, ge=1, le=365),
):
    cache_key = f"aq:history:{location_id}:{days}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    from datetime import datetime, timedelta
    start = (datetime.utcnow() - timedelta(days=days)).isoformat()

    result = await db_exec(lambda: client.table("air_quality_data")
        .select("observed_at,pm2_5,pm10,dust,aqi,no2,so2,o3,co")
        .eq("location_id", uuid)
        .gte("observed_at", start)
        .order("observed_at", desc=False)
        .limit(days * 4 + 50)
        .execute())

    response = {"data": result.data, "count": len(result.data)}
    _cache.set(cache_key, response, ttl=7_200)
    return response


@router.get("/predictions/{location_id}")
async def get_air_quality_predictions(location_id: str):
    cache_key = f"aq:predictions:{location_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    from datetime import datetime
    now = datetime.utcnow().isoformat()

    result = await db_exec(lambda: client.table("air_quality_predictions")
        .select("id,location_id,target_date,predicted_at,aqi,pm2_5,pm10,dust,model_version")
        .eq("location_id", uuid)
        .gte("target_date", now)
        .order("target_date", desc=False)
        .limit(10)
        .execute())

    response = {"data": result.data, "count": len(result.data)}
    _cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/map")
async def get_air_quality_map():
    cache_key = "aq:map"
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

    all_aq = await db_exec(lambda: client.table("air_quality_data")
        .select("location_id,pm2_5,pm10,dust,aqi,observed_at")
        .in_("location_id", list(loc_by_id.keys()))
        .order("observed_at", desc=True)
        .limit(500)
        .execute())
    latest_by_loc: dict = {}
    for row in (all_aq.data or []):
        lid = row["location_id"]
        if lid not in latest_by_loc:
            latest_by_loc[lid] = row

    aq_data = [
        {"location": loc, "latest": latest_by_loc.get(loc["id"])}
        for loc in locations.data
    ]
    response = {"data": aq_data}
    _cache.set(cache_key, response, ttl=1_800)
    return response
