from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase, resolve_location_uuid
from app.services import open_meteo
from app import cache as _cache
from typing import Optional

router = APIRouter(prefix="/api/air-quality", tags=["Air Quality"])


@router.get("/current/{location_id}")
async def get_current_air_quality(location_id: str):
    cache_key = f"aq:current:{location_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    uuid = resolve_location_uuid(location_id)

    result = (
        client.table("air_quality_data")
        .select("*")
        .eq("location_id", uuid)
        .order("observed_at", desc=True)
        .limit(1)
        .execute()
    )

    if not result.data:
        loc = (
            client.table("locations")
            .select("latitude, longitude, name")
            .eq("id", uuid)
            .execute()
        )
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
    client = get_supabase()
    uuid = resolve_location_uuid(location_id)

    loc = (
        client.table("locations")
        .select("latitude, longitude, name")
        .eq("id", uuid)
        .execute()
    )
    if not loc.data:
        raise HTTPException(status_code=404, detail="Location not found")

    live = await open_meteo.fetch_air_quality_forecast(
        loc.data[0]["latitude"], loc.data[0]["longitude"]
    )
    return {
        "data": live,
        "location": loc.data[0]["name"],
    }


@router.get("/history/{location_id}")
async def get_air_quality_history(
    location_id: str,
    days: int = Query(30, ge=1, le=365),
):
    client = get_supabase()
    uuid = resolve_location_uuid(location_id)

    from datetime import datetime, timedelta
    start = (datetime.utcnow() - timedelta(days=days)).isoformat()

    result = (
        client.table("air_quality_data")
        .select("observed_at,pm2_5,pm10,dust,aqi,no2,so2,o3,co")
        .eq("location_id", uuid)
        .gte("observed_at", start)
        .order("observed_at", desc=False)
        .execute()
    )

    return {"data": result.data, "count": len(result.data)}


@router.get("/predictions/{location_id}")
async def get_air_quality_predictions(location_id: str):
    client = get_supabase()
    uuid = resolve_location_uuid(location_id)

    from datetime import datetime
    now = datetime.utcnow().isoformat()

    result = (
        client.table("air_quality_predictions")
        .select("*")
        .eq("location_id", uuid)
        .gte("target_date", now)
        .order("target_date", desc=False)
        .limit(10)
        .execute()
    )

    return {"data": result.data, "count": len(result.data)}


@router.get("/map")
async def get_air_quality_map():
    cache_key = "aq:map"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    locations = (
        client.table("locations")
        .select("id, external_id, name, latitude, longitude")
        .eq("type", "city")
        .order("name")
        .execute()
    )
    loc_by_id = {loc["id"]: loc for loc in locations.data}

    # Single batch query — fetch latest per location by ordering + deduplicating in Python
    all_aq = (
        client.table("air_quality_data")
        .select("location_id,pm2_5,pm10,dust,aqi,observed_at")
        .in_("location_id", list(loc_by_id.keys()))
        .order("observed_at", desc=True)
        .limit(500)
        .execute()
    )
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
    _cache.set(cache_key, response, ttl=180)
    return response
