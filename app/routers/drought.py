from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase, resolve_location_uuid
from app import cache as _cache

router = APIRouter(prefix="/api/drought", tags=["Drought"])


@router.get("/current/{location_id}")
async def get_current_drought(location_id: str):
    client = get_supabase()
    uuid = resolve_location_uuid(location_id)

    result = (
        client.table("drought_data")
        .select("*")
        .eq("location_id", uuid)
        .order("observed_at", desc=True)
        .limit(1)
        .execute()
    )

    if not result.data:
        raise HTTPException(status_code=404, detail="No drought data available for this location")

    return {"data": result.data[0]}


@router.get("/history/{location_id}")
async def get_drought_history(
    location_id: str,
    days: int = Query(90, ge=1, le=730),
):
    client = get_supabase()
    uuid = resolve_location_uuid(location_id)

    from datetime import datetime, timedelta
    start = (datetime.utcnow() - timedelta(days=days)).isoformat()

    result = (
        client.table("drought_data")
        .select("observed_at,precipitation_30d,precipitation_90d,spi_value,drought_level,evapotranspiration")
        .eq("location_id", uuid)
        .gte("observed_at", start)
        .order("observed_at", desc=False)
        .execute()
    )

    return {"data": result.data, "count": len(result.data)}


@router.get("/predictions/{location_id}")
async def get_drought_predictions(location_id: str):
    client = get_supabase()
    uuid = resolve_location_uuid(location_id)

    from datetime import datetime
    now = datetime.utcnow().isoformat()

    result = (
        client.table("drought_predictions")
        .select("*")
        .eq("location_id", uuid)
        .gte("target_date", now)
        .order("target_date", desc=False)
        .limit(10)
        .execute()
    )

    return {"data": result.data, "count": len(result.data)}


@router.get("/map")
async def get_drought_map():
    cache_key = "drought:map"
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

    all_drought = (
        client.table("drought_data")
        .select("location_id,spi_value,drought_level,precipitation_30d,observed_at")
        .in_("location_id", list(loc_by_id.keys()))
        .order("observed_at", desc=True)
        .limit(500)
        .execute()
    )
    latest_by_loc: dict = {}
    for row in (all_drought.data or []):
        lid = row["location_id"]
        if lid not in latest_by_loc:
            latest_by_loc[lid] = row

    drought_data = [
        {"location": loc, "latest": latest_by_loc.get(loc["id"])}
        for loc in locations.data
    ]
    response = {"data": drought_data}
    _cache.set(cache_key, response, ttl=180)
    return response
