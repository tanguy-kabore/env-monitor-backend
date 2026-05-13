from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase, resolve_location_uuid
from app.services import open_meteo
from app import cache as _cache
from typing import Optional

router = APIRouter(prefix="/api/floods", tags=["Floods"])


@router.get("/current/{location_id}")
async def get_current_flood_data(location_id: str):
    cache_key = f"flood:current:{location_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    client = get_supabase()
    uuid = resolve_location_uuid(location_id)

    result = (
        client.table("flood_data")
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
        live = await open_meteo.fetch_flood_data(
            loc.data[0]["latitude"], loc.data[0]["longitude"]
        )
        return {"data": live, "source": "live_api", "location": loc.data[0]["name"]}

    return {"data": result.data[0], "source": "database"}


@router.get("/forecast/{location_id}")
async def get_flood_forecast(location_id: str):
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

    live = await open_meteo.fetch_flood_data(
        loc.data[0]["latitude"], loc.data[0]["longitude"]
    )
    return {
        "data": live,
        "location": loc.data[0]["name"],
    }


@router.get("/history/{location_id}")
async def get_flood_history(
    location_id: str,
    days: int = Query(90, ge=1, le=365),
):
    from datetime import datetime, timedelta
    from app.config import get_app_config

    client = get_supabase()
    uuid = resolve_location_uuid(location_id)
    thresholds = get_app_config().alert_thresholds.get("flood", {})
    high_t = thresholds.get("high", 100)
    moderate_t = thresholds.get("moderate", 50)
    extreme_t = thresholds.get("extreme", 200)

    now = datetime.utcnow()
    start = (now - timedelta(days=days)).isoformat()
    end = now.isoformat()

    result = (
        client.table("flood_data")
        .select("observed_at,river_discharge,flood_risk_level")
        .eq("location_id", uuid)
        .gte("observed_at", start)
        .lte("observed_at", end)
        .order("observed_at", desc=False)
        .execute()
    )

    db_rows = result.data or []
    db_dates = {r["observed_at"][:10] for r in db_rows}

    # Fetch from Open-Meteo when DB has less than 80% coverage
    api_rows = []
    if len(db_dates) < days * 0.8:
        loc = (
            client.table("locations")
            .select("latitude, longitude")
            .eq("id", uuid)
            .execute()
        )
        if loc.data:
            try:
                api_data = await open_meteo.fetch_flood_history(
                    loc.data[0]["latitude"], loc.data[0]["longitude"], past_days=days
                )
                daily = api_data.get("daily", {})
                times = daily.get("time", [])
                discharges = daily.get("river_discharge", [])

                for t, q in zip(times, discharges):
                    if q is None or t[:10] in db_dates:
                        continue  # Skip nulls and dates we already have in DB
                    if q >= extreme_t:
                        risk = "extreme"
                    elif q >= high_t:
                        risk = "high"
                    elif q >= moderate_t:
                        risk = "moderate"
                    else:
                        risk = "low"
                    api_rows.append({
                        "observed_at": t,
                        "river_discharge": q,
                        "flood_risk_level": risk,
                    })
            except Exception:
                pass  # Fallback to DB only if API fails

    # Merge: DB rows take priority, fill gaps with API rows
    all_rows = sorted(
        db_rows + api_rows,
        key=lambda r: r["observed_at"]
    )
    source = "database" if not api_rows else ("api_only" if not db_rows else "merged")
    return {"data": all_rows, "count": len(all_rows), "source": source}


@router.get("/predictions/{location_id}")
async def get_flood_predictions(location_id: str):
    client = get_supabase()
    uuid = resolve_location_uuid(location_id)

    from datetime import datetime
    now = datetime.utcnow().isoformat()

    result = (
        client.table("flood_predictions")
        .select("*")
        .eq("location_id", uuid)
        .gte("target_date", now)
        .order("target_date", desc=False)
        .limit(30)
        .execute()
    )

    return {"data": result.data, "count": len(result.data)}


@router.get("/risk-map")
async def get_flood_risk_map():
    cache_key = "flood:risk-map"
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

    all_flood = (
        client.table("flood_data")
        .select("location_id,river_discharge,flood_risk_level,observed_at")
        .in_("location_id", list(loc_by_id.keys()))
        .order("observed_at", desc=True)
        .limit(500)
        .execute()
    )
    latest_by_loc: dict = {}
    for row in (all_flood.data or []):
        lid = row["location_id"]
        if lid not in latest_by_loc:
            latest_by_loc[lid] = row

    risk_data = [
        {"location": loc, "latest_flood": latest_by_loc.get(loc["id"])}
        for loc in locations.data
    ]
    response = {"data": risk_data}
    _cache.set(cache_key, response, ttl=180)
    return response
