from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase, resolve_location_uuid_async, db_exec
from app.services import open_meteo
from app import cache as _cache
from typing import Optional

router = APIRouter(prefix="/api/v1/floods", tags=["Floods"])


@router.get("/current/{location_id}")
async def get_current_flood_data(location_id: str):
    cache_key = f"flood:current:{location_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    result = await db_exec(lambda: client.table("flood_data")
        .select("id,location_id,observed_at,river_discharge,flood_risk_level,source")
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
        live = await open_meteo.fetch_flood_data(
            loc.data[0]["latitude"], loc.data[0]["longitude"]
        )
        response = {"data": live, "source": "live_api", "location": loc.data[0]["name"]}
        _cache.set(cache_key, response, ttl=1_800)
        return response

    response = {"data": result.data[0], "source": "database"}
    _cache.set(cache_key, response, ttl=1_800)
    return response


@router.get("/forecast/{location_id}")
async def get_flood_forecast(location_id: str):
    cache_key = f"flood:forecast:{location_id}"
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

    live = await open_meteo.fetch_flood_data(
        loc.data[0]["latitude"], loc.data[0]["longitude"]
    )
    response = {
        "data": live,
        "location": loc.data[0]["name"],
    }
    _cache.set(cache_key, response, ttl=14_400)
    return response


@router.get("/history/{location_id}")
async def get_flood_history(
    location_id: str,
    days: int = Query(90, ge=1, le=365),
):
    cache_key = f"flood:history:{location_id}:{days}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    from datetime import datetime, timedelta
    from app.config import get_app_config

    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)
    thresholds = get_app_config().alert_thresholds.get("flood", {})
    high_t = thresholds.get("high")
    moderate_t = thresholds.get("moderate")
    extreme_t = thresholds.get("extreme")

    now = datetime.utcnow()
    start = (now - timedelta(days=days)).isoformat()
    end = now.isoformat()

    result = await db_exec(lambda: client.table("flood_data")
        .select("observed_at,river_discharge,flood_risk_level")
        .eq("location_id", uuid)
        .gte("observed_at", start)
        .lte("observed_at", end)
        .order("observed_at", desc=False)
        .execute())

    db_rows = result.data or []
    db_dates = {r["observed_at"][:10] for r in db_rows}

    api_rows = []
    if len(db_dates) < days * 0.8:
        loc = await db_exec(lambda: client.table("locations")
            .select("latitude, longitude")
            .eq("id", uuid)
            .limit(1)
            .execute())
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
                        continue
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
                pass

    all_rows = sorted(
        db_rows + api_rows,
        key=lambda r: r["observed_at"]
    )
    source = "database" if not api_rows else ("api_only" if not db_rows else "merged")
    response = {"data": all_rows, "count": len(all_rows), "source": source}
    _cache.set(cache_key, response, ttl=7_200)
    return response


@router.get("/predictions/{location_id}")
async def get_flood_predictions(location_id: str):
    cache_key = f"flood:predictions:{location_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    from datetime import datetime, timedelta
    now = datetime.utcnow()
    past_limit = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    future_limit = (now + timedelta(days=30)).strftime("%Y-%m-%d")

    result = await db_exec(lambda: client.table("flood_predictions")
        .select("id,location_id,target_date,predicted_at,river_discharge,flood_probability,risk_level,model_version")
        .eq("location_id", uuid)
        .gte("target_date", past_limit)
        .lte("target_date", future_limit)
        .order("target_date", desc=False)
        .order("predicted_at", desc=True)
        .limit(300)
        .execute())

    by_day: dict = {}
    for row in result.data:
        day = str(row["target_date"])[:10]
        if day not in by_day:
            by_day[day] = row

    deduped = sorted(by_day.values(), key=lambda r: r["target_date"])
    response = {"data": deduped, "count": len(deduped)}
    _cache.set(cache_key, response, ttl=1_800)
    return response


@router.get("/risk-map")
async def get_flood_risk_map():
    cache_key = "flood:risk-map"
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

    all_flood = await db_exec(lambda: client.table("flood_data")
        .select("location_id,river_discharge,flood_risk_level,observed_at")
        .in_("location_id", list(loc_by_id.keys()))
        .order("observed_at", desc=True)
        .limit(500)
        .execute())
    latest_by_loc: dict = {}
    for row in (all_flood.data or []):
        lid = row["location_id"]
        if lid not in latest_by_loc:
            latest_by_loc[lid] = row

    from datetime import datetime
    risk_data = [
        {"location": loc, "latest_flood": latest_by_loc.get(loc["id"])}
        for loc in locations.data
    ]
    response = {"data": risk_data, "updated_at": datetime.utcnow().isoformat()}
    _cache.set(cache_key, response, ttl=600)
    return response
