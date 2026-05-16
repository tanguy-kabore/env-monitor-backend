import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Query
from app.database import get_supabase, resolve_location_uuid_async, db_exec
from app.config import get_app_config
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/alerts", tags=["Alerts"])


@router.get("")
async def get_alerts(
    active: Optional[str] = Query(None),
    alert_type: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    client = get_supabase()

    def _fetch():
        query = client.table("alerts").select("*, locations(name, latitude, longitude)", count="exact")
        if active == "true":
            query = query.eq("is_active", True)
        elif active == "false":
            query = query.eq("is_active", False)
        if alert_type:
            query = query.eq("alert_type", alert_type)
        if severity:
            query = query.eq("severity", severity)
        return query.order("start_date", desc=True).range(offset, offset + limit - 1).execute()

    result = await db_exec(_fetch)
    total = result.count if result.count is not None else len(result.data)
    return {
        "data": result.data,
        "pagination": {"total": total, "limit": limit, "offset": offset, "has_more": offset + limit < total},
    }


@router.get("/location/{location_id}")
async def get_alerts_by_location(location_id: str):
    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    result = await db_exec(lambda: client.table("alerts")
        .select("id,location_id,alert_type,severity,title,description,start_date,end_date,is_active,metadata,last_checked_at")
        .eq("location_id", uuid)
        .eq("is_active", True)
        .order("created_at", desc=True)
        .limit(100)
        .execute())

    return {"data": result.data}


@router.get("/stats")
async def get_alert_stats():
    from app import cache as _cache
    cache_key = "alerts:stats"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()

    active = await db_exec(lambda: client.table("alerts")
        .select("alert_type, severity", count="exact")
        .eq("is_active", True)
        .execute())

    stats = {"total_active": active.count if active.count else len(active.data)}

    by_type = {}
    by_severity = {}
    for a in active.data:
        t = a.get("alert_type", "unknown")
        s = a.get("severity", "unknown")
        by_type[t] = by_type.get(t, 0) + 1
        by_severity[s] = by_severity.get(s, 0) + 1

    stats["by_type"] = by_type
    stats["by_severity"] = by_severity

    response = {"data": stats}
    _cache.set(cache_key, response, ttl=300)
    return response


@router.get("/history-stats")
async def get_alert_history_stats(days: int = Query(30, ge=7, le=90)):
    """Return per-day alert counts for the last N days, grouped by type and severity."""
    from datetime import timedelta
    client = get_supabase()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    result = await db_exec(lambda: client.table("alerts")
        .select("id, alert_type, severity, start_date, is_active")
        .gte("start_date", since)
        .order("start_date", desc=False)
        .limit(2000)
        .execute())
    rows = result.data or []

    from collections import defaultdict
    daily: dict = defaultdict(lambda: {"flood": 0, "air_quality": 0, "heat_wave": 0, "drought": 0, "total": 0})
    by_severity: dict = defaultdict(int)
    by_type: dict = defaultdict(int)

    for r in rows:
        raw = r.get("start_date") or ""
        day = raw[:10]
        if not day:
            continue
        t = r.get("alert_type", "other")
        s = r.get("severity", "unknown")
        daily[day][t] = daily[day].get(t, 0) + 1
        daily[day]["total"] += 1
        by_severity[s] += 1
        by_type[t] += 1

    from datetime import date as _date, timedelta
    start = (_date.today() - timedelta(days=days - 1))
    all_days = [(start + timedelta(i)).isoformat() for i in range(days)]
    timeline = []
    for d in all_days:
        entry = {"date": d, "total": 0, "flood": 0, "air_quality": 0, "heat_wave": 0, "drought": 0}
        entry.update(daily.get(d, {}))
        timeline.append(entry)

    return {
        "timeline": timeline,
        "by_type": dict(by_type),
        "by_severity": dict(by_severity),
        "total": len(rows),
        "days": days,
    }


@router.post("/{alert_id}/resolve")
async def resolve_alert(alert_id: str):
    """Manually resolve (close) an active alert."""
    client = get_supabase()
    now = datetime.now(timezone.utc).isoformat()
    result = await db_exec(lambda: client.table("alerts")
        .update({"is_active": False, "end_date": now, "updated_at": now})
        .eq("id", alert_id)
        .execute())
    if not result.data:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Alerte introuvable")
    return {"success": True, "id": alert_id}


@router.delete("/{alert_id}")
async def delete_alert(alert_id: str):
    """Permanently delete a resolved alert from history."""
    from fastapi import HTTPException
    client = get_supabase()
    check = await db_exec(lambda: client.table("alerts")
        .select("id, is_active").eq("id", alert_id).limit(1).execute())
    if not check.data:
        raise HTTPException(status_code=404, detail="Alerte introuvable")
    if check.data[0].get("is_active"):
        raise HTTPException(status_code=400, detail="Impossible de supprimer une alerte active — résolvez-la d'abord")
    await db_exec(lambda: client.table("alerts").delete().eq("id", alert_id).execute())
    return {"success": True, "id": alert_id}


@router.post("/resolve-all")
async def resolve_all_alerts():
    """Resolve all currently active alerts at once."""
    client = get_supabase()
    now = datetime.now(timezone.utc).isoformat()
    result = await db_exec(lambda: client.table("alerts")
        .update({"is_active": False, "end_date": now, "updated_at": now})
        .eq("is_active", True)
        .execute())
    count = len(result.data) if result.data else 0
    logger.info(f"resolve-all: {count} alerts resolved")
    return {"success": True, "resolved": count}


@router.post("/archive-daily")
async def archive_daily_alerts():
    """
    Run once per day (e.g. 23:59).
    For each alert still active and unchanged for >= 20 hours, close it (end_date=now)
    and immediately reopen a fresh copy (start_date=now).
    """
    from datetime import timedelta
    client = get_supabase()
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=20)).isoformat()
    now_iso = now.isoformat()

    old_alerts_res = await db_exec(lambda: client.table("alerts")
        .select("id,location_id,alert_type,severity,title,description,metadata")
        .eq("is_active", True)
        .lte("start_date", cutoff)
        .limit(500)
        .execute())
    old_alerts = old_alerts_res.data or []

    archived = 0
    reopened = 0

    if old_alerts:
        ids_to_archive = [a["id"] for a in old_alerts]
        await db_exec(lambda: client.table("alerts").update({
            "is_active": False,
            "end_date": now_iso,
            "updated_at": now_iso,
        }).in_("id", ids_to_archive).execute())
        archived = len(ids_to_archive)

        new_alerts_payload = [
            {k: a[k] for k in ("location_id", "alert_type", "severity", "title", "description", "metadata") if k in a}
            | {"start_date": now_iso, "last_checked_at": now_iso, "is_active": True}
            for a in old_alerts
        ]
        await db_exec(lambda: client.table("alerts").insert(new_alerts_payload).execute())
        reopened = len(new_alerts_payload)

    logger.info(f"Daily archive: {archived} archived, {reopened} reopened")
    return {"archived": archived, "reopened": reopened}


@router.post("/generate")
async def generate_alerts():
    """Scan latest data and create/resolve alerts based on configured thresholds."""
    import asyncio
    client = get_supabase()
    config = get_app_config()
    thresholds = config.alert_thresholds
    aq_t = thresholds.get("air_quality", {})
    temp_t = thresholds.get("temperature", {})
    now = datetime.now(timezone.utc).isoformat()

    locs_res = await db_exec(lambda: client.table("locations")
        .select("id, name, external_id")
        .eq("type", "city")
        .eq("is_active", True)
        .execute())
    locs = locs_res.data or []
    loc_ids = [l["id"] for l in locs]

    # Fetch all latest sensor data and active alerts in parallel
    flood_res, aq_res, wx_res, active_alerts_res = await asyncio.gather(
        db_exec(lambda: client.table("flood_data")
            .select("location_id, river_discharge, flood_risk_level, observed_at")
            .in_("location_id", loc_ids)
            .order("observed_at", desc=True)
            .limit(len(loc_ids) * 3)
            .execute()),
        db_exec(lambda: client.table("air_quality_data")
            .select("location_id, aqi, observed_at")
            .in_("location_id", loc_ids)
            .order("observed_at", desc=True)
            .limit(len(loc_ids) * 3)
            .execute()),
        db_exec(lambda: client.table("weather_data")
            .select("location_id, temperature, temperature_max, observed_at")
            .in_("location_id", loc_ids)
            .order("observed_at", desc=True)
            .limit(len(loc_ids) * 3)
            .execute()),
        db_exec(lambda: client.table("alerts")
            .select("id, location_id, alert_type, severity, metadata")
            .eq("is_active", True)
            .limit(2000)
            .execute()),
    )

    latest_flood: dict = {}
    for r in (flood_res.data or []):
        if r["location_id"] not in latest_flood:
            latest_flood[r["location_id"]] = r

    latest_aq: dict = {}
    for r in (aq_res.data or []):
        if r["location_id"] not in latest_aq:
            latest_aq[r["location_id"]] = r

    latest_wx: dict = {}
    for r in (wx_res.data or []):
        if r["location_id"] not in latest_wx:
            latest_wx[r["location_id"]] = r

    active_alerts = active_alerts_res.data or []
    active_index: dict = {}
    for a in active_alerts:
        active_index[(a["location_id"], a["alert_type"])] = a

    created = 0
    to_create = []
    to_update_checked = []   # IDs needing only last_checked_at bump
    to_update_escalated = [] # (id, payload) for severity changes

    def _upsert_alert(loc_id, alert_type, severity, title, description, metadata=None):
        nonlocal created
        key = (loc_id, alert_type)
        existing = active_index.get(key)
        if existing:
            if existing.get("severity") != severity:
                to_update_escalated.append((existing["id"], {
                    "severity": severity,
                    "title": title,
                    "description": description,
                    "metadata": metadata or {},
                    "last_checked_at": now,
                    "updated_at": now,
                }))
            else:
                to_update_checked.append(existing["id"])
            active_index.pop(key)
        else:
            to_create.append({
                "location_id": loc_id,
                "alert_type": alert_type,
                "severity": severity,
                "title": title,
                "description": description,
                "start_date": now,
                "last_checked_at": now,
                "is_active": True,
                "metadata": metadata or {},
            })
            created += 1

    for loc in locs:
        lid = loc["id"]
        name = loc["name"]

        fd = latest_flood.get(lid)
        if fd:
            risk = fd.get("flood_risk_level", "low")
            discharge = fd.get("river_discharge") or 0
            if risk == "extreme":
                _upsert_alert(lid, "flood", "critical",
                    f"Risque inondation extrême — {name}",
                    f"Débit fluvial critique : {discharge:.1f} m³/s. Évacuation recommandée.",
                    {"river_discharge": discharge, "risk_level": risk})
            elif risk == "high":
                _upsert_alert(lid, "flood", "danger",
                    f"Risque inondation élevé — {name}",
                    f"Débit fluvial élevé : {discharge:.1f} m³/s. Vigilance requise.",
                    {"river_discharge": discharge, "risk_level": risk})
            elif risk == "moderate":
                _upsert_alert(lid, "flood", "warning",
                    f"Risque inondation modéré — {name}",
                    f"Débit fluvial en hausse : {discharge:.1f} m³/s.",
                    {"river_discharge": discharge, "risk_level": risk})

        aq = latest_aq.get(lid)
        if aq and aq.get("aqi") is not None:
            aqi = aq["aqi"]
            if aqi > aq_t.get("very_poor", 100):
                _upsert_alert(lid, "air_quality", "danger",
                    f"Qualité de l'air très mauvaise — {name}",
                    f"Indice AQI : {aqi}. Sortir à l'extérieur déconseillé.",
                    {"aqi": aqi})
            elif aqi > aq_t.get("poor", 80):
                _upsert_alert(lid, "air_quality", "warning",
                    f"Qualité de l'air mauvaise — {name}",
                    f"Indice AQI : {aqi}. Populations sensibles à risque.",
                    {"aqi": aqi})

        wx = latest_wx.get(lid)
        if wx:
            temp = wx.get("temperature_max") or wx.get("temperature")
            if temp is not None:
                if temp >= temp_t.get("heat_extreme", 45):
                    _upsert_alert(lid, "heat_wave", "critical",
                        f"Canicule extrême — {name}",
                        f"Température max : {temp:.1f}°C. Danger vital.",
                        {"temperature_max": temp})
                elif temp >= temp_t.get("heat_warning", 42):
                    _upsert_alert(lid, "heat_wave", "warning",
                        f"Vague de chaleur — {name}",
                        f"Température max : {temp:.1f}°C. Risque sanitaire.",
                        {"temperature_max": temp})

    # Batch writes — run all in parallel
    write_tasks = []

    if to_create:
        write_tasks.append(db_exec(lambda: client.table("alerts").insert(to_create).execute()))

    if to_update_checked:
        ids = to_update_checked[:]
        write_tasks.append(db_exec(lambda: client.table("alerts")
            .update({"last_checked_at": now, "updated_at": now})
            .in_("id", ids).execute()))

    for (aid, payload) in to_update_escalated:
        write_tasks.append(db_exec(lambda a=aid, p=payload: client.table("alerts").update(p).eq("id", a).execute()))

    remaining_keys = list(active_index.keys())
    resolved = 0
    if remaining_keys:
        ids_to_resolve = [active_index[key]["id"] for key in remaining_keys]
        resolved = len(ids_to_resolve)
        write_tasks.append(db_exec(lambda: client.table("alerts").update({
            "is_active": False,
            "end_date": now,
            "updated_at": now,
        }).in_("id", ids_to_resolve).execute()))

    if write_tasks:
        await asyncio.gather(*write_tasks)

    logger.info(f"Alert generation: {created} created, {resolved} resolved")
    return {
        "created": created,
        "resolved": resolved,
        "active_total": len(active_alerts) - resolved + created,
    }
