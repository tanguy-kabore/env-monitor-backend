import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Query
from app.database import get_supabase, resolve_location_uuid, get_all_location_uuids
from app.config import get_app_config
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/alerts", tags=["Alerts"])


@router.get("")
async def get_alerts(
    active: Optional[str] = Query(None),   # "true" | "false" | None = all
    alert_type: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    client = get_supabase()
    query = client.table("alerts").select("*, locations(name, latitude, longitude)")

    if active == "true":
        query = query.eq("is_active", True)
    elif active == "false":
        query = query.eq("is_active", False)
    # else: no filter → return all

    if alert_type:
        query = query.eq("alert_type", alert_type)
    if severity:
        query = query.eq("severity", severity)

    result = query.order("start_date", desc=True).limit(limit).execute()
    return {"data": result.data, "count": len(result.data)}


@router.get("/location/{location_id}")
async def get_alerts_by_location(location_id: str):
    client = get_supabase()
    uuid = resolve_location_uuid(location_id)

    result = (
        client.table("alerts")
        .select("*")
        .eq("location_id", uuid)
        .eq("is_active", True)
        .order("created_at", desc=True)
        .execute()
    )

    return {"data": result.data}


@router.get("/stats")
async def get_alert_stats():
    client = get_supabase()

    active = (
        client.table("alerts")
        .select("alert_type, severity", count="exact")
        .eq("is_active", True)
        .execute()
    )

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

    return {"data": stats}


@router.get("/history-stats")
async def get_alert_history_stats(days: int = Query(30, ge=7, le=90)):
    """Return per-day alert counts (created) for the last N days, grouped by type and severity."""
    from datetime import timedelta
    client = get_supabase()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    result = (
        client.table("alerts")
        .select("id, alert_type, severity, start_date, is_active")
        .gte("start_date", since)
        .order("start_date", desc=False)
        .limit(2000)
        .execute()
    )
    rows = result.data or []

    # Build daily buckets
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

    # Fill missing days with zeros
    from datetime import date as _date
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
    result = (
        client.table("alerts")
        .update({"is_active": False, "end_date": now, "updated_at": now})
        .eq("id", alert_id)
        .execute()
    )
    if not result.data:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Alerte introuvable")
    return {"success": True, "id": alert_id}


@router.delete("/{alert_id}")
async def delete_alert(alert_id: str):
    """Permanently delete a resolved alert from history."""
    from fastapi import HTTPException
    client = get_supabase()
    # Only allow deleting resolved alerts
    check = client.table("alerts").select("id, is_active").eq("id", alert_id).execute()
    if not check.data:
        raise HTTPException(status_code=404, detail="Alerte introuvable")
    if check.data[0].get("is_active"):
        raise HTTPException(status_code=400, detail="Impossible de supprimer une alerte active — résolvez-la d'abord")
    client.table("alerts").delete().eq("id", alert_id).execute()
    return {"success": True, "id": alert_id}


@router.post("/resolve-all")
async def resolve_all_alerts():
    """Resolve all currently active alerts at once."""
    client = get_supabase()
    now = datetime.now(timezone.utc).isoformat()
    result = (
        client.table("alerts")
        .update({"is_active": False, "end_date": now, "updated_at": now})
        .eq("is_active", True)
        .execute()
    )
    count = len(result.data) if result.data else 0
    logger.info(f"resolve-all: {count} alerts resolved")
    return {"success": True, "resolved": count}


@router.post("/archive-daily")
async def archive_daily_alerts():
    """
    Run once per day (e.g. 23:59).
    For each alert still active and unchanged for >= 20 hours, close it (end_date=now)
    and immediately reopen a fresh copy (start_date=now).
    This creates a clean daily history record per persisting condition,
    without accumulating noise from hourly re-checks.
    """
    from datetime import timedelta
    client = get_supabase()
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=20)).isoformat()
    now_iso = now.isoformat()

    # Fetch active alerts that have been open for >= 20h without severity change
    old_alerts_res = (
        client.table("alerts")
        .select("*")
        .eq("is_active", True)
        .lte("start_date", cutoff)
        .execute()
    )
    old_alerts = old_alerts_res.data or []

    archived = 0
    reopened = 0

    for alert in old_alerts:
        # Close the old one
        client.table("alerts").update({
            "is_active": False,
            "end_date": now_iso,
            "updated_at": now_iso,
        }).eq("id", alert["id"]).execute()
        archived += 1

        # Reopen a fresh copy for the new day
        new_alert = {
            k: alert[k]
            for k in ("location_id", "alert_type", "severity", "title", "description", "metadata")
            if k in alert
        }
        new_alert["start_date"] = now_iso
        new_alert["last_checked_at"] = now_iso
        new_alert["is_active"] = True
        client.table("alerts").insert(new_alert).execute()
        reopened += 1

    logger.info(f"Daily archive: {archived} archived, {reopened} reopened")
    return {"archived": archived, "reopened": reopened}


@router.post("/generate")
async def generate_alerts():
    """Scan latest data and create/resolve alerts based on configured thresholds."""
    client = get_supabase()
    config = get_app_config()
    thresholds = config.alert_thresholds
    flood_t = thresholds.get("flood", {})
    aq_t = thresholds.get("air_quality", {})
    temp_t = thresholds.get("temperature", {})
    now = datetime.now(timezone.utc).isoformat()

    # Fetch all active city locations
    locs_res = (
        client.table("locations")
        .select("id, name, external_id")
        .eq("type", "city")
        .eq("is_active", True)
        .execute()
    )
    locs = locs_res.data or []
    loc_ids = [l["id"] for l in locs]
    loc_by_id = {l["id"]: l for l in locs}

    # Fetch latest flood data per location
    flood_rows = (
        client.table("flood_data")
        .select("location_id, river_discharge, flood_risk_level, observed_at")
        .in_("location_id", loc_ids)
        .order("observed_at", desc=True)
        .limit(len(loc_ids) * 3)
        .execute()
    ).data or []
    latest_flood: dict = {}
    for r in flood_rows:
        if r["location_id"] not in latest_flood:
            latest_flood[r["location_id"]] = r

    # Fetch latest air quality per location
    aq_rows = (
        client.table("air_quality_data")
        .select("location_id, aqi, observed_at")
        .in_("location_id", loc_ids)
        .order("observed_at", desc=True)
        .limit(len(loc_ids) * 3)
        .execute()
    ).data or []
    latest_aq: dict = {}
    for r in aq_rows:
        if r["location_id"] not in latest_aq:
            latest_aq[r["location_id"]] = r

    # Fetch latest weather per location
    wx_rows = (
        client.table("weather_data")
        .select("location_id, temperature, temperature_max, observed_at")
        .in_("location_id", loc_ids)
        .order("observed_at", desc=True)
        .limit(len(loc_ids) * 3)
        .execute()
    ).data or []
    latest_wx: dict = {}
    for r in wx_rows:
        if r["location_id"] not in latest_wx:
            latest_wx[r["location_id"]] = r

    # Fetch currently active alerts to avoid duplicates and resolve stale ones
    active_alerts_res = (
        client.table("alerts")
        .select("id, location_id, alert_type, severity, metadata")
        .eq("is_active", True)
        .execute()
    )
    active_alerts = active_alerts_res.data or []
    # Index: (location_id, alert_type) -> alert
    active_index: dict = {}
    for a in active_alerts:
        active_index[(a["location_id"], a["alert_type"])] = a

    created = 0
    resolved = 0
    to_create = []
    to_resolve = []

    def _upsert_alert(loc_id, alert_type, severity, title, description, metadata=None):
        nonlocal created
        key = (loc_id, alert_type)
        existing = active_index.get(key)
        if existing:
            # Update severity if escalated, but don't create duplicate
            # Always update last_checked_at; update severity/title if escalated
            update_payload: dict = {"last_checked_at": now, "updated_at": now}
            if existing.get("severity") != severity:
                update_payload.update({
                    "severity": severity,
                    "title": title,
                    "description": description,
                    "metadata": metadata or {},
                })
            client.table("alerts").update(update_payload).eq("id", existing["id"]).execute()
            active_index.pop(key)  # mark as still active (remove from "to resolve" pool)
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

        # --- Flood alerts ---
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

        # --- Air quality alerts ---
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

        # --- Heat wave alerts ---
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

    # Insert new alerts in batch
    if to_create:
        client.table("alerts").insert(to_create).execute()

    # Resolve stale alerts (conditions no longer met)
    remaining_keys = list(active_index.keys())
    for key in remaining_keys:
        alert = active_index[key]
        client.table("alerts").update({
            "is_active": False,
            "end_date": now,
            "updated_at": now,
        }).eq("id", alert["id"]).execute()
        resolved += 1

    logger.info(f"Alert generation: {created} created, {resolved} resolved")
    return {
        "created": created,
        "resolved": resolved,
        "active_total": len(active_alerts) - resolved + created,
    }
