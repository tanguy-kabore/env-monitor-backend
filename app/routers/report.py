import logging
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Query
from app.database import get_supabase
from app.config import get_app_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/report", tags=["Report"])


def _safe(val, decimals=2):
    if val is None:
        return None
    try:
        return round(float(val), decimals)
    except Exception:
        return val


@router.get("")
async def generate_report(days: int = Query(30, ge=7, le=365)):
    """
    Aggregate all real data from the DB and return a structured report payload.
    Nothing is invented: every value comes directly from the database.
    """
    client = get_supabase()
    config = get_app_config()
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=days)).isoformat()

    # ── 1. Locations ────────────────────────────────────────────────────────
    locs_res = (
        client.table("locations")
        .select("id, external_id, name, latitude, longitude, population, type")
        .eq("type", "city")
        .eq("is_active", True)
        .order("population", desc=True)
        .execute()
    )
    locs = locs_res.data or []
    loc_ids = [l["id"] for l in locs]
    loc_by_id = {l["id"]: l for l in locs}

    # ── 2. Weather ──────────────────────────────────────────────────────────
    w_res = (
        client.table("weather_data")
        .select("location_id,temperature,humidity,precipitation,wind_speed,observed_at")
        .in_("location_id", loc_ids)
        .not_.is_("temperature", "null")
        .gte("observed_at", since)
        .order("observed_at", desc=True)
        .limit(5000)
        .execute()
    )
    weather_rows = w_res.data or []

    # Per-location latest + aggregates
    weather_by_loc: dict = {}
    weather_agg: dict = {}  # location_id -> {temps, humidities, precipitations}
    for r in weather_rows:
        lid = r["location_id"]
        if lid not in weather_by_loc:
            weather_by_loc[lid] = r
        if lid not in weather_agg:
            weather_agg[lid] = {"temps": [], "humidities": [], "precipitations": []}
        if r.get("temperature") is not None:
            weather_agg[lid]["temps"].append(float(r["temperature"]))
        if r.get("humidity") is not None:
            weather_agg[lid]["humidities"].append(float(r["humidity"]))
        if r.get("precipitation") is not None:
            weather_agg[lid]["precipitations"].append(float(r["precipitation"]))

    # Daily precipitation series (last 30d for chart)
    precip_series_res = (
        client.table("weather_data")
        .select("location_id,precipitation,observed_at")
        .in_("location_id", loc_ids[:5])  # top 5 cities
        .not_.is_("precipitation", "null")
        .gte("observed_at", since)
        .order("observed_at", desc=False)
        .limit(1000)
        .execute()
    )

    # ── 3. Air Quality ──────────────────────────────────────────────────────
    aq_res = (
        client.table("air_quality_data")
        .select("location_id,pm2_5,pm10,aqi,dust,observed_at")
        .in_("location_id", loc_ids)
        .not_.is_("aqi", "null")
        .gte("observed_at", since)
        .order("observed_at", desc=True)
        .limit(3000)
        .execute()
    )
    aq_rows = aq_res.data or []
    aq_by_loc: dict = {}
    aq_agg: dict = {}
    for r in aq_rows:
        lid = r["location_id"]
        if lid not in aq_by_loc:
            aq_by_loc[lid] = r
        if lid not in aq_agg:
            aq_agg[lid] = {"aqis": [], "pm25s": []}
        if r.get("aqi") is not None:
            aq_agg[lid]["aqis"].append(float(r["aqi"]))
        if r.get("pm2_5") is not None:
            aq_agg[lid]["pm25s"].append(float(r["pm2_5"]))

    # ── 4. Floods ───────────────────────────────────────────────────────────
    fl_res = (
        client.table("flood_data")
        .select("location_id,river_discharge,flood_risk_level,observed_at")
        .in_("location_id", loc_ids)
        .gte("observed_at", since)
        .order("observed_at", desc=True)
        .limit(3000)
        .execute()
    )
    fl_rows = fl_res.data or []
    fl_by_loc: dict = {}
    fl_agg: dict = {}
    for r in fl_rows:
        lid = r["location_id"]
        if lid not in fl_by_loc:
            fl_by_loc[lid] = r
        if lid not in fl_agg:
            fl_agg[lid] = {"discharges": [], "risk_counts": {}}
        if r.get("river_discharge") is not None:
            fl_agg[lid]["discharges"].append(float(r["river_discharge"]))
        risk = r.get("flood_risk_level")
        if risk:
            fl_agg[lid]["risk_counts"][risk] = fl_agg[lid]["risk_counts"].get(risk, 0) + 1

    # ── 5. Drought ──────────────────────────────────────────────────────────
    dr_res = (
        client.table("drought_data")
        .select("location_id,spi_value,precipitation_30d,drought_level,observed_at")
        .in_("location_id", loc_ids)
        .gte("observed_at", since)
        .order("observed_at", desc=True)
        .limit(3000)
        .execute()
    )
    dr_rows = dr_res.data or []
    dr_by_loc: dict = {}
    dr_agg: dict = {}
    for r in dr_rows:
        lid = r["location_id"]
        if lid not in dr_by_loc:
            dr_by_loc[lid] = r
        if lid not in dr_agg:
            dr_agg[lid] = {"spis": [], "level_counts": {}}
        if r.get("spi_value") is not None:
            dr_agg[lid]["spis"].append(float(r["spi_value"]))
        lvl = r.get("drought_level")
        if lvl:
            dr_agg[lid]["level_counts"][lvl] = dr_agg[lid]["level_counts"].get(lvl, 0) + 1

    # ── 6. Alerts ───────────────────────────────────────────────────────────
    al_res = (
        client.table("alerts")
        .select("id,alert_type,severity,is_active,start_date,end_date,location_id,title,description")
        .gte("start_date", since)
        .order("start_date", desc=True)
        .limit(500)
        .execute()
    )
    al_rows = al_res.data or []

    alert_by_type: dict = {}
    alert_by_severity: dict = {}
    alert_by_loc: dict = {}
    for a in al_rows:
        t = a.get("alert_type", "unknown")
        s = a.get("severity", "unknown")
        lid = a.get("location_id")
        alert_by_type[t] = alert_by_type.get(t, 0) + 1
        alert_by_severity[s] = alert_by_severity.get(s, 0) + 1
        if lid:
            alert_by_loc[lid] = alert_by_loc.get(lid, 0) + 1

    active_alerts = [a for a in al_rows if a.get("is_active")]

    # ── 7. Build city summaries ─────────────────────────────────────────────
    city_summaries = []
    for loc in locs:
        lid = loc["id"]
        w = weather_by_loc.get(lid)
        wa = weather_agg.get(lid, {})
        aq = aq_by_loc.get(lid)
        aqa = aq_agg.get(lid, {})
        fl = fl_by_loc.get(lid)
        fla = fl_agg.get(lid, {})
        dr = dr_by_loc.get(lid)
        dra = dr_agg.get(lid, {})

        temps = wa.get("temps", [])
        humids = wa.get("humidities", [])
        precips = wa.get("precipitations", [])
        aqis = aqa.get("aqis", [])
        discharges = fla.get("discharges", [])
        spis = dra.get("spis", [])

        city_summaries.append({
            "location": {
                "id": loc["external_id"],
                "name": loc["name"],
                "latitude": loc["latitude"],
                "longitude": loc["longitude"],
                "population": loc.get("population"),
            },
            "weather": {
                "current_temp": _safe(w.get("temperature")) if w else None,
                "current_humidity": _safe(w.get("humidity")) if w else None,
                "avg_temp": _safe(sum(temps) / len(temps)) if temps else None,
                "max_temp": _safe(max(temps)) if temps else None,
                "min_temp": _safe(min(temps)) if temps else None,
                "avg_humidity": _safe(sum(humids) / len(humids)) if humids else None,
                "total_precip": _safe(sum(precips)) if precips else None,
                "data_points": len(temps),
                "last_observed": w.get("observed_at") if w else None,
            },
            "air_quality": {
                "current_aqi": _safe(aq.get("aqi")) if aq else None,
                "current_pm25": _safe(aq.get("pm2_5")) if aq else None,
                "avg_aqi": _safe(sum(aqis) / len(aqis)) if aqis else None,
                "max_aqi": _safe(max(aqis)) if aqis else None,
                "data_points": len(aqis),
                "last_observed": aq.get("observed_at") if aq else None,
            },
            "flood": {
                "current_discharge": _safe(fl.get("river_discharge"), 3) if fl else None,
                "current_risk": fl.get("flood_risk_level") if fl else None,
                "avg_discharge": _safe(sum(discharges) / len(discharges), 3) if discharges else None,
                "max_discharge": _safe(max(discharges), 3) if discharges else None,
                "risk_distribution": fla.get("risk_counts", {}),
                "data_points": len(discharges),
                "last_observed": fl.get("observed_at") if fl else None,
            },
            "drought": {
                "current_spi": _safe(dr.get("spi_value")) if dr else None,
                "current_level": dr.get("drought_level") if dr else None,
                "avg_spi": _safe(sum(spis) / len(spis)) if spis else None,
                "min_spi": _safe(min(spis)) if spis else None,
                "level_distribution": dra.get("level_counts", {}),
                "data_points": len(spis),
                "last_observed": dr.get("observed_at") if dr else None,
            },
            "alerts": {
                "total": alert_by_loc.get(lid, 0),
            },
        })

    # ── 8. Global aggregates ────────────────────────────────────────────────
    all_temps = [t for wa in weather_agg.values() for t in wa.get("temps", [])]
    all_aqis = [a for aqa in aq_agg.values() for a in aqa.get("aqis", [])]
    all_spis = [s for dra in dr_agg.values() for s in dra.get("spis", [])]
    all_discharges = [d for fla in fl_agg.values() for d in fla.get("discharges", [])]

    cities_with_weather = sum(1 for c in city_summaries if c["weather"]["data_points"] > 0)
    cities_with_aq = sum(1 for c in city_summaries if c["air_quality"]["data_points"] > 0)
    cities_with_flood = sum(1 for c in city_summaries if c["flood"]["data_points"] > 0)
    cities_with_drought = sum(1 for c in city_summaries if c["drought"]["data_points"] > 0)

    # Cities with high/extreme flood risk right now
    high_risk_flood = [
        c for c in city_summaries
        if c["flood"]["current_risk"] in ("high", "extreme")
    ]
    drought_alert_cities = [
        c for c in city_summaries
        if c["drought"]["current_spi"] is not None and c["drought"]["current_spi"] < -1
    ]
    bad_air_cities = [
        c for c in city_summaries
        if c["air_quality"]["current_aqi"] is not None and c["air_quality"]["current_aqi"] > 60
    ]

    # Alert timeline: daily counts over the period
    alert_timeline_res = (
        client.table("alerts")
        .select("start_date,alert_type,severity")
        .gte("start_date", since)
        .order("start_date", desc=False)
        .limit(1000)
        .execute()
    )
    alert_timeline_rows = alert_timeline_res.data or []
    # Group by date
    timeline_by_date: dict = {}
    for a in alert_timeline_rows:
        raw = a.get("start_date", "")
        date_key = raw[:10] if raw else "unknown"
        if date_key not in timeline_by_date:
            timeline_by_date[date_key] = {"date": date_key, "total": 0, "flood": 0, "air_quality": 0, "heat_wave": 0, "drought": 0}
        timeline_by_date[date_key]["total"] += 1
        t = a.get("alert_type", "")
        if t in timeline_by_date[date_key]:
            timeline_by_date[date_key][t] += 1
    alert_timeline = sorted(timeline_by_date.values(), key=lambda x: x["date"])

    return {
        "meta": {
            "generated_at": now.isoformat(),
            "period_days": days,
            "period_start": since,
            "period_end": now.isoformat(),
            "app_name": config.app_name,
            "country": config.country.get("name"),
            "version": config.app_version,
        },
        "summary": {
            "total_cities_monitored": len(locs),
            "cities_with_weather_data": cities_with_weather,
            "cities_with_air_quality_data": cities_with_aq,
            "cities_with_flood_data": cities_with_flood,
            "cities_with_drought_data": cities_with_drought,
            "total_alerts_period": len(al_rows),
            "active_alerts": len(active_alerts),
            "alerts_by_type": alert_by_type,
            "alerts_by_severity": alert_by_severity,
            "high_flood_risk_cities": len(high_risk_flood),
            "drought_alert_cities": len(drought_alert_cities),
            "poor_air_quality_cities": len(bad_air_cities),
        },
        "global_stats": {
            "weather": {
                "avg_temp": _safe(sum(all_temps) / len(all_temps)) if all_temps else None,
                "max_temp": _safe(max(all_temps)) if all_temps else None,
                "min_temp": _safe(min(all_temps)) if all_temps else None,
                "observations": len(all_temps),
            },
            "air_quality": {
                "avg_aqi": _safe(sum(all_aqis) / len(all_aqis)) if all_aqis else None,
                "max_aqi": _safe(max(all_aqis)) if all_aqis else None,
                "observations": len(all_aqis),
            },
            "flood": {
                "avg_discharge": _safe(sum(all_discharges) / len(all_discharges), 3) if all_discharges else None,
                "max_discharge": _safe(max(all_discharges), 3) if all_discharges else None,
                "observations": len(all_discharges),
            },
            "drought": {
                "avg_spi": _safe(sum(all_spis) / len(all_spis)) if all_spis else None,
                "min_spi": _safe(min(all_spis)) if all_spis else None,
                "observations": len(all_spis),
            },
        },
        "highlights": {
            "high_flood_risk_cities": [
                {"name": c["location"]["name"], "discharge": c["flood"]["current_discharge"], "risk": c["flood"]["current_risk"]}
                for c in high_risk_flood
            ],
            "drought_alert_cities": [
                {"name": c["location"]["name"], "spi": c["drought"]["current_spi"], "level": c["drought"]["current_level"]}
                for c in drought_alert_cities
            ],
            "poor_air_quality_cities": [
                {"name": c["location"]["name"], "aqi": c["air_quality"]["current_aqi"], "pm25": c["air_quality"]["current_pm25"]}
                for c in bad_air_cities
            ],
            "recent_active_alerts": [
                {
                    "title": a.get("title"),
                    "alert_type": a.get("alert_type"),
                    "severity": a.get("severity"),
                    "start_date": a.get("start_date"),
                    "location_id": a.get("location_id"),
                }
                for a in active_alerts[:20]
            ],
        },
        "alert_timeline": alert_timeline,
        "city_summaries": city_summaries,
    }
