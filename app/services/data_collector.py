import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from app.config import get_app_config
from app.database import (
    get_supabase,
    get_all_location_uuids,
    insert_batch,
    set_system_config,
    upsert_locations,
)
from app.services import open_meteo, nasa_power

logger = logging.getLogger(__name__)


async def initialize_locations() -> int:
    config = get_app_config()
    locations = config.get_all_locations()
    count = await upsert_locations(locations)
    logger.info(f"Initialized {count} new locations")
    return count


async def collect_current_weather() -> dict:
    config = get_app_config()
    locs = config.get_monitorable_locations()
    uuid_map = get_all_location_uuids()
    records = []
    errors = []
    start = time.time()

    for loc in locs:
        try:
            data = await open_meteo.fetch_current_weather(loc["latitude"], loc["longitude"])
            current = data.get("current", {})
            loc_uuid = uuid_map.get(loc["external_id"])
            if not loc_uuid:
                continue

            records.append({
                "location_id": loc_uuid,
                "observed_at": current.get("time"),
                "temperature": current.get("temperature_2m"),
                "humidity": current.get("relative_humidity_2m"),
                "precipitation": current.get("precipitation"),
                "wind_speed": current.get("wind_speed_10m"),
                "wind_direction": current.get("wind_direction_10m"),
                "pressure": current.get("pressure_msl"),
                "cloud_cover": current.get("cloud_cover"),
                "source": "open_meteo",
                "raw_data": current,
            })

            daily = data.get("daily", {})
            times = daily.get("time", [])
            for i, t in enumerate(times):
                records.append({
                    "location_id": loc_uuid,
                    "observed_at": t,
                    "temperature_max": _safe_idx(daily.get("temperature_2m_max"), i),
                    "temperature_min": _safe_idx(daily.get("temperature_2m_min"), i),
                    "precipitation": _safe_idx(daily.get("precipitation_sum"), i),
                    "wind_speed": _safe_idx(daily.get("wind_speed_10m_max"), i),
                    "evapotranspiration": _safe_idx(daily.get("et0_fao_evapotranspiration"), i),
                    "uv_index": _safe_idx(daily.get("uv_index_max"), i),
                    "source": "open_meteo_forecast",
                    "raw_data": {k: _safe_idx(v, i) for k, v in daily.items() if k != "time"},
                })
        except Exception as e:
            errors.append({"location": loc["name"], "error": str(e)})
            logger.error(f"Weather collect error for {loc['name']}: {e}")

    inserted = insert_batch("weather_data", records)
    duration = time.time() - start
    _log_collection("open_meteo", "weather", len(locs), inserted, errors, duration)
    return {"collected": len(records), "inserted": inserted, "errors": len(errors)}


async def collect_historical_weather() -> dict:
    config = get_app_config()
    locs = config.get_monitorable_locations()
    uuid_map = get_all_location_uuids()
    start_date = config.data_collection.get("historical_start_date", "2023-01-01")
    end_date = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")
    records = []
    errors = []
    start = time.time()

    # Find which locations already have archive data — skip them entirely
    # Use count query per uuid: fast (45 light queries vs 55000-row fetch)
    client = get_supabase()
    already_loaded = set()
    try:
        all_uuids = list(uuid_map.values())
        for uuid_val in all_uuids:
            res = (
                client.table("weather_data")
                .select("location_id", count="exact")
                .eq("source", "open_meteo_archive")
                .eq("location_id", uuid_val)
                .limit(1)
                .execute()
            )
            if (res.count or 0) > 0:
                already_loaded.add(uuid_val)
        logger.info(f"Historical check: {len(already_loaded)}/{len(all_uuids)} cities already loaded")
    except Exception as e:
        logger.warning(f"Could not check existing historical data: {e}")
        already_loaded = set()

    skipped = 0
    for loc in locs:
        loc_uuid = uuid_map.get(loc["external_id"])
        if not loc_uuid:
            continue
        if loc_uuid in already_loaded:
            skipped += 1
            logger.debug(f"Skipping {loc['name']} — historical data already in DB")
            continue
        try:
            data = await open_meteo.fetch_historical_weather(
                loc["latitude"], loc["longitude"], start_date, end_date
            )
            daily = data.get("daily", {})
            times = daily.get("time", [])

            for i, t in enumerate(times):
                records.append({
                    "location_id": loc_uuid,
                    "observed_at": t,
                    "temperature_max": _safe_idx(daily.get("temperature_2m_max"), i),
                    "temperature_min": _safe_idx(daily.get("temperature_2m_min"), i),
                    "temperature_mean": _safe_idx(daily.get("temperature_2m_mean"), i),
                    "precipitation": _safe_idx(daily.get("precipitation_sum"), i),
                    "wind_speed": _safe_idx(daily.get("wind_speed_10m_max"), i),
                    "humidity": _safe_idx(daily.get("relative_humidity_2m_mean"), i),
                    "evapotranspiration": _safe_idx(daily.get("et0_fao_evapotranspiration"), i),
                    "source": "open_meteo_archive",
                    "raw_data": {k: _safe_idx(v, i) for k, v in daily.items() if k != "time"},
                })
            await asyncio.sleep(1.5)
        except Exception as e:
            errors.append({"location": loc["name"], "error": str(e)})
            logger.error(f"Historical weather error for {loc['name']}: {e}")
            await asyncio.sleep(3)

    logger.info(f"Historical weather: {skipped}/{len(locs)} cities skipped (already in DB)")
    inserted = insert_batch("weather_data", records)
    duration = time.time() - start
    _log_collection("open_meteo_archive", "weather_historical", len(locs), inserted, errors, duration)
    return {"collected": len(records), "inserted": inserted, "errors": len(errors), "skipped": skipped}


async def collect_air_quality() -> dict:
    config = get_app_config()
    locs = config.get_monitorable_locations()
    uuid_map = get_all_location_uuids()
    records = []
    errors = []
    start = time.time()

    for loc in locs:
        try:
            data = await open_meteo.fetch_air_quality(loc["latitude"], loc["longitude"])
            current = data.get("current", {})
            loc_uuid = uuid_map.get(loc["external_id"])
            if not loc_uuid:
                continue

            records.append({
                "location_id": loc_uuid,
                "observed_at": current.get("time"),
                "pm2_5": current.get("pm2_5"),
                "pm10": current.get("pm10"),
                "no2": current.get("nitrogen_dioxide"),
                "so2": current.get("sulphur_dioxide"),
                "o3": current.get("ozone"),
                "co": current.get("carbon_monoxide"),
                "dust": current.get("dust"),
                "aqi": current.get("european_aqi"),
                "source": "open_meteo_aq",
                "raw_data": current,
            })
            await asyncio.sleep(0.5)
        except Exception as e:
            errors.append({"location": loc["name"], "error": str(e)})
            logger.error(f"Air quality error for {loc['name']}: {e}")
            await asyncio.sleep(2)

    inserted = insert_batch("air_quality_data", records)
    duration = time.time() - start
    _log_collection("open_meteo_aq", "air_quality", len(locs), inserted, errors, duration)
    return {"collected": len(records), "inserted": inserted, "errors": len(errors)}


async def collect_flood_data() -> dict:
    config = get_app_config()
    locs = config.get_monitorable_locations()
    uuid_map = get_all_location_uuids()
    thresholds = config.alert_thresholds.get("flood", {})
    records = []
    errors = []
    start = time.time()

    for loc in locs:
        try:
            data = await open_meteo.fetch_flood_data(loc["latitude"], loc["longitude"])
            daily = data.get("daily", {})
            times = daily.get("time", [])
            discharges = daily.get("river_discharge", [])
            loc_uuid = uuid_map.get(loc["external_id"])
            if not loc_uuid:
                continue

            for i, t in enumerate(times):
                discharge = _safe_idx(discharges, i)
                risk = _classify_flood_risk(discharge, thresholds)
                records.append({
                    "location_id": loc_uuid,
                    "observed_at": t,
                    "river_discharge": discharge,
                    "flood_risk_level": risk,
                    "source": "open_meteo_flood",
                    "raw_data": {"river_discharge": discharge},
                })
            await asyncio.sleep(0.5)
        except Exception as e:
            errors.append({"location": loc["name"], "error": str(e)})
            logger.error(f"Flood data error for {loc['name']}: {e}")
            await asyncio.sleep(2)

    inserted = insert_batch("flood_data", records)
    duration = time.time() - start
    _log_collection("open_meteo_flood", "flood", len(locs), inserted, errors, duration)
    return {"collected": len(records), "inserted": inserted, "errors": len(errors)}


async def collect_climate_data() -> dict:
    config = get_app_config()
    locs = config.get_monitorable_locations()
    uuid_map = get_all_location_uuids()
    records = []
    errors = []
    start = time.time()
    current_year = datetime.now(timezone.utc).year

    for loc in locs:
        try:
            data = await nasa_power.fetch_monthly_climate(
                loc["latitude"], loc["longitude"],
                current_year - 2, current_year - 1
            )
            properties = data.get("properties", {})
            parameters = properties.get("parameter", {})
            loc_uuid = uuid_map.get(loc["external_id"])
            if not loc_uuid:
                continue

            t2m = parameters.get("T2M", {})
            t2m_max = parameters.get("T2M_MAX", {})
            t2m_min = parameters.get("T2M_MIN", {})
            precip = parameters.get("PRECTOTCORR", {})
            rh = parameters.get("RH2M", {})
            ws = parameters.get("WS10M", {})
            solar = parameters.get("ALLSKY_SFC_SW_DWN", {})

            for key in t2m:
                if key.endswith("13"):
                    continue
                year = int(key[:4])
                month = int(key[4:])
                records.append({
                    "location_id": loc_uuid,
                    "year": year,
                    "month": month,
                    "avg_temperature": t2m.get(key),
                    "max_temperature": t2m_max.get(key),
                    "min_temperature": t2m_min.get(key),
                    "total_precipitation": precip.get(key),
                    "avg_humidity": rh.get(key),
                    "avg_wind_speed": ws.get(key),
                    "solar_radiation": solar.get(key),
                    "source": "nasa_power",
                    "raw_data": {
                        "T2M": t2m.get(key),
                        "T2M_MAX": t2m_max.get(key),
                        "T2M_MIN": t2m_min.get(key),
                        "PRECTOTCORR": precip.get(key),
                        "RH2M": rh.get(key),
                        "WS10M": ws.get(key),
                        "ALLSKY_SFC_SW_DWN": solar.get(key),
                    },
                })
        except Exception as e:
            errors.append({"location": loc["name"], "error": str(e)})
            logger.error(f"Climate data error for {loc['name']}: {e}")

    inserted = insert_batch("climate_data", records)
    duration = time.time() - start
    _log_collection("nasa_power", "climate", len(locs), inserted, errors, duration)
    return {"collected": len(records), "inserted": inserted, "errors": len(errors)}


async def compute_drought_indicators() -> dict:
    config = get_app_config()
    uuid_map = get_all_location_uuids()
    client = get_supabase()
    thresholds = config.alert_thresholds.get("drought", {})
    records = []
    now = datetime.now(timezone.utc)

    # Check which location+date combos already exist to avoid re-inserting
    try:
        existing_res = (
            client.table("drought_data")
            .select("location_id,observed_at")
            .gte("observed_at", (now - timedelta(days=200)).isoformat())
            .execute()
        )
        existing_keys = {(r["location_id"], r["observed_at"][:10]) for r in (existing_res.data or [])}
    except Exception:
        existing_keys = set()

    # Single batch query for all locations instead of N sequential queries
    all_uuids = list(uuid_map.values())
    cutoff_dt = (now - timedelta(days=270)).isoformat()
    try:
        all_hist_res = (
            client.table("weather_data")
            .select("location_id,observed_at,precipitation,evapotranspiration")
            .in_("location_id", all_uuids)
            .gte("observed_at", cutoff_dt)
            .not_.is_("precipitation", "null")
            .order("observed_at", desc=False)
            .limit(all_uuids.__len__() * 300)
            .execute()
        )
        all_hist_data = all_hist_res.data or []
    except Exception as e:
        logger.error(f"Batch drought weather fetch failed: {e}")
        all_hist_data = []

    from collections import defaultdict
    by_location: dict = defaultdict(dict)
    for row in all_hist_data:
        d = row["observed_at"][:10]
        by_location[row["location_id"]][d] = {
            "precip": (row["precipitation"] or 0),
            "et": (row["evapotranspiration"] or 0),
        }

    for ext_id, loc_uuid in uuid_map.items():
        try:
            by_date = by_location.get(loc_uuid, {})
            if not by_date:
                continue

            sorted_dates = sorted(by_date.keys())

            for date_str in sorted_dates:
                if (loc_uuid, date_str) in existing_keys:
                    continue
                d = datetime.strptime(date_str, "%Y-%m-%d")
                d30_start = (d - timedelta(days=30)).strftime("%Y-%m-%d")
                d90_start = (d - timedelta(days=90)).strftime("%Y-%m-%d")

                precip_30 = sum(v["precip"] for k, v in by_date.items() if d30_start <= k <= date_str)
                precip_90 = sum(v["precip"] for k, v in by_date.items() if d90_start <= k <= date_str)
                et_vals = [v["et"] for k, v in by_date.items() if d30_start <= k <= date_str and v["et"]]
                et_avg = sum(et_vals) / len(et_vals) if et_vals else 0

                spi = _compute_simple_spi(precip_30)
                drought_level = _classify_drought(spi, thresholds)

                records.append({
                    "location_id": loc_uuid,
                    "observed_at": date_str,
                    "precipitation_30d": round(precip_30, 2),
                    "precipitation_90d": round(precip_90, 2),
                    "evapotranspiration": round(et_avg, 4),
                    "spi_value": round(spi, 4),
                    "drought_level": drought_level,
                    "source": "computed",
                })

        except Exception as e:
            logger.error(f"Drought computation error for {ext_id}: {e}")

    inserted = insert_batch("drought_data", records)
    return {"computed": len(records), "inserted": inserted}


def _safe_idx(lst, idx):
    if lst and idx < len(lst):
        return lst[idx]
    return None


def _classify_flood_risk(discharge, thresholds):
    if discharge is None:
        return "low"
    if discharge >= thresholds.get("extreme"):
        return "extreme"
    if discharge >= thresholds.get("high"):
        return "high"
    if discharge >= thresholds.get("moderate"):
        return "moderate"
    return "low"


def _compute_simple_spi(precip_30d):
    mean_precip = 50.0
    std_precip = 40.0
    if std_precip == 0:
        return 0
    return (precip_30d - mean_precip) / std_precip


def _classify_drought(spi, thresholds):
    if spi <= thresholds.get("extreme"):
        return "extreme"
    if spi <= thresholds.get("severe"):
        return "severe"
    if spi <= thresholds.get("moderate"):
        return "moderate"
    return "normal"


def _log_collection(source, data_type, locs_processed, records_inserted, errors, duration):
    try:
        client = get_supabase()
        status = "success" if not errors else ("partial" if records_inserted > 0 else "failed")
        client.table("collection_log").insert({
            "source": source,
            "data_type": data_type,
            "locations_processed": locs_processed,
            "records_inserted": records_inserted,
            "status": status,
            "error_message": str(errors[:5]) if errors else None,
            "duration_seconds": round(duration, 2),
        }).execute()
        # Keep last_historical_load in sync and invalidate status cache
        if status in ("success", "partial"):
            set_system_config("last_historical_load", datetime.now(timezone.utc).isoformat())
            from app import cache as _cache
            _cache.delete("system:status")
    except Exception as e:
        logger.error(f"Failed to log collection: {e}")
