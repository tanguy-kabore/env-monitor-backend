import asyncio
from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase, get_location_uuid, resolve_location_uuid_async, db_exec
from typing import Optional
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/climate", tags=["Climate Change"])


@router.get("/data/{location_id}")
async def get_climate_data(
    location_id: str,
    start_year: int = Query(2020, ge=2000),
    end_year: int = Query(2025, le=2030),
):
    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)

    result = await db_exec(lambda: client.table("climate_data")
        .select("id,location_id,year,month,avg_temperature,max_temperature,min_temperature,total_precipitation,avg_humidity,avg_wind_speed,solar_radiation,source")
        .eq("location_id", uuid)
        .gte("year", start_year)
        .lte("year", end_year)
        .order("year")
        .order("month")
        .limit((end_year - start_year + 1) * 12 + 10)
        .execute())

    return {"data": result.data, "count": len(result.data)}


@router.get("/trends/{location_id}")
async def get_climate_trends(
    location_id: str,
    start_year: Optional[int] = Query(None, ge=2000, description="Première année (défaut: auto)"),
):
    from app import cache as _cache

    cache_key = f"climate:trends:{location_id}:{start_year}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    import pandas as pd
    import numpy as np
    from datetime import datetime as _dt
    from app.services.open_meteo import fetch_historical_weather

    client = get_supabase()
    uuid = await resolve_location_uuid_async(location_id)
    current_year = _dt.utcnow().year

    earliest_res = await db_exec(lambda: client.table("weather_data")
        .select("observed_at")
        .eq("location_id", uuid)
        .order("observed_at", desc=False)
        .limit(1)
        .execute())
    earliest_db_year = None
    if earliest_res.data:
        try:
            earliest_db_year = int(earliest_res.data[0]["observed_at"][:4])
        except Exception:
            pass

    if start_year is None:
        start_year = earliest_db_year or (current_year - 4)

    # Run these two reads in parallel since they're independent
    cd_res, loc_res = await asyncio.gather(
        db_exec(lambda: client.table("climate_data")
            .select("year,month,avg_temperature,total_precipitation,avg_humidity,solar_radiation")
            .eq("location_id", uuid)
            .order("year").order("month")
            .execute()),
        db_exec(lambda: client.table("locations")
            .select("latitude,longitude")
            .eq("id", uuid)
            .limit(1)
            .execute()),
    )
    climate_years = len(set(r["year"] for r in cd_res.data)) if cd_res.data else 0

    # Fetch weather_data in two range passes (run in parallel)
    def _build_q(lt_gte, page_start, page_limit):
        q = (client.table("weather_data")
            .select("observed_at,temperature,temperature_mean,temperature_max,temperature_min,precipitation,humidity")
            .eq("location_id", uuid)
            .order("observed_at", desc=False)
            .limit(3000))
        if lt_gte == "range":
            return q.gte("observed_at", page_start).lt("observed_at", page_limit)
        return q.gte("observed_at", page_start)

    q_hist = _build_q("range", f"{start_year}-01-01", f"{current_year}-01-01")
    q_curr = _build_q("gte", f"{current_year}-01-01", None)
    res_hist, res_curr = await asyncio.gather(
        db_exec(lambda q=q_hist: q.execute()),
        db_exec(lambda q=q_curr: q.execute()),
    )
    db_rows: list = (res_hist.data or []) + (res_curr.data or [])

    for row in db_rows:
        t = row.get("temperature_mean") or row.get("temperature") or row.get("temperature_max")
        row["_temp"] = t

    db_years_available = set()
    if db_rows:
        _tmp = pd.to_datetime([r["observed_at"] for r in db_rows])
        db_years_available = set(_tmp.year.tolist())

    needed_years = set(range(start_year, current_year)) - db_years_available
    era5_rows: list = []
    if needed_years:
        if loc_res.data:
            lat = loc_res.data[0]["latitude"]
            lon = loc_res.data[0]["longitude"]
            sorted_years = sorted(needed_years)
            ranges: list[tuple[int, int]] = []
            ry_s = ry_e = sorted_years[0]
            for y in sorted_years[1:]:
                if y == ry_e + 1:
                    ry_e = y
                else:
                    ranges.append((ry_s, ry_e))
                    ry_s = ry_e = y
            ranges.append((ry_s, ry_e))
            for (ys, ye) in ranges:
                end_dt = f"{min(ye, current_year - 1)}-12-31"
                if ys > current_year - 1:
                    continue
                try:
                    era5 = await fetch_historical_weather(lat, lon, f"{ys}-01-01", end_dt)
                    daily = era5.get("daily", {})
                    times = daily.get("time", [])
                    temp_mean = daily.get("temperature_2m_mean") or daily.get("temperature_2m_max", [])
                    precip = daily.get("precipitation_sum", [])
                    humidity = daily.get("relative_humidity_2m_mean") or [None] * len(times)
                    for i, t in enumerate(times):
                        era5_rows.append({
                            "observed_at": t,
                            "_temp": temp_mean[i] if i < len(temp_mean) else None,
                            "precipitation": precip[i] if i < len(precip) else None,
                            "humidity": humidity[i] if i < len(humidity) else None,
                        })
                except Exception as exc:
                    logger.warning(f"ERA5 fetch failed {ys}-{ye}: {exc}")

    combined = db_rows + era5_rows
    if not combined and climate_years < 2:
        raise HTTPException(status_code=404, detail="No climate data available")

    if not combined:
        df = pd.DataFrame(cd_res.data)
        yearly = None
        if not df.empty and "year" in df.columns:
            yearly = (
                df.groupby("year")
                .agg({"avg_temperature": "mean", "total_precipitation": "sum", "avg_humidity": "mean"})
                .reset_index().to_dict("records")
            )
        return {"monthly": cd_res.data, "yearly_trends": yearly, "source": "climate_data",
                "earliest_year": start_year}

    # Offload CPU-bound pandas aggregation to a thread so the event loop stays free
    def _compute(rows, sy, cy):
        import math

        def _sf(v):
            """Convert NaN/inf float to None — Starlette's JSONResponse uses
            allow_nan=False and would raise ValueError on bare NaN floats."""
            if v is None:
                return None
            try:
                f = float(v)
                return None if (math.isnan(f) or math.isinf(f)) else f
            except (TypeError, ValueError):
                return None

        df = pd.DataFrame(rows)
        df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
        df = df.dropna(subset=["observed_at"])
        df["year"] = df["observed_at"].dt.year.astype(int)
        df["month"] = df["observed_at"].dt.month.astype(int)
        df = df[df["year"] >= sy].copy()
        df["_temp"] = pd.to_numeric(df["_temp"], errors="coerce")
        if "precipitation" in df.columns:
            df["precipitation"] = pd.to_numeric(df["precipitation"], errors="coerce")
        else:
            df["precipitation"] = np.nan
        if "humidity" in df.columns:
            df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")
        else:
            df["humidity"] = np.nan

        monthly_df = df.groupby(["year", "month"]).agg(
            avg_temperature=("_temp", "mean"),
            total_precipitation=("precipitation", "sum"),
            avg_humidity=("humidity", "mean"),
        ).reset_index()
        monthly_df["solar_radiation"] = None

        # Replace NaN with None in records so json.dumps doesn't choke
        monthly = [
            {k: (_sf(v) if isinstance(v, float) else v) for k, v in rec.items()}
            for rec in monthly_df.to_dict("records")
        ]

        yearly_df = df.groupby("year").agg(
            avg_temperature=("_temp", "mean"),
            total_precipitation=("precipitation", "sum"),
            avg_humidity=("humidity", "mean"),
            temp_max=("_temp", "max"),
            temp_min=("_temp", "min"),
        ).reset_index()

        mean_series = yearly_df["avg_temperature"].dropna()
        all_mean_temp = _sf(mean_series.mean()) if len(mean_series) > 1 else None

        yearly = []
        for rec in yearly_df.to_dict("records"):
            cleaned = {k: (_sf(v) if isinstance(v, float) else v) for k, v in rec.items()}
            cleaned["partial"] = (cleaned["year"] == cy)
            avg_t = cleaned.get("avg_temperature")
            if all_mean_temp is not None and avg_t is not None:
                cleaned["temp_anomaly"] = _sf(round(avg_t - all_mean_temp, 3))
            else:
                cleaned["temp_anomaly"] = None
            yearly.append(cleaned)

        monthly_df["season"] = monthly_df["month"].apply(
            lambda m: "humide" if 5 <= m <= 10 else "sèche"
        )
        seasonal_df = monthly_df.groupby(["year", "season"]).agg(
            avg_temperature=("avg_temperature", "mean"),
            total_precipitation=("total_precipitation", "sum"),
        ).reset_index()
        seasonal = [
            {k: (_sf(v) if isinstance(v, float) else v) for k, v in rec.items()}
            for rec in seasonal_df.to_dict("records")
        ]

        extremes = []
        month_names_fr = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Août","Septembre","Octobre","Novembre","Décembre"]
        for yr, grp in monthly_df.groupby("year"):
            hottest = grp.loc[grp["avg_temperature"].idxmax()] if grp["avg_temperature"].notna().any() else None
            wettest = grp.loc[grp["total_precipitation"].idxmax()] if grp["total_precipitation"].notna().any() else None
            h_temp = _sf(hottest["avg_temperature"]) if hottest is not None else None
            w_precip = _sf(wettest["total_precipitation"]) if wettest is not None else None
            extremes.append({
                "year": int(yr),
                "hottest_month": month_names_fr[int(hottest["month"]) - 1] if hottest is not None else None,
                "hottest_temp": round(h_temp, 1) if h_temp is not None else None,
                "wettest_month": month_names_fr[int(wettest["month"]) - 1] if wettest is not None else None,
                "wettest_precip": round(w_precip, 1) if w_precip is not None else None,
            })

        trend_slope = None
        complete_years = yearly_df[yearly_df["year"] < cy]
        if len(complete_years) >= 3:
            x = complete_years["year"].values.astype(float)
            y_vals = complete_years["avg_temperature"].values.astype(float)
            mask = ~np.isnan(y_vals)
            if mask.sum() >= 3:
                slope, _ = np.polyfit(x[mask], y_vals[mask], 1)
                trend_slope = _sf(slope)

        earliest = int(df["year"].min()) if not df.empty else sy
        return {
            "monthly": monthly,
            "yearly_trends": yearly,
            "seasonal": seasonal,
            "extremes": extremes,
            "trend_slope": trend_slope,
            "long_mean_temp": all_mean_temp,
            "earliest_year": earliest,
        }

    computed = await asyncio.to_thread(_compute, combined, start_year, current_year)
    all_mean_temp = computed.pop("long_mean_temp")

    source = "era5+db" if era5_rows else "weather_archive"
    result = {
        **computed,
        "long_term_mean_temp": round(float(all_mean_temp), 2) if all_mean_temp is not None else None,
        "source": source,
    }
    _cache.set(cache_key, result, ttl=86_400)
    return result


@router.get("/comparison")
async def compare_climate(
    location_ids: str = Query(..., description="Comma-separated location external IDs"),
):
    client = get_supabase()
    ids = [lid.strip() for lid in location_ids.split(",")]

    async def _fetch_one(ext_id: str):
        uuid = await asyncio.to_thread(get_location_uuid, ext_id)
        if not uuid:
            return None
        loc, data = await asyncio.gather(
            db_exec(lambda u=uuid: client.table("locations").select("name").eq("id", u).limit(1).execute()),
            db_exec(lambda u=uuid: client.table("climate_data")
                .select("year,month,avg_temperature,total_precipitation,avg_humidity")
                .eq("location_id", u).order("year").order("month").execute()),
        )
        return {
            "location_id": ext_id,
            "name": loc.data[0]["name"] if loc.data else ext_id,
            "data": data.data,
        }

    results = await asyncio.gather(*[_fetch_one(eid) for eid in ids])
    comparison = [r for r in results if r is not None]
    return {"data": comparison}
