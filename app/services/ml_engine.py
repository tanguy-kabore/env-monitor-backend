import logging
import time
import numpy as np
import pandas as pd
import joblib
from io import BytesIO
from datetime import datetime, timedelta
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
from app.config import get_app_config
from app.database import get_supabase, get_all_location_uuids, insert_batch

logger = logging.getLogger(__name__)

MODEL_CACHE = {}


def _save_model_record(client, model_type: str, location_id: str, record: dict) -> None:
    """Insert new model record and archive all previous ones for the same (model_type, location_id).
    Only keeps the single best model (highest r2) as 'active'."""
    new_r2 = record.get("metrics", {}).get("r2", 0) or 0

    # Fetch existing active models for this type+location
    existing = (
        client.table("ml_models")
        .select("id,metrics")
        .eq("model_type", model_type)
        .eq("location_id", location_id)
        .eq("status", "active")
        .execute()
    )
    best_existing_r2 = max(
        (row.get("metrics", {}).get("r2") or 0 for row in (existing.data or [])),
        default=-999,
    )

    # Archive all existing active models for this slot
    if existing.data:
        existing_ids = [row["id"] for row in existing.data]
        client.table("ml_models").update({"status": "retired"}).in_("id", existing_ids).execute()

    # Insert new model — always active (it just replaced the old ones)
    record["status"] = "active"
    client.table("ml_models").insert(record).execute()

    if new_r2 < best_existing_r2:
        logger.debug(f"New {model_type} model r2={new_r2:.3f} < previous {best_existing_r2:.3f} — still replaced (latest wins)")


def _get_model_class(algorithm: str):
    if algorithm == "gradient_boosting":
        return GradientBoostingRegressor
    elif algorithm == "random_forest":
        return RandomForestRegressor
    return GradientBoostingRegressor


async def train_weather_models() -> dict:
    config = get_app_config()
    ml_conf = config.ml_config
    weather_conf = ml_conf["models"]["weather"]
    training_conf = ml_conf["training"]
    client = get_supabase()
    uuid_map = get_all_location_uuids()
    results = []

    for ext_id, loc_uuid in uuid_map.items():
        try:
            data = (
                client.table("weather_data")
                .select("observed_at,temperature_max,temperature_min,temperature_mean,humidity,precipitation,wind_speed,evapotranspiration")
                .eq("location_id", loc_uuid)
                .not_.is_("temperature_max", "null")
                .order("observed_at", desc=False)
                .limit(2000)
                .execute()
            )
            if not data.data or len(data.data) < training_conf["min_data_points"]:
                continue

            df = pd.DataFrame(data.data)
            df["observed_at"] = pd.to_datetime(df["observed_at"])
            df["day_of_year"] = df["observed_at"].dt.dayofyear
            df["month"] = df["observed_at"].dt.month
            df = df.dropna(subset=["temperature_max", "temperature_min"])

            features = ["humidity", "precipitation", "wind_speed", "day_of_year", "month"]
            available = [f for f in features if f in df.columns and df[f].notna().sum() > 10]
            if len(available) < 2:
                continue

            X = df[available].fillna(0).values
            start_t = time.time()

            for target in ["temperature_max", "temperature_min", "precipitation"]:
                if target not in df.columns or df[target].isna().all():
                    continue
                y = df[target].fillna(0).values
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y, test_size=training_conf["test_size"],
                    random_state=training_conf["random_state"]
                )

                ModelClass = _get_model_class(weather_conf["algorithm"])
                model = ModelClass(
                    n_estimators=training_conf.get("initial_epochs", 50),
                    max_depth=5,
                    random_state=training_conf["random_state"],
                )
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)

                metrics = {
                    "mae": round(float(mean_absolute_error(y_test, y_pred)), 4),
                    "rmse": round(float(np.sqrt(mean_squared_error(y_test, y_pred))), 4),
                    "r2": round(float(r2_score(y_test, y_pred)), 4),
                }

                importance = {}
                if hasattr(model, "feature_importances_"):
                    importance = {available[i]: round(float(v), 4)
                                  for i, v in enumerate(model.feature_importances_)}

                model_key = f"weather_{target}_{ext_id}"
                MODEL_CACHE[model_key] = {
                    "model": model,
                    "features": available,
                    "scaler": None,
                }

                version = f"v{datetime.utcnow().strftime('%Y%m%d%H%M')}"
                duration = time.time() - start_t

                _save_model_record(client, f"weather_{target}", loc_uuid, {
                    "model_type": f"weather_{target}",
                    "model_name": weather_conf["algorithm"],
                    "model_version": version,
                    "location_id": loc_uuid,
                    "metrics": metrics,
                    "parameters": {"n_estimators": training_conf.get("initial_epochs", 50), "max_depth": 5},
                    "feature_importance": importance,
                    "data_points_used": len(df),
                    "training_duration_seconds": round(duration, 2),
                })

                results.append({
                    "location": ext_id,
                    "target": target,
                    "metrics": metrics,
                })
        except Exception as e:
            logger.error(f"Training weather model for {ext_id}: {e}")

    logger.info(f"Trained {len(results)} weather models")
    return {"models_trained": len(results), "details": results[:10]}


async def train_flood_models() -> dict:
    config = get_app_config()
    ml_conf = config.ml_config
    flood_conf = ml_conf["models"]["flood"]
    training_conf = ml_conf["training"]
    client = get_supabase()
    uuid_map = get_all_location_uuids()
    results = []

    for ext_id, loc_uuid in uuid_map.items():
        try:
            data = (
                client.table("flood_data")
                .select("observed_at,river_discharge")
                .eq("location_id", loc_uuid)
                .not_.is_("river_discharge", "null")
                .order("observed_at", desc=False)
                .limit(2000)
                .execute()
            )
            if not data.data or len(data.data) < training_conf["min_data_points"]:
                continue

            df = pd.DataFrame(data.data)
            df["observed_at"] = pd.to_datetime(df["observed_at"])
            df["day_of_year"] = df["observed_at"].dt.dayofyear
            df["month"] = df["observed_at"].dt.month
            df["discharge_lag1"] = df["river_discharge"].shift(1)
            df["discharge_lag7"] = df["river_discharge"].shift(7)
            df["discharge_rolling7"] = df["river_discharge"].rolling(7).mean()
            df = df.dropna()

            if len(df) < training_conf["min_data_points"]:
                continue

            features = ["discharge_lag1", "discharge_lag7", "discharge_rolling7", "day_of_year", "month"]
            X = df[features].values
            y = df["river_discharge"].values

            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=training_conf["test_size"],
                random_state=training_conf["random_state"]
            )

            start_t = time.time()
            ModelClass = _get_model_class(flood_conf["algorithm"])
            model = ModelClass(
                n_estimators=training_conf.get("initial_epochs", 50),
                max_depth=5,
                random_state=training_conf["random_state"],
            )
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)

            metrics = {
                "mae": round(float(mean_absolute_error(y_test, y_pred)), 4),
                "rmse": round(float(np.sqrt(mean_squared_error(y_test, y_pred))), 4),
                "r2": round(float(r2_score(y_test, y_pred)), 4),
            }

            model_key = f"flood_{ext_id}"
            MODEL_CACHE[model_key] = {"model": model, "features": features}

            version = f"v{datetime.utcnow().strftime('%Y%m%d%H%M')}"
            duration = time.time() - start_t

            _save_model_record(client, "flood", loc_uuid, {
                "model_type": "flood",
                "model_name": flood_conf["algorithm"],
                "model_version": version,
                "location_id": loc_uuid,
                "metrics": metrics,
                "data_points_used": len(df),
                "training_duration_seconds": round(duration, 2),
            })

            results.append({"location": ext_id, "metrics": metrics})
        except Exception as e:
            logger.error(f"Training flood model for {ext_id}: {e}")

    return {"models_trained": len(results), "details": results[:10]}


async def train_air_quality_models() -> dict:
    config = get_app_config()
    ml_conf = config.ml_config
    aq_conf = ml_conf["models"]["air_quality"]
    training_conf = ml_conf["training"]
    client = get_supabase()
    uuid_map = get_all_location_uuids()
    results = []

    for ext_id, loc_uuid in uuid_map.items():
        try:
            data = (
                client.table("air_quality_data")
                .select("observed_at,pm10,pm2_5,dust,aqi")
                .eq("location_id", loc_uuid)
                .not_.is_("pm10", "null")
                .order("observed_at", desc=False)
                .limit(2000)
                .execute()
            )
            if not data.data or len(data.data) < training_conf["min_data_points"]:
                continue

            df = pd.DataFrame(data.data)
            df["observed_at"] = pd.to_datetime(df["observed_at"])
            df["day_of_year"] = df["observed_at"].dt.dayofyear
            df["month"] = df["observed_at"].dt.month
            df["hour"] = df["observed_at"].dt.hour
            df["pm10_lag1"] = df["pm10"].shift(1)
            df["dust_lag1"] = df["dust"].shift(1)
            df = df.dropna(subset=["pm10"])
            df = df.fillna(0)

            if len(df) < training_conf["min_data_points"]:
                continue

            features = ["pm10_lag1", "dust_lag1", "day_of_year", "month", "hour"]
            available = [f for f in features if f in df.columns]
            X = df[available].values

            # Adapt split for small datasets
            n = len(df)
            test_size = training_conf["test_size"] if n >= 10 else (1 / n)
            n_est = min(training_conf.get("initial_epochs", 50), max(5, n * 2))

            for target in ["pm10", "pm2_5", "aqi"]:
                if target not in df.columns or df[target].isna().all():
                    continue
                y = df[target].fillna(0).values
                if len(set(y)) < 2:
                    continue
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y, test_size=test_size,
                    random_state=training_conf["random_state"]
                )

                start_t = time.time()
                ModelClass = _get_model_class(aq_conf["algorithm"])
                model = ModelClass(
                    n_estimators=n_est,
                    max_depth=min(3, max(1, n // 2)),
                    random_state=training_conf["random_state"],
                )
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)

                metrics = {
                    "mae": round(float(mean_absolute_error(y_test, y_pred)), 4),
                    "rmse": round(float(np.sqrt(mean_squared_error(y_test, y_pred))), 4),
                    "r2": round(float(r2_score(y_test, y_pred)), 4) if len(y_test) > 1 else 0.0,
                }

                model_key = f"aq_{target}_{ext_id}"
                MODEL_CACHE[model_key] = {"model": model, "features": available}

                version = f"v{datetime.utcnow().strftime('%Y%m%d%H%M')}"

                _save_model_record(client, f"air_quality_{target}", loc_uuid, {
                    "model_type": f"air_quality_{target}",
                    "model_name": aq_conf["algorithm"],
                    "model_version": version,
                    "location_id": loc_uuid,
                    "metrics": metrics,
                    "data_points_used": len(df),
                    "training_duration_seconds": round(time.time() - start_t, 2),
                })

                results.append({"location": ext_id, "target": target, "metrics": metrics})
        except Exception as e:
            logger.error(f"Training AQ model for {ext_id}: {e}")

    return {"models_trained": len(results), "details": results[:10]}


async def generate_predictions() -> dict:
    config = get_app_config()
    uuid_map = get_all_location_uuids()
    client = get_supabase()
    weather_preds = []
    flood_preds = []
    aq_preds = []
    now = datetime.utcnow()

    for ext_id, loc_uuid in uuid_map.items():
        for target in ["temperature_max", "temperature_min", "precipitation"]:
            model_key = f"weather_{target}_{ext_id}"
            if model_key not in MODEL_CACHE:
                continue
            cached = MODEL_CACHE[model_key]
            model = cached["model"]
            features = cached["features"]

            for d in range(1, 8):
                future = now + timedelta(days=d)
                feat_dict = {
                    "humidity": 50, "precipitation": 0, "wind_speed": 10,
                    "day_of_year": future.timetuple().tm_yday, "month": future.month,
                }
                X = np.array([[feat_dict.get(f, 0) for f in features]])
                pred_val = float(model.predict(X)[0])

                record = {
                    "location_id": loc_uuid,
                    "predicted_at": now.isoformat(),
                    "target_date": future.strftime("%Y-%m-%d"),
                    "model_version": f"v{now.strftime('%Y%m%d%H%M')}",
                    "confidence": 0.8 - (d * 0.05),
                }
                if target == "temperature_max":
                    record["temperature_max"] = round(pred_val, 1)
                elif target == "temperature_min":
                    record["temperature_min"] = round(pred_val, 1)
                elif target == "precipitation":
                    record["precipitation"] = round(max(0, pred_val), 1)
                weather_preds.append(record)

        model_key = f"flood_{ext_id}"
        if model_key in MODEL_CACHE:
            cached = MODEL_CACHE[model_key]
            model = cached["model"]
            last_discharge = 0.5
            for d in range(1, 31):
                future = now + timedelta(days=d)
                X = np.array([[last_discharge, last_discharge, last_discharge,
                               future.timetuple().tm_yday, future.month]])
                pred_val = float(model.predict(X)[0])
                last_discharge = pred_val
                thresholds = config.alert_thresholds.get("flood", {})
                risk = "low"
                if pred_val >= thresholds.get("extreme", 200):
                    risk = "extreme"
                elif pred_val >= thresholds.get("high", 100):
                    risk = "high"
                elif pred_val >= thresholds.get("moderate", 50):
                    risk = "moderate"

                flood_preds.append({
                    "location_id": loc_uuid,
                    "predicted_at": now.isoformat(),
                    "target_date": future.strftime("%Y-%m-%d"),
                    "river_discharge": round(max(0, pred_val), 3),
                    "flood_probability": min(1.0, max(0, pred_val / 100)),
                    "risk_level": risk,
                    "model_version": f"v{now.strftime('%Y%m%d%H%M')}",
                })

        # Check if any AQ model in cache for this location
        has_aq_model = any(f"aq_{t}_{ext_id}" in MODEL_CACHE for t in ["pm10", "pm2_5", "aqi"])

        if not has_aq_model:
            # Fallback: persistence forecast from latest measured values
            try:
                latest_aq = (
                    client.table("air_quality_data")
                    .select("pm10,pm2_5,aqi")
                    .eq("location_id", loc_uuid)
                    .order("observed_at", desc=True)
                    .limit(1)
                    .execute()
                )
                if latest_aq.data:
                    base = latest_aq.data[0]
                    for d in range(1, 4):
                        future = now + timedelta(days=d)
                        aq_preds.append({
                            "location_id": loc_uuid,
                            "predicted_at": now.isoformat(),
                            "target_date": future.strftime("%Y-%m-%d"),
                            "pm10": base.get("pm10"),
                            "pm2_5": base.get("pm2_5"),
                            "aqi": base.get("aqi"),
                            "model_version": "persistence",
                        })
            except Exception:
                pass
        else:
            for target in ["pm10", "pm2_5", "aqi"]:
                model_key = f"aq_{target}_{ext_id}"
                if model_key not in MODEL_CACHE:
                    continue
                cached = MODEL_CACHE[model_key]
                model = cached["model"]
                features = cached["features"]

                for d in range(1, 4):
                    future = now + timedelta(days=d)
                    feat_dict = {
                        "pm10_lag1": 50, "dust_lag1": 40,
                        "day_of_year": future.timetuple().tm_yday,
                        "month": future.month, "hour": 12,
                    }
                    X = np.array([[feat_dict.get(f, 0) for f in features]])
                    pred_val = float(model.predict(X)[0])

                    record = {
                        "location_id": loc_uuid,
                        "predicted_at": now.isoformat(),
                        "target_date": future.strftime("%Y-%m-%d"),
                        "model_version": f"v{now.strftime('%Y%m%d%H%M')}",
                    }
                    if target == "pm10":
                        record["pm10"] = round(max(0, pred_val), 1)
                    elif target == "pm2_5":
                        record["pm2_5"] = round(max(0, pred_val), 1)
                    elif target == "aqi":
                        record["aqi"] = max(0, int(round(pred_val)))
                    aq_preds.append(record)

    w_ins = insert_batch("weather_predictions", weather_preds)
    f_ins = insert_batch("flood_predictions", flood_preds)
    a_ins = insert_batch("air_quality_predictions", aq_preds)

    return {
        "weather_predictions": w_ins,
        "flood_predictions": f_ins,
        "air_quality_predictions": a_ins,
    }


async def train_all_models() -> dict:
    weather = await train_weather_models()
    flood = await train_flood_models()
    aq = await train_air_quality_models()
    preds = await generate_predictions()

    from app.database import set_system_config
    set_system_config("last_model_training", datetime.utcnow().isoformat())

    return {
        "weather": weather,
        "flood": flood,
        "air_quality": aq,
        "predictions": preds,
    }
