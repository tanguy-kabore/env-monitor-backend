import csv
import io
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from app.database import get_supabase

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/export", tags=["Export"])

# ── Dataset catalogue ──────────────────────────────────────────────────────────
# Each entry fully describes the table: fields, units, source, notes.
DATASETS = {
    "weather_data": {
        "label": "Données météorologiques",
        "description": "Observations météorologiques actuelles et journalières collectées via Open-Meteo. Chaque enregistrement correspond à une mesure horodatée pour une ville.",
        "source": "Open-Meteo (open-meteo.com)",
        "standard": "WMO (Organisation Météorologique Mondiale)",
        "table": "weather_data",
        "time_col": "observed_at",
        "join": "locations(name,external_id)",
        "fields": [
            {"col": "observed_at",       "label": "Date/heure observation",   "unit": "ISO 8601 (UTC)",        "type": "datetime",  "note": "Horodatage UTC de la mesure"},
            {"col": "location_name",     "label": "Ville",                     "unit": "—",                    "type": "text",      "note": "Nom de la localité"},
            {"col": "temperature",       "label": "Température instantanée",   "unit": "°C",                   "type": "float",     "note": "Température à 2 m du sol (mesure courante)"},
            {"col": "temperature_max",   "label": "Température maximale",      "unit": "°C",                   "type": "float",     "note": "Température max journalière"},
            {"col": "temperature_min",   "label": "Température minimale",      "unit": "°C",                   "type": "float",     "note": "Température min journalière"},
            {"col": "temperature_mean",  "label": "Température moyenne",       "unit": "°C",                   "type": "float",     "note": "Moyenne journalière"},
            {"col": "humidity",          "label": "Humidité relative",         "unit": "%",                    "type": "float",     "note": "Humidité relative à 2 m du sol"},
            {"col": "precipitation",     "label": "Précipitations",            "unit": "mm",                   "type": "float",     "note": "Cumul de pluie sur la période"},
            {"col": "wind_speed",        "label": "Vitesse du vent",           "unit": "km/h",                 "type": "float",     "note": "Vitesse du vent à 10 m du sol"},
            {"col": "wind_direction",    "label": "Direction du vent",         "unit": "° (degrés)",           "type": "float",     "note": "0°=Nord, 90°=Est, 180°=Sud, 270°=Ouest"},
            {"col": "pressure",          "label": "Pression atmosphérique",    "unit": "hPa",                  "type": "float",     "note": "Pression au niveau de la mer"},
            {"col": "cloud_cover",       "label": "Couverture nuageuse",       "unit": "%",                    "type": "float",     "note": "Pourcentage de ciel couvert"},
            {"col": "uv_index",          "label": "Indice UV",                 "unit": "—",                    "type": "float",     "note": "Indice UV max journalier (0–11+)"},
            {"col": "evapotranspiration","label": "Évapotranspiration",        "unit": "mm/jour",              "type": "float",     "note": "ET0 FAO-56 Penman-Monteith"},
            {"col": "source",            "label": "Source de données",         "unit": "—",                    "type": "text",      "note": "open_meteo = temps réel, open_meteo_forecast = prévision journalière"},
        ],
    },
    "air_quality_data": {
        "label": "Qualité de l'air",
        "description": "Concentrations de polluants atmosphériques et indice de qualité de l'air (AQI européen – EAQI). Données collectées via Open-Meteo Air Quality.",
        "source": "Open-Meteo Air Quality (open-meteo.com)",
        "standard": "EAQI – European Air Quality Index (Agence Européenne pour l'Environnement)",
        "table": "air_quality_data",
        "time_col": "observed_at",
        "join": "locations(name,external_id)",
        "fields": [
            {"col": "observed_at", "label": "Date/heure observation",  "unit": "ISO 8601 (UTC)",    "type": "datetime", "note": "Horodatage UTC"},
            {"col": "location_name", "label": "Ville",                 "unit": "—",                 "type": "text",     "note": ""},
            {"col": "pm2_5",     "label": "PM2.5",                     "unit": "µg/m³",             "type": "float",    "note": "Particules fines ≤ 2.5 µm. Seuil OMS : 15 µg/m³/jour"},
            {"col": "pm10",      "label": "PM10",                      "unit": "µg/m³",             "type": "float",    "note": "Particules fines ≤ 10 µm. Seuil OMS : 45 µg/m³/jour"},
            {"col": "no2",       "label": "Dioxyde d'azote (NO₂)",     "unit": "µg/m³",             "type": "float",    "note": "Polluant issu de la combustion. Seuil OMS : 25 µg/m³"},
            {"col": "so2",       "label": "Dioxyde de soufre (SO₂)",   "unit": "µg/m³",             "type": "float",    "note": "Polluant industriel. Seuil OMS : 40 µg/m³/24h"},
            {"col": "o3",        "label": "Ozone (O₃)",                "unit": "µg/m³",             "type": "float",    "note": "Ozone troposphérique. Seuil OMS : 100 µg/m³/8h"},
            {"col": "co",        "label": "Monoxyde de carbone (CO)",  "unit": "µg/m³",             "type": "float",    "note": "Gaz toxique, combustion incomplète"},
            {"col": "dust",      "label": "Poussière atmosphérique",   "unit": "µg/m³",             "type": "float",    "note": "Aérosols minéraux — très présents au Sahel"},
            {"col": "aqi",       "label": "Indice Qualité Air (AQI)",  "unit": "—",                 "type": "integer",  "note": "0–20 Bon, 21–40 Acceptable, 41–60 Modéré, 61–80 Mauvais, >80 Très mauvais (EAQI)"},
            {"col": "source",    "label": "Source",                    "unit": "—",                 "type": "text",     "note": ""},
        ],
    },
    "flood_data": {
        "label": "Données hydrologiques / Inondations",
        "description": "Débit fluvial et niveau de risque d'inondation par ville, fournis par le modèle GloFAS via Open-Meteo. Conforme aux standards WMO/GloFAS.",
        "source": "GloFAS / Open-Meteo River Discharge",
        "standard": "GloFAS – Global Flood Awareness System (Copernicus/ECMWF)",
        "table": "flood_data",
        "time_col": "observed_at",
        "join": "locations(name,external_id)",
        "fields": [
            {"col": "observed_at",      "label": "Date/heure observation", "unit": "ISO 8601 (UTC)",                   "type": "datetime", "note": ""},
            {"col": "location_name",    "label": "Ville",                  "unit": "—",                                "type": "text",     "note": ""},
            {"col": "river_discharge",  "label": "Débit fluvial",          "unit": "m³/s",                             "type": "float",    "note": "Volume d'eau écoulé par seconde dans le cours d'eau principal"},
            {"col": "water_level",      "label": "Niveau d'eau",           "unit": "m",                                "type": "float",    "note": "Hauteur d'eau (si disponible)"},
            {"col": "flood_risk_level", "label": "Niveau de risque",       "unit": "—",                                "type": "text",     "note": "low / moderate / high / extreme. Basé sur les percentiles GloFAS"},
            {"col": "source",           "label": "Source",                 "unit": "—",                                "type": "text",     "note": ""},
        ],
    },
    "drought_data": {
        "label": "Données de sécheresse (SPI)",
        "description": "Indicateurs de sécheresse basés sur le SPI (Standardized Precipitation Index) de l'OMM. Calculés à partir des données de précipitation.",
        "source": "Calcul interne à partir de Open-Meteo",
        "standard": "SPI – WMO-No. 1090 (Guide to meteorological instruments and methods of observation)",
        "table": "drought_data",
        "time_col": "observed_at",
        "join": "locations(name,external_id)",
        "fields": [
            {"col": "observed_at",       "label": "Date/heure observation",    "unit": "ISO 8601 (UTC)",          "type": "datetime", "note": ""},
            {"col": "location_name",     "label": "Ville",                      "unit": "—",                      "type": "text",     "note": ""},
            {"col": "precipitation_30d", "label": "Précipitations 30 jours",   "unit": "mm",                      "type": "float",    "note": "Cumul de précipitations sur les 30 derniers jours"},
            {"col": "precipitation_90d", "label": "Précipitations 90 jours",   "unit": "mm",                      "type": "float",    "note": "Cumul de précipitations sur les 90 derniers jours"},
            {"col": "soil_moisture",     "label": "Humidité du sol",           "unit": "m³/m³",                   "type": "float",    "note": "Fraction volumique d'eau dans le sol"},
            {"col": "evapotranspiration","label": "Évapotranspiration",        "unit": "mm/jour",                  "type": "float",    "note": "ET0 FAO-56"},
            {"col": "spi_value",         "label": "SPI (Indice de précip. norm.)", "unit": "—",                   "type": "float",    "note": "≥0 Normal, -1 à 0 Légèrement sec, -1.5 à -1 Modérément sec, ≤-2 Extrême"},
            {"col": "drought_level",     "label": "Niveau de sécheresse",      "unit": "—",                       "type": "text",     "note": "normal / moderate / severe / extreme"},
            {"col": "source",            "label": "Source",                    "unit": "—",                        "type": "text",     "note": ""},
        ],
    },
    "climate_data": {
        "label": "Données climatiques long terme",
        "description": "Moyennes climatiques mensuelles et annuelles pour l'analyse des tendances. Sources : ERA5 (ECMWF) et Open-Meteo Historical.",
        "source": "ERA5 (ECMWF) / Open-Meteo Historical Weather",
        "standard": "ISO 19156 – Observations & Measurements / NASA POWER",
        "table": "climate_data",
        "time_col": "year",
        "join": "locations(name,external_id)",
        "fields": [
            {"col": "year",                "label": "Année",                    "unit": "—",            "type": "integer", "note": "Année de la mesure"},
            {"col": "month",               "label": "Mois",                     "unit": "1–12",         "type": "integer", "note": "1=Janvier … 12=Décembre. NULL = donnée annuelle"},
            {"col": "location_name",       "label": "Ville",                    "unit": "—",            "type": "text",    "note": ""},
            {"col": "avg_temperature",     "label": "Température moyenne",      "unit": "°C",           "type": "float",   "note": ""},
            {"col": "max_temperature",     "label": "Température maximale",     "unit": "°C",           "type": "float",   "note": ""},
            {"col": "min_temperature",     "label": "Température minimale",     "unit": "°C",           "type": "float",   "note": ""},
            {"col": "total_precipitation", "label": "Précipitations totales",   "unit": "mm",           "type": "float",   "note": "Cumul mensuel ou annuel"},
            {"col": "avg_humidity",        "label": "Humidité moyenne",         "unit": "%",            "type": "float",   "note": ""},
            {"col": "avg_wind_speed",      "label": "Vitesse vent moyenne",     "unit": "km/h",         "type": "float",   "note": ""},
            {"col": "solar_radiation",     "label": "Rayonnement solaire",      "unit": "MJ/m²/jour",   "type": "float",   "note": "Rayonnement solaire global"},
            {"col": "source",              "label": "Source",                   "unit": "—",            "type": "text",    "note": "era5 / weather_archive"},
        ],
    },
    "alerts": {
        "label": "Alertes environnementales",
        "description": "Alertes générées automatiquement par le système lorsque des seuils sont dépassés (débit, AQI, température, SPI).",
        "source": "Calcul interne Secheinon",
        "standard": "CAP – Common Alerting Protocol (ITU-T X.1303)",
        "table": "alerts",
        "time_col": "start_date",
        "join": "locations(name,external_id)",
        "fields": [
            {"col": "start_date",   "label": "Date de déclenchement", "unit": "ISO 8601 (UTC)",                                                         "type": "datetime", "note": ""},
            {"col": "end_date",     "label": "Date de résolution",    "unit": "ISO 8601 (UTC)",                                                         "type": "datetime", "note": "NULL si alerte encore active"},
            {"col": "location_name","label": "Ville",                 "unit": "—",                                                                      "type": "text",     "note": ""},
            {"col": "alert_type",   "label": "Type d'alerte",         "unit": "—",                                                                      "type": "text",     "note": "flood / drought / air_quality / heat_wave / storm / climate"},
            {"col": "severity",     "label": "Sévérité",              "unit": "—",                                                                      "type": "text",     "note": "info / warning / danger / critical"},
            {"col": "title",        "label": "Titre",                 "unit": "—",                                                                      "type": "text",     "note": ""},
            {"col": "description",  "label": "Description",           "unit": "—",                                                                      "type": "text",     "note": ""},
            {"col": "is_active",    "label": "Active",                "unit": "true/false",                                                             "type": "boolean",  "note": ""},
        ],
    },
    "locations": {
        "label": "Localités surveillées",
        "description": "Référentiel géographique de toutes les villes et régions surveillées par le système.",
        "source": "Configuration système (app_config.yaml)",
        "standard": "ISO 19115 – Geographic Information Metadata",
        "table": "locations",
        "time_col": None,
        "join": None,
        "fields": [
            {"col": "external_id",  "label": "Identifiant externe",  "unit": "—",          "type": "text",    "note": "Clé utilisée dans les API (ex: ouagadougou)"},
            {"col": "name",         "label": "Nom",                  "unit": "—",          "type": "text",    "note": ""},
            {"col": "type",         "label": "Type",                 "unit": "—",          "type": "text",    "note": "region / province / city / quartier"},
            {"col": "latitude",     "label": "Latitude",             "unit": "° (WGS84)",  "type": "float",   "note": "Coordonnée géographique (WGS84)"},
            {"col": "longitude",    "label": "Longitude",            "unit": "° (WGS84)",  "type": "float",   "note": "Coordonnée géographique (WGS84)"},
            {"col": "elevation",    "label": "Altitude",             "unit": "m",          "type": "float",   "note": "Altitude en mètres (WGS84)"},
            {"col": "population",   "label": "Population",           "unit": "habitants",  "type": "integer", "note": "Population estimée"},
            {"col": "is_active",    "label": "Actif",                "unit": "true/false", "type": "boolean", "note": ""},
        ],
    },
}


def _build_select(ds_key: str) -> str:
    """Build supabase select string with location join where needed."""
    ds = DATASETS[ds_key]
    if ds["join"]:
        return f"*, locations(name, external_id)"
    return "*"


def _flatten(row: dict, ds_key: str) -> dict:
    """Flatten joined location fields into the row."""
    flat = {k: v for k, v in row.items() if k != "locations"}
    loc = row.get("locations")
    if loc:
        flat["location_name"] = loc.get("name")
        flat["location_external_id"] = loc.get("external_id")
    return flat


# ── Catalogue endpoint ──────────────────────────────────────────────────────────

@router.get("/catalogue")
async def get_catalogue():
    """Return the full dataset catalogue with field descriptions."""
    client = get_supabase()
    result = []
    for key, ds in DATASETS.items():
        # Get row count
        try:
            count_res = client.table(ds["table"]).select("id", count="exact").limit(0).execute()
            count = count_res.count or 0
        except Exception:
            count = None
        result.append({
            "key": key,
            "label": ds["label"],
            "description": ds["description"],
            "source": ds["source"],
            "standard": ds["standard"],
            "row_count": count,
            "fields": ds["fields"],
        })
    return {"datasets": result}


# ── Preview endpoint ────────────────────────────────────────────────────────────

@router.get("/preview/{dataset}")
async def preview_dataset(
    dataset: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    location_id: Optional[str] = Query(None),
):
    if dataset not in DATASETS:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found")

    ds = DATASETS[dataset]
    client = get_supabase()
    sel = _build_select(dataset)

    query = client.table(ds["table"]).select(sel, count="exact")

    if location_id and ds["join"]:
        from app.database import resolve_location_uuid
        try:
            uuid = resolve_location_uuid(location_id)
            query = query.eq("location_id", uuid)
        except Exception:
            pass

    if ds["time_col"] and ds["time_col"] not in ("year",):
        query = query.order(ds["time_col"], desc=True)
    elif ds["time_col"] == "year":
        query = query.order("year", desc=True).order("month", desc=True)

    result = query.range(offset, offset + limit - 1).execute()
    rows = [_flatten(r, dataset) for r in (result.data or [])]

    return {
        "dataset": dataset,
        "total": result.count,
        "offset": offset,
        "limit": limit,
        "rows": rows,
        "fields": ds["fields"],
    }


# ── Download endpoint ───────────────────────────────────────────────────────────

@router.get("/download/{dataset}")
async def download_dataset(
    dataset: str,
    fmt: str = Query("csv", pattern="^(csv|json)$"),
    days: Optional[int] = Query(None, ge=1, le=3650),
    location_id: Optional[str] = Query(None),
):
    if dataset not in DATASETS:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found")

    ds = DATASETS[dataset]
    client = get_supabase()
    sel = _build_select(dataset)
    query = client.table(ds["table"]).select(sel)

    if location_id and ds["join"]:
        from app.database import resolve_location_uuid
        try:
            uuid = resolve_location_uuid(location_id)
            query = query.eq("location_id", uuid)
        except Exception:
            pass

    if days and ds["time_col"] and ds["time_col"] not in ("year",):
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        query = query.gte(ds["time_col"], since)

    if ds["time_col"] and ds["time_col"] not in ("year",):
        query = query.order(ds["time_col"], desc=False)
    elif ds["time_col"] == "year":
        query = query.order("year", desc=False).order("month", desc=False)

    result = query.limit(50000).execute()
    rows = [_flatten(r, dataset) for r in (result.data or [])]

    fname_base = f"{dataset}_{datetime.now(timezone.utc).strftime('%Y%m%d')}"

    if fmt == "json":
        content = json.dumps({
            "dataset": dataset,
            "label": ds["label"],
            "source": ds["source"],
            "standard": ds["standard"],
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "fields": ds["fields"],
            "data": rows,
        }, ensure_ascii=False, indent=2)
        return StreamingResponse(
            iter([content]),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.json"'},
        )

    # CSV
    if not rows:
        csv_content = ""
    else:
        # Use field col names as headers, add human labels as comment row
        field_cols = [f["col"] for f in ds["fields"]]
        field_labels = [f["label"] for f in ds["fields"]]
        field_units = [f["unit"] for f in ds["fields"]]

        # Add extra cols that may appear in flat row but not in field list
        extra = [k for k in rows[0].keys() if k not in field_cols and k not in ("id", "location_id", "raw_data", "created_at", "updated_at", "metadata")]
        all_cols = field_cols + extra

        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        # Row 1: column technical names
        writer.writerow(all_cols)
        # Row 2: human labels
        label_row = []
        for c in all_cols:
            f = next((f for f in ds["fields"] if f["col"] == c), None)
            label_row.append(f["label"] if f else c)
        writer.writerow(["# " + l for l in label_row])
        # Row 3: units
        unit_row = []
        for c in all_cols:
            f = next((f for f in ds["fields"] if f["col"] == c), None)
            unit_row.append(f["unit"] if f else "—")
        writer.writerow(["# " + u for u in unit_row])
        # Data rows
        for row in rows:
            writer.writerow([row.get(c, "") for c in all_cols])
        csv_content = "\uFEFF" + buf.getvalue()

    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname_base}.csv"'},
    )
