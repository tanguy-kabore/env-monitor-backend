# Secheinon Burkina — Backend

> **Version 1.0.0-alpha** · Build 20260513-001

API REST FastAPI pour le système national de surveillance environnementale du Burkina Faso.

---

## Structure

```
backend/
├── config/
│   ├── app_config.yaml         # Configuration pays, régions, villes, seuils, APIs
│   └── supabase_schema.sql     # Schéma PostgreSQL complet (tables, index, RLS)
├── app/
│   ├── config.py               # Chargeur YAML singleton (résolution absolue via __file__)
│   ├── database.py             # Client Supabase, upsert batch, helpers UUID
│   ├── cache.py                # Cache mémoire TTL simple
│   ├── routers/
│   │   ├── system.py           # Santé, config, statut, init, reset-status
│   │   ├── locations.py        # Arbre géographique régions/provinces/villes
│   │   ├── weather.py          # Météo courante, historique, prévisions, ML
│   │   ├── floods.py           # Débit fluvial, risque, historique, ML
│   │   ├── air_quality.py      # AQI, polluants, historique, prévisions CAMS
│   │   ├── drought.py          # SPI, sécheresse, historique, carte
│   │   ├── climate.py          # Tendances climatiques long terme
│   │   ├── alerts.py           # Alertes actives, historique, stats
│   │   ├── report.py           # Rapport de synthèse multi-domaines
│   │   └── export.py           # Export CSV/JSON avec filtres
│   └── services/
│       ├── data_collector.py   # Collecteurs météo, inondations, air, sécheresse, climat
│       ├── ml_engine.py        # Entraînement Gradient Boosting / Random Forest
│       ├── open_meteo.py       # Client Open-Meteo (weather, air quality, flood, archive)
│       ├── nasa_power.py       # Client NASA POWER (données climatiques)
│       └── scheduler.py        # Tâches périodiques APScheduler
├── main.py                     # Point d'entrée + endpoint /api/dashboard
├── requirements.txt
├── .env / .env.example
├── .gitignore
└── README.md
```

---

## Prérequis

- Python **3.11+**
- Compte [Supabase](https://supabase.com) (gratuit)

---

## Installation

```bash
python -m venv venv
.\venv\Scripts\activate          # Windows
# source venv/bin/activate       # Linux / macOS

pip install -r requirements.txt

cp .env.example .env
# Éditer .env
```

### Configuration `.env`

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key
# CONFIG_PATH est optionnel — par défaut : config/app_config.yaml
```

---

## Base de données

1. Créer un projet sur [supabase.com](https://supabase.com)
2. Ouvrir **SQL Editor** et exécuter `config/supabase_schema.sql`

Le schéma crée les tables suivantes :

| Table | Description |
|---|---|
| `locations` | Régions, provinces et villes |
| `weather_data` | Données météo horaires (ERA5 / Open-Meteo) |
| `weather_predictions` | Prédictions ML météo |
| `flood_data` | Débit fluvial GloFAS |
| `flood_predictions` | Prédictions ML inondations |
| `air_quality_data` | AQI et polluants CAMS |
| `air_quality_predictions` | Prédictions ML qualité air |
| `drought_data` | Indice SPI et précipitations |
| `drought_predictions` | Prédictions ML sécheresse |
| `climate_data` | Données climatiques annuelles / mensuelles |
| `ml_models` | Métadonnées des modèles entraînés |
| `alerts` | Alertes environnementales |
| `collection_log` | Journal de collecte |
| `system_config` | Configuration runtime (statut init, etc.) |

---

## Lancement

```bash
.\venv\Scripts\activate
python main.py
# API : http://localhost:8000
# Docs : http://localhost:8000/docs
```

### Première initialisation

```bash
curl -X POST http://localhost:8000/api/system/initialize
```

Ou via l'interface : `http://localhost:3000/system` → **Initialiser le système**

Le processus :
1. Insère les villes en base
2. Collecte 90 jours de météo, inondations, qualité air, sécheresse
3. Collecte 30 ans de données climatiques (NASA POWER)
4. Entraîne les modèles ML pour chaque domaine et chaque ville

---

## Endpoints complets

| Catégorie | Méthode | Endpoint | Description |
|---|---|---|---|
| Système | GET | `/api/system/health` | Santé et connectivité DB |
| Système | GET | `/api/system/config` | Config complète (version, pays…) |
| Système | GET | `/api/system/status` | Statut initialisation |
| Système | POST | `/api/system/initialize` | Lancer l'initialisation complète |
| Système | POST | `/api/system/reset-status` | Réinitialiser le statut |
| Dashboard | GET | `/api/dashboard` | Agrégat toutes villes |
| Localisations | GET | `/api/locations/tree` | Arbre géographique |
| Météo | GET | `/api/weather/current/{id}` | Conditions actuelles |
| Météo | GET | `/api/weather/forecast/{id}` | Prévisions 16 jours |
| Météo | GET | `/api/weather/history/{id}?days=N` | Historique |
| Météo | GET | `/api/weather/predictions/{id}` | Prédictions ML |
| Inondations | GET | `/api/floods/forecast/{id}` | Prévisions débit |
| Inondations | GET | `/api/floods/history/{id}?days=N` | Historique débit |
| Inondations | GET | `/api/floods/predictions/{id}` | Prédictions ML |
| Inondations | GET | `/api/floods/risk-map` | Carte des risques nationale |
| Qualité air | GET | `/api/air-quality/current/{id}` | Données temps réel |
| Qualité air | GET | `/api/air-quality/history/{id}?days=N` | Historique AQI/polluants |
| Qualité air | GET | `/api/air-quality/forecast/{id}` | Prévisions CAMS 5 j |
| Qualité air | GET | `/api/air-quality/predictions/{id}` | Prédictions ML |
| Qualité air | GET | `/api/air-quality/map` | Carte nationale AQI |
| Sécheresse | GET | `/api/drought/current/{id}` | Indice SPI actuel |
| Sécheresse | GET | `/api/drought/history/{id}?days=N` | Historique SPI |
| Sécheresse | GET | `/api/drought/map` | Carte de sécheresse |
| Climat | GET | `/api/climate/trends/{id}?start_year=N` | Tendances long terme |
| Alertes | GET | `/api/alerts` | Alertes (filtres : type, severity, active) |
| Alertes | GET | `/api/alerts/stats` | Statistiques alertes |
| Alertes | POST | `/api/alerts/{id}/resolve` | Résoudre une alerte |
| Rapport | GET | `/api/report?location_id=X` | Rapport multi-domaines |
| Export | GET | `/api/export/catalogue` | Catalogue jeux de données |
| Export | GET | `/api/export/preview/{dataset}` | Aperçu données |
| Export | GET | `/api/export/download/{dataset}?fmt=csv\|json` | Téléchargement |

---

## Adaptation à un autre pays

Modifier **uniquement** `config/app_config.yaml` :

```yaml
app:
  name: "Nom du système"
country:
  name: "Nom du pays"
  code: "XX"
  timezone: "Continent/Ville"
regions:
  - name: "Région 1"
    cities:
      - name: "Ville 1"
        latitude: 12.3
        longitude: -1.5
        monitorable: true
```

Aucun code Python à modifier.

---

## Technologies

| Lib | Version | Rôle |
|---|---|---|
| **FastAPI** | ≥0.115 | Framework API async |
| **Uvicorn** | ≥0.30 | Serveur ASGI |
| **Supabase** | ≥2.9 | PostgreSQL + Auth + RLS |
| **httpx** | ≥0.27 | Client HTTP async |
| **scikit-learn** | ≥1.5 | ML — GradientBoosting, RandomForest |
| **pandas** | ≥2.2 | Manipulation données |
| **APScheduler** | ≥3.10 | Collecte périodique |
| **PyYAML** | ≥6.0 | Chargement configuration |
| **pydantic-settings** | ≥2.5 | Variables d'environnement |

---

## Sources de données

| API | Endpoint | Données |
|---|---|---|
| Open-Meteo Weather | `api.open-meteo.com/v1/forecast` | Météo + prévisions 16 j |
| Open-Meteo Archive (ERA5) | `archive-api.open-meteo.com/v1/archive` | Historique météo |
| Open-Meteo Air Quality | `air-quality-api.open-meteo.com/v1/air-quality` | AQI, PM2.5, PM10, O₃… |
| Open-Meteo Flood (GloFAS) | `flood-api.open-meteo.com/v1/flood` | Débit fluvial |
| NASA POWER | `power.larc.nasa.gov/api/temporal/daily` | Données climatiques |

---

## Licence

Usage gouvernemental — Burkina Faso
