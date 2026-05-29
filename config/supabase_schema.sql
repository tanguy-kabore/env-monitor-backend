-- Secheinon Environmental Monitoring System - Database Schema
-- Compliant with ISO 19115 (Geographic Information - Metadata)
-- and WMO standards for environmental data

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";

-- ============================================================
-- LOCATIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS locations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    external_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    name_local TEXT,
    type TEXT NOT NULL CHECK (type IN ('region', 'province', 'city', 'quartier')),
    parent_id UUID REFERENCES locations(id) ON DELETE SET NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    elevation DOUBLE PRECISION,
    population INTEGER,
    metadata JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_locations_type ON locations(type);
CREATE INDEX idx_locations_parent ON locations(parent_id);
CREATE INDEX idx_locations_external ON locations(external_id);

-- ============================================================
-- WEATHER DATA (ISO 19156 - Observations & Measurements)
-- ============================================================
CREATE TABLE IF NOT EXISTS weather_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    observed_at TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION,
    temperature_max DOUBLE PRECISION,
    temperature_min DOUBLE PRECISION,
    temperature_mean DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    cloud_cover DOUBLE PRECISION,
    uv_index DOUBLE PRECISION,
    evapotranspiration DOUBLE PRECISION,
    source TEXT NOT NULL,
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_weather_location_time ON weather_data(location_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_weather_location_time ON weather_data(location_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_observed ON weather_data(observed_at DESC);

-- ============================================================
-- WEATHER PREDICTIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS weather_predictions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    predicted_at TIMESTAMPTZ NOT NULL,
    target_date TIMESTAMPTZ NOT NULL,
    temperature_max DOUBLE PRECISION,
    temperature_min DOUBLE PRECISION,
    temperature_mean DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    model_version TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_weather_pred_location ON weather_predictions(location_id, target_date DESC);

-- ============================================================
-- FLOOD DATA (GloFAS compliant)
-- ============================================================
CREATE TABLE IF NOT EXISTS flood_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    observed_at TIMESTAMPTZ NOT NULL,
    river_discharge DOUBLE PRECISION,
    water_level DOUBLE PRECISION,
    flood_risk_level TEXT CHECK (flood_risk_level IN ('low', 'moderate', 'high', 'extreme')),
    source TEXT NOT NULL,
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_flood_location_time ON flood_data(location_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_flood_location_time ON flood_data(location_id, observed_at DESC);

-- ============================================================
-- FLOOD PREDICTIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS flood_predictions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    predicted_at TIMESTAMPTZ NOT NULL,
    target_date TIMESTAMPTZ NOT NULL,
    river_discharge DOUBLE PRECISION,
    flood_probability DOUBLE PRECISION,
    risk_level TEXT CHECK (risk_level IN ('low', 'moderate', 'high', 'extreme')),
    model_version TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_flood_pred_location ON flood_predictions(location_id, target_date DESC);

-- ============================================================
-- AIR QUALITY DATA (EAQI - European Air Quality Index)
-- ============================================================
CREATE TABLE IF NOT EXISTS air_quality_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    observed_at TIMESTAMPTZ NOT NULL,
    pm2_5 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    no2 DOUBLE PRECISION,
    so2 DOUBLE PRECISION,
    o3 DOUBLE PRECISION,
    co DOUBLE PRECISION,
    dust DOUBLE PRECISION,
    aqi INTEGER,
    source TEXT NOT NULL,
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_aq_location_time ON air_quality_data(location_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_aq_location_time ON air_quality_data(location_id, observed_at DESC);

-- ============================================================
-- AIR QUALITY PREDICTIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS air_quality_predictions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    predicted_at TIMESTAMPTZ NOT NULL,
    target_date TIMESTAMPTZ NOT NULL,
    pm2_5 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    dust DOUBLE PRECISION,
    aqi INTEGER,
    model_version TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_aq_pred_location ON air_quality_predictions(location_id, target_date DESC);

-- ============================================================
-- DROUGHT DATA (SPI - Standardized Precipitation Index, WMO)
-- ============================================================
CREATE TABLE IF NOT EXISTS drought_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    observed_at TIMESTAMPTZ NOT NULL,
    precipitation_30d DOUBLE PRECISION,
    precipitation_90d DOUBLE PRECISION,
    soil_moisture DOUBLE PRECISION,
    evapotranspiration DOUBLE PRECISION,
    spi_value DOUBLE PRECISION,
    drought_level TEXT CHECK (drought_level IN ('normal', 'moderate', 'severe', 'extreme')),
    source TEXT NOT NULL,
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_drought_location_time ON drought_data(location_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_drought_location_time ON drought_data(location_id, observed_at DESC);

-- ============================================================
-- DROUGHT PREDICTIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS drought_predictions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    predicted_at TIMESTAMPTZ NOT NULL,
    target_date TIMESTAMPTZ NOT NULL,
    spi_predicted DOUBLE PRECISION,
    drought_probability DOUBLE PRECISION,
    risk_level TEXT CHECK (risk_level IN ('normal', 'moderate', 'severe', 'extreme')),
    model_version TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_drought_pred_location ON drought_predictions(location_id, target_date DESC);

-- ============================================================
-- CLIMATE DATA (Long-term, NASA POWER compliant)
-- ============================================================
CREATE TABLE IF NOT EXISTS climate_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    year INTEGER NOT NULL,
    month INTEGER,
    avg_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    min_temperature DOUBLE PRECISION,
    total_precipitation DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    avg_wind_speed DOUBLE PRECISION,
    solar_radiation DOUBLE PRECISION,
    source TEXT NOT NULL,
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_climate_location_year ON climate_data(location_id, year DESC, month);

-- ============================================================
-- ML MODEL METADATA
-- ============================================================
CREATE TABLE IF NOT EXISTS ml_models (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_type TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    location_id UUID REFERENCES locations(id) ON DELETE SET NULL,
    metrics JSONB DEFAULT '{}',
    parameters JSONB DEFAULT '{}',
    feature_importance JSONB DEFAULT '{}',
    data_points_used INTEGER,
    training_duration_seconds DOUBLE PRECISION,
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'training', 'retired', 'failed')),
    trained_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_ml_models_type ON ml_models(model_type, status);

-- ============================================================
-- ALERTS
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    alert_type TEXT NOT NULL CHECK (alert_type IN ('flood', 'drought', 'air_quality', 'heat_wave', 'storm', 'climate')),
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'danger', 'critical')),
    title TEXT NOT NULL,
    description TEXT,
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_alerts_active ON alerts(is_active, alert_type);
CREATE INDEX idx_alerts_location ON alerts(location_id, is_active);

-- ============================================================
-- DATA COLLECTION LOG
-- ============================================================
CREATE TABLE IF NOT EXISTS collection_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source TEXT NOT NULL,
    data_type TEXT NOT NULL,
    locations_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    status TEXT DEFAULT 'success' CHECK (status IN ('success', 'partial', 'failed')),
    error_message TEXT,
    duration_seconds DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_collection_log_time ON collection_log(created_at DESC);

-- ============================================================
-- SYSTEM CONFIGURATION
-- ============================================================
CREATE TABLE IF NOT EXISTS system_config (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key TEXT UNIQUE NOT NULL,
    value JSONB NOT NULL,
    description TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert default system config
INSERT INTO system_config (key, value, description) VALUES
    ('last_historical_load', '"never"', 'Timestamp of last historical data load'),
    ('last_model_training', '"never"', 'Timestamp of last ML model training'),
    ('app_initialized', 'false', 'Whether the app has been initialized with historical data')
ON CONFLICT (key) DO NOTHING;

-- ============================================================
-- ROW LEVEL SECURITY (RLS)
-- ============================================================
ALTER TABLE locations ENABLE ROW LEVEL SECURITY;
ALTER TABLE weather_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE weather_predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE flood_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE flood_predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE air_quality_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE air_quality_predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE drought_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE drought_predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE climate_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE ml_models ENABLE ROW LEVEL SECURITY;
ALTER TABLE alerts ENABLE ROW LEVEL SECURITY;
ALTER TABLE collection_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE system_config ENABLE ROW LEVEL SECURITY;

-- Public read policies
CREATE POLICY "Public read locations" ON locations FOR SELECT USING (true);
CREATE POLICY "Public read weather_data" ON weather_data FOR SELECT USING (true);
CREATE POLICY "Public read weather_predictions" ON weather_predictions FOR SELECT USING (true);
CREATE POLICY "Public read flood_data" ON flood_data FOR SELECT USING (true);
CREATE POLICY "Public read flood_predictions" ON flood_predictions FOR SELECT USING (true);
CREATE POLICY "Public read air_quality_data" ON air_quality_data FOR SELECT USING (true);
CREATE POLICY "Public read air_quality_predictions" ON air_quality_predictions FOR SELECT USING (true);
CREATE POLICY "Public read drought_data" ON drought_data FOR SELECT USING (true);
CREATE POLICY "Public read drought_predictions" ON drought_predictions FOR SELECT USING (true);
CREATE POLICY "Public read climate_data" ON climate_data FOR SELECT USING (true);
CREATE POLICY "Public read ml_models" ON ml_models FOR SELECT USING (true);
CREATE POLICY "Public read alerts" ON alerts FOR SELECT USING (true);
CREATE POLICY "Public read collection_log" ON collection_log FOR SELECT USING (true);
CREATE POLICY "Public read system_config" ON system_config FOR SELECT USING (true);

-- Service role insert/update policies (for backend)
CREATE POLICY "Service insert locations" ON locations FOR INSERT WITH CHECK (true);
CREATE POLICY "Service update locations" ON locations FOR UPDATE USING (true);
CREATE POLICY "Service insert weather_data" ON weather_data FOR INSERT WITH CHECK (true);
CREATE POLICY "Service insert weather_predictions" ON weather_predictions FOR INSERT WITH CHECK (true);
CREATE POLICY "Service insert flood_data" ON flood_data FOR INSERT WITH CHECK (true);
CREATE POLICY "Service insert flood_predictions" ON flood_predictions FOR INSERT WITH CHECK (true);
CREATE POLICY "Service insert air_quality_data" ON air_quality_data FOR INSERT WITH CHECK (true);
CREATE POLICY "Service insert air_quality_predictions" ON air_quality_predictions FOR INSERT WITH CHECK (true);
CREATE POLICY "Service insert drought_data" ON drought_data FOR INSERT WITH CHECK (true);
CREATE POLICY "Service insert drought_predictions" ON drought_predictions FOR INSERT WITH CHECK (true);
CREATE POLICY "Service insert climate_data" ON climate_data FOR INSERT WITH CHECK (true);
CREATE POLICY "Service insert ml_models" ON ml_models FOR INSERT WITH CHECK (true);
CREATE POLICY "Service update ml_models" ON ml_models FOR UPDATE USING (true);
CREATE POLICY "Service insert alerts" ON alerts FOR INSERT WITH CHECK (true);
CREATE POLICY "Service update alerts" ON alerts FOR UPDATE USING (true);
CREATE POLICY "Service insert collection_log" ON collection_log FOR INSERT WITH CHECK (true);
CREATE POLICY "Service insert system_config" ON system_config FOR INSERT WITH CHECK (true);
CREATE POLICY "Service update system_config" ON system_config FOR UPDATE USING (true);

-- Run this in Supabase SQL Editor to add unique constraints
-- Required for upsert deduplication to work in insert_batch()

-- Step 1: Remove existing duplicates (keep only latest created_at per group)
DELETE FROM weather_data a
USING weather_data b
WHERE a.id < b.id
  AND a.location_id = b.location_id
  AND a.observed_at = b.observed_at;

DELETE FROM flood_data a
USING flood_data b
WHERE a.id < b.id
  AND a.location_id = b.location_id
  AND a.observed_at = b.observed_at;

DELETE FROM air_quality_data a
USING air_quality_data b
WHERE a.id < b.id
  AND a.location_id = b.location_id
  AND a.observed_at = b.observed_at;

DELETE FROM drought_data a
USING drought_data b
WHERE a.id < b.id
  AND a.location_id = b.location_id
  AND a.observed_at = b.observed_at;

-- Step 2: Create unique indexes
CREATE UNIQUE INDEX IF NOT EXISTS uq_weather_location_time ON weather_data(location_id, observed_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_flood_location_time ON flood_data(location_id, observed_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_aq_location_time ON air_quality_data(location_id, observed_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_drought_location_time ON drought_data(location_id, observed_at);
