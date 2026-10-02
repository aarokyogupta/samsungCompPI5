-- Spatial and demographic context needed by aiEngine/indicatorCalculator.py
-- Adds the juvenile recruitment k-value baseline plus habitat zone and human infrastructure layers

-- Baseline juvenile-to-adult ratio (demographic k-value) below which recruitment is penalised
ALTER TABLE species ADD COLUMN juvenile_recruitment_ratio REAL
    CHECK (juvenile_recruitment_ratio IS NULL OR juvenile_recruitment_ratio >= 0.0);

-- Habitat Zones

-- Circular ecological zones: core breeding areas, watering holes, migratory corridor waypoints and core range
CREATE TABLE IF NOT EXISTS habitat_zones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    species_id INTEGER REFERENCES species (id) ON UPDATE CASCADE ON DELETE CASCADE,
    zone_name TEXT NOT NULL CHECK (length(trim(zone_name)) > 0),
    zone_type TEXT NOT NULL CHECK (zone_type IN ('BREEDING', 'WATER_SOURCE', 'CORRIDOR', 'CORE_RANGE')),
    latitude REAL NOT NULL CHECK (latitude BETWEEN -90.0 AND 90.0),
    longitude REAL NOT NULL CHECK (longitude BETWEEN -180.0 AND 180.0),
    radius_m REAL NOT NULL CHECK (radius_m > 0.0),
    -- Threat penalty multiplier applied inside the zone (NULL uses the config default for the zone type)
    risk_multiplier REAL CHECK (risk_multiplier IS NULL OR risk_multiplier > 0.0),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    -- Registered as a SpatiaLite POINT column below; STRICT tables require the BLOB declaration
    geom BLOB,
    UNIQUE (species_id, zone_name)
) STRICT;

-- Register the 2D WGS84 zone centre point with SpatiaLite
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM geometry_columns
        WHERE f_table_name = 'habitat_zones' AND f_geometry_column = 'geom'
    ) THEN RecoverGeometryColumn('habitat_zones', 'geom', 4326, 'POINT', 'XY')
    ELSE 1
END;

-- Created before the spatial index so SpatiaLite's own insert trigger fires first
CREATE TRIGGER IF NOT EXISTS habitat_zones_geom_insert
AFTER INSERT ON habitat_zones
WHEN NEW.geom IS NULL
BEGIN
    UPDATE habitat_zones
    SET geom = MakePoint(NEW.longitude, NEW.latitude, 4326)
    WHERE id = NEW.id;
END;

-- R-Tree spatial index for zone lookups around an assessment scope
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM geometry_columns
        WHERE f_table_name = 'habitat_zones' AND f_geometry_column = 'geom' AND spatial_index_enabled = 1
    ) THEN CreateSpatialIndex('habitat_zones', 'geom')
    ELSE 1
END;

-- Keep geometry in sync when a zone is moved
CREATE TRIGGER IF NOT EXISTS habitat_zones_geom_update
AFTER UPDATE OF latitude, longitude ON habitat_zones
BEGIN
    UPDATE habitat_zones
    SET geom = MakePoint(NEW.longitude, NEW.latitude, 4326)
    WHERE id = NEW.id;
END;

CREATE INDEX IF NOT EXISTS idx_habitat_zones_species_type
ON habitat_zones (species_id, zone_type, active);

-- Human Infrastructure

-- Newly detected roads, fences and settlements used for telemetry path fragmentation analysis
CREATE TABLE IF NOT EXISTS infrastructure_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    feature_type TEXT NOT NULL
        CHECK (feature_type IN ('ROAD', 'FENCE', 'SETTLEMENT', 'AGRICULTURE', 'PIPELINE', 'POWER_LINE', 'OTHER')),
    feature_name TEXT,
    -- WGS84 Well-Known Text, e.g. 'LINESTRING(36.80 -1.30, 36.85 -1.28)'
    geometry_wkt TEXT NOT NULL CHECK (length(trim(geometry_wkt)) > 0),
    detected_at TEXT NOT NULL CHECK (
        (detected_at GLOB '*Z' OR detected_at GLOB '*+00:00') AND julianday(detected_at) IS NOT NULL
    ),
    detected_epoch_ms INTEGER GENERATED ALWAYS AS (
        CAST(round((julianday(detected_at) - 2440587.5) * 86400000.0) AS INTEGER)
    ) STORED,
    removed_at TEXT CHECK (removed_at IS NULL OR julianday(removed_at) IS NOT NULL),
    source TEXT NOT NULL DEFAULT 'SURVEY' CHECK (source IN ('SURVEY', 'SATELLITE', 'DRONE', 'RANGER_REPORT')),
    -- Registered as a generic SpatiaLite GEOMETRY column so lines and polygons share one layer
    geom BLOB
) STRICT;

SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM geometry_columns
        WHERE f_table_name = 'infrastructure_features' AND f_geometry_column = 'geom'
    ) THEN RecoverGeometryColumn('infrastructure_features', 'geom', 4326, 'GEOMETRY', 'XY')
    ELSE 1
END;

-- Parse the WKT into native geometry; created before the spatial index for trigger ordering
CREATE TRIGGER IF NOT EXISTS infrastructure_features_geom_insert
AFTER INSERT ON infrastructure_features
WHEN NEW.geom IS NULL
BEGIN
    UPDATE infrastructure_features
    SET geom = GeomFromText(NEW.geometry_wkt, 4326)
    WHERE id = NEW.id;
END;

-- R-Tree spatial index used to prefilter ST_Intersects crossing checks
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM geometry_columns
        WHERE f_table_name = 'infrastructure_features' AND f_geometry_column = 'geom' AND spatial_index_enabled = 1
    ) THEN CreateSpatialIndex('infrastructure_features', 'geom')
    ELSE 1
END;

CREATE TRIGGER IF NOT EXISTS infrastructure_features_geom_update
AFTER UPDATE OF geometry_wkt ON infrastructure_features
BEGIN
    UPDATE infrastructure_features
    SET geom = GeomFromText(NEW.geometry_wkt, 4326)
    WHERE id = NEW.id;
END;

-- B-Tree index for filtering features that existed during an assessment window
CREATE INDEX IF NOT EXISTS idx_infrastructure_features_detected
ON infrastructure_features (detected_epoch_ms, feature_type);
