-- ICMIS core relational schema, applied by database/dbManager.py before numbered migrations
-- Every statement is idempotent so the baseline can be re-applied safely; changes to columns of
-- existing tables must go into database/schemaMigrations instead of being edited here
-- Timestamps are ISO 8601 UTC text (ending in Z or +00:00) with a generated epoch-millisecond
-- column used for indexing, so mixed UTC suffixes still sort and range-filter correctly
-- Requires SQLite 3.38+ (STRICT tables and JSON functions) and SpatiaLite

-- Biological Foundation

-- Root taxonomy dimension with population thresholds and physiological baselines
CREATE TABLE IF NOT EXISTS species (
    id INTEGER PRIMARY KEY,
    scientific_name TEXT NOT NULL UNIQUE COLLATE NOCASE CHECK (length(trim(scientific_name)) > 0),
    common_name TEXT,
    kingdom TEXT NOT NULL DEFAULT 'Animalia',
    phylum TEXT,
    taxonomic_class TEXT,
    taxonomic_order TEXT,
    family TEXT,
    genus TEXT,
    iucn_status TEXT NOT NULL DEFAULT 'NE'
        CHECK (iucn_status IN ('NE', 'DD', 'LC', 'NT', 'VU', 'EN', 'CR', 'EW', 'EX')),
    minimum_viable_population INTEGER CHECK (minimum_viable_population IS NULL OR minimum_viable_population > 0),
    census_population_estimate INTEGER CHECK (census_population_estimate IS NULL OR census_population_estimate >= 0),
    -- 50/500 rule defaults for short-term inbreeding and long-term adaptive potential
    critical_ne_threshold INTEGER NOT NULL DEFAULT 50 CHECK (critical_ne_threshold > 0),
    viable_ne_threshold INTEGER NOT NULL DEFAULT 500 CHECK (viable_ne_threshold > 0),
    generation_time_years REAL CHECK (generation_time_years IS NULL OR generation_time_years > 0),
    -- Expected physiological ranges used as anomaly detection baselines
    resting_heart_rate_min_bpm REAL CHECK (resting_heart_rate_min_bpm IS NULL OR resting_heart_rate_min_bpm > 0),
    resting_heart_rate_max_bpm REAL CHECK (resting_heart_rate_max_bpm IS NULL OR resting_heart_rate_max_bpm > 0),
    body_temperature_min_c REAL,
    body_temperature_max_c REAL,
    max_speed_kmh REAL CHECK (max_speed_kmh IS NULL OR max_speed_kmh > 0),
    max_acceleration_g REAL CHECK (max_acceleration_g IS NULL OR max_acceleration_g > 0),
    home_range_km2 REAL CHECK (home_range_km2 IS NULL OR home_range_km2 > 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (critical_ne_threshold < viable_ne_threshold),
    CHECK (resting_heart_rate_min_bpm IS NULL OR resting_heart_rate_max_bpm IS NULL
        OR resting_heart_rate_min_bpm <= resting_heart_rate_max_bpm),
    CHECK (body_temperature_min_c IS NULL OR body_temperature_max_c IS NULL
        OR body_temperature_min_c <= body_temperature_max_c)
) STRICT;

-- Refresh the modification time whenever a species baseline changes
CREATE TRIGGER IF NOT EXISTS species_updated_at
AFTER UPDATE ON species
WHEN NEW.updated_at = OLD.updated_at
BEGIN
    UPDATE species SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = NEW.id;
END;

-- Geospatial Telemetry

-- High-frequency 3D collar tracking log linked to the species dimension
CREATE TABLE IF NOT EXISTS telemetry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    species_id INTEGER NOT NULL REFERENCES species (id) ON UPDATE CASCADE ON DELETE RESTRICT,
    animal_id TEXT NOT NULL CHECK (length(trim(animal_id)) > 0),
    device_id TEXT NOT NULL CHECK (length(trim(device_id)) > 0),
    recorded_at TEXT NOT NULL CHECK (
        recorded_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]*'
        AND (recorded_at GLOB '*Z' OR recorded_at GLOB '*+00:00')
        AND julianday(recorded_at) IS NOT NULL
        AND date(recorded_at) = substr(recorded_at, 1, 10)
    ),
    recorded_epoch_ms INTEGER GENERATED ALWAYS AS (
        CAST(round((julianday(recorded_at) - 2440587.5) * 86400000.0) AS INTEGER)
    ) STORED,
    latitude REAL NOT NULL CHECK (latitude BETWEEN -90.0 AND 90.0),
    longitude REAL NOT NULL CHECK (longitude BETWEEN -180.0 AND 180.0),
    altitude REAL,
    fix_quality TEXT NOT NULL DEFAULT 'UNKNOWN'
        CHECK (fix_quality IN ('3D_FIX', '2D_FIX', 'ARGOS_LOCATION_CLASS', 'UNKNOWN')),
    satellite_count INTEGER CHECK (satellite_count IS NULL OR satellite_count >= 0),
    hdop REAL CHECK (hdop IS NULL OR hdop >= 0.0),
    speed_kmh REAL CHECK (speed_kmh IS NULL OR speed_kmh >= 0.0),
    battery_voltage REAL CHECK (battery_voltage IS NULL OR battery_voltage >= 0.0),
    -- Real-time physiology, e.g. {"heart_rate_bpm": 42, "accelerometer_g": [0.01, -0.2, 0.98]}
    physiological_metrics TEXT NOT NULL DEFAULT '{}' CHECK (
        json_valid(physiological_metrics)
        AND json_type(physiological_metrics) = 'object'
        AND coalesce(json_type(physiological_metrics, '$.heart_rate_bpm'), 'real') IN ('integer', 'real')
        AND coalesce(json_extract(physiological_metrics, '$.heart_rate_bpm'), 1) > 0
        AND (json_type(physiological_metrics, '$.accelerometer_g') IS NULL
            OR (json_type(physiological_metrics, '$.accelerometer_g') = 'array'
                AND json_array_length(physiological_metrics, '$.accelerometer_g') = 3))
    ),
    -- Registered as a SpatiaLite POINT Z column below; STRICT tables require the BLOB declaration
    geom BLOB,
    UNIQUE (animal_id, recorded_epoch_ms)
) STRICT;

-- Register the 3D WGS84 point column with SpatiaLite
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM geometry_columns
        WHERE f_table_name = 'telemetry' AND f_geometry_column = 'geom'
    ) THEN RecoverGeometryColumn('telemetry', 'geom', 4326, 'POINT', 'XYZ')
    ELSE 1
END;

-- Created before the spatial index so SpatiaLite's own insert trigger fires first;
-- SQLite runs the newest trigger first, which would otherwise drop the new R-Tree entry
CREATE TRIGGER IF NOT EXISTS telemetry_geom_insert
AFTER INSERT ON telemetry
WHEN NEW.geom IS NULL
BEGIN
    UPDATE telemetry
    SET geom = MakePointZ(NEW.longitude, NEW.latitude, coalesce(NEW.altitude, 0.0), 4326)
    WHERE id = NEW.id;
END;

-- Build the R-Tree spatial index for localized boundary queries
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM geometry_columns
        WHERE f_table_name = 'telemetry' AND f_geometry_column = 'geom' AND spatial_index_enabled = 1
    ) THEN CreateSpatialIndex('telemetry', 'geom')
    ELSE 1
END;

-- Keep geometry in sync when a fix is corrected
CREATE TRIGGER IF NOT EXISTS telemetry_geom_update
AFTER UPDATE OF latitude, longitude, altitude ON telemetry
BEGIN
    UPDATE telemetry
    SET geom = MakePointZ(NEW.longitude, NEW.latitude, coalesce(NEW.altitude, 0.0), 4326)
    WHERE id = NEW.id;
END;

-- B-Tree indexes for per-species and per-animal time-series scans
CREATE INDEX IF NOT EXISTS idx_telemetry_species_time ON telemetry (species_id, recorded_epoch_ms);
CREATE INDEX IF NOT EXISTS idx_telemetry_device_time ON telemetry (device_id, recorded_epoch_ms);

-- Environmental Monitoring

-- High-throughput abiotic readings keyed by hardware MAC address, time, and spatial grid cell
CREATE TABLE IF NOT EXISTS sensor_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_mac TEXT NOT NULL CHECK (
        sensor_mac GLOB '[0-9A-F][0-9A-F]:[0-9A-F][0-9A-F]:[0-9A-F][0-9A-F]:[0-9A-F][0-9A-F]:[0-9A-F][0-9A-F]:[0-9A-F][0-9A-F]'
    ),
    device_id TEXT,
    recorded_at TEXT NOT NULL CHECK (
        recorded_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]*'
        AND (recorded_at GLOB '*Z' OR recorded_at GLOB '*+00:00')
        AND julianday(recorded_at) IS NOT NULL
        AND date(recorded_at) = substr(recorded_at, 1, 10)
    ),
    recorded_epoch_ms INTEGER GENERATED ALWAYS AS (
        CAST(round((julianday(recorded_at) - 2440587.5) * 86400000.0) AS INTEGER)
    ) STORED,
    -- H3 hex-grid cell (same resolution as edna.h3Resolution) bounding the reading
    h3_cell TEXT NOT NULL CHECK (length(h3_cell) = 15 AND lower(h3_cell) = h3_cell),
    latitude REAL CHECK (latitude IS NULL OR latitude BETWEEN -90.0 AND 90.0),
    longitude REAL CHECK (longitude IS NULL OR longitude BETWEEN -180.0 AND 180.0),
    altitude REAL,
    -- Water quality
    water_ph REAL CHECK (water_ph IS NULL OR water_ph BETWEEN 0.0 AND 14.0),
    dissolved_oxygen_mg_l REAL CHECK (dissolved_oxygen_mg_l IS NULL OR dissolved_oxygen_mg_l >= 0.0),
    water_turbidity_ntu REAL CHECK (water_turbidity_ntu IS NULL OR water_turbidity_ntu >= 0.0),
    water_temperature_c REAL CHECK (water_temperature_c IS NULL OR water_temperature_c BETWEEN -5.0 AND 100.0),
    water_salinity_psu REAL CHECK (water_salinity_psu IS NULL OR water_salinity_psu >= 0.0),
    water_velocity_m_s REAL CHECK (water_velocity_m_s IS NULL OR water_velocity_m_s >= 0.0),
    electrical_conductivity_us_cm REAL CHECK (electrical_conductivity_us_cm IS NULL OR electrical_conductivity_us_cm >= 0.0),
    -- Climate
    ambient_temperature_c REAL CHECK (ambient_temperature_c IS NULL OR ambient_temperature_c BETWEEN -90.0 AND 70.0),
    relative_humidity_percent REAL CHECK (relative_humidity_percent IS NULL OR relative_humidity_percent BETWEEN 0.0 AND 100.0),
    barometric_pressure_hpa REAL CHECK (barometric_pressure_hpa IS NULL OR barometric_pressure_hpa BETWEEN 300.0 AND 1100.0),
    -- Chemical parameters
    nitrate_mg_l REAL CHECK (nitrate_mg_l IS NULL OR nitrate_mg_l >= 0.0),
    phosphate_mg_l REAL CHECK (phosphate_mg_l IS NULL OR phosphate_mg_l >= 0.0),
    ammonia_mg_l REAL CHECK (ammonia_mg_l IS NULL OR ammonia_mg_l >= 0.0),
    pollutant_concentration_level REAL CHECK (pollutant_concentration_level IS NULL OR pollutant_concentration_level >= 0.0),
    air_quality_level REAL CHECK (air_quality_level IS NULL OR air_quality_level BETWEEN 1.0 AND 10.0),
    quality_flag TEXT NOT NULL DEFAULT 'VALID' CHECK (quality_flag IN ('VALID', 'SUSPECT', 'INVALID')),
    UNIQUE (sensor_mac, recorded_epoch_ms),
    -- Reject empty rows so aggregations never count readings without a measurement
    CHECK (coalesce(
        water_ph, dissolved_oxygen_mg_l, water_turbidity_ntu, water_temperature_c, water_salinity_psu,
        water_velocity_m_s, electrical_conductivity_us_cm, ambient_temperature_c, relative_humidity_percent,
        barometric_pressure_hpa, nitrate_mg_l, phosphate_mg_l, ammonia_mg_l, pollutant_concentration_level,
        air_quality_level
    ) IS NOT NULL)
) STRICT;

-- B-Tree indexes for time-window, grid-cell, and hardware lookups (MAC is covered by the UNIQUE index)
CREATE INDEX IF NOT EXISTS idx_sensor_readings_time ON sensor_readings (recorded_epoch_ms);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_cell_time ON sensor_readings (h3_cell, recorded_epoch_ms);

-- Edge AI Inference

-- Event ledger for vision and acoustic classifications at the raw asset location
CREATE TABLE IF NOT EXISTS detections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL CHECK (source_type IN ('VISION', 'ACOUSTIC')),
    -- Row ID in camera_ingestion or acoustic_events for the originating record
    source_record_id INTEGER,
    detection_index INTEGER NOT NULL DEFAULT 0 CHECK (detection_index >= 0),
    device_id TEXT NOT NULL CHECK (length(trim(device_id)) > 0),
    asset_path TEXT,
    species_id INTEGER REFERENCES species (id) ON UPDATE CASCADE ON DELETE SET NULL,
    class_id INTEGER CHECK (class_id IS NULL OR class_id >= 0),
    class_name TEXT NOT NULL CHECK (length(trim(class_name)) > 0),
    category TEXT NOT NULL CHECK (category IN ('target_wildlife', 'immediate_threat', 'other')),
    confidence REAL NOT NULL CHECK (confidence BETWEEN 0.0 AND 1.0),
    detected_at TEXT NOT NULL CHECK (
        detected_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]*'
        AND (detected_at GLOB '*Z' OR detected_at GLOB '*+00:00')
        AND julianday(detected_at) IS NOT NULL
        AND date(detected_at) = substr(detected_at, 1, 10)
    ),
    detected_epoch_ms INTEGER GENERATED ALWAYS AS (
        CAST(round((julianday(detected_at) - 2440587.5) * 86400000.0) AS INTEGER)
    ) STORED,
    ended_at TEXT CHECK (
        ended_at IS NULL OR (
            (ended_at GLOB '*Z' OR ended_at GLOB '*+00:00')
            AND julianday(ended_at) >= julianday(detected_at)
        )
    ),
    -- Frame-relative box edges in [0, 1]; required for vision and absent for acoustic events
    frame_width INTEGER CHECK (frame_width IS NULL OR frame_width > 0),
    frame_height INTEGER CHECK (frame_height IS NULL OR frame_height > 0),
    bbox_left REAL CHECK (bbox_left IS NULL OR bbox_left BETWEEN 0.0 AND 1.0),
    bbox_top REAL CHECK (bbox_top IS NULL OR bbox_top BETWEEN 0.0 AND 1.0),
    bbox_right REAL CHECK (bbox_right IS NULL OR bbox_right BETWEEN 0.0 AND 1.0),
    bbox_bottom REAL CHECK (bbox_bottom IS NULL OR bbox_bottom BETWEEN 0.0 AND 1.0),
    latitude REAL CHECK (latitude IS NULL OR latitude BETWEEN -90.0 AND 90.0),
    longitude REAL CHECK (longitude IS NULL OR longitude BETWEEN -180.0 AND 180.0),
    altitude REAL,
    metadata TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata) AND json_type(metadata) = 'object'),
    geom BLOB,
    UNIQUE (source_type, source_record_id, detection_index),
    CHECK ((latitude IS NULL) = (longitude IS NULL)),
    CHECK (
        (source_type = 'VISION'
            AND frame_width IS NOT NULL AND frame_height IS NOT NULL
            AND bbox_left IS NOT NULL AND bbox_top IS NOT NULL
            AND bbox_right IS NOT NULL AND bbox_bottom IS NOT NULL
            AND bbox_left < bbox_right AND bbox_top < bbox_bottom)
        OR (source_type = 'ACOUSTIC'
            AND frame_width IS NULL AND frame_height IS NULL
            AND bbox_left IS NULL AND bbox_top IS NULL
            AND bbox_right IS NULL AND bbox_bottom IS NULL)
    )
) STRICT;

-- Register the 3D WGS84 asset location column with SpatiaLite
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM geometry_columns
        WHERE f_table_name = 'detections' AND f_geometry_column = 'geom'
    ) THEN RecoverGeometryColumn('detections', 'geom', 4326, 'POINT', 'XYZ')
    ELSE 1
END;

-- Created before the spatial index for the same trigger ordering reason as telemetry
CREATE TRIGGER IF NOT EXISTS detections_geom_insert
AFTER INSERT ON detections
WHEN NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL AND NEW.geom IS NULL
BEGIN
    UPDATE detections
    SET geom = MakePointZ(NEW.longitude, NEW.latitude, coalesce(NEW.altitude, 0.0), 4326)
    WHERE id = NEW.id;
END;

-- Build the R-Tree spatial index for threat radius and boundary queries
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM geometry_columns
        WHERE f_table_name = 'detections' AND f_geometry_column = 'geom' AND spatial_index_enabled = 1
    ) THEN CreateSpatialIndex('detections', 'geom')
    ELSE 1
END;

-- Keep geometry in sync when an asset location is corrected
CREATE TRIGGER IF NOT EXISTS detections_geom_update
AFTER UPDATE OF latitude, longitude, altitude ON detections
BEGIN
    UPDATE detections
    SET geom = CASE
        WHEN NEW.latitude IS NULL OR NEW.longitude IS NULL THEN NULL
        ELSE MakePointZ(NEW.longitude, NEW.latitude, coalesce(NEW.altitude, 0.0), 4326)
    END
    WHERE id = NEW.id;
END;

-- B-Tree indexes for category, species, and device time-window filtering
CREATE INDEX IF NOT EXISTS idx_detections_category_time ON detections (category, detected_epoch_ms);
CREATE INDEX IF NOT EXISTS idx_detections_species_time ON detections (species_id, detected_epoch_ms);
CREATE INDEX IF NOT EXISTS idx_detections_device_time ON detections (device_id, detected_epoch_ms);

-- Population Genetics

-- Per-locus eDNA assay results with mathematically bounded diversity metrics
CREATE TABLE IF NOT EXISTS genetics_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id TEXT NOT NULL CHECK (length(trim(sample_id)) > 0),
    species_id INTEGER NOT NULL REFERENCES species (id) ON UPDATE CASCADE ON DELETE RESTRICT,
    locus_name TEXT NOT NULL CHECK (length(trim(locus_name)) > 0),
    assay_method TEXT NOT NULL DEFAULT 'UNKNOWN'
        CHECK (assay_method IN ('MICROSATELLITE', 'SNP', 'AMPLICON', 'METABARCODING', 'UNKNOWN')),
    collection_date TEXT NOT NULL CHECK (date(collection_date) IS collection_date),
    h3_cell TEXT NOT NULL CHECK (length(h3_cell) = 15 AND lower(h3_cell) = h3_cell),
    latitude REAL CHECK (latitude IS NULL OR latitude BETWEEN -90.0 AND 90.0),
    longitude REAL CHECK (longitude IS NULL OR longitude BETWEEN -180.0 AND 180.0),
    -- Allele label to frequency map, e.g. {"A": 0.62, "B": 0.38}; sum is enforced by triggers below
    allele_frequencies TEXT NOT NULL CHECK (
        json_valid(allele_frequencies) AND json_type(allele_frequencies) = 'object'
    ),
    allele_count INTEGER NOT NULL CHECK (allele_count >= 1),
    genotyped_individuals INTEGER NOT NULL CHECK (genotyped_individuals >= 1),
    -- H_e = 1 - sum(p_i^2) is always below 1 and zero for monomorphic loci
    expected_heterozygosity REAL CHECK (
        expected_heterozygosity IS NULL OR (expected_heterozygosity >= 0.0 AND expected_heterozygosity < 1.0)
    ),
    observed_heterozygosity REAL CHECK (
        observed_heterozygosity IS NULL OR observed_heterozygosity BETWEEN 0.0 AND 1.0
    ),
    allelic_richness REAL CHECK (
        allelic_richness IS NULL OR (allelic_richness >= 1.0 AND allelic_richness <= allele_count + 1e-9)
    ),
    -- NULL N_e means undetermined or infinite (no detectable drift)
    effective_population_size REAL CHECK (effective_population_size IS NULL OR effective_population_size >= 0.0),
    ne_lower_bound REAL CHECK (ne_lower_bound IS NULL OR ne_lower_bound >= 0.0),
    ne_upper_bound REAL CHECK (ne_upper_bound IS NULL OR ne_upper_bound >= 0.0),
    ne_method TEXT CHECK (
        ne_method IS NULL OR ne_method IN ('LINKAGE_DISEQUILIBRIUM', 'TEMPORAL_VARIANCE', 'HETEROZYGOSITY')
    ),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (sample_id, locus_name),
    CHECK ((latitude IS NULL) = (longitude IS NULL)),
    CHECK (allele_count > 1 OR expected_heterozygosity IS NULL OR expected_heterozygosity = 0.0),
    CHECK (ne_lower_bound IS NULL OR effective_population_size IS NULL OR ne_lower_bound <= effective_population_size),
    CHECK (ne_upper_bound IS NULL OR effective_population_size IS NULL OR effective_population_size <= ne_upper_bound),
    CHECK ((effective_population_size IS NULL) OR (ne_method IS NOT NULL))
) STRICT;

-- Allele frequencies must be numeric in [0, 1], sum to 1, and match the stated allele count
CREATE TRIGGER IF NOT EXISTS genetics_records_frequencies_insert
BEFORE INSERT ON genetics_records
BEGIN
    SELECT RAISE(ABORT, 'Allele frequencies must be numbers between 0 and 1.')
    WHERE EXISTS (
        SELECT 1 FROM json_each(NEW.allele_frequencies)
        WHERE type NOT IN ('integer', 'real') OR value < 0.0 OR value > 1.0
    );
    SELECT RAISE(ABORT, 'Allele frequencies must sum to 1.')
    WHERE abs((SELECT total(value) FROM json_each(NEW.allele_frequencies)) - 1.0) > 1e-6;
    SELECT RAISE(ABORT, 'Allele count must equal the number of alleles with a non-zero frequency.')
    WHERE (SELECT count(*) FROM json_each(NEW.allele_frequencies) WHERE value > 0.0) != NEW.allele_count;
END;

CREATE TRIGGER IF NOT EXISTS genetics_records_frequencies_update
BEFORE UPDATE OF allele_frequencies, allele_count ON genetics_records
BEGIN
    SELECT RAISE(ABORT, 'Allele frequencies must be numbers between 0 and 1.')
    WHERE EXISTS (
        SELECT 1 FROM json_each(NEW.allele_frequencies)
        WHERE type NOT IN ('integer', 'real') OR value < 0.0 OR value > 1.0
    );
    SELECT RAISE(ABORT, 'Allele frequencies must sum to 1.')
    WHERE abs((SELECT total(value) FROM json_each(NEW.allele_frequencies)) - 1.0) > 1e-6;
    SELECT RAISE(ABORT, 'Allele count must equal the number of alleles with a non-zero frequency.')
    WHERE (SELECT count(*) FROM json_each(NEW.allele_frequencies) WHERE value > 0.0) != NEW.allele_count;
END;

-- B-Tree indexes for spatial-population and locus lookups
CREATE INDEX IF NOT EXISTS idx_genetics_records_population
ON genetics_records (species_id, h3_cell, collection_date);
CREATE INDEX IF NOT EXISTS idx_genetics_records_locus ON genetics_records (species_id, locus_name);

-- Advanced Analytics & Risk Assessment

-- Synthesized conservation intelligence keyed by species, grid cell, and assessment period
CREATE TABLE IF NOT EXISTS risk_assessments (
    species_id INTEGER NOT NULL REFERENCES species (id) ON UPDATE CASCADE ON DELETE RESTRICT,
    h3_cell TEXT NOT NULL CHECK (length(h3_cell) = 15 AND lower(h3_cell) = h3_cell),
    period_start TEXT NOT NULL CHECK (
        (period_start GLOB '*Z' OR period_start GLOB '*+00:00') AND julianday(period_start) IS NOT NULL
    ),
    period_end TEXT NOT NULL CHECK (
        (period_end GLOB '*Z' OR period_end GLOB '*+00:00') AND julianday(period_end) IS NOT NULL
    ),
    period_end_epoch_ms INTEGER GENERATED ALWAYS AS (
        CAST(round((julianday(period_end) - 2440587.5) * 86400000.0) AS INTEGER)
    ) STORED,
    -- Domain subscores S_j in [0, 100] feeding CRI = sum(w_j * S_j)
    population_subscore REAL CHECK (population_subscore IS NULL OR population_subscore BETWEEN 0.0 AND 100.0),
    habitat_subscore REAL CHECK (habitat_subscore IS NULL OR habitat_subscore BETWEEN 0.0 AND 100.0),
    threat_subscore REAL CHECK (threat_subscore IS NULL OR threat_subscore BETWEEN 0.0 AND 100.0),
    climate_subscore REAL CHECK (climate_subscore IS NULL OR climate_subscore BETWEEN 0.0 AND 100.0),
    genetics_subscore REAL CHECK (genetics_subscore IS NULL OR genetics_subscore BETWEEN 0.0 AND 100.0),
    behavior_subscore REAL CHECK (behavior_subscore IS NULL OR behavior_subscore BETWEEN 0.0 AND 100.0),
    conservation_risk_index REAL NOT NULL CHECK (conservation_risk_index BETWEEN 0.0 AND 100.0),
    inbreeding_penalty_index REAL NOT NULL CHECK (inbreeding_penalty_index >= 0.0),
    -- Momentum is delta CRI per day and is negative when risk is falling
    risk_momentum_per_day REAL,
    momentum_window_days REAL CHECK (momentum_window_days IS NULL OR momentum_window_days > 0.0),
    population_trend TEXT CHECK (population_trend IS NULL OR population_trend IN ('DECLINING', 'STABLE', 'INCREASING')),
    effective_population_size REAL CHECK (effective_population_size IS NULL OR effective_population_size >= 0.0),
    critical_inbreeding_risk INTEGER NOT NULL DEFAULT 0 CHECK (critical_inbreeding_risk IN (0, 1)),
    -- Level 1 (CRI < 20) through Level 5 (CRI > 80) intervention tiers
    escalation_level INTEGER GENERATED ALWAYS AS (
        CASE
            WHEN conservation_risk_index < 20.0 THEN 1
            WHEN conservation_risk_index < 40.0 THEN 2
            WHEN conservation_risk_index < 60.0 THEN 3
            WHEN conservation_risk_index <= 80.0 THEN 4
            ELSE 5
        END
    ) STORED,
    input_summary TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(input_summary) AND json_type(input_summary) = 'object'),
    narrative_report TEXT NOT NULL CHECK (length(trim(narrative_report)) > 0),
    model_version TEXT,
    generated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (species_id, h3_cell, period_start, period_end),
    CHECK (julianday(period_end) > julianday(period_start))
) STRICT;

-- B-Tree indexes for dashboard escalation feeds and per-cell history
CREATE INDEX IF NOT EXISTS idx_risk_assessments_level_time
ON risk_assessments (escalation_level, period_end_epoch_ms);
CREATE INDEX IF NOT EXISTS idx_risk_assessments_cell_time
ON risk_assessments (h3_cell, period_end_epoch_ms);
