-- Ensure the acoustic events table exists before spatial columns are mapped
CREATE TABLE IF NOT EXISTS acoustic_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    location_id TEXT,
    latitude REAL,
    longitude REAL,
    altitude REAL,
    class_id INTEGER NOT NULL,
    class_name TEXT NOT NULL,
    category TEXT NOT NULL,
    confidence REAL NOT NULL,
    start_timestamp TEXT NOT NULL,
    end_timestamp TEXT NOT NULL,
    window_count INTEGER NOT NULL DEFAULT 1,
    metadata TEXT NOT NULL,
    UNIQUE (device_id, class_id, start_timestamp)
);

-- Map a 3D WGS84 point column onto acoustic events
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM geometry_columns
        WHERE f_table_name = 'acoustic_events' AND f_geometry_column = 'geom'
    ) THEN AddGeometryColumn('acoustic_events', 'geom', 4326, 'POINT', 'XYZ')
    ELSE 1
END;

-- Created before the spatial index so SpatiaLite's own insert trigger fires first;
-- SQLite runs the newest trigger first, which would otherwise drop the new R-Tree entry
CREATE TRIGGER IF NOT EXISTS acoustic_events_geom_insert
AFTER INSERT ON acoustic_events
WHEN NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL AND NEW.geom IS NULL
BEGIN
    UPDATE acoustic_events
    SET geom = MakePointZ(NEW.longitude, NEW.latitude, COALESCE(NEW.altitude, 0.0), 4326)
    WHERE id = NEW.id;
END;

-- Build the R-Tree spatial index for coordinate lookups
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM geometry_columns
        WHERE f_table_name = 'acoustic_events'
            AND f_geometry_column = 'geom'
            AND spatial_index_enabled = 1
    ) THEN CreateSpatialIndex('acoustic_events', 'geom')
    ELSE 1
END;

-- Backfill geometry for events recorded before this migration
UPDATE acoustic_events
SET geom = MakePointZ(longitude, latitude, COALESCE(altitude, 0.0), 4326)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND geom IS NULL;

-- Keep geometry in sync when acoustic inference moves or merges an event
CREATE TRIGGER IF NOT EXISTS acoustic_events_geom_update
AFTER UPDATE OF latitude, longitude, altitude ON acoustic_events
BEGIN
    UPDATE acoustic_events
    SET geom = CASE
        WHEN NEW.latitude IS NULL OR NEW.longitude IS NULL THEN NULL
        ELSE MakePointZ(NEW.longitude, NEW.latitude, COALESCE(NEW.altitude, 0.0), 4326)
    END
    WHERE id = NEW.id;
END;

-- B-Tree index for category and timestamp filtering
CREATE INDEX IF NOT EXISTS idx_acoustic_events_category_start
ON acoustic_events (category, start_timestamp);
