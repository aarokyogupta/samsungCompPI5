-- Time-ordered indexes for api/routesTelemetry.py
-- GET /telemetry/data pages newest-first by (epoch_ms, id) without a species or device filter,
-- which the existing composite indexes cannot serve; the rowid is implicit, so (epoch_ms) covers the keyset

CREATE INDEX IF NOT EXISTS idx_telemetry_time ON telemetry (recorded_epoch_ms);

-- Detections are split into acoustic and vision streams by source_type
CREATE INDEX IF NOT EXISTS idx_detections_source_time ON detections (source_type, detected_epoch_ms);
