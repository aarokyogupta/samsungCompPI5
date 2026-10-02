-- Intervention Priority Index persistence for aiEngine/graphWorkflow.py
-- Promotes the IPI from input_summary JSON to a dedicated column so the dashboard can rank cells by it

-- IPI = CRI x momentum factor x operational feasibility; momentum can push it above 100
ALTER TABLE risk_assessments ADD COLUMN intervention_priority_index REAL
    CHECK (intervention_priority_index IS NULL OR intervention_priority_index >= 0.0);

-- Must match PRIORITY_TIERS in aiEngine/stateSchema.py; VIRTUAL because ALTER TABLE cannot add STORED columns
ALTER TABLE risk_assessments ADD COLUMN intervention_priority_tier TEXT GENERATED ALWAYS AS (
    CASE
        WHEN intervention_priority_index IS NULL THEN NULL
        WHEN intervention_priority_index >= 80.0 THEN 'IMMEDIATE'
        WHEN intervention_priority_index >= 60.0 THEN 'HIGH'
        WHEN intervention_priority_index >= 40.0 THEN 'ELEVATED'
        WHEN intervention_priority_index >= 20.0 THEN 'ROUTINE'
        ELSE 'MONITOR'
    END
) VIRTUAL;

-- B-Tree index for the dashboard's ranked intervention queue
CREATE INDEX IF NOT EXISTS idx_risk_assessments_priority_time
ON risk_assessments (intervention_priority_index DESC, period_end_epoch_ms);

-- Backfill rows written before this migration from their input_summary JSON
UPDATE risk_assessments
SET intervention_priority_index = json_extract(input_summary, '$.intervention_priority_index')
WHERE intervention_priority_index IS NULL
    AND json_type(input_summary, '$.intervention_priority_index') IN ('real', 'integer')
    AND json_extract(input_summary, '$.intervention_priority_index') >= 0.0;
