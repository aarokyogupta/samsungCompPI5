import asyncio
from datetime import datetime, timedelta, timezone
import h3
import json
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.graph import END, START, StateGraph
import math
import os
from pydantic import BaseModel
import sqlite3
import statistics
import sys
from typing import Any, Optional
import uuid
import yaml

# Allow "python aiEngine/graphWorkflow.py" as well as "python -m aiEngine.graphWorkflow" from the project root
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import aiEngine.stateSchema as stateSchema
from aiEngine.explainabilityPrompts import (
    ExplainabilityReport,
    buildFallbackReport,
    buildPromptContext,
    buildPromptMessages,
    calculatePercentageContributions,
    formatMitigations,
    getWeightedContributions,
    validateReportAgainstContext,
)
from aiEngine.llmProvider import LangChainBackend, LLMProvider, LLMRequest
from aiEngine.indicatorCalculator import (
    CLIMATE_DIRECTIONS,
    HETEROZYGOSITY_BASELINE,
    MILLISECONDS_PER_DAY,
    SENSOR_READING_COLUMNS,
    createIndicatorCalculatorNode,
    getThreatClassWeight,
    haversineMeters,
    toEpochMilliseconds,
)
from aiEngine.stateSchema import (
    CRITICAL_NE_THRESHOLD,
    DATABASE_SCORE_SCALE,
    DOMAIN_WEIGHTS,
    LEVEL_BASE_ACTIONS,
    MOMENTUM_WINDOW_DAYS,
    RISK_DOMAINS,
    WEIGHT_SUM_TOLERANCE,
    AssessmentScope,
    DroneDeployment,
    GraphState,
    InterventionRouting,
    MomentumVector,
    NarrativeBlock,
    NormalizedSubscores,
    RangerDispatchAlert,
    RawIndicators,
    RiskMetrics,
    StateValidationError,
    ThreatEvent,
    buildPersistenceStatement,
    buildSerializationPayload,
    calculateConservationRiskIndex,
    createInitialState,
    validatedNode,
)
from database.dbManager import THREAT_RADIUS_METERS, buildThreatRadiusQuery, getSearchFrames
from database.models import toUtcDatetime

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

WORKFLOW_CONFIG: dict = config.get("aiEngine", {}).get("graphWorkflow", {}) or {}

# Node names double as node_history entries and checkpoint keys
NODE_AGGREGATION: str = "dataAggregation"
NODE_NORMALIZATION: str = "indicatorCalculator"
NODE_SYNTHESIS: str = "riskSynthesis"
NODE_EXPLAINABILITY: str = "narrativeSynthesis"
NODE_MAPPING: str = "interventionMapping"

# LLM routing, timeouts and hardware guardrails live under aiEngine: llmProvider: in config.yaml
FALLBACK_GENERATOR: str = "deterministicTemplate"

# Spatial-temporal aggregation settings
ANALYSIS_RADIUS_METERS: float = float(WORKFLOW_CONFIG.get("analysisRadiusMeters", THREAT_RADIUS_METERS))
GENETICS_LOOKBACK_DAYS: float = float(WORKFLOW_CONFIG.get("geneticsLookbackDays", 365.0))
DENSITY_GRID_SIZE: int = int(WORKFLOW_CONFIG.get("densityGridSize", 5))
MAX_TELEMETRY_FIXES: int = int(WORKFLOW_CONFIG.get("maxTelemetryFixes", 20000))
MAX_TRACE_IDS_PER_TABLE: int = int(WORKFLOW_CONFIG.get("maxTraceIDsPerTable", 5000))
MORTALITY_KEYWORDS: tuple[str, ...] = tuple(
    str(keyword).casefold() for keyword in WORKFLOW_CONFIG.get("mortalityKeywords", ["carcass", "mortality", "dead"])
)
# JSON metadata keys written by the vision and acoustic inference workers
AGE_CLASS_KEYS: tuple[str, ...] = ("age_class", "ageClass")
DECIBEL_KEYS: tuple[str, ...] = ("peak_db", "peakDb", "sound_level_db", "decibels")
AGE_CLASSES: tuple[str, ...] = ("juvenile", "subadult", "adult", "elder")

# Mathematical synthesis settings
MOMENTUM_MAX_LOOKBACK_DAYS: float = float(WORKFLOW_CONFIG.get("momentumMaxLookbackDays", 90.0))
POACHING_CLASS_WEIGHT: float = float(WORKFLOW_CONFIG.get("poachingClassWeight", 0.9))
POACHING_MIN_CONFIDENCE: float = float(WORKFLOW_CONFIG.get("poachingMinConfidence", 0.6))
POACHING_RECENCY_HOURS: float = float(WORKFLOW_CONFIG.get("poachingRecencyHours", 48.0))
MOMENTUM_GAIN: float = float(WORKFLOW_CONFIG.get("momentumGain", 2.0))
MOMENTUM_FACTOR_MIN, MOMENTUM_FACTOR_MAX = (float(bound) for bound in WORKFLOW_CONFIG.get("momentumFactorBounds", [0.5, 2.0]))
FEASIBILITY_RADIUS_METERS: float = float(WORKFLOW_CONFIG.get("feasibilityRadiusMeters", 50000.0))
FEASIBILITY_NEAR, FEASIBILITY_FAR = (float(bound) for bound in WORKFLOW_CONFIG.get("feasibilityMultipliers", [1.2, 0.8]))
RANGER_OUTPOSTS: list[dict] = list(WORKFLOW_CONFIG.get("rangerOutposts") or [])
TREND_CHANGE_THRESHOLD: float = float(WORKFLOW_CONFIG.get("trendChangeThreshold", 0.05))
MINIMUM_TREND_POINTS: int = 4

# Inbreeding Penalty Index settings (Wright's F projected over a management horizon)
INBREEDING_HORIZON_GENERATIONS: float = float(WORKFLOW_CONFIG.get("inbreedingHorizonGenerations", 10.0))
INBREEDING_TOLERANCE_F: float = float(WORKFLOW_CONFIG.get("inbreedingToleranceF", 0.1))
INBREEDING_DRIFT_WEIGHT: float = float(WORKFLOW_CONFIG.get("inbreedingDriftWeight", 0.6))

# Intervention mapping settings (CRI on the graph state's 0.0-1.0 scale, so 0.8 == CRI 80)
CRITICAL_CRI_THRESHOLD: float = float(WORKFLOW_CONFIG.get("criticalCriThreshold", 0.8))
DRONE_ALTITUDE_M: float = float(WORKFLOW_CONFIG.get("droneAltitudeM", 120.0))
MAX_DISPATCH_TARGETS: int = int(WORKFLOW_CONFIG.get("maxDispatchTargets", 3))
ALERT_CHANNEL: str = str(WORKFLOW_CONFIG.get("alertChannel", "MQTT")).upper()

if ANALYSIS_RADIUS_METERS <= 0.0 or GENETICS_LOOKBACK_DAYS <= 0.0:
    raise ValueError("graphWorkflow analysis radius and genetics lookback must be positive.")
if DENSITY_GRID_SIZE < 1 or MAX_TELEMETRY_FIXES < 1 or MAX_TRACE_IDS_PER_TABLE < 1 or MAX_DISPATCH_TARGETS < 1:
    raise ValueError("graphWorkflow grid size, row caps and dispatch targets must be at least 1.")
if not 0.0 < MOMENTUM_FACTOR_MIN <= 1.0 <= MOMENTUM_FACTOR_MAX:
    raise ValueError("graphWorkflow momentumFactorBounds must satisfy 0 < min <= 1 <= max.")
if FEASIBILITY_NEAR <= 0.0 or FEASIBILITY_FAR <= 0.0 or FEASIBILITY_RADIUS_METERS <= 0.0:
    raise ValueError("graphWorkflow feasibility multipliers and radius must be positive.")
if INBREEDING_HORIZON_GENERATIONS <= 0.0 or not 0.0 < INBREEDING_TOLERANCE_F <= 1.0:
    raise ValueError("graphWorkflow inbreedingHorizonGenerations must be positive and inbreedingToleranceF in (0, 1].")
if not 0.0 <= INBREEDING_DRIFT_WEIGHT <= 1.0:
    raise ValueError("graphWorkflow inbreedingDriftWeight must be between 0 and 1.")
if not 0.0 < CRITICAL_CRI_THRESHOLD <= 1.0:
    raise ValueError("graphWorkflow criticalCriThreshold is on the 0.0-1.0 scale and must be in (0, 1].")
if not 0.0 <= DRONE_ALTITUDE_M <= 500.0 or ALERT_CHANNEL not in ("MQTT", "WEBSOCKET"):
    raise ValueError("graphWorkflow droneAltitudeM must be 0-500 and alertChannel MQTT or WEBSOCKET.")
for outpost in RANGER_OUTPOSTS:
    if not -90.0 <= float(outpost["latitude"]) <= 90.0 or not -180.0 <= float(outpost["longitude"]) <= 180.0:
        raise ValueError(f"graphWorkflow ranger outpost {outpost.get('name', '?')} has invalid coordinates.")


def validateDomainWeights(weights: dict, label: str) -> dict[str, float]:
    parsedWeights = {domain: float(weight) for domain, weight in weights.items()}
    if set(parsedWeights) != set(RISK_DOMAINS):
        raise ValueError(f"{label} must define exactly these domains: {', '.join(RISK_DOMAINS)}.")
    if any(weight < 0.0 or not math.isfinite(weight) for weight in parsedWeights.values()):
        raise ValueError(f"{label} must be finite and non-negative.")
    if abs(math.fsum(parsedWeights.values()) - 1.0) > WEIGHT_SUM_TOLERANCE:
        raise ValueError(f"{label} must sum to 1.0.")
    return parsedWeights


# Species ID or scientific name -> w_j overrides of the global aiEngine.domainWeights
SPECIES_DOMAIN_WEIGHTS: dict[str, dict[str, float]] = {
    str(key).strip().casefold(): validateDomainWeights(weights, f"graphWorkflow speciesDomainWeights[{key}]")
    for key, weights in (WORKFLOW_CONFIG.get("speciesDomainWeights") or {}).items()
}


# Shared helpers

def toFiniteFloat(value: object) -> Optional[float]:
    # Null masking: NULLs, non-numbers and NaN/inf never reach the strict state models
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def loadJsonObject(text: object) -> dict[str, Any]:
    if not isinstance(text, str) or not text:
        return {}
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def getScopeCentre(scope: AssessmentScope) -> tuple[float, float]:
    # Explicit scope coordinates win; otherwise the H3 cell centroid is the analysis centre
    if scope.latitude is not None and scope.longitude is not None:
        return scope.latitude, scope.longitude
    latitude, longitude = h3.cell_to_latlng(scope.h3_cell)
    return float(latitude), float(longitude)


def buildSpatialFilter(tableName: str, latitude: float, longitude: float, radiusMeters: float) -> tuple[str, list[object]]:
    # SpatialIndex subquery drives the R-Tree lookup, then ST_Distance trims the frame to the exact radius
    searchPoint = f"POINT({longitude:.9f} {latitude:.9f})"
    searchFrames = getSearchFrames(latitude, longitude, radiusMeters)
    frameQuery = " UNION ".join(
        f"""
                SELECT ROWID FROM SpatialIndex
                WHERE f_table_name = '{tableName}' AND f_geometry_column = 'geom'
                    AND search_frame = BuildMbr(?, ?, ?, ?, 4326)"""
        for _ in searchFrames
    )
    clause = (
        f"geom IS NOT NULL AND ROWID IN ({frameQuery}\n            )"
        " AND ST_Distance(CastToXY(geom), ST_GeomFromText(?, 4326), 1) <= ?"
    )
    parameters: list[object] = []
    for searchFrame in searchFrames:
        parameters.extend(searchFrame)
    parameters.extend([searchPoint, radiusMeters])
    return clause, parameters


def capTraceIDs(ids: list[int]) -> tuple[int, ...]:
    return tuple(sorted({int(recordID) for recordID in ids if recordID})[:MAX_TRACE_IDS_PER_TABLE])


SectionResult = tuple[dict[str, Any], dict[str, tuple[int, ...]], list[str]]


# Node 1: Spatial-Temporal Data Aggregation

async def aggregateTelemetry(
    databaseManager: Any, scope: AssessmentScope, centre: tuple[float, float], startMs: int, endMs: int
) -> SectionResult:
    values: dict[str, Any] = {}
    try:
        spatialClause, spatialParameters = buildSpatialFilter("telemetry", centre[0], centre[1], ANALYSIS_RADIUS_METERS)
        rows = await databaseManager.fetchAll(
            f"""
            SELECT id, animal_id, recorded_at, latitude, longitude, speed_kmh,
                   json_extract(physiological_metrics, '$.heart_rate_bpm') AS heart_rate_bpm
            FROM telemetry INDEXED BY idx_telemetry_species_time
            WHERE species_id = ? AND recorded_epoch_ms BETWEEN ? AND ?
                AND {spatialClause}
            ORDER BY recorded_epoch_ms
            LIMIT ?;
            """,
            [scope.species_id, startMs, endMs, *spatialParameters, MAX_TELEMETRY_FIXES],
        )
    except sqlite3.Error as error:
        return {}, {}, [f"dataAggregation: telemetry query failed: {error}"]

    speeds = [speed for speed in (toFiniteFloat(row["speed_kmh"]) for row in rows) if speed is not None and speed >= 0.0]
    heartRates = [rate for rate in (toFiniteFloat(row["heart_rate_bpm"]) for row in rows) if rate is not None and rate > 0.0]
    values["telemetry_fix_count"] = len(rows)
    values["telemetry_fixes"] = [
        {"animal_id": str(row["animal_id"]), "recorded_at": row["recorded_at"],
         "latitude": float(row["latitude"]), "longitude": float(row["longitude"])}
        for row in rows
    ]
    if speeds:
        values["mean_speed_kmh"] = statistics.fmean(speeds)
    if heartRates:
        values["mean_heart_rate_bpm"] = statistics.fmean(heartRates)
    return values, {"telemetry": capTraceIDs([row["id"] for row in rows])}, []


def getDecibelLevel(metadata: dict[str, Any]) -> Optional[float]:
    for key in DECIBEL_KEYS:
        decibels = toFiniteFloat(metadata.get(key))
        if decibels is not None and -200.0 <= decibels <= 250.0:
            return decibels
    return None


def getAgeClass(metadata: dict[str, Any]) -> Optional[str]:
    for key in AGE_CLASS_KEYS:
        ageClass = str(metadata.get(key) or "").strip().casefold()
        if ageClass in AGE_CLASSES:
            return ageClass
    return None


def buildThreatEvent(
    detectedAt: str, className: str, sourceType: str, confidence: object,
    latitude: object, longitude: object, recordID: Optional[int],
) -> dict[str, Any]:
    eventLatitude, eventLongitude = toFiniteFloat(latitude), toFiniteFloat(longitude)
    hasLocation = eventLatitude is not None and eventLongitude is not None
    return {
        "detected_at": detectedAt,
        "class_name": className,
        "source_type": sourceType,
        "confidence": clamp(toFiniteFloat(confidence) or 0.0, 0.0, 1.0),
        "latitude": eventLatitude if hasLocation else None,
        "longitude": eventLongitude if hasLocation else None,
        "record_id": recordID,
    }


async def aggregateDetections(
    databaseManager: Any, scope: AssessmentScope, centre: tuple[float, float], startMs: int, endMs: int
) -> SectionResult:
    try:
        spatialClause, spatialParameters = buildSpatialFilter("detections", centre[0], centre[1], ANALYSIS_RADIUS_METERS)
        # Unlocated rows are kept only when they are explicitly tagged with the target species
        rows = await databaseManager.fetchAll(
            f"""
            SELECT id, source_type, source_record_id, species_id, class_name, category, confidence,
                   detected_at, latitude, longitude, bbox_left, bbox_top, bbox_right, bbox_bottom, metadata
            FROM detections
            WHERE detected_epoch_ms BETWEEN ? AND ?
                AND (species_id = ? OR category = 'immediate_threat')
                AND (({spatialClause}) OR (geom IS NULL AND species_id = ?))
            ORDER BY detected_epoch_ms;
            """,
            [startMs, endMs, scope.species_id, *spatialParameters, scope.species_id],
        )
    except sqlite3.Error as error:
        return {}, {}, [f"dataAggregation: detections query failed: {error}"]

    frames: dict[object, dict[str, Any]] = {}
    ageClassCounts = {ageClass: 0 for ageClass in AGE_CLASSES}
    bboxAreas: list[float] = []
    densityCounts = [[0.0] * DENSITY_GRID_SIZE for _ in range(DENSITY_GRID_SIZE)]
    threatEvents: list[dict[str, Any]] = []
    mortalityEvents: list[dict[str, Any]] = []
    decibels: list[float] = []
    acousticThreatCount = 0
    visionCount = 0
    linkedAcousticIDs: set[int] = set()

    for row in rows:
        metadata = loadJsonObject(row["metadata"])
        className = str(row["class_name"])
        if row["source_type"] == "ACOUSTIC" and row["source_record_id"] is not None:
            linkedAcousticIDs.add(int(row["source_record_id"]))

        if row["category"] == "immediate_threat":
            threatEvents.append(buildThreatEvent(
                row["detected_at"], className, row["source_type"], row["confidence"],
                row["latitude"], row["longitude"], int(row["id"]),
            ))
            if row["source_type"] == "ACOUSTIC":
                acousticThreatCount += 1
                decibelLevel = getDecibelLevel(metadata)
                if decibelLevel is not None:
                    decibels.append(decibelLevel)
            continue
        if row["species_id"] != scope.species_id:
            continue

        # Carcass classifications feed the mortality penalty instead of the live herd census
        if any(keyword in className.casefold() for keyword in MORTALITY_KEYWORDS):
            eventLatitude, eventLongitude = toFiniteFloat(row["latitude"]), toFiniteFloat(row["longitude"])
            hasLocation = eventLatitude is not None and eventLongitude is not None
            mortalityEvents.append({
                "recorded_at": row["detected_at"],
                "latitude": eventLatitude if hasLocation else None,
                "longitude": eventLongitude if hasLocation else None,
                "cause": str(metadata.get("cause") or className),
            })
            continue
        if row["source_type"] != "VISION":
            continue

        visionCount += 1
        left, top = float(row["bbox_left"]), float(row["bbox_top"])
        right, bottom = float(row["bbox_right"]), float(row["bbox_bottom"])
        bboxAreas.append(clamp((right - left) * (bottom - top), 0.0, 1.0))
        column = min(int((left + right) / 2.0 * DENSITY_GRID_SIZE), DENSITY_GRID_SIZE - 1)
        gridRow = min(int((top + bottom) / 2.0 * DENSITY_GRID_SIZE), DENSITY_GRID_SIZE - 1)
        densityCounts[gridRow][column] += 1.0

        # One camera frame (source_record_id) is one herd sighting
        frameKey = row["source_record_id"] if row["source_record_id"] is not None else f"detection-{row['id']}"
        frame = frames.setdefault(frameKey, {"observed_at": row["detected_at"], "count": 0,
                                             "ageClasses": {ageClass: 0 for ageClass in AGE_CLASSES}})
        frame["count"] += 1
        ageClass = getAgeClass(metadata)
        if ageClass is not None:
            frame["ageClasses"][ageClass] += 1
            ageClassCounts[ageClass] += 1

    values: dict[str, Any] = {
        "threat_events": threatEvents,
        "mortality_events": mortalityEvents,
        "acoustic_threat_decibels": decibels,
        "acoustic_threat_event_count": acousticThreatCount,
        "vision_detection_count": visionCount,
        "vision_bbox_areas": bboxAreas,
    }
    if frames:
        # Herd count per day is the largest single-frame count, so repeated frames never double count
        dailyCounts: dict[str, int] = {}
        for frame in frames.values():
            day = toUtcDatetime(frame["observed_at"]).date().isoformat()
            dailyCounts[day] = max(dailyCounts.get(day, 0), frame["count"])
        values["herd_count_series"] = [float(dailyCounts[day]) for day in sorted(dailyCounts)]
        values["vision_herd_density"] = [[count / len(frames) for count in gridRow] for gridRow in densityCounts]
        values["social_groups"] = [
            {"observed_at": frame["observed_at"], **{f"{ageClass}_count": count for ageClass, count in frame["ageClasses"].items()}}
            for frame in frames.values()
            if any(frame["ageClasses"].values())
        ]
    if any(ageClassCounts.values()):
        values["age_class_counts"] = {ageClass: count for ageClass, count in ageClassCounts.items() if count}
    values["linkedAcousticIDs"] = linkedAcousticIDs
    return values, {"detections": capTraceIDs([row["id"] for row in rows])}, []


async def aggregateAcousticEvents(
    databaseManager: Any, scope: AssessmentScope, centre: tuple[float, float]
) -> SectionResult:
    periodDays = (scope.period_end - scope.period_start).total_seconds() / 86400.0
    try:
        query, parameters = buildThreatRadiusQuery(
            centre[0], centre[1], ANALYSIS_RADIUS_METERS, lookbackDays=periodDays, referenceTime=scope.period_end
        )
        rows = await databaseManager.fetchAll(query, parameters)
    except (sqlite3.Error, ValueError) as error:
        return {}, {}, [f"dataAggregation: acoustic_events query failed: {error}"]

    events: list[dict[str, Any]] = []
    for row in rows:
        try:
            startedAt = toUtcDatetime(row["start_timestamp"])
        except (TypeError, ValueError):
            continue
        # buildThreatRadiusQuery only bounds the lower edge, so the period end is enforced here
        if startedAt > scope.period_end:
            continue
        events.append({"row": row, "startedAt": startedAt})
    return {"acousticEventRows": events}, {"acoustic_events": capTraceIDs([event["row"]["id"] for event in events])}, []


async def aggregateSensorReadings(
    databaseManager: Any, scope: AssessmentScope, startMs: int, endMs: int
) -> SectionResult:
    metrics = sorted(SENSOR_READING_COLUMNS)
    climateMetrics = sorted(set(CLIMATE_DIRECTIONS) & SENSOR_READING_COLUMNS)
    whereClause = """
        FROM sensor_readings INDEXED BY idx_sensor_readings_cell_time
        WHERE h3_cell = ? AND recorded_epoch_ms BETWEEN ? AND ? AND quality_flag != 'INVALID'
    """
    parameters = [scope.h3_cell, startMs, endMs]
    try:
        meanRows, dailyRows, idRows = await asyncio.gather(
            databaseManager.fetchAll(
                "SELECT " + ", ".join(f"avg({metric}) AS {metric}" for metric in metrics) + whereClause + ";",
                parameters,
            ),
            databaseManager.fetchAll(
                f"SELECT recorded_epoch_ms / {MILLISECONDS_PER_DAY} AS day_index, "
                + ", ".join(f"avg({metric}) AS {metric}" for metric in climateMetrics)
                + whereClause + " GROUP BY day_index ORDER BY day_index;",
                parameters,
            ),
            databaseManager.fetchAll("SELECT id" + whereClause + " LIMIT ?;", [*parameters, MAX_TRACE_IDS_PER_TABLE]),
        )
    except sqlite3.Error as error:
        return {}, {}, [f"dataAggregation: sensor_readings query failed: {error}"]

    metricMeans = {
        metric: mean for metric in metrics
        if meanRows and (mean := toFiniteFloat(meanRows[0][metric])) is not None
    }
    climateDailyMeans: dict[str, list[dict[str, Any]]] = {}
    for metric in climateMetrics:
        dailyValues = [
            {"day": datetime.fromtimestamp(int(row["day_index"]) * 86400, tz=timezone.utc), "value": value}
            for row in dailyRows
            if (value := toFiniteFloat(row[metric])) is not None
        ]
        if dailyValues:
            climateDailyMeans[metric] = dailyValues
    values = {"sensor_metric_means": metricMeans, "climate_daily_means": climateDailyMeans}
    return values, {"sensor_readings": capTraceIDs([row["id"] for row in idRows])}, []


async def aggregateGenetics(databaseManager: Any, scope: AssessmentScope) -> SectionResult:
    lookbackStart = (scope.period_end - timedelta(days=GENETICS_LOOKBACK_DAYS)).date().isoformat()
    try:
        rows = await databaseManager.fetchAll(
            """
            SELECT id, sample_id, locus_name, collection_date, allele_frequencies, genotyped_individuals,
                   expected_heterozygosity, allelic_richness, effective_population_size
            FROM genetics_records INDEXED BY idx_genetics_records_population
            WHERE species_id = ? AND h3_cell = ? AND collection_date BETWEEN ? AND ?
            ORDER BY collection_date DESC, id DESC;
            """,
            [scope.species_id, scope.h3_cell, lookbackStart, scope.period_end.date().isoformat()],
        )
    except sqlite3.Error as error:
        return {}, {}, [f"dataAggregation: genetics_records query failed: {error}"]
    if not rows:
        return {}, {}, []

    # Diploid gene copies per allele = frequency x 2 x genotyped individuals
    lociCounts: dict[str, dict[str, int]] = {}
    for row in rows:
        individuals = int(row["genotyped_individuals"] or 0)
        for allele, frequency in loadJsonObject(row["allele_frequencies"]).items():
            geneCopies = round((toFiniteFloat(frequency) or 0.0) * 2 * individuals)
            if geneCopies > 0:
                alleleCounts = lociCounts.setdefault(str(row["locus_name"]), {})
                alleleCounts[str(allele)] = alleleCounts.get(str(allele), 0) + geneCopies

    heterozygosities = [value for value in (toFiniteFloat(row["expected_heterozygosity"]) for row in rows) if value is not None]
    richnessValues = [value for value in (toFiniteFloat(row["allelic_richness"]) for row in rows) if value is not None]
    # Rows are newest first, so the first N_e estimate is the most recent one
    effectiveSizes = [value for value in (toFiniteFloat(row["effective_population_size"]) for row in rows) if value is not None]
    values: dict[str, Any] = {
        "edna_loci_counts": lociCounts,
        "edna_sample_count": len({row["sample_id"] for row in rows}),
    }
    if heterozygosities:
        values["expected_heterozygosity"] = clamp(statistics.fmean(heterozygosities), 0.0, 1.0)
    if richnessValues:
        values["allelic_richness"] = statistics.fmean(richnessValues)
    if effectiveSizes:
        values["effective_population_size"] = max(0.0, effectiveSizes[0])
    return values, {"genetics_records": capTraceIDs([row["id"] for row in rows])}, []


async def aggregateSpeciesBaseline(databaseManager: Any, scope: AssessmentScope) -> SectionResult:
    try:
        rows = await databaseManager.fetchAll(
            "SELECT minimum_viable_population, census_population_estimate FROM species WHERE id = ?;",
            [scope.species_id],
        )
    except sqlite3.Error as error:
        return {}, {}, [f"dataAggregation: species query failed: {error}"]
    if not rows:
        return {}, {}, [f"dataAggregation: species {scope.species_id} was not found."]
    values: dict[str, Any] = {}
    if rows[0]["census_population_estimate"] is not None:
        values["census_count"] = int(rows[0]["census_population_estimate"])
    if rows[0]["minimum_viable_population"] is not None:
        values["minimum_viable_population"] = int(rows[0]["minimum_viable_population"])
    return values, {}, []


def mergeAcousticEvents(values: dict[str, Any]) -> None:
    # acoustic_events already promoted into detections are skipped so one gunshot is never counted twice
    linkedIDs: set[int] = values.pop("linkedAcousticIDs", set())
    for event in values.pop("acousticEventRows", []):
        row = event["row"]
        if int(row["id"]) in linkedIDs:
            continue
        values.setdefault("threat_events", []).append(buildThreatEvent(
            row["start_timestamp"], str(row["class_name"]), "ACOUSTIC", row["confidence"],
            row["latitude"], row["longitude"], None,
        ))
        values["acoustic_threat_event_count"] = values.get("acoustic_threat_event_count", 0) + 1
        decibelLevel = getDecibelLevel(loadJsonObject(row["metadata"]))
        if decibelLevel is not None:
            values.setdefault("acoustic_threat_decibels", []).append(decibelLevel)


async def aggregateRawIndicators(
    databaseManager: Any, scope: AssessmentScope
) -> tuple[RawIndicators, dict[str, tuple[int, ...]], list[str]]:
    centre = getScopeCentre(scope)
    startMs, endMs = toEpochMilliseconds(scope.period_start), toEpochMilliseconds(scope.period_end)
    # Independent reads run concurrently across the dbManager read pool
    sections = await asyncio.gather(
        aggregateTelemetry(databaseManager, scope, centre, startMs, endMs),
        aggregateDetections(databaseManager, scope, centre, startMs, endMs),
        aggregateAcousticEvents(databaseManager, scope, centre),
        aggregateSensorReadings(databaseManager, scope, startMs, endMs),
        aggregateGenetics(databaseManager, scope),
        aggregateSpeciesBaseline(databaseManager, scope),
    )
    values: dict[str, Any] = {}
    recordIDs: dict[str, tuple[int, ...]] = {}
    warnings: list[str] = []
    for sectionValues, sectionIDs, sectionWarnings in sections:
        values.update(sectionValues)
        recordIDs.update(sectionIDs)
        warnings.extend(sectionWarnings)
    mergeAcousticEvents(values)

    raw = RawIndicators.model_validate({
        **values,
        "aggregated_at": datetime.now(timezone.utc),
        "window_start": scope.period_start,
        "window_end": scope.period_end,
    })
    return raw, recordIDs, warnings


def createAggregationNode(
    databaseManager: Optional[Any] = None,
    rawIndicators: Optional[RawIndicators] = None,
) -> Any:
    @validatedNode(NODE_AGGREGATION)
    async def aggregationNode(state: dict[str, Any]) -> dict[str, Any]:
        # raw_indicators is write-once, so a pre-hydrated state passes straight through
        if state.get("raw_indicators") is not None:
            return {}
        scope: Optional[AssessmentScope] = state.get("assessment_scope")
        if scope is None:
            raise StateValidationError("The Data Aggregation Node requires an assessment_scope.")

        if databaseManager is None:
            raw = rawIndicators or RawIndicators(
                aggregated_at=datetime.now(timezone.utc), window_start=scope.period_start, window_end=scope.period_end
            )
            warning = "dataAggregation: no database manager supplied; using preset raw indicators."
            return {"raw_indicators": raw, "pipeline_errors": (warning,)}

        raw, recordIDs, warnings = await aggregateRawIndicators(databaseManager, scope)
        update: dict[str, Any] = {"raw_indicators": raw, "tracing_keys": {"record_ids": recordIDs}}
        if warnings:
            update["pipeline_errors"] = tuple(warnings)
        return update

    return aggregationNode


# Node 3: Mathematical Synthesis (CRI, momentum & IPI)

def resolveDomainWeights(scope: AssessmentScope) -> dict[str, float]:
    for key in (str(scope.species_id), scope.scientific_name):
        if key and key.strip().casefold() in SPECIES_DOMAIN_WEIGHTS:
            return dict(SPECIES_DOMAIN_WEIGHTS[key.strip().casefold()])
    return dict(DOMAIN_WEIGHTS)


async def fetchPreviousAssessment(databaseManager: Any, scope: AssessmentScope) -> Optional[dict[str, Any]]:
    periodEndMs = toEpochMilliseconds(scope.period_end)
    earliestMs = periodEndMs - int(MOMENTUM_MAX_LOOKBACK_DAYS * MILLISECONDS_PER_DAY)
    rows = await databaseManager.fetchAll(
        """
        SELECT species_id, h3_cell, period_start, period_end, period_end_epoch_ms,
               conservation_risk_index, risk_momentum_per_day
        FROM risk_assessments INDEXED BY idx_risk_assessments_cell_time
        WHERE h3_cell = ? AND species_id = ? AND period_end_epoch_ms < ? AND period_end_epoch_ms >= ?
        ORDER BY period_end_epoch_ms DESC
        LIMIT 1;
        """,
        [scope.h3_cell, scope.species_id, periodEndMs, earliestMs],
    )
    return rows[0] if rows else None


async def fetchSpeciesCriticalNe(databaseManager: Any, scope: AssessmentScope) -> Optional[float]:
    rows = await databaseManager.fetchAll("SELECT critical_ne_threshold FROM species WHERE id = ?;", [scope.species_id])
    return toFiniteFloat(rows[0]["critical_ne_threshold"]) if rows else None


def calculateProjectedInbreeding(effectivePopulationSize: float, generations: float = INBREEDING_HORIZON_GENERATIONS) -> float:
    # Wright's accumulation of inbreeding under drift: F_t = 1 - (1 - 1 / (2 Ne))^t
    if effectivePopulationSize <= 0.5:
        return 1.0
    return 1.0 - (1.0 - 1.0 / (2.0 * effectivePopulationSize)) ** generations


def calculateInbreedingPenaltyIndex(
    effectivePopulationSize: Optional[float],
    expectedHeterozygosity: Optional[float],
    criticalNe: float,
    subscores: NormalizedSubscores,
) -> tuple[float, dict[str, float]]:
    # Returns the Inbreeding Penalty Index (0-100) plus its components for the narrative justification
    components: dict[str, float] = {}
    # Hard stop: below the critical Ne the population is already in an inbreeding depression crisis
    if effectivePopulationSize is not None and effectivePopulationSize < criticalNe:
        components["projected_inbreeding_f"] = calculateProjectedInbreeding(effectivePopulationSize)
        return DATABASE_SCORE_SCALE, components

    weightedPenalties: list[tuple[float, float]] = []
    if effectivePopulationSize is not None:
        # Drift penalty: projected F over the horizon relative to the tolerable inbreeding level
        projectedInbreeding = calculateProjectedInbreeding(effectivePopulationSize)
        components["projected_inbreeding_f"] = projectedInbreeding
        components["drift_penalty"] = DATABASE_SCORE_SCALE * min(1.0, projectedInbreeding / INBREEDING_TOLERANCE_F)
        weightedPenalties.append((INBREEDING_DRIFT_WEIGHT, components["drift_penalty"]))
    if expectedHeterozygosity is not None:
        # Diversity penalty: fraction of baseline heterozygosity already lost
        components["heterozygosity_penalty"] = DATABASE_SCORE_SCALE * clamp(
            1.0 - expectedHeterozygosity / HETEROZYGOSITY_BASELINE, 0.0, 1.0
        )
        weightedPenalties.append((1.0 - INBREEDING_DRIFT_WEIGHT, components["heterozygosity_penalty"]))

    # With no genetic assays the genetics subscore is the best available proxy
    if not weightedPenalties:
        return subscores.genetics * DATABASE_SCORE_SCALE, components
    totalWeight = math.fsum(weight for weight, _ in weightedPenalties)
    if totalWeight <= 0.0:
        return max(penalty for _, penalty in weightedPenalties), components
    return math.fsum(weight * penalty for weight, penalty in weightedPenalties) / totalWeight, components


def calculateFeasibilityMultiplier(centre: tuple[float, float]) -> float:
    # Closer ranger outposts make an intervention cheaper and faster, raising its priority
    if not RANGER_OUTPOSTS:
        return 1.0
    nearestMeters = min(
        haversineMeters(centre[0], centre[1], float(outpost["latitude"]), float(outpost["longitude"]))
        for outpost in RANGER_OUTPOSTS
    )
    distanceFraction = min(1.0, nearestMeters / FEASIBILITY_RADIUS_METERS)
    return FEASIBILITY_NEAR + (FEASIBILITY_FAR - FEASIBILITY_NEAR) * distanceFraction


def calculateInterventionPriorityIndex(
    conservationRiskIndex: float, momentum: MomentumVector, centre: tuple[float, float]
) -> tuple[float, dict[str, float]]:
    # IPI = CRI (0-100) x momentum factor x operational feasibility; uncapped so critical cells still rank apart
    momentumFactor = clamp(
        1.0 + MOMENTUM_GAIN * momentum.slope_per_day * MOMENTUM_WINDOW_DAYS, MOMENTUM_FACTOR_MIN, MOMENTUM_FACTOR_MAX
    )
    feasibilityMultiplier = calculateFeasibilityMultiplier(centre)
    priorityIndex = conservationRiskIndex * DATABASE_SCORE_SCALE * momentumFactor * feasibilityMultiplier
    components = {"momentum_factor": momentumFactor, "feasibility_multiplier": feasibilityMultiplier}
    return priorityIndex, components


def isPoachingThreat(event: ThreatEvent, windowEnd: datetime) -> bool:
    ageHours = (windowEnd - event.detected_at).total_seconds() / 3600.0
    return (
        getThreatClassWeight(event.class_name) >= POACHING_CLASS_WEIGHT
        and event.confidence >= POACHING_MIN_CONFIDENCE
        and 0.0 <= ageHours <= POACHING_RECENCY_HOURS
    )


def classifyPopulationTrend(herdCounts: tuple[float, ...]) -> Optional[str]:
    if len(herdCounts) < MINIMUM_TREND_POINTS:
        return None
    half = len(herdCounts) // 2
    earlierMean = statistics.fmean(herdCounts[:half])
    laterMean = statistics.fmean(herdCounts[-half:])
    if earlierMean <= 0.0:
        return "INCREASING" if laterMean > 0.0 else "STABLE"
    change = (laterMean - earlierMean) / earlierMean
    if change <= -TREND_CHANGE_THRESHOLD:
        return "DECLINING"
    if change >= TREND_CHANGE_THRESHOLD:
        return "INCREASING"
    return "STABLE"


def createSynthesisNode(databaseManager: Optional[Any] = None) -> Any:
    @validatedNode(NODE_SYNTHESIS)
    async def synthesisNode(state: dict[str, Any]) -> dict[str, Any]:
        scope: Optional[AssessmentScope] = state.get("assessment_scope")
        subscores: Optional[NormalizedSubscores] = state.get("normalized_subscores")
        raw: Optional[RawIndicators] = state.get("raw_indicators")
        if scope is None or subscores is None or raw is None:
            raise StateValidationError("The Synthesis Node requires assessment_scope, raw_indicators and normalized_subscores.")

        # CRI = sum(w_j * S_j) using species-specific weights where configured
        domainWeights = resolveDomainWeights(scope)
        conservationRiskIndex = calculateConservationRiskIndex(subscores, domainWeights)

        warnings: list[str] = []
        update: dict[str, Any] = {}
        previousCri: Optional[float] = None
        previousSlope: Optional[float] = None
        windowDays = MOMENTUM_WINDOW_DAYS
        criticalNe = CRITICAL_NE_THRESHOLD
        if databaseManager is not None:
            try:
                previous = await fetchPreviousAssessment(databaseManager, scope)
                if previous is not None:
                    previousCri = clamp(float(previous["conservation_risk_index"]) / DATABASE_SCORE_SCALE, 0.0, 1.0)
                    # Momentum = dCRI / dt over the real gap between the two assessment epochs
                    elapsedMs = toEpochMilliseconds(scope.period_end) - int(previous["period_end_epoch_ms"])
                    windowDays = elapsedMs / MILLISECONDS_PER_DAY
                    storedSlope = toFiniteFloat(previous["risk_momentum_per_day"])
                    previousSlope = None if storedSlope is None else storedSlope / DATABASE_SCORE_SCALE
                    update["tracing_keys"] = {"previous_assessment_key": {
                        key: previous[key] for key in ("species_id", "h3_cell", "period_start", "period_end")
                    }}
                speciesCriticalNe = await fetchSpeciesCriticalNe(databaseManager, scope)
                if speciesCriticalNe is not None:
                    criticalNe = max(criticalNe, speciesCriticalNe)
            except sqlite3.Error as error:
                warnings.append(f"riskSynthesis: history query failed, momentum defaults to STABLE: {error}")

        momentum = MomentumVector.fromHistory(conservationRiskIndex, previousCri, windowDays, previousSlope)
        effectivePopulationSize = raw.effective_population_size
        centre = getScopeCentre(scope)
        inbreedingPenaltyIndex, inbreedingComponents = calculateInbreedingPenaltyIndex(
            effectivePopulationSize, raw.expected_heterozygosity, criticalNe, subscores
        )
        interventionPriorityIndex, priorityComponents = calculateInterventionPriorityIndex(
            conservationRiskIndex, momentum, centre
        )
        update["risk_metrics"] = RiskMetrics(
            conservation_risk_index=conservationRiskIndex,
            inbreeding_penalty_index=inbreedingPenaltyIndex,
            intervention_priority_index=interventionPriorityIndex,
            inbreeding_components=inbreedingComponents,
            priority_components=priorityComponents,
            momentum_vector=momentum,
            population_trend=classifyPopulationTrend(raw.herd_count_series),
            effective_population_size=effectivePopulationSize,
            domain_weights=domainWeights,
            critical_inbreeding_flag=effectivePopulationSize is not None and effectivePopulationSize < criticalNe,
            immediate_poaching_threat=any(isPoachingThreat(event, raw.window_end) for event in raw.threat_events),
        )
        if warnings:
            update["pipeline_errors"] = tuple(warnings)
        return update

    return synthesisNode


# Node 4: Explainable Generation (Narrative Synthesis)

def createLanguageProvider(llm: Optional[Any] = None) -> LLMProvider:
    # An injected LangChain chat model (tests, custom deployments) bypasses hardware checks and the cloud route
    if llm is not None:
        return LLMProvider(localBackend=LangChainBackend(llm), cloudBackend=None, assessHardware=False)
    return LLMProvider()


def formatNumber(value: Optional[float], digits: int = 1) -> str:
    return "unknown" if value is None else f"{value:.{digits}f}"


def describeInbreedingPenalty(riskMetrics: RiskMetrics, raw: RawIndicators) -> str:
    components = riskMetrics.inbreeding_components
    penalty = riskMetrics.inbreeding_penalty_index
    if riskMetrics.critical_inbreeding_flag:
        return (
            f"Inbreeding Penalty Index pinned to {penalty:.1f} because Ne = {formatNumber(raw.effective_population_size)} "
            f"is below the critical threshold;"
        )
    terms: list[str] = []
    if "drift_penalty" in components:
        terms.append(
            f"drift 100 x F_{INBREEDING_HORIZON_GENERATIONS:g} / {INBREEDING_TOLERANCE_F:g} = {components['drift_penalty']:.1f} "
            f"(F = 1 - (1 - 1/(2 x {formatNumber(raw.effective_population_size)}))^{INBREEDING_HORIZON_GENERATIONS:g} "
            f"= {components['projected_inbreeding_f']:.4f})"
        )
    if "heterozygosity_penalty" in components:
        terms.append(
            f"diversity 100 x (1 - He / {HETEROZYGOSITY_BASELINE:g}) = {components['heterozygosity_penalty']:.1f} "
            f"(He = {formatNumber(raw.expected_heterozygosity, 3)})"
        )
    if not terms:
        return f"Inbreeding Penalty Index = genetics subscore proxy = {penalty:.1f} (no Ne or He assays);"
    return f"Inbreeding Penalty Index = weighted({'; '.join(terms)}) = {penalty:.1f};"


def buildNarrativeBlocks(
    report: ExplainabilityReport, state: dict[str, Any], dominantDomain: str, generatedBy: str
) -> tuple[NarrativeBlock, ...]:
    subscores: NormalizedSubscores = state["normalized_subscores"]
    riskMetrics: RiskMetrics = state["risk_metrics"]
    raw: RawIndicators = state["raw_indicators"]
    contributions = getWeightedContributions(riskMetrics, subscores)
    momentum = riskMetrics.momentum_vector
    criScore = riskMetrics.conservation_risk_index * DATABASE_SCORE_SCALE
    # Mathematical justifications are always built from state so the numbers never depend on LLM output
    criTerms = " + ".join(
        f"{riskMetrics.domain_weights[domain]:.2f}x{getattr(subscores, domain) * DATABASE_SCORE_SCALE:.1f}"
        for domain in RISK_DOMAINS
    )
    overallMetrics = {
        "conservation_risk_index": criScore,
        "inbreeding_penalty_index": riskMetrics.inbreeding_penalty_index,
        "risk_momentum_per_day": momentum.slope_per_day * DATABASE_SCORE_SCALE,
        "escalation_level": float(riskMetrics.escalation_level),
    }
    if riskMetrics.intervention_priority_index is not None:
        overallMetrics["intervention_priority_index"] = riskMetrics.intervention_priority_index
    overallMetrics.update(riskMetrics.priority_components)
    priorityJustification = ""
    if riskMetrics.intervention_priority_index is not None and riskMetrics.priority_components:
        priorityJustification = (
            f" IPI = CRI x momentum factor x feasibility = {criScore:.2f} x "
            f"{riskMetrics.priority_components['momentum_factor']:.2f} x "
            f"{riskMetrics.priority_components['feasibility_multiplier']:.2f} = "
            f"{riskMetrics.intervention_priority_index:.2f} ({riskMetrics.intervention_priority_tier})."
        )

    percentages = calculatePercentageContributions(contributions)
    geneticMetrics = {"genetics_subscore": subscores.genetics * DATABASE_SCORE_SCALE}
    for metricName, value in (
        ("effective_population_size", raw.effective_population_size),
        ("expected_heterozygosity", raw.expected_heterozygosity),
        ("census_count", raw.census_count),
        ("minimum_viable_population", raw.minimum_viable_population),
    ):
        if value is not None:
            geneticMetrics[metricName] = float(value)
    geneticMetrics["inbreeding_penalty_index"] = riskMetrics.inbreeding_penalty_index
    geneticMetrics.update(riskMetrics.inbreeding_components)
    inbreedingJustification = describeInbreedingPenalty(riskMetrics, raw)
    behaviorMetrics = {"behavior_subscore": subscores.behavior * DATABASE_SCORE_SCALE}
    behaviorMetrics.update({
        name: value for name, value in subscores.score_components.get("behavior", {}).items() if value is not None
    })
    mitigationText = formatMitigations(report)
    overallSummary = f"{report.executive_summary} Recommended mitigations: {mitigationText}" if mitigationText else report.executive_summary

    return (
        NarrativeBlock(
            domain="overall",
            summary_text=overallSummary,
            mathematical_justification=(
                f"CRI = sum(w_j x S_j) = {criTerms} = {criScore:.2f}; momentum = dCRI/dt = "
                f"{momentum.delta_cri * DATABASE_SCORE_SCALE:.2f} / {momentum.window_days:.1f} days.{priorityJustification}"
            ),
            cited_metrics=overallMetrics,
            generated_by=generatedBy,
        ),
        NarrativeBlock(
            domain=dominantDomain,
            summary_text=report.primary_threat_drivers,
            mathematical_justification=(
                f"{dominantDomain} contributes w x S = {riskMetrics.domain_weights[dominantDomain]:.2f} x "
                f"{getattr(subscores, dominantDomain) * DATABASE_SCORE_SCALE:.1f} = {contributions[dominantDomain]:.2f} "
                f"of the {criScore:.2f} CRI; shares: "
                + ", ".join(f"{domain} {percentages[domain]}%" for domain in RISK_DOMAINS) + "."
            ),
            cited_metrics={
                f"{dominantDomain}_subscore": getattr(subscores, dominantDomain) * DATABASE_SCORE_SCALE,
                f"{dominantDomain}_weight": riskMetrics.domain_weights[dominantDomain],
                f"{dominantDomain}_contribution": contributions[dominantDomain],
                **{f"{domain}_percentage": percentages[domain] for domain in RISK_DOMAINS},
            },
            generated_by=generatedBy,
        ),
        NarrativeBlock(
            domain="genetics",
            summary_text=report.genetic_status_breakdown,
            mathematical_justification=(
                f"{inbreedingJustification} genetics subscore = {subscores.genetics * DATABASE_SCORE_SCALE:.1f}."
            ),
            cited_metrics=geneticMetrics,
            generated_by=generatedBy,
        ),
        NarrativeBlock(
            domain="behavior",
            summary_text=report.behavioral_loss_impact,
            mathematical_justification=(
                f"behavior subscore = {subscores.behavior * DATABASE_SCORE_SCALE:.1f}; weighted contribution "
                f"{riskMetrics.domain_weights['behavior']:.2f} x {subscores.behavior * DATABASE_SCORE_SCALE:.1f} = "
                f"{contributions['behavior']:.2f} ({percentages['behavior']}% of CRI)."
            ),
            cited_metrics=behaviorMetrics,
            generated_by=generatedBy,
        ),
    )


def createExplainabilityNode(provider: Optional[LLMProvider] = None) -> Any:
    provider = provider or createLanguageProvider()

    @validatedNode(NODE_EXPLAINABILITY)
    async def explainabilityNode(state: dict[str, Any]) -> dict[str, Any]:
        subscores: Optional[NormalizedSubscores] = state.get("normalized_subscores")
        riskMetrics: Optional[RiskMetrics] = state.get("risk_metrics")
        if subscores is None or riskMetrics is None or state.get("raw_indicators") is None:
            raise StateValidationError("The Explainability Node requires raw_indicators, normalized_subscores and risk_metrics.")

        promptContext = buildPromptContext(state)
        dominantDomain = promptContext["primary_driver"]

        # The provider routes local -> cloud -> self-correction, rejecting reports whose figures, safety wording
        # or mitigations disagree with the payload, and returns the deterministic template if every route fails
        result = await provider.generateStructured(
            LLMRequest.fromMessages(buildPromptMessages(promptContext)),
            ExplainabilityReport,
            validator=lambda parsedReport: validateReportAgainstContext(parsedReport, promptContext),
            fallback=lambda: buildFallbackReport(promptContext),
        )
        report: ExplainabilityReport = result.output
        generatedBy = FALLBACK_GENERATOR if result.usedFallback else result.generated_by
        warnings = [f"narrativeSynthesis: {warning}" for warning in result.warnings]
        if result.usedFallback:
            warnings.append("narrativeSynthesis: LLM generation failed, used template")

        update: dict[str, Any] = {
            "narrative_blocks": buildNarrativeBlocks(report, state, dominantDomain, generatedBy)
        }
        if warnings:
            update["pipeline_errors"] = tuple(warnings)
        return update

    return explainabilityNode


# Node 5: Intervention Mapping & Edge Routing

def selectThreatTargets(raw: RawIndicators, centre: tuple[float, float]) -> list[dict[str, Any]]:
    # Most severe then most recent located threats become dispatch targets; the scope centre is the fallback
    locatedThreats = sorted(
        (event for event in raw.threat_events if event.latitude is not None and event.longitude is not None),
        key=lambda event: (getThreatClassWeight(event.class_name), event.detected_at),
        reverse=True,
    )
    targets: dict[tuple[float, float], dict[str, Any]] = {}
    for event in locatedThreats:
        key = (round(event.latitude, 4), round(event.longitude, 4))
        if key not in targets and len(targets) >= MAX_DISPATCH_TARGETS:
            continue
        target = targets.setdefault(key, {"latitude": event.latitude, "longitude": event.longitude,
                                          "classNames": [], "detectionIDs": []})
        if event.class_name not in target["classNames"]:
            target["classNames"].append(event.class_name)
        if event.record_id is not None:
            target["detectionIDs"].append(event.record_id)
    if not targets:
        return [{"latitude": centre[0], "longitude": centre[1], "classNames": [], "detectionIDs": []}]
    return list(targets.values())


def buildDroneDeployment(latitude: float, longitude: float, resolution: int, priority: str, reason: str) -> DroneDeployment:
    return DroneDeployment(
        latitude=latitude,
        longitude=longitude,
        altitude_m=DRONE_ALTITUDE_M,
        h3_cell=h3.latlng_to_cell(latitude, longitude, resolution),
        priority=priority,
        reason=reason,
    )


@validatedNode(NODE_MAPPING)
async def interventionMappingNode(state: dict[str, Any]) -> dict[str, Any]:
    scope: Optional[AssessmentScope] = state.get("assessment_scope")
    subscores: Optional[NormalizedSubscores] = state.get("normalized_subscores")
    riskMetrics: Optional[RiskMetrics] = state.get("risk_metrics")
    raw: Optional[RawIndicators] = state.get("raw_indicators")
    if scope is None or subscores is None or riskMetrics is None or raw is None:
        raise StateValidationError("The Intervention Mapping Node requires the scope, raw indicators, subscores and risk metrics.")

    level = riskMetrics.escalation_level
    actions: list[str] = list(LEVEL_BASE_ACTIONS[level])
    contributions = getWeightedContributions(riskMetrics, subscores)
    dominantDomain = max(RISK_DOMAINS, key=lambda domain: contributions[domain])
    centre = getScopeCentre(scope)
    resolution = h3.get_resolution(scope.h3_cell)
    droneDeployments: list[DroneDeployment] = []
    rangerAlerts: list[RangerDispatchAlert] = []
    rationale: list[str] = [
        f"CRI {riskMetrics.conservation_risk_index * DATABASE_SCORE_SCALE:.1f} maps to Level {level}; "
        f"dominant driver is {dominantDomain} ({contributions[dominantDomain]:.1f} CRI points)."
    ]

    def dispatchToThreats(priority: str, reason: str) -> None:
        # Ranger alerts and drone overwatch are paired at each threat coordinate
        for target in selectThreatTargets(raw, centre):
            threatLabel = ", ".join(target["classNames"]) or "elevated threat pressure"
            rangerAlerts.append(RangerDispatchAlert(
                latitude=target["latitude"],
                longitude=target["longitude"],
                priority=priority,
                message=f"{reason}: {threatLabel} near {target['latitude']:.5f}, {target['longitude']:.5f}.",
                channel=ALERT_CHANNEL,
                source_detection_ids=tuple(sorted(set(target["detectionIDs"]))),
            ))
            droneDeployments.append(buildDroneDeployment(
                target["latitude"], target["longitude"], resolution, priority, f"{reason}: overwatch for {threatLabel}"
            ))
        actions.extend(["RANGER_DISPATCH", "DRONE_DEPLOYMENT"])

    # Critical risk (CRI >= 80) maps a directive onto the primary threat driver
    if riskMetrics.conservation_risk_index >= CRITICAL_CRI_THRESHOLD:
        if dominantDomain == "threat" and not riskMetrics.immediate_poaching_threat:
            dispatchToThreats("HIGH", "Critical threat pressure")
        elif dominantDomain == "genetics":
            actions.extend(["GENETIC_RESCUE", "CAPTIVE_BREEDING_PREP"])
        elif dominantDomain in ("habitat", "climate"):
            actions.append("HABITAT_RESTORATION")
        elif dominantDomain == "population":
            actions.append("TARGETED_PATROLS")
        elif dominantDomain == "behavior":
            droneDeployments.append(buildDroneDeployment(
                centre[0], centre[1], resolution, "HIGH", "Behavioural disruption: aerial herd structure survey"
            ))
            actions.append("DRONE_DEPLOYMENT")
        rationale.append(f"CRI is at or above {CRITICAL_CRI_THRESHOLD * DATABASE_SCORE_SCALE:.0f}, so {dominantDomain} directives apply.")

    # Deterministic trigger flags always add their mandatory protocols
    if riskMetrics.immediate_poaching_threat:
        dispatchToThreats("CRITICAL", "Immediate poaching threat")
        rationale.append("Immediate poaching threat flag requires ranger dispatch.")
    if riskMetrics.critical_inbreeding_flag:
        actions.append("GENETIC_RESCUE")
        rationale.append(f"Ne {formatNumber(riskMetrics.effective_population_size)} is below the critical threshold.")

    routing = InterventionRouting(
        escalation_level=level,
        actions=tuple(dict.fromkeys(actions)),
        drone_deployments=tuple(droneDeployments),
        ranger_alerts=tuple(rangerAlerts),
        rationale=" ".join(rationale),
    )
    update: dict[str, Any] = {"intervention_routing": routing}
    # Flattened risk_assessments row for asynchronous persistence by the database manager
    update["serialization_payload"] = buildSerializationPayload({**state, **update})
    return update


# Directed edge compilation

def buildWorkflow(
    databaseManager: Optional[Any] = None,
    llm: Optional[Any] = None,
    rawIndicators: Optional[RawIndicators] = None,
    provider: Optional[LLMProvider] = None,
) -> StateGraph:
    workflow = StateGraph(GraphState)
    workflow.add_node(NODE_AGGREGATION, createAggregationNode(databaseManager, rawIndicators))
    workflow.add_node(NODE_NORMALIZATION, createIndicatorCalculatorNode(databaseManager))
    workflow.add_node(NODE_SYNTHESIS, createSynthesisNode(databaseManager))
    workflow.add_node(NODE_EXPLAINABILITY, createExplainabilityNode(provider or createLanguageProvider(llm)))
    workflow.add_node(NODE_MAPPING, interventionMappingNode)

    # Aggregation -> Normalization -> Synthesis -> Explainability -> Mapping -> END
    workflow.add_edge(START, NODE_AGGREGATION)
    workflow.add_edge(NODE_AGGREGATION, NODE_NORMALIZATION)
    workflow.add_edge(NODE_NORMALIZATION, NODE_SYNTHESIS)
    workflow.add_edge(NODE_SYNTHESIS, NODE_EXPLAINABILITY)
    workflow.add_edge(NODE_EXPLAINABILITY, NODE_MAPPING)
    workflow.add_edge(NODE_MAPPING, END)
    return workflow


def createCheckpointer() -> MemorySaver:
    # Register every stateSchema model so checkpoints deserialize them without the unregistered-type warning
    stateModels = [
        ("aiEngine.stateSchema", name)
        for name, value in vars(stateSchema).items()
        if isinstance(value, type) and issubclass(value, BaseModel) and value.__module__ == "aiEngine.stateSchema"
    ]
    return MemorySaver(serde=JsonPlusSerializer(allowed_msgpack_modules=stateModels))


def compileWorkflow(
    databaseManager: Optional[Any] = None,
    llm: Optional[Any] = None,
    rawIndicators: Optional[RawIndicators] = None,
    checkpointer: Optional[Any] = None,
    provider: Optional[LLMProvider] = None,
) -> Any:
    # MemorySaver checkpoints every node so a run can be inspected or resumed by its thread_id
    return buildWorkflow(databaseManager, llm, rawIndicators, provider).compile(checkpointer=checkpointer or createCheckpointer())


# Default database-less graph; the orchestrator builds its own with compileWorkflow(databaseManager=manager)
workflow = buildWorkflow()
app = workflow.compile(checkpointer=createCheckpointer())


async def runAssessment(
    scope: AssessmentScope | dict[str, Any],
    runID: Optional[str] = None,
    databaseManager: Optional[Any] = None,
    graphApp: Optional[Any] = None,
    persist: bool = True,
) -> dict[str, Any]:
    runID = runID or f"assessment-{uuid.uuid4().hex}"
    graphApp = graphApp or (compileWorkflow(databaseManager) if databaseManager is not None else app)
    # thread_id keys the checkpoint so app.aget_state() can audit this run afterwards
    result = await graphApp.ainvoke(createInitialState(runID, scope), config={"configurable": {"thread_id": runID}})
    if persist and databaseManager is not None:
        await databaseManager.executeWrite(*buildPersistenceStatement(result))
    return result


async def main() -> None:
    windowEnd = datetime.now(timezone.utc).replace(microsecond=0)
    windowStart = windowEnd - timedelta(days=30)
    demoRaw = RawIndicators.model_validate({
        "aggregated_at": windowEnd,
        "window_start": windowStart,
        "window_end": windowEnd,
        "effective_population_size": 42.0,
        "expected_heterozygosity": 0.31,
        "census_count": 380,
        "minimum_viable_population": 500,
        "herd_count_series": [52, 50, 49, 51, 47, 46, 44, 45, 43, 41, 40, 39, 40, 38],
        "age_class_counts": {"juvenile": 9, "adult": 40, "elder": 1},
        "threat_events": [
            {"detected_at": windowEnd - timedelta(hours=2), "class_name": "gunshot", "confidence": 0.92,
             "latitude": -1.31, "longitude": 36.81},
            {"detected_at": windowEnd - timedelta(days=6), "class_name": "chainsaw", "confidence": 0.8,
             "latitude": -1.28, "longitude": 36.77},
        ],
        "sensor_metric_means": {"dissolved_oxygen_mg_l": 5.1, "water_turbidity_ntu": 22.0, "ambient_temperature_c": 31.5},
    })
    scope = {
        "species_id": 1,
        "scientific_name": "Loxodonta africana",
        "h3_cell": "877a6e5a4ffffff",
        "period_start": windowStart,
        "period_end": windowEnd,
        "latitude": -1.30,
        "longitude": 36.80,
    }
    demoApp = compileWorkflow(rawIndicators=demoRaw)
    result = await runAssessment(scope, runID="graph-demo", graphApp=demoApp, persist=False)

    riskMetrics: RiskMetrics = result["risk_metrics"]
    routing: InterventionRouting = result["intervention_routing"]
    print(f"Node history: {' -> '.join(result['node_history'])}")
    print(f"CRI: {riskMetrics.conservation_risk_index * DATABASE_SCORE_SCALE:.1f} (Level {riskMetrics.escalation_level})")
    print(f"Intervention Priority Index: {riskMetrics.intervention_priority_index:.1f} ({riskMetrics.intervention_priority_tier})")
    print(f"Inbreeding Penalty Index: {riskMetrics.inbreeding_penalty_index:.1f}")
    for block in result["narrative_blocks"]:
        print(f"[{block.domain} maths] {block.mathematical_justification}")
    print(f"Actions: {', '.join(routing.actions)}")
    for block in result["narrative_blocks"]:
        print(f"[{block.domain}] {block.summary_text}")
    for warning in result.get("pipeline_errors", ()):
        print(f"Warning: {warning}")
    snapshot = await demoApp.aget_state({"configurable": {"thread_id": "graph-demo"}})
    print(f"Checkpointed channels: {len(snapshot.values)}")


if __name__ == "__main__":
    asyncio.run(main())