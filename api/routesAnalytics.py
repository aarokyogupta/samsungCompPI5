import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, FastAPI, Query, Request
import h3
import httpx
import logging
import math
import numpy as np
import os
from starlette.exceptions import HTTPException
import sys
from typing import Any, Literal, Optional
import yaml

# Allow "python api/routesAnalytics.py" as well as loading through api/main.py
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from api.routesTelemetry import (  # noqa: E402
    H3_RESOLUTION,
    MEASUREMENT_COLUMNS,
    STATS_METRICS,
    TABLE_SPECS,
    AsyncTtlCache,
    buildFilters,
    fromEpochMs,
    getDatabase,
    toEpochMs,
)
from database.models import formatUtcTimestamp  # noqa: E402

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

ANALYTICS_CONFIG: dict = config.get("analyticsApi", {}) or {}

# Define cache settings
CACHE_TTL_SEC: float = float(ANALYTICS_CONFIG.get("cacheTtlSec", 300))
CACHE_MAX_ENTRIES: int = int(ANALYTICS_CONFIG.get("cacheMaxEntries", 128))

# Define summary settings
SUMMARY_WINDOW_HOURS: float = float(ANALYTICS_CONFIG.get("summaryWindowHours", 24))
SUMMARY_MAX_WINDOW_HOURS: float = float(ANALYTICS_CONFIG.get("summaryMaxWindowHours", 720))
ROLLING_PRECEDING_ROWS: int = int(ANALYTICS_CONFIG.get("rollingPrecedingRows", 6))
SUMMARY_METRICS: tuple[str, ...] = tuple(
    ANALYTICS_CONFIG.get("summaryMetrics", ["ambient_temperature_c", "relative_humidity_percent"]) or []
)
DELTA_FLAT_PERCENT: float = float(ANALYTICS_CONFIG.get("deltaFlatPercent", 1.0))

# Define historical downsampling settings
HOURLY_MAX_DAYS: float = float(ANALYTICS_CONFIG.get("hourlyMaxDays", 14))
DAILY_MAX_DAYS: float = float(ANALYTICS_CONFIG.get("dailyMaxDays", 180))
HISTORICAL_MAX_POINTS: int = int(ANALYTICS_CONFIG.get("historicalMaxPoints", 1000))
HISTORICAL_DEFAULT_LOOKBACK_DAYS: float = float(ANALYTICS_CONFIG.get("historicalDefaultLookbackDays", 30))
IMPUTATION_METHOD: str = str(ANALYTICS_CONFIG.get("imputation", "forward_fill"))
IMPUTATION_MAX_GAP_BUCKETS: int = int(ANALYTICS_CONFIG.get("imputationMaxGapBuckets", 12))

# Define radar settings
RADAR_ZONE_RESOLUTION: int = int(ANALYTICS_CONFIG.get("radarZoneResolution", 6))
RADAR_DEFAULT_ZONES: int = int(ANALYTICS_CONFIG.get("radarDefaultZones", 5))
RADAR_MAX_ZONES: int = int(ANALYTICS_CONFIG.get("radarMaxZones", 8))
RADAR_LOOKBACK_DAYS: float = float(ANALYTICS_CONFIG.get("radarLookbackDays", 30))
RADAR_AXIS_WEIGHTS: dict[str, float] = {
    str(name): float(weight) for name, weight in (ANALYTICS_CONFIG.get("radarAxisWeights", {}) or {}).items()
}

# Define spatial clustering settings
SPATIAL_GRID_DIVISIONS: int = int(ANALYTICS_CONFIG.get("spatialGridDivisions", 64))
SPATIAL_MAX_GRID_DIVISIONS: int = int(ANALYTICS_CONFIG.get("spatialMaxGridDivisions", 256))
SPATIAL_LOOKBACK_DAYS: float = float(ANALYTICS_CONFIG.get("spatialLookbackDays", 7))
SPATIAL_MAX_KMEANS_CLUSTERS: int = int(ANALYTICS_CONFIG.get("spatialMaxKMeansClusters", 50))
SPATIAL_KMEANS_ITERATIONS: int = int(ANALYTICS_CONFIG.get("spatialKMeansIterations", 25))

HOUR_MS: int = 3_600_000
DAY_MS: int = 86_400_000
WEEK_MS: int = 7 * DAY_MS
# The Unix epoch was a Thursday; shifting by four days aligns week buckets to Monday 00:00 UTC
WEEK_OFFSET_MS: int = 4 * DAY_MS
BUCKET_SIZES_MS: dict[str, int] = {"hour": HOUR_MS, "day": DAY_MS, "week": WEEK_MS}
BUCKET_ORDER: tuple[str, ...] = ("hour", "day", "week")

ImputationMethod = Literal["forward_fill", "linear", "none"]
HistoricalSource = Literal["risk", "environmental", "gps", "acoustic", "vision"]
HistoricalBucket = Literal["auto", "hour", "day", "week"]
SpatialLayer = Literal["gps", "acoustic", "vision", "environmental"]
SpatialMethod = Literal["grid", "kmeans"]

# Radar axes; the sign of each configured weight decides whether a larger value is healthier
RADAR_AXES: dict[str, tuple[str, str]] = {
    "wildlife_sightings": ("Wildlife Sightings", "detections"),
    "animal_presence": ("Animal Presence", "animals"),
    "temperature_variability": ("Temperature Stability", "°C std dev"),
    "acoustic_threats": ("Acoustic Peace", "threat events"),
    "conservation_risk": ("Conservation Risk", "CRI"),
}

if CACHE_TTL_SEC < 0 or CACHE_MAX_ENTRIES <= 0:
    raise ValueError("analyticsApi cacheTtlSec must be non-negative and cacheMaxEntries positive.")
if not 0 < SUMMARY_WINDOW_HOURS <= SUMMARY_MAX_WINDOW_HOURS or ROLLING_PRECEDING_ROWS < 0:
    raise ValueError("analyticsApi summary window must be positive and within summaryMaxWindowHours.")
if any(name not in MEASUREMENT_COLUMNS for name in SUMMARY_METRICS):
    raise ValueError(f"analyticsApi summaryMetrics must be sensor_readings measurements: {list(MEASUREMENT_COLUMNS)}.")
if not 0 < HOURLY_MAX_DAYS <= DAILY_MAX_DAYS or HISTORICAL_MAX_POINTS <= 0 or HISTORICAL_DEFAULT_LOOKBACK_DAYS <= 0:
    raise ValueError("analyticsApi historical day limits and maximum points must be positive and ordered.")
if IMPUTATION_METHOD not in ("forward_fill", "linear", "none") or IMPUTATION_MAX_GAP_BUCKETS < 0:
    raise ValueError("analyticsApi imputation must be forward_fill, linear or none with a non-negative gap limit.")
if not 0 <= RADAR_ZONE_RESOLUTION <= H3_RESOLUTION:
    raise ValueError(f"analyticsApi radarZoneResolution must be between 0 and edna.h3Resolution ({H3_RESOLUTION}).")
if not 0 < RADAR_DEFAULT_ZONES <= RADAR_MAX_ZONES or RADAR_LOOKBACK_DAYS <= 0:
    raise ValueError("analyticsApi radar zone counts and lookback must be positive with default <= max.")
if not RADAR_AXIS_WEIGHTS or any(name not in RADAR_AXES for name in RADAR_AXIS_WEIGHTS):
    raise ValueError(f"analyticsApi radarAxisWeights must use the axes {list(RADAR_AXES)}.")
if not 2 <= SPATIAL_GRID_DIVISIONS <= SPATIAL_MAX_GRID_DIVISIONS or SPATIAL_LOOKBACK_DAYS <= 0:
    raise ValueError("analyticsApi spatial grid divisions must be at least 2 and within the maximum.")
if SPATIAL_MAX_KMEANS_CLUSTERS <= 0 or SPATIAL_KMEANS_ITERATIONS <= 0:
    raise ValueError("analyticsApi k-means cluster and iteration limits must be positive.")

logger = logging.getLogger("icmis.api.analytics")


# Shared helpers: cache access, window snapping and delta maths

def getAnalyticsCache(request: Request) -> AsyncTtlCache:
    cache: Optional[AsyncTtlCache] = getattr(request.app.state, "analyticsCache", None)
    if cache is None:
        # Created lazily when the router is mounted without its startup hook (e.g. in tests)
        cache = AsyncTtlCache(CACHE_TTL_SEC, CACHE_MAX_ENTRIES)
        request.app.state.analyticsCache = cache
    return cache


def resolveWindow(
    startTime: Optional[datetime], endTime: Optional[datetime], lookbackMs: int
) -> tuple[int, int]:
    # Default windows end on the next whole minute so repeated dashboard loads share one cache key
    endMs = toEpochMs(endTime) if endTime is not None else -(-toEpochMs(datetime.now(timezone.utc)) // 60_000) * 60_000
    startMs = toEpochMs(startTime) if startTime is not None else endMs - lookbackMs
    if startMs >= endMs:
        raise HTTPException(status_code=422, detail="start_time must be earlier than end_time.")
    return startMs, endMs


def roundOrNone(value: Optional[float], digits: int = 3) -> Optional[float]:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return None
    return round(float(value), digits)


def computeDelta(current: Optional[float], previous: Optional[float]) -> tuple[Optional[float], str]:
    if current is None or previous is None:
        return None, "unknown"
    if previous == 0:
        # A rise from zero has no finite percentage; report the direction only
        if current == 0:
            return 0.0, "flat"
        return None, "up" if current > 0 else "down"
    deltaPercent = (current - previous) / abs(previous) * 100.0
    if abs(deltaPercent) < DELTA_FLAT_PERCENT:
        return round(deltaPercent, 2), "flat"
    return round(deltaPercent, 2), "up" if deltaPercent > 0 else "down"


def buildStatCard(
    key: str, label: str, unit: str, current: Optional[float], previous: Optional[float], windowText: str
) -> dict[str, Any]:
    deltaPercent, trend = computeDelta(current, previous)
    if trend == "unknown":
        deltaText = "No comparison data"
    elif deltaPercent is None:
        deltaText = f"{'Up' if trend == 'up' else 'Down'} from 0 vs previous {windowText}"
    elif trend == "flat":
        deltaText = f"Stable vs previous {windowText}"
    else:
        deltaText = f"{'Up' if trend == 'up' else 'Down'} {abs(deltaPercent):.1f}% vs previous {windowText}"
    return {
        "key": key,
        "label": label,
        "value": roundOrNone(current),
        "unit": unit,
        "previous_value": roundOrNone(previous),
        "delta_percent": deltaPercent,
        "trend": trend,
        "delta_text": deltaText,
    }


def measurementUnit(column: str) -> str:
    if column == "water_ph":
        return "pH"
    if column == "air_quality_level" or column == "pollutant_concentration_level":
        return "level"
    suffixes = {
        "_c": "°C", "_percent": "%", "_mg_l": "mg/L", "_ntu": "NTU", "_psu": "PSU",
        "_m_s": "m/s", "_us_cm": "µS/cm", "_hpa": "hPa",
    }
    for suffix, unit in suffixes.items():
        if column.endswith(suffix):
            return unit
    return ""


def measurementLabel(column: str) -> str:
    return column.replace("_", " ").title()


# Module-level router; api/main.py mounts it at /api/v1/analytics

router = APIRouter()


# Lifecycle hooks called by api/main.py

async def onStartup(app: FastAPI) -> None:
    app.state.analyticsCache = AsyncTtlCache(CACHE_TTL_SEC, CACHE_MAX_ENTRIES)


async def onShutdown(app: FastAPI) -> None:
    app.state.analyticsCache = None


# 1. Statistical aggregation

async def queryDetectionWindow(
    databaseManager: Any, startMs: int, endMs: int, speciesID: Optional[int]
) -> dict[str, Any]:
    conditions = ["detected_epoch_ms >= ?", "detected_epoch_ms < ?"]
    parameters: list[object] = [startMs, endMs]
    if speciesID is not None:
        conditions.append("species_id = ?")
        parameters.append(speciesID)
    rows = await databaseManager.fetchAll(
        "SELECT COUNT(*) AS total_detections, "
        "COALESCE(SUM(category = 'target_wildlife'), 0) AS wildlife_sightings, "
        "COALESCE(SUM(category = 'immediate_threat'), 0) AS threat_alerts, "
        "COUNT(DISTINCT species_id) AS species_detected "
        f"FROM detections WHERE {' AND '.join(conditions)};",
        parameters,
    )
    return rows[0] if rows else {}


async def queryGpsWindow(databaseManager: Any, startMs: int, endMs: int, speciesID: Optional[int]) -> dict[str, Any]:
    conditions = ["recorded_epoch_ms >= ?", "recorded_epoch_ms < ?"]
    parameters: list[object] = [startMs, endMs]
    if speciesID is not None:
        conditions.append("species_id = ?")
        parameters.append(speciesID)
    rows = await databaseManager.fetchAll(
        "SELECT COUNT(*) AS gps_fixes, COUNT(DISTINCT animal_id) AS active_animals, "
        "AVG(speed_kmh) AS average_speed_kmh "
        f"FROM telemetry WHERE {' AND '.join(conditions)};",
        parameters,
    )
    return rows[0] if rows else {}


async def queryEnvironmentalWindow(databaseManager: Any, startMs: int, endMs: int) -> dict[str, Any]:
    if not SUMMARY_METRICS:
        return {}
    # Each sensor's latest rolling average smooths single noisy readings; the card is the mean across sensors
    rollingParts = [f"AVG({name}) OVER sensor_window AS {name}_rolling" for name in SUMMARY_METRICS]
    outerParts = [f"AVG({name}_rolling) AS {name}" for name in SUMMARY_METRICS]
    rows = await databaseManager.fetchAll(
        f"SELECT COUNT(*) AS sensor_count, SUM(reading_count) AS reading_count, {', '.join(outerParts)} FROM ("
        f"SELECT sensor_mac, {', '.join(rollingParts)}, "
        "ROW_NUMBER() OVER (PARTITION BY sensor_mac ORDER BY recorded_epoch_ms DESC, id DESC) AS latest_rank, "
        "COUNT(*) OVER (PARTITION BY sensor_mac) AS reading_count "
        "FROM sensor_readings "
        "WHERE recorded_epoch_ms >= ? AND recorded_epoch_ms < ? AND quality_flag != 'INVALID' "
        "WINDOW sensor_window AS ("
        f"PARTITION BY sensor_mac ORDER BY recorded_epoch_ms, id ROWS BETWEEN {ROLLING_PRECEDING_ROWS} PRECEDING AND CURRENT ROW)"
        ") WHERE latest_rank = 1;",
        [startMs, endMs],
    )
    return rows[0] if rows else {}


async def queryRiskWindow(databaseManager: Any, startMs: int, endMs: int, speciesID: Optional[int]) -> dict[str, Any]:
    conditions = ["period_end_epoch_ms >= ?", "period_end_epoch_ms < ?"]
    parameters: list[object] = [startMs, endMs]
    if speciesID is not None:
        conditions.append("species_id = ?")
        parameters.append(speciesID)
    # Only the latest assessment per species and cell counts, so re-runs do not inflate the averages
    rows = await databaseManager.fetchAll(
        "SELECT COUNT(*) AS assessed_cells, MAX(conservation_risk_index) AS max_cri, "
        "AVG(conservation_risk_index) AS mean_cri, COALESCE(SUM(escalation_level >= 4), 0) AS critical_cells, "
        "MAX(intervention_priority_index) AS top_ipi FROM ("
        "SELECT conservation_risk_index, escalation_level, intervention_priority_index, "
        "ROW_NUMBER() OVER (PARTITION BY species_id, h3_cell ORDER BY period_end_epoch_ms DESC) AS latest_rank "
        f"FROM risk_assessments WHERE {' AND '.join(conditions)}"
        ") WHERE latest_rank = 1;",
        parameters,
    )
    return rows[0] if rows else {}


async def queryWindowMetrics(
    databaseManager: Any, startMs: int, endMs: int, speciesID: Optional[int]
) -> dict[str, dict[str, Any]]:
    detections, gps, environmental, risk = await asyncio.gather(
        queryDetectionWindow(databaseManager, startMs, endMs, speciesID),
        queryGpsWindow(databaseManager, startMs, endMs, speciesID),
        queryEnvironmentalWindow(databaseManager, startMs, endMs),
        queryRiskWindow(databaseManager, startMs, endMs, speciesID),
    )
    return {"detections": detections, "gps": gps, "environmental": environmental, "risk": risk}


def formatWindowText(windowHours: float) -> str:
    if windowHours % 24 == 0 and windowHours >= 48:
        return f"{int(windowHours // 24)}d"
    return f"{windowHours:g}h"


async def computeSummary(
    databaseManager: Any, startMs: int, endMs: int, speciesID: Optional[int]
) -> dict[str, Any]:
    windowMs = endMs - startMs
    previousStartMs = startMs - windowMs
    # Current and previous windows run concurrently on the read pool
    current, previous = await asyncio.gather(
        queryWindowMetrics(databaseManager, startMs, endMs, speciesID),
        queryWindowMetrics(databaseManager, previousStartMs, startMs, speciesID),
    )
    windowText = formatWindowText(windowMs / HOUR_MS)
    cardSpecs: list[tuple[str, str, str, str]] = [
        ("detections", "total_detections", "Total Detections", "detections"),
        ("detections", "wildlife_sightings", "Wildlife Sightings", "detections"),
        ("detections", "threat_alerts", "Threat Alerts", "alerts"),
        ("detections", "species_detected", "Species Detected", "species"),
        ("gps", "gps_fixes", "GPS Fixes", "fixes"),
        ("gps", "active_animals", "Active Collared Animals", "animals"),
        ("gps", "average_speed_kmh", "Average Animal Speed", "km/h"),
        *[("environmental", name, f"{measurementLabel(name)} (rolling)", measurementUnit(name)) for name in SUMMARY_METRICS],
        ("risk", "assessed_cells", "Assessed Grid Cells", "cells"),
        ("risk", "mean_cri", "Mean Conservation Risk", "CRI"),
        ("risk", "max_cri", "Peak Conservation Risk", "CRI"),
        ("risk", "critical_cells", "Critical Cells (Level 4+)", "cells"),
        ("risk", "top_ipi", "Top Intervention Priority", "IPI"),
    ]
    cards = [
        buildStatCard(
            key, label, unit, current[domain].get(key), previous[domain].get(key), windowText
        ) | {"domain": domain}
        for domain, key, label, unit in cardSpecs
    ]
    return {
        "window_hours": round(windowMs / HOUR_MS, 3),
        "start_time": fromEpochMs(startMs),
        "end_time": fromEpochMs(endMs),
        "previous_start_time": fromEpochMs(previousStartMs),
        "rolling_window_rows": ROLLING_PRECEDING_ROWS + 1,
        "cards": cards,
        "environmental": {
            "sensor_count": int(current["environmental"].get("sensor_count") or 0),
            "reading_count": int(current["environmental"].get("reading_count") or 0),
        },
        "generated_at": formatUtcTimestamp(datetime.now(timezone.utc)),
    }


@router.get("/summary")
async def getAnalyticsSummary(
    request: Request,
    window_hours: float = Query(SUMMARY_WINDOW_HOURS, gt=0, le=SUMMARY_MAX_WINDOW_HOURS),
    end_time: Optional[datetime] = None,
    species_id: Optional[int] = Query(None, gt=0),
) -> dict[str, Any]:
    databaseManager = getDatabase(request)
    windowMs = int(round(window_hours * HOUR_MS))
    _, endMs = resolveWindow(None, end_time, windowMs)
    startMs = endMs - windowMs
    cacheKey = ("summary", startMs, endMs, species_id)
    result, isCached = await getAnalyticsCache(request).getOrCompute(
        cacheKey, lambda: computeSummary(databaseManager, startMs, endMs, species_id)
    )
    return {**result, "cached": isCached}


# 2. Historical subscore breakdowns

@dataclass(frozen=True)
class HistoricalMetric:
    expression: str
    label: str
    unit: str
    # "avg" series are imputed across gaps; "count" series are zero when nothing was recorded
    aggregate: Literal["avg", "count"]


@dataclass(frozen=True)
class HistoricalSourceSpec:
    tableName: str
    epochColumn: str
    metrics: dict[str, HistoricalMetric]
    defaultMetrics: tuple[str, ...]


RISK_SUBSCORES: tuple[str, ...] = (
    "population_subscore", "habitat_subscore", "threat_subscore",
    "climate_subscore", "genetics_subscore", "behavior_subscore",
)
DETECTION_HISTORICAL_METRICS: dict[str, HistoricalMetric] = {
    "detection_count": HistoricalMetric("COUNT(*)", "Detections", "detections", "count"),
    "wildlife_count": HistoricalMetric("SUM(category = 'target_wildlife')", "Wildlife Sightings", "detections", "count"),
    "threat_count": HistoricalMetric("SUM(category = 'immediate_threat')", "Threat Events", "detections", "count"),
    "mean_confidence": HistoricalMetric("AVG(confidence)", "Mean Confidence", "probability", "avg"),
}
HISTORICAL_SOURCES: dict[str, HistoricalSourceSpec] = {
    "risk": HistoricalSourceSpec(
        "risk_assessments", "period_end_epoch_ms",
        {
            "conservation_risk_index": HistoricalMetric("AVG(conservation_risk_index)", "Conservation Risk Index", "CRI", "avg"),
            "intervention_priority_index": HistoricalMetric("AVG(intervention_priority_index)", "Intervention Priority Index", "IPI", "avg"),
            "inbreeding_penalty_index": HistoricalMetric("AVG(inbreeding_penalty_index)", "Inbreeding Penalty Index", "IPI", "avg"),
            "risk_momentum_per_day": HistoricalMetric("AVG(risk_momentum_per_day)", "Risk Momentum", "CRI/day", "avg"),
            **{
                name: HistoricalMetric(f"AVG({name})", measurementLabel(name), "score", "avg")
                for name in RISK_SUBSCORES
            },
        },
        ("conservation_risk_index", *RISK_SUBSCORES),
    ),
    "environmental": HistoricalSourceSpec(
        "sensor_readings", "recorded_epoch_ms",
        {
            "reading_count": HistoricalMetric("COUNT(*)", "Readings", "readings", "count"),
            **{
                name: HistoricalMetric(f"AVG({name})", measurementLabel(name), measurementUnit(name), "avg")
                for name in MEASUREMENT_COLUMNS
            },
        },
        SUMMARY_METRICS or MEASUREMENT_COLUMNS,
    ),
    "gps": HistoricalSourceSpec(
        "telemetry", "recorded_epoch_ms",
        {
            "fix_count": HistoricalMetric("COUNT(*)", "GPS Fixes", "fixes", "count"),
            "active_animals": HistoricalMetric("COUNT(DISTINCT animal_id)", "Active Animals", "animals", "count"),
            "speed_kmh": HistoricalMetric("AVG(speed_kmh)", "Average Speed", "km/h", "avg"),
            "heart_rate_bpm": HistoricalMetric(
                f"AVG({STATS_METRICS['gps']['heart_rate_bpm']})", "Heart Rate", "bpm", "avg"
            ),
        },
        ("fix_count", "active_animals", "speed_kmh"),
    ),
    "acoustic": HistoricalSourceSpec(
        "detections", "detected_epoch_ms", DETECTION_HISTORICAL_METRICS, tuple(DETECTION_HISTORICAL_METRICS)
    ),
    "vision": HistoricalSourceSpec(
        "detections", "detected_epoch_ms", DETECTION_HISTORICAL_METRICS, tuple(DETECTION_HISTORICAL_METRICS)
    ),
}


def bucketStart(epochMs: int, bucket: str) -> int:
    bucketMs = BUCKET_SIZES_MS[bucket]
    if bucket == "week":
        return (epochMs - WEEK_OFFSET_MS) // bucketMs * bucketMs + WEEK_OFFSET_MS
    return epochMs // bucketMs * bucketMs


def countBuckets(startMs: int, endMs: int, bucket: str) -> int:
    return (bucketStart(endMs - 1, bucket) - bucketStart(startMs, bucket)) // BUCKET_SIZES_MS[bucket] + 1


def chooseBucket(startMs: int, endMs: int, requested: str) -> str:
    if requested != "auto":
        if countBuckets(startMs, endMs, requested) > HISTORICAL_MAX_POINTS:
            raise HTTPException(
                status_code=422,
                detail=f"Window spans more than {HISTORICAL_MAX_POINTS} {requested} buckets; use a larger bucket or shorter window.",
            )
        return requested
    spanDays = (endMs - startMs) / DAY_MS
    bucket = "hour" if spanDays <= HOURLY_MAX_DAYS else "day" if spanDays <= DAILY_MAX_DAYS else "week"
    # Step up to coarser buckets until the chart fits within the point budget
    for candidate in BUCKET_ORDER[BUCKET_ORDER.index(bucket):]:
        if countBuckets(startMs, endMs, candidate) <= HISTORICAL_MAX_POINTS:
            return candidate
    raise HTTPException(status_code=422, detail=f"Window is too long to fit {HISTORICAL_MAX_POINTS} weekly points.")


def imputeSeries(
    values: list[Optional[float]], method: str, maxGapBuckets: int
) -> tuple[list[Optional[float]], list[bool]]:
    filled = list(values)
    imputed = [False] * len(values)
    if method == "none":
        return filled, imputed
    index = 0
    while index < len(values):
        if values[index] is not None:
            index += 1
            continue
        gapStart = index
        while index < len(values) and values[index] is None:
            index += 1
        gapEnd = index
        # Leading gaps have nothing to carry and long outages stay as visible breaks
        if gapStart == 0 or gapEnd - gapStart > maxGapBuckets:
            continue
        leftValue = values[gapStart - 1]
        rightValue = values[gapEnd] if gapEnd < len(values) else None
        assert leftValue is not None
        for position in range(gapStart, gapEnd):
            if method == "linear" and rightValue is not None:
                fraction = (position - gapStart + 1) / (gapEnd - gapStart + 1)
                filled[position] = round(leftValue + (rightValue - leftValue) * fraction, 4)
            else:
                # Trailing gaps have no right neighbour, so linear falls back to carrying the last value
                filled[position] = leftValue
            imputed[position] = True
    return filled, imputed


async def computeHistorical(
    databaseManager: Any,
    source: str,
    metricNames: tuple[str, ...],
    bucket: str,
    imputation: str,
    startMs: int,
    endMs: int,
    speciesID: Optional[int],
    h3Cell: Optional[str],
    sensorID: Optional[str],
) -> dict[str, Any]:
    sourceSpec = HISTORICAL_SOURCES[source]
    bucketMs = BUCKET_SIZES_MS[bucket]
    conditions = [f"{sourceSpec.epochColumn} >= ?", f"{sourceSpec.epochColumn} < ?"]
    parameters: list[object] = [startMs, endMs]
    if source in TABLE_SPECS:
        conditions, parameters = buildFilters(TABLE_SPECS[source], startMs, endMs, sensorID, speciesID, h3Cell, None, None)
        if source == "environmental":
            conditions.append("quality_flag != 'INVALID'")
    else:
        if sensorID is not None:
            raise HTTPException(status_code=422, detail="sensor_id filtering is not available for risk assessments.")
        if speciesID is not None:
            conditions.append("species_id = ?")
            parameters.append(speciesID)
        if h3Cell is not None:
            conditions.append("h3_cell = ?")
            parameters.append(h3Cell.strip().lower())

    # Integer division buckets the indexed epoch column, the SQLite equivalent of date_trunc
    if bucket == "week":
        bucketExpression = f"(({sourceSpec.epochColumn} - ?) / ?) * ? + ?"
        bucketParameters: list[object] = [WEEK_OFFSET_MS, bucketMs, bucketMs, WEEK_OFFSET_MS]
    else:
        bucketExpression = f"({sourceSpec.epochColumn} / ?) * ?"
        bucketParameters = [bucketMs, bucketMs]
    selectParts = [f"{bucketExpression} AS bucket_epoch_ms", "COUNT(*) AS sample_count"]
    selectParts.extend(
        f"{sourceSpec.metrics[name].expression} AS m{index}" for index, name in enumerate(metricNames)
    )
    rows = await databaseManager.fetchAll(
        f"SELECT {', '.join(selectParts)} FROM {sourceSpec.tableName} WHERE {' AND '.join(conditions)} "
        "GROUP BY bucket_epoch_ms ORDER BY bucket_epoch_ms;",
        [*bucketParameters, *parameters],
    )
    rowsByBucket = {int(row["bucket_epoch_ms"]): row for row in rows}

    # Dense bucket axis so the chart shows offline periods instead of silently joining distant points
    firstBucketMs = bucketStart(startMs, bucket)
    bucketEpochs = [firstBucketMs + step * bucketMs for step in range(countBuckets(startMs, endMs, bucket))]
    datasets = []
    for index, name in enumerate(metricNames):
        metric = sourceSpec.metrics[name]
        rawValues: list[Optional[float]] = []
        for epochMs in bucketEpochs:
            value = rowsByBucket[epochMs][f"m{index}"] if epochMs in rowsByBucket else None
            if metric.aggregate == "count":
                rawValues.append(float(value or 0))
            else:
                rawValues.append(roundOrNone(value, 4))
        if metric.aggregate == "avg":
            data, imputedMask = imputeSeries(rawValues, imputation, IMPUTATION_MAX_GAP_BUCKETS)
        else:
            data, imputedMask = rawValues, [False] * len(rawValues)
        datasets.append({
            "key": name,
            "label": metric.label,
            "unit": metric.unit,
            "aggregate": metric.aggregate,
            "data": data,
            "imputed": imputedMask,
            "imputed_count": sum(imputedMask),
            "missing_count": sum(1 for value in data if value is None),
        })
    return {
        "source": source,
        "bucket": bucket,
        "bucket_ms": bucketMs,
        "imputation": imputation,
        "max_gap_buckets": IMPUTATION_MAX_GAP_BUCKETS,
        "start_time": fromEpochMs(startMs),
        "end_time": fromEpochMs(endMs),
        "labels": [fromEpochMs(epochMs) for epochMs in bucketEpochs],
        "datasets": datasets,
        "sample_counts": [int(rowsByBucket[epochMs]["sample_count"]) if epochMs in rowsByBucket else 0 for epochMs in bucketEpochs],
        "generated_at": formatUtcTimestamp(datetime.now(timezone.utc)),
    }


@router.get("/historical")
async def getAnalyticsHistorical(
    request: Request,
    source: HistoricalSource = "risk",
    metric: Optional[list[str]] = Query(None),
    bucket: HistoricalBucket = "auto",
    imputation: Optional[ImputationMethod] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    species_id: Optional[int] = Query(None, gt=0),
    h3_cell: Optional[str] = None,
    sensor_id: Optional[str] = None,
) -> dict[str, Any]:
    databaseManager = getDatabase(request)
    sourceSpec = HISTORICAL_SOURCES[source]
    metricNames = tuple(dict.fromkeys(metric)) if metric else sourceSpec.defaultMetrics
    unknownMetrics = [name for name in metricNames if name not in sourceSpec.metrics]
    if unknownMetrics:
        raise HTTPException(status_code=422, detail=f"Unknown {source} metrics {unknownMetrics}; allowed: {list(sourceSpec.metrics)}.")
    startMs, endMs = resolveWindow(start_time, end_time, int(HISTORICAL_DEFAULT_LOOKBACK_DAYS * DAY_MS))
    resolvedBucket = chooseBucket(startMs, endMs, bucket)
    imputationMethod = imputation or IMPUTATION_METHOD
    cacheKey = ("historical", source, metricNames, resolvedBucket, imputationMethod, startMs, endMs, species_id, h3_cell, sensor_id)
    result, isCached = await getAnalyticsCache(request).getOrCompute(
        cacheKey,
        lambda: computeHistorical(databaseManager, source, metricNames, resolvedBucket, imputationMethod,
                                  startMs, endMs, species_id, h3_cell, sensor_id),
    )
    return {**result, "cached": isCached}


# 3. Radar chart datasets

@dataclass
class ZoneAccumulator:
    readingCount: int = 0
    temperatureCount: int = 0
    temperatureSum: float = 0.0
    temperatureSquareSum: float = 0.0
    detectionCount: int = 0
    wildlifeSightings: int = 0
    acousticThreats: int = 0
    fixCount: int = 0
    riskSum: float = 0.0
    riskCount: int = 0
    animalIDs: set[str] = field(default_factory=set)

    @property
    def activity(self) -> int:
        return self.readingCount + self.detectionCount + self.fixCount + self.riskCount

    def axisValues(self) -> dict[str, Optional[float]]:
        temperatureVariability: Optional[float] = None
        if self.temperatureCount >= 2:
            # Population standard deviation from running sums; the Pi's SQLite build may lack SQRT
            mean = self.temperatureSum / self.temperatureCount
            variance = max(self.temperatureSquareSum / self.temperatureCount - mean * mean, 0.0)
            temperatureVariability = math.sqrt(variance)
        return {
            "wildlife_sightings": float(self.wildlifeSightings),
            "animal_presence": float(len(self.animalIDs)),
            "temperature_variability": temperatureVariability,
            "acoustic_threats": float(self.acousticThreats),
            "conservation_risk": self.riskSum / self.riskCount if self.riskCount else None,
        }


def parseZones(zones: Optional[list[str]], resolution: Optional[int]) -> tuple[Optional[list[str]], int]:
    if not zones:
        return None, RADAR_ZONE_RESOLUTION if resolution is None else resolution
    cleanedZones = list(dict.fromkeys(zone.strip().lower() for zone in zones))
    if len(cleanedZones) > RADAR_MAX_ZONES:
        raise HTTPException(status_code=422, detail=f"At most {RADAR_MAX_ZONES} zones can be compared.")
    invalidZones = [zone for zone in cleanedZones if not h3.is_valid_cell(zone)]
    if invalidZones:
        raise HTTPException(status_code=422, detail=f"Invalid H3 zone cells: {invalidZones}.")
    resolutions = {h3.get_resolution(zone) for zone in cleanedZones}
    if len(resolutions) != 1:
        raise HTTPException(status_code=422, detail="All zones must share one H3 resolution.")
    zoneResolution = resolutions.pop()
    if zoneResolution > H3_RESOLUTION:
        raise HTTPException(status_code=422, detail=f"Zone resolution must not exceed {H3_RESOLUTION}, the sensor grid resolution.")
    if resolution is not None and resolution != zoneResolution:
        raise HTTPException(status_code=422, detail="resolution does not match the resolution of the supplied zones.")
    return cleanedZones, zoneResolution


def toZone(h3Cell: str, resolution: int) -> Optional[str]:
    try:
        cellResolution = h3.get_resolution(h3Cell)
    except (ValueError, TypeError, h3.H3BaseException):
        return None
    if cellResolution < resolution:
        return None
    return h3Cell if cellResolution == resolution else h3.cell_to_parent(h3Cell, resolution)


def normalizeAxis(values: list[Optional[float]]) -> tuple[list[Optional[float]], Optional[float], Optional[float]]:
    knownValues = [value for value in values if value is not None]
    if not knownValues:
        return [None] * len(values), None, None
    minimum, maximum = min(knownValues), max(knownValues)
    if maximum == minimum:
        # Identical zones sit on the midpoint rather than all reading best or worst
        return [0.5 if value is not None else None for value in values], minimum, maximum
    return [
        (value - minimum) / (maximum - minimum) if value is not None else None for value in values
    ], minimum, maximum


def computeHealthScore(normalizedValues: dict[str, Optional[float]]) -> Optional[float]:
    # Rescales sum(w * n) between its worst and best possible values, renormalising over axes with data
    achieved = 0.0
    possible = 0.0
    for axisName, weight in RADAR_AXIS_WEIGHTS.items():
        value = normalizedValues.get(axisName)
        if value is None or weight == 0:
            continue
        achieved += abs(weight) * (value if weight > 0 else 1.0 - value)
        possible += abs(weight)
    return round(achieved / possible * 100.0, 2) if possible > 0 else None


async def computeRadar(
    databaseManager: Any,
    zones: Optional[list[str]],
    resolution: int,
    zoneLimit: int,
    startMs: int,
    endMs: int,
    speciesID: Optional[int],
) -> dict[str, Any]:
    speciesCondition = " AND species_id = ?" if speciesID is not None else ""
    speciesParameters: list[object] = [speciesID] if speciesID is not None else []
    # SQL aggregates to a few rows per cell or location; Python maps them to parent zones
    environmentalRows, detectionRows, gpsRows, riskRows = await asyncio.gather(
        databaseManager.fetchAll(
            "SELECT h3_cell, COUNT(*) AS reading_count, COUNT(ambient_temperature_c) AS temperature_count, "
            "SUM(ambient_temperature_c) AS temperature_sum, "
            "SUM(ambient_temperature_c * ambient_temperature_c) AS temperature_square_sum "
            "FROM sensor_readings WHERE recorded_epoch_ms >= ? AND recorded_epoch_ms < ? "
            "AND quality_flag != 'INVALID' GROUP BY h3_cell;",
            [startMs, endMs],
        ),
        databaseManager.fetchAll(
            "SELECT round(latitude, 4) AS latitude, round(longitude, 4) AS longitude, COUNT(*) AS detection_count, "
            "SUM(category = 'target_wildlife') AS wildlife_sightings, "
            "SUM(source_type = 'ACOUSTIC' AND category = 'immediate_threat') AS acoustic_threats "
            "FROM detections WHERE detected_epoch_ms >= ? AND detected_epoch_ms < ? "
            f"AND latitude IS NOT NULL AND longitude IS NOT NULL{speciesCondition} "
            "GROUP BY 1, 2;",
            [startMs, endMs, *speciesParameters],
        ),
        databaseManager.fetchAll(
            "SELECT animal_id, round(latitude, 4) AS latitude, round(longitude, 4) AS longitude, COUNT(*) AS fix_count "
            f"FROM telemetry WHERE recorded_epoch_ms >= ? AND recorded_epoch_ms < ?{speciesCondition} "
            "GROUP BY 1, 2, 3;",
            [startMs, endMs, *speciesParameters],
        ),
        databaseManager.fetchAll(
            "SELECT h3_cell, conservation_risk_index FROM ("
            "SELECT h3_cell, conservation_risk_index, "
            "ROW_NUMBER() OVER (PARTITION BY species_id, h3_cell ORDER BY period_end_epoch_ms DESC) AS latest_rank "
            f"FROM risk_assessments WHERE period_end_epoch_ms >= ? AND period_end_epoch_ms < ?{speciesCondition}"
            ") WHERE latest_rank = 1;",
            [startMs, endMs, *speciesParameters],
        ),
    )

    accumulators: dict[str, ZoneAccumulator] = {}

    def accumulatorFor(zone: Optional[str]) -> Optional[ZoneAccumulator]:
        if zone is None or (zones is not None and zone not in zones):
            return None
        return accumulators.setdefault(zone, ZoneAccumulator())

    for row in environmentalRows:
        accumulator = accumulatorFor(toZone(row["h3_cell"], resolution))
        if accumulator is not None:
            accumulator.readingCount += int(row["reading_count"])
            accumulator.temperatureCount += int(row["temperature_count"] or 0)
            accumulator.temperatureSum += float(row["temperature_sum"] or 0.0)
            accumulator.temperatureSquareSum += float(row["temperature_square_sum"] or 0.0)
    for row in detectionRows:
        accumulator = accumulatorFor(h3.latlng_to_cell(row["latitude"], row["longitude"], resolution))
        if accumulator is not None:
            accumulator.detectionCount += int(row["detection_count"])
            accumulator.wildlifeSightings += int(row["wildlife_sightings"] or 0)
            accumulator.acousticThreats += int(row["acoustic_threats"] or 0)
    for row in gpsRows:
        accumulator = accumulatorFor(h3.latlng_to_cell(row["latitude"], row["longitude"], resolution))
        if accumulator is not None:
            accumulator.fixCount += int(row["fix_count"])
            accumulator.animalIDs.add(str(row["animal_id"]))
    for row in riskRows:
        accumulator = accumulatorFor(toZone(row["h3_cell"], resolution))
        if accumulator is not None:
            accumulator.riskSum += float(row["conservation_risk_index"])
            accumulator.riskCount += 1

    if zones is not None:
        # Requested zones are always returned, even when they recorded nothing in the window
        selectedZones = zones
        for zone in zones:
            accumulators.setdefault(zone, ZoneAccumulator())
    else:
        # Auto-selection compares the most active sectors; ties break on the cell ID for stable output
        selectedZones = sorted(accumulators, key=lambda zone: (-accumulators[zone].activity, zone))[:zoneLimit]

    rawByZone = {zone: accumulators[zone].axisValues() for zone in selectedZones}
    axisNames = list(RADAR_AXIS_WEIGHTS)
    normalizedByZone: dict[str, dict[str, Optional[float]]] = {zone: {} for zone in selectedZones}
    axes = []
    for axisName in axisNames:
        normalizedValues, minimum, maximum = normalizeAxis([rawByZone[zone][axisName] for zone in selectedZones])
        for zone, value in zip(selectedZones, normalizedValues):
            normalizedByZone[zone][axisName] = value
        label, unit = RADAR_AXES[axisName]
        weight = RADAR_AXIS_WEIGHTS[axisName]
        axes.append({
            "key": axisName,
            "label": label,
            "unit": unit,
            "weight": weight,
            "higher_is": "better" if weight > 0 else "worse",
            "min": roundOrNone(minimum),
            "max": roundOrNone(maximum),
        })

    zoneEntries = []
    datasets = []
    for zone in selectedZones:
        centerLat, centerLon = h3.cell_to_latlng(zone)
        healthScore = computeHealthScore(normalizedByZone[zone])
        zoneEntries.append({
            "zone_id": zone,
            "center": {"latitude": round(centerLat, 6), "longitude": round(centerLon, 6)},
            "activity": accumulators[zone].activity,
            "raw": {axisName: roundOrNone(rawByZone[zone][axisName]) for axisName in axisNames},
            "normalized": {axisName: roundOrNone(normalizedByZone[zone][axisName], 4) for axisName in axisNames},
            "health_score": healthScore,
        })
        datasets.append({
            "label": zone,
            "zone_id": zone,
            # Chart values are scaled 0-100 so they share one axis with the health score
            "data": [
                round(normalizedByZone[zone][axisName] * 100.0, 2) if normalizedByZone[zone][axisName] is not None else None
                for axisName in axisNames
            ],
        })
    return {
        "zone_resolution": resolution,
        "start_time": fromEpochMs(startMs),
        "end_time": fromEpochMs(endMs),
        "chart_type_hint": "grouped_bar",
        "scale": {"min": 0, "max": 100},
        "labels": [RADAR_AXES[axisName][0] for axisName in axisNames],
        "axes": axes,
        "datasets": datasets,
        "zones": zoneEntries,
        "health_scores": {
            "labels": list(selectedZones),
            "data": [entry["health_score"] for entry in zoneEntries],
        },
        "generated_at": formatUtcTimestamp(datetime.now(timezone.utc)),
    }


@router.get("/radar")
async def getAnalyticsRadar(
    request: Request,
    zone: Optional[list[str]] = Query(None),
    resolution: Optional[int] = Query(None, ge=0, le=H3_RESOLUTION),
    limit: int = Query(RADAR_DEFAULT_ZONES, ge=1, le=RADAR_MAX_ZONES),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    species_id: Optional[int] = Query(None, gt=0),
) -> dict[str, Any]:
    databaseManager = getDatabase(request)
    zones, zoneResolution = parseZones(zone, resolution)
    startMs, endMs = resolveWindow(start_time, end_time, int(RADAR_LOOKBACK_DAYS * DAY_MS))
    cacheKey = ("radar", tuple(zones) if zones else None, zoneResolution, limit, startMs, endMs, species_id)
    result, isCached = await getAnalyticsCache(request).getOrCompute(
        cacheKey,
        lambda: computeRadar(databaseManager, zones, zoneResolution, limit, startMs, endMs, species_id),
    )
    return {**result, "cached": isCached}


# 4. Spatial cluster maps

@dataclass(frozen=True)
class Viewport:
    north: float
    south: float
    east: float
    west: float

    @property
    def crossesAntimeridian(self) -> bool:
        return self.east < self.west

    @property
    def lonSpan(self) -> float:
        return self.east - self.west if not self.crossesAntimeridian else self.east + 360.0 - self.west

    def unwrapLongitude(self, offset: float) -> float:
        # Grid maths works on longitude east of the west edge; convert back into [-180, 180]
        longitude = self.west + offset
        return longitude - 360.0 if longitude > 180.0 else longitude


SPATIAL_LAYER_SPATIAL_INDEX: dict[str, Optional[str]] = {
    "gps": "telemetry", "acoustic": "detections", "vision": "detections", "environmental": None,
}


def buildViewportFilter(viewport: Viewport, spatialIndexTable: Optional[str]) -> tuple[list[str], list[object]]:
    conditions = ["latitude IS NOT NULL", "longitude IS NOT NULL", "latitude BETWEEN ? AND ?"]
    parameters: list[object] = [viewport.south, viewport.north]
    if viewport.crossesAntimeridian:
        conditions.append("(longitude >= ? OR longitude <= ?)")
        parameters.extend([viewport.west, viewport.east])
        frames = [(viewport.west, 180.0), (-180.0, viewport.east)]
    else:
        conditions.append("longitude BETWEEN ? AND ?")
        parameters.extend([viewport.west, viewport.east])
        frames = [(viewport.west, viewport.east)]
    if spatialIndexTable is not None:
        # The SpatiaLite R-Tree culls the viewport first; the exact BETWEEN checks absorb its float32 rounding
        frameConditions = []
        for west, east in frames:
            frameConditions.append(
                "id IN (SELECT ROWID FROM SpatialIndex WHERE f_table_name = ? AND f_geometry_column = 'geom' "
                "AND search_frame = BuildMbr(?, ?, ?, ?, 4326))"
            )
            parameters.extend([spatialIndexTable, west, viewport.south, east, viewport.north])
        conditions.append(f"({' OR '.join(frameConditions)})")
    return conditions, parameters


def runWeightedKMeans(
    points: np.ndarray, weights: np.ndarray, clusterCount: int, iterations: int
) -> np.ndarray:
    # Deterministic k-means++ seeding: heaviest cell first, then the cell with the largest weighted squared distance
    centroids = [points[int(np.argmax(weights))]]
    for _ in range(1, clusterCount):
        distances = np.min(((points[:, None, :] - np.array(centroids)[None, :, :]) ** 2).sum(axis=2), axis=1)
        centroids.append(points[int(np.argmax(distances * weights))])
    centroidArray = np.array(centroids, dtype=float)
    labels = np.zeros(len(points), dtype=int)
    for _ in range(iterations):
        distances = ((points[:, None, :] - centroidArray[None, :, :]) ** 2).sum(axis=2)
        labels = np.argmin(distances, axis=1)
        updated = centroidArray.copy()
        for clusterIndex in range(clusterCount):
            members = labels == clusterIndex
            if members.any():
                updated[clusterIndex] = np.average(points[members], axis=0, weights=weights[members])
        if np.allclose(updated, centroidArray):
            break
        centroidArray = updated
    return labels


async def computeSpatial(
    databaseManager: Any,
    viewport: Viewport,
    layer: str,
    method: str,
    gridDivisions: int,
    clusterCount: Optional[int],
    metric: Optional[str],
    category: Optional[str],
    speciesID: Optional[int],
    startMs: int,
    endMs: int,
) -> dict[str, Any]:
    spec = TABLE_SPECS[layer]
    conditions, parameters = buildFilters(spec, startMs, endMs, None, speciesID, None, category, None)
    if layer == "environmental":
        conditions.append("quality_flag != 'INVALID'")
    viewportConditions, viewportParameters = buildViewportFilter(viewport, SPATIAL_LAYER_SPATIAL_INDEX[layer])
    conditions.extend(viewportConditions)
    parameters.extend(viewportParameters)

    latCell = (viewport.north - viewport.south) / gridDivisions
    lonCell = viewport.lonSpan / gridDivisions
    if viewport.crossesAntimeridian:
        offsetExpression = "(CASE WHEN longitude >= ? THEN longitude - ? ELSE longitude + 360.0 - ? END)"
        offsetParameters: list[object] = [viewport.west, viewport.west, viewport.west]
    else:
        offsetExpression = "(longitude - ?)"
        offsetParameters = [viewport.west]
    # Snap-to-grid clustering in SQL: only one row per occupied cell leaves the database
    selectParts = [
        f"MIN(CAST((latitude - ?) / ? AS INTEGER), {gridDivisions - 1}) AS cell_y",
        f"MIN(CAST({offsetExpression} / ? AS INTEGER), {gridDivisions - 1}) AS cell_x",
        "COUNT(*) AS point_count",
        "AVG(latitude) AS latitude",
        f"AVG({offsetExpression}) AS lon_offset",
        "MIN(latitude) AS min_latitude",
        "MAX(latitude) AS max_latitude",
        f"MIN({offsetExpression}) AS min_lon_offset",
        f"MAX({offsetExpression}) AS max_lon_offset",
    ]
    selectParameters: list[object] = [
        viewport.south, latCell, *offsetParameters, lonCell,
        *offsetParameters, *offsetParameters, *offsetParameters,
    ]
    if spec.sourceType is not None:
        selectParts.append("SUM(category = 'immediate_threat') AS threat_count")
    if layer == "gps":
        selectParts.append("COUNT(DISTINCT animal_id) AS animal_count")
    if metric is not None:
        expression = STATS_METRICS[layer][metric]
        selectParts.extend([f"SUM({expression}) AS metric_sum", f"COUNT({expression}) AS metric_count"])
    rows = await databaseManager.fetchAll(
        f"SELECT {', '.join(selectParts)} FROM {spec.tableName} WHERE {' AND '.join(conditions)} "
        "GROUP BY cell_y, cell_x;",
        [*selectParameters, *parameters],
    )

    clusters: list[dict[str, Any]] = []
    if method == "kmeans" and rows:
        # Weighted k-means over grid centroids merges neighbouring cells into organic clusters
        midLatitude = math.radians((viewport.north + viewport.south) / 2.0)
        points = np.array([[row["lon_offset"] * math.cos(midLatitude), row["latitude"]] for row in rows], dtype=float)
        weights = np.array([float(row["point_count"]) for row in rows], dtype=float)
        targetClusters = clusterCount or max(1, round(math.sqrt(len(rows))))
        targetClusters = min(targetClusters, len(rows), SPATIAL_MAX_KMEANS_CLUSTERS)
        labels = runWeightedKMeans(points, weights, targetClusters, SPATIAL_KMEANS_ITERATIONS)
        for clusterIndex in range(targetClusters):
            members = [row for row, label in zip(rows, labels) if label == clusterIndex]
            if not members:
                continue
            pointCount = sum(int(row["point_count"]) for row in members)
            cluster: dict[str, Any] = {
                "point_count": pointCount,
                "latitude": sum(row["latitude"] * row["point_count"] for row in members) / pointCount,
                "lon_offset": sum(row["lon_offset"] * row["point_count"] for row in members) / pointCount,
                "min_latitude": min(row["min_latitude"] for row in members),
                "max_latitude": max(row["max_latitude"] for row in members),
                "min_lon_offset": min(row["min_lon_offset"] for row in members),
                "max_lon_offset": max(row["max_lon_offset"] for row in members),
                "grid_cells": len(members),
            }
            if spec.sourceType is not None:
                cluster["threat_count"] = sum(int(row["threat_count"] or 0) for row in members)
            if metric is not None:
                cluster["metric_sum"] = sum(float(row["metric_sum"] or 0.0) for row in members)
                cluster["metric_count"] = sum(int(row["metric_count"] or 0) for row in members)
            clusters.append(cluster)
    else:
        clusters = [{**row, "grid_cells": 1} for row in rows]

    maxCount = max((int(cluster["point_count"]) for cluster in clusters), default=0)
    features = []
    for clusterID, cluster in enumerate(sorted(clusters, key=lambda item: -int(item["point_count"]))):
        pointCount = int(cluster["point_count"])
        properties: dict[str, Any] = {
            "cluster_id": clusterID,
            "cluster": pointCount > 1,
            "point_count": pointCount,
            "weight": round(pointCount / maxCount, 4) if maxCount else 0.0,
            "grid_cells": cluster["grid_cells"],
            "bbox": [
                round(viewport.unwrapLongitude(cluster["min_lon_offset"]), 6),
                round(cluster["min_latitude"], 6),
                round(viewport.unwrapLongitude(cluster["max_lon_offset"]), 6),
                round(cluster["max_latitude"], 6),
            ],
        }
        if spec.sourceType is not None:
            properties["threat_count"] = int(cluster.get("threat_count") or 0)
        if "animal_count" in cluster:
            # Distinct animals cannot be summed across merged cells, so the count is only reported per grid cell
            properties["animal_count"] = int(cluster["animal_count"])
        if metric is not None:
            metricCount = int(cluster.get("metric_count") or 0)
            properties["metric"] = metric
            properties["metric_avg"] = round(float(cluster["metric_sum"]) / metricCount, 4) if metricCount else None
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                # GeoJSON positions are [longitude, latitude]
                "coordinates": [round(viewport.unwrapLongitude(cluster["lon_offset"]), 6), round(cluster["latitude"], 6)],
            },
            "properties": properties,
        })
    return {
        "type": "FeatureCollection",
        "bbox": [viewport.west, viewport.south, viewport.east, viewport.north],
        "features": features,
        "metadata": {
            "layer": layer,
            "method": method,
            "grid_divisions": gridDivisions,
            "total_points": sum(int(cluster["point_count"]) for cluster in clusters),
            "cluster_count": len(features),
            "crosses_antimeridian": viewport.crossesAntimeridian,
            "start_time": fromEpochMs(startMs),
            "end_time": fromEpochMs(endMs),
            "generated_at": formatUtcTimestamp(datetime.now(timezone.utc)),
        },
    }


@router.get("/spatial")
async def getAnalyticsSpatial(
    request: Request,
    north: float = Query(..., ge=-90.0, le=90.0),
    south: float = Query(..., ge=-90.0, le=90.0),
    east: float = Query(..., ge=-180.0, le=180.0),
    west: float = Query(..., ge=-180.0, le=180.0),
    layer: SpatialLayer = "gps",
    method: SpatialMethod = "grid",
    grid_divisions: int = Query(SPATIAL_GRID_DIVISIONS, ge=2, le=SPATIAL_MAX_GRID_DIVISIONS),
    clusters: Optional[int] = Query(None, ge=1, le=SPATIAL_MAX_KMEANS_CLUSTERS),
    metric: Optional[str] = None,
    category: Optional[str] = None,
    species_id: Optional[int] = Query(None, gt=0),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> dict[str, Any]:
    databaseManager = getDatabase(request)
    if south >= north:
        raise HTTPException(status_code=422, detail="south must be less than north.")
    if east == west:
        raise HTTPException(status_code=422, detail="east and west must differ; a viewport cannot have zero width.")
    if metric is not None and metric not in STATS_METRICS[layer]:
        raise HTTPException(status_code=422, detail=f"Unknown {layer} metric {metric!r}; allowed: {list(STATS_METRICS[layer])}.")
    if clusters is not None and method != "kmeans":
        raise HTTPException(status_code=422, detail="clusters is only used with method=kmeans.")
    viewport = Viewport(north=north, south=south, east=east, west=west)
    startMs, endMs = resolveWindow(start_time, end_time, int(SPATIAL_LOOKBACK_DAYS * DAY_MS))
    cacheKey = ("spatial", north, south, east, west, layer, method, grid_divisions, clusters, metric, category,
                species_id, startMs, endMs)
    result, isCached = await getAnalyticsCache(request).getOrCompute(
        cacheKey,
        lambda: computeSpatial(databaseManager, viewport, layer, method, grid_divisions, clusters, metric,
                               category, species_id, startMs, endMs),
    )
    return {**result, "cached": isCached}


async def main() -> None:
    # Demo: run the full API lifespan in-process, ingest a few readings, then request every analytics view
    from api.main import createApp

    app = createApp()
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://icmis.local") as client:
            now = datetime.now(timezone.utc)
            payloads = [
                {
                    "reading_type": "environmental", "sensor_id": "river-probe-01", "sensor_mac": "AA:BB:CC:DD:EE:01",
                    "timestamp": (now - timedelta(hours=index)).isoformat(), "latitude": -1.30, "longitude": 36.80,
                    "measurements": {"ambient_temperature_c": 24.0 + index % 3, "relative_humidity_percent": 60.0 + index},
                }
                for index in range(6)
            ]
            payloads.append({
                "reading_type": "acoustic", "sensor_id": "acoustic-array-07", "timestamp": now.isoformat(),
                "latitude": -1.31, "longitude": 36.81, "class_name": "gunshot", "category": "immediate_threat",
                "confidence": 0.93,
            })
            response = await client.post("/api/v1/telemetry/ingest", json=payloads)
            print(f"Ingest: {response.status_code}")
            # Let the ingest worker and the 500 ms database batch flush
            await asyncio.sleep(1.5)
            response = await client.get("/api/v1/analytics/summary")
            for card in response.json()["cards"][:4]:
                print(f"Summary card {card['label']}: {card['value']} ({card['delta_text']})")
            response = await client.get("/api/v1/analytics/historical", params={"source": "environmental"})
            historical = response.json()
            print(f"Historical: {len(historical['labels'])} {historical['bucket']} bucket(s)")
            response = await client.get("/api/v1/analytics/radar")
            print(f"Radar health scores: {response.json()['health_scores']}")
            response = await client.get(
                "/api/v1/analytics/spatial",
                params={"north": -1.0, "south": -1.6, "east": 37.1, "west": 36.5, "layer": "acoustic"},
            )
            print(f"Spatial: {len(response.json()['features'])} feature(s)")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())