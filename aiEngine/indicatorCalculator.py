import asyncio
from datetime import datetime, timedelta, timezone
import math
import os
from pydantic import Field, NonNegativeInt, PositiveFloat
import sqlite3
import statistics
import sys
from typing import Any, Iterable, Literal, Optional
import yaml

# Allow "python aiEngine/indicatorCalculator.py" as well as "python -m aiEngine.indicatorCalculator" from the project root
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from aiEngine.stateSchema import (
    CRITICAL_NE_THRESHOLD,
    RISK_DOMAINS,
    AssessmentScope,
    CoordinateModel,
    Latitude,
    Longitude,
    MortalityEvent,
    NonEmptyText,
    NormalizedSubscores,
    RawIndicators,
    SocialGroupObservation,
    StateModel,
    StateValidationError,
    TelemetryFix,
    ThreatEvent,
    validatedNode,
)
from database.dbManager import THREAT_LOOKBACK_DAYS, getSearchFrames

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

INDICATOR_CONFIG: dict = config.get("aiEngine", {}).get("indicatorCalculator", {}) or {}
GENETIC_CONFIG: dict = config.get("edna", {}).get("geneticAnalysis", {})

# 0.0 is optimal baseline health and 100.0 is imminent collapse or maximum threat
RISK_SCALE_MIN: float = 0.0
RISK_SCALE_MAX: float = 100.0
# Precautionary score for a domain with no usable data, so blind spots never read as "healthy"
MISSING_DOMAIN_SCORE: float = float(INDICATOR_CONFIG.get("missingDomainScore", 50.0))

# Standardized scaling matrix: metric -> (value scored 0.0, value scored 100.0)
DEFAULT_SCALING_BOUNDS: dict[str, tuple[float, float]] = {
    "herdDeclineFraction": (0.0, 0.5),
    "viabilityShortfallFraction": (0.0, 1.0),
    "recruitmentDeficitFraction": (0.0, 1.0),
    "rangeLossFraction": (0.0, 0.6),
    "fragmentationCrossingRate": (0.0, 0.1),
    "degradationZScore": (0.0, 3.0),
    "threatPressure": (0.0, 3.0),
    "extremeWeatherDayFraction": (0.0, 0.3),
    "heterozygosityDeficitFraction": (0.0, 1.0),
    "corridorRetentionLoss": (0.0, 1.0),
    "elderAbsenceFraction": (0.0, 1.0),
    "heartRateDeviationFraction": (0.0, 0.5),
}
SCALING_BOUNDS: dict[str, tuple[float, float]] = {
    metric: (float(bounds[0]), float(bounds[1]))
    for metric, bounds in {**DEFAULT_SCALING_BOUNDS, **(INDICATOR_CONFIG.get("scalingBounds") or {})}.items()
}

# Population health settings
POPULATION_WEIGHTS: dict[str, float] = {
    "trend": 0.4, "viability": 0.3, "recruitment": 0.3, **(INDICATOR_CONFIG.get("populationWeights") or {})
}
HERD_MOVING_AVERAGE_DAYS: int = int(INDICATOR_CONFIG.get("herdMovingAverageDays", 7))
DEFAULT_JUVENILE_RECRUITMENT_RATIO: float = float(INDICATOR_CONFIG.get("defaultJuvenileRecruitmentRatio", 0.3))
BBOX_CLUSTER_MIN_SEPARATION: float = float(INDICATOR_CONFIG.get("bboxClusterMinSeparation", 1.8))
BBOX_CLUSTER_MIN_BOXES: int = int(INDICATOR_CONFIG.get("bboxClusterMinBoxes", 10))
MORTALITY_BASE_PENALTY: float = float(INDICATOR_CONFIG.get("mortalityBasePenalty", 5.0))
MORTALITY_CLUSTER_GROWTH: float = float(INDICATOR_CONFIG.get("mortalityClusterGrowth", 2.0))
MORTALITY_CLUSTER_RADIUS_M: float = float(INDICATOR_CONFIG.get("mortalityClusterRadiusMeters", 5000.0))
MORTALITY_CLUSTER_WINDOW_DAYS: float = float(INDICATOR_CONFIG.get("mortalityClusterWindowDays", 7.0))

# Habitat condition settings
HABITAT_WEIGHTS: dict[str, float] = {
    "rangeLoss": 0.4, "fragmentation": 0.3, "degradation": 0.3, **(INDICATOR_CONFIG.get("habitatWeights") or {})
}
MCP_PERCENT: float = float(INDICATOR_CONFIG.get("mcpPercent", 95.0))
MINIMUM_FIXES_PER_ANIMAL: int = int(INDICATOR_CONFIG.get("minimumFixesPerAnimal", 5))
# +1 means higher values are worse, -1 means lower values are worse, 0 means any deviation is worse
DEFAULT_DEGRADATION_DIRECTIONS: dict[str, int] = {
    "dissolved_oxygen_mg_l": -1,
    "water_turbidity_ntu": 1,
    "water_ph": 0,
    "water_salinity_psu": 0,
    "electrical_conductivity_us_cm": 1,
    "nitrate_mg_l": 1,
    "phosphate_mg_l": 1,
    "ammonia_mg_l": 1,
    "pollutant_concentration_level": 1,
    "air_quality_level": 1,
}
DEGRADATION_DIRECTIONS: dict[str, int] = {
    **DEFAULT_DEGRADATION_DIRECTIONS, **(INDICATOR_CONFIG.get("degradationDirections") or {})
}

# Threat pressure settings
THREAT_HALF_LIFE_HOURS: float = float(INDICATOR_CONFIG.get("threatHalfLifeHours", 48.0))
THREAT_WINDOW_DAYS: float = float(INDICATOR_CONFIG.get("threatWindowDays", THREAT_LOOKBACK_DAYS))
DEFAULT_THREAT_CLASS_WEIGHTS: dict[str, float] = {
    "gunshot": 1.0,
    "poaching": 1.0,
    "snare": 0.9,
    "chainsaw": 0.7,
    "vessel": 0.6,
    "engine": 0.5,
    "vehicle": 0.4,
}
THREAT_CLASS_WEIGHTS: dict[str, float] = {
    key.casefold(): float(weight)
    for key, weight in {**DEFAULT_THREAT_CLASS_WEIGHTS, **(INDICATOR_CONFIG.get("threatClassWeights") or {})}.items()
}
DEFAULT_THREAT_WEIGHT: float = float(INDICATOR_CONFIG.get("defaultThreatWeight", 0.5))
DEFAULT_ZONE_MULTIPLIERS: dict[str, float] = {
    "BREEDING": 3.0, "WATER_SOURCE": 2.5, "CORE_RANGE": 1.5, "CORRIDOR": 1.5
}
ZONE_MULTIPLIERS: dict[str, float] = {
    **DEFAULT_ZONE_MULTIPLIERS, **(INDICATOR_CONFIG.get("zoneMultipliers") or {})
}
PERIPHERY_MULTIPLIER: float = float(INDICATOR_CONFIG.get("peripheryMultiplier", 0.5))
ZONE_SEARCH_RADIUS_M: float = float(INDICATOR_CONFIG.get("zoneSearchRadiusMeters", 100000.0))

# Climate stress settings
DEFAULT_CLIMATE_DIRECTIONS: dict[str, int] = {
    "ambient_temperature_c": 1,
    "water_temperature_c": 1,
    "barometric_pressure_hpa": 0,
    "relative_humidity_percent": -1,
}
CLIMATE_DIRECTIONS: dict[str, int] = {
    **DEFAULT_CLIMATE_DIRECTIONS, **(INDICATOR_CONFIG.get("climateDirections") or {})
}
CLIMATE_ROLLING_DAYS: float = float(INDICATOR_CONFIG.get("climateRollingDays", 30.0))
CLIMATE_BASELINE_YEARS: int = int(INDICATOR_CONFIG.get("climateBaselineYears", 10))
MINIMUM_BASELINE_YEARS: int = int(INDICATOR_CONFIG.get("minimumBaselineYears", 3))
MINIMUM_BASELINE_DAYS: int = int(INDICATOR_CONFIG.get("minimumBaselineDays", 14))
CLIMATE_SIGMA_THRESHOLD: float = float(INDICATOR_CONFIG.get("climateSigmaThreshold", 2.0))
CLIMATE_SUBTHRESHOLD_MAX: float = float(INDICATOR_CONFIG.get("climateSubthresholdMax", 40.0))
CLIMATE_ESCALATION_RATE: float = float(INDICATOR_CONFIG.get("climateEscalationRate", 1.5))
EXTREME_WEATHER_WEIGHT: float = float(INDICATOR_CONFIG.get("extremeWeatherWeight", 0.25))

# Genetic resilience settings
GENETIC_WEIGHTS: dict[str, float] = {
    "effectivePopulation": 0.75, "heterozygosity": 0.25, **(INDICATOR_CONFIG.get("geneticWeights") or {})
}
VIABLE_NE_THRESHOLD: float = float(GENETIC_CONFIG.get("vulnerableNeThreshold", 500.0))
HETEROZYGOSITY_BASELINE: float = float(INDICATOR_CONFIG.get("heterozygosityBaseline", 0.5))

# Behavioral stability settings
BEHAVIOR_WEIGHTS: dict[str, float] = {
    "corridor": 0.4, "social": 0.4, "physiology": 0.2, **(INDICATOR_CONFIG.get("behaviorWeights") or {})
}
CORRIDOR_FIX_SHARE: float = float(INDICATOR_CONFIG.get("corridorFixShare", 0.5))
CORRIDOR_EXPECTED_RETENTION: float = float(INDICATOR_CONFIG.get("corridorExpectedRetention", 0.8))
ORPHAN_MINIMUM_DAYS: int = int(INDICATOR_CONFIG.get("orphanMinimumDays", 3))
ORPHAN_PENALTY_FLOOR: float = float(INDICATOR_CONFIG.get("orphanPenaltyFloor", 85.0))
ORPHAN_DAY_PENALTY: float = float(INDICATOR_CONFIG.get("orphanDayPenalty", 15.0))

SUBSCORE_WEIGHT_GROUPS: dict[str, dict[str, float]] = {
    "populationWeights": POPULATION_WEIGHTS,
    "habitatWeights": HABITAT_WEIGHTS,
    "geneticWeights": GENETIC_WEIGHTS,
    "behaviorWeights": BEHAVIOR_WEIGHTS,
}
for groupName, weights in SUBSCORE_WEIGHT_GROUPS.items():
    if any(not math.isfinite(float(weight)) or float(weight) < 0.0 for weight in weights.values()):
        raise ValueError(f"indicatorCalculator {groupName} must be finite and non-negative.")
    if math.fsum(float(weight) for weight in weights.values()) <= 0.0:
        raise ValueError(f"indicatorCalculator {groupName} must contain a positive weight.")
for metric, (optimalValue, collapseValue) in SCALING_BOUNDS.items():
    if not math.isfinite(optimalValue) or not math.isfinite(collapseValue) or optimalValue == collapseValue:
        raise ValueError(f"indicatorCalculator scalingBounds for {metric} must be two different finite values.")
if not RISK_SCALE_MIN <= MISSING_DOMAIN_SCORE <= RISK_SCALE_MAX:
    raise ValueError("indicatorCalculator missingDomainScore must be between 0 and 100.")
if HERD_MOVING_AVERAGE_DAYS < 1 or MINIMUM_FIXES_PER_ANIMAL < 3 or ORPHAN_MINIMUM_DAYS < 1:
    raise ValueError("indicatorCalculator day and fix counts must be positive (at least 3 fixes per animal).")
if not 50.0 <= MCP_PERCENT <= 100.0:
    raise ValueError("indicatorCalculator mcpPercent must be between 50 and 100.")
if THREAT_HALF_LIFE_HOURS <= 0.0 or THREAT_WINDOW_DAYS <= 0.0 or CLIMATE_ROLLING_DAYS <= 0.0:
    raise ValueError("indicatorCalculator half-life and window lengths must be positive.")
if CLIMATE_SIGMA_THRESHOLD <= 0.0 or not 0.0 <= CLIMATE_SUBTHRESHOLD_MAX <= RISK_SCALE_MAX:
    raise ValueError("indicatorCalculator climate thresholds are out of range.")
if CRITICAL_NE_THRESHOLD >= VIABLE_NE_THRESHOLD:
    raise ValueError("The critical Ne threshold must be lower than the viable Ne threshold.")
if not 0.0 < HETEROZYGOSITY_BASELINE <= 1.0 or not 0.0 < CORRIDOR_EXPECTED_RETENTION <= 1.0:
    raise ValueError("indicatorCalculator heterozygosity and corridor baselines must be in (0, 1].")
if any(direction not in (-1, 0, 1) for direction in (*DEGRADATION_DIRECTIONS.values(), *CLIMATE_DIRECTIONS.values())):
    raise ValueError("indicatorCalculator metric directions must be -1, 0 or 1.")

# Metric names are interpolated into SQL, so only known sensor_readings columns are allowed
SENSOR_READING_COLUMNS: frozenset[str] = frozenset({
    "water_ph", "dissolved_oxygen_mg_l", "water_turbidity_ntu", "water_temperature_c", "water_salinity_psu",
    "water_velocity_m_s", "electrical_conductivity_us_cm", "ambient_temperature_c", "relative_humidity_percent",
    "barometric_pressure_hpa", "nitrate_mg_l", "phosphate_mg_l", "ammonia_mg_l",
    "pollutant_concentration_level", "air_quality_level",
})
BASELINE_METRICS: tuple[str, ...] = tuple(
    sorted((set(DEGRADATION_DIRECTIONS) | set(CLIMATE_DIRECTIONS)) & SENSOR_READING_COLUMNS)
)
EARTH_RADIUS_M: float = 6371008.8
MILLISECONDS_PER_DAY: int = 86400000
DAYS_PER_YEAR: float = 365.2425

ZoneType = Literal["BREEDING", "WATER_SOURCE", "CORRIDOR", "CORE_RANGE"]


# Indicator context models (baselines that live in the database rather than the graph state)

class MetricBaseline(StateModel):
    mean: float
    # Spread of same-season window means, used to score a rolling mean
    std: PositiveFloat
    # Spread of individual daily means, used to flag single extreme-weather days
    daily_std: Optional[PositiveFloat] = None
    sample_count: NonNegativeInt = 0


class HabitatZone(CoordinateModel):
    zone_name: NonEmptyText
    zone_type: ZoneType
    latitude: Latitude
    longitude: Longitude
    radius_m: PositiveFloat
    risk_multiplier: Optional[PositiveFloat] = None


class IndicatorContext(StateModel):
    juvenile_recruitment_ratio: Optional[PositiveFloat] = None
    minimum_viable_population: Optional[PositiveFloat] = None
    critical_ne_threshold: PositiveFloat = CRITICAL_NE_THRESHOLD
    viable_ne_threshold: PositiveFloat = VIABLE_NE_THRESHOLD
    resting_heart_rate_min_bpm: Optional[PositiveFloat] = None
    resting_heart_rate_max_bpm: Optional[PositiveFloat] = None
    home_range_km2: Optional[PositiveFloat] = None
    baseline_range_km2: Optional[PositiveFloat] = None
    metric_baselines: dict[NonEmptyText, MetricBaseline] = Field(default_factory=dict)
    habitat_zones: tuple[HabitatZone, ...] = ()
    telemetry_segment_count: Optional[NonNegativeInt] = None
    infrastructure_crossings: Optional[NonNegativeInt] = None
    warnings: tuple[str, ...] = ()


# Scaling helpers

def clampRisk(value: float) -> float:
    return min(RISK_SCALE_MAX, max(RISK_SCALE_MIN, float(value)))


def scaleToRisk(metric: str, value: float) -> float:
    # Linear map from the metric's optimal value (0.0) to its collapse value (100.0)
    optimalValue, collapseValue = SCALING_BOUNDS[metric]
    return clampRisk(RISK_SCALE_MAX * (value - optimalValue) / (collapseValue - optimalValue))


def weightedScore(components: dict[str, Optional[float]], weights: dict[str, float]) -> Optional[float]:
    # Renormalise over the components that actually had data
    available = {name: score for name, score in components.items() if score is not None and weights.get(name, 0.0) > 0}
    totalWeight = math.fsum(weights[name] for name in available)
    if totalWeight <= 0.0:
        return None
    return clampRisk(math.fsum(weights[name] * score for name, score in available.items()) / totalWeight)


def adverseZScore(value: float, baseline: MetricBaseline, direction: int, useDailySpread: bool = False) -> float:
    spread = baseline.daily_std if useDailySpread and baseline.daily_std else baseline.std
    zScore = (value - baseline.mean) / spread
    if direction > 0:
        return max(0.0, zScore)
    if direction < 0:
        return max(0.0, -zScore)
    return abs(zScore)


# Geometry helpers

def haversineMeters(latitude1: float, longitude1: float, latitude2: float, longitude2: float) -> float:
    phi1, phi2 = math.radians(latitude1), math.radians(latitude2)
    deltaPhi = phi2 - phi1
    deltaLambda = math.radians(longitude2 - longitude1)
    a = math.sin(deltaPhi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(deltaLambda / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def projectLocal(
    points: Iterable[tuple[float, float]], originLatitude: float, originLongitude: float
) -> list[tuple[float, float]]:
    # Equirectangular projection in meters; accurate for home-range scale areas away from the poles
    cosLatitude = math.cos(math.radians(originLatitude))
    projected = []
    for latitude, longitude in points:
        deltaLongitude = (longitude - originLongitude + 540.0) % 360.0 - 180.0
        projected.append((
            math.radians(deltaLongitude) * EARTH_RADIUS_M * cosLatitude,
            math.radians(latitude - originLatitude) * EARTH_RADIUS_M,
        ))
    return projected


def convexHull(points: list[tuple[float, float]]) -> list[tuple[float, float]]:
    # Andrew's monotone chain
    uniquePoints = sorted(set(points))
    if len(uniquePoints) < 3:
        return uniquePoints

    def cross(origin: tuple[float, float], a: tuple[float, float], b: tuple[float, float]) -> float:
        return (a[0] - origin[0]) * (b[1] - origin[1]) - (a[1] - origin[1]) * (b[0] - origin[0])

    lower: list[tuple[float, float]] = []
    for point in uniquePoints:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], point) <= 0:
            lower.pop()
        lower.append(point)
    upper: list[tuple[float, float]] = []
    for point in reversed(uniquePoints):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], point) <= 0:
            upper.pop()
        upper.append(point)
    return lower[:-1] + upper[:-1]


def polygonArea(vertices: list[tuple[float, float]]) -> float:
    # Shoelace formula
    if len(vertices) < 3:
        return 0.0
    twiceArea = math.fsum(
        vertices[index][0] * vertices[(index + 1) % len(vertices)][1]
        - vertices[(index + 1) % len(vertices)][0] * vertices[index][1]
        for index in range(len(vertices))
    )
    return abs(twiceArea) / 2.0


def minimumConvexPolygonKm2(points: list[tuple[float, float]], percent: float = MCP_PERCENT) -> Optional[float]:
    if len(points) < 3:
        return None
    originLatitude = statistics.fmean(latitude for latitude, _ in points)
    originLongitude = math.degrees(math.atan2(
        math.fsum(math.sin(math.radians(longitude)) for _, longitude in points),
        math.fsum(math.cos(math.radians(longitude)) for _, longitude in points),
    ))
    projected = projectLocal(points, originLatitude, originLongitude)
    # Percent MCP drops the fixes farthest from the centroid to exclude exploratory outliers
    centroidX = statistics.fmean(x for x, _ in projected)
    centroidY = statistics.fmean(y for _, y in projected)
    projected.sort(key=lambda point: (point[0] - centroidX) ** 2 + (point[1] - centroidY) ** 2)
    keptCount = max(3, math.ceil(len(projected) * percent / 100.0))
    return polygonArea(convexHull(projected[:keptCount])) / 1_000_000.0


def meanAnimalRangeKm2(fixes: Iterable[tuple[str, float, float]]) -> Optional[float]:
    pointsByAnimal: dict[str, list[tuple[float, float]]] = {}
    for animalID, latitude, longitude in fixes:
        pointsByAnimal.setdefault(animalID, []).append((latitude, longitude))
    areas = [
        area
        for points in pointsByAnimal.values()
        if len(points) >= MINIMUM_FIXES_PER_ANIMAL
        for area in [minimumConvexPolygonKm2(points)]
        if area is not None and area > 0.0
    ]
    return statistics.fmean(areas) if areas else None


def estimateScopeCentre(
    scope: Optional[AssessmentScope], raw: RawIndicators
) -> Optional[tuple[float, float]]:
    if scope is not None and scope.latitude is not None and scope.longitude is not None:
        return scope.latitude, scope.longitude
    if raw.telemetry_fixes:
        latitude = statistics.fmean(fix.latitude for fix in raw.telemetry_fixes)
        longitude = math.degrees(math.atan2(
            math.fsum(math.sin(math.radians(fix.longitude)) for fix in raw.telemetry_fixes),
            math.fsum(math.cos(math.radians(fix.longitude)) for fix in raw.telemetry_fixes),
        ))
        return latitude, longitude
    return None


# Population Health Subscore (S_pop)

def movingAverage(values: tuple[float, ...], windowSize: int) -> list[float]:
    return [
        statistics.fmean(values[index:index + windowSize])
        for index in range(len(values) - windowSize + 1)
    ]


def classifyAgeByBoxArea(boxAreas: tuple[float, ...]) -> Optional[tuple[int, int]]:
    # Two-cluster 1D k-means on normalized box areas: the small cluster is treated as juveniles.
    # Camera distance also changes box size, so the split is only trusted when the clusters are well separated
    if len(boxAreas) < BBOX_CLUSTER_MIN_BOXES:
        return None
    ordered = sorted(boxAreas)
    smallCentre, largeCentre = ordered[len(ordered) // 4], ordered[(3 * len(ordered)) // 4]
    for _ in range(50):
        boundary = (smallCentre + largeCentre) / 2.0
        smallCluster = [area for area in ordered if area <= boundary]
        largeCluster = [area for area in ordered if area > boundary]
        if not smallCluster or not largeCluster:
            return None
        newSmall, newLarge = statistics.fmean(smallCluster), statistics.fmean(largeCluster)
        if math.isclose(newSmall, smallCentre) and math.isclose(newLarge, largeCentre):
            break
        smallCentre, largeCentre = newSmall, newLarge
    if smallCentre <= 0.0 or largeCentre / smallCentre < BBOX_CLUSTER_MIN_SEPARATION:
        return None
    return len(smallCluster), len(largeCluster)


def clusterMortalityEvents(events: tuple[MortalityEvent, ...]) -> list[int]:
    # Union-find linking deaths that fall within the cluster radius and time window of each other
    parents = list(range(len(events)))

    def findRoot(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    windowSeconds = MORTALITY_CLUSTER_WINDOW_DAYS * 86400.0
    for first in range(len(events)):
        for second in range(first + 1, len(events)):
            eventA, eventB = events[first], events[second]
            if eventA.latitude is None or eventB.latitude is None:
                continue
            if abs((eventA.recorded_at - eventB.recorded_at).total_seconds()) > windowSeconds:
                continue
            distance = haversineMeters(eventA.latitude, eventA.longitude, eventB.latitude, eventB.longitude)
            if distance <= MORTALITY_CLUSTER_RADIUS_M:
                parents[findRoot(first)] = findRoot(second)

    clusterSizes: dict[int, int] = {}
    for index in range(len(events)):
        root = findRoot(index)
        clusterSizes[root] = clusterSizes.get(root, 0) + 1
    return sorted(clusterSizes.values(), reverse=True)


def calculatePopulationSubscore(
    raw: RawIndicators, context: IndicatorContext
) -> tuple[Optional[float], dict[str, Optional[float]]]:
    components: dict[str, Optional[float]] = {"trend": None, "viability": None, "recruitment": None}

    # Moving averages smooth day-to-day camera trap detection variance before measuring decline
    series = raw.herd_count_series
    windowSize = min(HERD_MOVING_AVERAGE_DAYS, len(series) // 2)
    if windowSize >= 1:
        averages = movingAverage(series, windowSize)
        if averages[0] > 0.0:
            declineFraction = max(0.0, 1.0 - averages[-1] / averages[0])
            components["trend"] = scaleToRisk("herdDeclineFraction", declineFraction)

    minimumViablePopulation = raw.minimum_viable_population or context.minimum_viable_population
    if raw.census_count is not None and minimumViablePopulation:
        shortfall = max(0.0, 1.0 - raw.census_count / minimumViablePopulation)
        components["viability"] = scaleToRisk("viabilityShortfallFraction", shortfall)

    ageCounts = raw.age_class_counts
    juveniles = ageCounts.get("juvenile", 0)
    adults = ageCounts.get("adult", 0) + ageCounts.get("elder", 0)
    ageSource = "subclassification"
    if juveniles + adults == 0:
        boxClusters = classifyAgeByBoxArea(raw.vision_bbox_areas)
        if boxClusters is not None:
            juveniles, adults = boxClusters
            ageSource = "bbox_clustering"
    if adults > 0:
        recruitmentRatio = juveniles / adults
        baselineRatio = context.juvenile_recruitment_ratio or DEFAULT_JUVENILE_RECRUITMENT_RATIO
        deficit = max(0.0, 1.0 - recruitmentRatio / baselineRatio)
        components["recruitment"] = scaleToRisk("recruitmentDeficitFraction", deficit)
        components["recruitment_ratio"] = recruitmentRatio
        components["recruitment_from_bbox"] = 1.0 if ageSource == "bbox_clustering" else 0.0

    # Each mortality cluster of n deaths adds base * n * growth^(n - 1), so clustered deaths escalate exponentially
    mortalityPenalty = math.fsum(
        MORTALITY_BASE_PENALTY * size * MORTALITY_CLUSTER_GROWTH ** (size - 1)
        for size in clusterMortalityEvents(raw.mortality_events)
    ) if raw.mortality_events else None
    components["mortality"] = clampRisk(mortalityPenalty) if mortalityPenalty is not None else None

    baseScore = weightedScore(components, POPULATION_WEIGHTS)
    if baseScore is None and mortalityPenalty is None:
        return None, components
    return clampRisk((baseScore or 0.0) + (mortalityPenalty or 0.0)), components


# Habitat Condition Subscore (S_hab)

def calculateDegradationScore(
    metricMeans: dict[str, float], baselines: dict[str, MetricBaseline]
) -> tuple[Optional[float], dict[str, Optional[float]]]:
    metricScores: dict[str, Optional[float]] = {}
    for metric, direction in DEGRADATION_DIRECTIONS.items():
        if metric in metricMeans and metric in baselines:
            zScore = adverseZScore(metricMeans[metric], baselines[metric], direction)
            metricScores[f"degradation_{metric}"] = scaleToRisk("degradationZScore", zScore)
    scores = [score for score in metricScores.values() if score is not None]
    if not scores:
        return None, metricScores
    # Blend the mean with the worst metric so one severe spike (e.g. a DO crash) is never averaged away
    return clampRisk(0.5 * statistics.fmean(scores) + 0.5 * max(scores)), metricScores


def calculateHabitatSubscore(
    raw: RawIndicators, context: IndicatorContext
) -> tuple[Optional[float], dict[str, Optional[float]]]:
    components: dict[str, Optional[float]] = {"rangeLoss": None, "fragmentation": None, "degradation": None}

    currentRange = meanAnimalRangeKm2((fix.animal_id, fix.latitude, fix.longitude) for fix in raw.telemetry_fixes)
    baselineRange = context.baseline_range_km2 or context.home_range_km2
    components["current_range_km2"] = currentRange
    components["baseline_range_km2"] = baselineRange
    if currentRange is not None and baselineRange:
        components["rangeLoss"] = scaleToRisk("rangeLossFraction", max(0.0, 1.0 - currentRange / baselineRange))

    if context.telemetry_segment_count and context.infrastructure_crossings is not None:
        crossingRate = context.infrastructure_crossings / context.telemetry_segment_count
        components["fragmentation"] = scaleToRisk("fragmentationCrossingRate", crossingRate)
        components["crossing_rate"] = crossingRate

    degradationScore, metricScores = calculateDegradationScore(dict(raw.sensor_metric_means), context.metric_baselines)
    components["degradation"] = degradationScore
    components.update(metricScores)
    return weightedScore(components, HABITAT_WEIGHTS), components


# Threat Pressure Subscore (S_thr)

def getThreatClassWeight(className: str) -> float:
    # Longest matching keyword wins, so "vessel_engine" uses the vessel weight rather than the generic engine weight
    normalizedName = className.casefold().replace("-", "_").replace(" ", "_")
    matches = [keyword for keyword in THREAT_CLASS_WEIGHTS if keyword in normalizedName]
    if not matches:
        return DEFAULT_THREAT_WEIGHT
    return THREAT_CLASS_WEIGHTS[max(matches, key=len)]


def getZoneMultiplier(zone: HabitatZone) -> float:
    return zone.risk_multiplier or float(ZONE_MULTIPLIERS.get(zone.zone_type, 1.0))


def getProximityMultiplier(
    event: ThreatEvent,
    zones: tuple[HabitatZone, ...],
    centre: Optional[tuple[float, float]],
    homeRangeRadiusMeters: Optional[float],
) -> float:
    if event.latitude is None or event.longitude is None:
        return 1.0
    containingMultipliers = [
        getZoneMultiplier(zone)
        for zone in zones
        if haversineMeters(event.latitude, event.longitude, zone.latitude, zone.longitude) <= zone.radius_m
    ]
    if containingMultipliers:
        return max(containingMultipliers)
    if centre is not None and homeRangeRadiusMeters:
        distance = haversineMeters(event.latitude, event.longitude, centre[0], centre[1])
        if distance > homeRangeRadiusMeters:
            return PERIPHERY_MULTIPLIER
    return 1.0


def calculateThreatSubscore(
    raw: RawIndicators,
    context: IndicatorContext,
    centre: Optional[tuple[float, float]],
) -> tuple[Optional[float], dict[str, Optional[float]]]:
    # Threats are event-driven, so an empty 30-day ledger is a genuine zero-pressure reading rather than missing data
    referenceTime = raw.window_end
    windowSeconds = THREAT_WINDOW_DAYS * 86400.0
    halfLifeSeconds = THREAT_HALF_LIFE_HOURS * 3600.0
    homeRangeKm2 = context.baseline_range_km2 or context.home_range_km2
    homeRangeRadiusMeters = math.sqrt(homeRangeKm2 / math.pi) * 1000.0 if homeRangeKm2 else None

    pressure = 0.0
    eventCount = 0
    zoneEventCount = 0
    for event in raw.threat_events:
        ageSeconds = (referenceTime - event.detected_at).total_seconds()
        if ageSeconds < 0.0 or ageSeconds > windowSeconds:
            continue
        # Exponential time decay: a threat loses half of its weight every half-life
        decay = 2.0 ** (-ageSeconds / halfLifeSeconds)
        multiplier = getProximityMultiplier(event, context.habitat_zones, centre, homeRangeRadiusMeters)
        pressure += getThreatClassWeight(event.class_name) * event.confidence * decay * multiplier
        eventCount += 1
        zoneEventCount += 1 if multiplier > 1.0 else 0

    usedCountFallback = 0.0
    if not raw.threat_events and raw.acoustic_threat_event_count > 0:
        # Undated counts assume events spread evenly across the window, i.e. the mean decay over the window
        windowLengthSeconds = min(windowSeconds, (raw.window_end - raw.window_start).total_seconds())
        meanDecay = halfLifeSeconds / (windowLengthSeconds * math.log(2.0)) * (
            1.0 - 2.0 ** (-windowLengthSeconds / halfLifeSeconds)
        )
        pressure = raw.acoustic_threat_event_count * DEFAULT_THREAT_WEIGHT * meanDecay
        eventCount = raw.acoustic_threat_event_count
        usedCountFallback = 1.0

    # Saturating curve 1 - e^(-3p / ceiling): pressure at the ceiling scores 95, approaching 100 without a hard cliff
    pressureCeiling = SCALING_BOUNDS["threatPressure"][1]
    score = RISK_SCALE_MAX * (1.0 - math.exp(-3.0 * pressure / pressureCeiling))
    return clampRisk(score), {
        "decayed_pressure": pressure,
        "event_count": float(eventCount),
        "core_zone_event_count": float(zoneEventCount),
        "used_count_fallback": usedCountFallback,
    }


# Climate Stress Subscore (S_cli)

def escalateClimateAnomaly(zScore: float) -> float:
    # Quadratic rise below the sigma threshold, then non-linear escalation toward 100 once it is breached
    if zScore <= 0.0:
        return 0.0
    if zScore < CLIMATE_SIGMA_THRESHOLD:
        return CLIMATE_SUBTHRESHOLD_MAX * (zScore / CLIMATE_SIGMA_THRESHOLD) ** 2
    excess = zScore - CLIMATE_SIGMA_THRESHOLD
    return clampRisk(
        CLIMATE_SUBTHRESHOLD_MAX
        + (RISK_SCALE_MAX - CLIMATE_SUBTHRESHOLD_MAX) * (1.0 - math.exp(-CLIMATE_ESCALATION_RATE * excess))
    )


def calculateClimateSubscore(
    raw: RawIndicators, context: IndicatorContext
) -> tuple[Optional[float], dict[str, Optional[float]]]:
    components: dict[str, Optional[float]] = {}
    anomalyScores: list[float] = []
    extremeFractions: list[float] = []
    rollingStart = raw.window_end - timedelta(days=CLIMATE_ROLLING_DAYS)

    for metric, direction in CLIMATE_DIRECTIONS.items():
        baseline = context.metric_baselines.get(metric)
        if baseline is None:
            continue
        dailyValues = [
            sample.value
            for sample in raw.climate_daily_means.get(metric, ())
            if rollingStart < sample.day <= raw.window_end
        ]
        if dailyValues:
            rollingMean = statistics.fmean(dailyValues)
            extremeDays = sum(
                1 for value in dailyValues
                if adverseZScore(value, baseline, direction, useDailySpread=True) >= CLIMATE_SIGMA_THRESHOLD
            )
            extremeFractions.append(extremeDays / len(dailyValues))
            components[f"extreme_day_fraction_{metric}"] = extremeDays / len(dailyValues)
        elif metric in raw.sensor_metric_means:
            rollingMean = raw.sensor_metric_means[metric]
        else:
            continue
        zScore = adverseZScore(rollingMean, baseline, direction)
        components[f"z_{metric}"] = zScore
        anomalyScores.append(escalateClimateAnomaly(zScore))

    if not anomalyScores:
        return None, components
    worstAnomaly = max(anomalyScores)
    frequencyScore = scaleToRisk("extremeWeatherDayFraction", max(extremeFractions)) if extremeFractions else 0.0
    components["worst_anomaly"] = worstAnomaly
    components["extreme_weather_frequency"] = frequencyScore
    return clampRisk(worstAnomaly + EXTREME_WEATHER_WEIGHT * frequencyScore), components


# Genetic Resilience Subscore (S_gen)

def meanExpectedHeterozygosity(lociCounts: dict[str, dict[str, int]]) -> Optional[float]:
    # Nei's gene diversity He = 1 - sum(p_i^2), averaged across loci
    values = []
    for alleleCounts in lociCounts.values():
        geneCopies = sum(alleleCounts.values())
        if geneCopies > 0:
            values.append(1.0 - math.fsum((count / geneCopies) ** 2 for count in alleleCounts.values()))
    return statistics.fmean(values) if values else None


def calculateGeneticSubscore(
    raw: RawIndicators, context: IndicatorContext
) -> tuple[Optional[float], dict[str, Optional[float]]]:
    components: dict[str, Optional[float]] = {"effectivePopulation": None, "heterozygosity": None}
    effectivePopulationSize = raw.effective_population_size
    criticalThreshold = context.critical_ne_threshold
    viableThreshold = max(context.viable_ne_threshold, criticalThreshold + 1.0)

    if effectivePopulationSize is not None:
        # S_gen = max(0, 100 * (1 - Ne / 500)) using the species' long-term viability threshold
        components["effectivePopulation"] = clampRisk(
            RISK_SCALE_MAX * (1.0 - effectivePopulationSize / viableThreshold)
        )

    heterozygosity = raw.expected_heterozygosity
    if heterozygosity is None:
        heterozygosity = meanExpectedHeterozygosity(raw.edna_loci_counts)
    if heterozygosity is not None:
        # Inverted: lower diversity means higher risk
        deficit = max(0.0, 1.0 - heterozygosity / HETEROZYGOSITY_BASELINE)
        components["heterozygosity"] = scaleToRisk("heterozygosityDeficitFraction", deficit)
        components["expected_heterozygosity"] = heterozygosity

    # Hard stop: Ne below the critical threshold pins the score to 100 as an inbreeding depression crisis
    if effectivePopulationSize is not None and effectivePopulationSize < criticalThreshold:
        components["critical_inbreeding_override"] = 1.0
        return RISK_SCALE_MAX, components
    return weightedScore(components, GENETIC_WEIGHTS), components


# Behavioral Stability Subscore (S_beh)

def calculateCorridorRetention(
    fixes: tuple[TelemetryFix, ...], zones: tuple[HabitatZone, ...]
) -> Optional[float]:
    corridors = [zone for zone in zones if zone.zone_type == "CORRIDOR"]
    if not corridors or not fixes:
        return None
    insideCounts: dict[str, list[int]] = {}
    for fix in fixes:
        counts = insideCounts.setdefault(fix.animal_id, [0, 0])
        counts[1] += 1
        if any(
            haversineMeters(fix.latitude, fix.longitude, zone.latitude, zone.longitude) <= zone.radius_m
            for zone in corridors
        ):
            counts[0] += 1
    retainedAnimals = sum(1 for inside, total in insideCounts.values() if inside / total >= CORRIDOR_FIX_SHARE)
    return retainedAnimals / len(insideCounts)


def isOrphanGroup(group: SocialGroupObservation) -> bool:
    return group.juvenile_count > 0 and group.adult_count == 0 and group.elder_count == 0


def calculateBehaviorSubscore(
    raw: RawIndicators, context: IndicatorContext
) -> tuple[Optional[float], dict[str, Optional[float]]]:
    components: dict[str, Optional[float]] = {"corridor": None, "social": None, "physiology": None}

    retention = calculateCorridorRetention(raw.telemetry_fixes, context.habitat_zones)
    if retention is not None:
        components["corridor_retention"] = retention
        components["corridor"] = scaleToRisk(
            "corridorRetentionLoss", max(0.0, 1.0 - retention / CORRIDOR_EXPECTED_RETENTION)
        )

    groups = raw.social_groups
    if groups:
        # Missing elder matriarchs signal lost ecological knowledge; groups with no mature animals at all count double
        elderAbsence = math.fsum(
            (1.0 if group.adult_count == 0 else 0.5) for group in groups if group.elder_count == 0
        ) / len(groups)
        components["elder_absence_fraction"] = elderAbsence
        components["social"] = scaleToRisk("elderAbsenceFraction", elderAbsence)

    heartRate = raw.mean_heart_rate_bpm
    restingMin, restingMax = context.resting_heart_rate_min_bpm, context.resting_heart_rate_max_bpm
    if heartRate is not None and restingMin is not None and restingMax is not None:
        if heartRate > restingMax:
            deviation = (heartRate - restingMax) / restingMax
        elif heartRate < restingMin:
            deviation = (restingMin - heartRate) / restingMin
        else:
            deviation = 0.0
        components["physiology"] = scaleToRisk("heartRateDeviationFraction", deviation)

    score = weightedScore(components, BEHAVIOR_WEIGHTS)
    orphanDays = len({group.observed_at.date() for group in groups if isOrphanGroup(group)})
    components["orphan_group_days"] = float(orphanDays)
    if orphanDays == 0:
        return score, components
    # Juvenile-only clusters sustained over several days apply a severe floor; brief sightings add a smaller penalty
    if orphanDays >= ORPHAN_MINIMUM_DAYS:
        return max(score or 0.0, ORPHAN_PENALTY_FLOOR), components
    return clampRisk((score or 0.0) + ORPHAN_DAY_PENALTY * orphanDays), components


# Subscore compilation

def calculateNormalizedSubscores(
    raw: RawIndicators,
    context: Optional[IndicatorContext] = None,
    scope: Optional[AssessmentScope] = None,
) -> tuple[NormalizedSubscores, list[str]]:
    context = context or IndicatorContext()
    centre = estimateScopeCentre(scope, raw)
    domainResults = {
        "population": calculatePopulationSubscore(raw, context),
        "habitat": calculateHabitatSubscore(raw, context),
        "threat": calculateThreatSubscore(raw, context, centre),
        "climate": calculateClimateSubscore(raw, context),
        "genetics": calculateGeneticSubscore(raw, context),
        "behavior": calculateBehaviorSubscore(raw, context),
    }

    warnings = list(context.warnings)
    scores: dict[str, float] = {}
    missingDomains: list[str] = []
    scoreComponents: dict[str, dict[str, Optional[float]]] = {}
    for domain in RISK_DOMAINS:
        score, components = domainResults[domain]
        if score is None:
            missingDomains.append(domain)
            warnings.append(
                f"indicatorCalculator: no usable {domain} data; applied precautionary score {MISSING_DOMAIN_SCORE:.1f}."
            )
            score = MISSING_DOMAIN_SCORE
        scores[domain] = clampRisk(score)
        scoreComponents[domain] = {
            name: (round(value, 6) if value is not None else None) for name, value in components.items()
        }

    # The 0-100 working scale is stored on the graph state's 0.0-1.0 UnitScore scale
    subscores = NormalizedSubscores.model_validate({
        **{domain: scores[domain] / RISK_SCALE_MAX for domain in RISK_DOMAINS},
        "score_components": scoreComponents,
        "missing_domains": tuple(missingDomains),
    })
    return subscores, warnings


# Database context fetch

def toEpochMilliseconds(timestamp: datetime) -> int:
    return int(round(timestamp.astimezone(timezone.utc).timestamp() * 1000.0))


def buildMetricBaselines(
    dailyRows: list[dict[str, object]], windowEnd: datetime
) -> dict[str, MetricBaseline]:
    # Same-season windows from previous years give the long-term baseline for a rolling 30-day mean
    windowEndMs = toEpochMilliseconds(windowEnd)
    rollingMs = int(CLIMATE_ROLLING_DAYS * MILLISECONDS_PER_DAY)
    baselines: dict[str, MetricBaseline] = {}
    for metric in BASELINE_METRICS:
        dailyValues = [
            (int(row["day_index"]) * MILLISECONDS_PER_DAY, float(row[metric]))
            for row in dailyRows
            if row.get(metric) is not None
        ]
        if len(dailyValues) < MINIMUM_BASELINE_DAYS:
            continue
        windowMeans = []
        seasonalValues = []
        for yearOffset in range(1, CLIMATE_BASELINE_YEARS + 1):
            seasonEndMs = windowEndMs - int(yearOffset * DAYS_PER_YEAR * MILLISECONDS_PER_DAY)
            seasonValues = [value for dayMs, value in dailyValues if seasonEndMs - rollingMs < dayMs <= seasonEndMs]
            if len(seasonValues) >= MINIMUM_BASELINE_DAYS:
                windowMeans.append(statistics.fmean(seasonValues))
                seasonalValues.extend(seasonValues)

        if len(windowMeans) >= MINIMUM_BASELINE_YEARS:
            dailySpread = statistics.stdev(seasonalValues)
            # Floor at the standard error of a rolling mean so very stable years never collapse the spread to zero
            samplingSpread = dailySpread / math.sqrt(len(seasonalValues) / len(windowMeans))
            mean, spread = statistics.fmean(windowMeans), max(statistics.stdev(windowMeans), samplingSpread)
            sampleCount = len(windowMeans)
        else:
            # Short histories fall back to the full daily distribution, which is a wider and more cautious spread
            allValues = [value for _, value in dailyValues]
            mean, spread = statistics.fmean(allValues), statistics.stdev(allValues)
            dailySpread = spread
            sampleCount = len(allValues)
        if spread <= 1e-9:
            continue
        baselines[metric] = MetricBaseline(
            mean=mean, std=spread, daily_std=dailySpread if dailySpread > 1e-9 else None, sample_count=sampleCount
        )
    return baselines


async def fetchIndicatorContext(
    databaseManager: Any, scope: AssessmentScope, raw: RawIndicators
) -> IndicatorContext:
    # Each section degrades independently so one missing table never blinds every subscore
    contextValues: dict[str, Any] = {}
    warnings: list[str] = []
    windowStartMs = toEpochMilliseconds(raw.window_start)
    windowEndMs = toEpochMilliseconds(raw.window_end)

    try:
        speciesRows = await databaseManager.fetchAll(
            """
            SELECT minimum_viable_population, critical_ne_threshold, viable_ne_threshold,
                   resting_heart_rate_min_bpm, resting_heart_rate_max_bpm, home_range_km2,
                   juvenile_recruitment_ratio
            FROM species WHERE id = ?;
            """,
            (scope.species_id,),
        )
        if speciesRows:
            for column, value in speciesRows[0].items():
                # A zero recruitment ratio cannot act as a baseline, so it falls back to the config default
                if value is not None and float(value) > 0.0:
                    contextValues[column] = float(value)
        else:
            warnings.append(f"indicatorCalculator: species {scope.species_id} not found; using config baselines.")
    except sqlite3.Error as error:
        warnings.append(f"indicatorCalculator: species baseline query failed: {error}")

    try:
        metricColumns = ", ".join(f"AVG({metric}) AS {metric}" for metric in BASELINE_METRICS)
        baselineStartMs = windowEndMs - int((CLIMATE_BASELINE_YEARS * DAYS_PER_YEAR + CLIMATE_ROLLING_DAYS) * MILLISECONDS_PER_DAY)
        dailyRows = await databaseManager.fetchAll(
            f"""
            SELECT recorded_epoch_ms / {MILLISECONDS_PER_DAY} AS day_index, {metricColumns}
            FROM sensor_readings
            WHERE h3_cell = ? AND quality_flag <> 'INVALID'
                AND recorded_epoch_ms >= ? AND recorded_epoch_ms < ?
            GROUP BY day_index;
            """,
            (scope.h3_cell, baselineStartMs, windowStartMs),
        )
        contextValues["metric_baselines"] = buildMetricBaselines(dailyRows, raw.window_end)
    except sqlite3.Error as error:
        warnings.append(f"indicatorCalculator: sensor baseline query failed: {error}")

    centre = estimateScopeCentre(scope, raw)
    try:
        zoneQuery = """
            SELECT zone_name, zone_type, latitude, longitude, radius_m, risk_multiplier
            FROM habitat_zones
            WHERE active = 1 AND (species_id = ? OR species_id IS NULL)
        """
        zoneParameters: list[object] = [scope.species_id]
        if centre is not None:
            searchFrames = getSearchFrames(centre[0], centre[1], ZONE_SEARCH_RADIUS_M)
            # SpatialIndex subquery drives the R-Tree lookup, as in dbManager.buildThreatRadiusQuery
            zoneQuery += " AND ROWID IN (" + " UNION ".join(
                """
                SELECT ROWID FROM SpatialIndex
                WHERE f_table_name = 'habitat_zones' AND f_geometry_column = 'geom'
                    AND search_frame = BuildMbr(?, ?, ?, ?, 4326)"""
                for _ in searchFrames
            ) + ")"
            for searchFrame in searchFrames:
                zoneParameters.extend(searchFrame)
        zoneRows = await databaseManager.fetchAll(zoneQuery + ";", zoneParameters)
        contextValues["habitat_zones"] = tuple(
            HabitatZone.model_validate({
                **row,
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "radius_m": float(row["radius_m"]),
                "risk_multiplier": float(row["risk_multiplier"]) if row["risk_multiplier"] is not None else None,
            })
            for row in zoneRows
        )
    except sqlite3.Error as error:
        warnings.append(f"indicatorCalculator: habitat zone query failed: {error}")

    try:
        # Consecutive fixes per animal become path segments tested against infrastructure with ST_Intersects
        crossingRows = await databaseManager.fetchAll(
            """
            WITH ordered AS (
                SELECT CastToXY(geom) AS point,
                       LAG(CastToXY(geom)) OVER (PARTITION BY animal_id ORDER BY recorded_epoch_ms) AS previous_point
                FROM telemetry
                WHERE species_id = ? AND recorded_epoch_ms >= ? AND recorded_epoch_ms <= ? AND geom IS NOT NULL
            ),
            segments AS (
                SELECT MakeLine(previous_point, point) AS path
                FROM ordered
                WHERE previous_point IS NOT NULL AND NOT ST_Equals(previous_point, point)
            )
            SELECT
                (SELECT COUNT(*) FROM segments) AS segment_count,
                (SELECT COUNT(*) FROM segments
                 WHERE EXISTS (
                     SELECT 1 FROM infrastructure_features AS features
                     WHERE features.detected_epoch_ms <= ?
                         AND (features.removed_at IS NULL OR julianday(features.removed_at) > julianday(?))
                         AND features.ROWID IN (
                             SELECT ROWID FROM SpatialIndex
                             WHERE f_table_name = 'infrastructure_features'
                                 AND f_geometry_column = 'geom'
                                 AND search_frame = segments.path
                         )
                         AND ST_Intersects(features.geom, segments.path) = 1
                 )) AS crossing_count;
            """,
            (
                scope.species_id,
                windowStartMs,
                windowEndMs,
                windowEndMs,
                raw.window_end.isoformat(),
            ),
        )
        if crossingRows:
            contextValues["telemetry_segment_count"] = int(crossingRows[0]["segment_count"] or 0)
            contextValues["infrastructure_crossings"] = int(crossingRows[0]["crossing_count"] or 0)
    except sqlite3.Error as error:
        warnings.append(f"indicatorCalculator: infrastructure crossing query failed: {error}")

    try:
        # Same-length window one year earlier, so the MCP comparison is not biased by tracking duration
        yearMs = int(DAYS_PER_YEAR * MILLISECONDS_PER_DAY)
        baselineFixes = await databaseManager.fetchAll(
            """
            SELECT animal_id, latitude, longitude FROM telemetry
            WHERE species_id = ? AND recorded_epoch_ms >= ? AND recorded_epoch_ms <= ?;
            """,
            (scope.species_id, windowStartMs - yearMs, windowEndMs - yearMs),
        )
        baselineRange = meanAnimalRangeKm2(
            (str(row["animal_id"]), float(row["latitude"]), float(row["longitude"])) for row in baselineFixes
        )
        if baselineRange is not None:
            contextValues["baseline_range_km2"] = baselineRange
    except sqlite3.Error as error:
        warnings.append(f"indicatorCalculator: baseline range query failed: {error}")

    return IndicatorContext.model_validate({**contextValues, "warnings": tuple(warnings)})


# LangGraph node

def createIndicatorCalculatorNode(
    databaseManager: Optional[Any] = None,
    context: Optional[IndicatorContext] = None,
) -> Any:
    @validatedNode("indicatorCalculator")
    async def indicatorCalculatorNode(state: dict[str, Any]) -> dict[str, Any]:
        raw = state.get("raw_indicators")
        if raw is None:
            raise StateValidationError("The Indicator Calculator Node requires raw_indicators from the Data Aggregation Node.")
        scope = state.get("assessment_scope")

        indicatorContext = context
        if indicatorContext is None and databaseManager is not None and scope is not None:
            indicatorContext = await fetchIndicatorContext(databaseManager, scope, raw)

        subscores, warnings = calculateNormalizedSubscores(raw, indicatorContext, scope)
        # Only changed channels are returned; LangGraph reducers merge them without mutating the input state
        update: dict[str, Any] = {"normalized_subscores": subscores}
        if warnings:
            update["pipeline_errors"] = tuple(warnings)
        return update

    return indicatorCalculatorNode


async def main() -> None:
    from aiEngine.stateSchema import createInitialState

    windowEnd = datetime.now(timezone.utc).replace(microsecond=0)
    windowStart = windowEnd - timedelta(days=30)
    state = dict(createInitialState(
        "indicator-demo",
        {
            "species_id": 1,
            "h3_cell": "872830828ffffff",
            "period_start": windowStart,
            "period_end": windowEnd,
            "latitude": -1.30,
            "longitude": 36.80,
        },
    ))
    state["raw_indicators"] = RawIndicators.model_validate({
        "aggregated_at": windowEnd,
        "window_start": windowStart,
        "window_end": windowEnd,
        "effective_population_size": 180.0,
        "expected_heterozygosity": 0.41,
        "census_count": 420,
        "minimum_viable_population": 500,
        "herd_count_series": [52, 50, 49, 51, 47, 46, 44, 45, 43, 41, 40, 39, 40, 38],
        "age_class_counts": {"juvenile": 9, "adult": 40, "elder": 3},
        "threat_events": [
            {"detected_at": windowEnd - timedelta(hours=2), "class_name": "gunshot", "confidence": 0.92,
             "latitude": -1.31, "longitude": 36.81},
            {"detected_at": windowEnd - timedelta(days=28), "class_name": "chainsaw", "confidence": 0.8},
        ],
        "sensor_metric_means": {"dissolved_oxygen_mg_l": 5.1, "water_turbidity_ntu": 22.0, "ambient_temperature_c": 31.5},
        "social_groups": [
            {"observed_at": windowEnd - timedelta(days=1), "juvenile_count": 4, "adult_count": 6, "elder_count": 1},
            {"observed_at": windowEnd - timedelta(days=3), "juvenile_count": 2, "adult_count": 3},
        ],
    })
    demoContext = IndicatorContext(
        metric_baselines={
            "dissolved_oxygen_mg_l": MetricBaseline(mean=7.5, std=0.8),
            "water_turbidity_ntu": MetricBaseline(mean=12.0, std=4.0),
            "ambient_temperature_c": MetricBaseline(mean=27.0, std=1.5),
        },
        habitat_zones=(HabitatZone(zone_name="Main watering hole", zone_type="WATER_SOURCE",
                                   latitude=-1.31, longitude=36.81, radius_m=2000.0),),
        home_range_km2=150.0,
    )
    update = await createIndicatorCalculatorNode(context=demoContext)(state)
    subscores: NormalizedSubscores = update["normalized_subscores"]
    for domain, score in subscores.toDatabaseScale().items():
        print(f"{domain}: {score:.1f}")
    for warning in update.get("pipeline_errors", ()):
        print(f"Warning: {warning}")


if __name__ == "__main__":
    asyncio.run(main())
