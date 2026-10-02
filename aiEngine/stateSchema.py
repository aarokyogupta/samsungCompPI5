from datetime import datetime, timezone
import functools
import inspect
import math
import os
import sys
from typing import Any, Callable, Iterable, Literal, Optional, TypedDict
from langgraph.types import Overwrite
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    StrictBool,
    StringConstraints,
    ValidationError,
    computed_field,
    field_validator,
    model_validator,
)
from typing_extensions import Annotated
import yaml

# Allow "python aiEngine/graphWorkflow.py" as well as "python -m aiEngine.graphWorkflow" from the project root
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database.models import (
    H3_CELL_PATTERN,
    RiskAssessment,
    RiskAssessmentSchema,
    clampUnitInterval,
    formatUtcTimestamp,
    toPythonValue,
    toUtcDatetime,
)

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

AI_ENGINE_CONFIG: dict = config.get("aiEngine", {})
GENETIC_CONFIG: dict = config.get("edna", {}).get("geneticAnalysis", {})
STATE_SCHEMA_VERSION: str = str(AI_ENGINE_CONFIG.get("stateSchemaVersion", "1.0"))
MODEL_VERSION: str = str(AI_ENGINE_CONFIG.get("modelVersion", "icmis-risk-engine-1.0"))
MOMENTUM_WINDOW_DAYS: float = float(AI_ENGINE_CONFIG.get("momentumWindowDays", 30.0))
MOMENTUM_STABLE_TOLERANCE: float = float(AI_ENGINE_CONFIG.get("momentumStableTolerancePerDay", 0.001))
CRITICAL_NE_THRESHOLD: float = float(
    AI_ENGINE_CONFIG.get("criticalNeThreshold", GENETIC_CONFIG.get("criticalNeThreshold", 50.0))
)

RISK_DOMAINS: tuple[str, ...] = ("population", "habitat", "threat", "climate", "genetics", "behavior")
DEFAULT_DOMAIN_WEIGHTS: dict[str, float] = {
    "population": 0.25,
    "habitat": 0.15,
    "threat": 0.25,
    "climate": 0.10,
    "genetics": 0.15,
    "behavior": 0.10,
}
DOMAIN_WEIGHTS: dict[str, float] = {
    domain: float(weight)
    for domain, weight in (AI_ENGINE_CONFIG.get("domainWeights") or DEFAULT_DOMAIN_WEIGHTS).items()
}

# Graph state works on a 0.0-1.0 scale while risk_assessments stores 0-100 scores
DATABASE_SCORE_SCALE: float = 100.0
# Must match the generated escalation_level column in schema.sql (expressed on the 0.0-1.0 scale)
ESCALATION_BOUNDARIES: tuple[float, float, float, float] = (0.20, 0.40, 0.60, 0.80)
WEIGHT_SUM_TOLERANCE: float = 1e-6
CRI_CONSISTENCY_TOLERANCE: float = 1e-6
# Intervention Priority Index tiers on the 0-100 column scale, checked from highest to lowest
PRIORITY_TIERS: tuple[tuple[float, str], ...] = (
    (80.0, "IMMEDIATE"),
    (60.0, "HIGH"),
    (40.0, "ELEVATED"),
    (20.0, "ROUTINE"),
    (0.0, "MONITOR"),
)
TRACEABLE_TABLES: tuple[str, ...] = (
    "telemetry",
    "sensor_readings",
    "detections",
    "genetics_records",
    "acoustic_events",
    "camera_ingestion",
    "edna_samples",
)

if MOMENTUM_WINDOW_DAYS <= 0.0:
    raise ValueError("aiEngine momentumWindowDays must be positive.")
if MOMENTUM_STABLE_TOLERANCE < 0.0:
    raise ValueError("aiEngine momentumStableTolerancePerDay must be non-negative.")
if CRITICAL_NE_THRESHOLD < 0.0:
    raise ValueError("aiEngine criticalNeThreshold must be non-negative.")
if set(DOMAIN_WEIGHTS) != set(RISK_DOMAINS):
    raise ValueError(f"aiEngine domainWeights must define exactly these domains: {', '.join(RISK_DOMAINS)}.")
if any(weight < 0.0 or not math.isfinite(weight) for weight in DOMAIN_WEIGHTS.values()):
    raise ValueError("aiEngine domainWeights must be finite and non-negative.")
if abs(math.fsum(DOMAIN_WEIGHTS.values()) - 1.0) > WEIGHT_SUM_TOLERANCE:
    raise ValueError("aiEngine domainWeights must sum to 1.0.")


class StateValidationError(ValueError):
    pass


class ImmutableStateError(StateValidationError):
    pass


# Shared field types

RiskDomain = Literal["population", "habitat", "threat", "climate", "genetics", "behavior"]
TraceableTable = Literal[
    "telemetry",
    "sensor_readings",
    "detections",
    "genetics_records",
    "acoustic_events",
    "camera_ingestion",
    "edna_samples",
]
Trajectory = Literal["ACCELERATING", "STABLE", "DECAYING"]
PopulationTrend = Literal["DECLINING", "STABLE", "INCREASING"]
AlertPriority = Literal["ROUTINE", "HIGH", "CRITICAL"]
InterventionAction = Literal[
    "ROUTINE_MONITORING",
    "TARGETED_PATROLS",
    "HABITAT_RESTORATION",
    "GENETIC_ASSAY",
    "CAPTIVE_BREEDING_PREP",
    "GENETIC_RESCUE",
    "RANGER_DISPATCH",
    "DRONE_DEPLOYMENT",
]

NonEmptyText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
def clampUnitScore(value: object) -> object:
    # Absorb float rounding such as 1.0000001 while leaving non-numeric input for strict type errors
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return clampUnitInterval(float(value))
    return value


UtcDatetime = Annotated[datetime, BeforeValidator(toUtcDatetime)]
UnitScore = Annotated[float, BeforeValidator(clampUnitScore), Field(ge=0.0, le=1.0)]
Decibel = Annotated[float, Field(ge=-200.0, le=250.0)]
Latitude = Annotated[float, Field(ge=-90.0, le=90.0)]
Longitude = Annotated[float, Field(ge=-180.0, le=180.0)]

# Baseline directives for each escalation tier, matching the Level 1-5 plan in aiResponse.txt
LEVEL_BASE_ACTIONS: dict[int, tuple[str, ...]] = {
    1: ("ROUTINE_MONITORING",),
    2: ("TARGETED_PATROLS",),
    3: ("HABITAT_RESTORATION", "GENETIC_ASSAY"),
    4: ("CAPTIVE_BREEDING_PREP",),
    5: ("GENETIC_RESCUE",),
}


def freezeValue(value: object) -> object:
    # NumPy arrays and lists become tuples so frozen state models cannot be mutated in place
    value = toPythonValue(value)
    if isinstance(value, dict):
        return {key: freezeValue(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return tuple(freezeValue(item) for item in value)
    return value


def escalationLevelForCri(conservationRiskIndex: float) -> int:
    for level, upperBound in enumerate(ESCALATION_BOUNDARIES[:3], start=1):
        if conservationRiskIndex < upperBound:
            return level
    # Level 4 includes CRI == 0.80 exactly, as in the schema's "<= 80.0" branch
    return 4 if conservationRiskIndex <= ESCALATION_BOUNDARIES[3] else 5


def priorityTierForIndex(interventionPriorityIndex: Optional[float]) -> Optional[str]:
    if interventionPriorityIndex is None:
        return None
    for lowerBound, tier in PRIORITY_TIERS:
        if interventionPriorityIndex >= lowerBound:
            return tier
    return PRIORITY_TIERS[-1][1]


class StateModel(BaseModel):
    # Frozen strict models stop nodes from mutating or silently coercing data inside the graph state
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", allow_inf_nan=False)

    @model_validator(mode="before")
    @classmethod
    def freezeInputValues(cls, data: object) -> object:
        if isinstance(data, dict):
            return {key: freezeValue(value) for key, value in data.items()}
        return data


class CoordinateModel(StateModel):
    @model_validator(mode="after")
    def requireCoordinatePair(self) -> "CoordinateModel":
        latitude = getattr(self, "latitude", None)
        longitude = getattr(self, "longitude", None)
        if (latitude is None) != (longitude is None):
            raise ValueError("Latitude and longitude must both be provided or both be omitted.")
        return self


def normalizeH3Cell(h3Cell: object) -> object:
    if isinstance(h3Cell, str):
        h3Cell = h3Cell.strip().lower()
        if not H3_CELL_PATTERN.match(h3Cell):
            raise ValueError(f"H3 cell {h3Cell!r} must be a 15-character hexadecimal index.")
    return h3Cell


# Assessment scope & tracing keys

class AssessmentScope(CoordinateModel):
    species_id: PositiveInt
    scientific_name: Optional[NonEmptyText] = None
    h3_cell: str
    period_start: UtcDatetime
    period_end: UtcDatetime
    latitude: Optional[Latitude] = None
    longitude: Optional[Longitude] = None

    @field_validator("h3_cell", mode="before")
    @classmethod
    def validateH3Cell(cls, h3Cell: object) -> object:
        return normalizeH3Cell(h3Cell)

    @model_validator(mode="after")
    def validatePeriod(self) -> "AssessmentScope":
        if self.period_end <= self.period_start:
            raise ValueError("period_end must be later than period_start.")
        return self


class TracingKeys(StateModel):
    # Primary keys of the SpatiaLite rows that fed this graph run, grouped by source table
    record_ids: dict[TraceableTable, tuple[PositiveInt, ...]] = Field(default_factory=dict)
    previous_assessment_key: Optional[dict[str, Any]] = None

    @field_validator("record_ids")
    @classmethod
    def deduplicateRecordIDs(
        cls, recordIDs: dict[str, tuple[int, ...]]
    ) -> dict[str, tuple[int, ...]]:
        return {table: tuple(sorted(set(ids))) for table, ids in recordIDs.items() if ids}

    def merge(self, other: "TracingKeys") -> "TracingKeys":
        mergedIDs: dict[str, set[int]] = {table: set(ids) for table, ids in self.record_ids.items()}
        for table, ids in other.record_ids.items():
            mergedIDs.setdefault(table, set()).update(ids)
        previousKey = other.previous_assessment_key or self.previous_assessment_key
        return TracingKeys.model_validate(
            {
                "record_ids": {table: tuple(ids) for table, ids in mergedIDs.items()},
                "previous_assessment_key": previousKey,
            }
        )

    def recordCount(self) -> int:
        return sum(len(ids) for ids in self.record_ids.values())


# Detailed observations used by the Indicator Calculator Node

AgeClass = Literal["juvenile", "subadult", "adult", "elder"]
ThreatSource = Literal["ACOUSTIC", "VISION", "PATROL_REPORT"]


class TelemetryFix(CoordinateModel):
    animal_id: NonEmptyText
    recorded_at: UtcDatetime
    latitude: Latitude
    longitude: Longitude


class ThreatEvent(CoordinateModel):
    # immediate_threat detections (gunshots, chainsaws, vessel engines) and ranger poaching reports
    detected_at: UtcDatetime
    class_name: NonEmptyText
    source_type: ThreatSource = "ACOUSTIC"
    confidence: UnitScore = 1.0
    latitude: Optional[Latitude] = None
    longitude: Optional[Longitude] = None
    record_id: Optional[PositiveInt] = None


class MortalityEvent(CoordinateModel):
    recorded_at: UtcDatetime
    latitude: Optional[Latitude] = None
    longitude: Optional[Longitude] = None
    cause: Optional[NonEmptyText] = None


class SocialGroupObservation(StateModel):
    # Age-class makeup of one herd sighting from vision subclassifications
    observed_at: UtcDatetime
    juvenile_count: NonNegativeInt = 0
    subadult_count: NonNegativeInt = 0
    adult_count: NonNegativeInt = 0
    elder_count: NonNegativeInt = 0

    @model_validator(mode="after")
    def requireMembers(self) -> "SocialGroupObservation":
        if self.juvenile_count + self.subadult_count + self.adult_count + self.elder_count == 0:
            raise ValueError("A social group observation must contain at least one individual.")
        return self


class DailyMetricValue(StateModel):
    day: UtcDatetime
    value: Annotated[float, Field(allow_inf_nan=False)]


# Raw indicator payloads populated once by the Data Aggregation Node

class RawIndicators(StateModel):
    aggregated_at: UtcDatetime
    window_start: UtcDatetime
    window_end: UtcDatetime
    # Genetics: locus -> allele -> observed gene-copy count from edna_samples/genetics_records
    edna_loci_counts: dict[NonEmptyText, dict[NonEmptyText, NonNegativeInt]] = Field(default_factory=dict)
    edna_sample_count: NonNegativeInt = 0
    expected_heterozygosity: Optional[UnitScore] = None
    allelic_richness: Optional[NonNegativeFloat] = None
    effective_population_size: Optional[NonNegativeFloat] = None
    # Threat: peak sound levels of acoustic immediate_threat events in the window
    acoustic_threat_decibels: tuple[Decibel, ...] = ()
    acoustic_threat_event_count: NonNegativeInt = 0
    # Population: vision herd density grid (animals per cell) plus census baselines
    vision_herd_density: tuple[tuple[NonNegativeFloat, ...], ...] = ()
    vision_detection_count: NonNegativeInt = 0
    census_count: Optional[NonNegativeInt] = None
    minimum_viable_population: Optional[PositiveInt] = None
    # Habitat & climate: window means of sensor_readings columns, keyed by column name
    sensor_metric_means: dict[NonEmptyText, float] = Field(default_factory=dict)
    # Behavior: telemetry movement and physiology summaries
    telemetry_fix_count: NonNegativeInt = 0
    mean_speed_kmh: Optional[NonNegativeFloat] = None
    mean_heart_rate_bpm: Optional[PositiveFloat] = None
    # Detailed arrays for subscore calculation (all optional so partial aggregations still validate)
    herd_count_series: tuple[NonNegativeFloat, ...] = ()
    age_class_counts: dict[AgeClass, NonNegativeInt] = Field(default_factory=dict)
    vision_bbox_areas: tuple[UnitScore, ...] = ()
    mortality_events: tuple[MortalityEvent, ...] = ()
    threat_events: tuple[ThreatEvent, ...] = ()
    telemetry_fixes: tuple[TelemetryFix, ...] = ()
    climate_daily_means: dict[NonEmptyText, tuple[DailyMetricValue, ...]] = Field(default_factory=dict)
    social_groups: tuple[SocialGroupObservation, ...] = ()

    @field_validator("edna_loci_counts")
    @classmethod
    def validateLociCounts(
        cls, lociCounts: dict[str, dict[str, int]]
    ) -> dict[str, dict[str, int]]:
        for locus, alleleCounts in lociCounts.items():
            if not alleleCounts:
                raise ValueError(f"Locus {locus!r} must contain at least one allele count.")
        return lociCounts

    @field_validator("vision_herd_density")
    @classmethod
    def validateDensityMatrix(
        cls, densityMatrix: tuple[tuple[float, ...], ...]
    ) -> tuple[tuple[float, ...], ...]:
        rowWidths = {len(row) for row in densityMatrix}
        if densityMatrix and (len(rowWidths) != 1 or 0 in rowWidths):
            raise ValueError("vision_herd_density must be a non-empty rectangular matrix.")
        return densityMatrix

    @model_validator(mode="after")
    def validateWindow(self) -> "RawIndicators":
        if self.window_end <= self.window_start:
            raise ValueError("window_end must be later than window_start.")
        if self.acoustic_threat_event_count < len(self.acoustic_threat_decibels):
            raise ValueError("acoustic_threat_event_count cannot be lower than the number of decibel readings.")
        return self

    def summary(self) -> dict[str, Any]:
        # Compact audit summary stored in risk_assessments.input_summary instead of the full arrays
        decibels = self.acoustic_threat_decibels
        densityValues = [value for row in self.vision_herd_density for value in row]
        return {
            "aggregated_at": formatUtcTimestamp(self.aggregated_at),
            "window_start": formatUtcTimestamp(self.window_start),
            "window_end": formatUtcTimestamp(self.window_end),
            "edna_locus_count": len(self.edna_loci_counts),
            "edna_gene_copy_count": sum(sum(alleles.values()) for alleles in self.edna_loci_counts.values()),
            "edna_sample_count": self.edna_sample_count,
            "expected_heterozygosity": self.expected_heterozygosity,
            "allelic_richness": self.allelic_richness,
            "effective_population_size": self.effective_population_size,
            "acoustic_threat_event_count": self.acoustic_threat_event_count,
            "acoustic_peak_decibels": max(decibels) if decibels else None,
            "acoustic_mean_decibels": math.fsum(decibels) / len(decibels) if decibels else None,
            "vision_density_shape": [len(self.vision_herd_density), len(self.vision_herd_density[0])]
            if self.vision_herd_density
            else [0, 0],
            "vision_density_total": math.fsum(densityValues),
            "vision_detection_count": self.vision_detection_count,
            "census_count": self.census_count,
            "minimum_viable_population": self.minimum_viable_population,
            "sensor_metric_means": dict(self.sensor_metric_means),
            "telemetry_fix_count": self.telemetry_fix_count,
            "mean_speed_kmh": self.mean_speed_kmh,
            "mean_heart_rate_bpm": self.mean_heart_rate_bpm,
            "herd_count_days": len(self.herd_count_series),
            "age_class_counts": dict(self.age_class_counts),
            "vision_bbox_count": len(self.vision_bbox_areas),
            "mortality_event_count": len(self.mortality_events),
            "threat_event_count": len(self.threat_events),
            "telemetry_fixes_supplied": len(self.telemetry_fixes),
            "climate_metrics": sorted(self.climate_daily_means),
            "social_group_count": len(self.social_groups),
        }


# Normalized subscores from the Indicator Calculator Node

class NormalizedSubscores(StateModel):
    # Every domain is scaled onto 0.0 (no risk) to 1.0 (maximum risk)
    population: UnitScore
    habitat: UnitScore
    threat: UnitScore
    climate: UnitScore
    genetics: UnitScore
    behavior: UnitScore
    calculated_at: UtcDatetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    # Audit trail of the 0-100 component penalties behind each domain score
    score_components: dict[RiskDomain, dict[NonEmptyText, Optional[float]]] = Field(default_factory=dict)
    # Domains scored with the precautionary fallback because no usable data reached the calculator
    missing_domains: tuple[RiskDomain, ...] = ()

    def asDomainDict(self) -> dict[str, float]:
        return {domain: getattr(self, domain) for domain in RISK_DOMAINS}

    def toDatabaseScale(self) -> dict[str, float]:
        # risk_assessments stores population_subscore etc. on the 0-100 scale
        return {f"{domain}_subscore": getattr(self, domain) * DATABASE_SCORE_SCALE for domain in RISK_DOMAINS}


def calculateConservationRiskIndex(
    subscores: NormalizedSubscores,
    domainWeights: Optional[dict[str, float]] = None,
) -> float:
    weights = domainWeights or DOMAIN_WEIGHTS
    # CRI = sum(w_j * S_j) across the six domains
    riskIndex = clampUnitInterval(math.fsum(weights[domain] * getattr(subscores, domain) for domain in RISK_DOMAINS))
    assert riskIndex is not None
    return riskIndex


# Risk Scoring Engine outputs

class MomentumVector(StateModel):
    window_days: PositiveFloat = MOMENTUM_WINDOW_DAYS
    previous_cri: Optional[UnitScore] = None
    delta_cri: Annotated[float, Field(ge=-1.0, le=1.0)] = 0.0
    # dCRI/dt on the 0.0-1.0 scale per day; positive values mean risk is rising
    slope_per_day: float = 0.0
    # Change in slope versus the previous window, when a second historical assessment exists
    acceleration_per_day: Optional[float] = None
    trajectory: Trajectory = "STABLE"

    @model_validator(mode="after")
    def validateTrajectory(self) -> "MomentumVector":
        expectedSlope = self.delta_cri / self.window_days
        if not math.isclose(self.slope_per_day, expectedSlope, rel_tol=1e-9, abs_tol=1e-12):
            raise ValueError("slope_per_day must equal delta_cri / window_days.")
        if self.previous_cri is None and self.delta_cri != 0.0:
            raise ValueError("delta_cri requires a previous_cri baseline.")
        if self.trajectory != classifyTrajectory(self.slope_per_day):
            raise ValueError(f"trajectory {self.trajectory} does not match slope_per_day {self.slope_per_day}.")
        return self

    @classmethod
    def fromHistory(
        cls,
        currentCri: float,
        previousCri: Optional[float],
        windowDays: float = MOMENTUM_WINDOW_DAYS,
        previousSlopePerDay: Optional[float] = None,
    ) -> "MomentumVector":
        # Without a prior assessment in the window the trajectory defaults to STABLE
        deltaCri = 0.0 if previousCri is None else currentCri - previousCri
        slopePerDay = deltaCri / windowDays
        accelerationPerDay = None
        if previousSlopePerDay is not None:
            accelerationPerDay = (slopePerDay - previousSlopePerDay) / windowDays
        return cls(
            window_days=windowDays,
            previous_cri=previousCri,
            delta_cri=deltaCri,
            slope_per_day=slopePerDay,
            acceleration_per_day=accelerationPerDay,
            trajectory=classifyTrajectory(slopePerDay),
        )


def classifyTrajectory(slopePerDay: float) -> str:
    if slopePerDay > MOMENTUM_STABLE_TOLERANCE:
        return "ACCELERATING"
    if slopePerDay < -MOMENTUM_STABLE_TOLERANCE:
        return "DECAYING"
    return "STABLE"


class RiskMetrics(StateModel):
    conservation_risk_index: UnitScore
    # Inbreeding Penalty Index (0-100) stored in risk_assessments.inbreeding_penalty_index
    inbreeding_penalty_index: Annotated[float, Field(ge=0.0, le=DATABASE_SCORE_SCALE)]
    # Intervention Priority Index (CRI x momentum x feasibility) stored in risk_assessments.intervention_priority_index
    intervention_priority_index: Optional[NonNegativeFloat] = None
    # Intermediate terms behind each index, kept for the narrative justification and audit trail
    inbreeding_components: dict[NonEmptyText, float] = Field(default_factory=dict)
    priority_components: dict[NonEmptyText, float] = Field(default_factory=dict)
    momentum_vector: MomentumVector
    population_trend: Optional[PopulationTrend] = None
    effective_population_size: Optional[NonNegativeFloat] = None
    domain_weights: dict[RiskDomain, NonNegativeFloat] = Field(default_factory=lambda: dict(DOMAIN_WEIGHTS))
    # Deterministic conditional routing switches for the LangGraph edges
    critical_inbreeding_flag: StrictBool = False
    immediate_poaching_threat: StrictBool = False
    scored_at: UtcDatetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @computed_field
    @property
    def escalation_level(self) -> int:
        return escalationLevelForCri(self.conservation_risk_index)

    @computed_field
    @property
    def intervention_priority_tier(self) -> Optional[str]:
        return priorityTierForIndex(self.intervention_priority_index)

    @field_validator("domain_weights")
    @classmethod
    def validateDomainWeights(cls, domainWeights: dict[str, float]) -> dict[str, float]:
        if set(domainWeights) != set(RISK_DOMAINS):
            raise ValueError(f"domain_weights must define exactly these domains: {', '.join(RISK_DOMAINS)}.")
        if abs(math.fsum(domainWeights.values()) - 1.0) > WEIGHT_SUM_TOLERANCE:
            raise ValueError("domain_weights must sum to 1.0.")
        return domainWeights

    @model_validator(mode="after")
    def validateRiskFlags(self) -> "RiskMetrics":
        momentum = self.momentum_vector
        if momentum.previous_cri is not None and not math.isclose(
            momentum.delta_cri,
            self.conservation_risk_index - momentum.previous_cri,
            abs_tol=CRI_CONSISTENCY_TOLERANCE,
        ):
            raise ValueError("momentum_vector.delta_cri must equal the current CRI minus previous_cri.")
        # The 50/500 rule makes the inbreeding flag deterministic whenever N_e is known
        if (
            self.effective_population_size is not None
            and self.effective_population_size < CRITICAL_NE_THRESHOLD
            and not self.critical_inbreeding_flag
        ):
            raise ValueError(
                f"critical_inbreeding_flag must be True when N_e {self.effective_population_size} "
                f"is below {CRITICAL_NE_THRESHOLD}."
            )
        return self


# Explainability & narrative outputs

class NarrativeBlock(StateModel):
    domain: Optional[Literal["population", "habitat", "threat", "climate", "genetics", "behavior", "overall"]] = None
    # LLM prose and the mathematical basis are kept apart so reports can cite the numbers verbatim
    summary_text: NonEmptyText
    mathematical_justification: NonEmptyText
    cited_metrics: dict[NonEmptyText, float] = Field(default_factory=dict)
    generated_by: NonEmptyText
    generated_at: UtcDatetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# Intervention routing directives

class DroneDeployment(CoordinateModel):
    latitude: Latitude
    longitude: Longitude
    altitude_m: Annotated[float, Field(ge=0.0, le=500.0)] = 120.0
    h3_cell: Optional[str] = None
    priority: AlertPriority = "HIGH"
    reason: NonEmptyText

    @field_validator("h3_cell", mode="before")
    @classmethod
    def validateH3Cell(cls, h3Cell: object) -> object:
        return normalizeH3Cell(h3Cell)


class RangerDispatchAlert(CoordinateModel):
    latitude: Latitude
    longitude: Longitude
    priority: AlertPriority = "CRITICAL"
    message: NonEmptyText
    channel: Literal["MQTT", "WEBSOCKET"] = "MQTT"
    source_detection_ids: tuple[PositiveInt, ...] = ()


class InterventionRouting(StateModel):
    escalation_level: Annotated[int, Field(ge=1, le=5)]
    actions: tuple[InterventionAction, ...] = Field(min_length=1)
    drone_deployments: tuple[DroneDeployment, ...] = ()
    ranger_alerts: tuple[RangerDispatchAlert, ...] = ()
    rationale: NonEmptyText
    issued_at: UtcDatetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def validateDirectives(self) -> "InterventionRouting":
        # Dispatch actions and their coordinate payloads must always travel together
        if ("DRONE_DEPLOYMENT" in self.actions) != bool(self.drone_deployments):
            raise ValueError("DRONE_DEPLOYMENT requires drone_deployments, and drone_deployments require the action.")
        if ("RANGER_DISPATCH" in self.actions) != bool(self.ranger_alerts):
            raise ValueError("RANGER_DISPATCH requires ranger_alerts, and ranger_alerts require the action.")
        if len(set(self.actions)) != len(self.actions):
            raise ValueError("actions must not contain duplicates.")
        return self


# Channel reducers

def writeOnce(existing: Any, update: Any) -> Any:
    # Raw aggregation data may be written once; identical re-sends from retried nodes are accepted
    if update is None:
        return existing
    # LangGraph seeds str channels with "" and model channels with no value, so both count as unset
    if existing is None or existing == "":
        return update
    if existing == update:
        return existing
    raise ImmutableStateError("Write-once graph state cannot be overwritten after aggregation.")


def mergeTracingKeys(existing: Optional[TracingKeys], update: Any) -> Optional[TracingKeys]:
    if update is None:
        return existing
    updateKeys = TracingKeys.model_validate(update)
    return updateKeys if existing is None else TracingKeys.model_validate(existing).merge(updateKeys)


def toSequence(update: Any) -> list[Any]:
    if update is None:
        return []
    if isinstance(update, (list, tuple)):
        return list(update)
    return [update]


def appendNarrativeBlocks(
    existing: Optional[tuple[NarrativeBlock, ...]], update: Any
) -> tuple[NarrativeBlock, ...]:
    newBlocks = tuple(NarrativeBlock.model_validate(block) for block in toSequence(update))
    return tuple(existing or ()) + newBlocks


def appendMessages(existing: Optional[tuple[str, ...]], update: Any) -> tuple[str, ...]:
    newMessages = []
    for message in toSequence(update):
        if not isinstance(message, str) or not message.strip():
            raise StateValidationError("Appended state messages must be non-empty strings.")
        newMessages.append(message.strip())
    return tuple(existing or ()) + tuple(newMessages)


# Primary LangGraph state

class GraphState(TypedDict, total=False):
    # Immutable identity and aggregation channels
    run_id: Annotated[str, writeOnce]
    state_schema_version: Annotated[str, writeOnce]
    assessment_scope: Annotated[AssessmentScope, writeOnce]
    raw_indicators: Annotated[RawIndicators, writeOnce]
    # Audit links back to the originating SpatiaLite rows
    tracing_keys: Annotated[TracingKeys, mergeTracingKeys]
    # Latest-value channels recalculated by their owning nodes
    normalized_subscores: NormalizedSubscores
    risk_metrics: RiskMetrics
    intervention_routing: InterventionRouting
    serialization_payload: dict[str, Any]
    # Append-only channels
    narrative_blocks: Annotated[tuple[NarrativeBlock, ...], appendNarrativeBlocks]
    node_history: Annotated[tuple[str, ...], appendMessages]
    pipeline_errors: Annotated[tuple[str, ...], appendMessages]


STATE_FIELD_MODELS: dict[str, type[StateModel]] = {
    "assessment_scope": AssessmentScope,
    "raw_indicators": RawIndicators,
    "tracing_keys": TracingKeys,
    "normalized_subscores": NormalizedSubscores,
    "risk_metrics": RiskMetrics,
    "intervention_routing": InterventionRouting,
}
WRITE_ONCE_FIELDS: frozenset[str] = frozenset({"run_id", "state_schema_version", "assessment_scope", "raw_indicators"})
APPEND_FIELDS: frozenset[str] = frozenset({"narrative_blocks", "node_history", "pipeline_errors"})
GRAPH_STATE_FIELDS: frozenset[str] = frozenset(GraphState.__annotations__)


# State boundary validation

def validateStateUpdate(update: dict[str, Any], currentState: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    # LangGraph stores the first write to a channel without calling its reducer, so every node output is validated here
    if not isinstance(update, dict):
        raise StateValidationError(f"Graph nodes must return a dict update, not {type(update).__name__}.")
    currentState = currentState or {}
    unknownFields = set(update) - GRAPH_STATE_FIELDS
    if unknownFields:
        raise StateValidationError(f"Unknown graph state fields: {', '.join(sorted(unknownFields))}.")

    validatedUpdate: dict[str, Any] = {}
    for fieldName, value in update.items():
        if isinstance(value, Overwrite):
            if fieldName in WRITE_ONCE_FIELDS or fieldName in APPEND_FIELDS:
                raise ImmutableStateError(f"{fieldName} cannot be replaced with Overwrite.")
            value = value.value
        try:
            validatedUpdate[fieldName] = validateStateField(fieldName, value)
        except ValidationError as error:
            raise StateValidationError(f"Invalid {fieldName}: {error}") from error

        existingValue = currentState.get(fieldName)
        if fieldName in WRITE_ONCE_FIELDS and existingValue is not None and existingValue != validatedUpdate[fieldName]:
            raise ImmutableStateError(f"{fieldName} is immutable after it has been populated.")

    mergedState = {**currentState, **validatedUpdate}
    checkStateConsistency(mergedState)
    return validatedUpdate


def validateStateField(fieldName: str, value: Any) -> Any:
    if value is None:
        return None
    if fieldName in STATE_FIELD_MODELS:
        return STATE_FIELD_MODELS[fieldName].model_validate(value)
    if fieldName == "narrative_blocks":
        return appendNarrativeBlocks((), value)
    if fieldName in ("node_history", "pipeline_errors"):
        return appendMessages((), value)
    if fieldName in ("run_id", "state_schema_version"):
        if not isinstance(value, str) or not value.strip():
            raise StateValidationError(f"{fieldName} must be a non-empty string.")
        return value.strip()
    if fieldName == "serialization_payload":
        # Payload must already satisfy the risk_assessments row schema
        RiskAssessmentSchema.model_validate(value)
        return dict(value)
    return value


def checkStateConsistency(state: dict[str, Any]) -> None:
    subscores = state.get("normalized_subscores")
    riskMetrics = state.get("risk_metrics")
    routing = state.get("intervention_routing")
    rawIndicators = state.get("raw_indicators")

    if subscores is not None and riskMetrics is not None:
        expectedCri = calculateConservationRiskIndex(subscores, riskMetrics.domain_weights)
        if not math.isclose(riskMetrics.conservation_risk_index, expectedCri, abs_tol=CRI_CONSISTENCY_TOLERANCE):
            raise StateValidationError(
                f"CRI {riskMetrics.conservation_risk_index} does not equal the weighted subscores {expectedCri}."
            )
    if riskMetrics is not None and routing is not None and routing.escalation_level != riskMetrics.escalation_level:
        raise StateValidationError("intervention_routing.escalation_level must match risk_metrics.escalation_level.")
    if riskMetrics is not None and routing is not None and riskMetrics.immediate_poaching_threat:
        if "RANGER_DISPATCH" not in routing.actions:
            raise StateValidationError("An immediate poaching threat must route a RANGER_DISPATCH directive.")
    if (
        riskMetrics is not None
        and rawIndicators is not None
        and riskMetrics.effective_population_size is None
        and rawIndicators.effective_population_size is not None
    ):
        raise StateValidationError("risk_metrics must carry forward the aggregated effective_population_size.")


def validateGraphState(state: dict[str, Any]) -> dict[str, Any]:
    return validateStateUpdate(state, None)


def createInitialState(
    runID: str,
    assessmentScope: AssessmentScope | dict[str, Any],
    tracingKeys: Optional[TracingKeys | dict[str, Any]] = None,
) -> GraphState:
    initialState: dict[str, Any] = {
        "run_id": runID,
        "state_schema_version": STATE_SCHEMA_VERSION,
        "assessment_scope": assessmentScope,
        "tracing_keys": tracingKeys or TracingKeys(),
        "narrative_blocks": (),
        "node_history": (),
        "pipeline_errors": (),
    }
    return GraphState(**validateGraphState(initialState))


def validatedNode(nodeName: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    # Wraps a LangGraph node so its output is validated and recorded in node_history before the reducers run
    def decorator(nodeFunction: Callable[..., Any]) -> Callable[..., Any]:
        def finalizeUpdate(state: dict[str, Any], update: Optional[dict[str, Any]]) -> dict[str, Any]:
            validatedUpdate = validateStateUpdate(update or {}, state)
            validatedUpdate["node_history"] = appendMessages((), [nodeName]) + tuple(
                validatedUpdate.get("node_history", ())
            )
            return validatedUpdate

        if inspect.iscoroutinefunction(nodeFunction):
            @functools.wraps(nodeFunction)
            async def asyncNode(state: dict[str, Any], *args: Any, **kwargs: Any) -> dict[str, Any]:
                return finalizeUpdate(state, await nodeFunction(state, *args, **kwargs))

            return asyncNode

        @functools.wraps(nodeFunction)
        def syncNode(state: dict[str, Any], *args: Any, **kwargs: Any) -> dict[str, Any]:
            return finalizeUpdate(state, nodeFunction(state, *args, **kwargs))

        return syncNode

    return decorator


# Conditional edge routing

ROUTE_POACHING_RESPONSE: str = "poachingResponse"
ROUTE_GENETIC_RESCUE: str = "geneticRescue"
ROUTE_STANDARD_NARRATIVE: str = "standardNarrative"


def routeOnRiskTriggers(state: dict[str, Any]) -> str:
    riskMetrics = state.get("risk_metrics")
    if riskMetrics is None:
        raise StateValidationError("Risk triggers cannot be routed before risk_metrics is populated.")
    # Life-safety threats outrank genetic risk because rangers must move immediately
    if riskMetrics.immediate_poaching_threat:
        return ROUTE_POACHING_RESPONSE
    if riskMetrics.critical_inbreeding_flag:
        return ROUTE_GENETIC_RESCUE
    return ROUTE_STANDARD_NARRATIVE


# Serialization for asynchronous persistence

def formatNarrativeReport(narrativeBlocks: Iterable[NarrativeBlock]) -> str:
    sections = []
    for block in narrativeBlocks:
        heading = (block.domain or "overall").capitalize()
        sections.append(
            f"[{heading}]\nSummary: {block.summary_text}\nMathematical basis: {block.mathematical_justification}"
        )
    return "\n\n".join(sections)


def buildSerializationPayload(state: dict[str, Any], modelVersion: str = MODEL_VERSION) -> dict[str, Any]:
    missingFields = [
        fieldName
        for fieldName in ("assessment_scope", "normalized_subscores", "risk_metrics")
        if state.get(fieldName) is None
    ]
    if missingFields:
        raise StateValidationError(f"Cannot serialize graph state without: {', '.join(missingFields)}.")
    narrativeBlocks = tuple(state.get("narrative_blocks") or ())
    if not narrativeBlocks:
        raise StateValidationError("Cannot serialize graph state without at least one narrative block.")

    scope: AssessmentScope = state["assessment_scope"]
    subscores: NormalizedSubscores = state["normalized_subscores"]
    riskMetrics: RiskMetrics = state["risk_metrics"]
    rawIndicators: Optional[RawIndicators] = state.get("raw_indicators")
    tracingKeys: Optional[TracingKeys] = state.get("tracing_keys")
    routing: Optional[InterventionRouting] = state.get("intervention_routing")
    momentum = riskMetrics.momentum_vector

    # Everything that does not have a dedicated column is flattened into input_summary JSON
    inputSummary: dict[str, Any] = {
        "run_id": state.get("run_id"),
        "state_schema_version": state.get("state_schema_version", STATE_SCHEMA_VERSION),
        "score_scale": "0-100 columns; graph state uses 0.0-1.0",
        "scientific_name": scope.scientific_name,
        "latitude": scope.latitude,
        "longitude": scope.longitude,
        "tracing_keys": tracingKeys.model_dump(mode="json") if tracingKeys else {"record_ids": {}},
        "raw_indicators": rawIndicators.summary() if rawIndicators else None,
        "normalized_subscores": subscores.asDomainDict(),
        "subscore_components": subscores.model_dump(mode="json")["score_components"],
        "missing_domains": list(subscores.missing_domains),
        "domain_weights": dict(riskMetrics.domain_weights),
        "momentum_vector": momentum.model_dump(mode="json"),
        "intervention_priority_tier": riskMetrics.intervention_priority_tier,
        "inbreeding_components": dict(riskMetrics.inbreeding_components),
        "priority_components": dict(riskMetrics.priority_components),
        "critical_inbreeding_flag": riskMetrics.critical_inbreeding_flag,
        "immediate_poaching_threat": riskMetrics.immediate_poaching_threat,
        "escalation_level": riskMetrics.escalation_level,
        "intervention_routing": routing.model_dump(mode="json") if routing else None,
        "narrative_blocks": [block.model_dump(mode="json") for block in narrativeBlocks],
        "node_history": list(state.get("node_history") or ()),
        "pipeline_errors": list(state.get("pipeline_errors") or ()),
    }

    payload: dict[str, Any] = {
        "species_id": scope.species_id,
        "h3_cell": scope.h3_cell,
        "period_start": formatUtcTimestamp(scope.period_start),
        "period_end": formatUtcTimestamp(scope.period_end),
        **subscores.toDatabaseScale(),
        "conservation_risk_index": riskMetrics.conservation_risk_index * DATABASE_SCORE_SCALE,
        "inbreeding_penalty_index": riskMetrics.inbreeding_penalty_index,
        "intervention_priority_index": riskMetrics.intervention_priority_index,
        "risk_momentum_per_day": momentum.slope_per_day * DATABASE_SCORE_SCALE,
        "momentum_window_days": momentum.window_days,
        "population_trend": riskMetrics.population_trend,
        "effective_population_size": riskMetrics.effective_population_size,
        "critical_inbreeding_risk": riskMetrics.critical_inbreeding_flag,
        "input_summary": inputSummary,
        "narrative_report": formatNarrativeReport(narrativeBlocks),
        "model_version": modelVersion,
        "generated_at": formatUtcTimestamp(datetime.now(timezone.utc)),
    }
    # Validate against the ORM schema so the payload is guaranteed to fit the STRICT table
    RiskAssessmentSchema.model_validate(payload)
    return payload


def buildPersistenceStatement(state: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
    # Returns (sql, parameters) for dbManager.executeWrite
    payload = state.get("serialization_payload") or buildSerializationPayload(state)
    return RiskAssessment.buildInsertStatement(payload)