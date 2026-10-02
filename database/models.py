import asyncio
from datetime import date, datetime, timezone
import json
import math
import os
import re
import struct
from typing import Any, ClassVar, Iterable, Literal, Optional
from pydantic import (
    AliasChoices,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    StringConstraints,
    field_validator,
    model_validator,
)
from sqlalchemy import (
    Boolean,
    Column,
    Computed,
    FetchedValue,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    Select,
    Text,
    TypeDecorator,
    event,
    func,
    inspect,
    select,
)
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, joinedload, mapped_column, relationship
from typing_extensions import Annotated
import yaml

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

DATABASE_CONFIG: dict = config.get("database", {})
DB_FILE: str = os.path.expanduser(config["storage"]["databasePath"])
SPATIALITE_EXT: str = config["storage"]["spatialiteExtension"]
BUSY_TIMEOUT_MS: int = int(DATABASE_CONFIG.get("busyTimeoutMs", 5000))

if BUSY_TIMEOUT_MS < 0:
    raise ValueError("Database busyTimeoutMs must be non-negative.")

# Floating-point slack absorbed when probabilities are computed as 1.0000001 or -0.0000001
UNIT_INTERVAL_TOLERANCE: float = 1e-6
MAC_ADDRESS_PATTERN = re.compile(r"^[0-9A-F]{2}(:[0-9A-F]{2}){5}$")
H3_CELL_PATTERN = re.compile(r"^[0-9a-f]{15}$")
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SQLITE_DIALECT = sqlite.dialect()

# SpatiaLite BLOB geometry class types and ISO WKB point types mapped to (has Z, has M)
SPATIALITE_POINT_TYPES: dict[int, tuple[bool, bool]] = {
    1: (False, False),
    1001: (True, False),
    2001: (False, True),
    3001: (True, True),
}
EWKB_Z_FLAG: int = 0x80000000
EWKB_M_FLAG: int = 0x40000000
EWKB_SRID_FLAG: int = 0x20000000


def toUtcDatetime(value: object) -> datetime:
    if isinstance(value, datetime):
        parsedTime = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        # Numeric timestamps are Unix epoch seconds
        return datetime.fromtimestamp(float(value), timezone.utc)
    elif isinstance(value, str):
        timestampText = value.strip()
        if timestampText.endswith(("Z", "z")):
            timestampText = timestampText[:-1] + "+00:00"
        try:
            parsedTime = datetime.fromisoformat(timestampText)
        except ValueError as error:
            raise ValueError(f"Invalid ISO 8601 timestamp: {value!r}.") from error
    else:
        raise ValueError(f"Unsupported timestamp type: {type(value).__name__}.")

    # Naive sensor clocks are treated as UTC; offset-aware inputs are converted to UTC
    if parsedTime.tzinfo is None or parsedTime.utcoffset() is None:
        return parsedTime.replace(tzinfo=timezone.utc)
    return parsedTime.astimezone(timezone.utc)


def formatUtcTimestamp(value: object) -> str:
    return toUtcDatetime(value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def toUtcDate(value: object) -> date:
    if isinstance(value, datetime):
        return toUtcDatetime(value).date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        dateText = value.strip()
        if ISO_DATE_PATTERN.match(dateText):
            try:
                return date.fromisoformat(dateText)
            except ValueError as error:
                raise ValueError(f"Invalid calendar date: {value!r}.") from error
        return toUtcDatetime(dateText).date()
    raise ValueError(f"Unsupported date type: {type(value).__name__}.")


def clampUnitInterval(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if -UNIT_INTERVAL_TOLERANCE <= value < 0.0:
        return 0.0
    if 1.0 < value <= 1.0 + UNIT_INTERVAL_TOLERANCE:
        return 1.0
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"Value {value} must be between 0.0 and 1.0.")
    return value


def toPythonValue(value: object) -> object:
    # NumPy scalars and arrays from inference modules are converted before strict validation
    if hasattr(value, "dtype") and hasattr(value, "tolist"):
        return value.tolist()
    if isinstance(value, dict):
        return {key: toPythonValue(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(toPythonValue(item) for item in value)
    return value


def parseGeometryBlob(blob: Optional[bytes]) -> Optional[dict[str, Any]]:
    if blob is None:
        return None
    data = bytes(blob)

    # SpatiaLite internal BLOB: 0x00, endian, SRID, MBR, 0x7C, class type, coordinates, 0xFE
    if len(data) >= 60 and data[0] == 0x00 and data[38] == 0x7C and data[-1] == 0xFE:
        if data[1] not in (0x00, 0x01):
            raise ValueError("Invalid SpatiaLite geometry byte order.")
        byteOrder = "<" if data[1] == 0x01 else ">"
        classType = struct.unpack_from(f"{byteOrder}i", data, 39)[0]
        if classType not in SPATIALITE_POINT_TYPES:
            raise ValueError(f"Unsupported SpatiaLite geometry class {classType}; only points are mapped.")
        hasZ, hasM = SPATIALITE_POINT_TYPES[classType]
        offset = 43
    # Standard ISO or extended (PostGIS-style) WKB point
    elif len(data) >= 21 and data[0] in (0x00, 0x01):
        byteOrder = "<" if data[0] == 0x01 else ">"
        geometryType = struct.unpack_from(f"{byteOrder}I", data, 1)[0]
        offset = 5
        if geometryType & EWKB_SRID_FLAG:
            offset += 4
        baseType = geometryType & 0x0FFFFFFF
        if geometryType & (EWKB_Z_FLAG | EWKB_M_FLAG):
            hasZ, hasM = bool(geometryType & EWKB_Z_FLAG), bool(geometryType & EWKB_M_FLAG)
        elif baseType in SPATIALITE_POINT_TYPES:
            hasZ, hasM = SPATIALITE_POINT_TYPES[baseType]
            baseType = 1
        else:
            raise ValueError(f"Unsupported WKB geometry type {geometryType}.")
        if baseType != 1:
            raise ValueError(f"Unsupported WKB geometry type {geometryType}; only points are mapped.")
    else:
        raise ValueError("Unrecognized geometry BLOB; expected SpatiaLite or WKB point data.")

    dimensionCount = 2 + int(hasZ) + int(hasM)
    if len(data) < offset + 8 * dimensionCount:
        raise ValueError("Truncated point geometry BLOB.")
    coordinates = list(struct.unpack_from(f"{byteOrder}{dimensionCount}d", data, offset))
    if math.isnan(coordinates[0]) or math.isnan(coordinates[1]):
        return None

    # GeoJSON positions are [longitude, latitude, altitude]; measure values are not part of GeoJSON
    return {"type": "Point", "coordinates": coordinates[:3] if hasZ else coordinates[:2]}


# SQLite column types

class UtcTimestamp(TypeDecorator):
    impl = Text
    cache_ok = True

    def process_bind_param(self, value: object, dialect: object) -> Optional[str]:
        return None if value is None else formatUtcTimestamp(value)

    def process_result_value(self, value: object, dialect: object) -> Optional[datetime]:
        return None if value is None else toUtcDatetime(value)


class IsoDate(TypeDecorator):
    impl = Text
    cache_ok = True

    def process_bind_param(self, value: object, dialect: object) -> Optional[str]:
        return None if value is None else toUtcDate(value).isoformat()

    def process_result_value(self, value: object, dialect: object) -> Optional[date]:
        return None if value is None else toUtcDate(value)


class JsonText(TypeDecorator):
    impl = Text
    cache_ok = True

    def process_bind_param(self, value: object, dialect: object) -> Optional[str]:
        if value is None:
            return None
        return json.dumps(toPythonValue(value), separators=(",", ":"), allow_nan=False)

    def process_result_value(self, value: object, dialect: object) -> object:
        return None if value is None else json.loads(str(value))


class PointGeometry(TypeDecorator):
    # Geometry is derived from latitude/longitude/altitude by schema.sql triggers, so it is read-only here
    impl = LargeBinary
    cache_ok = True

    def process_bind_param(self, value: object, dialect: object) -> None:
        if value is not None:
            raise ValueError("Geometry is generated by the database; set latitude, longitude, and altitude instead.")
        return None

    def process_result_value(self, value: object, dialect: object) -> Optional[dict[str, Any]]:
        return parseGeometryBlob(value)


def epochComputed(columnName: str) -> Computed:
    return Computed(
        f"CAST(round((julianday({columnName}) - 2440587.5) * 86400000.0) AS INTEGER)",
        persisted=True,
    )


def jsonObjectColumn(columnName: Optional[str] = None) -> Mapped[dict[str, Any]]:
    # MutableDict marks the row dirty when a nested key is edited in place
    columnType = MutableDict.as_mutable(JsonText())
    if columnName is None:
        return mapped_column(columnType, nullable=False)
    return mapped_column(columnName, columnType, nullable=False)


def geometryColumn() -> Mapped[Optional[dict[str, Any]]]:
    return mapped_column(
        PointGeometry(),
        server_default=FetchedValue(),
        server_onupdate=FetchedValue(),
        info={"readOnly": True},
    )


# Pydantic validation schemas

NonEmptyText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
UtcDatetime = Annotated[datetime, BeforeValidator(toUtcDatetime)]
UtcDate = Annotated[date, BeforeValidator(toUtcDate)]
Subscore = Annotated[float, Field(ge=0.0, le=100.0)]
GeoJsonPoint = Annotated[Optional[dict[str, Any]], Field(default=None, serialization_alias="geometry")]


class RecordSchema(BaseModel):
    # Strict mode mirrors the STRICT tables, so "7.2" is rejected for a REAL column instead of coerced
    model_config = ConfigDict(strict=True, from_attributes=True, extra="forbid", allow_inf_nan=False)

    @model_validator(mode="before")
    @classmethod
    def convertNumpyValues(cls, data: object) -> object:
        if isinstance(data, dict):
            return {key: toPythonValue(value) for key, value in data.items()}
        return data


class CoordinateSchema(RecordSchema):
    @field_validator("latitude", check_fields=False)
    @classmethod
    def validateLatitude(cls, latitude: Optional[float]) -> Optional[float]:
        if latitude is not None and not -90.0 <= latitude <= 90.0:
            raise ValueError(f"Latitude {latitude} is outside [-90, 90].")
        return latitude

    @field_validator("longitude", check_fields=False)
    @classmethod
    def validateLongitude(cls, longitude: Optional[float]) -> Optional[float]:
        if longitude is not None and not -180.0 <= longitude <= 180.0:
            raise ValueError(f"Longitude {longitude} is outside [-180, 180].")
        return longitude

    @field_validator("h3_cell", mode="before", check_fields=False)
    @classmethod
    def validateH3Cell(cls, h3Cell: object) -> object:
        if isinstance(h3Cell, str):
            h3Cell = h3Cell.strip().lower()
            if not H3_CELL_PATTERN.match(h3Cell):
                raise ValueError(f"H3 cell {h3Cell!r} must be a 15-character hexadecimal index.")
        return h3Cell


def requireCoordinatePair(latitude: Optional[float], longitude: Optional[float]) -> None:
    if (latitude is None) != (longitude is None):
        raise ValueError("Latitude and longitude must both be provided or both be omitted.")


class SpeciesSchema(RecordSchema):
    id: Optional[int] = None
    scientific_name: NonEmptyText
    common_name: Optional[str] = None
    kingdom: NonEmptyText = "Animalia"
    phylum: Optional[str] = None
    taxonomic_class: Optional[str] = None
    taxonomic_order: Optional[str] = None
    family: Optional[str] = None
    genus: Optional[str] = None
    iucn_status: Literal["NE", "DD", "LC", "NT", "VU", "EN", "CR", "EW", "EX"] = "NE"
    minimum_viable_population: Optional[PositiveInt] = None
    census_population_estimate: Optional[NonNegativeInt] = None
    critical_ne_threshold: PositiveInt = 50
    viable_ne_threshold: PositiveInt = 500
    generation_time_years: Optional[PositiveFloat] = None
    resting_heart_rate_min_bpm: Optional[PositiveFloat] = None
    resting_heart_rate_max_bpm: Optional[PositiveFloat] = None
    body_temperature_min_c: Optional[float] = None
    body_temperature_max_c: Optional[float] = None
    max_speed_kmh: Optional[PositiveFloat] = None
    max_acceleration_g: Optional[PositiveFloat] = None
    home_range_km2: Optional[PositiveFloat] = None
    juvenile_recruitment_ratio: Optional[NonNegativeFloat] = None
    created_at: Optional[UtcDatetime] = None
    updated_at: Optional[UtcDatetime] = None

    @model_validator(mode="after")
    def validateBaselines(self) -> "SpeciesSchema":
        # 50/500 rule: the short-term inbreeding threshold must sit below the long-term viability threshold
        if self.critical_ne_threshold >= self.viable_ne_threshold:
            raise ValueError("critical_ne_threshold must be lower than viable_ne_threshold.")
        for minimum, maximum, label in (
            (self.resting_heart_rate_min_bpm, self.resting_heart_rate_max_bpm, "resting heart rate"),
            (self.body_temperature_min_c, self.body_temperature_max_c, "body temperature"),
        ):
            if minimum is not None and maximum is not None and minimum > maximum:
                raise ValueError(f"Minimum {label} cannot exceed the maximum.")
        return self


class TelemetrySchema(CoordinateSchema):
    id: Optional[int] = None
    species_id: Optional[int] = None
    animal_id: NonEmptyText
    device_id: NonEmptyText
    recorded_at: UtcDatetime
    recorded_epoch_ms: Optional[int] = None
    latitude: float
    longitude: float
    altitude: Optional[float] = None
    fix_quality: Literal["3D_FIX", "2D_FIX", "ARGOS_LOCATION_CLASS", "UNKNOWN"] = "UNKNOWN"
    satellite_count: Optional[NonNegativeInt] = None
    hdop: Optional[NonNegativeFloat] = None
    speed_kmh: Optional[NonNegativeFloat] = None
    battery_voltage: Optional[NonNegativeFloat] = None
    physiological_metrics: dict[str, Any] = Field(default_factory=dict)
    geom: GeoJsonPoint

    @field_validator("physiological_metrics")
    @classmethod
    def validatePhysiologicalMetrics(cls, metrics: dict[str, Any]) -> dict[str, Any]:
        heartRate = metrics.get("heart_rate_bpm")
        if "heart_rate_bpm" in metrics and (
            isinstance(heartRate, bool) or not isinstance(heartRate, (int, float)) or not heartRate > 0
        ):
            raise ValueError("heart_rate_bpm must be a positive number.")
        if "accelerometer_g" in metrics:
            vector = metrics["accelerometer_g"]
            if not isinstance(vector, list) or len(vector) != 3 or not all(
                isinstance(axis, (int, float)) and not isinstance(axis, bool) and math.isfinite(axis)
                for axis in vector
            ):
                raise ValueError("accelerometer_g must be a list of three finite numbers [x, y, z].")
        return metrics


class SensorReadingSchema(CoordinateSchema):
    id: Optional[int] = None
    sensor_mac: str
    device_id: Optional[str] = None
    recorded_at: UtcDatetime
    recorded_epoch_ms: Optional[int] = None
    h3_cell: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None
    water_ph: Optional[Annotated[float, Field(ge=0.0, le=14.0)]] = None
    dissolved_oxygen_mg_l: Optional[NonNegativeFloat] = None
    water_turbidity_ntu: Optional[NonNegativeFloat] = None
    water_temperature_c: Optional[Annotated[float, Field(ge=-5.0, le=100.0)]] = None
    water_salinity_psu: Optional[NonNegativeFloat] = None
    water_velocity_m_s: Optional[NonNegativeFloat] = None
    electrical_conductivity_us_cm: Optional[NonNegativeFloat] = None
    ambient_temperature_c: Optional[Annotated[float, Field(ge=-90.0, le=70.0)]] = None
    relative_humidity_percent: Optional[Annotated[float, Field(ge=0.0, le=100.0)]] = None
    barometric_pressure_hpa: Optional[Annotated[float, Field(ge=300.0, le=1100.0)]] = None
    nitrate_mg_l: Optional[NonNegativeFloat] = None
    phosphate_mg_l: Optional[NonNegativeFloat] = None
    ammonia_mg_l: Optional[NonNegativeFloat] = None
    pollutant_concentration_level: Optional[NonNegativeFloat] = None
    air_quality_level: Optional[Annotated[float, Field(ge=1.0, le=10.0)]] = None
    quality_flag: Literal["VALID", "SUSPECT", "INVALID"] = "VALID"

    @field_validator("sensor_mac", mode="before")
    @classmethod
    def normalizeMacAddress(cls, sensorMac: object) -> object:
        if isinstance(sensorMac, str):
            sensorMac = sensorMac.strip().upper().replace("-", ":")
            if not MAC_ADDRESS_PATTERN.match(sensorMac):
                raise ValueError(f"Sensor MAC {sensorMac!r} must look like AA:BB:CC:DD:EE:FF.")
        return sensorMac

    @model_validator(mode="after")
    def requireMeasurement(self) -> "SensorReadingSchema":
        measurementFields = [
            name for name in type(self).model_fields
            if name not in SENSOR_READING_CONTEXT_FIELDS
        ]
        if all(getattr(self, name) is None for name in measurementFields):
            raise ValueError("A sensor reading must contain at least one measurement.")
        return self


SENSOR_READING_CONTEXT_FIELDS: frozenset[str] = frozenset({
    "id", "sensor_mac", "device_id", "recorded_at", "recorded_epoch_ms", "h3_cell",
    "latitude", "longitude", "altitude", "quality_flag",
})

class DetectionSchema(CoordinateSchema):
    id: Optional[int] = None
    source_type: Literal["VISION", "ACOUSTIC"]
    source_record_id: Optional[int] = None
    detection_index: NonNegativeInt = 0
    device_id: NonEmptyText
    asset_path: Optional[str] = None
    species_id: Optional[int] = None
    class_id: Optional[NonNegativeInt] = None
    class_name: NonEmptyText
    category: Literal["target_wildlife", "immediate_threat", "other"]
    confidence: float
    detected_at: UtcDatetime
    detected_epoch_ms: Optional[int] = None
    ended_at: Optional[UtcDatetime] = None
    frame_width: Optional[PositiveInt] = None
    frame_height: Optional[PositiveInt] = None
    bbox_left: Optional[float] = None
    bbox_top: Optional[float] = None
    bbox_right: Optional[float] = None
    bbox_bottom: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("detection_metadata", "metadata"),
    )
    geom: GeoJsonPoint

    @field_validator("confidence", "bbox_left", "bbox_top", "bbox_right", "bbox_bottom")
    @classmethod
    def validateUnitInterval(cls, value: Optional[float]) -> Optional[float]:
        return clampUnitInterval(value)

    @model_validator(mode="after")
    def validateDetectionShape(self) -> "DetectionSchema":
        requireCoordinatePair(self.latitude, self.longitude)
        if self.ended_at is not None and self.ended_at < self.detected_at:
            raise ValueError("ended_at cannot be earlier than detected_at.")

        # Vision detections carry a frame-relative box; acoustic events never do
        frameFields = (
            self.frame_width, self.frame_height,
            self.bbox_left, self.bbox_top, self.bbox_right, self.bbox_bottom,
        )
        if self.source_type == "VISION":
            if any(value is None for value in frameFields):
                raise ValueError("VISION detections require frame dimensions and all four bounding box edges.")
            if not (self.bbox_left < self.bbox_right and self.bbox_top < self.bbox_bottom):
                raise ValueError("Bounding box requires left < right and top < bottom.")
        elif any(value is not None for value in frameFields):
            raise ValueError("ACOUSTIC detections cannot contain frame dimensions or bounding boxes.")
        return self


class GeneticsRecordSchema(CoordinateSchema):
    id: Optional[int] = None
    sample_id: NonEmptyText
    species_id: Optional[int] = None
    locus_name: NonEmptyText
    assay_method: Literal["MICROSATELLITE", "SNP", "AMPLICON", "METABARCODING", "UNKNOWN"] = "UNKNOWN"
    collection_date: UtcDate
    h3_cell: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    allele_frequencies: dict[str, float]
    allele_count: PositiveInt
    genotyped_individuals: PositiveInt
    expected_heterozygosity: Optional[Annotated[float, Field(ge=0.0, lt=1.0)]] = None
    observed_heterozygosity: Optional[Annotated[float, Field(ge=0.0, le=1.0)]] = None
    allelic_richness: Optional[Annotated[float, Field(ge=1.0)]] = None
    # N_e stays a non-negative float because LD and temporal estimators return fractional sizes
    effective_population_size: Optional[NonNegativeFloat] = None
    ne_lower_bound: Optional[NonNegativeFloat] = None
    ne_upper_bound: Optional[NonNegativeFloat] = None
    ne_method: Optional[Literal["LINKAGE_DISEQUILIBRIUM", "TEMPORAL_VARIANCE", "HETEROZYGOSITY"]] = None
    created_at: Optional[UtcDatetime] = None

    @field_validator("allele_frequencies")
    @classmethod
    def validateAlleleFrequencies(cls, frequencies: dict[str, float]) -> dict[str, float]:
        if not frequencies:
            raise ValueError("allele_frequencies must contain at least one allele.")
        if any(not 0.0 <= frequency <= 1.0 for frequency in frequencies.values()):
            raise ValueError("Allele frequencies must be between 0 and 1.")
        frequencyTotal = math.fsum(frequencies.values())
        if abs(frequencyTotal - 1.0) > UNIT_INTERVAL_TOLERANCE:
            raise ValueError(f"Allele frequencies must sum to 1 (got {frequencyTotal:.8f}).")
        return frequencies

    @model_validator(mode="after")
    def validateDiversityMetrics(self) -> "GeneticsRecordSchema":
        requireCoordinatePair(self.latitude, self.longitude)
        nonZeroAlleles = sum(1 for frequency in self.allele_frequencies.values() if frequency > 0.0)
        if nonZeroAlleles != self.allele_count:
            raise ValueError(
                f"allele_count {self.allele_count} must equal the {nonZeroAlleles} alleles with non-zero frequency."
            )

        # Monomorphic loci carry no heterozygosity
        if self.allele_count == 1 and self.expected_heterozygosity not in (None, 0.0):
            raise ValueError("Expected heterozygosity must be 0 for a monomorphic locus.")
        if self.allelic_richness is not None and self.allelic_richness > self.allele_count + 1e-9:
            raise ValueError("Allelic richness cannot exceed allele_count.")

        # Confidence bounds must bracket the N_e point estimate
        effectiveSize = self.effective_population_size
        if effectiveSize is not None:
            if self.ne_method is None:
                raise ValueError("ne_method is required when effective_population_size is set.")
            if self.ne_lower_bound is not None and self.ne_lower_bound > effectiveSize:
                raise ValueError("ne_lower_bound cannot exceed effective_population_size.")
            if self.ne_upper_bound is not None and effectiveSize > self.ne_upper_bound:
                raise ValueError("effective_population_size cannot exceed ne_upper_bound.")
        return self


class RiskAssessmentSchema(CoordinateSchema):
    species_id: Optional[int] = None
    h3_cell: str
    period_start: UtcDatetime
    period_end: UtcDatetime
    period_end_epoch_ms: Optional[int] = None
    population_subscore: Optional[Subscore] = None
    habitat_subscore: Optional[Subscore] = None
    threat_subscore: Optional[Subscore] = None
    climate_subscore: Optional[Subscore] = None
    genetics_subscore: Optional[Subscore] = None
    behavior_subscore: Optional[Subscore] = None
    conservation_risk_index: Subscore
    inbreeding_penalty_index: NonNegativeFloat
    intervention_priority_index: Optional[NonNegativeFloat] = None
    risk_momentum_per_day: Optional[float] = None
    momentum_window_days: Optional[PositiveFloat] = None
    population_trend: Optional[Literal["DECLINING", "STABLE", "INCREASING"]] = None
    effective_population_size: Optional[NonNegativeFloat] = None
    critical_inbreeding_risk: bool = False
    escalation_level: Optional[Annotated[int, Field(ge=1, le=5)]] = None
    # Generated from intervention_priority_index by schemaMigrations/0003interventionPriorityIndex.sql
    intervention_priority_tier: Optional[Literal["IMMEDIATE", "HIGH", "ELEVATED", "ROUTINE", "MONITOR"]] = None
    input_summary: dict[str, Any] = Field(default_factory=dict)
    narrative_report: NonEmptyText
    model_version: Optional[str] = None
    generated_at: Optional[UtcDatetime] = None

    @field_validator("critical_inbreeding_risk", mode="before")
    @classmethod
    def convertRiskFlag(cls, flag: object) -> object:
        # SQLite stores booleans as 0/1 integers
        if isinstance(flag, int) and not isinstance(flag, bool) and flag in (0, 1):
            return bool(flag)
        return flag

    @model_validator(mode="after")
    def validatePeriod(self) -> "RiskAssessmentSchema":
        if self.period_end <= self.period_start:
            raise ValueError("period_end must be later than period_start.")
        return self

# Declarative ORM base

READ_ONLY: dict[str, bool] = {"readOnly": True}


class Base(AsyncAttrs, DeclarativeBase):
    validationSchema: ClassVar[type[RecordSchema]]
    apiExcludedFields: ClassVar[frozenset[str]] = frozenset()
    apiDefaultIncludes: ClassVar[tuple[str, ...]] = ()
    requiredParents: ClassVar[tuple[tuple[str, str], ...]] = ()

    def __init__(self, **fields: Any) -> None:
        # Relationship objects are assigned after the column values have been validated in memory
        relationshipNames = inspect(type(self)).relationships.keys()
        relationshipValues = {name: fields.pop(name) for name in list(fields) if name in relationshipNames}
        readOnlyFields = sorted(
            column.name for attributeKey, column in self.columnProperties()
            if self.isReadOnlyColumn(column) and (attributeKey in fields or column.name in fields)
        )
        if readOnlyFields:
            raise ValueError(f"{type(self).__name__} fields {readOnlyFields} are generated by the database.")
        record = self.validationSchema.model_validate(fields)
        self.applySchemaValues(record)
        for name, value in relationshipValues.items():
            setattr(self, name, value)
        self.checkRequiredParents()

    @classmethod
    def columnProperties(cls) -> list[tuple[str, Column]]:
        return [(prop.key, prop.columns[0]) for prop in inspect(cls).column_attrs]

    @staticmethod
    def isReadOnlyColumn(column: Column) -> bool:
        return bool(column.info.get("readOnly")) or column.computed is not None

    def applySchemaValues(self, record: RecordSchema) -> None:
        for attributeKey, column in self.columnProperties():
            if self.isReadOnlyColumn(column):
                continue
            value = getattr(record, column.name)
            # Leave keys and database defaults unset so SQLite or the relationship can populate them
            if value is None and (column.primary_key or column.foreign_keys or column.server_default is not None):
                continue
            setattr(self, attributeKey, dict(value) if isinstance(value, dict) else value)

    def checkRequiredParents(self) -> None:
        for foreignKeyName, relationshipName in self.requiredParents:
            if getattr(self, foreignKeyName) is None and getattr(self, relationshipName) is None:
                raise ValueError(
                    f"{type(self).__name__} requires {foreignKeyName} or a {relationshipName} object."
                )

    def loadedColumnValues(self) -> tuple[dict[str, Any], set[str]]:
        state = inspect(self)
        values: dict[str, Any] = {}
        unloadedColumns: set[str] = set()
        for attributeKey, column in self.columnProperties():
            if attributeKey in state.unloaded:
                unloadedColumns.add(column.name)
            else:
                values[column.name] = getattr(self, attributeKey)
        return values, unloadedColumns

    def validateRecord(self) -> RecordSchema:
        values, _ = self.loadedColumnValues()
        record = self.validationSchema.model_validate(values)
        self.checkRequiredParents()
        return record

    def toApiPayload(
        self,
        include: Optional[Iterable[str]] = None,
        exclude: Iterable[str] = (),
    ) -> dict[str, Any]:
        excludedFields = set(self.apiExcludedFields) | set(exclude)
        values, unloadedColumns = self.loadedColumnValues()
        schemaFields = self.validationSchema.model_fields

        # Generated columns such as geom are expired after a flush and must be refreshed before serializing
        for columnName in unloadedColumns:
            if columnName not in excludedFields or schemaFields[columnName].is_required():
                raise RuntimeError(
                    f"{type(self).__name__}.{columnName} is not loaded; call 'await session.refresh(record)' first."
                )

        record = self.validationSchema.model_validate(values)
        payload = record.model_dump(mode="json", by_alias=True, exclude=excludedFields)

        # Nest related records using their own exclusion filters
        state = inspect(self)
        for relationshipName in (self.apiDefaultIncludes if include is None else include):
            if relationshipName in excludedFields:
                continue
            if relationshipName in state.unloaded:
                raise RuntimeError(
                    f"{type(self).__name__}.{relationshipName} is not loaded; eager load it before serializing."
                )
            related = getattr(self, relationshipName)
            if related is None:
                payload[relationshipName] = None
            elif isinstance(related, Base):
                payload[relationshipName] = related.toApiPayload(include=())
            else:
                payload[relationshipName] = [child.toApiPayload(include=()) for child in related]
        return payload

    @classmethod
    def fromDatabaseRow(cls, row: dict[str, Any]) -> RecordSchema:
        # Converts raw DatabaseManager.fetchAll rows (TEXT timestamps, JSON strings, geometry BLOBs)
        values: dict[str, Any] = {}
        for _, column in cls.columnProperties():
            if column.name not in row:
                continue
            value = row[column.name]
            resultProcessor = column.type.result_processor(SQLITE_DIALECT, None)
            values[column.name] = resultProcessor(value) if resultProcessor is not None else value
        return cls.validationSchema.model_validate(values)

    @classmethod
    def buildInsertStatement(cls, record: "RecordSchema | dict[str, Any]") -> tuple[str, tuple[Any, ...]]:
        # Produces SQL and parameters for DatabaseManager.executeWrite so writes still use the batched queue
        if not isinstance(record, cls.validationSchema):
            record = cls.validationSchema.model_validate(record)
        columnNames: list[str] = []
        params: list[Any] = []
        for _, column in cls.columnProperties():
            if cls.isReadOnlyColumn(column):
                continue
            value = getattr(record, column.name)
            if value is None and (column.primary_key or column.server_default is not None):
                continue
            bindProcessor = column.type.bind_processor(SQLITE_DIALECT)
            columnNames.append(column.name)
            params.append(bindProcessor(value) if bindProcessor is not None else value)
        placeholders = ", ".join("?" for _ in columnNames)
        sql = f"INSERT INTO {cls.__tablename__} ({', '.join(columnNames)}) VALUES ({placeholders})"
        return sql, tuple(params)


# "all" minus refresh-expire, so refreshing a species never expires already-loaded child rows in async code
OWNED_CHILD_CASCADE: str = "save-update, merge, expunge, delete, delete-orphan"


def speciesForeignKey() -> ForeignKey:
    return ForeignKey("species.id", onupdate="CASCADE", ondelete="RESTRICT")


# Biological foundation

class Species(Base):
    __tablename__ = "species"
    validationSchema = SpeciesSchema
    apiExcludedFields = frozenset({"id"})

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scientific_name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    common_name: Mapped[Optional[str]] = mapped_column(Text)
    kingdom: Mapped[str] = mapped_column(Text, nullable=False)
    phylum: Mapped[Optional[str]] = mapped_column(Text)
    taxonomic_class: Mapped[Optional[str]] = mapped_column(Text)
    taxonomic_order: Mapped[Optional[str]] = mapped_column(Text)
    family: Mapped[Optional[str]] = mapped_column(Text)
    genus: Mapped[Optional[str]] = mapped_column(Text)
    iucn_status: Mapped[str] = mapped_column(Text, nullable=False)
    minimum_viable_population: Mapped[Optional[int]] = mapped_column(Integer)
    census_population_estimate: Mapped[Optional[int]] = mapped_column(Integer)
    critical_ne_threshold: Mapped[int] = mapped_column(Integer, nullable=False)
    viable_ne_threshold: Mapped[int] = mapped_column(Integer, nullable=False)
    generation_time_years: Mapped[Optional[float]] = mapped_column(Float)
    resting_heart_rate_min_bpm: Mapped[Optional[float]] = mapped_column(Float)
    resting_heart_rate_max_bpm: Mapped[Optional[float]] = mapped_column(Float)
    body_temperature_min_c: Mapped[Optional[float]] = mapped_column(Float)
    body_temperature_max_c: Mapped[Optional[float]] = mapped_column(Float)
    max_speed_kmh: Mapped[Optional[float]] = mapped_column(Float)
    max_acceleration_g: Mapped[Optional[float]] = mapped_column(Float)
    home_range_km2: Mapped[Optional[float]] = mapped_column(Float)
    # Added by schemaMigrations/0002habitatSpatialContext.sql
    juvenile_recruitment_ratio: Mapped[Optional[float]] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(UtcTimestamp(), server_default=FetchedValue(), info=READ_ONLY)
    # Refreshed by the species_updated_at trigger, so it is expired after every UPDATE
    updated_at: Mapped[datetime] = mapped_column(
        UtcTimestamp(), server_default=FetchedValue(), server_onupdate=FetchedValue(), info=READ_ONLY
    )

    # Child collections load on demand; use selectSpeciesWithRecords for joined eager loading
    telemetry: Mapped[list["Telemetry"]] = relationship(
        back_populates="species", cascade=OWNED_CHILD_CASCADE, order_by="Telemetry.recorded_epoch_ms"
    )
    detections: Mapped[list["Detection"]] = relationship(
        back_populates="species", cascade="save-update, merge", passive_deletes=True,
        order_by="Detection.detected_epoch_ms",
    )
    genetics_records: Mapped[list["GeneticsRecord"]] = relationship(
        back_populates="species", cascade=OWNED_CHILD_CASCADE, order_by="GeneticsRecord.collection_date"
    )
    risk_assessments: Mapped[list["RiskAssessment"]] = relationship(
        back_populates="species", cascade=OWNED_CHILD_CASCADE, order_by="RiskAssessment.period_end_epoch_ms"
    )


class Telemetry(Base):
    __tablename__ = "telemetry"
    # Geometry is written by an AFTER INSERT trigger that RETURNING cannot see, so generated values are expired
    __mapper_args__ = {"eager_defaults": False}
    validationSchema = TelemetrySchema
    apiExcludedFields = frozenset({"id", "species_id", "device_id", "recorded_epoch_ms"})
    apiDefaultIncludes = ("species",)
    requiredParents = (("species_id", "species"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    species_id: Mapped[int] = mapped_column(Integer, speciesForeignKey(), nullable=False)
    animal_id: Mapped[str] = mapped_column(Text, nullable=False)
    device_id: Mapped[str] = mapped_column(Text, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(UtcTimestamp(), nullable=False)
    recorded_epoch_ms: Mapped[int] = mapped_column(Integer, epochComputed("recorded_at"), info=READ_ONLY)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    altitude: Mapped[Optional[float]] = mapped_column(Float)
    fix_quality: Mapped[str] = mapped_column(Text, nullable=False)
    satellite_count: Mapped[Optional[int]] = mapped_column(Integer)
    hdop: Mapped[Optional[float]] = mapped_column(Float)
    speed_kmh: Mapped[Optional[float]] = mapped_column(Float)
    battery_voltage: Mapped[Optional[float]] = mapped_column(Float)
    physiological_metrics: Mapped[dict[str, Any]] = jsonObjectColumn()
    geom: Mapped[Optional[dict[str, Any]]] = geometryColumn()

    species: Mapped[Species] = relationship(back_populates="telemetry", lazy="joined", innerjoin=True)


# Environmental monitoring

class SensorReading(Base):
    __tablename__ = "sensor_readings"
    validationSchema = SensorReadingSchema
    apiExcludedFields = frozenset({"id", "sensor_mac", "device_id", "recorded_epoch_ms"})

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sensor_mac: Mapped[str] = mapped_column(Text, nullable=False)
    device_id: Mapped[Optional[str]] = mapped_column(Text)
    recorded_at: Mapped[datetime] = mapped_column(UtcTimestamp(), nullable=False)
    recorded_epoch_ms: Mapped[int] = mapped_column(Integer, epochComputed("recorded_at"), info=READ_ONLY)
    h3_cell: Mapped[str] = mapped_column(Text, nullable=False)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    altitude: Mapped[Optional[float]] = mapped_column(Float)
    water_ph: Mapped[Optional[float]] = mapped_column(Float)
    dissolved_oxygen_mg_l: Mapped[Optional[float]] = mapped_column(Float)
    water_turbidity_ntu: Mapped[Optional[float]] = mapped_column(Float)
    water_temperature_c: Mapped[Optional[float]] = mapped_column(Float)
    water_salinity_psu: Mapped[Optional[float]] = mapped_column(Float)
    water_velocity_m_s: Mapped[Optional[float]] = mapped_column(Float)
    electrical_conductivity_us_cm: Mapped[Optional[float]] = mapped_column(Float)
    ambient_temperature_c: Mapped[Optional[float]] = mapped_column(Float)
    relative_humidity_percent: Mapped[Optional[float]] = mapped_column(Float)
    barometric_pressure_hpa: Mapped[Optional[float]] = mapped_column(Float)
    nitrate_mg_l: Mapped[Optional[float]] = mapped_column(Float)
    phosphate_mg_l: Mapped[Optional[float]] = mapped_column(Float)
    ammonia_mg_l: Mapped[Optional[float]] = mapped_column(Float)
    pollutant_concentration_level: Mapped[Optional[float]] = mapped_column(Float)
    air_quality_level: Mapped[Optional[float]] = mapped_column(Float)
    quality_flag: Mapped[str] = mapped_column(Text, nullable=False)


# Edge AI inference

class Detection(Base):
    __tablename__ = "detections"
    __mapper_args__ = {"eager_defaults": False}
    validationSchema = DetectionSchema
    apiExcludedFields = frozenset({
        "id", "source_record_id", "device_id", "asset_path", "species_id", "detected_epoch_ms",
    })
    apiDefaultIncludes = ("species",)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_record_id: Mapped[Optional[int]] = mapped_column(Integer)
    detection_index: Mapped[int] = mapped_column(Integer, nullable=False)
    device_id: Mapped[str] = mapped_column(Text, nullable=False)
    asset_path: Mapped[Optional[str]] = mapped_column(Text)
    species_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("species.id", onupdate="CASCADE", ondelete="SET NULL")
    )
    class_id: Mapped[Optional[int]] = mapped_column(Integer)
    class_name: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    detected_at: Mapped[datetime] = mapped_column(UtcTimestamp(), nullable=False)
    detected_epoch_ms: Mapped[int] = mapped_column(Integer, epochComputed("detected_at"), info=READ_ONLY)
    ended_at: Mapped[Optional[datetime]] = mapped_column(UtcTimestamp())
    frame_width: Mapped[Optional[int]] = mapped_column(Integer)
    frame_height: Mapped[Optional[int]] = mapped_column(Integer)
    bbox_left: Mapped[Optional[float]] = mapped_column(Float)
    bbox_top: Mapped[Optional[float]] = mapped_column(Float)
    bbox_right: Mapped[Optional[float]] = mapped_column(Float)
    bbox_bottom: Mapped[Optional[float]] = mapped_column(Float)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    altitude: Mapped[Optional[float]] = mapped_column(Float)
    # The column is named metadata, which is reserved on declarative classes
    detection_metadata: Mapped[dict[str, Any]] = jsonObjectColumn("metadata")
    geom: Mapped[Optional[dict[str, Any]]] = geometryColumn()

    species: Mapped[Optional[Species]] = relationship(back_populates="detections", lazy="joined")


# Population genetics

class GeneticsRecord(Base):
    __tablename__ = "genetics_records"
    validationSchema = GeneticsRecordSchema
    apiExcludedFields = frozenset({"id", "species_id"})
    apiDefaultIncludes = ("species",)
    requiredParents = (("species_id", "species"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sample_id: Mapped[str] = mapped_column(Text, nullable=False)
    species_id: Mapped[int] = mapped_column(Integer, speciesForeignKey(), nullable=False)
    locus_name: Mapped[str] = mapped_column(Text, nullable=False)
    assay_method: Mapped[str] = mapped_column(Text, nullable=False)
    collection_date: Mapped[date] = mapped_column(IsoDate(), nullable=False)
    h3_cell: Mapped[str] = mapped_column(Text, nullable=False)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    allele_frequencies: Mapped[dict[str, Any]] = jsonObjectColumn()
    allele_count: Mapped[int] = mapped_column(Integer, nullable=False)
    genotyped_individuals: Mapped[int] = mapped_column(Integer, nullable=False)
    expected_heterozygosity: Mapped[Optional[float]] = mapped_column(Float)
    observed_heterozygosity: Mapped[Optional[float]] = mapped_column(Float)
    allelic_richness: Mapped[Optional[float]] = mapped_column(Float)
    effective_population_size: Mapped[Optional[float]] = mapped_column(Float)
    ne_lower_bound: Mapped[Optional[float]] = mapped_column(Float)
    ne_upper_bound: Mapped[Optional[float]] = mapped_column(Float)
    ne_method: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UtcTimestamp(), server_default=FetchedValue(), info=READ_ONLY)

    species: Mapped[Species] = relationship(back_populates="genetics_records", lazy="joined", innerjoin=True)


# Advanced analytics & risk assessment

class RiskAssessment(Base):
    __tablename__ = "risk_assessments"
    validationSchema = RiskAssessmentSchema
    apiExcludedFields = frozenset({"species_id", "period_end_epoch_ms"})
    apiDefaultIncludes = ("species",)
    requiredParents = (("species_id", "species"),)

    species_id: Mapped[int] = mapped_column(Integer, speciesForeignKey(), primary_key=True)
    h3_cell: Mapped[str] = mapped_column(Text, primary_key=True)
    period_start: Mapped[datetime] = mapped_column(UtcTimestamp(), primary_key=True)
    period_end: Mapped[datetime] = mapped_column(UtcTimestamp(), primary_key=True)
    period_end_epoch_ms: Mapped[int] = mapped_column(Integer, epochComputed("period_end"), info=READ_ONLY)
    population_subscore: Mapped[Optional[float]] = mapped_column(Float)
    habitat_subscore: Mapped[Optional[float]] = mapped_column(Float)
    threat_subscore: Mapped[Optional[float]] = mapped_column(Float)
    climate_subscore: Mapped[Optional[float]] = mapped_column(Float)
    genetics_subscore: Mapped[Optional[float]] = mapped_column(Float)
    behavior_subscore: Mapped[Optional[float]] = mapped_column(Float)
    conservation_risk_index: Mapped[float] = mapped_column(Float, nullable=False)
    inbreeding_penalty_index: Mapped[float] = mapped_column(Float, nullable=False)
    risk_momentum_per_day: Mapped[Optional[float]] = mapped_column(Float)
    momentum_window_days: Mapped[Optional[float]] = mapped_column(Float)
    population_trend: Mapped[Optional[str]] = mapped_column(Text)
    effective_population_size: Mapped[Optional[float]] = mapped_column(Float)
    critical_inbreeding_risk: Mapped[bool] = mapped_column(Boolean(create_constraint=False), nullable=False)
    escalation_level: Mapped[int] = mapped_column(
        Integer,
        Computed(
            "CASE WHEN conservation_risk_index < 20.0 THEN 1 WHEN conservation_risk_index < 40.0 THEN 2 "
            "WHEN conservation_risk_index < 60.0 THEN 3 WHEN conservation_risk_index <= 80.0 THEN 4 ELSE 5 END",
            persisted=True,
        ),
        info=READ_ONLY,
    )
    input_summary: Mapped[dict[str, Any]] = jsonObjectColumn()
    narrative_report: Mapped[str] = mapped_column(Text, nullable=False)
    model_version: Mapped[Optional[str]] = mapped_column(Text)
    generated_at: Mapped[datetime] = mapped_column(UtcTimestamp(), server_default=FetchedValue())
    # Added by schemaMigrations/0003interventionPriorityIndex.sql
    intervention_priority_index: Mapped[Optional[float]] = mapped_column(Float)
    intervention_priority_tier: Mapped[Optional[str]] = mapped_column(
        Text,
        Computed(
            "CASE WHEN intervention_priority_index IS NULL THEN NULL "
            "WHEN intervention_priority_index >= 80.0 THEN 'IMMEDIATE' "
            "WHEN intervention_priority_index >= 60.0 THEN 'HIGH' "
            "WHEN intervention_priority_index >= 40.0 THEN 'ELEVATED' "
            "WHEN intervention_priority_index >= 20.0 THEN 'ROUTINE' ELSE 'MONITOR' END",
            persisted=False,
        ),
        info=READ_ONLY,
    )

    species: Mapped[Species] = relationship(back_populates="risk_assessments", lazy="joined", innerjoin=True)


ORM_MODELS: tuple[type[Base], ...] = (Species, Telemetry, SensorReading, Detection, GeneticsRecord, RiskAssessment)

# Validate pending and modified rows in memory before SQLite constraints run
@event.listens_for(Session, "before_flush")
def validateBeforeFlush(session: Session, flushContext: object, instances: object) -> None:
    for record in list(session.new) + list(session.dirty):
        if isinstance(record, Base) and record not in session.deleted:
            record.validateRecord()


# Engine and session factory

def createDatabaseEngine(
    databasePath: str = DB_FILE,
    spatialiteExtension: str = SPATIALITE_EXT,
    echo: bool = False,
) -> AsyncEngine:
    resolvedPath = os.path.abspath(os.path.expanduser(databasePath))
    os.makedirs(os.path.dirname(resolvedPath), exist_ok=True)
    engine = create_async_engine(f"sqlite+aiosqlite:///{resolvedPath}", echo=echo)

    # Every pooled connection needs SpatiaLite for geometry triggers and the same pragmas as DatabaseManager
    @event.listens_for(engine.sync_engine, "connect")
    def configureConnection(dbapiConnection: Any, connectionRecord: object) -> None:
        dbapiConnection.run_async(lambda connection: connection.enable_load_extension(True))
        cursor = dbapiConnection.cursor()
        try:
            cursor.execute("SELECT load_extension(?)", (spatialiteExtension,))
            cursor.execute("PRAGMA foreign_keys = ON")
            cursor.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
            cursor.execute("PRAGMA synchronous = NORMAL")
        finally:
            cursor.close()
        dbapiConnection.run_async(lambda connection: connection.enable_load_extension(False))

    return engine


def createSessionFactory(engine: AsyncEngine) -> async_sessionmaker:
    # Keep objects usable after commit; generated columns still need an explicit refresh
    return async_sessionmaker(engine, expire_on_commit=False)


def selectSpeciesWithRecords(scientificName: str, *relationshipNames: str) -> Select:
    # Collection joins duplicate parent rows, so call .unique() on the result
    loadOptions = [joinedload(getattr(Species, name)) for name in relationshipNames]
    return select(Species).where(Species.scientific_name == scientificName).options(*loadOptions)


async def verifyMappings(engine: AsyncEngine) -> dict[str, dict[str, list[str]]]:
    # table_xinfo includes generated columns, unlike table_info
    mappingDrift: dict[str, dict[str, list[str]]] = {}
    async with engine.connect() as connection:
        for model in ORM_MODELS:
            result = await connection.exec_driver_sql(f"PRAGMA table_xinfo({model.__tablename__})")
            databaseColumns = {row[1] for row in result.fetchall()}
            mappedColumns = {column.name for _, column in model.columnProperties()}
            missingColumns = sorted(mappedColumns - databaseColumns)
            unmappedColumns = sorted(databaseColumns - mappedColumns)
            if missingColumns or unmappedColumns:
                mappingDrift[model.__tablename__] = {"missing": missingColumns, "unmapped": unmappedColumns}
    return mappingDrift


async def main() -> None:
    engine = createDatabaseEngine()
    try:
        mappingDrift = await verifyMappings(engine)
        print(json.dumps({"mappingDrift": mappingDrift}, indent=2))

        # Row counts confirm each mapped table is queryable through the ORM
        sessionFactory = createSessionFactory(engine)
        async with sessionFactory() as session:
            rowCounts = {}
            for model in ORM_MODELS:
                if model.__tablename__ in mappingDrift:
                    continue
                rowCounts[model.__tablename__] = await session.scalar(select(func.count()).select_from(model))
        print(json.dumps({"rowCounts": rowCounts}, indent=2))
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())