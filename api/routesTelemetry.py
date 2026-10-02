import asyncio
import base64
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Body, FastAPI, Query, Request
from fastapi.responses import StreamingResponse
import h3
import httpx
import itertools
import json
import logging
import math
import os
from pydantic import (
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
from starlette.exceptions import HTTPException
import sys
import time
from typing import Annotated, Any, AsyncIterator, Awaitable, Callable, ClassVar, Literal, Optional, Union
import uuid
import yaml

# Allow "python api/routesTelemetry.py" as well as loading through api/main.py
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from aiEngine.graphWorkflow import runAssessment  # noqa: E402
from database.models import H3_CELL_PATTERN, MAC_ADDRESS_PATTERN, formatUtcTimestamp, toUtcDatetime  # noqa: E402

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

TELEMETRY_CONFIG: dict = config.get("telemetryApi", {}) or {}
H3_RESOLUTION: int = int((config.get("edna", {}) or {}).get("h3Resolution", 7))

# Define ingestion settings
INGEST_QUEUE_SIZE: int = int(TELEMETRY_CONFIG.get("ingestQueueSize", 5000))
INGEST_WORKERS: int = int(TELEMETRY_CONFIG.get("ingestWorkers", 1))
INGEST_BATCH_MAX: int = int(TELEMETRY_CONFIG.get("ingestBatchMax", 250))
MAX_PAYLOADS_PER_REQUEST: int = int(TELEMETRY_CONFIG.get("maxPayloadsPerRequest", 500))
FUTURE_TOLERANCE_SEC: float = float(TELEMETRY_CONFIG.get("futureToleranceSec", 300))
SHUTDOWN_DRAIN_TIMEOUT_SEC: float = float(TELEMETRY_CONFIG.get("shutdownDrainTimeoutSec", 15.0))

# Define threat fast-path settings
CRITICAL_CATEGORIES: frozenset[str] = frozenset(TELEMETRY_CONFIG.get("criticalCategories", ["immediate_threat"]) or [])
THREAT_CONFIDENCE_MIN: float = float(TELEMETRY_CONFIG.get("threatConfidenceMin", 0.5))
THREAT_WEBHOOK_URL: str = str(TELEMETRY_CONFIG.get("threatWebhookUrl", "") or "").strip()
THREAT_WEBHOOK_TIMEOUT_SEC: float = float(TELEMETRY_CONFIG.get("threatWebhookTimeoutSec", 5.0))
WAKE_AI_ENGINE: bool = bool(TELEMETRY_CONFIG.get("wakeAiEngine", True))
THREAT_WAKE_COOLDOWN_SEC: float = float(TELEMETRY_CONFIG.get("threatWakeCooldownSec", 300))
ASSESSMENT_LOOKBACK_DAYS: float = float(TELEMETRY_CONFIG.get("assessmentLookbackDays", 30))
DEFAULT_SPECIES_ID: Optional[int] = (
    int(TELEMETRY_CONFIG["defaultSpeciesID"]) if TELEMETRY_CONFIG.get("defaultSpeciesID") is not None else None
)
# How long a woken assessment waits for the triggering row to be committed before reading the database
ASSESSMENT_STORE_WAIT_SEC: float = 10.0

# Define retrieval, streaming and statistics settings
DEFAULT_PAGE_SIZE: int = int(TELEMETRY_CONFIG.get("defaultPageSize", 200))
MAX_PAGE_SIZE: int = int(TELEMETRY_CONFIG.get("maxPageSize", 1000))
STREAM_HEARTBEAT_SEC: float = float(TELEMETRY_CONFIG.get("streamHeartbeatSec", 15))
STREAM_CLIENT_QUEUE_SIZE: int = int(TELEMETRY_CONFIG.get("streamClientQueueSize", 100))
MAX_STREAM_CLIENTS: int = int(TELEMETRY_CONFIG.get("maxStreamClients", 20))
STREAM_RETRY_MS: int = int(TELEMETRY_CONFIG.get("streamRetryMs", 5000))
STATS_CACHE_TTL_SEC: float = float(TELEMETRY_CONFIG.get("statsCacheTtlSec", 300))
STATS_CACHE_MAX_ENTRIES: int = int(TELEMETRY_CONFIG.get("statsCacheMaxEntries", 128))
STATS_MAX_BUCKETS: int = int(TELEMETRY_CONFIG.get("statsMaxBuckets", 2000))
STATS_DEFAULT_LOOKBACK_DAYS: float = float(TELEMETRY_CONFIG.get("statsDefaultLookbackDays", 7))
BUCKET_SIZES_MS: dict[str, int] = {"hour": 3_600_000, "day": 86_400_000}

if min(INGEST_QUEUE_SIZE, INGEST_WORKERS, INGEST_BATCH_MAX, MAX_PAYLOADS_PER_REQUEST) <= 0:
    raise ValueError("telemetryApi queue, worker, batch and request sizes must be positive.")
if not 0 < DEFAULT_PAGE_SIZE <= MAX_PAGE_SIZE:
    raise ValueError("telemetryApi defaultPageSize must be positive and no larger than maxPageSize.")
if STREAM_HEARTBEAT_SEC <= 0 or STREAM_CLIENT_QUEUE_SIZE <= 0 or MAX_STREAM_CLIENTS <= 0:
    raise ValueError("telemetryApi stream heartbeat, queue size and client limit must be positive.")
if STATS_CACHE_TTL_SEC < 0 or STATS_CACHE_MAX_ENTRIES <= 0 or STATS_MAX_BUCKETS <= 0:
    raise ValueError("telemetryApi statistics cache and bucket limits must be positive.")

ReadingType = Literal["gps", "environmental", "acoustic", "vision"]
BucketSize = Literal["hour", "day"]

# Measurement columns in sensor_readings with the same bounds as the schema CHECK constraints
MEASUREMENT_RANGES: dict[str, tuple[Optional[float], Optional[float]]] = {
    "water_ph": (0.0, 14.0),
    "dissolved_oxygen_mg_l": (0.0, None),
    "water_turbidity_ntu": (0.0, None),
    "water_temperature_c": (-5.0, 100.0),
    "water_salinity_psu": (0.0, None),
    "water_velocity_m_s": (0.0, None),
    "electrical_conductivity_us_cm": (0.0, None),
    "ambient_temperature_c": (-90.0, 70.0),
    "relative_humidity_percent": (0.0, 100.0),
    "barometric_pressure_hpa": (300.0, 1100.0),
    "nitrate_mg_l": (0.0, None),
    "phosphate_mg_l": (0.0, None),
    "ammonia_mg_l": (0.0, None),
    "pollutant_concentration_level": (0.0, None),
    "air_quality_level": (1.0, 10.0),
}
MEASUREMENT_COLUMNS: tuple[str, ...] = tuple(MEASUREMENT_RANGES)

logger = logging.getLogger("icmis.api.telemetry")


# Ingestion payload schemas: reading_type selects the table each payload is stored in

UtcTimestamp = Annotated[datetime, BeforeValidator(toUtcDatetime)]
IdentifierText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
Latitude = Annotated[float, Field(ge=-90.0, le=90.0)]
Longitude = Annotated[float, Field(ge=-180.0, le=180.0)]
UnitInterval = Annotated[float, Field(ge=0.0, le=1.0)]


class IngestBase(BaseModel):
    # NaN and Infinity are valid in Python's JSON parser but would poison every average
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    tableName: ClassVar[str] = ""

    sensor_id: IdentifierText
    timestamp: UtcTimestamp
    latitude: Latitude
    longitude: Longitude
    altitude: Optional[float] = Field(None, ge=-1000.0, le=12000.0)
    threat_detected: bool = False

    @field_validator("timestamp")
    @classmethod
    def rejectFutureTimestamp(cls, value: datetime) -> datetime:
        # A reading far in the future means a drifting sensor clock and would sit at the top of every page
        if value > datetime.now(timezone.utc) + timedelta(seconds=FUTURE_TOLERANCE_SEC):
            raise ValueError(f"timestamp is more than {FUTURE_TOLERANCE_SEC:.0f}s in the future; check the sensor clock.")
        return value

    @property
    def isCritical(self) -> bool:
        return self.threat_detected

    @property
    def speciesID(self) -> Optional[int]:
        return getattr(self, "species_id", None)

    @property
    def h3Cell(self) -> str:
        return h3.latlng_to_cell(self.latitude, self.longitude, H3_RESOLUTION)

    def toStatement(self) -> tuple[str, list[object]]:
        raise NotImplementedError


class GpsIngest(IngestBase):
    tableName: ClassVar[str] = "telemetry"

    reading_type: Literal["gps"]
    species_id: PositiveInt
    animal_id: IdentifierText
    fix_quality: Literal["3D_FIX", "2D_FIX", "ARGOS_LOCATION_CLASS", "UNKNOWN"] = "UNKNOWN"
    satellite_count: Optional[NonNegativeInt] = None
    hdop: Optional[NonNegativeFloat] = None
    speed_kmh: Optional[NonNegativeFloat] = None
    battery_voltage: Optional[NonNegativeFloat] = None
    heart_rate_bpm: Optional[PositiveFloat] = None
    accelerometer_g: Optional[tuple[float, float, float]] = None

    def toStatement(self) -> tuple[str, list[object]]:
        physiologicalMetrics: dict[str, Any] = {}
        if self.heart_rate_bpm is not None:
            physiologicalMetrics["heart_rate_bpm"] = self.heart_rate_bpm
        if self.accelerometer_g is not None:
            physiologicalMetrics["accelerometer_g"] = list(self.accelerometer_g)
        # geom is left NULL so the telemetry_geom_insert trigger builds the POINT Z
        return (
            "INSERT INTO telemetry (species_id, animal_id, device_id, recorded_at, latitude, longitude, altitude, "
            "fix_quality, satellite_count, hdop, speed_kmh, battery_voltage, physiological_metrics) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING;",
            [
                self.species_id, self.animal_id, self.sensor_id, formatUtcTimestamp(self.timestamp),
                self.latitude, self.longitude, self.altitude, self.fix_quality, self.satellite_count,
                self.hdop, self.speed_kmh, self.battery_voltage, json.dumps(physiologicalMetrics),
            ],
        )


class EnvironmentalIngest(IngestBase):
    tableName: ClassVar[str] = "sensor_readings"

    reading_type: Literal["environmental"]
    sensor_mac: str
    h3_cell: Optional[str] = None
    measurements: dict[str, float] = Field(min_length=1)
    quality_flag: Literal["VALID", "SUSPECT", "INVALID"] = "VALID"

    @field_validator("sensor_mac", mode="before")
    @classmethod
    def normalizeMacAddress(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        macAddress = value.strip().upper().replace("-", ":")
        if not MAC_ADDRESS_PATTERN.match(macAddress):
            raise ValueError("sensor_mac must be a MAC address like AA:BB:CC:DD:EE:FF.")
        return macAddress

    @field_validator("h3_cell", mode="before")
    @classmethod
    def normalizeH3Cell(cls, value: object) -> object:
        if value is None or not isinstance(value, str):
            return value
        h3Cell = value.strip().lower()
        if not H3_CELL_PATTERN.match(h3Cell) or not h3.is_valid_cell(h3Cell):
            raise ValueError("h3_cell must be a valid 15-character H3 cell index.")
        return h3Cell

    @field_validator("measurements")
    @classmethod
    def checkMeasurements(cls, measurements: dict[str, float]) -> dict[str, float]:
        unknownNames = sorted(set(measurements) - set(MEASUREMENT_COLUMNS))
        if unknownNames:
            raise ValueError(f"Unknown measurements {unknownNames}; allowed: {list(MEASUREMENT_COLUMNS)}.")
        for name, value in measurements.items():
            lower, upper = MEASUREMENT_RANGES[name]
            if (lower is not None and value < lower) or (upper is not None and value > upper):
                raise ValueError(f"{name}={value} is outside the physical range [{lower}, {upper}].")
        return measurements

    @model_validator(mode="after")
    def deriveH3Cell(self) -> "EnvironmentalIngest":
        # Sensors that do not know their grid cell are binned at the same resolution as the eDNA samples
        if self.h3_cell is None:
            self.h3_cell = h3.latlng_to_cell(self.latitude, self.longitude, H3_RESOLUTION)
        return self

    @property
    def h3Cell(self) -> str:
        return self.h3_cell or super().h3Cell

    def toStatement(self) -> tuple[str, list[object]]:
        measurementValues = [self.measurements.get(name) for name in MEASUREMENT_COLUMNS]
        return (
            "INSERT INTO sensor_readings (sensor_mac, device_id, recorded_at, h3_cell, latitude, longitude, altitude, "
            f"{', '.join(MEASUREMENT_COLUMNS)}, quality_flag) "
            f"VALUES ({', '.join('?' * (8 + len(MEASUREMENT_COLUMNS)))}) ON CONFLICT DO NOTHING;",
            [
                self.sensor_mac, self.sensor_id, formatUtcTimestamp(self.timestamp), self.h3_cell,
                self.latitude, self.longitude, self.altitude, *measurementValues, self.quality_flag,
            ],
        )


class DetectionIngestBase(IngestBase):
    tableName: ClassVar[str] = "detections"
    sourceType: ClassVar[str] = ""

    class_name: IdentifierText
    category: Literal["target_wildlife", "immediate_threat", "other"]
    confidence: UnitInterval
    species_id: Optional[PositiveInt] = None
    class_id: Optional[NonNegativeInt] = None
    # Row ID in camera_ingestion / acoustic_events; with detection_index it de-duplicates re-sent events
    source_record_id: Optional[PositiveInt] = None
    detection_index: NonNegativeInt = 0
    ended_at: Optional[UtcTimestamp] = None
    asset_path: Optional[str] = Field(None, max_length=1024)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def checkEventWindow(self) -> "DetectionIngestBase":
        if self.ended_at is not None and self.ended_at < self.timestamp:
            raise ValueError("ended_at must not be earlier than timestamp.")
        return self

    @property
    def isCritical(self) -> bool:
        return self.threat_detected or (self.category in CRITICAL_CATEGORIES and self.confidence >= THREAT_CONFIDENCE_MIN)

    def getFrameValues(self) -> list[object]:
        return [None] * 6

    def toStatement(self) -> tuple[str, list[object]]:
        return (
            "INSERT INTO detections (source_type, source_record_id, detection_index, device_id, asset_path, species_id, "
            "class_id, class_name, category, confidence, detected_at, ended_at, frame_width, frame_height, "
            "bbox_left, bbox_top, bbox_right, bbox_bottom, latitude, longitude, altitude, metadata) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING;",
            [
                self.sourceType, self.source_record_id, self.detection_index, self.sensor_id, self.asset_path,
                self.species_id, self.class_id, self.class_name, self.category, self.confidence,
                formatUtcTimestamp(self.timestamp),
                formatUtcTimestamp(self.ended_at) if self.ended_at is not None else None,
                *self.getFrameValues(), self.latitude, self.longitude, self.altitude,
                json.dumps(self.metadata, default=str),
            ],
        )


class AcousticIngest(DetectionIngestBase):
    sourceType: ClassVar[str] = "ACOUSTIC"

    reading_type: Literal["acoustic"]


class VisionIngest(DetectionIngestBase):
    sourceType: ClassVar[str] = "VISION"

    reading_type: Literal["vision"]
    frame_width: PositiveInt
    frame_height: PositiveInt
    # Frame-relative box edges in [0, 1], matching the detections CHECK constraints
    bbox_left: UnitInterval
    bbox_top: UnitInterval
    bbox_right: UnitInterval
    bbox_bottom: UnitInterval

    @model_validator(mode="after")
    def checkBoundingBox(self) -> "VisionIngest":
        if self.bbox_left >= self.bbox_right or self.bbox_top >= self.bbox_bottom:
            raise ValueError("Bounding box needs bbox_left < bbox_right and bbox_top < bbox_bottom.")
        return self

    def getFrameValues(self) -> list[object]:
        return [self.frame_width, self.frame_height, self.bbox_left, self.bbox_top, self.bbox_right, self.bbox_bottom]


IngestPayload = Annotated[
    Union[GpsIngest, EnvironmentalIngest, AcousticIngest, VisionIngest],
    Field(discriminator="reading_type"),
]


@dataclass(eq=False)
class IngestItem:
    ingestID: str
    payload: IngestBase
    receivedAt: datetime
    # Resolved with "stored", "duplicate" or "failed"; never an exception, so nothing is left unretrieved
    storedFuture: asyncio.Future


# Pub/sub broker: /ingest publishes, every /stream client owns a bounded queue

class TelemetryBroadcaster:
    def __init__(self, clientQueueSize: int = STREAM_CLIENT_QUEUE_SIZE, maxClients: int = MAX_STREAM_CLIENTS) -> None:
        self.clientQueueSize = clientQueueSize
        self.maxClients = maxClients
        self.subscribers: set[asyncio.Queue] = set()
        self.eventSequence = itertools.count(1)
        self.droppedEvents = 0
        self.isClosed = False

    def subscribe(self) -> asyncio.Queue:
        if self.isClosed:
            raise HTTPException(status_code=503, detail="Telemetry stream is shutting down.")
        if len(self.subscribers) >= self.maxClients:
            raise HTTPException(status_code=503, detail=f"Stream client limit ({self.maxClients}) reached; try again later.")
        clientQueue: asyncio.Queue = asyncio.Queue(maxsize=self.clientQueueSize)
        self.subscribers.add(clientQueue)
        return clientQueue

    def unsubscribe(self, clientQueue: asyncio.Queue) -> None:
        self.subscribers.discard(clientQueue)

    @staticmethod
    def offer(clientQueue: asyncio.Queue, event: Optional[dict[str, Any]]) -> bool:
        # A slow dashboard loses its oldest event instead of stalling ingestion or growing without bound
        dropped = False
        if clientQueue.full():
            clientQueue.get_nowait()
            dropped = True
        clientQueue.put_nowait(event)
        return dropped

    def publish(self, eventType: str, data: dict[str, Any]) -> int:
        if self.isClosed:
            return 0
        event = {"id": next(self.eventSequence), "event": eventType, "data": data}
        for clientQueue in list(self.subscribers):
            if self.offer(clientQueue, event):
                self.droppedEvents += 1
        return len(self.subscribers)

    def close(self) -> None:
        self.isClosed = True
        # None tells every open stream to finish so uvicorn can shut down without waiting on them
        for clientQueue in list(self.subscribers):
            self.offer(clientQueue, None)


def formatServerSentEvent(event: dict[str, Any]) -> str:
    data = json.dumps(event["data"], separators=(",", ":"), default=str)
    return f"id: {event['id']}\nevent: {event['event']}\ndata: {data}\n\n"


# Async TTL cache: functools.lru_cache cannot await coroutines or expire entries

class AsyncTtlCache:
    def __init__(self, ttlSec: float = STATS_CACHE_TTL_SEC, maxEntries: int = STATS_CACHE_MAX_ENTRIES) -> None:
        self.ttlSec = ttlSec
        self.maxEntries = maxEntries
        self.entries: OrderedDict[tuple, tuple[float, Any]] = OrderedDict()
        self.inFlight: dict[tuple, asyncio.Task] = {}
        self.hits = 0
        self.misses = 0

    async def getOrCompute(self, key: tuple, factory: Callable[[], Awaitable[Any]]) -> tuple[Any, bool]:
        entry = self.entries.get(key)
        if entry is not None and entry[0] > time.monotonic():
            self.entries.move_to_end(key)
            self.hits += 1
            return entry[1], True
        task = self.inFlight.get(key)
        if task is not None:
            # Single-flight: dashboards opened together share one aggregation instead of each running it
            self.hits += 1
            return await asyncio.shield(task), True
        self.misses += 1
        task = asyncio.create_task(factory())
        self.inFlight[key] = task
        task.add_done_callback(lambda finishedTask: self.storeResult(key, finishedTask))
        # shield keeps the shared computation alive if the first caller disconnects
        return await asyncio.shield(task), False

    def storeResult(self, key: tuple, task: asyncio.Task) -> None:
        self.inFlight.pop(key, None)
        if task.cancelled() or task.exception() is not None or self.ttlSec <= 0:
            return
        self.entries[key] = (time.monotonic() + self.ttlSec, task.result())
        self.entries.move_to_end(key)
        while len(self.entries) > self.maxEntries:
            self.entries.popitem(last=False)

    def describe(self) -> dict[str, Any]:
        now = time.monotonic()
        return {
            "entries": sum(1 for expiresAt, _ in self.entries.values() if expiresAt > now),
            "hits": self.hits,
            "misses": self.misses,
            "ttl_sec": self.ttlSec,
        }


# Telemetry service: ingest queue, workers, threat dispatch and the shared broadcaster/cache

class TelemetryService:
    def __init__(self, app: FastAPI) -> None:
        self.app = app
        self.ingestQueue: asyncio.Queue[IngestItem] = asyncio.Queue(maxsize=INGEST_QUEUE_SIZE)
        self.broadcaster = TelemetryBroadcaster()
        self.statsCache = AsyncTtlCache()
        self.workerTasks: list[asyncio.Task] = []
        self.backgroundTasks: set[asyncio.Task] = set()
        self.assessmentSemaphore = asyncio.Semaphore(1)
        self.lastWakeAt: dict[tuple[int, str], float] = {}
        self.httpClient: Optional[httpx.AsyncClient] = None
        self.isAccepting = False
        self.lastError: Optional[str] = None
        self.counters: dict[str, int] = {
            "received": 0,
            "stored": 0,
            "duplicates": 0,
            "failed": 0,
            "rejected_queue_full": 0,
            "threats": 0,
            "webhooks_failed": 0,
            "assessments_started": 0,
            "assessments_failed": 0,
            "wakes_suppressed": 0,
        }

    async def start(self) -> None:
        if THREAT_WEBHOOK_URL:
            self.httpClient = httpx.AsyncClient(timeout=THREAT_WEBHOOK_TIMEOUT_SEC)
        self.workerTasks = [asyncio.create_task(self.runIngestWorker(), name=f"telemetry-ingest-{index}") for index in range(INGEST_WORKERS)]
        self.isAccepting = True

    async def stop(self) -> None:
        self.isAccepting = False
        try:
            # Workers keep writing until everything accepted with a 202 has reached the database queue
            await asyncio.wait_for(self.ingestQueue.join(), timeout=SHUTDOWN_DRAIN_TIMEOUT_SEC)
        except TimeoutError:
            logger.error("Telemetry ingest queue not drained within %.0fs; %d payload(s) lost.",
                         SHUTDOWN_DRAIN_TIMEOUT_SEC, self.ingestQueue.qsize())
        for task in [*self.workerTasks, *self.backgroundTasks]:
            task.cancel()
        await asyncio.gather(*self.workerTasks, *self.backgroundTasks, return_exceptions=True)
        self.workerTasks = []
        self.backgroundTasks.clear()
        self.broadcaster.close()
        if self.httpClient is not None:
            await self.httpClient.aclose()
            self.httpClient = None

    def spawn(self, coroutine: Awaitable[Any], name: str) -> None:
        task = asyncio.ensure_future(coroutine)
        task.set_name(name)
        self.backgroundTasks.add(task)

        def finishTask(finishedTask: asyncio.Task) -> None:
            self.backgroundTasks.discard(finishedTask)
            if not finishedTask.cancelled() and finishedTask.exception() is not None:
                logger.error("%s failed: %s", name, finishedTask.exception())

        task.add_done_callback(finishTask)

    # High-velocity ingestion

    def accept(self, payloads: list[IngestBase]) -> list[IngestItem]:
        if not self.isAccepting:
            raise HTTPException(status_code=503, detail="Telemetry ingestion is shutting down.")
        # All-or-nothing, so a sensor retrying a rejected batch never creates partial duplicates
        freeSlots = self.ingestQueue.maxsize - self.ingestQueue.qsize()
        if len(payloads) > freeSlots:
            self.counters["rejected_queue_full"] += len(payloads)
            raise HTTPException(
                status_code=503,
                detail=f"Ingest queue is full ({self.ingestQueue.qsize()}/{self.ingestQueue.maxsize}); retry shortly.",
                headers={"Retry-After": "2"},
            )
        loop = asyncio.get_running_loop()
        receivedAt = datetime.now(timezone.utc)
        items = [
            IngestItem(ingestID=uuid.uuid4().hex, payload=payload, receivedAt=receivedAt, storedFuture=loop.create_future())
            for payload in payloads
        ]
        for item in items:
            # Threats are announced before the database write so the response never waits on the batch interval
            if item.payload.isCritical:
                self.dispatchThreat(item)
            self.ingestQueue.put_nowait(item)
        self.counters["received"] += len(items)
        return items

    async def runIngestWorker(self) -> None:
        while True:
            batch = [await self.ingestQueue.get()]
            while len(batch) < INGEST_BATCH_MAX:
                try:
                    batch.append(self.ingestQueue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                await self.storeBatch(batch)
            except Exception as error:
                logger.exception("Telemetry ingest batch failed.")
                for item in batch:
                    self.finishItem(item, error)
            finally:
                for _ in batch:
                    self.ingestQueue.task_done()

    async def storeBatch(self, batch: list[IngestItem]) -> None:
        databaseManager = getattr(self.app.state, "databaseManager", None)
        if databaseManager is None:
            for item in batch:
                self.finishItem(item, RuntimeError("Database is unavailable."))
            return
        pending: list[tuple[IngestItem, asyncio.Future]] = []
        for item in batch:
            try:
                sql, parameters = item.payload.toStatement()
                # Enqueue every row before awaiting any, so the write worker commits them in one transaction
                pending.append((item, await databaseManager.enqueueWrite(sql, parameters, requireLastRowID=True)))
            except Exception as error:
                self.finishItem(item, error)
        results = await asyncio.gather(*(future for _, future in pending), return_exceptions=True)
        for (item, _), result in zip(pending, results):
            self.finishItem(item, result)

    def finishItem(self, item: IngestItem, result: object) -> None:
        if item.storedFuture.done():
            return
        if isinstance(result, BaseException):
            self.counters["failed"] += 1
            self.lastError = f"{type(result).__name__}: {result}"
            logger.warning("Telemetry %s from %s rejected by the database: %s",
                           item.payload.tableName, item.payload.sensor_id, self.lastError)
            item.storedFuture.set_result("failed")
            return
        # ON CONFLICT DO NOTHING reports zero changed rows for a re-sent reading
        if getattr(result, "rowCount", 1) == 0:
            self.counters["duplicates"] += 1
            item.storedFuture.set_result("duplicate")
            return
        self.counters["stored"] += 1
        item.storedFuture.set_result("stored")
        self.broadcaster.publish("telemetry", describeItem(item, "stored", getattr(result, "lastRowID", None)))

    # Threat fast path

    def dispatchThreat(self, item: IngestItem) -> None:
        self.counters["threats"] += 1
        threatData = describeItem(item, "received")
        self.broadcaster.publish("threat", threatData)
        if self.httpClient is not None:
            self.spawn(self.postThreatWebhook(threatData), f"threat-webhook-{item.ingestID}")
        if WAKE_AI_ENGINE:
            self.scheduleAssessment(item)

    async def postThreatWebhook(self, threatData: dict[str, Any]) -> None:
        if self.httpClient is None:
            return
        try:
            response = await self.httpClient.post(THREAT_WEBHOOK_URL, json=threatData)
            response.raise_for_status()
        except httpx.HTTPError as error:
            self.counters["webhooks_failed"] += 1
            logger.warning("Threat webhook %s failed: %s", THREAT_WEBHOOK_URL, error)

    def scheduleAssessment(self, item: IngestItem) -> None:
        speciesID = item.payload.speciesID or DEFAULT_SPECIES_ID
        if speciesID is None:
            logger.info("Threat from %s has no species_id and no defaultSpeciesID; assessment skipped.", item.payload.sensor_id)
            return
        h3Cell = item.payload.h3Cell
        wakeKey = (int(speciesID), h3Cell)
        now = time.monotonic()
        # Cooldown per species and cell stops a burst of gunshot detections queuing dozens of LLM runs
        if now - self.lastWakeAt.get(wakeKey, -math.inf) < THREAT_WAKE_COOLDOWN_SEC:
            self.counters["wakes_suppressed"] += 1
            return
        self.lastWakeAt[wakeKey] = now
        if len(self.lastWakeAt) > 1000:
            self.lastWakeAt = {key: wokenAt for key, wokenAt in self.lastWakeAt.items() if now - wokenAt < THREAT_WAKE_COOLDOWN_SEC}
        self.spawn(self.runThreatAssessment(item, int(speciesID), h3Cell), f"threat-assessment-{item.ingestID}")

    async def runThreatAssessment(self, item: IngestItem, speciesID: int, h3Cell: str) -> None:
        try:
            # The graph reads the database, so give the triggering row a chance to be committed first
            await asyncio.wait_for(asyncio.shield(item.storedFuture), timeout=ASSESSMENT_STORE_WAIT_SEC)
        except TimeoutError:
            logger.warning("Threat row %s not committed after %.0fs; assessing with committed data.", item.ingestID, ASSESSMENT_STORE_WAIT_SEC)
        graphApp = getattr(self.app.state, "graphApp", None)
        if graphApp is None:
            return
        databaseManager = getattr(self.app.state, "databaseManager", None)
        # One assessment at a time; the LLM step alone can use most of the Pi's RAM
        async with self.assessmentSemaphore:
            self.counters["assessments_started"] += 1
            windowEnd = datetime.now(timezone.utc)
            scope = {
                "species_id": speciesID,
                "h3_cell": h3Cell,
                "period_start": windowEnd - timedelta(days=ASSESSMENT_LOOKBACK_DAYS),
                "period_end": windowEnd,
                "latitude": item.payload.latitude,
                "longitude": item.payload.longitude,
            }
            runID = f"threat-{item.ingestID}"
            try:
                result = await runAssessment(
                    scope, runID=runID, databaseManager=databaseManager, graphApp=graphApp, persist=databaseManager is not None
                )
            except Exception as error:
                self.counters["assessments_failed"] += 1
                logger.error("Threat assessment %s failed: %s", runID, error)
                return
        riskMetrics = result.get("risk_metrics")
        self.broadcaster.publish("assessment", {
            "run_id": runID,
            "ingest_id": item.ingestID,
            "reading_type": getattr(item.payload, "reading_type", None),
            "species_id": speciesID,
            "h3_cell": h3Cell,
            "conservation_risk_index": getattr(riskMetrics, "conservation_risk_index", None),
            "escalation_level": getattr(riskMetrics, "escalation_level", None),
            "intervention_priority_index": getattr(riskMetrics, "intervention_priority_index", None),
            "intervention_priority_tier": getattr(riskMetrics, "intervention_priority_tier", None),
            "pipeline_errors": list(result.get("pipeline_errors", ()) or []),
        })

    def describe(self) -> dict[str, Any]:
        return {
            "accepting": self.isAccepting,
            "queue_depth": self.ingestQueue.qsize(),
            "queue_capacity": self.ingestQueue.maxsize,
            "workers": sum(1 for task in self.workerTasks if not task.done()),
            "counters": dict(self.counters),
            "last_error": self.lastError,
            "stream_clients": len(self.broadcaster.subscribers),
            "stream_events_dropped": self.broadcaster.droppedEvents,
            "background_tasks": len(self.backgroundTasks),
            "stats_cache": self.statsCache.describe(),
        }


def describeItem(item: IngestItem, status: str, recordID: Optional[int] = None) -> dict[str, Any]:
    return {
        "ingest_id": item.ingestID,
        "status": status,
        "table": item.payload.tableName,
        "record_id": recordID,
        "received_at": formatUtcTimestamp(item.receivedAt),
        "critical": item.payload.isCritical,
        **item.payload.model_dump(mode="json"),
    }


def getTelemetryService(request: Request) -> TelemetryService:
    service = getattr(request.app.state, "telemetryService", None)
    if service is None:
        raise HTTPException(status_code=503, detail="Telemetry service is not running.")
    return service


# Lifecycle hooks called by api/main.py (shutdown runs before the database is closed)

async def onStartup(app: FastAPI) -> None:
    service = TelemetryService(app)
    await service.start()
    app.state.telemetryService = service


async def onShutdown(app: FastAPI) -> None:
    service: Optional[TelemetryService] = getattr(app.state, "telemetryService", None)
    if service is not None:
        await service.stop()
        app.state.telemetryService = None


# Table metadata shared by retrieval and statistics; column names never come from the request

@dataclass(frozen=True)
class TableSpec:
    tableName: str
    epochColumn: str
    columns: tuple[str, ...]
    sourceType: Optional[str] = None
    jsonColumns: tuple[str, ...] = ()


DETECTION_COLUMNS: tuple[str, ...] = (
    "id", "source_type", "source_record_id", "detection_index", "device_id", "asset_path", "species_id", "class_id",
    "class_name", "category", "confidence", "detected_at", "detected_epoch_ms", "ended_at",
)
TABLE_SPECS: dict[str, TableSpec] = {
    "gps": TableSpec(
        "telemetry", "recorded_epoch_ms",
        ("id", "species_id", "animal_id", "device_id", "recorded_at", "recorded_epoch_ms", "latitude", "longitude",
         "altitude", "fix_quality", "satellite_count", "hdop", "speed_kmh", "battery_voltage", "physiological_metrics"),
        jsonColumns=("physiological_metrics",),
    ),
    "environmental": TableSpec(
        "sensor_readings", "recorded_epoch_ms",
        ("id", "sensor_mac", "device_id", "recorded_at", "recorded_epoch_ms", "h3_cell", "latitude", "longitude",
         "altitude", *MEASUREMENT_COLUMNS, "quality_flag"),
    ),
    "acoustic": TableSpec(
        "detections", "detected_epoch_ms",
        (*DETECTION_COLUMNS, "latitude", "longitude", "altitude", "metadata"),
        sourceType="ACOUSTIC", jsonColumns=("metadata",),
    ),
    "vision": TableSpec(
        "detections", "detected_epoch_ms",
        (*DETECTION_COLUMNS, "frame_width", "frame_height", "bbox_left", "bbox_top", "bbox_right", "bbox_bottom",
         "latitude", "longitude", "altitude", "metadata"),
        sourceType="VISION", jsonColumns=("metadata",),
    ),
}
DETECTION_STATS_METRICS: dict[str, str] = {"confidence": "confidence"}
STATS_METRICS: dict[str, dict[str, str]] = {
    "gps": {
        "speed_kmh": "speed_kmh",
        "altitude": "altitude",
        "hdop": "hdop",
        "satellite_count": "satellite_count",
        "battery_voltage": "battery_voltage",
        "heart_rate_bpm": "json_extract(physiological_metrics, '$.heart_rate_bpm')",
    },
    "environmental": {name: name for name in MEASUREMENT_COLUMNS},
    "acoustic": DETECTION_STATS_METRICS,
    "vision": DETECTION_STATS_METRICS,
}


def toEpochMs(value: datetime) -> int:
    return int(round(toUtcDatetime(value).timestamp() * 1000))


def fromEpochMs(epochMs: int) -> str:
    return formatUtcTimestamp(datetime.fromtimestamp(epochMs / 1000.0, timezone.utc))


def buildFilters(
    spec: TableSpec,
    startMs: Optional[int],
    endMs: Optional[int],
    sensorID: Optional[str],
    speciesID: Optional[int],
    h3Cell: Optional[str],
    category: Optional[str],
    boundingBox: Optional[tuple[float, float, float, float]],
) -> tuple[list[str], list[object]]:
    conditions: list[str] = []
    parameters: list[object] = []
    if spec.sourceType is not None:
        conditions.append("source_type = ?")
        parameters.append(spec.sourceType)
    if startMs is not None:
        conditions.append(f"{spec.epochColumn} >= ?")
        parameters.append(startMs)
    if endMs is not None:
        conditions.append(f"{spec.epochColumn} < ?")
        parameters.append(endMs)
    if sensorID is not None:
        conditions.append("device_id = ?")
        parameters.append(sensorID)
    if speciesID is not None:
        if "species_id" not in spec.columns:
            raise HTTPException(status_code=422, detail="species_id filtering is not available for environmental readings.")
        conditions.append("species_id = ?")
        parameters.append(speciesID)
    if h3Cell is not None:
        if "h3_cell" not in spec.columns:
            raise HTTPException(status_code=422, detail="h3_cell filtering is only available for environmental readings.")
        conditions.append("h3_cell = ?")
        parameters.append(h3Cell.strip().lower())
    if category is not None:
        if spec.sourceType is None:
            raise HTTPException(status_code=422, detail="category filtering is only available for acoustic and vision detections.")
        conditions.append("category = ?")
        parameters.append(category)
    if boundingBox is not None:
        minLat, maxLat, minLon, maxLon = boundingBox
        conditions.append("latitude BETWEEN ? AND ?")
        parameters.extend([minLat, maxLat])
        if minLon <= maxLon:
            conditions.append("longitude BETWEEN ? AND ?")
            parameters.extend([minLon, maxLon])
        else:
            # A map view spanning the antimeridian arrives with min_lon east of max_lon
            conditions.append("(longitude >= ? OR longitude <= ?)")
            parameters.extend([minLon, maxLon])
    return conditions, parameters


def parseBoundingBox(
    minLat: Optional[float], maxLat: Optional[float], minLon: Optional[float], maxLon: Optional[float]
) -> Optional[tuple[float, float, float, float]]:
    values = (minLat, maxLat, minLon, maxLon)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise HTTPException(status_code=422, detail="min_lat, max_lat, min_lon and max_lon must be supplied together.")
    assert minLat is not None and maxLat is not None and minLon is not None and maxLon is not None
    if minLat > maxLat:
        raise HTTPException(status_code=422, detail="min_lat must not be greater than max_lat.")
    return minLat, maxLat, minLon, maxLon


def encodeCursor(readingType: str, epochMs: int, recordID: int) -> str:
    cursorBytes = json.dumps([readingType, epochMs, recordID], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(cursorBytes).decode().rstrip("=")


def decodeCursor(cursor: str, readingType: str) -> tuple[int, int]:
    try:
        decoded = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        cursorType, epochMs, recordID = decoded
        if cursorType != readingType or not isinstance(epochMs, int) or not isinstance(recordID, int):
            raise ValueError
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail="cursor is invalid or belongs to a different reading_type.")
    return epochMs, recordID


def formatRow(readingType: str, spec: TableSpec, row: dict[str, Any]) -> dict[str, Any]:
    for column in spec.jsonColumns:
        if isinstance(row.get(column), str):
            try:
                row[column] = json.loads(row[column])
            except json.JSONDecodeError:
                pass
    if readingType == "environmental":
        # Only the measurements a sensor actually reported, mirroring the ingest payload
        row["measurements"] = {name: row.pop(name) for name in MEASUREMENT_COLUMNS if row.get(name) is not None}
        for name in MEASUREMENT_COLUMNS:
            row.pop(name, None)
    row["reading_type"] = readingType
    return row


def getDatabase(request: Request) -> Any:
    databaseManager = getattr(request.app.state, "databaseManager", None)
    if databaseManager is None:
        raise HTTPException(status_code=503, detail="Database is unavailable; the API is running in degraded mode.")
    return databaseManager


# Module-level router; api/main.py mounts it at /api/v1/telemetry

router = APIRouter()


# 1. High-velocity ingestion endpoint

@router.post("/ingest", status_code=202)
async def ingestTelemetry(
    request: Request,
    payload: Annotated[Union[IngestPayload, list[IngestPayload]], Body()],
) -> dict[str, Any]:
    service = getTelemetryService(request)
    getDatabase(request)
    payloads: list[IngestBase] = list(payload) if isinstance(payload, list) else [payload]
    if not payloads:
        raise HTTPException(status_code=422, detail="Send at least one telemetry payload.")
    if len(payloads) > MAX_PAYLOADS_PER_REQUEST:
        raise HTTPException(status_code=413, detail=f"At most {MAX_PAYLOADS_PER_REQUEST} payloads per request.")
    items = service.accept(payloads)
    return {
        "accepted": len(items),
        "ingest_ids": [item.ingestID for item in items],
        "threats_dispatched": sum(1 for item in items if item.payload.isCritical),
        "queue_depth": service.ingestQueue.qsize(),
    }


@router.get("/ingest/status")
async def ingestStatus(request: Request) -> dict[str, Any]:
    return getTelemetryService(request).describe()


# 2. Paginated data retrieval

@router.get("/data")
async def getTelemetryData(
    request: Request,
    reading_type: ReadingType = "gps",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    min_lat: Optional[float] = Query(None, ge=-90.0, le=90.0),
    max_lat: Optional[float] = Query(None, ge=-90.0, le=90.0),
    min_lon: Optional[float] = Query(None, ge=-180.0, le=180.0),
    max_lon: Optional[float] = Query(None, ge=-180.0, le=180.0),
    sensor_id: Optional[str] = None,
    species_id: Optional[int] = Query(None, gt=0),
    h3_cell: Optional[str] = None,
    category: Optional[Literal["target_wildlife", "immediate_threat", "other"]] = None,
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    databaseManager = getDatabase(request)
    spec = TABLE_SPECS[reading_type]
    startMs = toEpochMs(start_time) if start_time is not None else None
    endMs = toEpochMs(end_time) if end_time is not None else None
    if startMs is not None and endMs is not None and startMs >= endMs:
        raise HTTPException(status_code=422, detail="start_time must be earlier than end_time.")
    conditions, parameters = buildFilters(
        spec, startMs, endMs, sensor_id, species_id, h3_cell, category, parseBoundingBox(min_lat, max_lat, min_lon, max_lon)
    )
    if cursor:
        # Keyset pagination: seek past the last row seen instead of scanning and discarding OFFSET rows
        cursorEpochMs, cursorID = decodeCursor(cursor, reading_type)
        conditions.append(f"({spec.epochColumn} < ? OR ({spec.epochColumn} = ? AND id < ?))")
        parameters.extend([cursorEpochMs, cursorEpochMs, cursorID])
    whereClause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    # One extra row tells us whether another page exists without a COUNT(*) scan
    rows = await databaseManager.fetchAll(
        f"SELECT {', '.join(spec.columns)} FROM {spec.tableName} {whereClause} "
        f"ORDER BY {spec.epochColumn} DESC, id DESC LIMIT ?;",
        [*parameters, limit + 1],
    )
    hasMore = len(rows) > limit
    rows = rows[:limit]
    nextCursor = encodeCursor(reading_type, int(rows[-1][spec.epochColumn]), int(rows[-1]["id"])) if hasMore and rows else None
    return {
        "reading_type": reading_type,
        "count": len(rows),
        "items": [formatRow(reading_type, spec, row) for row in rows],
        "next_cursor": nextCursor,
        "has_more": hasMore,
    }


# 3. Real-time streaming (Server-Sent Events)

@router.get("/stream")
async def streamTelemetry(
    request: Request,
    reading_type: Optional[list[ReadingType]] = Query(None),
    include_threats: bool = True,
) -> StreamingResponse:
    service = getTelemetryService(request)
    readingTypes = set(reading_type) if reading_type else None
    # Subscribing before the response starts turns a full server into a clean 503 instead of a broken stream
    clientQueue = service.broadcaster.subscribe()

    def isWanted(event: dict[str, Any]) -> bool:
        if event["event"] in ("threat", "assessment"):
            return include_threats
        return readingTypes is None or event["data"].get("reading_type") in readingTypes

    async def eventStream() -> AsyncIterator[str]:
        try:
            yield f"retry: {STREAM_RETRY_MS}\n: connected {formatUtcTimestamp(datetime.now(timezone.utc))}\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(clientQueue.get(), timeout=STREAM_HEARTBEAT_SEC)
                except TimeoutError:
                    if await request.is_disconnected():
                        break
                    # SSE comment lines keep proxies and satellite links from closing an idle connection
                    yield f": ping {formatUtcTimestamp(datetime.now(timezone.utc))}\n\n"
                    continue
                if event is None:
                    break
                if isWanted(event):
                    yield formatServerSentEvent(event)
        finally:
            service.broadcaster.unsubscribe(clientQueue)

    return StreamingResponse(
        eventStream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


# 4. Aggregation and statistics

async def computeStats(
    databaseManager: Any,
    readingType: str,
    bucket: str,
    metricNames: tuple[str, ...],
    startMs: int,
    endMs: int,
    sensorID: Optional[str],
    speciesID: Optional[int],
    h3Cell: Optional[str],
    includeInvalid: bool,
) -> dict[str, Any]:
    spec = TABLE_SPECS[readingType]
    bucketMs = BUCKET_SIZES_MS[bucket]
    conditions, parameters = buildFilters(spec, startMs, endMs, sensorID, speciesID, h3Cell, None, None)
    if readingType == "environmental" and not includeInvalid:
        conditions.append("quality_flag != 'INVALID'")
    metricExpressions = STATS_METRICS[readingType]
    selectParts = [f"({spec.epochColumn} / ?) * ? AS bucket_epoch_ms", "COUNT(*) AS sample_count"]
    if spec.sourceType is not None:
        selectParts.append("SUM(category = 'immediate_threat') AS threat_count")
    for index, name in enumerate(metricNames):
        expression = metricExpressions[name]
        selectParts.extend([
            f"AVG({expression}) AS m{index}_avg",
            f"MIN({expression}) AS m{index}_min",
            f"MAX({expression}) AS m{index}_max",
            f"COUNT({expression}) AS m{index}_count",
        ])
    # Integer division buckets the indexed epoch column, the SQLite equivalent of date_trunc
    rows = await databaseManager.fetchAll(
        f"SELECT {', '.join(selectParts)} FROM {spec.tableName} WHERE {' AND '.join(conditions)} "
        "GROUP BY bucket_epoch_ms ORDER BY bucket_epoch_ms;",
        [bucketMs, bucketMs, *parameters],
    )
    buckets = []
    for row in rows:
        bucketEntry: dict[str, Any] = {
            "bucket_start": fromEpochMs(int(row["bucket_epoch_ms"])),
            "bucket_epoch_ms": int(row["bucket_epoch_ms"]),
            "sample_count": int(row["sample_count"]),
        }
        if spec.sourceType is not None:
            bucketEntry["threat_count"] = int(row["threat_count"] or 0)
        bucketEntry["metrics"] = {
            name: {
                "avg": round(row[f"m{index}_avg"], 4) if row[f"m{index}_avg"] is not None else None,
                "min": row[f"m{index}_min"],
                "max": row[f"m{index}_max"],
                "count": int(row[f"m{index}_count"]),
            }
            for index, name in enumerate(metricNames)
        }
        buckets.append(bucketEntry)
    return {
        "reading_type": readingType,
        "bucket": bucket,
        "start_time": fromEpochMs(startMs),
        "end_time": fromEpochMs(endMs),
        "metrics": list(metricNames),
        "buckets": buckets,
        "generated_at": formatUtcTimestamp(datetime.now(timezone.utc)),
    }


@router.get("/stats")
async def getTelemetryStats(
    request: Request,
    reading_type: ReadingType = "environmental",
    bucket: BucketSize = "hour",
    metric: Optional[list[str]] = Query(None),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    sensor_id: Optional[str] = None,
    species_id: Optional[int] = Query(None, gt=0),
    h3_cell: Optional[str] = None,
    include_invalid: bool = False,
) -> dict[str, Any]:
    service = getTelemetryService(request)
    databaseManager = getDatabase(request)
    availableMetrics = STATS_METRICS[reading_type]
    metricNames = tuple(dict.fromkeys(metric)) if metric else tuple(availableMetrics)
    unknownMetrics = [name for name in metricNames if name not in availableMetrics]
    if unknownMetrics:
        raise HTTPException(status_code=422, detail=f"Unknown {reading_type} metrics {unknownMetrics}; allowed: {list(availableMetrics)}.")

    bucketMs = BUCKET_SIZES_MS[bucket]
    # Default windows snap to bucket edges so repeated dashboard loads hit the same cache key
    endMs = toEpochMs(end_time) if end_time is not None else -(-toEpochMs(datetime.now(timezone.utc)) // bucketMs) * bucketMs
    startMs = (
        toEpochMs(start_time) if start_time is not None
        else (endMs - int(STATS_DEFAULT_LOOKBACK_DAYS * 86_400_000)) // bucketMs * bucketMs
    )
    if startMs >= endMs:
        raise HTTPException(status_code=422, detail="start_time must be earlier than end_time.")
    if (endMs - startMs) / bucketMs > STATS_MAX_BUCKETS:
        raise HTTPException(status_code=422, detail=f"Window spans more than {STATS_MAX_BUCKETS} {bucket} buckets; use a larger bucket or shorter window.")

    cacheKey = (reading_type, bucket, metricNames, startMs, endMs, sensor_id, species_id, h3_cell, include_invalid)
    result, isCached = await service.statsCache.getOrCompute(
        cacheKey,
        lambda: computeStats(databaseManager, reading_type, bucket, metricNames, startMs, endMs,
                             sensor_id, species_id, h3_cell, include_invalid),
    )
    return {**result, "cached": isCached}


async def main() -> None:
    # Demo: run the full API lifespan in-process, ingest a few readings, then page, aggregate and inspect them
    from api.main import createApp

    app = createApp()
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://icmis.local") as client:
            now = datetime.now(timezone.utc)
            payloads = [
                {
                    "reading_type": "environmental", "sensor_id": "river-probe-01", "sensor_mac": "AA:BB:CC:DD:EE:01",
                    "timestamp": (now - timedelta(minutes=10 * index)).isoformat(), "latitude": -1.30, "longitude": 36.80,
                    "measurements": {"water_ph": 7.1 + index / 10, "ambient_temperature_c": 24.0 + index},
                }
                for index in range(3)
            ]
            payloads.append({
                "reading_type": "acoustic", "sensor_id": "acoustic-array-07", "timestamp": now.isoformat(),
                "latitude": -1.31, "longitude": 36.81, "class_name": "gunshot", "category": "immediate_threat",
                "confidence": 0.93, "metadata": {"decibel_level": 118.0},
            })
            response = await client.post("/api/v1/telemetry/ingest", json=payloads)
            print(f"Ingest: {response.status_code} {response.json()}")
            # Let the ingest worker and the 500 ms database batch flush
            await asyncio.sleep(1.5)
            response = await client.get("/api/v1/telemetry/data", params={"reading_type": "environmental", "limit": 2})
            page = response.json()
            print(f"Data page: {page['count']} row(s), has_more={page['has_more']}")
            response = await client.get("/api/v1/telemetry/stats", params={"reading_type": "environmental", "bucket": "hour"})
            print(f"Stats: {len(response.json()['buckets'])} bucket(s), cached={response.json()['cached']}")
            response = await client.get("/api/v1/telemetry/ingest/status")
            print(f"Status: {response.json()['counters']}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())