import asyncio
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
import h3
import httpx
import json
import logging
import os
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator
import sqlite3
from starlette.exceptions import HTTPException
import sys
import time
import traceback
from typing import Any, Literal, Optional
import uuid
import yaml

# Allow "python api/routesReports.py" as well as loading through api/main.py
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from aiEngine.graphWorkflow import (  # noqa: E402
    NODE_AGGREGATION,
    NODE_EXPLAINABILITY,
    NODE_MAPPING,
    NODE_NORMALIZATION,
    NODE_SYNTHESIS,
)
from aiEngine.stateSchema import (  # noqa: E402
    buildPersistenceStatement,
    buildSerializationPayload,
    createInitialState,
)
from api.routesTelemetry import AsyncTtlCache, getDatabase  # noqa: E402
from database.models import formatUtcTimestamp, toUtcDatetime  # noqa: E402

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

REPORTS_CONFIG: dict = config.get("reportsApi", {}) or {}

# Define job queue settings
JOB_QUEUE_SIZE: int = int(REPORTS_CONFIG.get("jobQueueSize", 50))
JOB_WORKERS: int = int(REPORTS_CONFIG.get("jobWorkers", 1))
JOB_TIMEOUT_SEC: float = float(REPORTS_CONFIG.get("jobTimeoutSec", 900))
JOB_RETENTION_SEC: float = float(REPORTS_CONFIG.get("jobRetentionSec", 3600))
JOB_RETENTION_MAX: int = int(REPORTS_CONFIG.get("jobRetentionMax", 200))
SHUTDOWN_DRAIN_TIMEOUT_SEC: float = float(REPORTS_CONFIG.get("shutdownDrainTimeoutSec", 5.0))
DEDUPLICATE_SCOPES: bool = bool(REPORTS_CONFIG.get("deduplicateScopes", True))

# Define assessment scope settings
DEFAULT_LOOKBACK_DAYS: float = float(REPORTS_CONFIG.get("defaultLookbackDays", 30))
MAX_LOOKBACK_DAYS: float = float(REPORTS_CONFIG.get("maxLookbackDays", 365))
SCOPE_RESOLUTION: int = int(REPORTS_CONFIG.get("scopeResolution", 7))
ESCALATION_TIERS: tuple[str, ...] = tuple(
    str(tier).upper() for tier in REPORTS_CONFIG.get("escalationTiers", ["LOW", "GUARDED", "ELEVATED", "HIGH", "CRITICAL"]) or []
)

# Define library listing and detail settings
DEFAULT_PAGE_SIZE: int = int(REPORTS_CONFIG.get("defaultPageSize", 20))
MAX_PAGE_SIZE: int = int(REPORTS_CONFIG.get("maxPageSize", 100))
LIST_NARRATIVE_CHARS: int = int(REPORTS_CONFIG.get("listNarrativeChars", 400))
HYDRATION_ROW_LIMIT: int = int(REPORTS_CONFIG.get("hydrationRowLimit", 50))
HYDRATION_ENABLED: bool = bool(REPORTS_CONFIG.get("hydrationEnabled", True))
DETAIL_CACHE_TTL_SEC: float = float(REPORTS_CONFIG.get("detailCacheTtlSec", 60))
DETAIL_CACHE_MAX_ENTRIES: int = int(REPORTS_CONFIG.get("detailCacheMaxEntries", 64))

if min(JOB_QUEUE_SIZE, JOB_WORKERS, JOB_RETENTION_MAX, HYDRATION_ROW_LIMIT) <= 0:
    raise ValueError("reportsApi queue, worker, retention and hydration limits must be positive.")
if JOB_TIMEOUT_SEC <= 0 or JOB_RETENTION_SEC < 0:
    raise ValueError("reportsApi jobTimeoutSec must be positive and jobRetentionSec must not be negative.")
if not 0 < DEFAULT_LOOKBACK_DAYS <= MAX_LOOKBACK_DAYS:
    raise ValueError("reportsApi defaultLookbackDays must be positive and no larger than maxLookbackDays.")
if not 0 < DEFAULT_PAGE_SIZE <= MAX_PAGE_SIZE:
    raise ValueError("reportsApi defaultPageSize must be positive and no larger than maxPageSize.")
if len(ESCALATION_TIERS) != 5:
    raise ValueError("reportsApi escalationTiers must name all five escalation levels.")
if not 0 < SCOPE_RESOLUTION <= 15:
    raise ValueError("reportsApi scopeResolution must be a valid H3 resolution.")

JobStatus = Literal["queued", "processing", "completed", "failed"]

# Graph nodes in execution order; each completed node moves the polling progress bar
PIPELINE_NODES: tuple[str, ...] = (
    NODE_AGGREGATION, NODE_NORMALIZATION, NODE_SYNTHESIS, NODE_EXPLAINABILITY, NODE_MAPPING,
)
# Persistence and the report id lookup happen after the last node, so streaming stops short of 100
STREAMING_PROGRESS_MAX: int = 95

# Tables that TracingKeys can reference; the whitelist keeps table names out of request-driven SQL
TRACEABLE_TABLES: frozenset[str] = frozenset(
    {"telemetry", "sensor_readings", "detections", "genetics_records", "acoustic_events", "camera_ingestion", "edna_samples"}
)

REPORT_COLUMNS: tuple[str, ...] = (
    "species_id", "h3_cell", "period_start", "period_end", "population_subscore", "habitat_subscore",
    "threat_subscore", "climate_subscore", "genetics_subscore", "behavior_subscore", "conservation_risk_index",
    "inbreeding_penalty_index", "risk_momentum_per_day", "momentum_window_days", "population_trend",
    "effective_population_size", "critical_inbreeding_risk", "escalation_level", "model_version", "generated_at",
)
SUBSCORE_COLUMNS: tuple[str, ...] = (
    "population_subscore", "habitat_subscore", "threat_subscore", "climate_subscore", "genetics_subscore",
    "behavior_subscore",
)
# Composite primary key of risk_assessments; a re-run of the same scope upserts onto these columns
SCOPE_KEY_COLUMNS: tuple[str, ...] = ("species_id", "h3_cell", "period_start", "period_end")

logger = logging.getLogger("icmis.api.reports")


# Trigger payload: the scope the LangGraph run is given

class ReportTriggerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    species_id: PositiveInt
    scientific_name: Optional[str] = None
    # Location: an H3 cell, a point, or a bounding box whose centre is used
    h3_cell: Optional[str] = None
    latitude: Optional[float] = Field(None, ge=-90.0, le=90.0)
    longitude: Optional[float] = Field(None, ge=-180.0, le=180.0)
    min_lat: Optional[float] = Field(None, ge=-90.0, le=90.0)
    max_lat: Optional[float] = Field(None, ge=-90.0, le=90.0)
    min_lon: Optional[float] = Field(None, ge=-180.0, le=180.0)
    max_lon: Optional[float] = Field(None, ge=-180.0, le=180.0)
    # Time window: explicit bounds, or a lookback ending now
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    lookback_days: Optional[float] = Field(None, gt=0.0)
    # Run again even when an identical scope is already queued or running
    force: bool = False

    @model_validator(mode="after")
    def resolveScope(self) -> "ReportTriggerRequest":
        latitude, longitude = self.latitude, self.longitude
        if self.h3_cell:
            h3Cell = str(self.h3_cell).strip().lower()
            if not h3.is_valid_cell(h3Cell):
                raise ValueError(f"{self.h3_cell} is not a valid H3 cell index.")
            if latitude is None or longitude is None:
                latitude, longitude = h3.cell_to_latlng(h3Cell)
        else:
            if latitude is None or longitude is None:
                boundingBox = (self.min_lat, self.max_lat, self.min_lon, self.max_lon)
                if any(bound is None for bound in boundingBox):
                    raise ValueError("Supply h3_cell, latitude and longitude, or a complete bounding box.")
                if self.min_lat >= self.max_lat or self.min_lon >= self.max_lon:  # type: ignore[operator]
                    raise ValueError("Bounding box minimums must be smaller than their maximums.")
                # The graph assesses one cell, so a viewport box collapses to its centre
                latitude = (self.min_lat + self.max_lat) / 2.0  # type: ignore[operator]
                longitude = (self.min_lon + self.max_lon) / 2.0  # type: ignore[operator]
            h3Cell = h3.latlng_to_cell(latitude, longitude, SCOPE_RESOLUTION)

        periodEnd = toUtcDatetime(self.end_time) if self.end_time is not None else datetime.now(timezone.utc)
        if self.start_time is not None:
            periodStart = toUtcDatetime(self.start_time)
        else:
            periodStart = periodEnd - timedelta(days=self.lookback_days or DEFAULT_LOOKBACK_DAYS)
        if periodStart >= periodEnd:
            raise ValueError("start_time must be earlier than end_time.")
        if (periodEnd - periodStart).total_seconds() > MAX_LOOKBACK_DAYS * 86400.0:
            raise ValueError(f"The assessment window may not exceed {MAX_LOOKBACK_DAYS:g} days.")

        # Writing the resolved values back keeps toScope() and the echoed response consistent
        object.__setattr__(self, "h3_cell", h3Cell)
        object.__setattr__(self, "latitude", latitude)
        object.__setattr__(self, "longitude", longitude)
        object.__setattr__(self, "start_time", periodStart)
        object.__setattr__(self, "end_time", periodEnd)
        return self

    def toScope(self) -> dict[str, Any]:
        return {
            "species_id": int(self.species_id),
            "scientific_name": (self.scientific_name or "").strip() or None,
            "h3_cell": self.h3_cell,
            "period_start": self.start_time,
            "period_end": self.end_time,
            "latitude": self.latitude,
            "longitude": self.longitude,
        }

    def scopeKey(self) -> tuple:
        return (
            int(self.species_id), self.h3_cell,
            formatUtcTimestamp(self.start_time), formatUtcTimestamp(self.end_time),
        )


# Job records: the in-memory registry the frontend polls while the graph runs

@dataclass
class ReportJob:
    jobID: str
    runID: str
    scope: dict[str, Any]
    scopeKey: tuple
    requestedAt: datetime
    status: JobStatus = "queued"
    progress: int = 0
    currentNode: Optional[str] = None
    completedNodes: tuple[str, ...] = ()
    reportID: Optional[int] = None
    summary: dict[str, Any] = field(default_factory=dict)
    pipelineErrors: list[str] = field(default_factory=list)
    error: Optional[str] = None
    errorType: Optional[str] = None
    traceback: Optional[str] = None
    startedAt: Optional[datetime] = None
    finishedAt: Optional[datetime] = None
    retiresAt: Optional[float] = None

    @property
    def isFinished(self) -> bool:
        return self.status in ("completed", "failed")

    def durationSec(self) -> Optional[float]:
        if self.startedAt is None:
            return None
        finishedAt = self.finishedAt or datetime.now(timezone.utc)
        return round((finishedAt - self.startedAt).total_seconds(), 3)

    def describe(self, includeTraceback: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "job_id": self.jobID,
            "run_id": self.runID,
            "status": self.status,
            "progress": self.progress,
            "current_node": self.currentNode,
            "completed_nodes": list(self.completedNodes),
            "pipeline_nodes": list(PIPELINE_NODES),
            "report_id": self.reportID,
            "scope": {
                "species_id": self.scope["species_id"],
                "scientific_name": self.scope.get("scientific_name"),
                "h3_cell": self.scope["h3_cell"],
                "period_start": formatUtcTimestamp(self.scope["period_start"]),
                "period_end": formatUtcTimestamp(self.scope["period_end"]),
                "latitude": self.scope.get("latitude"),
                "longitude": self.scope.get("longitude"),
            },
            "requested_at": formatUtcTimestamp(self.requestedAt),
            "started_at": formatUtcTimestamp(self.startedAt) if self.startedAt else None,
            "finished_at": formatUtcTimestamp(self.finishedAt) if self.finishedAt else None,
            "duration_sec": self.durationSec(),
            "pipeline_errors": list(self.pipelineErrors),
        }
        if self.summary:
            payload["summary"] = dict(self.summary)
        if self.status == "failed":
            payload["error"] = {"type": self.errorType, "message": self.error}
            if includeTraceback and self.traceback:
                payload["error"]["traceback"] = self.traceback
        return payload


# Reports service: one job queue, one worker, and the LangGraph run it streams

class ReportsService:
    def __init__(self, app: FastAPI) -> None:
        self.app = app
        self.jobQueue: asyncio.Queue[ReportJob] = asyncio.Queue(maxsize=JOB_QUEUE_SIZE)
        self.jobs: OrderedDict[str, ReportJob] = OrderedDict()
        self.activeScopes: dict[tuple, str] = {}
        self.detailCache = AsyncTtlCache(DETAIL_CACHE_TTL_SEC, DETAIL_CACHE_MAX_ENTRIES)
        self.workerTasks: list[asyncio.Task] = []
        # One assessment at a time; the LLM step alone can use most of the Pi's RAM
        self.assessmentSemaphore = asyncio.Semaphore(1)
        self.isAccepting = False
        self.lastError: Optional[str] = None
        self.counters: dict[str, int] = {
            "submitted": 0, "reused": 0, "completed": 0, "failed": 0, "timed_out": 0, "rejected_queue_full": 0,
        }

    async def start(self) -> None:
        self.isAccepting = True
        self.workerTasks = [
            asyncio.create_task(self.runJobWorker(), name=f"reports-worker-{index}") for index in range(JOB_WORKERS)
        ]

    async def stop(self) -> None:
        self.isAccepting = False
        try:
            # A queued job has not touched the LLM yet, so a short drain is enough
            await asyncio.wait_for(self.jobQueue.join(), timeout=SHUTDOWN_DRAIN_TIMEOUT_SEC)
        except TimeoutError:
            logger.warning("Report job queue not drained within %.0fs; %d job(s) abandoned.",
                           SHUTDOWN_DRAIN_TIMEOUT_SEC, self.jobQueue.qsize())
        for task in self.workerTasks:
            task.cancel()
        await asyncio.gather(*self.workerTasks, return_exceptions=True)
        self.workerTasks = []

    # Job registry

    def pruneJobs(self) -> None:
        now = time.monotonic()
        for jobID, job in list(self.jobs.items()):
            if job.retiresAt is not None and job.retiresAt <= now:
                self.jobs.pop(jobID, None)
        while len(self.jobs) > JOB_RETENTION_MAX:
            _, dropped = self.jobs.popitem(last=False)
            if not dropped.isFinished:
                # Never evict work that is still running; put it back and stop pruning
                self.jobs[dropped.jobID] = dropped
                self.jobs.move_to_end(dropped.jobID, last=False)
                break

    def getJob(self, jobID: str) -> ReportJob:
        self.pruneJobs()
        job = self.jobs.get(jobID)
        if job is None:
            raise HTTPException(status_code=404, detail=f"No report job {jobID}; it may have expired.")
        return job

    def submit(self, trigger: ReportTriggerRequest) -> tuple[ReportJob, bool]:
        if not self.isAccepting:
            raise HTTPException(status_code=503, detail="Report generation is shutting down.")
        self.pruneJobs()
        scopeKey = trigger.scopeKey()
        if DEDUPLICATE_SCOPES and not trigger.force:
            # An impatient dashboard clicking twice should watch one run, not queue a second LLM job
            existingID = self.activeScopes.get(scopeKey)
            existing = self.jobs.get(existingID) if existingID else None
            if existing is not None and not existing.isFinished:
                self.counters["reused"] += 1
                return existing, True
        if self.jobQueue.full():
            self.counters["rejected_queue_full"] += 1
            raise HTTPException(
                status_code=503,
                detail=f"Report queue is full ({self.jobQueue.qsize()}/{self.jobQueue.maxsize}); retry shortly.",
                headers={"Retry-After": "30"},
            )
        jobID = uuid.uuid4().hex
        job = ReportJob(
            jobID=jobID, runID=f"report-{jobID}", scope=trigger.toScope(), scopeKey=scopeKey,
            requestedAt=datetime.now(timezone.utc),
        )
        self.jobs[jobID] = job
        self.activeScopes[scopeKey] = jobID
        self.jobQueue.put_nowait(job)
        self.counters["submitted"] += 1
        return job, False

    # Worker and graph execution

    async def runJobWorker(self) -> None:
        while True:
            job = await self.jobQueue.get()
            try:
                await self.runJob(job)
            except asyncio.CancelledError:
                self.finishJob(job, RuntimeError("The API shut down before this report finished."))
                raise
            except Exception as error:
                logger.exception("Report job %s crashed.", job.jobID)
                self.finishJob(job, error)
            finally:
                self.jobQueue.task_done()

    async def runJob(self, job: ReportJob) -> None:
        graphApp = getattr(self.app.state, "graphApp", None)
        if graphApp is None:
            self.finishJob(job, RuntimeError("Assessment workflow is not initialised."))
            return
        databaseManager = getattr(self.app.state, "databaseManager", None)
        async with self.assessmentSemaphore:
            job.status = "processing"
            job.startedAt = datetime.now(timezone.utc)
            try:
                finalState = await asyncio.wait_for(self.streamAssessment(job, graphApp), timeout=JOB_TIMEOUT_SEC)
            except TimeoutError:
                self.counters["timed_out"] += 1
                self.finishJob(job, TimeoutError(f"The assessment exceeded {JOB_TIMEOUT_SEC:g}s and was abandoned."))
                return
            except Exception as error:
                self.finishJob(job, error)
                return
        try:
            await self.persistResult(job, finalState, databaseManager)
        except Exception as error:
            self.finishJob(job, error)
            return
        self.finishJob(job, None)

    async def streamAssessment(self, job: ReportJob, graphApp: Any) -> dict[str, Any]:
        initialState = createInitialState(job.runID, job.scope)
        runConfig = {"configurable": {"thread_id": job.runID}}
        finalState: dict[str, Any] = dict(initialState)
        # "values" streams the whole state after every node, so node_history doubles as a progress counter
        async for chunk in graphApp.astream(initialState, config=runConfig, stream_mode="values"):
            finalState = dict(chunk)
            self.recordProgress(job, finalState)
        return finalState

    def recordProgress(self, job: ReportJob, state: dict[str, Any]) -> None:
        history = tuple(str(node) for node in (state.get("node_history") or ()))
        completed = tuple(node for node in PIPELINE_NODES if node in history)
        job.completedNodes = completed
        job.progress = min(round(len(completed) / len(PIPELINE_NODES) * 100), STREAMING_PROGRESS_MAX)
        remaining = [node for node in PIPELINE_NODES if node not in completed]
        job.currentNode = remaining[0] if remaining else None
        job.pipelineErrors = [str(error) for error in (state.get("pipeline_errors") or ())]

    async def persistResult(self, job: ReportJob, finalState: dict[str, Any], databaseManager: Any) -> None:
        payload = finalState.get("serialization_payload") or buildSerializationPayload(finalState)
        riskMetrics = finalState.get("risk_metrics")
        job.summary = {
            "conservation_risk_index": payload.get("conservation_risk_index"),
            "escalation_level": getattr(riskMetrics, "escalation_level", None),
            "escalation_tier": escalationTierName(getattr(riskMetrics, "escalation_level", None)),
            "intervention_priority_index": getattr(riskMetrics, "intervention_priority_index", None),
            "intervention_priority_tier": getattr(riskMetrics, "intervention_priority_tier", None),
            "inbreeding_penalty_index": payload.get("inbreeding_penalty_index"),
            "population_trend": payload.get("population_trend"),
            "narrative_characters": len(str(payload.get("narrative_report", ""))),
        }
        if databaseManager is None:
            # Degraded mode: the narrative still reached the client, it just has no row to point at
            job.pipelineErrors.append("Database unavailable; the report was not persisted.")
            return
        sql, parameters = buildPersistenceStatement({**finalState, "serialization_payload": payload})
        await databaseManager.executeWrite(upsertOnScope(sql), parameters)
        job.reportID = await lookupReportID(
            databaseManager, payload["species_id"], payload["h3_cell"], payload["period_start"], payload["period_end"]
        )

    def finishJob(self, job: ReportJob, error: Optional[BaseException]) -> None:
        if job.isFinished:
            return
        job.finishedAt = datetime.now(timezone.utc)
        job.retiresAt = time.monotonic() + JOB_RETENTION_SEC
        if self.activeScopes.get(job.scopeKey) == job.jobID:
            self.activeScopes.pop(job.scopeKey, None)
        if error is not None:
            job.status = "failed"
            job.errorType = type(error).__name__
            job.error = str(error) or job.errorType
            job.traceback = "".join(traceback.format_exception(type(error), error, error.__traceback__))
            self.lastError = f"{job.errorType}: {job.error}"
            self.counters["failed"] += 1
            logger.error("Report job %s failed: %s", job.jobID, self.lastError)
            return
        job.status = "completed"
        job.progress = 100
        job.currentNode = None
        self.counters["completed"] += 1

    def describe(self) -> dict[str, Any]:
        self.pruneJobs()
        active = [job for job in self.jobs.values() if not job.isFinished]
        return {
            "accepting": self.isAccepting,
            "queue_depth": self.jobQueue.qsize(),
            "queue_capacity": self.jobQueue.maxsize,
            "workers": sum(1 for task in self.workerTasks if not task.done()),
            "active_jobs": [job.describe() for job in active],
            "retained_jobs": len(self.jobs),
            "counters": dict(self.counters),
            "last_error": self.lastError,
            "detail_cache": self.detailCache.describe(),
        }


def getReportsService(request: Request) -> ReportsService:
    service = getattr(request.app.state, "reportsService", None)
    if service is None:
        raise HTTPException(status_code=503, detail="Reports service is not running.")
    return service


def escalationTierName(escalationLevel: Optional[Any]) -> Optional[str]:
    if escalationLevel is None:
        return None
    level = int(escalationLevel)
    return ESCALATION_TIERS[level - 1] if 1 <= level <= len(ESCALATION_TIERS) else None


def escalationLevelForTier(threatTier: str) -> int:
    tier = threatTier.strip().upper()
    if tier.isdigit():
        level = int(tier)
        if 1 <= level <= len(ESCALATION_TIERS):
            return level
    elif tier in ESCALATION_TIERS:
        return ESCALATION_TIERS.index(tier) + 1
    raise HTTPException(
        status_code=422,
        detail=f"threat_tier must be one of {', '.join(ESCALATION_TIERS)} or a level from 1 to {len(ESCALATION_TIERS)}.",
    )


def upsertOnScope(insertSql: str) -> str:
    # Re-running a scope (force=true, or a refreshed window) must replace the previous row, not collide with it
    opening = insertSql.index("(") + 1
    columns = [column.strip() for column in insertSql[opening:insertSql.index(")", opening)].split(",")]
    assignments = ", ".join(
        f"{column} = excluded.{column}" for column in columns if column not in SCOPE_KEY_COLUMNS
    )
    return (
        f"{insertSql.rstrip().rstrip(';')} ON CONFLICT ({', '.join(SCOPE_KEY_COLUMNS)}) DO UPDATE SET {assignments};"
    )


async def lookupReportID(
    databaseManager: Any, speciesID: int, h3Cell: str, periodStart: str, periodEnd: str
) -> Optional[int]:
    # risk_assessments has a composite primary key, so rowid is the stable single-value id the API exposes
    rows = await databaseManager.fetchAll(
        "SELECT rowid AS id FROM risk_assessments "
        "WHERE species_id = ? AND h3_cell = ? AND period_start = ? AND period_end = ?;",
        (speciesID, h3Cell, periodStart, periodEnd),
    )
    return int(rows[0]["id"]) if rows else None


def decodeJsonColumns(row: dict[str, Any]) -> dict[str, Any]:
    # Source tables store JSON in TEXT columns; decode anything that is obviously an object or array
    for column, value in list(row.items()):
        if isinstance(value, (bytes, bytearray, memoryview)):
            # Geometry BLOBs are not JSON serialisable and are redundant beside the latitude/longitude columns
            row.pop(column)
        elif isinstance(value, str) and value[:1] in ("{", "["):
            try:
                row[column] = json.loads(value)
            except json.JSONDecodeError:
                pass
    return row


def formatReportRow(row: dict[str, Any], narrativeChars: Optional[int] = None) -> dict[str, Any]:
    inputSummary = row.pop("input_summary", None)
    if isinstance(inputSummary, str):
        try:
            inputSummary = json.loads(inputSummary)
        except json.JSONDecodeError:
            inputSummary = {}
    summary: dict[str, Any] = inputSummary if isinstance(inputSummary, dict) else {}
    narrative = str(row.pop("narrative_report", "") or "")
    formatted: dict[str, Any] = {
        "id": int(row["id"]),
        "species": {
            "id": row.get("species_id"),
            "scientific_name": row.get("scientific_name") or summary.get("scientific_name"),
            "common_name": row.get("common_name"),
            "iucn_status": row.get("iucn_status"),
        },
        "scope": {
            "h3_cell": row.get("h3_cell"),
            "period_start": row.get("period_start"),
            "period_end": row.get("period_end"),
            "latitude": summary.get("latitude"),
            "longitude": summary.get("longitude"),
        },
        "scores": {
            "conservation_risk_index": row.get("conservation_risk_index"),
            "escalation_level": row.get("escalation_level"),
            "escalation_tier": escalationTierName(row.get("escalation_level")),
            "intervention_priority_index": summary.get("priority_components", {}).get("intervention_priority_index"),
            "intervention_priority_tier": summary.get("intervention_priority_tier"),
            "inbreeding_penalty_index": row.get("inbreeding_penalty_index"),
            "critical_inbreeding_risk": bool(row.get("critical_inbreeding_risk")),
            "risk_momentum_per_day": row.get("risk_momentum_per_day"),
            "momentum_window_days": row.get("momentum_window_days"),
            "population_trend": row.get("population_trend"),
            "effective_population_size": row.get("effective_population_size"),
            "subscores": {column.removesuffix("_subscore"): row.get(column) for column in SUBSCORE_COLUMNS},
        },
        "model_version": row.get("model_version"),
        "generated_at": row.get("generated_at"),
    }
    if narrativeChars is not None:
        excerpt = narrative[:narrativeChars]
        formatted["narrative_excerpt"] = f"{excerpt}..." if len(narrative) > narrativeChars else excerpt
        formatted["narrative_characters"] = len(narrative)
        return formatted
    formatted["narrative_report"] = narrative
    formatted["input_summary"] = summary
    return formatted


# Module-level router; api/main.py mounts it at /api/v1/reports

router = APIRouter()


# Lifecycle hooks called by api/main.py (shutdown runs before the database is closed)

async def onStartup(app: FastAPI) -> None:
    service = ReportsService(app)
    await service.start()
    app.state.reportsService = service
    app.state.reportsCache = service.detailCache


async def onShutdown(app: FastAPI) -> None:
    service: Optional[ReportsService] = getattr(app.state, "reportsService", None)
    if service is not None:
        await service.stop()
        app.state.reportsService = None
    app.state.reportsCache = None


# 1. On-demand trigger

@router.post("/generate", status_code=202)
async def generateReport(request: Request, trigger: ReportTriggerRequest) -> Response:
    service = getReportsService(request)
    getDatabase(request)
    job, wasReused = service.submit(trigger)
    statusUrl = request.app.url_path_for("getReportJobStatus", job_id=job.jobID)
    body = {
        **job.describe(),
        "reused_existing_job": wasReused,
        "status_url": statusUrl,
        "queue_depth": service.jobQueue.qsize(),
        "poll_after_sec": 5,
    }
    # 202 lets the dashboard switch to a loading state immediately; Location points at the poll endpoint
    return JSONResponse(status_code=202, content=body, headers={"Location": statusUrl})


@router.get("/jobs")
async def listReportJobs(request: Request) -> dict[str, Any]:
    return getReportsService(request).describe()


# 2. Status polling

@router.get("/status/{job_id}", name="getReportJobStatus")
async def getReportJobStatus(request: Request, job_id: str, redirect: bool = True) -> Response:
    service = getReportsService(request)
    job = service.getJob(job_id)
    settings = getattr(request.app.state, "settings", None)
    body = job.describe(includeTraceback=bool(getattr(settings, "debug", False)))
    if job.status == "failed":
        # A custom 500 payload, so the client can show why the AI could not compile the analysis
        return JSONResponse(status_code=500, content=body)
    if job.status == "completed" and job.reportID is not None:
        reportUrl = request.app.url_path_for("getReportDetail", report_id=job.reportID)
        body["report_url"] = reportUrl
        if redirect:
            # 303 sends the poller to the finished report with a GET, matching the trigger-and-poll contract
            return RedirectResponse(url=reportUrl, status_code=303)
    return JSONResponse(status_code=200, content=body)


# 3. Fetching past diagnostics

@router.get("", name="listReports")
async def listReports(
    request: Request,
    species_id: Optional[int] = Query(None, gt=0),
    species: Optional[str] = None,
    h3_cell: Optional[str] = None,
    threat_tier: Optional[str] = None,
    min_escalation_level: Optional[int] = Query(None, ge=1, le=5),
    priority_tier: Optional[str] = None,
    min_cri: Optional[float] = Query(None, ge=0.0, le=100.0),
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    critical_inbreeding_only: bool = False,
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    databaseManager = getDatabase(request)
    conditions: list[str] = []
    parameters: list[object] = []
    if species_id is not None:
        conditions.append("r.species_id = ?")
        parameters.append(species_id)
    if species:
        # scientific_name is COLLATE NOCASE, so the subquery matches regardless of capitalisation
        conditions.append("r.species_id IN (SELECT id FROM species WHERE scientific_name = ?)")
        parameters.append(species.strip())
    if h3_cell:
        conditions.append("r.h3_cell = ?")
        parameters.append(h3_cell.strip().lower())
    if threat_tier:
        conditions.append("r.escalation_level = ?")
        parameters.append(escalationLevelForTier(threat_tier))
    if min_escalation_level is not None:
        conditions.append("r.escalation_level >= ?")
        parameters.append(min_escalation_level)
    if priority_tier:
        conditions.append("json_extract(r.input_summary, '$.intervention_priority_tier') = ?")
        parameters.append(priority_tier.strip().upper())
    if min_cri is not None:
        conditions.append("r.conservation_risk_index >= ?")
        parameters.append(min_cri)
    if start_date is not None:
        conditions.append("r.generated_at >= ?")
        parameters.append(formatUtcTimestamp(toUtcDatetime(start_date)))
    if end_date is not None:
        conditions.append("r.generated_at <= ?")
        parameters.append(formatUtcTimestamp(toUtcDatetime(end_date)))
    if critical_inbreeding_only:
        conditions.append("r.critical_inbreeding_risk = 1")
    whereClause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    totalRows = await databaseManager.fetchAll(
        f"SELECT COUNT(*) AS total FROM risk_assessments r {whereClause};", parameters
    )
    total = int(totalRows[0]["total"]) if totalRows else 0
    columns = ", ".join(f"r.{column}" for column in REPORT_COLUMNS)
    rows = await databaseManager.fetchAll(
        f"SELECT r.rowid AS id, {columns}, r.input_summary, r.narrative_report, "
        "s.scientific_name, s.common_name, s.iucn_status "
        f"FROM risk_assessments r LEFT JOIN species s ON s.id = r.species_id {whereClause} "
        "ORDER BY r.generated_at DESC, r.rowid DESC LIMIT ? OFFSET ?;",
        [*parameters, limit, offset],
    )
    return {
        "total": total,
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(rows) < total,
        "items": [formatReportRow(row, LIST_NARRATIVE_CHARS) for row in rows],
    }


# 4. Fetching specific details

async def hydrateSources(databaseManager: Any, tracingKeys: dict[str, Any]) -> dict[str, Any]:
    # Relational hydration: pull back the exact rows the graph read, so the UI can render them beside the narrative
    recordIDs = tracingKeys.get("record_ids") if isinstance(tracingKeys, dict) else None
    if not isinstance(recordIDs, dict):
        return {}
    sources: dict[str, Any] = {}
    for tableName, ids in recordIDs.items():
        if tableName not in TRACEABLE_TABLES or not isinstance(ids, (list, tuple)) or not ids:
            continue
        rowIDs = [int(recordID) for recordID in ids][:HYDRATION_ROW_LIMIT]
        placeholders = ", ".join("?" * len(rowIDs))
        try:
            rows = await databaseManager.fetchAll(
                f"SELECT * FROM {tableName} WHERE id IN ({placeholders}) ORDER BY id;", rowIDs
            )
        except sqlite3.OperationalError as error:
            # Some traceable sources are only present once their migration has been applied
            logger.warning("Skipped hydration of %s: %s", tableName, error)
            sources[tableName] = {"total": len(ids), "returned": 0, "truncated": False, "rows": [],
                                  "unavailable": str(error)}
            continue
        sources[tableName] = {
            "total": len(ids),
            "returned": len(rows),
            "truncated": len(ids) > len(rowIDs),
            "rows": [decodeJsonColumns(row) for row in rows],
        }
    return sources


async def buildReportDetail(databaseManager: Any, reportID: int, includeSources: bool) -> dict[str, Any]:
    columns = ", ".join(f"r.{column}" for column in REPORT_COLUMNS)
    rows = await databaseManager.fetchAll(
        f"SELECT r.rowid AS id, {columns}, r.input_summary, r.narrative_report, "
        "s.scientific_name, s.common_name, s.iucn_status "
        "FROM risk_assessments r LEFT JOIN species s ON s.id = r.species_id WHERE r.rowid = ?;",
        (reportID,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No report with id {reportID}.")
    report = formatReportRow(rows[0])
    summary: dict[str, Any] = report.get("input_summary") or {}
    report["run_id"] = summary.get("run_id")
    report["narrative_blocks"] = summary.get("narrative_blocks") or []
    report["intervention_routing"] = summary.get("intervention_routing")
    report["momentum_vector"] = summary.get("momentum_vector")
    report["subscore_components"] = summary.get("subscore_components")
    report["missing_domains"] = summary.get("missing_domains") or []
    report["domain_weights"] = summary.get("domain_weights")
    report["node_history"] = summary.get("node_history") or []
    report["pipeline_errors"] = summary.get("pipeline_errors") or []
    report["sources"] = (
        await hydrateSources(databaseManager, summary.get("tracing_keys") or {})
        if includeSources and HYDRATION_ENABLED else {}
    )
    return report


@router.get("/{report_id}", name="getReportDetail")
async def getReportDetail(
    request: Request,
    report_id: int,
    include_sources: bool = True,
) -> dict[str, Any]:
    databaseManager = getDatabase(request)
    cache: Optional[AsyncTtlCache] = getattr(request.app.state, "reportsCache", None)
    factory: Any = lambda: buildReportDetail(databaseManager, report_id, include_sources)
    if cache is None:
        return {**await factory(), "cached": False}
    # Reports are immutable once written, so a short TTL is enough to absorb repeated dashboard opens
    result, isCached = await cache.getOrCompute((report_id, include_sources), factory)
    return {**result, "cached": isCached}


async def main() -> None:
    # Demo: run the full API lifespan in-process, trigger an assessment, poll it, then browse the library
    from api.main import createApp

    app = createApp()
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        databaseManager = app.state.databaseManager
        speciesRows = await databaseManager.fetchAll("SELECT id, scientific_name FROM species ORDER BY id LIMIT 1;")
        if not speciesRows:
            print("No species rows yet; add one before triggering a report.")
            return
        print(f"Assessing species {speciesRows[0]['id']} ({speciesRows[0]['scientific_name']}).")
        async with httpx.AsyncClient(transport=transport, base_url="http://icmis.local") as client:
            trigger = {
                "species_id": speciesRows[0]["id"], "latitude": -1.30, "longitude": 36.80,
                "lookback_days": 30, "force": True,
            }
            response = await client.post("/api/v1/reports/generate", json=trigger)
            print(f"Trigger: {response.status_code} job={response.json().get('job_id')}")
            statusUrl = response.json()["status_url"]
            for _ in range(120):
                await asyncio.sleep(1.0)
                poll = await client.get(statusUrl, params={"redirect": "false"})
                body = poll.json()
                print(f"Poll: {poll.status_code} {body['status']} {body['progress']}% node={body['current_node']}")
                if body["status"] in ("completed", "failed"):
                    break
            response = await client.get("/api/v1/reports", params={"limit": 5})
            page = response.json()
            print(f"Library: {page['count']} of {page['total']} report(s)")
            if page["items"]:
                reportID = page["items"][0]["id"]
                response = await client.get(f"/api/v1/reports/{reportID}")
                detail = response.json()
                print(f"Report {reportID}: CRI={detail['scores']['conservation_risk_index']} "
                      f"tier={detail['scores']['escalation_tier']} sources={list(detail['sources'])}")
            response = await client.get("/api/v1/reports/jobs")
            print(f"Jobs: {response.json()['counters']}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())