import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from fastapi import APIRouter, Body
import glob
import hashlib
import h3
import io
import json
import logging
import os
import re
import shutil
import subprocess
from starlette.exceptions import HTTPException
import sys
import tempfile
from typing import Any, Callable, Optional
import yaml
import zoneinfo

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import DoubleQuotedScalarString, SingleQuotedScalarString

# Allow "python api/routesSettings.py" as well as loading through api/main.py
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from deploy.serviceCatalog import (  # noqa: E402
    API_SERVICE,
    APPLY_REQUEST_FILE,
    APPLY_STATUS_FILE,
    APPLY_TARGETS,
    PENDING_FILE,
    RESTARTABLE_SERVICES,
    SCHEDULE_TIMERS,
    SCHEDULES_SERVICE,
    SUPPORT_UNITS,
    SYSTEM_SERVICE,
    WORKER_LABELS,
    WORKER_SCRIPTS,
    checkWorkerReady,
    getSection,
    resolveProjectPath,
    unitForService,
)

SCHEDULE_LABELS: dict[str, str] = {"databaseBackup": "Database backup", "riskAssessment": "Scheduled risk assessment"}

# Load settings from YAML
CONFIG_PATH: str = os.path.abspath("config.yaml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SETTINGS_CONFIG: dict = config.get("settingsApi", {}) or {}

# Define storage settings for apply requests and config backups
STATE_DIRECTORY: str = resolveProjectPath(SETTINGS_CONFIG.get("stateDirectory", "~/icmis/state"))
BACKUP_DIRECTORY: str = resolveProjectPath(SETTINGS_CONFIG.get("backupDirectory", "~/icmis/configBackups"))
MAX_BACKUPS: int = int(SETTINGS_CONFIG.get("maxBackups", 20))

# Define pre-flight settings
PREFLIGHT_ENABLED: bool = bool(SETTINGS_CONFIG.get("preflightEnabled", True))
PREFLIGHT_TIMEOUT_SEC: float = float(SETTINGS_CONFIG.get("preflightTimeoutSec", 180.0))

if MAX_BACKUPS < 1:
    raise ValueError("settingsApi.maxBackups must be at least 1.")
if PREFLIGHT_TIMEOUT_SEC <= 0:
    raise ValueError("settingsApi.preflightTimeoutSec must be positive.")

# The installer puts this unit in place; without it Apply cannot restart anything
APPLY_UNIT_PATH: str = "/etc/systemd/system/icmis-apply.path"
ENV_PATH: str = os.path.join(PROJECT_ROOT, ".env")
BACKUP_NAME_PATTERN = re.compile(r"^config-\d{8}T\d{12}Z\.yaml$")

# Secrets are write-only: the API reports whether they are set but never returns them
SECRET_NAMES: dict[str, tuple[str, int]] = {
    "ICMIS_API_KEY": ("Dashboard / app API key", 16),
    "OPENAI_API_KEY": ("OpenAI API key (cloud LLM fallback)", 8),
    "ANTHROPIC_API_KEY": ("Anthropic API key (cloud LLM fallback)", 8),
}
SECRET_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9_\-.~+/=:]{1,512}$")

HOSTNAME_PATTERN = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$")
CALENDAR_PATTERN = re.compile(r"^[A-Za-z0-9*:,./~ -]{1,64}$")
HOST_PATTERN = re.compile(r"^[A-Za-z0-9.\-:\[\]]{1,253}$")
URL_PATTERN = re.compile(r"^https?://[^\s]{1,2000}$")
MQTT_TOPIC_PATTERN = re.compile(r"^[^\s\x00]{1,256}$")
DOMAIN_NAMES: tuple[str, ...] = ("population", "habitat", "threat", "climate", "genetics", "behavior")

ALL_WORKERS: tuple[str, ...] = tuple(WORKER_SCRIPTS)
logger = logging.getLogger("icmis.api.settings")

# One save at a time so two dashboards cannot interleave writes to config.yaml
settingsLock = asyncio.Lock()


# Setting catalogue: everything a user may change from the dashboard or the app

@dataclass
class SettingField:
    key: str
    label: str
    group: str
    valueType: str
    services: tuple[str, ...]
    description: str = ""
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    options: tuple[str, ...] = ()
    nullable: bool = False
    pattern: Optional[str] = None
    validator: Optional[str] = None

    def toSchema(self) -> dict:
        schema = asdict(self)
        schema["services"] = list(self.services)
        schema["options"] = list(self.options)
        return schema


SETTING_GROUPS: list[dict[str, str]] = [
    {"id": "system", "label": "Pi & network", "description": "Hostname, timezone and the MQTT broker the sensors publish to."},
    {"id": "services", "label": "Programs", "description": "Which background programs start automatically at boot."},
    {"id": "schedules", "label": "Schedules", "description": "Timed jobs that run without anyone logged in."},
    {"id": "storage", "label": "Storage", "description": "Where the database and incoming files live on the Pi."},
    {"id": "sensors", "label": "Sensors & camera", "description": "Field sensor thresholds and camera polling."},
    {"id": "vision", "label": "Vision model", "description": "Camera-trap object detection."},
    {"id": "acoustic", "label": "Acoustic monitoring", "description": "Microphones, hydrophones and the sound classifier."},
    {"id": "edna", "label": "eDNA & genetics", "description": "eDNA ingestion and population-genetics thresholds."},
    {"id": "risk", "label": "Risk engine", "description": "How the Conservation Risk Index is weighted and escalated."},
    {"id": "llm", "label": "AI language model", "description": "Local and cloud LLMs used to write the risk reports."},
    {"id": "api", "label": "Server & alerts", "description": "Dashboard access, webhooks and live alert thresholds."},
]

MQTT_WORKERS: tuple[str, ...] = ("gpsTelemetry", "environmentalSensors")

SETTING_FIELDS: list[SettingField] = [
    # Pi & network
    SettingField("system.hostname", "Hostname", "system", "string", (SYSTEM_SERVICE,),
                 "The Pi is reachable at http://<hostname>.local. Lowercase letters, digits and hyphens.",
                 pattern=HOSTNAME_PATTERN.pattern),
    SettingField("system.timezone", "Timezone", "system", "string", (SYSTEM_SERVICE,),
                 "IANA name such as Europe/London or Africa/Nairobi; used for logs and schedules.", validator="timezone"),
    SettingField("mqtt.brokerHost", "MQTT broker host", "system", "string", MQTT_WORKERS,
                 "localhost when Mosquitto runs on this Pi.", pattern=HOST_PATTERN.pattern),
    SettingField("mqtt.port", "MQTT broker port", "system", "integer", MQTT_WORKERS, minimum=1, maximum=65535),
    SettingField("mqtt.topics.environmental", "Environmental topic", "system", "string", MQTT_WORKERS,
                 "Topic filter the environmental listener subscribes to.", pattern=MQTT_TOPIC_PATTERN.pattern),
    SettingField("mqtt.topics.telemetry", "GPS telemetry topic", "system", "string", MQTT_WORKERS,
                 "Topic filter the GPS listener subscribes to.", pattern=MQTT_TOPIC_PATTERN.pattern),

    # Programs
    *[
        SettingField(f"services.{worker}.enabled", WORKER_LABELS[worker], "services", "boolean", (worker,),
                     f"Start {WORKER_SCRIPTS[worker]} at boot and keep it running.")
        for worker in ALL_WORKERS
    ],

    # Schedules
    SettingField("schedules.databaseBackup.enabled", "Database backups", "schedules", "boolean", (SCHEDULES_SERVICE,)),
    SettingField("schedules.databaseBackup.onCalendar", "Backup time", "schedules", "string", (SCHEDULES_SERVICE,),
                 "systemd calendar syntax, e.g. *-*-* 02:30:00 (daily) or Sun *-*-* 03:00:00 (weekly).", validator="calendar"),
    SettingField("schedules.databaseBackup.keepCopies", "Backups kept", "schedules", "integer", (SCHEDULES_SERVICE,),
                 minimum=1, maximum=365),
    SettingField("schedules.databaseBackup.directory", "Backup folder", "schedules", "path", (SCHEDULES_SERVICE,)),
    SettingField("schedules.riskAssessment.enabled", "Scheduled risk assessments", "schedules", "boolean", (SCHEDULES_SERVICE,)),
    SettingField("schedules.riskAssessment.onCalendar", "Assessment time", "schedules", "string", (SCHEDULES_SERVICE,),
                 "systemd calendar syntax.", validator="calendar"),
    SettingField("schedules.riskAssessment.defaultLookbackDays", "Assessment lookback (days)", "schedules", "number",
                 (SCHEDULES_SERVICE,), minimum=1, maximum=365),
    SettingField("schedules.riskAssessment.targets", "Assessment targets", "schedules", "json", (SCHEDULES_SERVICE,),
                 'List of {"speciesID": 1, "scientificName": "Loxodonta africana", "latitude": -2.33, "longitude": 34.82} '
                 'or {"speciesID": 1, "h3Cell": "87..."}.', validator="assessmentTargets"),

    # Storage
    SettingField("storage.databasePath", "Database file", "storage", "path", (API_SERVICE, *ALL_WORKERS),
                 "Moving it starts a fresh, empty database unless you copy the old file there first."),
    SettingField("filePaths.cameraTraps", "Camera-trap folder", "storage", "path", ("camera",)),
    SettingField("filePaths.droneUploads", "Drone upload folder", "storage", "path", ("camera",)),
    SettingField("filePaths.quarantine", "Camera quarantine folder", "storage", "path", ("camera",)),
    SettingField("acoustic.inputDirectory", "Acoustic recordings folder", "storage", "path", ("acousticStream",),
                 "Each recording goes in a subfolder named after its deviceID."),
    SettingField("edna.inputDirectory", "eDNA incoming folder", "storage", "path", ("ednaParser",)),
    SettingField("edna.quarantineDirectory", "eDNA quarantine folder", "storage", "path", ("ednaParser",)),

    # Sensors & camera
    SettingField("validation.batteryLowThreshold", "Low battery warning (%)", "sensors", "number", MQTT_WORKERS,
                 minimum=0, maximum=100),
    SettingField("camera.pollingIntervalSec", "Camera folder check interval (s)", "sensors", "number", ("camera",),
                 minimum=0.05, maximum=60),

    # Vision model
    SettingField("vision.modelPath", "Vision model (.onnx)", "vision", "path", ("visionInference",)),
    SettingField("vision.inputWidth", "Model input width", "vision", "integer", ("visionInference",), minimum=32, maximum=4096),
    SettingField("vision.inputHeight", "Model input height", "vision", "integer", ("visionInference",), minimum=32, maximum=4096),
    SettingField("vision.confidenceThreshold", "Detection confidence", "vision", "number", ("visionInference",), minimum=0, maximum=1),
    SettingField("vision.iouThreshold", "Overlap (IoU) threshold", "vision", "number", ("visionInference",), minimum=0, maximum=1),
    SettingField("vision.inferenceThreads", "CPU threads", "vision", "integer", ("visionInference",), minimum=1, maximum=4),
    SettingField("vision.classNames", "Class names", "vision", "stringList", ("visionInference",),
                 "In the same order as the trained model's class IDs. The vision program stays off until this is filled in."),
    SettingField("vision.threatClasses", "Threat classes", "vision", "stringList", ("visionInference",),
                 "Labels (from Class names) that raise high-priority alerts, e.g. person, vehicle."),

    # Acoustic monitoring
    SettingField("acoustic.liveEnabled", "Live microphone capture", "acoustic", "boolean", ("acousticStream",),
                 "Record continuously from a USB microphone on the Pi."),
    SettingField("acoustic.liveDeviceID", "Live microphone device ID", "acoustic", "string", ("acousticStream",)),
    SettingField("acoustic.liveInputSampleRate", "Microphone sample rate (Hz)", "acoustic", "integer", ("acousticStream",),
                 minimum=8000, maximum=192000),
    SettingField("acoustic.liveChannels", "Microphone channels", "acoustic", "integer", ("acousticStream",), minimum=1, maximum=8),
    SettingField("acoustic.devices", "Recording devices", "acoustic", "json", ("acousticStream", "acousticInference"),
                 'List of {"deviceID": "hydrophone-01", "locationID": "site-01", "latitude": 51.5, "longitude": -0.12, "altitude": 0}.',
                 validator="acousticDevices"),
    SettingField("acoustic.inference.modelPath", "Sound classifier (.onnx)", "acoustic", "path", ("acousticInference",)),
    SettingField("acoustic.inference.classMapPath", "Class map (.csv)", "acoustic", "path", ("acousticInference",)),
    SettingField("acoustic.inference.inferenceThreads", "CPU threads", "acoustic", "integer", ("acousticInference",),
                 minimum=1, maximum=4),
    SettingField("acoustic.inference.confidenceThreshold", "Wildlife confidence", "acoustic", "number", ("acousticInference",),
                 minimum=0, maximum=1),
    SettingField("acoustic.inference.threatConfidenceThreshold", "Threat confidence", "acoustic", "number",
                 ("acousticInference",), "Lower than wildlife so gunshots and chainsaws are not missed.", minimum=0, maximum=1),
    SettingField("acoustic.inference.wildlifeClasses", "Wildlife classes", "acoustic", "stringList", ("acousticInference",)),
    SettingField("acoustic.inference.threatClasses", "Threat classes", "acoustic", "stringList", ("acousticInference",),
                 "e.g. gunshot, chainsaw, vehicle engine (names must match the class map)."),
    SettingField("acoustic.inference.classThresholds", "Per-class thresholds", "acoustic", "json", ("acousticInference",),
                 'Optional {"gunshot": 0.3} overrides.', validator="probabilityMap"),

    # eDNA & genetics
    SettingField("edna.projectID", "eDNA project ID", "edna", "string", ("ednaParser",)),
    SettingField("edna.studyAreaBounds", "Study area bounds", "edna", "json", ("ednaParser",),
                 '{"minLatitude": ..., "maxLatitude": ..., "minLongitude": ..., "maxLongitude": ...}; samples outside are quarantined.',
                 validator="bounds"),
    SettingField("edna.approvedLoci", "Approved loci", "edna", "json", ("ednaParser",),
                 '{"Salmo salar": ["COI", "12S"]}. The eDNA program stays off until at least one species is listed.',
                 validator="approvedLoci"),
    SettingField("edna.geneticAnalysis.analysisIntervalSec", "Genetic analysis interval (s)", "edna", "number",
                 ("geneticAnalytics",), minimum=60, maximum=604800),
    SettingField("edna.geneticAnalysis.criticalNeThreshold", "Critical Ne", "edna", "number", ("geneticAnalytics", API_SERVICE),
                 "Effective population size below which a population is critical.", minimum=1),
    SettingField("edna.geneticAnalysis.vulnerableNeThreshold", "Vulnerable Ne", "edna", "number", ("geneticAnalytics", API_SERVICE),
                 minimum=1),
    SettingField("edna.geneticAnalysis.generationTimeYears", "Default generation time (years)", "edna", "number",
                 ("geneticAnalytics", API_SERVICE), minimum=0.01, maximum=200, nullable=True),
    SettingField("edna.geneticAnalysis.generationTimeYearsBySpecies", "Generation time by species", "edna", "json",
                 ("geneticAnalytics", API_SERVICE), '{"Loxodonta africana": 25}', validator="positiveMap"),

    # Risk engine
    SettingField("aiEngine.domainWeights", "CRI domain weights", "risk", "json", (API_SERVICE,),
                 "Six weights (population, habitat, threat, climate, genetics, behavior) that sum to 1.0.", validator="domainWeights"),
    SettingField("aiEngine.graphWorkflow.speciesDomainWeights", "Per-species domain weights", "risk", "json", (API_SERVICE,),
                 'Species ID or scientific name -> six weights summing to 1.0.', validator="speciesDomainWeights"),
    SettingField("aiEngine.graphWorkflow.criticalCriThreshold", "Critical CRI (0-1)", "risk", "number", (API_SERVICE,),
                 "0.8 == CRI 80; at or above this the engine recommends emergency interventions.", minimum=0, maximum=1),
    SettingField("aiEngine.criticalNeThreshold", "Genetics critical Ne", "risk", "number", (API_SERVICE,), minimum=1),
    SettingField("aiEngine.graphWorkflow.rangerOutposts", "Ranger outposts", "risk", "json", (API_SERVICE,),
                 '[{"name": "HQ", "latitude": -2.3, "longitude": 34.8}] used to rank intervention feasibility.',
                 validator="outposts"),

    # AI language model
    SettingField("aiEngine.llmProvider.routingMode", "Routing mode", "llm", "enum", (API_SERVICE,),
                 "auto = local first, cloud when the Pi is busy or hot.", options=("auto", "local", "cloud", "none")),
    SettingField("aiEngine.llmProvider.localBackend", "Local backend", "llm", "enum", (API_SERVICE,),
                 options=("ollama", "llamacpp", "none")),
    SettingField("aiEngine.llmProvider.ollamaModel", "Ollama model", "llm", "string", (API_SERVICE,),
                 "Pull it first with: ollama pull <model>.", pattern=r"^[A-Za-z0-9._:/\-]{1,128}$"),
    SettingField("aiEngine.llmProvider.ollamaBaseUrl", "Ollama URL", "llm", "string", (API_SERVICE,), pattern=URL_PATTERN.pattern),
    SettingField("aiEngine.llmProvider.ggufModelPath", "GGUF model (llamacpp)", "llm", "path", (API_SERVICE,)),
    SettingField("aiEngine.llmProvider.localThreads", "Local LLM threads", "llm", "integer", (API_SERVICE,), minimum=1, maximum=4),
    SettingField("aiEngine.llmProvider.maxCpuTempC", "Max CPU temperature (°C)", "llm", "number", (API_SERVICE,),
                 "Local inference pauses above this.", minimum=50, maximum=95),
    SettingField("aiEngine.llmProvider.minFreeRamGb", "Min free RAM (GB)", "llm", "number", (API_SERVICE,), minimum=0.5, maximum=16),
    SettingField("aiEngine.llmProvider.cloudProvider", "Cloud provider", "llm", "enum", (API_SERVICE,),
                 "Needs the matching API key under Secrets.", options=("openai", "anthropic", "none")),
    SettingField("aiEngine.llmProvider.openaiModel", "OpenAI model", "llm", "string", (API_SERVICE,), pattern=r"^[A-Za-z0-9._:\-]{1,128}$"),
    SettingField("aiEngine.llmProvider.anthropicModel", "Anthropic model", "llm", "string", (API_SERVICE,),
                 pattern=r"^[A-Za-z0-9._:\-]{1,128}$"),

    # Server & alerts
    SettingField("api.allowedOrigins", "Allowed browser origins", "api", "stringList", (API_SERVICE,),
                 "Only needed for dashboards hosted somewhere other than this Pi. '*' is not allowed.", validator="origins"),
    SettingField("api.enableDocs", "Interactive API docs (/docs)", "api", "boolean", (API_SERVICE,)),
    SettingField("api.logLevel", "Log level", "api", "enum", (API_SERVICE,), options=("DEBUG", "INFO", "WARNING", "ERROR")),
    SettingField("api.llmWarmUp", "LLM warm-up at start", "api", "enum", (API_SERVICE,), options=("background", "blocking", "off")),
    SettingField("telemetryApi.threatWebhookUrl", "Threat webhook URL", "api", "string", (API_SERVICE,),
                 "Every critical payload is POSTed here (leave empty for none).", validator="optionalUrl"),
    SettingField("telemetryApi.wakeAiEngine", "Assess threats immediately", "api", "boolean", (API_SERVICE,),
                 "Critical sensor events start a risk assessment for their area."),
    SettingField("telemetryApi.defaultSpeciesID", "Default species ID", "api", "integer", (API_SERVICE,),
                 "Species assessed when a threat has no species attached (empty = skip).", minimum=1, nullable=True),
    SettingField("websocketApi.criCriticalThreshold", "CRITICAL alert at CRI", "api", "number", (API_SERVICE,), minimum=0, maximum=100),
    SettingField("websocketApi.criHighThreshold", "HIGH alert at CRI", "api", "number", (API_SERVICE,), minimum=0, maximum=100),
    SettingField("websocketApi.criElevatedThreshold", "ELEVATED alert at CRI", "api", "number", (API_SERVICE,), minimum=0, maximum=100),
    SettingField("websocketApi.forwardTelemetry", "Stream raw telemetry", "api", "boolean", (API_SERVICE,),
                 "Push every reading to live clients, not just threats (noisy)."),
    SettingField("websocketApi.requireToken", "Require key for live alerts", "api", "boolean", (API_SERVICE,),
                 "Live alert sockets must present the API key."),
]

FIELDS_BY_KEY: dict[str, SettingField] = {settingField.key: settingField for settingField in SETTING_FIELDS}

# Top-level section -> services that read it; used when a rollback swaps the whole file
SECTION_SERVICES: dict[str, tuple[str, ...]] = {
    "mqtt": MQTT_WORKERS,
    "storage": (API_SERVICE, *ALL_WORKERS),
    "database": (API_SERVICE, *ALL_WORKERS),
    "filePaths": ("camera",),
    "validation": MQTT_WORKERS,
    "emaAlphas": ("environmentalSensors",),
    "camera": ("camera",),
    "vision": ("visionInference",),
    "acoustic": ("acousticStream", "acousticInference"),
    "edna": ("ednaParser", "geneticAnalytics", API_SERVICE),
    "services": ALL_WORKERS,
    "schedules": (SCHEDULES_SERVICE,),
    "system": (SYSTEM_SERVICE,),
}


# Path helpers shared by the plain dict and the comment-preserving ruamel tree

def getPath(tree: Any, key: str, default: Any = None) -> Any:
    node = tree
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node


def setPath(tree: dict, key: str, value: Any, mapFactory: Callable[[], dict] = dict) -> None:
    parts = key.split(".")
    node = tree
    for part in parts[:-1]:
        if not isinstance(node.get(part), dict):
            node[part] = mapFactory()
        node = node[part]
    node[parts[-1]] = value


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def servicesForSections(before: dict, after: dict) -> set[str]:
    services: set[str] = set()
    for section in set(before) | set(after):
        if canonical(before.get(section)) != canonical(after.get(section)):
            services.update(SECTION_SERVICES.get(section, (API_SERVICE,)))
    return services


# Value validation: type coercion first, then the per-field rule, then cross-field checks

def requireNumber(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a number.")
    return float(value)


def requireCoordinates(item: dict, label: str) -> None:
    latitude = requireNumber(item.get("latitude"), f"{label} latitude")
    longitude = requireNumber(item.get("longitude"), f"{label} longitude")
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ValueError(f"{label} coordinates are out of range.")


def validateWeights(weights: Any, label: str) -> dict:
    if not isinstance(weights, dict) or set(weights) != set(DOMAIN_NAMES):
        raise ValueError(f"{label} needs exactly these keys: {', '.join(DOMAIN_NAMES)}.")
    values = {name: requireNumber(weights[name], f"{label}.{name}") for name in DOMAIN_NAMES}
    if any(weight < 0 for weight in values.values()):
        raise ValueError(f"{label} weights cannot be negative.")
    if abs(sum(values.values()) - 1.0) > 1e-3:
        raise ValueError(f"{label} weights must sum to 1.0 (they sum to {sum(values.values()):.3f}).")
    return {name: weights[name] for name in DOMAIN_NAMES}


def validateTimezone(value: str) -> str:
    available = zoneinfo.available_timezones()
    # Windows without the tzdata package reports no zones; the Pi always has them
    if available and value not in available:
        raise ValueError(f"Unknown timezone {value!r}; use an IANA name such as Europe/London.")
    return value


def validateCalendar(value: str) -> str:
    if not CALENDAR_PATTERN.match(value):
        raise ValueError("Schedule may only contain letters, digits, spaces and * : , . / ~ -")
    return value


def validateAcousticDevices(value: Any) -> list:
    if not isinstance(value, list):
        raise ValueError("Recording devices must be a list.")
    seen: set[str] = set()
    for index, device in enumerate(value):
        if not isinstance(device, dict) or not str(device.get("deviceID", "")).strip():
            raise ValueError(f"Device {index + 1} needs a deviceID.")
        if device["deviceID"] in seen:
            raise ValueError(f"deviceID {device['deviceID']!r} is listed twice.")
        seen.add(device["deviceID"])
        requireCoordinates(device, f"Device {device['deviceID']}")
        if device.get("altitude") is not None:
            requireNumber(device["altitude"], f"Device {device['deviceID']} altitude")
    return value


def validateAssessmentTargets(value: Any) -> list:
    if not isinstance(value, list):
        raise ValueError("Assessment targets must be a list.")
    for index, target in enumerate(value):
        label = f"Target {index + 1}"
        if not isinstance(target, dict):
            raise ValueError(f"{label} must be an object.")
        speciesID = target.get("speciesID")
        if isinstance(speciesID, bool) or not isinstance(speciesID, int) or speciesID < 1:
            raise ValueError(f"{label} needs a positive integer speciesID.")
        if target.get("h3Cell"):
            if not h3.is_valid_cell(str(target["h3Cell"]).strip().lower()):
                raise ValueError(f"{label} h3Cell is not a valid H3 index.")
        else:
            requireCoordinates(target, label)
        if target.get("lookbackDays") is not None and requireNumber(target["lookbackDays"], f"{label} lookbackDays") <= 0:
            raise ValueError(f"{label} lookbackDays must be positive.")
    return value


def validateBounds(value: Any) -> dict:
    keys = ("minLatitude", "maxLatitude", "minLongitude", "maxLongitude")
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError(f"Study area bounds need exactly {', '.join(keys)}.")
    minLatitude, maxLatitude, minLongitude, maxLongitude = (requireNumber(value[key], key) for key in keys)
    if not -90 <= minLatitude < maxLatitude <= 90:
        raise ValueError("Latitudes must satisfy -90 <= minLatitude < maxLatitude <= 90.")
    if not -180 <= minLongitude < maxLongitude <= 180:
        raise ValueError("Longitudes must satisfy -180 <= minLongitude < maxLongitude <= 180.")
    return value


def validateApprovedLoci(value: Any) -> dict:
    if not isinstance(value, dict):
        raise ValueError("Approved loci must map a species name to a list of loci.")
    for species, loci in value.items():
        if not str(species).strip() or not isinstance(loci, list) or not loci:
            raise ValueError(f"{species!r} needs a non-empty list of loci.")
        if any(not isinstance(locus, str) or not locus.strip() for locus in loci):
            raise ValueError(f"Loci for {species!r} must be non-empty text.")
    return value


def validateProbabilityMap(value: Any) -> dict:
    if not isinstance(value, dict):
        raise ValueError("Expected an object of name -> threshold.")
    for name, threshold in value.items():
        if not 0 <= requireNumber(threshold, str(name)) <= 1:
            raise ValueError(f"Threshold for {name!r} must be between 0 and 1.")
    return value


def validatePositiveMap(value: Any) -> dict:
    if not isinstance(value, dict):
        raise ValueError("Expected an object of species -> years.")
    for name, years in value.items():
        if requireNumber(years, str(name)) <= 0:
            raise ValueError(f"Value for {name!r} must be positive.")
    return value


def validateSpeciesDomainWeights(value: Any) -> dict:
    if not isinstance(value, dict):
        raise ValueError("Expected an object of species -> domain weights.")
    for species, weights in value.items():
        validateWeights(weights, f"Weights for {species}")
    return value


def validateOutposts(value: Any) -> list:
    if not isinstance(value, list):
        raise ValueError("Ranger outposts must be a list.")
    for index, outpost in enumerate(value):
        if not isinstance(outpost, dict) or not str(outpost.get("name", "")).strip():
            raise ValueError(f"Outpost {index + 1} needs a name.")
        requireCoordinates(outpost, f"Outpost {outpost['name']}")
    return value


def validateOrigins(value: list) -> list:
    if "*" in value:
        raise ValueError("'*' is not allowed because the API sends credentials; list each origin.")
    for origin in value:
        if not URL_PATTERN.match(origin):
            raise ValueError(f"{origin!r} must look like http://host:port.")
    return [origin.rstrip("/") for origin in value]


def validateOptionalUrl(value: str) -> str:
    if value and not URL_PATTERN.match(value):
        raise ValueError("Must be empty or start with http:// or https://.")
    return value


FIELD_VALIDATORS: dict[str, Callable[[Any], Any]] = {
    "timezone": validateTimezone,
    "calendar": validateCalendar,
    "acousticDevices": validateAcousticDevices,
    "assessmentTargets": validateAssessmentTargets,
    "bounds": validateBounds,
    "approvedLoci": validateApprovedLoci,
    "probabilityMap": validateProbabilityMap,
    "positiveMap": validatePositiveMap,
    "domainWeights": lambda value: validateWeights(value, "Domain weights"),
    "speciesDomainWeights": validateSpeciesDomainWeights,
    "outposts": validateOutposts,
    "origins": validateOrigins,
    "optionalUrl": validateOptionalUrl,
}


def coerceValue(settingField: SettingField, value: Any) -> Any:
    if value is None or (value == "" and settingField.valueType in ("integer", "number")):
        if settingField.nullable:
            return None
        raise ValueError("A value is required.")

    valueType = settingField.valueType
    if valueType == "boolean":
        if not isinstance(value, bool):
            raise ValueError("Must be true or false.")
    elif valueType in ("integer", "number"):
        number = requireNumber(value, "Value")
        if valueType == "integer":
            if not number.is_integer():
                raise ValueError("Must be a whole number.")
            value = int(number)
        else:
            value = int(number) if isinstance(value, int) else number
        if settingField.minimum is not None and number < settingField.minimum:
            raise ValueError(f"Must be at least {settingField.minimum:g}.")
        if settingField.maximum is not None and number > settingField.maximum:
            raise ValueError(f"Must be at most {settingField.maximum:g}.")
    elif valueType in ("string", "path", "enum"):
        if not isinstance(value, str):
            raise ValueError("Must be text.")
        value = value.strip()
        if "\x00" in value or "\n" in value or len(value) > 1024:
            raise ValueError("Text contains invalid characters or is too long.")
        if valueType == "path" and not value:
            raise ValueError("A path is required.")
        if valueType == "enum" and value not in settingField.options:
            raise ValueError(f"Must be one of {', '.join(settingField.options)}.")
        if settingField.pattern and not re.fullmatch(settingField.pattern, value):
            raise ValueError("Value has an invalid format.")
    elif valueType == "stringList":
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            raise ValueError("Must be a list of text values.")
        value = [item.strip() for item in value if item.strip()]
        if len(set(value)) != len(value):
            raise ValueError("List contains duplicates.")
    elif valueType == "json":
        if not isinstance(value, (dict, list)):
            raise ValueError("Must be a JSON object or list.")

    if settingField.validator:
        value = FIELD_VALIDATORS[settingField.validator](value)
    return value


def crossValidate(candidate: dict) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    elevated = getPath(candidate, "websocketApi.criElevatedThreshold", 40.0)
    high = getPath(candidate, "websocketApi.criHighThreshold", 60.0)
    critical = getPath(candidate, "websocketApi.criCriticalThreshold", 80.0)
    if not float(elevated) < float(high) < float(critical):
        errors.append({"key": "websocketApi.criHighThreshold", "message": "Alert thresholds must rise: ELEVATED < HIGH < CRITICAL."})

    criticalNe = getPath(candidate, "edna.geneticAnalysis.criticalNeThreshold", 50.0)
    vulnerableNe = getPath(candidate, "edna.geneticAnalysis.vulnerableNeThreshold", 500.0)
    if float(criticalNe) >= float(vulnerableNe):
        errors.append({"key": "edna.geneticAnalysis.vulnerableNeThreshold", "message": "Vulnerable Ne must be larger than critical Ne."})

    classNames = getPath(candidate, "vision.classNames", []) or []
    unknownThreats = [name for name in getPath(candidate, "vision.threatClasses", []) or [] if classNames and name not in classNames]
    if unknownThreats:
        errors.append({"key": "vision.threatClasses", "message": f"Not in Class names: {', '.join(unknownThreats)}."})
    return errors


# File handling: revisions, backups, atomic writes and the round-trip YAML writer

def readConfigText() -> str:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return f.read()


def revisionOf(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def atomicWrite(path: str, text: str, mode: Optional[int] = None) -> None:
    # A power cut mid-save must never leave a half-written config.yaml behind
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    descriptor, temporaryPath = tempfile.mkstemp(dir=directory, prefix=".tmp-")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        if mode is not None:
            os.chmod(temporaryPath, mode)
        os.replace(temporaryPath, path)
    except BaseException:
        if os.path.exists(temporaryPath):
            os.remove(temporaryPath)
        raise


def createRoundTripYaml() -> YAML:
    roundTrip = YAML()
    roundTrip.preserve_quotes = True
    roundTrip.width = 4096
    # Matches the existing layout: two-space maps, list dashes indented under their key
    roundTrip.indent(mapping=2, sequence=4, offset=2)
    return roundTrip


def toRoundTripValue(oldValue: Any, newValue: Any) -> Any:
    # Keep each value's original quoting and flow style so the file still reads like it was hand-written
    if isinstance(newValue, str):
        if isinstance(oldValue, DoubleQuotedScalarString) or (oldValue is None or not isinstance(oldValue, str)):
            return DoubleQuotedScalarString(newValue)
        if isinstance(oldValue, SingleQuotedScalarString):
            return SingleQuotedScalarString(newValue)
        return newValue
    if isinstance(newValue, list) and all(not isinstance(item, (dict, list)) for item in newValue):
        sequence = CommentedSeq([toRoundTripValue(None, item) for item in newValue])
        sequence.fa.set_flow_style()
        return sequence
    if isinstance(newValue, dict) and not newValue:
        emptyMap = CommentedMap()
        emptyMap.fa.set_flow_style()
        return emptyMap
    return newValue


def renderConfig(text: str, changes: dict[str, Any]) -> str:
    roundTrip = createRoundTripYaml()
    tree = roundTrip.load(text)
    for key, value in changes.items():
        setPath(tree, key, toRoundTripValue(getPath(tree, key), value), CommentedMap)
    buffer = io.StringIO()
    roundTrip.dump(tree, buffer)
    return buffer.getvalue()


def backupConfig(text: str) -> str:
    os.makedirs(BACKUP_DIRECTORY, exist_ok=True)
    name = f"config-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}Z.yaml"
    atomicWrite(os.path.join(BACKUP_DIRECTORY, name), text)
    for oldPath in sorted(glob.glob(os.path.join(BACKUP_DIRECTORY, "config-*Z.yaml")))[:-MAX_BACKUPS]:
        os.remove(oldPath)
    return name


def listBackups() -> list[dict[str, Any]]:
    if not os.path.isdir(BACKUP_DIRECTORY):
        return []
    backups = []
    for name in sorted(os.listdir(BACKUP_DIRECTORY), reverse=True):
        if BACKUP_NAME_PATTERN.match(name):
            path = os.path.join(BACKUP_DIRECTORY, name)
            stamp = datetime.strptime(name[7:22], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
            backups.append({"name": name, "createdAt": stamp.isoformat(), "sizeBytes": os.path.getsize(path)})
    return backups


# State shared with the root apply helper

def readStateFile(name: str, default: Any) -> Any:
    try:
        with open(os.path.join(STATE_DIRECTORY, name), "r") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def writeStateFile(name: str, payload: dict) -> None:
    atomicWrite(os.path.join(STATE_DIRECTORY, name), json.dumps(payload, indent=2))


def getPendingServices() -> list[str]:
    services = readStateFile(PENDING_FILE, {}).get("services", [])
    return [service for service in APPLY_TARGETS if service in services]


def addPendingServices(services: set[str]) -> list[str]:
    pending = sorted(set(getPendingServices()) | services, key=APPLY_TARGETS.index)
    writeStateFile(PENDING_FILE, {"services": pending, "updatedAt": datetime.now(timezone.utc).isoformat()})
    return pending


# Pre-flight: import every affected module against the candidate file in a throwaway process

PREFLIGHT_SCRIPT: str = """
import importlib, importlib.util, sys
failures = []
for name, target in zip(sys.argv[1::2], sys.argv[2::2]):
    try:
        if target.endswith(".py"):
            spec = importlib.util.spec_from_file_location("preflight_" + name, target)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            importlib.import_module(target)
    except BaseException as error:
        failures.append(f"{name}: {type(error).__name__}: {error}")
print("\\n".join(failures))
sys.exit(1 if failures else 0)
"""


def preflightTargets(services: set[str]) -> list[str]:
    arguments: list[str] = []
    for service in sorted(services):
        if service == API_SERVICE:
            arguments += [API_SERVICE, "api.main"]
        elif service in WORKER_SCRIPTS:
            arguments += [service, os.path.join(PROJECT_ROOT, WORKER_SCRIPTS[service])]
        elif service == SCHEDULES_SERVICE:
            arguments += ["databaseBackup", os.path.join(PROJECT_ROOT, "deploy", "backupDatabase.py")]
            arguments += ["riskAssessment", os.path.join(PROJECT_ROOT, "deploy", "scheduledAssessment.py")]
    return arguments


def runPreflight(candidateText: str, services: set[str]) -> list[str]:
    targets = preflightTargets(services)
    if not PREFLIGHT_ENABLED or not targets:
        return []
    with tempfile.TemporaryDirectory(prefix="icmisPreflight-") as workDirectory:
        with open(os.path.join(workDirectory, "config.yaml"), "w", encoding="utf-8") as f:
            f.write(candidateText)
        environment = {**os.environ, "PYTHONPATH": PROJECT_ROOT, "PYTHONDONTWRITEBYTECODE": "1"}
        try:
            result = subprocess.run(
                [sys.executable, "-c", PREFLIGHT_SCRIPT, *targets],
                cwd=workDirectory, env=environment, capture_output=True, text=True, timeout=PREFLIGHT_TIMEOUT_SEC,
            )
        except subprocess.TimeoutExpired:
            return [f"Pre-flight check timed out after {PREFLIGHT_TIMEOUT_SEC:g}s; raise settingsApi.preflightTimeoutSec."]
    if result.returncode == 0:
        return []
    failures = [line for line in result.stdout.splitlines() if line.strip()]
    return failures or [result.stderr.strip()[-2000:] or "Pre-flight check failed."]


# systemd status (read-only, works unprivileged)

def querySystemd(units: list[str]) -> dict[str, dict[str, str]]:
    if not shutil.which("systemctl"):
        return {}
    properties = "Id,LoadState,ActiveState,SubState,UnitFileState,Result,ActiveEnterTimestamp,NextElapseUSecRealtime,LastTriggerUSec"
    try:
        result = subprocess.run(["systemctl", "show", f"--property={properties}", "--", *units],
                                capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return {}
    states: dict[str, dict[str, str]] = {}
    for block in result.stdout.strip().split("\n\n"):
        entry = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        if entry.get("Id"):
            states[entry["Id"]] = entry
    return states


def buildStatus() -> dict[str, Any]:
    currentConfig = yaml.safe_load(readConfigText()) or {}
    services = [API_SERVICE, *ALL_WORKERS]
    timerUnits = [timer for timer, _ in SCHEDULE_TIMERS.values()]
    units = [unitForService(service) for service in services] + timerUnits + list(SUPPORT_UNITS.values())
    states = querySystemd(units)

    def describe(unit: str) -> dict[str, Any]:
        state = states.get(unit)
        if not state:
            return {"unit": unit, "available": False}
        return {
            "unit": unit,
            "available": state.get("LoadState") == "loaded",
            "activeState": state.get("ActiveState"),
            "subState": state.get("SubState"),
            "enabled": state.get("UnitFileState"),
            "result": state.get("Result"),
            "since": state.get("ActiveEnterTimestamp") or None,
            "nextRun": state.get("NextElapseUSecRealtime") or None,
            "lastRun": state.get("LastTriggerUSec") or None,
        }

    workers = []
    for worker in ALL_WORKERS:
        ready, reason = checkWorkerReady(currentConfig, worker)
        workers.append({"service": worker, "label": WORKER_LABELS[worker], "ready": ready, "reason": reason,
                        **describe(unitForService(worker))})
    return {
        "systemdAvailable": bool(states),
        "applyAvailable": os.path.exists(APPLY_UNIT_PATH),
        "applyInProgress": os.path.exists(os.path.join(STATE_DIRECTORY, APPLY_REQUEST_FILE)),
        "pending": getPendingServices(),
        "lastApply": readStateFile(APPLY_STATUS_FILE, None),
        "api": describe(unitForService(API_SERVICE)),
        "workers": workers,
        "schedules": [{"schedule": key, "label": SCHEDULE_LABELS.get(key, key),
                       "configEnabled": bool(getSection(currentConfig, "schedules").get(key, {}).get("enabled", True)),
                       "onCalendar": getSection(currentConfig, "schedules").get(key, {}).get("onCalendar"),
                       **describe(timer)} for key, (timer, _) in SCHEDULE_TIMERS.items()],
        "support": [{"name": name, **describe(unit)} for name, unit in SUPPORT_UNITS.items()],
    }


def buildValues(text: str) -> dict[str, Any]:
    plainConfig = yaml.safe_load(text) or {}
    return {
        "values": {settingField.key: getPath(plainConfig, settingField.key) for settingField in SETTING_FIELDS},
        "revision": revisionOf(text),
        "pending": getPendingServices(),
    }


# .env secrets

def readEnvLines() -> list[str]:
    try:
        with open(ENV_PATH, "r", encoding="utf-8") as f:
            return f.read().splitlines()
    except FileNotFoundError:
        return []


def secretState() -> dict[str, dict[str, Any]]:
    present = {line.split("=", 1)[0].strip() for line in readEnvLines() if "=" in line and not line.lstrip().startswith("#")}
    return {name: {"label": label, "isSet": name in present, "minLength": minLength}
            for name, (label, minLength) in SECRET_NAMES.items()}


router = APIRouter()


@router.get("/schema")
async def getSchema() -> dict[str, Any]:
    timezones = sorted(zoneinfo.available_timezones())
    return {
        "groups": SETTING_GROUPS,
        "fields": [settingField.toSchema() for settingField in SETTING_FIELDS],
        "services": [{"service": worker, "label": WORKER_LABELS[worker]} for worker in ALL_WORKERS],
        "timezones": timezones,
        "secrets": secretState(),
    }


@router.get("")
async def getSettings() -> dict[str, Any]:
    return buildValues(await asyncio.to_thread(readConfigText))


@router.patch("")
async def updateSettings(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    values = payload.get("values")
    revision = payload.get("revision")
    if not isinstance(values, dict) or not values:
        raise HTTPException(status_code=422, detail="Send {\"values\": {<key>: <value>}, \"revision\": <revision>}.")

    async with settingsLock:
        text = await asyncio.to_thread(readConfigText)
        if revision and revision != revisionOf(text):
            raise HTTPException(status_code=409, detail="Settings were changed somewhere else; reload and try again.")

        # Field-level validation, reporting every problem at once
        currentConfig = yaml.safe_load(text) or {}
        errors: list[dict[str, str]] = []
        changes: dict[str, Any] = {}
        for key, value in values.items():
            settingField = FIELDS_BY_KEY.get(key)
            if settingField is None:
                errors.append({"key": key, "message": "This setting cannot be changed remotely."})
                continue
            try:
                coerced = coerceValue(settingField, value)
            except ValueError as error:
                errors.append({"key": key, "message": str(error)})
                continue
            if canonical(coerced) != canonical(getPath(currentConfig, key)):
                changes[key] = coerced
        if errors:
            raise HTTPException(status_code=422, detail=errors)
        if not changes:
            return {**buildValues(text), "changed": [], "services": []}

        candidate = yaml.safe_load(text) or {}
        for key, value in changes.items():
            setPath(candidate, key, value)
        errors = crossValidate(candidate)
        if errors:
            raise HTTPException(status_code=422, detail=errors)

        candidateText = renderConfig(text, changes)
        rendered = yaml.safe_load(candidateText) or {}
        if any(canonical(getPath(rendered, key)) != canonical(value) for key, value in changes.items()):
            raise HTTPException(status_code=500, detail="Rendered config.yaml did not round-trip; nothing was saved.")

        services = {service for key in changes for service in FIELDS_BY_KEY[key].services}
        failures = await asyncio.to_thread(runPreflight, candidateText, services)
        if failures:
            raise HTTPException(status_code=422, detail=[{"key": "preflight", "message": failure} for failure in failures])

        backupName = await asyncio.to_thread(backupConfig, text)
        await asyncio.to_thread(atomicWrite, CONFIG_PATH, candidateText)
        pending = await asyncio.to_thread(addPendingServices, services)
        logger.info("Settings saved (%s); backup %s; pending %s", ", ".join(sorted(changes)), backupName, pending)

    return {**buildValues(candidateText), "changed": sorted(changes), "services": sorted(services), "backup": backupName}


@router.post("/apply")
async def applySettings(payload: Optional[dict[str, Any]] = Body(None)) -> dict[str, Any]:
    requested = (payload or {}).get("services")
    if requested is None:
        services = getPendingServices()
    elif isinstance(requested, list) and all(service in APPLY_TARGETS for service in requested):
        services = [service for service in APPLY_TARGETS if service in requested]
    else:
        raise HTTPException(status_code=422, detail=f"services must be a list drawn from {list(APPLY_TARGETS)}.")
    if not services:
        return {"queued": [], "message": "Nothing to apply."}

    if not os.path.exists(APPLY_UNIT_PATH):
        units = " ".join(unitForService(service) for service in services if service in RESTARTABLE_SERVICES)
        raise HTTPException(status_code=503, detail=(
            "Automatic apply is not installed; run 'sudo bash deploy/installServices.sh' on the Pi, "
            f"or restart manually with: sudo systemctl restart {units or 'icmis.target'}"
        ))

    # The request file is what icmis-apply.path watches; the root helper deletes it once read
    async with settingsLock:
        writeStateFile(APPLY_STATUS_FILE, {"state": "queued", "services": services,
                                           "queuedAt": datetime.now(timezone.utc).isoformat()})
        writeStateFile(APPLY_REQUEST_FILE, {"services": services})
        remaining = [service for service in getPendingServices() if service not in services]
        writeStateFile(PENDING_FILE, {"services": remaining, "updatedAt": datetime.now(timezone.utc).isoformat()})
    logger.info("Apply queued for %s", services)
    return {"queued": services, "apiRestarting": API_SERVICE in services}


@router.get("/status")
async def getStatus() -> dict[str, Any]:
    return await asyncio.to_thread(buildStatus)


@router.get("/backups")
async def getBackups() -> dict[str, Any]:
    return {"backups": await asyncio.to_thread(listBackups)}


@router.post("/rollback")
async def rollbackSettings(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    name = str(payload.get("name", ""))
    # Only names this router created, so the request cannot point at any other file
    if not BACKUP_NAME_PATTERN.match(name):
        raise HTTPException(status_code=422, detail="Unknown backup name.")
    backupPath = os.path.join(BACKUP_DIRECTORY, name)
    if not os.path.isfile(backupPath):
        raise HTTPException(status_code=404, detail=f"Backup {name} not found.")

    async with settingsLock:
        text = await asyncio.to_thread(readConfigText)
        with open(backupPath, "r", encoding="utf-8") as f:
            backupText = f.read()
        try:
            services = servicesForSections(yaml.safe_load(text) or {}, yaml.safe_load(backupText) or {})
        except yaml.YAMLError as error:
            raise HTTPException(status_code=422, detail=f"Backup is not valid YAML: {error}")
        failures = await asyncio.to_thread(runPreflight, backupText, services)
        if failures:
            raise HTTPException(status_code=422, detail=[{"key": "preflight", "message": failure} for failure in failures])
        safetyName = await asyncio.to_thread(backupConfig, text)
        await asyncio.to_thread(atomicWrite, CONFIG_PATH, backupText)
        await asyncio.to_thread(addPendingServices, services)

    return {**buildValues(backupText), "restored": name, "backup": safetyName, "services": sorted(services)}


@router.get("/secrets")
async def getSecrets() -> dict[str, Any]:
    return {"secrets": secretState()}


@router.put("/secrets")
async def updateSecret(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    name = str(payload.get("name", ""))
    value = payload.get("value")
    if name not in SECRET_NAMES:
        raise HTTPException(status_code=422, detail=f"name must be one of {list(SECRET_NAMES)}.")
    if not isinstance(value, str):
        raise HTTPException(status_code=422, detail="value must be text (empty removes the secret).")
    value = value.strip()
    minLength = SECRET_NAMES[name][1]
    if value and (not SECRET_VALUE_PATTERN.match(value) or len(value) < minLength):
        raise HTTPException(status_code=422, detail=f"{name} must be at least {minLength} characters of A-Z a-z 0-9 _ - . ~ + / = :")
    if name == "ICMIS_API_KEY" and not value:
        raise HTTPException(status_code=422, detail="The API key cannot be removed remotely; that would unlock Settings for everyone.")

    async with settingsLock:
        lines = [line for line in readEnvLines() if line.split("=", 1)[0].strip() != name]
        if value:
            lines.append(f"{name}={value}")
        await asyncio.to_thread(atomicWrite, ENV_PATH, "\n".join(lines) + "\n", 0o600)
        # The API reads .env at start-up; the scheduled jobs read it on every run
        pending = await asyncio.to_thread(addPendingServices, {API_SERVICE})
    return {"secrets": secretState(), "pending": pending}
