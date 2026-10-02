import os
from typing import Any

# Shared by the settings API, the worker launcher and the root apply helper so they always agree on unit names
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Continuous workers: settings key -> script run from the project root
WORKER_SCRIPTS: dict[str, str] = {
    "gpsTelemetry": "inputs/gpsTelemetry.py",
    "environmentalSensors": "inputs/environmentalSensors.py",
    "camera": "inputs/camera.py",
    "visionInference": "processing/visionInference.py",
    "acousticStream": "inputs/acousticStream.py",
    "acousticInference": "processing/acousticInference.py",
    "ednaParser": "inputs/ednaParser.py",
    "geneticAnalytics": "processing/geneticAnalytics.py",
}

WORKER_LABELS: dict[str, str] = {
    "gpsTelemetry": "GPS telemetry listener",
    "environmentalSensors": "Environmental sensor listener",
    "camera": "Camera & drone ingestion",
    "visionInference": "Vision inference",
    "acousticStream": "Acoustic ingestion",
    "acousticInference": "Acoustic inference",
    "ednaParser": "eDNA ingestion",
    "geneticAnalytics": "Genetic analytics",
}

API_SERVICE: str = "api"
SCHEDULES_SERVICE: str = "schedules"
SYSTEM_SERVICE: str = "system"

# Everything the apply helper is allowed to act on; anything else in a request is ignored
APPLY_TARGETS: tuple[str, ...] = (API_SERVICE, *WORKER_SCRIPTS, SCHEDULES_SERVICE, SYSTEM_SERVICE)
RESTARTABLE_SERVICES: tuple[str, ...] = (API_SERVICE, *WORKER_SCRIPTS)

# Scheduled jobs: settings key -> (timer unit, default OnCalendar expression)
SCHEDULE_TIMERS: dict[str, tuple[str, str]] = {
    "databaseBackup": ("icmis-backup.timer", "*-*-* 02:30:00"),
    "riskAssessment": ("icmis-assessment.timer", "*-*-* 06:00:00"),
}

# Units that are not ICMIS programs but decide whether ICMIS works
SUPPORT_UNITS: dict[str, str] = {
    "database": "icmis-db.service",
    "supervisor": "icmis-apply.path",
    "mqttBroker": "mosquitto.service",
    "ollama": "ollama.service",
}

APPLY_REQUEST_FILE: str = "applyRequest.json"
APPLY_STATUS_FILE: str = "applyStatus.json"
PENDING_FILE: str = "pendingRestart.json"


def unitForService(service: str) -> str:
    if service == API_SERVICE:
        return "icmis-api.service"
    if service in WORKER_SCRIPTS:
        return f"icmis-worker@{service}.service"
    raise KeyError(f"{service} is not a restartable ICMIS service.")


def resolveProjectPath(path: str, projectRoot: str = PROJECT_ROOT) -> str:
    # Same rule as the rest of the project: "~" is the service user's home, relative paths start at the project folder
    path = os.path.expanduser(str(path))
    return path if os.path.isabs(path) else os.path.join(projectRoot, path)


def getSection(config: dict, *keys: str) -> dict:
    section: Any = config
    for key in keys:
        section = section.get(key, {}) if isinstance(section, dict) else {}
    return section if isinstance(section, dict) else {}


def isWorkerEnabled(config: dict, worker: str) -> bool:
    return bool(getSection(config, "services", worker).get("enabled", True))


def checkWorkerReady(
    config: dict,
    worker: str,
    homeDirectory: str | None = None,
    projectRoot: str = PROJECT_ROOT,
) -> tuple[bool, str]:
    # A worker that is disabled or missing its model is skipped by systemd instead of crash-looping on every boot
    if worker not in WORKER_SCRIPTS:
        return False, f"Unknown worker {worker!r}."
    if not isWorkerEnabled(config, worker):
        return False, "Disabled in Settings."

    def resolve(path: Any) -> str:
        # The root apply helper passes the service user's home so "~" does not become /root
        text = str(path or "")
        if homeDirectory and (text == "~" or text.startswith("~/")):
            text = homeDirectory + text[1:]
        return resolveProjectPath(text, projectRoot)

    if worker == "visionInference":
        vision = getSection(config, "vision")
        modelPath = resolve(vision.get("modelPath"))
        if not os.path.isfile(modelPath):
            return False, f"Vision model not found at {modelPath}."
        if not vision.get("classNames"):
            return False, "vision.classNames is empty; list the model's classes in Settings."
    elif worker == "acousticInference":
        inference = getSection(config, "acoustic", "inference")
        modelPath = resolve(inference.get("modelPath"))
        classMapPath = resolve(inference.get("classMapPath"))
        if not os.path.isfile(modelPath):
            return False, f"Acoustic model not found at {modelPath}."
        if not os.path.isfile(classMapPath):
            return False, f"Acoustic class map not found at {classMapPath}."
    elif worker == "ednaParser":
        if not getSection(config, "edna", "approvedLoci"):
            return False, "edna.approvedLoci is empty; add the approved loci for each study species."
    return True, "Ready."
