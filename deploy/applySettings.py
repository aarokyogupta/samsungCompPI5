import argparse
import json
import os
import pwd
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

import yaml

# Runs as root from icmis-apply.service; everything it reads from the (user-writable) state directory is untrusted
PROJECT_ROOT: str = os.environ.get("ICMIS_ROOT") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from deploy.serviceCatalog import (  # noqa: E402
    APPLY_REQUEST_FILE,
    APPLY_STATUS_FILE,
    APPLY_TARGETS,
    RESTARTABLE_SERVICES,
    SCHEDULE_TIMERS,
    SCHEDULES_SERVICE,
    SYSTEM_SERVICE,
    API_SERVICE,
    WORKER_SCRIPTS,
    checkWorkerReady,
    getSection,
    unitForService,
)

SERVICE_USER: str = os.environ.get("ICMIS_USER", "icmis")
STATE_DIRECTORY: str = os.environ.get("ICMIS_STATE_DIR", "")
MAX_REQUEST_BYTES: int = 16384
SYSTEMD_DIRECTORY: str = "/etc/systemd/system"
DROP_IN_NAME: str = "icmisSchedule.conf"

# Same limits the settings API enforces, repeated here because this process must not trust the API
HOSTNAME_PATTERN = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$")
CALENDAR_PATTERN = re.compile(r"^[A-Za-z0-9*:,./~ -]{1,64}$")
TIMEZONE_PATTERN = re.compile(r"^[A-Za-z0-9_+\-]+(/[A-Za-z0-9_+\-]+){0,2}$")


def runCommand(arguments: list[str], check: bool = True) -> subprocess.CompletedProcess:
    # Argument lists only, never a shell, so no setting value can inject a command
    return subprocess.run(arguments, capture_output=True, text=True, timeout=60, check=check)


def loadConfig() -> dict:
    with open(os.path.join(PROJECT_ROOT, "config.yaml"), "r") as f:
        return yaml.safe_load(f) or {}


def readRequest(path: str) -> list[str]:
    # O_NOFOLLOW stops a symlinked request pointing root at an arbitrary file
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return []
    try:
        with os.fdopen(descriptor, "r") as f:
            raw = f.read(MAX_REQUEST_BYTES + 1)
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
    if len(raw) > MAX_REQUEST_BYTES:
        raise ValueError("Apply request is too large.")
    request = json.loads(raw or "{}")
    services = request.get("services", []) if isinstance(request, dict) else []
    return [service for service in services if isinstance(service, str) and service in APPLY_TARGETS]


def writeStateFile(name: str, payload: dict) -> None:
    # Written atomically and owned by the service user so the API can read and replace it
    if not STATE_DIRECTORY:
        return
    account = pwd.getpwnam(SERVICE_USER)
    descriptor, temporaryPath = tempfile.mkstemp(dir=STATE_DIRECTORY, prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(descriptor, "w") as f:
            json.dump(payload, f, indent=2)
            os.fchown(f.fileno(), account.pw_uid, account.pw_gid)
            os.fchmod(f.fileno(), 0o644)
        os.replace(temporaryPath, os.path.join(STATE_DIRECTORY, name))
    except BaseException:
        if os.path.exists(temporaryPath):
            os.unlink(temporaryPath)
        raise


def applySystem(config: dict, results: dict) -> None:
    system = getSection(config, "system")
    hostname = str(system.get("hostname", "")).strip().lower()
    if hostname:
        if not HOSTNAME_PATTERN.match(hostname):
            results["system.hostname"] = f"Rejected invalid hostname {hostname!r}."
        elif hostname != os.uname().nodename:
            runCommand(["hostnamectl", "set-hostname", hostname])
            # Keep sudo and local name lookups working after the rename
            with open("/etc/hosts", "r") as f:
                lines = f.read().splitlines()
            lines = [line for line in lines if not line.startswith("127.0.1.1")]
            lines.append(f"127.0.1.1\t{hostname}")
            with open("/etc/hosts", "w") as f:
                f.write("\n".join(lines) + "\n")
            runCommand(["systemctl", "try-restart", "avahi-daemon.service"], check=False)
            results["system.hostname"] = f"Hostname set to {hostname} (reach the Pi at http://{hostname}.local)."
        else:
            results["system.hostname"] = "Unchanged."

    timezoneName = str(system.get("timezone", "")).strip()
    if timezoneName:
        zonePath = os.path.join("/usr/share/zoneinfo", timezoneName)
        if not TIMEZONE_PATTERN.match(timezoneName) or ".." in timezoneName or not os.path.isfile(zonePath):
            results["system.timezone"] = f"Rejected unknown timezone {timezoneName!r}."
        else:
            runCommand(["timedatectl", "set-timezone", timezoneName])
            results["system.timezone"] = f"Timezone set to {timezoneName}."


def applySchedules(config: dict, results: dict) -> None:
    schedules = getSection(config, "schedules")
    for key, (timerUnit, defaultCalendar) in SCHEDULE_TIMERS.items():
        schedule = schedules.get(key, {}) if isinstance(schedules.get(key), dict) else {}
        calendar = str(schedule.get("onCalendar") or defaultCalendar).strip()
        enabled = bool(schedule.get("enabled", True))

        # systemd-analyze is the final authority on whether an OnCalendar expression is valid
        if not CALENDAR_PATTERN.match(calendar) or runCommand(["systemd-analyze", "calendar", calendar], check=False).returncode != 0:
            results[f"schedules.{key}"] = f"Rejected invalid schedule {calendar!r}; kept previous timing."
            continue

        # The blank OnCalendar= clears the default from the base unit before the new value is added
        dropInDirectory = os.path.join(SYSTEMD_DIRECTORY, f"{timerUnit}.d")
        os.makedirs(dropInDirectory, exist_ok=True)
        with open(os.path.join(dropInDirectory, DROP_IN_NAME), "w") as f:
            f.write(f"# Written by ICMIS Settings\n[Timer]\nOnCalendar=\nOnCalendar={calendar}\n")
        results[f"schedules.{key}"] = f"{'Enabled' if enabled else 'Disabled'} ({calendar})."

    runCommand(["systemctl", "daemon-reload"])
    for key, (timerUnit, _) in SCHEDULE_TIMERS.items():
        schedule = schedules.get(key, {}) if isinstance(schedules.get(key), dict) else {}
        action = "enable" if schedule.get("enabled", True) else "disable"
        runCommand(["systemctl", action, "--now", timerUnit], check=False)
        if action == "enable":
            # Restart so a changed OnCalendar recalculates the next trigger immediately
            runCommand(["systemctl", "restart", timerUnit], check=False)


def restartServices(config: dict, services: list[str], results: dict) -> None:
    homeDirectory = pwd.getpwnam(SERVICE_USER).pw_dir
    for worker in [service for service in services if service in WORKER_SCRIPTS]:
        unit = unitForService(worker)
        runCommand(["systemctl", "reset-failed", unit], check=False)
        # The unit's ExecCondition skips it cleanly when disabled or not ready, so restart is always safe
        outcome = runCommand(["systemctl", "restart", unit], check=False)
        ready, reason = checkWorkerReady(config, worker, homeDirectory, PROJECT_ROOT)
        if outcome.returncode != 0:
            results[worker] = f"Restart failed: {outcome.stderr.strip() or outcome.returncode}"
        else:
            results[worker] = "Restarted." if ready else f"Stopped: {reason}"

    # The API goes last and without waiting, because the API may be the process that asked for this apply
    if API_SERVICE in services:
        runCommand(["systemctl", "reset-failed", unitForService(API_SERVICE)], check=False)
        runCommand(["systemctl", "restart", "--no-block", unitForService(API_SERVICE)], check=False)
        results[API_SERVICE] = "Restarting."


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply saved ICMIS settings to the running Pi (root only).")
    parser.add_argument("--all", action="store_true", help="Apply system settings and schedules without a request file.")
    arguments = parser.parse_args()

    if os.geteuid() != 0:
        print("applySettings.py must run as root (it is started by icmis-apply.service).")
        return 1

    startedAt = datetime.now(timezone.utc).isoformat()
    results: dict[str, str] = {}
    try:
        if arguments.all:
            services = [SYSTEM_SERVICE, SCHEDULES_SERVICE]
        else:
            services = readRequest(os.path.join(STATE_DIRECTORY, APPLY_REQUEST_FILE)) if STATE_DIRECTORY else []
        if not services:
            return 0

        config = loadConfig()
        if SYSTEM_SERVICE in services:
            applySystem(config, results)
        if SCHEDULES_SERVICE in services:
            applySchedules(config, results)
        restartServices(config, [service for service in services if service in RESTARTABLE_SERVICES], results)

        writeStateFile(APPLY_STATUS_FILE, {
            "state": "completed",
            "services": services,
            "results": results,
            "startedAt": startedAt,
            "finishedAt": datetime.now(timezone.utc).isoformat(),
        })
        print(json.dumps(results, indent=2))
        return 0
    except Exception as error:
        writeStateFile(APPLY_STATUS_FILE, {
            "state": "failed",
            "error": str(error),
            "results": results,
            "startedAt": startedAt,
            "finishedAt": datetime.now(timezone.utc).isoformat(),
        })
        print(f"Apply failed: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
