#!/usr/bin/env bash
# One-time (and re-runnable) installer that makes ICMIS start by itself every time the Pi powers on.
# Usage:  sudo bash deploy/installServices.sh [--mqtt-lan] [--python /path/to/venv/bin/python]
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
    echo "Run with sudo: sudo bash deploy/installServices.sh" >&2
    exit 1
fi

ICMIS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ICMIS_USER="${ICMIS_USER:-${SUDO_USER:-}}"
if [[ -z "${ICMIS_USER}" || "${ICMIS_USER}" == "root" ]]; then
    echo "Run this through sudo from the account that owns ${ICMIS_ROOT} (or set ICMIS_USER)." >&2
    exit 1
fi
ICMIS_HOME="$(getent passwd "${ICMIS_USER}" | cut -d: -f6)"
ICMIS_PYTHON="${ICMIS_PYTHON:-${ICMIS_HOME}/icmis/venv/bin/python}"
MQTT_LAN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mqtt-lan) MQTT_LAN=1 ;;
        --python) ICMIS_PYTHON="$2"; shift ;;
        *) echo "Unknown option $1" >&2; exit 1 ;;
    esac
    shift
done

if [[ ! -x "${ICMIS_PYTHON}" ]]; then
    echo "Python not found at ${ICMIS_PYTHON}; create the venv first (see README) or pass --python." >&2
    exit 1
fi

runAsUser() { sudo -u "${ICMIS_USER}" -H "$@"; }

echo "==> Installing ICMIS for user ${ICMIS_USER} from ${ICMIS_ROOT}"

# State directory is where the settings API drops apply requests for the root helper
ICMIS_STATE_DIR="$(cd "${ICMIS_ROOT}" && runAsUser "${ICMIS_PYTHON}" -c '
import yaml
from deploy.serviceCatalog import getSection, resolveProjectPath
config = yaml.safe_load(open("config.yaml")) or {}
print(resolveProjectPath(getSection(config, "settingsApi").get("stateDirectory", "~/icmis/state")))
')"
BACKUP_DIR="$(cd "${ICMIS_ROOT}" && runAsUser "${ICMIS_PYTHON}" -c '
import yaml
from deploy.serviceCatalog import getSection, resolveProjectPath
config = yaml.safe_load(open("config.yaml")) or {}
print(resolveProjectPath(getSection(config, "schedules", "databaseBackup").get("directory", "~/icmis/backups")))
')"
runAsUser mkdir -p "${ICMIS_STATE_DIR}" "${BACKUP_DIR}" "${ICMIS_HOME}/icmis/models" "${ICMIS_HOME}/icmis/configBackups"
chmod 755 "${ICMIS_STATE_DIR}"

# Secrets live in one .env next to config.yaml; generate the API key once so Settings is protected from day one
ENV_FILE="${ICMIS_ROOT}/.env"
if [[ ! -f "${ENV_FILE}" ]]; then
    install -o "${ICMIS_USER}" -g "${ICMIS_USER}" -m 600 /dev/null "${ENV_FILE}"
fi
chown "${ICMIS_USER}:${ICMIS_USER}" "${ENV_FILE}"
chmod 600 "${ENV_FILE}"
if ! grep -q '^ICMIS_API_KEY=' "${ENV_FILE}"; then
    GENERATED_KEY="$(runAsUser "${ICMIS_PYTHON}" -c 'import secrets; print(secrets.token_urlsafe(32))')"
    echo "ICMIS_API_KEY=${GENERATED_KEY}" >> "${ENV_FILE}"
    echo "==> Generated a new API key (also stored in ${ENV_FILE}):"
    echo "    ${GENERATED_KEY}"
fi

# Supporting system services must also come up at boot
systemctl enable --now mosquitto.service
systemctl enable --now avahi-daemon.service 2>/dev/null || true
if systemctl list-unit-files ollama.service >/dev/null 2>&1; then
    systemctl enable --now ollama.service || true
fi

if [[ "${MQTT_LAN}" -eq 1 ]]; then
    # Lets field sensors on the LAN publish; anonymous, so only use it on a private network
    cat > /etc/mosquitto/conf.d/icmis.conf <<'EOF'
# Written by deploy/installServices.sh --mqtt-lan
listener 1883 0.0.0.0
allow_anonymous true
EOF
    systemctl restart mosquitto.service
fi

# Keep logs across reboots so field problems can be diagnosed afterwards, and reboot the Pi if it ever hangs
mkdir -p /etc/systemd/journald.conf.d /etc/systemd/system.conf.d
cat > /etc/systemd/journald.conf.d/icmis.conf <<'EOF'
[Journal]
Storage=persistent
SystemMaxUse=200M
EOF
cat > /etc/systemd/system.conf.d/icmisWatchdog.conf <<'EOF'
[Manager]
RuntimeWatchdogSec=15s
EOF
systemctl restart systemd-journald.service

# Build the dashboard once if it has not been compiled yet
if [[ ! -f "${ICMIS_ROOT}/webDashboard/dist/index.html" ]] && command -v npm >/dev/null 2>&1; then
    echo "==> Building the web dashboard"
    (cd "${ICMIS_ROOT}/webDashboard" && runAsUser npm ci && runAsUser npm run build)
fi

# Render the unit templates (CR stripped in case the repo was copied from Windows)
echo "==> Installing systemd units"
for template in "${ICMIS_ROOT}"/deploy/systemd/*; do
    sed -e 's/\r$//' \
        -e "s|@USER@|${ICMIS_USER}|g" \
        -e "s|@ROOT@|${ICMIS_ROOT}|g" \
        -e "s|@PYTHON@|${ICMIS_PYTHON}|g" \
        -e "s|@STATE_DIR@|${ICMIS_STATE_DIR}|g" \
        "${template}" > "/etc/systemd/system/$(basename "${template}")"
done
systemctl daemon-reload

WORKERS="$(cd "${ICMIS_ROOT}" && runAsUser "${ICMIS_PYTHON}" -c 'from deploy.serviceCatalog import WORKER_SCRIPTS; print(" ".join(WORKER_SCRIPTS))')"
systemctl enable icmis.target icmis-db.service icmis-api.service icmis-apply.path
for worker in ${WORKERS}; do
    systemctl enable "icmis-worker@${worker}.service"
done

# Hostname, timezone and timer schedules straight from config.yaml (also enables/disables each timer)
PYTHONDONTWRITEBYTECODE=1 ICMIS_ROOT="${ICMIS_ROOT}" ICMIS_STATE_DIR="${ICMIS_STATE_DIR}" ICMIS_USER="${ICMIS_USER}" \
    "${ICMIS_PYTHON}" "${ICMIS_ROOT}/deploy/applySettings.py" --all

echo "==> Starting ICMIS"
systemctl start icmis.target
for worker in ${WORKERS}; do
    systemctl restart "icmis-worker@${worker}.service" || true
done

HOSTNAME_NOW="$(hostname)"
echo
echo "ICMIS is installed and will start automatically at every boot."
echo "  Dashboard:  http://${HOSTNAME_NOW}.local:$(cd "${ICMIS_ROOT}" && runAsUser "${ICMIS_PYTHON}" -c 'import yaml; print((yaml.safe_load(open("config.yaml")) or {}).get("api", {}).get("port", 8000))')"
echo "  Status:     systemctl list-units 'icmis*' --all"
echo "  Logs:       journalctl -u 'icmis*' -f"