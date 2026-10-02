import glob
import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone

import yaml

PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from deploy.serviceCatalog import getSection, resolveProjectPath  # noqa: E402

# Started by icmis-backup.timer; the schedule itself is set from Settings > Schedules
os.chdir(PROJECT_ROOT)
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f) or {}

backupSettings = getSection(config, "schedules", "databaseBackup")
DATABASE_PATH: str = resolveProjectPath(getSection(config, "storage").get("databasePath", "~/icmis/inputs.db"))
BACKUP_DIRECTORY: str = resolveProjectPath(backupSettings.get("directory", "~/icmis/backups"))
KEEP_COPIES: int = int(backupSettings.get("keepCopies", 7))

if KEEP_COPIES < 1:
    raise ValueError("schedules.databaseBackup.keepCopies must be at least 1.")


def pruneBackups(pattern: str) -> None:
    # Timestamped names sort chronologically, so everything before the newest KEEP_COPIES goes
    for oldPath in sorted(glob.glob(os.path.join(BACKUP_DIRECTORY, pattern)))[:-KEEP_COPIES]:
        os.remove(oldPath)


def main() -> int:
    if not backupSettings.get("enabled", True):
        print("Database backups are disabled in Settings.")
        return 0
    if not os.path.isfile(DATABASE_PATH):
        print(f"No database at {DATABASE_PATH} yet; nothing to back up.")
        return 0

    os.makedirs(BACKUP_DIRECTORY, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backupPath = os.path.join(BACKUP_DIRECTORY, f"inputs-{timestamp}.db")

    # sqlite3's online backup API copies a consistent snapshot while the workers keep writing
    source = sqlite3.connect(f"file:{DATABASE_PATH}?mode=ro", uri=True, timeout=30)
    destination = sqlite3.connect(backupPath)
    try:
        source.backup(destination, pages=1024)
        integrity = destination.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        destination.close()
        source.close()
    if integrity != "ok":
        os.remove(backupPath)
        print(f"Backup failed its integrity check ({integrity}); the broken copy was removed.")
        return 1

    # The settings file is tiny and needed to restore a working Pi, so it travels with every backup
    shutil.copy2(os.path.join(PROJECT_ROOT, "config.yaml"), os.path.join(BACKUP_DIRECTORY, f"config-{timestamp}.yaml"))
    pruneBackups("inputs-*.db")
    pruneBackups("config-*.yaml")
    print(f"Backed up {DATABASE_PATH} to {backupPath}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
