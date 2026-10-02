import argparse
import os
import sys
import yaml

# Allow "python deploy/runWorker.py" from systemd as well as from the project root
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from deploy.serviceCatalog import WORKER_SCRIPTS, checkWorkerReady  # noqa: E402

# systemd ExecCondition: 0 runs the unit, 1-254 skips it without marking it failed (so Restart= never loops on it)
EXIT_READY: int = 0
EXIT_SKIP: int = 1


def loadConfig() -> dict:
    with open(os.path.join(PROJECT_ROOT, "config.yaml"), "r") as f:
        return yaml.safe_load(f) or {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch or gate one ICMIS worker.")
    parser.add_argument("worker", choices=sorted(WORKER_SCRIPTS))
    parser.add_argument("--check", action="store_true", help="Only report whether the worker should run.")
    arguments = parser.parse_args()

    # Every module opens config.yaml relative to the working directory
    os.chdir(PROJECT_ROOT)
    try:
        ready, reason = checkWorkerReady(loadConfig(), arguments.worker)
    except (OSError, yaml.YAMLError) as error:
        ready, reason = False, f"config.yaml could not be read: {error}"

    if arguments.check:
        print(f"{arguments.worker}: {reason}")
        return EXIT_READY if ready else EXIT_SKIP
    if not ready:
        print(f"{arguments.worker} not started: {reason}")
        return EXIT_SKIP

    # exec keeps the same PID so systemd supervises the real worker, exactly as "python inputs/x.py" would run
    scriptPath = os.path.join(PROJECT_ROOT, WORKER_SCRIPTS[arguments.worker])
    os.execv(sys.executable, [sys.executable, "-u", scriptPath])
    return EXIT_READY


if __name__ == "__main__":
    sys.exit(main())
