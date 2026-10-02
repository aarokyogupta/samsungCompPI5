import json
import os
import sys
import urllib.error
import urllib.request

import yaml

PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from deploy.serviceCatalog import getSection  # noqa: E402

# Started by icmis-assessment.timer; it asks the running API for a report on each configured target
os.chdir(PROJECT_ROOT)
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f) or {}

assessmentSettings = getSection(config, "schedules", "riskAssessment")
apiSettings = getSection(config, "api")
API_PORT: int = int(os.environ.get("ICMIS_PORT", apiSettings.get("port", 8000)))
API_PREFIX: str = str(apiSettings.get("apiPrefix", "/api/v1")).rstrip("/")
GENERATE_URL: str = f"http://127.0.0.1:{API_PORT}{API_PREFIX}/reports/generate"
REQUEST_TIMEOUT_SEC: float = 30.0

# Settings field name -> ReportTriggerRequest field name
TARGET_FIELDS: dict[str, str] = {
    "speciesID": "species_id",
    "scientificName": "scientific_name",
    "h3Cell": "h3_cell",
    "latitude": "latitude",
    "longitude": "longitude",
    "lookbackDays": "lookback_days",
}


def buildPayload(target: dict) -> dict:
    payload = {TARGET_FIELDS[key]: value for key, value in target.items() if key in TARGET_FIELDS and value not in (None, "")}
    payload.setdefault("lookback_days", float(assessmentSettings.get("defaultLookbackDays", 30)))
    return payload


def main() -> int:
    if not assessmentSettings.get("enabled", True):
        print("Scheduled risk assessments are disabled in Settings.")
        return 0
    targets = assessmentSettings.get("targets") or []
    if not targets:
        print("No assessment targets configured; add them in Settings > Schedules.")
        return 0

    headers = {"Content-Type": "application/json"}
    if os.environ.get("ICMIS_API_KEY"):
        headers["X-API-Key"] = os.environ["ICMIS_API_KEY"]

    failures = 0
    for target in targets:
        if not isinstance(target, dict):
            continue
        payload = buildPayload(target)
        request = urllib.request.Request(GENERATE_URL, data=json.dumps(payload).encode(), headers=headers, method="POST")
        try:
            # The API returns 202 at once; the LangGraph run continues inside the API process
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SEC) as response:
                body = json.loads(response.read() or b"{}")
            print(f"Queued assessment for species {payload.get('species_id')}: job {body.get('job_id')}.")
        except urllib.error.HTTPError as error:
            failures += 1
            print(f"Assessment for species {payload.get('species_id')} rejected ({error.code}): {error.read().decode(errors='replace')}")
        except (urllib.error.URLError, TimeoutError) as error:
            failures += 1
            print(f"API unreachable at {GENERATE_URL}: {error}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
