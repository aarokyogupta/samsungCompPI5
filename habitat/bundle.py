import argparse
import json
import math
import os
import shutil
import sqlite3
import tempfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from aiEngine.focalSpecies import LANGUR

MAX_BYTES = 30_000_000
MAX_CELLS = 150_000


def bundlePath(config: dict) -> Path:
    return Path(config.get("habitatModel", {}).get("bundlePath", "~/icmis/habitat/active.db")).expanduser()


def numberIn(value: object, lower: float, upper: float) -> bool:
    return type(value) in (float, int) and lower <= value <= upper


def suitabilityClass(vote: float | None) -> str | None:
    if vote is None:
        return None
    return "High" if vote >= 0.75 else "Moderate" if vote >= 0.5 else "Low" if vote >= 0.25 else "Very Low"


def validateBundle(data: dict) -> None:
    if not isinstance(data, dict) or (data.get("version"), data.get("species"), data.get("cell_size_m")) != (1, LANGUR, 1000):
        raise ValueError("Invalid bundle schema, species, or resolution.")
    if not isinstance(data.get("cells"), list) or not 1 <= len(data["cells"]) <= MAX_CELLS:
        raise ValueError("Invalid number of cells.")
    stamp = datetime.fromisoformat(data["created_at"])
    if stamp.tzinfo is None or stamp > datetime.now(timezone.utc):
        raise ValueError("The bundle requires a valid creation timestamp with timezone.")
    metrics = data["metrics"]
    if (not numberIn(metrics["auc"], 0.90, 1) or not numberIn(metrics["tss"], 0.70, 1)
            or not isinstance(metrics["members"], list) or not 1 <= len(metrics["members"]) <= 15):
        raise ValueError("Bundle did not pass habitat model validation.")
    if not all(numberIn(m["auc"], 0.90, 1) and numberIn(m["tss"], 0.70, 1) for m in metrics["members"]):
        raise ValueError("Invalid retained member metrics.")
    year = data.get("loss_year")
    if year is not None and (type(year) is not int or not 2001 <= year < datetime.now(timezone.utc).year):
        raise ValueError("Invalid forest-loss year.")
    usable = False
    for cell in data["cells"]:
        if not numberIn(cell["lat"], -90, 90) or not numberIn(cell["lon"], -180, 180):
            raise ValueError("Invalid cell coordinates.")
        vote, loss, status = cell["vote"], cell["loss"], cell["status"]
        if status == "suitable_model_domain":
            if not numberIn(vote, 0, 1):
                raise ValueError("Invalid committee vote.")
            usable = True
        elif status not in {"nodata", "extrapolation"} or vote is not None:
            raise ValueError("Invalid cell status or vote.")
        if loss is not None and (year is None or not numberIn(loss, 0, 1)):
            raise ValueError("Invalid cell forest loss.")
    if not usable:
        raise ValueError("Bundle has no usable suitability cells.")


def installBundle(source: Path, target: Path) -> None:
    with source.open("rb") as stream:
        payload = stream.read(MAX_BYTES + 1)
    if len(payload) > MAX_BYTES:
        raise ValueError("Habitat bundle exceeds size limit.")
    data = json.loads(payload)
    validateBundle(data)
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as stream:
        staging = Path(stream.name)
    try:
        with closing(sqlite3.connect(staging)) as connection, connection:
            connection.executescript(
                "CREATE TABLE metadata (payload TEXT NOT NULL);"
                "CREATE TABLE cells (lat REAL, lon REAL, vote REAL, status TEXT, loss REAL, PRIMARY KEY(lat, lon));"
            )
            meta = {k: data[k] for k in ("version", "species", "created_at", "cell_size_m", "metrics", "layers")}
            meta["loss_year"] = data.get("loss_year")
            connection.execute("INSERT INTO metadata VALUES (?);", (json.dumps(meta, allow_nan=False),))
            connection.executemany("INSERT INTO cells VALUES (:lat, :lon, :vote, :status, :loss);", data["cells"])
        if target.is_file():
            backup = target.with_name("previous.db")
            with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as stream:
                previous = Path(stream.name)
                try:
                    with target.open("rb") as current:
                        shutil.copyfileobj(current, stream)
                    stream.close()
                    os.replace(previous, backup)
                finally:
                    previous.unlink(missing_ok=True)
        os.replace(staging, target)
    finally:
        staging.unlink(missing_ok=True)


def readCells(path: Path, bounds: tuple[float, float, float, float] | None = None, limit: int = 2000) -> dict:
    if not path.is_file():
        return {"status": "not_installed", "cells": []}
    with closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        meta = json.loads(connection.execute("SELECT payload FROM metadata").fetchone()[0])
        if (meta["version"], meta["species"]) != (1, LANGUR):
            return {"status": "incompatible", "cells": []}
        where = "WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?" if bounds else ""
        rows = connection.execute(
            f"SELECT * FROM cells {where} ORDER BY lat, lon LIMIT ?;", (*bounds, limit + 1) if bounds else (limit + 1,)
        ).fetchall()
    stale = (datetime.now(timezone.utc) - datetime.fromisoformat(meta["created_at"])).days > 730
    return {
        "status": "stale" if stale else "ready",
        "metadata": meta,
        "cells": [dict(row) | {"suitability": suitabilityClass(row["vote"])} for row in rows[:limit]],
        "truncated": len(rows) > limit,
    }


def recentLoss(path: Path, latitude: float, longitude: float, end: datetime) -> tuple[float, int] | None:
    latDelta = 0.009
    lonDelta = latDelta / max(0.1, math.cos(math.radians(latitude)))
    result = readCells(path, (latitude - latDelta, latitude + latDelta, longitude - lonDelta, longitude + lonDelta))
    year = result.get("metadata", {}).get("loss_year")
    if result["status"] != "ready" or year is None or not 1 <= end.year - year <= 2:
        return None
    cells = result["cells"]
    if not cells:
        return None
    nearest = min(cells, key=lambda cell: ((cell["lat"] - latitude) / latDelta) ** 2 +
                  ((cell["lon"] - longitude) / lonDelta) ** 2)
    distance = math.hypot((nearest["lat"] - latitude) / latDelta, (nearest["lon"] - longitude) / lonDelta)
    return (nearest["loss"], year) if distance <= 0.72 and nearest["loss"] is not None else None


def main() -> None:
    import yaml

    parser = argparse.ArgumentParser(description="Install a validated ICMIS habitat bundle.")
    parser.add_argument("bundle", type=Path)
    args = parser.parse_args()
    with Path("config.yaml").open(encoding="utf-8") as stream:
        target = bundlePath(yaml.safe_load(stream))
    try:
        installBundle(args.bundle, target)
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError) as error:
        parser.exit(1, f"Habitat activation failed: {error}\n")
    print(f"Activated {target}")


if __name__ == "__main__":
    main()
