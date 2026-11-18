import json
import csv
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from aiEngine.focalSpecies import LANGUR, primaryScores
from habitat.bundle import installBundle, readCells, recentLoss, suitabilityClass


class BundleTest(unittest.TestCase):
    def test_import_reads_offline_and_keeps_last_valid_bundle(self):
        now = datetime.now(timezone.utc)
        document = {
            "version": 1, "species": LANGUR, "created_at": now.isoformat(),
            "cell_size_m": 1000, "metrics": {"auc": 0.90, "tss": 0.70, "members": [{"auc": 0.90, "tss": 0.70}]},
            "layers": {}, "loss_year": now.year - 1,
            "cells": [
                {"lat": 35, "lon": 70, "vote": 0.75, "status": "suitable_model_domain", "loss": 0.2},
                {"lat": 36, "lon": 70, "vote": None, "status": "extrapolation", "loss": None},
            ],
        }
        with tempfile.TemporaryDirectory() as temp:
            source, database = (Path(temp) / name for name in ("input.json", "active.db"))
            source.write_text(json.dumps(document), encoding="utf-8")
            installBundle(source, database)
            result = readCells(database, (34, 36, 69, 71))
            self.assertEqual(["High", None], [cell["suitability"] for cell in result["cells"]])
            self.assertEqual((0.2, now.year - 1), recentLoss(database, 35, 70, now))
            self.assertIsNone(recentLoss(database, 35.1, 70, now))
            document["metrics"]["tss"] = 0.69
            source.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "validation"):
                installBundle(source, database)
            self.assertEqual(0.75, readCells(database)["cells"][0]["vote"])
            document["metrics"]["tss"] = 0.70
            document["cells"][0]["vote"] = 0.25
            source.write_text(json.dumps(document), encoding="utf-8")
            installBundle(source, database)
            self.assertEqual("Low", readCells(database)["cells"][0]["suitability"])
            self.assertEqual("High", readCells(database.with_name("previous.db"))["cells"][0]["suitability"])

    def test_class_boundaries(self):
        self.assertEqual(["Very Low", "Low", "Moderate", "High"],
                         [suitabilityClass(vote) for vote in (0.24999, 0.25, 0.5, 0.75)])

    def test_primary_scores_do_not_display_imputed_values(self):
        scores = primaryScores({"population": 50, "habitat": 0, "threat": 12}, ["population"], {}, True)
        self.assertEqual([None, 0, 12, None], [score["risk"] for score in scores])
        self.assertTrue(all(score["status"] == "unknown" for score in primaryScores({}, [], {}, False)))

    def test_migration_preserves_existing_species_and_baselines(self):
        migrations = Path("database") / "schemaMigrations"
        script = (migrations / "0005focalLangur.sql").read_text(encoding="utf-8")
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        schema = Path("database", "schema.sql").read_text(encoding="utf-8")
        connection.executescript(schema[schema.index("CREATE TABLE IF NOT EXISTS species ("):].split(";", 1)[0] + ";")
        connection.execute("INSERT INTO species (scientific_name) VALUES ('Other species');")
        connection.executescript(script)
        self.assertEqual((2, None), connection.execute(
            "SELECT id, census_population_estimate FROM species WHERE scientific_name = ?;", (LANGUR,)
        ).fetchone())
        connection.execute("UPDATE species SET census_population_estimate = 100 WHERE id = 2;")
        connection.executescript(script)
        self.assertEqual(100, connection.execute("SELECT census_population_estimate FROM species WHERE id = 2;").fetchone()[0])


class TrainingTest(unittest.TestCase):
    def test_synthetic_rasters_train_validate_and_install(self):
        try:
            import numpy as np
            import rasterio
            from rasterio.transform import from_origin, xy
            from rasterio.warp import transform
            from habitat.train import REQUIRED, buildBundle
        except ModuleNotFoundError as error:
            if error.name not in {"numpy", "rasterio", "shapely", "sklearn"}:
                raise
            self.skipTest("Install habitat/requirements.txt to run the workstation test.")

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            affine = from_origin(500000, 3900000, 1000, 1000)
            crs = "EPSG:32642"
            x = [500000, 560000, 560000, 500000, 500000]
            y = [3900000, 3900000, 3840000, 3840000, 3900000]
            lon, lat = transform(crs, "EPSG:4326", x, y)
            (root / "area.json").write_text(json.dumps({
                "type": "Polygon", "coordinates": [list(zip(lon, lat))],
            }), encoding="utf-8")
            rows, cols = np.indices((60, 60))
            forest = (cols % 10 <= 3).astype("float32")
            arrays = {"forest_fraction": forest, "elevation": 2000 + rows * 2 + cols}
            arrays.update({name: forest * 10 + (rows + cols) / 1000
                           for name in REQUIRED - arrays.keys()})
            manifest = {"boundary": "area.json", "presences": "presences.csv", "layers": {}}
            for name, array in arrays.items():
                filename = f"{name}.tif"
                with rasterio.open(root / filename, "w", driver="GTiff", width=60, height=60,
                                   count=1, dtype="float32", crs=crs, transform=affine, nodata=-9999) as raster:
                    raster.write(array.astype("float32"), 1)
                manifest["layers"][name] = {
                    "path": filename, "source": "synthetic", "version": "1", "license": "test fixture", "units": "test",
                }
            manifestPath = root / "manifest.json"
            manifestPath.write_text(json.dumps(manifest), encoding="utf-8")
            points = [(row, col) for row in range(2, 60, 5) for col in range(2, 60, 10)]
            x, y = xy(affine, *zip(*points))
            lon, lat = transform(crs, "EPSG:4326", list(x), list(y))
            with (root / "presences.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream)
                writer.writerow(("scientific_name", "longitude", "latitude", "uncertainty_m", "source", "approved"))
                writer.writerows((LANGUR, a, b, 0, "synthetic", "true") for a, b in zip(lon, lat))
            bundle = buildBundle(manifestPath, 42)
            self.assertEqual(len(points), bundle["presence_cells"])
            self.assertGreaterEqual(bundle["metrics"]["tss"], 0.70)
            self.assertGreaterEqual(bundle["metrics"]["auc"], 0.90)
            self.assertTrue(any(cell["vote"] is not None for cell in bundle["cells"]))
            source = root / "bundle.json"
            source.write_text(json.dumps(bundle, allow_nan=False), encoding="utf-8")
            target = root / "installed.db"
            installBundle(source, target)
            self.assertEqual("ready", readCells(target, limit=1)["status"])


if __name__ == "__main__":
    unittest.main()
