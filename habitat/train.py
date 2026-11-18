import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import rasterio
from rasterio.features import geometry_mask
from rasterio.transform import from_origin, rowcol, xy
from rasterio.warp import Resampling, reproject, transform, transform_geom
from shapely.geometry import shape
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, roc_curve
from sklearn.model_selection import GroupKFold, GroupShuffleSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC

from aiEngine.focalSpecies import LANGUR

AUC_MIN = 0.90
TSS_MIN = 0.70
CELL_SIZE = 1000
MAX_CELLS = 150_000
REQUIRED = {"elevation", "forest_fraction", "ndvi", "evi", *(f"bio{i}" for i in range(1, 20))}


def loadGrid(manifest, directory):
    aoi = shape(json.loads((directory / manifest["boundary"]).read_text(encoding="utf-8")))
    if not aoi.is_valid or aoi.is_empty or aoi.geom_type not in {"Polygon", "MultiPolygon"}:
        raise ValueError("Boundary must be a valid non-empty WGS84 GeoJSON Polygon or MultiPolygon.")
    west, south, east, north = aoi.bounds
    if not (-180 <= west < east <= 180 and -80 <= south < north <= 84):
        raise ValueError("Boundary exceeds UTM coverage.")
    center = aoi.centroid
    zone = min(60, int((center.x + 180) / 6) + 1)
    crs = f"EPSG:{(32600 if center.y >= 0 else 32700) + zone}"
    geometry = transform_geom("EPSG:4326", crs, aoi.__geo_interface__)
    left, bottom, right, top = shape(geometry).bounds
    left = np.floor(left / CELL_SIZE) * CELL_SIZE
    top = np.ceil(top / CELL_SIZE) * CELL_SIZE
    width = int(np.ceil((right - left) / CELL_SIZE))
    height = int(np.ceil((top - bottom) / CELL_SIZE))
    if width * height > MAX_CELLS:
        raise ValueError("Boundary is too large; split the study area.")
    affine = from_origin(left, top, CELL_SIZE, CELL_SIZE)
    inside = geometry_mask([geometry], (height, width), affine, invert=True)
    layers, provenance = {}, {}
    if missing := REQUIRED - manifest["layers"].keys():
        raise ValueError(f"Missing predictor layers: {', '.join(sorted(missing))}")
    for name, spec in sorted(manifest["layers"].items()):
        if not all(spec.get(key) for key in ("path", "source", "version", "license", "units")):
            raise ValueError(f"Layer {name} needs path, source, version, license and units.")
        path = directory / spec["path"]
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        with rasterio.open(path) as dataset:
            if dataset.count != 1 or dataset.crs is None or dataset.nodata is None:
                raise ValueError(f"{name} must be a georeferenced single-band raster with nodata.")
            if spec.get("resampling", "average") not in {"average", "bilinear", "nearest"}:
                raise ValueError(f"Unsupported resampling for {name}.")
            output = np.full(inside.shape, np.nan, dtype=np.float32)
            reproject(rasterio.band(dataset, 1), output, dst_transform=affine, dst_crs=crs,
                      dst_nodata=np.nan, resampling=Resampling[spec.get("resampling", "average")])
        layers[name] = output * float(spec.get("scale", 1)) + float(spec.get("offset", 0))
        provenance[name] = {key: spec[key] for key in ("source", "version", "license", "units")}
        provenance[name].update(sha256=digest, scale=spec.get("scale", 1), offset=spec.get("offset", 0))
    for name in ("forest_fraction", "tree_cover_loss_fraction"):
        if name in layers and np.any(inside & np.isfinite(layers[name]) &
                                     ((layers[name] < 0) | (layers[name] > 1))):
            raise ValueError(f"{name} must contain fractions in [0, 1].")
    return layers, provenance, inside, affine, crs


def presenceCells(path, valid, affine, crs):
    result = set()
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for record in csv.DictReader(stream):
            if record["scientific_name"].strip().casefold() != LANGUR.casefold():
                continue
            if record["approved"].strip().lower() != "true" or not record["source"].strip():
                continue
            longitude, latitude = float(record["longitude"]), float(record["latitude"])
            uncertainty = float(record["uncertainty_m"])
            if not (0 <= uncertainty <= CELL_SIZE and -180 <= longitude <= 180 and -90 <= latitude <= 90):
                continue
            x, y = transform("EPSG:4326", crs, [longitude], [latitude])
            row, col = map(int, rowcol(affine, x[0], y[0]))
            if 0 <= row < valid.shape[0] and 0 <= col < valid.shape[1] and valid[row, col]:
                result.add((row, col))
    return np.asarray(sorted(result), dtype=int).reshape(-1, 2)


def score(y, estimates, cutoff):
    classified = estimates >= cutoff
    return float(roc_auc_score(y, estimates)), float(classified[y == 1].mean() - classified[y == 0].mean())


def optimalThreshold(y, estimates):
    fpr, tpr, thresholds = roc_curve(y, estimates)
    return float(thresholds[1 + np.argmax((tpr - fpr)[1:])])


def fitEnsemble(grid, presence, candidates, seed):
    if len(presence) < 20:
        raise ValueError("At least 20 reviewed presence cells are required; none will be fabricated.")
    rng = np.random.default_rng(seed)
    points = np.vstack((presence, candidates))
    labels = np.r_[np.ones(len(presence), dtype=int), np.zeros(len(candidates), dtype=int)]
    blocks = points // 10
    _, groups = np.unique(blocks, axis=0, return_inverse=True)
    if len(np.unique(groups[labels == 1])) < 6:
        raise ValueError("Presence cells must cover at least six 10 km blocks.")
    features = grid[points[:, 0], points[:, 1]]
    for train, test in GroupShuffleSplit(n_splits=20, test_size=0.25, random_state=seed).split(
        features, labels, groups
    ):
        if (min(np.bincount(labels[test], minlength=2)) >= 5 and
                len(np.unique(groups[train][labels[train] == 1])) >= 4):
            break
    else:
        raise ValueError("Cannot create independent spatial training and test partitions.")

    positive = train[labels[train] == 1]
    negative = train[labels[train] == 0]
    if len(negative) < len(positive):
        raise ValueError("Too few eligible training pseudo-absence cells.")
    retained = []
    for repeat in range(5):
        selected = np.r_[positive, rng.choice(negative, len(positive), replace=False)]
        folds = list(GroupKFold(n_splits=3).split(features[selected], labels[selected], groups[selected]))
        if any(len(np.unique(labels[selected[fit]])) < 2 for fit, _ in folds):
            continue
        models = {
            "rbf_svm": SVC(probability=True, random_state=seed + repeat),
            "random_forest": RandomForestClassifier(n_estimators=80, min_samples_leaf=2, random_state=seed + repeat),
            "logistic": LogisticRegression(max_iter=1000),
        }
        for name, model in models.items():
            pipeline = make_pipeline(StandardScaler(), PCA(n_components=min(5, grid.shape[-1])), model)
            out = np.empty(len(selected))
            for fit, validation in folds:
                pipeline.fit(features[selected[fit]], labels[selected[fit]])
                out[validation] = pipeline.predict_proba(features[selected[validation]])[:, 1]
            cutoff = optimalThreshold(labels[selected], out)
            auc, tss = score(labels[selected], out, cutoff)
            if auc >= AUC_MIN and tss >= TSS_MIN:
                pipeline.fit(features[selected], labels[selected])
                retained.append((pipeline, cutoff, {
                    "model": name, "repeat": repeat, "auc": auc, "tss": tss, "threshold": cutoff,
                    "explained_variance": float(pipeline.named_steps["pca"].explained_variance_ratio_.sum()),
                }))
    if not retained:
        raise ValueError("No candidates passed blocked cross-validation (AUC >= 0.90, TSS >= 0.70).")

    def votes(values):
        return np.mean([model.predict_proba(values)[:, 1] >= cutoff for model, cutoff, _ in retained], axis=0)

    testAuc, testTss = score(labels[test], votes(features[test]), 0.5)
    if testAuc < AUC_MIN or testTss < TSS_MIN:
        raise ValueError(f"Independent spatial validation failed: AUC {testAuc:.3f}, TSS {testTss:.3f}.")
    bounds = (features[train].min(axis=0), features[train].max(axis=0))
    return votes, bounds, {
        "auc": testAuc, "tss": testTss, "test_presences": int(labels[test].sum()),
        "test_pseudo_absences": int((labels[test] == 0).sum()),
        "test_blocks": int(len(np.unique(groups[test]))), "members": [details for _, _, details in retained],
    }


def buildBundle(manifestPath, seed):
    data = json.loads(manifestPath.read_text(encoding="utf-8"))
    layers, provenance, inside, affine, crs = loadGrid(data, manifestPath.parent)
    loss = layers.pop("tree_cover_loss_fraction", None)
    lossYear = data.get("loss_year")
    if loss is not None and (type(lossYear) is not int or not 2001 <= lossYear < datetime.now(timezone.utc).year):
        raise ValueError("Set loss_year to the completed year represented by the tree-cover loss layer.")
    names = list(layers)
    grid = np.stack(list(layers.values()), axis=-1)
    valid = inside & np.isfinite(grid).all(axis=-1)
    presence = presenceCells(manifestPath.parent / data["presences"], valid, affine, crs)
    candidateMask = (valid & (layers["elevation"] >= 1400) & (layers["elevation"] <= 3600)
                     & (layers["forest_fraction"] < 0.1))
    for row, col in presence:
        candidateMask[max(0, row - 1):row + 2, max(0, col - 1):col + 2] = False
    voters, (lower, upper), metrics = fitEnsemble(grid, presence, np.argwhere(candidateMask), seed)
    rows, cols = np.where(inside)
    wx, wy = xy(affine, rows, cols)
    lon, lat = transform(crs, "EPSG:4326", list(wx), list(wy))
    prediction = np.full(inside.shape, np.nan)
    prediction[valid] = voters(grid[valid])
    outside = np.any((grid < lower) | (grid > upper), axis=-1)
    cells = []
    for row, col, x, y in zip(rows, cols, lon, lat):
        usable = valid[row, col] and not outside[row, col]
        cells.append({
            "lon": round(x, 6), "lat": round(y, 6),
            "vote": round(float(prediction[row, col]), 6) if usable else None,
            "status": "suitable_model_domain" if usable else "extrapolation" if valid[row, col] else "nodata",
            "loss": float(loss[row, col]) if loss is not None and np.isfinite(loss[row, col]) else None,
        })
    return {
        "version": 1, "species": LANGUR, "created_at": datetime.now(timezone.utc).isoformat(),
        "cell_size_m": CELL_SIZE, "seed": seed, "loss_year": lossYear, "predictors": names,
        "presence_cells": len(presence), "metrics": metrics, "layers": provenance, "cells": cells,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate an offline Kashmir Grey Langur habitat bundle.")
    parser.add_argument("manifest", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    try:
        bundle = buildBundle(args.manifest, args.seed)
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(bundle, stream, allow_nan=False, separators=(",", ":"))
    except (OSError, KeyError, ValueError, TypeError) as error:
        parser.exit(1, f"Habitat training failed: {error}\n")
    print(f"Validated {len(bundle['metrics']['members'])} members; wrote {len(bundle['cells'])} cells.")


if __name__ == "__main__":
    main()
