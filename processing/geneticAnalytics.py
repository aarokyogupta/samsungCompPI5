import asyncio
from datetime import datetime, timezone
import h3
import json
import math
import numpy as np
import os
import re
import sqlite3
from typing import Optional
import yaml

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

EDNA_CONFIG: dict = config["edna"]
GENETIC_CONFIG: dict = EDNA_CONFIG.get("geneticAnalysis", {})
DB_FILE: str = os.path.expanduser(config["storage"]["databasePath"])
SPATIALITE_EXT: str = config["storage"]["spatialiteExtension"]

# Define population grouping and risk settings
H3_RESOLUTION: int = int(EDNA_CONFIG["h3Resolution"])
TIME_BIN: str = str(GENETIC_CONFIG.get("timeBin", "year")).casefold()
ANALYSIS_INTERVAL_SEC: float = float(GENETIC_CONFIG.get("analysisIntervalSec", 3600.0))
MINIMUM_LOCUS_SAMPLES: int = int(GENETIC_CONFIG.get("minimumLocusSamples", 2))
CRITICAL_NE_THRESHOLD: float = float(GENETIC_CONFIG.get("criticalNeThreshold", 50.0))
VULNERABLE_NE_THRESHOLD: float = float(GENETIC_CONFIG.get("vulnerableNeThreshold", 500.0))
GENERATION_TIME_YEARS: Optional[float] = GENETIC_CONFIG.get("generationTimeYears")
GENERATION_TIME_YEARS_BY_SPECIES: dict[str, float] = {
    str(species): float(generationTime)
    for species, generationTime in GENETIC_CONFIG.get(
        "generationTimeYearsBySpecies", {}
    ).items()
}
ALERT_QUEUE_SIZE: int = int(GENETIC_CONFIG.get("alertQueueSize", 100))

if not 0 <= H3_RESOLUTION <= 15:
    raise ValueError("eDNA H3 resolution must be between 0 and 15.")
if TIME_BIN not in {"year", "season", "month", "all"}:
    raise ValueError("Genetic analysis timeBin must be year, season, month, or all.")
if ANALYSIS_INTERVAL_SEC <= 0 or MINIMUM_LOCUS_SAMPLES < 2 or ALERT_QUEUE_SIZE <= 0:
    raise ValueError("Genetic analysis intervals, minimum samples, and queue size must be positive.")
if CRITICAL_NE_THRESHOLD <= 0 or VULNERABLE_NE_THRESHOLD <= CRITICAL_NE_THRESHOLD:
    raise ValueError("Ne risk thresholds must be positive and ordered.")
if GENERATION_TIME_YEARS is not None and float(GENERATION_TIME_YEARS) <= 0:
    raise ValueError("generationTimeYears must be positive when configured.")
if any(generationTime <= 0 for generationTime in GENERATION_TIME_YEARS_BY_SPECIES.values()):
    raise ValueError("All species-specific generation times must be positive.")

CRITICAL_INBREEDING_RISK: str = "CRITICAL_INBREEDING_RISK"
VULNERABLE_POPULATION_RISK: str = "VULNERABLE_POPULATION_RISK"

# Metrics alerts can be consumed by the API/WebSocket process in the same event loop
GENETIC_ALERT_QUEUE: asyncio.Queue[dict[str, object]] = asyncio.Queue(
    maxsize=ALERT_QUEUE_SIZE
)

def getTimeBin(collectionDate: str) -> str:
    try:
        timestamp = datetime.fromisoformat(collectionDate.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"Invalid stored eDNA collection date: {collectionDate}.") from error
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    timestamp = timestamp.astimezone(timezone.utc)

    if TIME_BIN == "all":
        return "all"
    if TIME_BIN == "year":
        return f"{timestamp.year:04d}"
    if TIME_BIN == "month":
        return f"{timestamp.year:04d}-{timestamp.month:02d}"
    season = ("winter", "winter", "spring", "spring", "spring", "summer",
              "summer", "summer", "autumn", "autumn", "autumn", "winter")[timestamp.month - 1]
    seasonYear = timestamp.year - 1 if timestamp.month == 12 else timestamp.year
    return f"{seasonYear:04d}-{season}"


def normalizeAlleleState(record: dict[str, object]) -> list[str]:
    alleleState = record.get("allele_state")
    if alleleState is None or not str(alleleState).strip():
        alleleState = record.get("sequence")
    if alleleState is None or not str(alleleState).strip():
        return []

    value = str(alleleState).strip()
    # Genotype calls such as A/G contribute one gene copy per separated allele.
    alleles = [item.strip() for item in re.split(r"[/|;,]", value) if item.strip()]
    return alleles or [value]


def calculateAlleleFrequencies(
    records: list[dict[str, object]],
) -> dict[str, object]:
    alleleCopies: dict[str, float] = {}
    alleleSampleCounts: dict[str, int] = {}
    sampleIDs: set[str] = set()
    allelesBySample: dict[str, set[str]] = {}

    for record in records:
        sampleID = str(record["sample_id"])
        alleles = normalizeAlleleState(record)
        if not alleles:
            continue
        sampleIDs.add(sampleID)
        rawCopyCount = record.get("allele_count")
        try:
            copyCount = float(rawCopyCount) if rawCopyCount is not None else float(len(alleles))
        except (TypeError, ValueError) as error:
            raise ValueError(f"Invalid allele_count for sample {sampleID}.") from error
        if not np.isfinite(copyCount) or copyCount <= 0:
            continue

        if len(alleles) > 1:
            copiesPerAllele = copyCount / len(alleles)
            for allele in alleles:
                alleleCopies[allele] = alleleCopies.get(allele, 0.0) + copiesPerAllele
                allelesBySample.setdefault(sampleID, set()).add(allele)
        else:
            allele = alleles[0]
            alleleCopies[allele] = alleleCopies.get(allele, 0.0) + copyCount
            allelesBySample.setdefault(sampleID, set()).add(allele)

    for sampleAlleles in allelesBySample.values():
        for allele in sampleAlleles:
            alleleSampleCounts[allele] = alleleSampleCounts.get(allele, 0) + 1
    totalCopies = sum(alleleCopies.values())
    frequencies = {
        allele: count / totalCopies
        for allele, count in sorted(alleleCopies.items())
    } if totalCopies > 0 else {}
    return {
        "frequencies": frequencies,
        "allele_sample_counts": alleleSampleCounts,
        "sample_count": len(sampleIDs),
        "gene_copy_count": totalCopies,
        "allele_count": len(frequencies),
    }


def calculateExpectedHeterozygosity(frequencies: dict[str, float]) -> Optional[float]:
    if len(frequencies) < 2:
        return None
    heterozygosity = 1.0 - sum(frequency ** 2 for frequency in frequencies.values())
    return min(1.0, max(0.0, heterozygosity))


def calculateRarefiedRichness(
    alleleSampleCounts: dict[str, int],
    sampleCount: int,
    rarefactionSampleSize: int,
) -> Optional[float]:
    if sampleCount < rarefactionSampleSize or rarefactionSampleSize <= 0:
        return None
    if not alleleSampleCounts:
        return 0.0

    expectedAlleles = 0.0
    for observedSampleCount in alleleSampleCounts.values():
        if observedSampleCount <= 0:
            continue
        absentProbability = 0.0
        if sampleCount - observedSampleCount >= rarefactionSampleSize:
            absentProbability = math.prod(
                (sampleCount - observedSampleCount - offset) / (sampleCount - offset)
                for offset in range(rarefactionSampleSize)
            )
        expectedAlleles += 1.0 - absentProbability
    return expectedAlleles


def getCommonRarefactionSizes(
    groupedRecords: dict[tuple[str, str, str, str], list[dict[str, object]]],
) -> dict[tuple[str, str, str, str], int]:
    comparativeSamples: dict[tuple[str, str, str, str], set[str]] = {}
    for groupKey, records in groupedRecords.items():
        projectID, species, timeBin = groupKey[0], groupKey[1], groupKey[3]
        recordsByLocus: dict[str, set[str]] = {}
        for record in records:
            if normalizeAlleleState(record):
                recordsByLocus.setdefault(str(record["locus_name"]), set()).add(
                    str(record["sample_id"])
                )
        for locusName, sampleIDs in recordsByLocus.items():
            comparisonKey = (projectID, species, timeBin, locusName)
            comparativeSamples.setdefault(comparisonKey, set()).update(sampleIDs)

    minimumByComparison: dict[tuple[str, str, str, str], int] = {}
    for comparisonKey in comparativeSamples:
        projectID, species, timeBin, locusName = comparisonKey
        cellSampleCounts: list[int] = []
        for groupKey, records in groupedRecords.items():
            groupProject, groupSpecies, groupTime = (
                groupKey[0], groupKey[1], groupKey[3]
            )
            if (groupProject, groupSpecies, groupTime) != (projectID, species, timeBin):
                continue
            locusSampleIDs = {
                str(record["sample_id"])
                for record in records
                if str(record["locus_name"]) == locusName and normalizeAlleleState(record)
            }
            if locusSampleIDs:
                cellSampleCounts.append(len(locusSampleIDs))
        if cellSampleCounts:
            minimumByComparison[comparisonKey] = min(cellSampleCounts)
    return {
        comparisonKey: sampleCount
        for comparisonKey, sampleCount in minimumByComparison.items()
    }


def calculateTemporalEffectivePopulationSize(
    recordsByYear: dict[str, list[dict[str, object]]],
    generationTimeYears: Optional[float],
) -> tuple[Optional[float], Optional[str]]:
    if generationTimeYears is None or len(recordsByYear) < 2:
        return None, None

    annualFrequencies: dict[str, dict[str, dict[str, float]]] = {}
    annualSampleCounts: dict[str, dict[str, int]] = {}
    for year, yearRecords in recordsByYear.items():
        recordsByLocus: dict[str, list[dict[str, object]]] = {}
        for record in yearRecords:
            recordsByLocus.setdefault(str(record["locus_name"]), []).append(record)
        annualFrequencies[year] = {}
        annualSampleCounts[year] = {}
        for locusName, locusRecords in recordsByLocus.items():
            statistics = calculateAlleleFrequencies(locusRecords)
            if statistics["frequencies"]:
                annualFrequencies[year][locusName] = statistics["frequencies"]
                annualSampleCounts[year][locusName] = int(statistics["sample_count"])

    estimates: list[float] = []
    sortedYears = sorted(annualFrequencies)
    for firstIndex, firstYear in enumerate(sortedYears):
        for secondYear in sortedYears[firstIndex + 1:]:
            elapsedYears = int(secondYear) - int(firstYear)
            if elapsedYears <= 0:
                continue
            generationsElapsed = elapsedYears / float(generationTimeYears)
            sharedLoci = (
                annualFrequencies[firstYear].keys()
                & annualFrequencies[secondYear].keys()
            )
            for locusName in sharedLoci:
                firstFrequencies = annualFrequencies[firstYear][locusName]
                secondFrequencies = annualFrequencies[secondYear][locusName]
                firstSampleCount = annualSampleCounts[firstYear][locusName]
                secondSampleCount = annualSampleCounts[secondYear][locusName]
                alleleCategories = firstFrequencies.keys() | secondFrequencies.keys()
                correctedDriftValues: list[float] = []
                for allele in alleleCategories:
                    firstFrequency = firstFrequencies.get(allele, 0.0)
                    secondFrequency = secondFrequencies.get(allele, 0.0)
                    meanFrequency = (firstFrequency + secondFrequency) / 2.0
                    denominator = meanFrequency * (1.0 - meanFrequency)
                    if denominator <= 0:
                        continue
                    standardizedVariance = (
                        (secondFrequency - firstFrequency) ** 2 / denominator
                    )
                    samplingCorrection = (
                        1.0 / (2.0 * firstSampleCount)
                        + 1.0 / (2.0 * secondSampleCount)
                    )
                    correctedDriftValues.append(
                        max(0.0, standardizedVariance - samplingCorrection)
                    )
                if correctedDriftValues:
                    meanDrift = float(np.mean(correctedDriftValues))
                    if meanDrift > 0:
                        estimates.append(generationsElapsed / (2.0 * meanDrift))

    if not estimates:
        return None, None
    return float(np.median(estimates)), "TEMPORAL_VARIANCE_APPROXIMATION"


def calculatePopulationMetrics(
    groupedRecords: dict[tuple[str, str, str, str], list[dict[str, object]]],
    generationTimeYears: Optional[float] = GENERATION_TIME_YEARS,
) -> list[dict[str, object]]:
    rarefactionSizes = getCommonRarefactionSizes(groupedRecords)
    temporalRecords: dict[tuple[str, str, str], dict[str, list[dict[str, object]]]] = {}
    metrics: list[dict[str, object]] = []
    for (projectID, species, cellID, _), records in groupedRecords.items():
        temporalGroupKey = (projectID, species, cellID)
        for record in records:
            timestampText = str(record["collection_date"])
            try:
                year = datetime.fromisoformat(
                    timestampText.replace("Z", "+00:00")
                ).year
            except ValueError:
                continue
            temporalRecords.setdefault(temporalGroupKey, {}).setdefault(
                str(year), []
            ).append(record)

    for (projectID, species, cellID, timeBin), records in groupedRecords.items():
        recordsByLocus: dict[str, list[dict[str, object]]] = {}
        for record in records:
            recordsByLocus.setdefault(str(record["locus_name"]), []).append(record)
        groupSamples = len({str(record["sample_id"]) for record in records})
        locusMetrics: list[dict[str, object]] = []

        for locusName, locusRecords in sorted(recordsByLocus.items()):
            stats = calculateAlleleFrequencies(locusRecords)
            sampleCount = int(stats["sample_count"])
            frequencies = stats["frequencies"]
            if sampleCount < MINIMUM_LOCUS_SAMPLES or len(frequencies) < 2:
                continue
            rarefactionSize = rarefactionSizes.get(
                (projectID, species, timeBin, locusName),
                sampleCount,
            )
            rarefactionSize = min(rarefactionSize, sampleCount)
            heterozygosity = calculateExpectedHeterozygosity(frequencies)
            richness = calculateRarefiedRichness(
                stats["allele_sample_counts"],
                sampleCount,
                rarefactionSize,
            )
            if heterozygosity is None or richness is None:
                continue
            locusMetrics.append({
                "locus_name": locusName,
                "expected_heterozygosity": heterozygosity,
                "allelic_richness": richness,
                "sample_count": sampleCount,
                "rarefaction_sample_size": rarefactionSize,
                "gene_copy_count": stats["gene_copy_count"],
                "allele_frequencies": frequencies,
            })

        if not locusMetrics:
            continue

        meanHeterozygosity = float(np.mean([
            locus["expected_heterozygosity"] for locus in locusMetrics
        ]))
        meanRichness = float(np.mean([
            locus["allelic_richness"] for locus in locusMetrics
        ]))
        temporalGroupKey = (projectID, species, cellID)
        generationTime = GENERATION_TIME_YEARS_BY_SPECIES.get(
            species,
            generationTimeYears,
        )
        effectivePopulationSize, effectivePopulationMethod = (
            calculateTemporalEffectivePopulationSize(
                temporalRecords.get(temporalGroupKey, {}),
                generationTime,
            )
        )
        riskStatus = "INSUFFICIENT_DATA"
        if effectivePopulationSize is not None:
            riskStatus = "LOW_RISK"
            if effectivePopulationSize < CRITICAL_NE_THRESHOLD:
                riskStatus = CRITICAL_INBREEDING_RISK
            elif effectivePopulationSize < VULNERABLE_NE_THRESHOLD:
                riskStatus = VULNERABLE_POPULATION_RISK

        metrics.append({
            "project_id": projectID,
            "target_species": species,
            "h3_cell": cellID,
            "time_bin": timeBin,
            "sample_count": groupSamples,
            "locus_count": len(locusMetrics),
            "locus_metrics": locusMetrics,
            "mean_expected_heterozygosity": meanHeterozygosity,
            "mean_allelic_richness": meanRichness,
            "rarefaction_sample_size": min(
                locus["rarefaction_sample_size"] for locus in locusMetrics
            ),
            "effective_population_size": effectivePopulationSize,
            "effective_population_method": effectivePopulationMethod,
            "critical_inbreeding_risk": (
                effectivePopulationSize is not None
                and effectivePopulationSize < CRITICAL_NE_THRESHOLD
            ),
            "risk_status": riskStatus,
            "ne_estimate_note": (
                "Temporal variance estimate is approximate and assumes random mating, "
                "neutral loci, comparable sampling, and configured generation time."
                if effectivePopulationMethod is not None
                else "Ne unavailable: this ingestion schema has unphased locus calls and "
                "does not provide validated LD genotypes; configure generationTimeYears "
                "and collect multiple years for temporal estimation."
            ),
        })

    return metrics


def fetchEdnaRecords(connection: sqlite3.Connection) -> list[dict[str, object]]:
    cursor = connection.cursor()
    cursor.execute("PRAGMA table_info(edna_samples);")
    columns = {row[1] for row in cursor.fetchall()}
    if not columns:
        raise sqlite3.OperationalError(
            "The edna_samples table does not exist; run inputs/ednaParser.py first."
        )
    requiredColumns = {
        "sample_id", "project_id", "target_species", "collection_date",
        "h3_cell", "locus_name", "allele_count",
    }
    missingColumns = requiredColumns - columns
    if missingColumns:
        raise sqlite3.OperationalError(
            f"edna_samples is missing required columns: {', '.join(sorted(missingColumns))}."
        )
    alleleStateExpression = "allele_state" if "allele_state" in columns else "NULL"
    sequenceExpression = "sequence" if "sequence" in columns else "NULL"
    cursor.execute(f"""
        SELECT sample_id, project_id, target_species, collection_date, h3_cell,
               locus_name, allele_count, {alleleStateExpression}, {sequenceExpression}
        FROM edna_samples
        WHERE h3_cell IS NOT NULL AND locus_name IS NOT NULL;
    """)
    records: list[dict[str, object]] = []
    for row in cursor.fetchall():
        records.append({
            "sample_id": row[0],
            "project_id": row[1],
            "target_species": row[2],
            "collection_date": row[3],
            "h3_cell": row[4],
            "locus_name": row[5],
            "allele_count": row[6],
            "allele_state": row[7],
            "sequence": row[8],
        })
    return records


def groupEdnaRecords(
    records: list[dict[str, object]],
) -> dict[tuple[str, str, str, str], list[dict[str, object]]]:
    groupedRecords: dict[
        tuple[str, str, str, str],
        list[dict[str, object]],
    ] = {}
    for record in records:
        if record["allele_state"] is None and record["sequence"] is None:
            continue
        groupingKey = (
            str(record["project_id"]),
            str(record["target_species"]),
            str(record["h3_cell"]),
            getTimeBin(str(record["collection_date"])),
        )
        groupedRecords.setdefault(groupingKey, []).append(record)
    return groupedRecords


def initializeDatabase() -> sqlite3.Connection:
    databaseDirectory = os.path.dirname(DB_FILE)
    if databaseDirectory:
        os.makedirs(databaseDirectory, exist_ok=True)
    connection = sqlite3.connect(DB_FILE, timeout=30.0)
    try:
        cursor = connection.cursor()
        connection.enable_load_extension(True)
        cursor.execute("SELECT load_extension(?);", (SPATIALITE_EXT,))
        connection.enable_load_extension(False)
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'spatial_ref_sys';"
        )
        if cursor.fetchone() is None:
            cursor.execute("SELECT InitSpatialMetadata(1);")
        ensurePopulationTables(cursor)
        connection.commit()
    except (sqlite3.Error, OSError):
        connection.close()
        raise
    return connection


def ensurePopulationTables(cursor: sqlite3.Cursor) -> None:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS population_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id TEXT NOT NULL,
            target_species TEXT NOT NULL,
            h3_cell TEXT NOT NULL,
            time_bin TEXT NOT NULL,
            sample_count INTEGER NOT NULL,
            locus_count INTEGER NOT NULL,
            mean_expected_heterozygosity REAL NOT NULL,
            mean_allelic_richness REAL NOT NULL,
            rarefaction_sample_size INTEGER NOT NULL,
            effective_population_size REAL,
            effective_population_method TEXT,
            critical_inbreeding_risk INTEGER NOT NULL,
            risk_status TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            calculated_at TEXT NOT NULL,
            UNIQUE(project_id, target_species, h3_cell, time_bin)
        );
    """)
    cursor.execute("PRAGMA table_info(population_metrics);")
    metricColumns = {row[1] for row in cursor.fetchall()}
    if "geom" not in metricColumns:
        cursor.execute(
            "SELECT AddGeometryColumn('population_metrics', 'geom', 4326, 'POINT', 'XYZ');"
        )
        cursor.execute(
            "SELECT CreateSpatialIndex('population_metrics', 'geom');"
        )
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS population_alert_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id TEXT NOT NULL,
            target_species TEXT NOT NULL,
            h3_cell TEXT NOT NULL,
            time_bin TEXT NOT NULL,
            risk_status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            created_at TEXT NOT NULL,
            UNIQUE(project_id, target_species, h3_cell, time_bin, risk_status)
        );
    """)
    cursor.execute("PRAGMA table_info(population_alert_outbox);")
    outboxColumns = {row[1] for row in cursor.fetchall()}
    if "risk_status" not in outboxColumns:
        cursor.execute(
            "ALTER TABLE population_alert_outbox ADD COLUMN risk_status TEXT NOT NULL DEFAULT 'LOW_RISK';"
        )


def getCellCenter(cellID: str) -> tuple[float, float]:
    try:
        latitude, longitude = h3.cell_to_latlng(cellID)
    except (ValueError, TypeError) as error:
        raise ValueError(f"Invalid H3 cell in eDNA database: {cellID}.") from error
    if not np.isfinite([latitude, longitude]).all():
        raise ValueError(f"Invalid H3 cell center for {cellID}.")
    return float(latitude), float(longitude)


def persistPopulationMetrics(
    connection: sqlite3.Connection,
    metrics: list[dict[str, object]],
) -> list[dict[str, object]]:
    cursor = connection.cursor()
    ensurePopulationTables(cursor)
    calculatedAt = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    alerts: list[dict[str, object]] = []
    try:
        if metrics:
            cursor.execute("BEGIN IMMEDIATE;")
        for metric in metrics:
            latitude, longitude = getCellCenter(str(metric["h3_cell"]))
            metric["cell_center"] = {
                "latitude": latitude,
                "longitude": longitude,
            }
            metric["calculated_at"] = calculatedAt
            metricsJson = json.dumps(metric, separators=(",", ":"), allow_nan=False)
            cursor.execute("""
                INSERT INTO population_metrics (
                    project_id, target_species, h3_cell, time_bin, sample_count,
                    locus_count, mean_expected_heterozygosity, mean_allelic_richness,
                    rarefaction_sample_size, effective_population_size,
                    effective_population_method, critical_inbreeding_risk,
                    risk_status, metrics_json, calculated_at, geom
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    MakePointZ(?, ?, 0.0, 4326))
                ON CONFLICT(project_id, target_species, h3_cell, time_bin) DO UPDATE SET
                    sample_count = excluded.sample_count,
                    locus_count = excluded.locus_count,
                    mean_expected_heterozygosity = excluded.mean_expected_heterozygosity,
                    mean_allelic_richness = excluded.mean_allelic_richness,
                    rarefaction_sample_size = excluded.rarefaction_sample_size,
                    effective_population_size = excluded.effective_population_size,
                    effective_population_method = excluded.effective_population_method,
                    critical_inbreeding_risk = excluded.critical_inbreeding_risk,
                    risk_status = excluded.risk_status,
                    metrics_json = excluded.metrics_json,
                    calculated_at = excluded.calculated_at,
                    geom = excluded.geom;
            """, (
                metric["project_id"],
                metric["target_species"],
                metric["h3_cell"],
                metric["time_bin"],
                metric["sample_count"],
                metric["locus_count"],
                metric["mean_expected_heterozygosity"],
                metric["mean_allelic_richness"],
                metric["rarefaction_sample_size"],
                metric["effective_population_size"],
                metric["effective_population_method"],
                int(bool(metric["critical_inbreeding_risk"])),
                metric["risk_status"],
                metricsJson,
                calculatedAt,
                longitude,
                latitude,
            ))

            if metric["risk_status"] in {
                CRITICAL_INBREEDING_RISK,
                VULNERABLE_POPULATION_RISK,
            }:
                cursor.execute("""
                    INSERT OR IGNORE INTO population_alert_outbox (
                        project_id, target_species, h3_cell, time_bin,
                        risk_status, payload_json, status, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?);
                """, (
                    metric["project_id"],
                    metric["target_species"],
                    metric["h3_cell"],
                    metric["time_bin"],
                    metric["risk_status"],
                    metricsJson,
                    calculatedAt,
                ))
                if cursor.rowcount > 0:
                    alerts.append(metric)
        connection.commit()
    except (sqlite3.Error, TypeError, ValueError):
        connection.rollback()
        raise
    return alerts


async def publishPendingAlerts(
    connection: sqlite3.Connection,
    alertQueue: asyncio.Queue[dict[str, object]] = GENETIC_ALERT_QUEUE,
) -> None:
    cursor = connection.cursor()
    cursor.execute("""
        SELECT id, payload_json
        FROM population_alert_outbox
        WHERE status = 'PENDING'
        ORDER BY id;
    """)
    pendingAlerts = cursor.fetchall()
    for outboxID, payloadJson in pendingAlerts:
        alert = json.loads(payloadJson)
        try:
            alertQueue.put_nowait(alert)
        except asyncio.QueueFull:
            print(f"Population genetics alert queue full; alert {outboxID} remains pending.")
            return
        cursor.execute(
            "UPDATE population_alert_outbox SET status = 'QUEUED' WHERE id = ?;",
            (outboxID,),
        )
        connection.commit()
        print(
            f"{alert['risk_status']}: {alert['target_species']} in "
            f"H3 cell {alert['h3_cell']}."
        )


async def runAnalysisOnce(
    connection: sqlite3.Connection,
    alertQueue: asyncio.Queue[dict[str, object]] = GENETIC_ALERT_QUEUE,
) -> list[dict[str, object]]:
    records = fetchEdnaRecords(connection)
    groupedRecords = groupEdnaRecords(records)
    metrics = await asyncio.to_thread(calculatePopulationMetrics, groupedRecords)
    alerts = persistPopulationMetrics(connection, metrics)
    await publishPendingAlerts(connection, alertQueue)
    print(
        f"Calculated population metrics for {len(metrics)} population/time group(s) "
        f"from {len(records)} eDNA records."
    )
    return alerts


async def main() -> None:
    print("Connecting to the SpatiaLite eDNA database...")
    connection = initializeDatabase()
    try:
        while True:
            try:
                await runAnalysisOnce(connection)
            except (OSError, sqlite3.Error, TypeError, ValueError) as error:
                print(f"Population genetics analysis failed: {error}")
            await asyncio.sleep(ANALYSIS_INTERVAL_SEC)
    finally:
        connection.close()


if __name__ == "__main__":
    asyncio.run(main())