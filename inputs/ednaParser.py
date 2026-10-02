import asyncio
from Bio import SeqIO
from dataclasses import dataclass
from datetime import datetime, timezone
import difflib
import hashlib
import h3
import json
import mimetypes
import numpy as np
import os
import pandas as pd
from pathlib import Path
import re
import shutil
import sqlite3
import time
from typing import Optional
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
import yaml
import zipfile

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

EDNA_CONFIG: dict = config["edna"]
DB_FILE: str = os.path.expanduser(config["storage"]["databasePath"])
SPATIALITE_EXT: str = config["storage"]["spatialiteExtension"]
INPUT_DIRECTORY: str = os.path.expanduser(EDNA_CONFIG["inputDirectory"])
QUARANTINE_DIRECTORY: str = os.path.expanduser(EDNA_CONFIG["quarantineDirectory"])
PROJECT_ID: str = str(EDNA_CONFIG["projectID"])

# Define accepted input formats
TABULAR_EXTENSIONS: set[str] = {".csv", ".tsv", ".xlsx"}
FASTA_EXTENSIONS: set[str] = {".fasta", ".fa", ".fas"}
FASTQ_EXTENSIONS: set[str] = {".fastq", ".fq"}
SUPPORTED_EXTENSIONS: set[str] = TABULAR_EXTENSIONS | FASTA_EXTENSIONS | FASTQ_EXTENSIONS

# Define validation and queue behaviour
POLLING_INTERVAL_SEC: float = float(EDNA_CONFIG["pollingIntervalSec"])
ANALYSIS_QUEUE_SIZE: int = int(EDNA_CONFIG["inferenceQueueSize"])
H3_RESOLUTION: int = int(EDNA_CONFIG["h3Resolution"])
DEFAULT_MAX_ALLELE_COUNT: int = int(EDNA_CONFIG["maxAlleleCount"])
STUDY_AREA_BOUNDS: dict = EDNA_CONFIG["studyAreaBounds"]
APPROVED_LOCI: dict = EDNA_CONFIG.get("approvedLoci", {})
SEQUENCE_ALPHABET: set[str] = {"A", "C", "G", "T", "N"}
ROW_METADATA_PATTERN = re.compile(
    r"(?i)([A-Za-z][A-Za-z0-9_-]*)\s*=\s*(\"[^\"]*\"|'[^']*'|[^\s;,|]+)"
)

if POLLING_INTERVAL_SEC <= 0 or ANALYSIS_QUEUE_SIZE <= 0:
    raise ValueError("eDNA polling interval and analysis queue size must be greater than zero.")
if not 0 <= H3_RESOLUTION <= 15:
    raise ValueError("H3 resolution must be between 0 and 15.")
if DEFAULT_MAX_ALLELE_COUNT < 0:
    raise ValueError("The default maximum allele count cannot be negative.")

# Queue for population-genetics analysis notifications
EDNA_ANALYSIS_QUEUE: asyncio.Queue[dict[str, object]] = asyncio.Queue(maxsize=ANALYSIS_QUEUE_SIZE)

# Normalize common spreadsheet header variations
HEADER_ALIASES: dict[str, set[str]] = {
    "sample_id": {"sample", "sampleid", "samplecode", "specimenid", "specimen", "individualid"},
    "project_id": {"project", "projectid", "study", "studyid"},
    "target_species": {"species", "targetspecies", "scientificname", "taxon"},
    "collection_date": {"date", "collectiondate", "collectiondatetime", "collectiontime", "timestamp"},
    "latitude": {"lat", "latitude", "ycoord", "ycoordinate", "gpslatitude"},
    "longitude": {"lon", "lng", "longitude", "xcoord", "xcoordinate", "gpslongitude"},
    "altitude": {"alt", "altitude", "elevation", "height"},
    "locus_name": {"locus", "locusname", "marker", "markername", "gene"},
    "allele_count": {"alleles", "allelecount", "count", "copycount"},
    "allele_state": {"allele", "allelestate", "genotype", "genotypecall", "variant"},
    "sequence": {"dna", "sequence", "dnasequence", "nucleotidesequence", "rawsequence"},
}


@dataclass
class ParsedInput:
    records: list[dict[str, object]]
    rejectedRows: list[dict[str, object]]


def normalizeHeader(header: object) -> str:
    normalizedHeader: str = re.sub(r"[^a-z0-9]", "", str(header).casefold())
    for canonicalName, aliases in HEADER_ALIASES.items():
        normalizedAliases = {re.sub(r"[^a-z0-9]", "", alias.casefold()) for alias in aliases | {canonicalName}}
        if normalizedHeader in normalizedAliases:
            return canonicalName

    aliasToCanonical = {
        re.sub(r"[^a-z0-9]", "", alias.casefold()): canonicalName
        for canonicalName, aliases in HEADER_ALIASES.items()
        for alias in aliases | {canonicalName}
    }
    closeAlias = difflib.get_close_matches(normalizedHeader, aliasToCanonical, n=1, cutoff=0.88)
    if closeAlias:
        return aliasToCanonical[closeAlias[0]]

    return re.sub(r"[^a-z0-9]+", "_", str(header).strip().casefold()).strip("_")


def normalizeTabularHeaders(dataFrame: pd.DataFrame) -> pd.DataFrame:
    renamedColumns = [normalizeHeader(columnName) for columnName in dataFrame.columns]
    if len(renamedColumns) != len(set(renamedColumns)):
        raise ValueError("Header normalization produced duplicate columns; rename the ambiguous source columns.")

    dataFrame.columns = renamedColumns
    return dataFrame


def isMissing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def getText(value: object, fieldName: str, required: bool = True) -> Optional[str]:
    if isMissing(value) or not str(value).strip():
        if required:
            raise ValueError(f"{fieldName} is required.")
        return None
    return str(value).strip()


def parseCollectionDate(value: object) -> str:
    if isMissing(value):
        raise ValueError("collection_date is required.")
    parsedTimestamp = pd.to_datetime(value, utc=True, errors="raise")
    if pd.isna(parsedTimestamp):
        raise ValueError(f"collection_date is invalid: {value}.")
    return parsedTimestamp.to_pydatetime().astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def parseFiniteFloat(value: object, fieldName: str, required: bool = True) -> Optional[float]:
    if isMissing(value) or (isinstance(value, str) and not value.strip()):
        if required:
            raise ValueError(f"{fieldName} is required.")
        return None
    try:
        parsedValue = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{fieldName} must be a number.") from error
    if not np.isfinite(parsedValue):
        raise ValueError(f"{fieldName} must be finite.")
    return parsedValue


def parseAlleleCount(value: object, maximumAlleleCount: int) -> int:
    parsedCount = parseFiniteFloat(value, "allele_count")
    if parsedCount is None or not parsedCount.is_integer():
        raise ValueError("allele_count must be a non-negative integer.")
    alleleCount = int(parsedCount)
    if not 0 <= alleleCount <= maximumAlleleCount:
        raise ValueError(
            f"allele_count ({alleleCount}) must be between 0 and {maximumAlleleCount}."
        )
    return alleleCount


def validateCoordinate(latitude: float, longitude: float) -> None:
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ValueError("Coordinates are outside valid WGS84 latitude/longitude bounds.")

    minLatitude = float(STUDY_AREA_BOUNDS["minLatitude"])
    maxLatitude = float(STUDY_AREA_BOUNDS["maxLatitude"])
    minLongitude = float(STUDY_AREA_BOUNDS["minLongitude"])
    maxLongitude = float(STUDY_AREA_BOUNDS["maxLongitude"])
    if not minLatitude <= latitude <= maxLatitude or not minLongitude <= longitude <= maxLongitude:
        raise ValueError("Coordinates fall outside the configured study area.")


def validateStudyAreaBounds() -> None:
    minLatitude = float(STUDY_AREA_BOUNDS["minLatitude"])
    maxLatitude = float(STUDY_AREA_BOUNDS["maxLatitude"])
    minLongitude = float(STUDY_AREA_BOUNDS["minLongitude"])
    maxLongitude = float(STUDY_AREA_BOUNDS["maxLongitude"])
    if not -90 <= minLatitude <= maxLatitude <= 90:
        raise ValueError("Configured study-area latitude bounds must be ordered within -90 to 90.")
    if not -180 <= minLongitude <= maxLongitude <= 180:
        raise ValueError("Configured study-area longitude bounds must be ordered within -180 to 180.")


def validateSequence(sequenceValue: object) -> Optional[str]:
    if isMissing(sequenceValue) or not str(sequenceValue).strip():
        return None
    sequence = re.sub(r"\s+", "", str(sequenceValue)).upper()
    invalidSymbols = set(sequence) - SEQUENCE_ALPHABET
    if invalidSymbols:
        raise ValueError(f"Sequence contains invalid nucleotide symbols: {''.join(sorted(invalidSymbols))}.")
    return sequence


def getLocusMaximum(
    targetSpecies: str,
    locusName: str,
    approvedLoci: dict[str, dict[str, int]],
) -> int:
    speciesLoci = approvedLoci.get(targetSpecies.casefold())
    if speciesLoci is None:
        raise ValueError(f"No approved assay loci are configured for target species '{targetSpecies}'.")
    for approvedName, maximumCount in speciesLoci.items():
        if approvedName.casefold() == locusName.casefold():
            return maximumCount
    raise ValueError(f"Unrecognized locus '{locusName}' for target species '{targetSpecies}'.")


def validateRecord(
    sourceRecord: dict[str, object],
    rowNumber: int,
    sourceHash: str,
    sourcePath: str,
    approvedLoci: dict[str, dict[str, int]],
    sequenceHeader: Optional[str] = None,
    meanQuality: Optional[float] = None,
) -> dict[str, object]:
    sampleID = getText(sourceRecord.get("sample_id"), "sample_id")
    projectID = getText(sourceRecord.get("project_id"), "project_id", required=False) or PROJECT_ID
    targetSpecies = getText(sourceRecord.get("target_species"), "target_species").replace("_", " ")
    locusName = getText(sourceRecord.get("locus_name"), "locus_name")
    collectionTimestamp = parseCollectionDate(sourceRecord.get("collection_date"))
    latitude = parseFiniteFloat(sourceRecord.get("latitude"), "latitude")
    longitude = parseFiniteFloat(sourceRecord.get("longitude"), "longitude")
    altitude = parseFiniteFloat(sourceRecord.get("altitude", 0.0), "altitude", required=False) or 0.0
    validateCoordinate(latitude, longitude)

    maximumAlleleCount = getLocusMaximum(targetSpecies, locusName, approvedLoci)
    alleleCount = parseAlleleCount(sourceRecord.get("allele_count"), maximumAlleleCount)
    alleleState = getText(sourceRecord.get("allele_state"), "allele_state", required=False)
    sequence = validateSequence(sourceRecord.get("sequence"))
    if sequenceHeader is not None and sequence is None:
        raise ValueError("Sequence record has an empty nucleotide sequence.")

    cellID = h3.latlng_to_cell(latitude, longitude, H3_RESOLUTION)
    return {
        "sample_id": sampleID,
        "project_id": projectID,
        "target_species": targetSpecies,
        "collection_date": collectionTimestamp,
        "latitude": latitude,
        "longitude": longitude,
        "altitude": altitude,
        "h3_cell": cellID,
        "locus_name": locusName,
        "allele_count": alleleCount,
        "allele_state": alleleState,
        "sequence": sequence,
        "sequence_header": sequenceHeader,
        "mean_quality": meanQuality,
        "source_file": os.path.basename(sourcePath),
        "source_hash": sourceHash,
        "source_row": rowNumber,
    }


def parseSequenceHeader(header: str, recordID: str) -> dict[str, object]:
    headerFields: dict[str, object] = {}
    for fieldName, fieldValue in ROW_METADATA_PATTERN.findall(header):
        canonicalName = normalizeHeader(fieldName)
        if canonicalName in HEADER_ALIASES:
            cleanedValue = fieldValue.strip("\"'")
            headerFields[canonicalName] = cleanedValue

    if "sample_id" not in headerFields:
        headerFields["sample_id"] = recordID
    if "project_id" not in headerFields:
        headerFields["project_id"] = PROJECT_ID
    return headerFields


# Route spreadsheets and sequence files to their matching parser
def loadTabularRecords(filePath: str) -> list[dict[str, object]]:
    extension = Path(filePath).suffix.casefold()
    if extension == ".csv":
        dataFrame = pd.read_csv(filePath, encoding="utf-8-sig")
    elif extension == ".tsv":
        dataFrame = pd.read_csv(filePath, sep="\t", encoding="utf-8-sig")
    elif extension == ".xlsx":
        dataFrame = pd.read_excel(filePath, engine="openpyxl")
    else:
        raise ValueError(f"Unsupported tabular format: {extension}.")

    dataFrame = normalizeTabularHeaders(dataFrame)
    requiredColumns = {"sample_id", "target_species", "collection_date", "latitude", "longitude", "locus_name", "allele_count"}
    missingColumns = requiredColumns - set(dataFrame.columns)
    if missingColumns:
        raise ValueError(f"Tabular input is missing required columns: {', '.join(sorted(missingColumns))}.")

    return [
        {columnName: rowValue for columnName, rowValue in row.items()}
        for row in dataFrame.to_dict(orient="records")
    ]


def loadSequenceRecords(filePath: str) -> list[tuple[dict[str, object], str, Optional[float]]]:
    extension = Path(filePath).suffix.casefold()
    sequenceFormat = "fastq" if extension in FASTQ_EXTENSIONS else "fasta"
    parsedRecords: list[tuple[dict[str, object], str, Optional[float]]] = []

    for record in SeqIO.parse(filePath, sequenceFormat):
        sourceFields = parseSequenceHeader(record.description, record.id)
        sequence = str(record.seq)
        sourceFields["sequence"] = sequence
        sourceFields["allele_count"] = 1

        qualityScores = record.letter_annotations.get("phred_quality")
        meanQuality = float(np.mean(qualityScores)) if qualityScores else None
        parsedRecords.append((sourceFields, record.description, meanQuality))

    if not parsedRecords:
        raise ValueError(f"No sequence records were found in {filePath}.")
    return parsedRecords


# Validate normalized sample/locus records before they are eligible for storage
def parseInputFile(filePath: str, sourceHash: str, approvedLoci: dict[str, dict[str, int]]) -> ParsedInput:
    extension = Path(filePath).suffix.casefold()
    validRecords: list[dict[str, object]] = []
    rejectedRows: list[dict[str, object]] = []

    if extension in TABULAR_EXTENSIONS:
        sourceRows = loadTabularRecords(filePath)
        recordsWithMetadata = [(sourceRow, None, None) for sourceRow in sourceRows]
    elif extension in FASTA_EXTENSIONS | FASTQ_EXTENSIONS:
        recordsWithMetadata = loadSequenceRecords(filePath)
    else:
        raise ValueError(f"Unsupported eDNA input format: {extension}.")

    seenSampleLoci: set[tuple[str, str, str]] = set()
    sampleMetadata: dict[tuple[str, str], tuple[str, str, float, float]] = {}
    for rowNumber, (sourceRow, sequenceHeader, meanQuality) in enumerate(recordsWithMetadata, start=1):
        try:
            validatedRecord = validateRecord(
                sourceRow,
                rowNumber,
                sourceHash,
                filePath,
                approvedLoci,
                sequenceHeader,
                meanQuality,
            )
            sampleLocusKey = (
                str(validatedRecord["project_id"]).casefold(),
                str(validatedRecord["sample_id"]).casefold(),
                str(validatedRecord["locus_name"]).casefold(),
            )
            if sampleLocusKey in seenSampleLoci:
                raise ValueError("Duplicate sample_id and locus_name pair in this input file.")
            sampleKey = sampleLocusKey[:2]
            profileMetadata = (
                str(validatedRecord["target_species"]).casefold(),
                str(validatedRecord["collection_date"]),
                float(validatedRecord["latitude"]),
                float(validatedRecord["longitude"]),
            )
            if sampleKey in sampleMetadata and sampleMetadata[sampleKey] != profileMetadata:
                raise ValueError("Records for the same sample_id have conflicting species, date, or coordinates.")
            sampleMetadata[sampleKey] = profileMetadata
            seenSampleLoci.add(sampleLocusKey)
            validRecords.append(validatedRecord)
        except (TypeError, ValueError) as error:
            rejectedRows.append({
                "source_row": rowNumber,
                "reason": str(error),
                "source_data": sourceRow,
                "sequence_header": sequenceHeader,
            })

    return ParsedInput(validRecords, rejectedRows)


def canonicalMimeType(filePath: str) -> Optional[str]:
    return mimetypes.guess_type(filePath, strict=False)[0]


def validateInputMimeType(filePath: str) -> None:
    extension = Path(filePath).suffix.casefold()
    mimeType = canonicalMimeType(filePath)
    acceptedMimeTypes: dict[str, set[Optional[str]]] = {
        ".xlsx": {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/octet-stream",
        },
        ".csv": {
            None,
            "text/plain",
            "text/csv",
            "application/vnd.ms-excel",
            "application/octet-stream",
        },
        ".tsv": {None, "text/plain", "text/tab-separated-values", "application/octet-stream"},
    }
    for extensionName in FASTA_EXTENSIONS | FASTQ_EXTENSIONS:
        acceptedMimeTypes[extensionName] = {None, "text/plain", "application/octet-stream"}

    if mimeType not in acceptedMimeTypes[extension]:
        raise ValueError(f"Input extension does not match its detected MIME type ({mimeType}).")


def ednaSamplesTableExists(cursor: sqlite3.Cursor) -> bool:
    cursor.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'edna_samples';")
    return cursor.fetchone() is not None


def ensureEdnaSamplesColumns(cursor: sqlite3.Cursor) -> None:
    cursor.execute("PRAGMA table_info(edna_samples);")
    existingColumns = {row[1] for row in cursor.fetchall()}
    ednaColumns: dict[str, str] = {
        "sample_id": "TEXT",
        "project_id": "TEXT",
        "target_species": "TEXT",
        "collection_date": "TEXT",
        "latitude": "REAL",
        "longitude": "REAL",
        "altitude": "REAL",
        "h3_cell": "TEXT",
        "locus_name": "TEXT",
        "allele_count": "INTEGER",
        "allele_state": "TEXT",
        "sequence": "TEXT",
        "sequence_header": "TEXT",
        "mean_quality": "REAL",
        "source_file": "TEXT",
        "source_hash": "TEXT",
        "source_row": "INTEGER",
    }
    for columnName, columnType in ednaColumns.items():
        if columnName not in existingColumns:
            cursor.execute(f"ALTER TABLE edna_samples ADD COLUMN {columnName} {columnType};")


# Initialize the eDNA schema and preserve compatible existing table columns
def ensureEdnaDatabase(cursor: sqlite3.Cursor) -> None:
    cursor.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'spatial_ref_sys';")
    if cursor.fetchone() is None:
        cursor.execute("SELECT InitSpatialMetadata(1);")

    if not ednaSamplesTableExists(cursor):
        cursor.execute("""
            CREATE TABLE edna_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sample_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                target_species TEXT NOT NULL,
                collection_date TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                altitude REAL NOT NULL DEFAULT 0.0,
                h3_cell TEXT NOT NULL,
                locus_name TEXT NOT NULL,
                allele_count INTEGER NOT NULL,
                allele_state TEXT,
                sequence TEXT,
                sequence_header TEXT,
                mean_quality REAL,
                source_file TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                source_row INTEGER NOT NULL,
                UNIQUE(source_hash, source_row)
            );
        """)
        cursor.execute("SELECT AddGeometryColumn('edna_samples', 'geom', 4326, 'POINT', 'XYZ');")
        cursor.execute("SELECT CreateSpatialIndex('edna_samples', 'geom');")
    else:
        ensureEdnaSamplesColumns(cursor)
        cursor.execute("PRAGMA table_info(edna_samples);")
        existingColumns = {row[1] for row in cursor.fetchall()}
        if "geom" not in existingColumns:
            cursor.execute("SELECT AddGeometryColumn('edna_samples', 'geom', 4326, 'POINT', 'XYZ');")
            cursor.execute("SELECT CreateSpatialIndex('edna_samples', 'geom');")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edna_approved_loci (
            target_species TEXT NOT NULL,
            locus_name TEXT NOT NULL,
            max_allele_count INTEGER NOT NULL,
            PRIMARY KEY(target_species, locus_name)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edna_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_hash TEXT NOT NULL UNIQUE,
            source_file TEXT NOT NULL,
            processed_at TEXT NOT NULL,
            sample_record_count INTEGER NOT NULL
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edna_analysis_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_hash TEXT NOT NULL,
            project_id TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            UNIQUE(source_hash, project_id)
        );
    """)


def syncApprovedLoci(cursor: sqlite3.Cursor) -> None:
    if not isinstance(APPROVED_LOCI, dict):
        raise ValueError("The edna.approvedLoci setting must map target species to locus lists.")

    for targetSpecies, locusNames in APPROVED_LOCI.items():
        if not isinstance(locusNames, list):
            raise ValueError(f"Approved loci for {targetSpecies} must be a list.")
        for locusName in locusNames:
            normalizedLocusName = str(locusName).strip()
            if not normalizedLocusName:
                raise ValueError(f"Approved loci for {targetSpecies} cannot contain an empty name.")
            cursor.execute("""
                INSERT INTO edna_approved_loci (target_species, locus_name, max_allele_count)
                VALUES (?, ?, ?)
                ON CONFLICT(target_species, locus_name) DO UPDATE SET
                    max_allele_count = excluded.max_allele_count;
            """, (str(targetSpecies).strip(), normalizedLocusName, DEFAULT_MAX_ALLELE_COUNT))


def getApprovedLoci(connection: sqlite3.Connection) -> dict[str, dict[str, int]]:
    cursor = connection.cursor()
    cursor.execute("SELECT target_species, locus_name, max_allele_count FROM edna_approved_loci;")
    approvedLoci: dict[str, dict[str, int]] = {}
    for targetSpecies, locusName, maximumCount in cursor.fetchall():
        speciesLoci = approvedLoci.setdefault(str(targetSpecies).casefold(), {})
        speciesLoci[str(locusName)] = int(maximumCount)
    return approvedLoci


# Open the SpatiaLite database and synchronize assay loci from configuration
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
        ensureEdnaDatabase(cursor)
        syncApprovedLoci(cursor)
        validateStudyAreaBounds()
        connection.commit()
    except (sqlite3.Error, TypeError, ValueError):
        connection.close()
        raise
    return connection


# Transform long-form assay rows into numerical matrices and explicit missing masks
def makeGenotypeMatrices(records: list[dict[str, object]]) -> dict[str, object]:
    dataFrame = pd.DataFrame(records)
    sortedSamples = sorted(dataFrame["sample_id"].astype(str).unique())
    sortedLoci = sorted(dataFrame["locus_name"].astype(str).unique())

    alleleCountMatrix = dataFrame.pivot(
        index="sample_id",
        columns="locus_name",
        values="allele_count",
    ).reindex(index=sortedSamples, columns=sortedLoci)
    alleleCountMatrix = alleleCountMatrix.astype(object).where(pd.notna(alleleCountMatrix), None)

    categoryCodes: dict[str, dict[str, int]] = {}
    genotypeMatrix = np.full((len(sortedSamples), len(sortedLoci)), -1, dtype=np.int32)
    sampleIndexes = {sampleID: index for index, sampleID in enumerate(sortedSamples)}
    locusIndexes = {locusName: index for index, locusName in enumerate(sortedLoci)}

    for locusName in sortedLoci:
        locusRows = dataFrame[dataFrame["locus_name"] == locusName]
        genotypeValues = [
            str(row["allele_state"] if row["allele_state"] is not None else row["sequence"])
            for row in locusRows.to_dict(orient="records")
            if row["allele_state"] is not None or row["sequence"] is not None
        ]
        categories = {value: index for index, value in enumerate(sorted(set(genotypeValues)))}
        categoryCodes[locusName] = categories
        for row in locusRows.to_dict(orient="records"):
            genotypeValue = row["allele_state"] if row["allele_state"] is not None else row["sequence"]
            if genotypeValue is not None:
                genotypeMatrix[sampleIndexes[str(row["sample_id"])], locusIndexes[str(locusName)]] = categories[
                    str(genotypeValue)
                ]

    missingMask = genotypeMatrix == -1
    return {
        "sample_ids": sortedSamples,
        "locus_names": sortedLoci,
        "allele_count_matrix": alleleCountMatrix.values.tolist(),
        "genotype_matrix": genotypeMatrix.tolist(),
        "genotype_codes": categoryCodes,
        "missing_mask": missingMask.tolist(),
        "missing_value": -1,
    }


def getGridBounds(cellIDs: list[str]) -> dict[str, float]:
    cellBoundaries = [h3.cell_to_boundary(cellID) for cellID in cellIDs]
    latitudes = [coordinate[0] for boundary in cellBoundaries for coordinate in boundary]
    longitudes = [coordinate[1] for boundary in cellBoundaries for coordinate in boundary]
    return {
        "min_latitude": min(latitudes),
        "max_latitude": max(latitudes),
        "min_longitude": min(longitudes),
        "max_longitude": max(longitudes),
    }


def makeAnalysisNotifications(records: list[dict[str, object]]) -> list[dict[str, object]]:
    recordsByProject: dict[str, list[dict[str, object]]] = {}
    for record in records:
        recordsByProject.setdefault(str(record["project_id"]), []).append(record)

    notifications: list[dict[str, object]] = []
    for projectID, projectRecords in recordsByProject.items():
        cells = sorted({str(record["h3_cell"]) for record in projectRecords})
        matrixData = makeGenotypeMatrices(projectRecords)
        notifications.append({
            "project_id": projectID,
            "sample_count": len({str(record["sample_id"]) for record in projectRecords}),
            "record_count": len(projectRecords),
            "h3_resolution": H3_RESOLUTION,
            "h3_cells": cells,
            "spatial_grid_bounds": getGridBounds(cells),
            **matrixData,
        })
    return notifications


# Persist every valid row and its analysis notification in one transaction
def persistBatch(
    connection: sqlite3.Connection,
    sourceHash: str,
    sourcePath: str,
    records: list[dict[str, object]],
    notifications: list[dict[str, object]],
) -> bool:
    cursor = connection.cursor()
    cursor.execute("SELECT 1 FROM edna_batches WHERE source_hash = ?;", (sourceHash,))
    if cursor.fetchone() is not None:
        return False

    sampleRows = [
        (
            record["sample_id"], record["project_id"], record["target_species"],
            record["collection_date"], record["latitude"], record["longitude"],
            record["altitude"], record["h3_cell"], record["locus_name"],
            record["allele_count"], record["allele_state"], record["sequence"],
            record["sequence_header"], record["mean_quality"], record["source_file"],
            record["source_hash"], record["source_row"], record["latitude"],
            record["longitude"], record["altitude"],
        )
        for record in records
    ]

    try:
        cursor.execute("BEGIN IMMEDIATE;")
        cursor.executemany("""
            INSERT INTO edna_samples (
                sample_id, project_id, target_species, collection_date, latitude,
                longitude, altitude, h3_cell, locus_name, allele_count, allele_state,
                sequence, sequence_header, mean_quality, source_file, source_hash,
                source_row, geom
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                MakePointZ(?, ?, ?, 4326));
        """, sampleRows)
        cursor.execute("""
            INSERT INTO edna_batches (source_hash, source_file, processed_at, sample_record_count)
            VALUES (?, ?, ?, ?);
        """, (
            sourceHash,
            os.path.basename(sourcePath),
            datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            len(records),
        ))
        cursor.executemany("""
            INSERT INTO edna_analysis_outbox (source_hash, project_id, payload_json, status)
            VALUES (?, ?, ?, 'PENDING');
        """, [
            (sourceHash, str(notification["project_id"]), json.dumps(notification, allow_nan=False))
            for notification in notifications
        ])
        connection.commit()
    except (sqlite3.Error, TypeError, ValueError):
        connection.rollback()
        raise

    return True


async def publishOutbox(
    connection: sqlite3.Connection,
    analysisQueue: asyncio.Queue[dict[str, object]],
    sourceHash: Optional[str] = None,
) -> None:
    cursor = connection.cursor()
    if sourceHash is None:
        cursor.execute("""
            SELECT id, source_hash, payload_json
            FROM edna_analysis_outbox
            WHERE status = 'PENDING'
            ORDER BY id;
        """)
    else:
        cursor.execute("""
            SELECT id, source_hash, payload_json
            FROM edna_analysis_outbox
            WHERE status = 'PENDING' AND source_hash = ?
            ORDER BY id;
        """, (sourceHash,))
    pendingNotifications = cursor.fetchall()

    for outboxID, notificationHash, payloadJson in pendingNotifications:
        payload = json.loads(payloadJson)
        payload["batch_id"] = notificationHash
        await analysisQueue.put(payload)
        cursor.execute("UPDATE edna_analysis_outbox SET status = 'QUEUED' WHERE id = ?;", (outboxID,))
        connection.commit()


def writeQuarantineRows(filePath: str, sourceHash: str, rejectedRows: list[dict[str, object]]) -> None:
    if not rejectedRows:
        return
    os.makedirs(QUARANTINE_DIRECTORY, exist_ok=True)
    quarantineName = f"{Path(filePath).stem}_{sourceHash[:12]}.quarantine.jsonl"
    quarantinePath = os.path.join(QUARANTINE_DIRECTORY, quarantineName)
    with open(quarantinePath, "w", encoding="utf-8") as quarantineFile:
        for rejectedRow in rejectedRows:
            quarantineFile.write(json.dumps(rejectedRow, ensure_ascii=False, default=str) + "\n")


def quarantineFile(filePath: str, sourceHash: Optional[str] = None) -> str:
    os.makedirs(QUARANTINE_DIRECTORY, exist_ok=True)
    fileHash = sourceHash or "invalid"
    destination = os.path.join(
        QUARANTINE_DIRECTORY,
        f"{Path(filePath).stem}_{fileHash[:12]}{Path(filePath).suffix}",
    )
    if os.path.exists(destination):
        destination = os.path.join(
            QUARANTINE_DIRECTORY,
            f"{Path(filePath).stem}_{fileHash[:12]}_{int(time.time())}{Path(filePath).suffix}",
        )
    return shutil.move(filePath, destination)


def getFileHash(filePath: str) -> str:
    hasher = hashlib.sha256()
    with open(filePath, "rb") as fileHandle:
        for chunk in iter(lambda: fileHandle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def fileIsStable(filePath: str, delaySeconds: float = 0.5) -> bool:
    try:
        initialStat = os.stat(filePath)
    except FileNotFoundError:
        return False
    time.sleep(delaySeconds)
    try:
        finalStat = os.stat(filePath)
    except FileNotFoundError:
        return False
    return (
        initialStat.st_size > 0
        and initialStat.st_size == finalStat.st_size
        and initialStat.st_mtime_ns == finalStat.st_mtime_ns
    )


async def processInputFile(
    filePath: str,
    connection: sqlite3.Connection,
    analysisQueue: asyncio.Queue[dict[str, object]],
) -> bool:
    if not await asyncio.to_thread(fileIsStable, filePath):
        return False

    sourceHash = await asyncio.to_thread(getFileHash, filePath)
    cursor = connection.cursor()
    cursor.execute("SELECT 1 FROM edna_batches WHERE source_hash = ?;", (sourceHash,))
    if cursor.fetchone() is not None:
        await publishOutbox(connection, analysisQueue, sourceHash)
        return True

    try:
        validateInputMimeType(filePath)
        approvedLoci = getApprovedLoci(connection)
        parsedInput = await asyncio.to_thread(parseInputFile, filePath, sourceHash, approvedLoci)
        if not parsedInput.records:
            writeQuarantineRows(filePath, sourceHash, parsedInput.rejectedRows)
            quarantinePath = await asyncio.to_thread(quarantineFile, filePath, sourceHash)
            print(f"No valid eDNA records found; quarantined file: {quarantinePath}")
            return True
        notifications = makeAnalysisNotifications(parsedInput.records)
    except (OSError, RuntimeError, TypeError, ValueError, zipfile.BadZipFile) as error:
        quarantinePath = await asyncio.to_thread(quarantineFile, filePath, sourceHash)
        print(f"Quarantined invalid eDNA input {quarantinePath}: {error}")
        return True

    writeQuarantineRows(filePath, sourceHash, parsedInput.rejectedRows)
    # Database failures leave the source in place so the next scan can retry it.
    persistBatch(connection, sourceHash, filePath, parsedInput.records, notifications)
    await publishOutbox(connection, analysisQueue, sourceHash)
    print(
        f"Stored {len(parsedInput.records)} eDNA records from {os.path.basename(filePath)}; "
        f"quarantined {len(parsedInput.rejectedRows)} invalid row(s)."
    )
    return True


# Bridge filesystem watcher events to the async file-processing queue
class EdnaFileEventHandler(FileSystemEventHandler):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        fileQueue: asyncio.Queue[str],
        queuedPaths: set[str],
        completedFiles: dict[str, tuple[int, int]],
    ):
        super().__init__()
        self.loop = loop
        self.fileQueue = fileQueue
        self.queuedPaths = queuedPaths
        self.completedFiles = completedFiles

    def queuePath(self, filePath: str) -> None:
        if Path(filePath).suffix.casefold() not in SUPPORTED_EXTENSIONS:
            return

        normalizedPath = os.path.abspath(filePath)

        def enqueueOnLoop() -> None:
            try:
                fileStat = os.stat(normalizedPath)
            except FileNotFoundError:
                return
            fileSignature = (fileStat.st_mtime_ns, fileStat.st_size)
            if self.completedFiles.get(normalizedPath) == fileSignature:
                return
            if normalizedPath in self.queuedPaths:
                return
            if self.fileQueue.full():
                print(f"eDNA file queue is full; file will be discovered by the next scan: {normalizedPath}")
                return
            self.queuedPaths.add(normalizedPath)
            self.fileQueue.put_nowait(normalizedPath)

        self.loop.call_soon_threadsafe(enqueueOnLoop)

    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.queuePath(str(event.src_path))

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.queuePath(str(event.src_path))

    def on_moved(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.queuePath(str(event.dest_path))


async def scanInputDirectory(
    fileQueue: asyncio.Queue[str],
    queuedPaths: set[str],
    completedFiles: dict[str, tuple[int, int]],
) -> None:
    while True:
        for root, _, fileNames in os.walk(INPUT_DIRECTORY):
            for fileName in fileNames:
                if Path(fileName).suffix.casefold() not in SUPPORTED_EXTENSIONS:
                    continue
                filePath = os.path.abspath(os.path.join(root, fileName))
                try:
                    fileStat = os.stat(filePath)
                except FileNotFoundError:
                    continue
                fileSignature = (fileStat.st_mtime_ns, fileStat.st_size)
                if (
                    filePath in queuedPaths
                    or completedFiles.get(filePath) == fileSignature
                    or fileQueue.full()
                ):
                    continue
                queuedPaths.add(filePath)
                fileQueue.put_nowait(filePath)
        await asyncio.sleep(POLLING_INTERVAL_SEC)


async def fileWorker(
    fileQueue: asyncio.Queue[str],
    queuedPaths: set[str],
    completedFiles: dict[str, tuple[int, int]],
    connection: sqlite3.Connection,
    analysisQueue: asyncio.Queue[dict[str, object]],
) -> None:
    while True:
        filePath = await fileQueue.get()
        try:
            wasProcessed = await processInputFile(filePath, connection, analysisQueue)
            if wasProcessed:
                try:
                    fileStat = os.stat(filePath)
                    completedFiles[filePath] = (fileStat.st_mtime_ns, fileStat.st_size)
                except FileNotFoundError:
                    pass
        except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error, zipfile.BadZipFile) as error:
            print(f"Error processing eDNA file {filePath}: {error}")
        finally:
            queuedPaths.discard(filePath)
            fileQueue.task_done()


async def main(
    analysisQueue: Optional[asyncio.Queue[dict[str, object]]] = None,
) -> None:
    os.makedirs(INPUT_DIRECTORY, exist_ok=True)
    os.makedirs(QUARANTINE_DIRECTORY, exist_ok=True)
    connection = initializeDatabase()
    outputQueue = analysisQueue if analysisQueue is not None else EDNA_ANALYSIS_QUEUE
    await publishOutbox(connection, outputQueue)

    loop = asyncio.get_running_loop()
    fileQueue: asyncio.Queue[str] = asyncio.Queue(maxsize=ANALYSIS_QUEUE_SIZE)
    queuedPaths: set[str] = set()
    completedFiles: dict[str, tuple[int, int]] = {}
    eventHandler = EdnaFileEventHandler(loop, fileQueue, queuedPaths, completedFiles)
    observer = Observer()
    observer.schedule(eventHandler, INPUT_DIRECTORY, recursive=True)
    observer.start()

    scanTask = asyncio.create_task(scanInputDirectory(fileQueue, queuedPaths, completedFiles))
    workerTask = asyncio.create_task(
        fileWorker(fileQueue, queuedPaths, completedFiles, connection, outputQueue)
    )
    print(f"Watching eDNA input directory: {INPUT_DIRECTORY}")
    try:
        await asyncio.gather(scanTask, workerTask)
    finally:
        scanTask.cancel()
        workerTask.cancel()
        await asyncio.gather(scanTask, workerTask, return_exceptions=True)
        observer.stop()
        observer.join(timeout=5.0)
        connection.close()


if __name__ == "__main__":
    asyncio.run(main())
