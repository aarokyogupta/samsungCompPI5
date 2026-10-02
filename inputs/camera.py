import asyncio
import cv2
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import PIL
from PIL import Image
from PIL.ExifTags import TAGS
import shutil
import sqlite3
from typing import Optional
import yaml

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Define local input paths for camera trap and drone survey ingestion
CAMERA_TRAPS_FILE_PATH: str = os.path.expanduser(config["filePaths"]["cameraTraps"])
DRONE_UPLOADS_FILE_PATH: str = os.path.expanduser(config["filePaths"]["droneUploads"])
QUARANTINE_FILE_PATH: str = os.path.expanduser(config["filePaths"]["quarantine"])
ARCHIVE_FILE_PATH: str = os.path.expanduser("~/icmis/data/archive")
DB_FILE: str = os.path.expanduser(config["storage"]["databasePath"])
SPATIALITE_EXT: str = config["storage"]["spatialiteExtension"]

# Define supported file extensions for visual ingestion
IMAGE_EXTENSIONS: set[str] = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif", ".cr2", ".nef", ".arw", ".dng", ".heic", ".heif"}
VIDEO_EXTENSIONS: set[str] = {".mp4", ".avi", ".mov", ".mkv", ".wmv", ".webm", ".flv", ".m4v"}

# Polling behaviour for filesystem-based ingestion
POLLING_INTERVAL_SEC: float = config["camera"]["pollingIntervalSec"]
FILE_STABILITY_DELAY_SEC: float = 0.5
MAX_INFERENCE_QUEUE_SIZE: int = 100

# Timestamp format used for storing & parsing camera capture timestamps
TIMESTAMP_FORMAT: str = "%Y %m %d %H %M %S %f"

# Ingestion status flags
DISCOVERED: str = "DISCOVERED"
VALIDATING: str = "VALIDATING"
PROCESSED: str = "PROCESSED"
QUEUED_FOR_INFERENCE: str = "QUEUED_FOR_INFERENCE"
INFERENCE_COMPLETE: str = "INFERENCE_COMPLETE"
PROCESSED_VISION: str = "PROCESSED_VISION"
FAILED_VISION: str = "FAILED_VISION"
QUARANTINED: str = "QUARANTINED"
MISSING_LOCATION: str = "MISSING_LOCATION"
FAILED: str = "FAILED"

VALID_INGESTION_STATUSES: set[str] = {DISCOVERED, VALIDATING, PROCESSED, QUEUED_FOR_INFERENCE, INFERENCE_COMPLETE, PROCESSED_VISION, FAILED_VISION, QUARANTINED, MISSING_LOCATION, FAILED}

@dataclass
class CameraMetadata:
    deviceID: str
    locationID: str
    sourceType: str
    sourcePath: str
    archivePath: str
    captureTimestamp: datetime
    latitude: Optional[float]
    longitude: Optional[float]
    altitude: Optional[float]
    status: str
    width: Optional[int] = None
    height: Optional[int] = None

def ensureDirectory(directoryPath: str) -> None:
    os.makedirs(directoryPath, exist_ok=True)

def getField(payloadData: dict, fields: dict, fieldName: str, default: object = None) -> object:
    if fieldName in fields:
        return fields[fieldName]
    
    return payloadData.get(fieldName, default)

def loadCameraConfig() -> dict:
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

async def fileIsStable(filePath: str, delaySeconds: float = FILE_STABILITY_DELAY_SEC) -> bool:
    if not os.path.exists(filePath):
        return False
    
    initialSize: int = os.path.getsize(filePath)
    await asyncio.sleep(delaySeconds)
    
    finalSize: int = os.path.getsize(filePath)
    return initialSize > 0 and initialSize == finalSize

def getExifTimestamp(exifData: dict) -> Optional[datetime]:
    dateTimeOriginal: Optional[str] = exifData.get(36867) or exifData.get(306)
    if not dateTimeOriginal:
        return None

    try:
        timestamp: datetime = datetime.strptime(dateTimeOriginal, "%Y:%m:%d %H:%M:%S")
        return timestamp.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

def convertGpsToDecimal(values: tuple[int, int, int, int]) -> float:
    degrees, minutes, seconds, direction = values
    decimal: float = float(degrees) + (float(minutes) / 60.0) + (float(seconds) / 3600.0)
    if direction in {"S", "W"}:
        return -decimal
    return decimal

def extractGpsCoordinates(exifData: dict) -> tuple[Optional[float], Optional[float], Optional[float]]:
    GPS_INFO_TAG: int = 34853
    gpsInfo: Optional[dict] = exifData.get(GPS_INFO_TAG)
    if not gpsInfo:
        return None, None, None

    try:
        latitudeRef = gpsInfo.get(1)
        latitudeValues = gpsInfo.get(2)
        longitudeRef = gpsInfo.get(3)
        longitudeValues = gpsInfo.get(4)
        altitude = gpsInfo.get(6)
        altitudeRef = gpsInfo.get(5)
        if latitudeValues is None or longitudeValues is None:
            return None, None, None

        latitude = convertGpsToDecimal(latitudeValues)
        longitude = convertGpsToDecimal(longitudeValues)
        if latitudeRef == "S":
            latitude = -latitude
        if longitudeRef == "W":
            longitude = -longitude

        altitudeValue: Optional[float] = None
        if altitude is not None and altitudeRef is not None:
            altitudeValue = float(altitude)
            if altitudeRef == 1:
                altitudeValue = -altitudeValue

        return latitude, longitude, altitudeValue
    except (TypeError, ValueError):
        return None, None, None

def getFileHash(filePath: str) -> str:
    hasher = hashlib.sha256()
    with open(filePath, "rb") as fileHandle:
        for chunk in iter(lambda: fileHandle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def isVideoFile(filePath: str) -> bool:
    return os.path.splitext(filePath)[1].lower() in VIDEO_EXTENSIONS

def isImageFile(filePath: str) -> bool:
    return os.path.splitext(filePath)[1].lower() in IMAGE_EXTENSIONS

async def extractMetadata(filePath: str) -> dict:
    fileExtension: str = os.path.splitext(filePath)[1].lower()
    deviceID: str = os.path.basename(os.path.dirname(filePath)) or "unknown"
    locationID: str = "unknown"
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None
    captureTimestamp: Optional[datetime] = None
    width: Optional[int] = None
    height: Optional[int] = None

    if Image is not None and isImageFile(filePath):
        with Image.open(filePath) as image:
            width, height = image.size
            exifData = image.getexif()
            if exifData:
                captureTimestamp = getExifTimestamp(exifData)
                latitude, longitude, altitude = extractGpsCoordinates(exifData)

    if captureTimestamp is None:
        captureTimestamp = datetime.now(timezone.utc)

    if latitude is not None and longitude is not None and (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        pass
    else:
        latitude = None
        longitude = None
        altitude = None

    return {
        "device_id": deviceID,
        "location_id": locationID,
        "source_type": "IMAGE" if isImageFile(filePath) else "VIDEO",
        "source_path": filePath,
        "capture_timestamp": captureTimestamp,
        "latitude": latitude,
        "longitude": longitude,
        "altitude": altitude,
        "width": width,
        "height": height,
        "hash": getFileHash(filePath),
    }

def cameraIngestionTableExists(cursor: sqlite3.Cursor) -> bool:
    cursor.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
            AND name = 'camera_ingestion';
    """)
    return cursor.fetchone() is not None

def ensureCameraIngestionColumns(cursor: sqlite3.Cursor) -> None:
    cursor.execute("PRAGMA table_info(camera_ingestion);")
    existingColumns = {row[1] for row in cursor.fetchall()}
    
    cameraIngestionColumns: dict[str, str] = {
        "device_id": "TEXT",
        "location_id": "TEXT",
        "source_type": "TEXT",
        "source_path": "TEXT",
        "archive_path": "TEXT",
        "capture_timestamp": "TEXT",
        "status": "TEXT",
        "latitude": "REAL",
        "longitude": "REAL",
        "altitude": "REAL",
        "file_hash": "TEXT",
        "width": "INTEGER",
        "height": "INTEGER",
    }

    for columnName, columnType in cameraIngestionColumns.items():
        if columnName not in existingColumns:
            cursor.execute(f"ALTER TABLE camera_ingestion ADD COLUMN {columnName} {columnType};")

async def archiveFile(filePath: str) -> str:
    ensureDirectory(ARCHIVE_FILE_PATH)
    fileName: str = os.path.basename(filePath)
    archiveDestination: str = os.path.join(ARCHIVE_FILE_PATH, fileName)
    duplicateIndex: int = 1
    while os.path.exists(archiveDestination):
        archiveDestination = os.path.join(ARCHIVE_FILE_PATH, f"{os.path.splitext(fileName)[0]}_{duplicateIndex}{os.path.splitext(fileName)[1]}")
        duplicateIndex += 1
    
    await asyncio.to_thread(shutil.copy2, filePath, archiveDestination)
    return archiveDestination

def buildSpatialPoint(latitude: Optional[float], longitude: Optional[float], altitude: Optional[float]):
    if latitude is None or longitude is None:
        return None
    return f"MakePointZ({longitude}, {latitude}, {altitude if altitude is not None else 0.0}, 4326)"

async def processFile(filePath: str, queue: asyncio.Queue, conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    try:
        if not os.path.exists(filePath):
            return

        if not await fileIsStable(filePath):
            return

        fileExtension: str = os.path.splitext(filePath)[1].lower()
        if fileExtension not in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS:
            return

        metadata: dict = await extractMetadata(filePath)
        archiveDestination: str = await archiveFile(filePath)

        latitude: Optional[float] = metadata["latitude"]
        longitude: Optional[float] = metadata["longitude"]
        altitude: Optional[float] = metadata["altitude"]

        if latitude is None or longitude is None:
            geometryValue = "NULL"
        else:
            geometryValue = f"MakePointZ({longitude}, {latitude}, {altitude if altitude is not None else 0.0}, 4326)"
        status = QUEUED_FOR_INFERENCE

        cursor.execute("""
            INSERT INTO camera_ingestion (
                device_id, location_id, source_type, source_path, archive_path, capture_timestamp,
                status, latitude, longitude, altitude, file_hash, width, height, geom
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            metadata["device_id"],
            metadata["location_id"],
            metadata["source_type"],
            metadata["source_path"],
            archiveDestination,
            metadata["capture_timestamp"].strftime(TIMESTAMP_FORMAT),
            status,
            latitude,
            longitude,
            altitude,
            metadata["hash"],
            metadata["width"],
            metadata["height"],
            geometryValue,
        ))
        databaseID: int = int(cursor.lastrowid)
        conn.commit()

        await queue.put({
            "database_id": databaseID,
            "device_id": metadata["device_id"],
            "location_id": metadata["location_id"],
            "source_type": metadata["source_type"],
            "source_path": archiveDestination,
            "capture_timestamp": metadata["capture_timestamp"].strftime(TIMESTAMP_FORMAT),
            "status": status,
            "file_hash": metadata["hash"],
        })
    except Exception as e:
        print(f"Error processing file: {filePath} - {e}")

async def pollDirectory(directoryPath: str, queue: asyncio.Queue, conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    seenFiles: set[str] = set()

    while True:
        for root, _, files in os.walk(directoryPath):
            for fileName in files:
                filePath = os.path.join(root, fileName)
                fileExtension = os.path.splitext(fileName)[1].lower()
                if fileExtension in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS and filePath not in seenFiles:
                    seenFiles.add(filePath)
                    await processFile(filePath, queue, conn, cursor)
        await asyncio.sleep(POLLING_INTERVAL_SEC)

async def queueWorker(queue: asyncio.Queue) -> None:
    while True:
        item = await queue.get()
        try:
            print(f"Queued for inference: {item}")
        finally:
            queue.task_done()

async def main() -> None:
    print(f"Connecting to SQLite camera ingestion database...")
    conn: sqlite3.Connection = sqlite3.connect(DB_FILE, timeout=30.0)
    cursor: sqlite3.Cursor = conn.cursor()

    try:
        conn.enable_load_extension(True)
        cursor.execute("SELECT load_extension(?);", (config["storage"]["spatialiteExtension"],))
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")

        if not cameraIngestionTableExists(cursor):
            cursor.execute("SELECT InitSpatialMetadata(1);")
            cursor.execute("""
                CREATE TABLE camera_ingestion (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT,
                    location_id TEXT,
                    source_type TEXT,
                    source_path TEXT,
                    archive_path TEXT,
                    capture_timestamp TEXT,
                    status TEXT,
                    latitude REAL,
                    longitude REAL,
                    altitude REAL,
                    file_hash TEXT,
                    width INTEGER,
                    height INTEGER,
                    geom GEOMETRY
                );
            """)
            cursor.execute("SELECT AddGeometryColumn('camera_ingestion', 'geom', 4326, 'POINT', 'XYZ');")
            cursor.execute("SELECT CreateSpatialIndex('camera_ingestion', 'geom');")
            conn.commit()
            print("Camera ingestion database initialized successfully.")
        else:
            ensureCameraIngestionColumns(cursor)
            conn.commit()

        queue: "asyncio.Queue[dict]" = asyncio.Queue(maxsize=MAX_INFERENCE_QUEUE_SIZE)
        queueTask = asyncio.create_task(queueWorker(queue))

        pollingTasks = [
            asyncio.create_task(pollDirectory(CAMERA_TRAPS_FILE_PATH, queue, conn, cursor)),
            asyncio.create_task(pollDirectory(DRONE_UPLOADS_FILE_PATH, queue, conn, cursor)),
        ]

        try:
            await asyncio.gather(*pollingTasks)
        finally:
            for task in pollingTasks:
                task.cancel()
            await asyncio.gather(*pollingTasks, return_exceptions=True)
            queueTask.cancel()
            await asyncio.gather(queueTask, return_exceptions=True)
    except sqlite3.OperationalError as e:
        print(f"\nDatabase Error: {e}")
    except KeyboardInterrupt:
        print("\nCamera ingestion stopped gracefully.")
    finally:
        conn.close()

if __name__ == "__main__":
    ensureDirectory(CAMERA_TRAPS_FILE_PATH)
    ensureDirectory(DRONE_UPLOADS_FILE_PATH)
    ensureDirectory(QUARANTINE_FILE_PATH)
    asyncio.run(main())