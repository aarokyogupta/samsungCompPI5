import asyncio
from contextlib import closing
import csv
import cv2
from datetime import datetime, timedelta, timezone
import json
import numpy as np
import onnxruntime as ort
import os
import sqlite3
from typing import Optional
import yaml

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

ACOUSTIC_CONFIG: dict = config["acoustic"]
INFERENCE_CONFIG: dict = ACOUSTIC_CONFIG.get("inference", {})
DB_FILE: str = os.path.expanduser(config["storage"]["databasePath"])
SPATIALITE_EXT: str = config["storage"]["spatialiteExtension"]
MODEL_PATH: str = os.path.expanduser(
    INFERENCE_CONFIG.get("modelPath", "~/icmis/models/acoustic.onnx")
)
CLASS_MAP_PATH: str = os.path.expanduser(
    INFERENCE_CONFIG.get("classMapPath", "~/icmis/models/acousticClasses.csv")
)

# Define inference settings
INFERENCE_THREADS: int = int(INFERENCE_CONFIG.get("inferenceThreads", 2))
POLLING_INTERVAL_SEC: float = float(INFERENCE_CONFIG.get("pollingIntervalSec", 0.5))
INFERENCE_QUEUE_SIZE: int = int(INFERENCE_CONFIG.get("inferenceQueueSize", 32))
INFERENCE_LEASE_SEC: int = int(INFERENCE_CONFIG.get("inferenceLeaseSec", 600))
INPUT_FRAMES: int = int(INFERENCE_CONFIG.get("inputFrames", 96))
INPUT_MEL_BANDS: int = int(INFERENCE_CONFIG.get("inputMelBands", ACOUSTIC_CONFIG["melBands"]))
INPUT_LAYOUT: str = str(INFERENCE_CONFIG.get("inputLayout", "NCHW")).upper()
DEFAULT_CONFIDENCE_THRESHOLD: float = float(
    INFERENCE_CONFIG.get("confidenceThreshold", 0.65)
)
THREAT_CONFIDENCE_THRESHOLD: float = float(
    INFERENCE_CONFIG.get("threatConfidenceThreshold", 0.4)
)
DEBOUNCE_WINDOW_SEC: float = float(INFERENCE_CONFIG.get("debounceWindowSec", 4.0))
INPUT_SCALE: Optional[float] = INFERENCE_CONFIG.get("inputScale")
INPUT_ZERO_POINT: int = int(INFERENCE_CONFIG.get("inputZeroPoint", 0))
THREAT_CLASSES: set[str] = {
    str(className).strip().casefold()
    for className in INFERENCE_CONFIG.get("threatClasses", [])
}
WILDLIFE_CLASSES: set[str] = {
    str(className).strip().casefold()
    for className in INFERENCE_CONFIG.get("wildlifeClasses", [])
}
CLASS_THRESHOLDS: dict[str, float] = {
    str(className).strip().casefold(): float(threshold)
    for className, threshold in INFERENCE_CONFIG.get("classThresholds", {}).items()
}

STORED_PENDING: str = "PENDING"
PENDING: str = "QUEUED_FOR_INFERENCE"
INFERENCE_RUNNING: str = "INFERENCE_RUNNING"
PROCESSED_ACOUSTIC: str = "PROCESSED_ACOUSTIC"
FAILED_ACOUSTIC: str = "FAILED_ACOUSTIC"
VALID: str = "VALID"
MISSING_DEVICE_LOCATION: str = "MISSING_DEVICE_LOCATION"

if INFERENCE_THREADS <= 0 or INFERENCE_QUEUE_SIZE <= 0 or INFERENCE_LEASE_SEC <= 0:
    raise ValueError("Acoustic inference threads, queue size, and lease must be positive.")
if POLLING_INTERVAL_SEC <= 0 or INPUT_FRAMES <= 0 or INPUT_MEL_BANDS <= 0:
    raise ValueError("Acoustic polling and model input dimensions must be positive.")
if INPUT_LAYOUT not in {"NCHW", "NHWC"}:
    raise ValueError("Acoustic inference inputLayout must be NCHW or NHWC.")
if not 0 <= DEFAULT_CONFIDENCE_THRESHOLD <= 1 or not 0 <= THREAT_CONFIDENCE_THRESHOLD <= 1:
    raise ValueError("Acoustic confidence thresholds must be between zero and one.")
if DEBOUNCE_WINDOW_SEC < 0:
    raise ValueError("The acoustic debounce window cannot be negative.")
if any(not 0 <= threshold <= 1 for threshold in CLASS_THRESHOLDS.values()):
    raise ValueError("Per-class acoustic thresholds must be between zero and one.")

# Keep model scores and alerts available to consumers in the same process
ACOUSTIC_INFERENCE_QUEUE: asyncio.Queue[dict[str, object]] = asyncio.Queue(
    maxsize=INFERENCE_QUEUE_SIZE
)
ACOUSTIC_ALERT_QUEUE: asyncio.Queue[dict[str, object]] = asyncio.Queue(
    maxsize=INFERENCE_QUEUE_SIZE
)


def loadClassMap(classMapPath: str = CLASS_MAP_PATH) -> dict[int, str]:
    if not os.path.isfile(classMapPath):
        raise FileNotFoundError(
            f"Acoustic class map not found at '{classMapPath}'. "
            "Create the model's class-index CSV and set acoustic.inference.classMapPath."
        )

    classMap: dict[int, str] = {}
    with open(classMapPath, "r", encoding="utf-8-sig", newline="") as fileHandle:
        reader = csv.DictReader(fileHandle)
        if reader.fieldnames is None:
            raise ValueError("The acoustic class map CSV must contain a header row.")
        indexColumn = next(
            (
                name for name in reader.fieldnames
                if name.strip().casefold() in {"index", "class_id", "class_index", "id"}
            ),
            None,
        )
        labelColumn = next(
            (
                name for name in reader.fieldnames
                if name.strip().casefold()
                in {"class_name", "label", "name", "class", "display_name"}
            ),
            None,
        )
        if indexColumn is None or labelColumn is None:
            raise ValueError(
                "The acoustic class map must include index/class_id and class_name/label columns."
            )

        for row in reader:
            try:
                classIndex = int(row[indexColumn])
            except (TypeError, ValueError) as error:
                raise ValueError(f"Invalid class index in acoustic class map row: {row}") from error
            label = str(row[labelColumn]).strip()
            if classIndex < 0 or not label:
                raise ValueError(f"Invalid class index or label in acoustic class map row: {row}")
            if classIndex in classMap:
                raise ValueError(f"Duplicate class index {classIndex} in acoustic class map.")
            classMap[classIndex] = label

    if not classMap:
        raise ValueError("The acoustic class map contains no class rows.")
    return classMap


def createInferenceSession(modelPath: str = MODEL_PATH) -> ort.InferenceSession:
    if not os.path.isfile(modelPath):
        raise FileNotFoundError(
            f"Acoustic ONNX model not found at '{modelPath}'. Copy a compatible model there "
            "or update acoustic.inference.modelPath in config.yaml."
        )

    sessionOptions = ort.SessionOptions()
    sessionOptions.intra_op_num_threads = INFERENCE_THREADS
    sessionOptions.inter_op_num_threads = 1
    sessionOptions.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    # ONNX Runtime's optimized CPU provider uses the instructions available on the Pi's ARM cores.
    return ort.InferenceSession(
        modelPath,
        sess_options=sessionOptions,
        providers=["CPUExecutionProvider"],
    )


def getModelDimensions(modelInput: ort.NodeArg) -> tuple[int, int, int]:
    inputShape = modelInput.shape
    if len(inputShape) == 4 and INPUT_LAYOUT == "NCHW":
        frames, melBands = inputShape[2], inputShape[3]
    elif len(inputShape) == 4 and INPUT_LAYOUT == "NHWC":
        frames, melBands = inputShape[1], inputShape[2]
    elif len(inputShape) == 3:
        frames, melBands = inputShape[1], inputShape[2]
    else:
        raise ValueError(
            f"Expected an ONNX acoustic input with rank 3 or 4, got shape {inputShape}."
        )

    resolvedFrames = int(frames) if isinstance(frames, (int, np.integer)) and frames > 0 else INPUT_FRAMES
    resolvedMelBands = (
        int(melBands)
        if isinstance(melBands, (int, np.integer)) and melBands > 0
        else INPUT_MEL_BANDS
    )
    channelDimension = (
        inputShape[1]
        if len(inputShape) == 4 and INPUT_LAYOUT == "NCHW"
        else inputShape[3]
        if len(inputShape) == 4 and INPUT_LAYOUT == "NHWC"
        else 1
    )
    channels = (
        int(channelDimension)
        if isinstance(channelDimension, (int, np.integer))
        else 1
    )
    if channels != 1:
        raise ValueError("Acoustic spectrogram models must accept exactly one image channel.")
    return resolvedFrames, resolvedMelBands, channels


def prepareSpectrogramTensor(
    spectrogram: np.ndarray,
    frames: int,
    melBands: int,
    layout: str,
    inputType: str,
    inputScale: Optional[float] = INPUT_SCALE,
    zeroPoint: int = INPUT_ZERO_POINT,
) -> np.ndarray:
    spectrogramArray = np.asarray(spectrogram, dtype=np.float32)
    if spectrogramArray.ndim != 2 or not spectrogramArray.size:
        raise ValueError("Acoustic Log-Mel input must be a non-empty two-dimensional matrix.")
    if not np.isfinite(spectrogramArray).all():
        raise ValueError("Acoustic Log-Mel input contains NaN or infinite values.")
    if frames <= 0 or melBands <= 0:
        raise ValueError("Acoustic model input dimensions must be positive.")

    # Ingestion outputs [mel bands, time]; models consume [time, mel bands].
    timeFrequency = spectrogramArray.T
    resized = cv2.resize(
        timeFrequency,
        (melBands, frames),
        interpolation=cv2.INTER_LINEAR,
    ).astype(np.float32, copy=False)
    if inputType in {"tensor(float)", "tensor(float16)"}:
        tensor = resized.astype(np.float16 if inputType == "tensor(float16)" else np.float32)
    elif inputType in {"tensor(int8)", "tensor(uint8)"}:
        if inputScale is None or float(inputScale) <= 0:
            raise ValueError(
                "Quantized acoustic inputs require a positive acoustic.inference.inputScale."
            )
        quantized = np.rint(resized / float(inputScale) + zeroPoint)
        if inputType == "tensor(int8)":
            tensor = np.clip(quantized, -128, 127).astype(np.int8)
        else:
            tensor = np.clip(quantized, 0, 255).astype(np.uint8)
    else:
        raise TypeError(f"Unsupported ONNX acoustic input type: {inputType}")

    if layout == "NCHW":
        tensor = tensor[np.newaxis, np.newaxis, :, :]
    elif layout == "NHWC":
        tensor = tensor[np.newaxis, :, :, np.newaxis]
    else:
        raise ValueError("Acoustic inference input layout must be NCHW or NHWC.")
    return tensor


def inferSpectrogram(
    spectrogram: np.ndarray,
    session: ort.InferenceSession,
    classMap: dict[int, str],
) -> list[dict[str, object]]:
    if not classMap:
        raise ValueError("The acoustic class map must not be empty.")
    modelInput = session.get_inputs()[0]
    frames, melBands, _ = getModelDimensions(modelInput)
    if len(modelInput.shape) == 3:
        tensor = prepareSpectrogramTensor(
            spectrogram, frames, melBands, INPUT_LAYOUT, modelInput.type
        )[:, 0, :, :] if INPUT_LAYOUT == "NCHW" else prepareSpectrogramTensor(
            spectrogram, frames, melBands, INPUT_LAYOUT, modelInput.type
        )[:, :, :, 0]
    else:
        tensor = prepareSpectrogramTensor(
            spectrogram, frames, melBands, INPUT_LAYOUT, modelInput.type
        )

    outputs = session.run(None, {modelInput.name: tensor})
    if not outputs:
        raise ValueError("The acoustic model returned no output tensors.")
    scores = np.asarray(outputs[0], dtype=np.float32).squeeze()
    if scores.ndim == 0:
        scores = scores.reshape(1)
    if scores.ndim != 1:
        raise ValueError(f"Expected one class-score vector, received output shape {scores.shape}.")
    if not np.isfinite(scores).all():
        raise ValueError("The acoustic model returned NaN or infinite class scores.")
    if scores.size <= max(classMap):
        raise ValueError(
            f"The acoustic model returned {scores.size} scores, but the class map includes "
            f"index {max(classMap)}."
        )
    if np.any(scores < 0) or np.any(scores > 1):
        scores = 1.0 / (1.0 + np.exp(-np.clip(scores, -80.0, 80.0)))

    detections: list[dict[str, object]] = []
    for classIndex, className in classMap.items():
        normalizedName = className.casefold()
        if normalizedName in THREAT_CLASSES:
            category = "immediate_threat"
            threshold = CLASS_THRESHOLDS.get(normalizedName, THREAT_CONFIDENCE_THRESHOLD)
        elif normalizedName in WILDLIFE_CLASSES:
            category = "target_wildlife"
            threshold = CLASS_THRESHOLDS.get(normalizedName, DEFAULT_CONFIDENCE_THRESHOLD)
        else:
            continue
        confidence = float(scores[classIndex])
        if confidence >= threshold:
            detections.append({
                "class_id": classIndex,
                "class_name": className,
                "confidence": confidence,
                "category": category,
                "threshold": threshold,
            })
    detections.sort(key=lambda detection: float(detection["confidence"]), reverse=True)
    return detections


def parseTimestamp(timestampValue: object) -> datetime:
    if isinstance(timestampValue, datetime):
        parsedTimestamp = timestampValue
    elif isinstance(timestampValue, str):
        parsedTimestamp = datetime.fromisoformat(timestampValue.replace("Z", "+00:00"))
    else:
        raise ValueError("Acoustic window_timestamp must be an ISO timestamp or datetime.")
    if parsedTimestamp.tzinfo is None:
        parsedTimestamp = parsedTimestamp.replace(tzinfo=timezone.utc)
    return parsedTimestamp.astimezone(timezone.utc)


def ensureInferenceColumns(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute("PRAGMA table_info(acoustic_windows);")
    columns = {row[1] for row in cursor.fetchall()}
    if not columns:
        raise sqlite3.OperationalError("The acoustic_windows table does not exist.")

    columnTypes = {
        "spectrogram": "BLOB",
        "inference_metadata": "TEXT",
        "inference_started_at": "TEXT",
    }
    for columnName, columnType in columnTypes.items():
        if columnName not in columns:
            cursor.execute(
                f"ALTER TABLE acoustic_windows ADD COLUMN {columnName} {columnType};"
            )

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS acoustic_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            location_id TEXT,
            latitude REAL,
            longitude REAL,
            altitude REAL,
            class_id INTEGER NOT NULL,
            class_name TEXT NOT NULL,
            category TEXT NOT NULL,
            confidence REAL NOT NULL,
            start_timestamp TEXT NOT NULL,
            end_timestamp TEXT NOT NULL,
            window_count INTEGER NOT NULL,
            metadata TEXT NOT NULL,
            UNIQUE(device_id, class_id, start_timestamp)
        );
    """)
    cursor.execute("PRAGMA table_info(acoustic_events);")
    eventColumns = {row[1] for row in cursor.fetchall()}
    for columnName in ("latitude", "longitude", "altitude"):
        if columnName not in eventColumns:
            cursor.execute(
                f"ALTER TABLE acoustic_events ADD COLUMN {columnName} REAL;"
            )


def connectDatabase() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_FILE, timeout=30.0)
    try:
        # SpatiaLite geometry triggers on acoustic_events require the extension on every writer
        connection.enable_load_extension(True)
        connection.execute("SELECT load_extension(?);", (SPATIALITE_EXT,))
        connection.enable_load_extension(False)
    except (sqlite3.Error, AttributeError):
        connection.close()
        raise
    return connection


def saveSpectrogram(databaseRecordID: int, spectrogram: np.ndarray) -> None:
    with closing(connectDatabase()) as connection:
        with connection:
            ensureInferenceColumns(connection)
            connection.execute(
                "UPDATE acoustic_windows SET spectrogram = ? WHERE id = ?;",
                (sqlite3.Binary(encodeSpectrogram(spectrogram)), databaseRecordID),
            )


def encodeSpectrogram(spectrogram: np.ndarray) -> bytes:
    import io

    buffer = io.BytesIO()
    np.save(buffer, np.asarray(spectrogram, dtype=np.float32), allow_pickle=False)
    return buffer.getvalue()


def decodeSpectrogram(spectrogramBytes: bytes) -> np.ndarray:
    import io

    with io.BytesIO(spectrogramBytes) as buffer:
        return np.load(buffer, allow_pickle=False)


def setRecordStatus(
    databaseRecordID: int,
    status: str,
    metadata: Optional[dict[str, object]] = None,
) -> bool:
    with closing(connectDatabase()) as connection:
        with connection:
            ensureInferenceColumns(connection)
            cursor = connection.cursor()
            if status == INFERENCE_RUNNING:
                cursor.execute(
                    """
                    UPDATE acoustic_windows
                    SET processing_status = ?, inference_started_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND (
                        processing_status IN (?, ?) OR (
                            processing_status = ? AND (
                                inference_started_at IS NULL
                                OR inference_started_at <= datetime('now', ?)
                            )
                        )
                    );
                    """,
                    (
                        status,
                        databaseRecordID,
                        STORED_PENDING,
                        PENDING,
                        INFERENCE_RUNNING,
                        f"-{INFERENCE_LEASE_SEC} seconds",
                    ),
                )
            elif metadata is not None:
                cursor.execute(
                    """
                    UPDATE acoustic_windows
                    SET processing_status = ?, inference_metadata = ?,
                        inference_started_at = NULL
                    WHERE id = ? AND processing_status = ?;
                    """,
                    (
                        status,
                        json.dumps(metadata, separators=(",", ":"), allow_nan=False),
                        databaseRecordID,
                        INFERENCE_RUNNING,
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE acoustic_windows
                    SET processing_status = ?, inference_started_at = NULL
                    WHERE id = ? AND processing_status = ?;
                    """,
                    (status, databaseRecordID, INFERENCE_RUNNING),
                )
            return cursor.rowcount > 0


def getQueuedRecords(limit: int) -> list[dict[str, object]]:
    with closing(connectDatabase()) as connection:
        with connection:
            ensureInferenceColumns(connection)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT windows.id, windows.device_id, windows.location_id,
                       windows.window_timestamp, windows.validation_status,
                       devices.latitude, devices.longitude, devices.altitude,
                       windows.spectrogram
                FROM acoustic_windows AS windows
                LEFT JOIN acoustic_devices AS devices
                    ON devices.device_id = windows.device_id
                WHERE (windows.processing_status IN (?, ?) OR (
                    windows.processing_status = ? AND (
                        windows.inference_started_at IS NULL
                        OR windows.inference_started_at <= datetime('now', ?)
                    )
                )) AND windows.spectrogram IS NOT NULL
                ORDER BY windows.window_timestamp, windows.id
                LIMIT ?;
                """,
                (
                    STORED_PENDING,
                    PENDING,
                    INFERENCE_RUNNING,
                    f"-{INFERENCE_LEASE_SEC} seconds",
                    limit,
                ),
            )
            records: list[dict[str, object]] = []
            for row in cursor.fetchall():
                record = dict(row)
                record["database_record_id"] = int(record.pop("id"))
                records.append(record)
            return records


def saveDetectionEvents(
    connection: sqlite3.Connection,
    databaseRecordID: int,
    payload: dict[str, object],
    detections: list[dict[str, object]],
) -> list[dict[str, object]]:
    cursor = connection.cursor()
    newEvents: list[dict[str, object]] = []
    deviceID = str(payload.get("device_id", "unknown"))
    locationID = payload.get("location_id")
    latitude = payload.get("latitude")
    longitude = payload.get("longitude")
    altitude = payload.get("altitude")
    startTime = parseTimestamp(payload["window_timestamp"])
    endTime = startTime + timedelta(seconds=float(ACOUSTIC_CONFIG["segmentDurationSec"]))
    startText = startTime.isoformat(timespec="microseconds")
    endText = endTime.isoformat(timespec="microseconds")
    mergeBefore = (startTime - timedelta(seconds=DEBOUNCE_WINDOW_SEC)).isoformat(
        timespec="microseconds"
    )

    for detection in detections:
        classID = int(detection["class_id"])
        className = str(detection["class_name"])
        category = str(detection["category"])
        confidence = float(detection["confidence"])
        cursor.execute(
            """
            SELECT id, start_timestamp, end_timestamp, window_count, confidence, metadata
            FROM acoustic_events
            WHERE device_id = ? AND class_id = ? AND category = ?
                AND end_timestamp >= ?
            ORDER BY end_timestamp DESC
            LIMIT 1;
            """,
            (deviceID, classID, category, mergeBefore),
        )
        previousEvent = cursor.fetchone()
        if previousEvent is None:
            eventMetadata = {
                "source_window_ids": [databaseRecordID],
                "window_timestamps": [startText],
                "probability": confidence,
            }
            cursor.execute(
                """
                INSERT INTO acoustic_events (
                    device_id, location_id, latitude, longitude, altitude,
                    class_id, class_name, category, confidence, start_timestamp,
                    end_timestamp, window_count, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?);
                """,
                (
                    deviceID,
                    locationID,
                    latitude,
                    longitude,
                    altitude,
                    classID,
                    className,
                    category,
                    confidence,
                    startText,
                    endText,
                    json.dumps(eventMetadata, separators=(",", ":"), allow_nan=False),
                ),
            )
            if category == "immediate_threat":
                newEvents.append({
                    "event_id": int(cursor.lastrowid),
                    "device_id": deviceID,
                    "location_id": locationID,
                    "latitude": latitude,
                    "longitude": longitude,
                    "altitude": altitude,
                    "class_name": className,
                    "category": category,
                    "confidence": confidence,
                    "start_timestamp": startText,
                    "end_timestamp": endText,
                })
        else:
            eventID, previousStart, previousEnd, windowCount, previousConfidence, metadataText = previousEvent
            eventMetadata = json.loads(metadataText)
            windowIDs = eventMetadata.setdefault("source_window_ids", [])
            if databaseRecordID not in windowIDs:
                windowIDs.append(databaseRecordID)
                eventMetadata.setdefault("window_timestamps", []).append(startText)
                windowCount += 1
            cursor.execute(
                """
                UPDATE acoustic_events
                SET location_id = ?, latitude = ?, longitude = ?, altitude = ?,
                    confidence = ?, start_timestamp = ?, end_timestamp = ?,
                    window_count = ?, metadata = ?
                WHERE id = ?;
                """,
                (
                    locationID,
                    latitude,
                    longitude,
                    altitude,
                    max(float(previousConfidence), confidence),
                    min(str(previousStart), startText),
                    max(str(previousEnd), endText),
                    windowCount,
                    json.dumps(eventMetadata, separators=(",", ":"), allow_nan=False),
                    eventID,
                ),
            )

    cursor.execute(
        """
        UPDATE acoustic_windows
        SET processing_status = ?, inference_metadata = ?, inference_started_at = NULL
        WHERE id = ? AND processing_status = ?;
        """,
        (
            PROCESSED_ACOUSTIC,
            json.dumps(
                {
                    "detections": detections,
                    "window_timestamp": startText,
                    "device_id": deviceID,
                    "location_id": locationID,
                },
                separators=(",", ":"),
                allow_nan=False,
            ),
            databaseRecordID,
            INFERENCE_RUNNING,
        ),
    )
    if cursor.rowcount == 0:
        raise sqlite3.OperationalError(
            f"Acoustic window {databaseRecordID} changed status before inference was saved."
        )
    return newEvents


def persistResult(
    databaseRecordID: int,
    payload: dict[str, object],
    detections: list[dict[str, object]],
) -> dict[str, object]:
    latitude = payload.get("latitude")
    longitude = payload.get("longitude")
    altitude = payload.get("altitude")
    with closing(connectDatabase()) as connection:
        with connection:
            ensureInferenceColumns(connection)
            newThreatEvents = saveDetectionEvents(
                connection, databaseRecordID, payload, detections
            )

    result: dict[str, object] = {
        "database_record_id": databaseRecordID,
        "device_id": str(payload.get("device_id", "unknown")),
        "location_id": payload.get("location_id"),
        "latitude": latitude,
        "longitude": longitude,
        "altitude": altitude,
        "window_timestamp": parseTimestamp(payload["window_timestamp"]).isoformat(
            timespec="microseconds"
        ),
        "detections": detections,
        "new_threat_events": newThreatEvents,
    }
    return result


async def processQueueItem(
    payload: dict[str, object],
    session: ort.InferenceSession,
    classMap: dict[int, str],
) -> None:
    recordIDValue = payload.get("database_record_id")
    if not isinstance(recordIDValue, (int, np.integer)):
        raise ValueError("Acoustic inference payload must include database_record_id.")
    databaseRecordID = int(recordIDValue)

    claimed = await asyncio.to_thread(
        setRecordStatus, databaseRecordID, INFERENCE_RUNNING
    )
    if not claimed:
        return

    try:
        spectrogramValue = payload.get("log_mel_spectrogram")
        if spectrogramValue is None:
            spectrogramBytes = payload.get("spectrogram")
            if not isinstance(spectrogramBytes, bytes):
                raise ValueError("Acoustic inference payload has no Log-Mel spectrogram.")
            spectrogramValue = decodeSpectrogram(spectrogramBytes)
        detections = await asyncio.to_thread(
            inferSpectrogram, np.asarray(spectrogramValue), session, classMap
        )
        result = await asyncio.to_thread(
            persistResult, databaseRecordID, payload, detections
        )
        newThreatEvents = result["new_threat_events"]
        if newThreatEvents:
            try:
                ACOUSTIC_ALERT_QUEUE.put_nowait({
                    "database_record_id": databaseRecordID,
                    "device_id": result["device_id"],
                    "location_id": result["location_id"],
                    "latitude": result["latitude"],
                    "longitude": result["longitude"],
                    "altitude": result["altitude"],
                    "events": newThreatEvents,
                })
            except asyncio.QueueFull:
                print(
                    f"Acoustic alert queue is full; threat alert for window "
                    f"{databaseRecordID} was not queued."
                )
            print(
                f"High-priority acoustic threat at device {result['device_id']}: "
                f"{[detection['class_name'] for detection in detections if detection['category'] == 'immediate_threat']}"
            )
        try:
            ACOUSTIC_INFERENCE_QUEUE.put_nowait(result)
        except asyncio.QueueFull:
            print(f"Acoustic result queue is full; result for window {databaseRecordID} was not queued.")
    except Exception as error:
        failureMetadata = {"error": str(error), "database_record_id": databaseRecordID}
        await asyncio.to_thread(
            setRecordStatus,
            databaseRecordID,
            FAILED_ACOUSTIC,
            failureMetadata,
        )
        raise


async def pollDatabaseQueue(
    inferenceQueue: asyncio.Queue[dict[str, object]],
    pendingIDs: set[int],
) -> None:
    while True:
        queuedRecords = await asyncio.to_thread(getQueuedRecords, INFERENCE_QUEUE_SIZE)
        for record in queuedRecords:
            databaseRecordID = int(record["database_record_id"])
            if databaseRecordID in pendingIDs:
                continue
            pendingIDs.add(databaseRecordID)
            try:
                await inferenceQueue.put(record)
            except BaseException:
                pendingIDs.discard(databaseRecordID)
                raise
        await asyncio.sleep(POLLING_INTERVAL_SEC)


async def runInferenceWorker(
    inferenceQueue: asyncio.Queue[dict[str, object]],
    session: ort.InferenceSession,
    classMap: dict[int, str],
    pendingIDs: set[int],
) -> None:
    while True:
        payload = await inferenceQueue.get()
        recordID = payload.get("database_record_id")
        try:
            await processQueueItem(payload, session, classMap)
        except Exception as error:
            print(f"Acoustic inference failed for window {recordID}: {error}")
        finally:
            if isinstance(recordID, (int, np.integer)):
                pendingIDs.discard(int(recordID))
            inferenceQueue.task_done()


async def main(
    inferenceQueue: Optional[asyncio.Queue[dict[str, object]]] = None,
) -> None:
    print("Loading the ONNX acoustic model and class map...")
    session, classMap = await asyncio.gather(
        asyncio.to_thread(createInferenceSession),
        asyncio.to_thread(loadClassMap),
    )
    outputQueue = (
        inferenceQueue
        if inferenceQueue is not None
        else asyncio.Queue(maxsize=INFERENCE_QUEUE_SIZE)
    )
    pendingIDs: set[int] = set()
    tasks = [asyncio.create_task(
        runInferenceWorker(outputQueue, session, classMap, pendingIDs)
    )]
    if inferenceQueue is None:
        tasks.append(asyncio.create_task(pollDatabaseQueue(outputQueue, pendingIDs)))
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())