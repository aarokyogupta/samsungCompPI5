import asyncio
from contextlib import closing
import cv2
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

VISION_CONFIG: dict = config["vision"]
DB_FILE: str = os.path.expanduser(config["storage"]["databasePath"])
MODEL_PATH: str = os.path.expanduser(VISION_CONFIG["modelPath"])

# Define inference settings
INPUT_WIDTH: int = int(VISION_CONFIG["inputWidth"])
INPUT_HEIGHT: int = int(VISION_CONFIG["inputHeight"])
INPUT_LAYOUT: str = str(VISION_CONFIG["inputLayout"]).upper()
OUTPUT_FORMAT: str = str(VISION_CONFIG["outputFormat"]).lower()
CONFIDENCE_THRESHOLD: float = float(VISION_CONFIG["confidenceThreshold"])
IOU_THRESHOLD: float = float(VISION_CONFIG["iouThreshold"])
INFERENCE_THREADS: int = int(VISION_CONFIG["inferenceThreads"])
POLLING_INTERVAL_SEC: float = float(VISION_CONFIG["pollingIntervalSec"])
INFERENCE_QUEUE_SIZE: int = int(VISION_CONFIG["inferenceQueueSize"])
INFERENCE_LEASE_SEC: int = int(VISION_CONFIG["inferenceLeaseSec"])
HERD_PROXIMITY_PIXELS: float = float(VISION_CONFIG["herdProximityPixels"])
VIDEO_SAMPLE_INTERVAL_SEC: float = float(VISION_CONFIG["videoSampleIntervalSec"])
VIDEO_MAX_FRAMES: int = int(VISION_CONFIG["videoMaxFrames"])
INPUT_SCALE: Optional[float] = VISION_CONFIG.get("inputScale")
INPUT_ZERO_POINT: int = int(VISION_CONFIG.get("inputZeroPoint", 0))

CLASS_NAMES: list[str] = [str(name) for name in VISION_CONFIG.get("classNames", [])]
THREAT_CLASSES: set[str] = {
    str(name).casefold() for name in VISION_CONFIG.get("threatClasses", [])
}

QUEUED_FOR_INFERENCE: str = "QUEUED_FOR_INFERENCE"
INFERENCE_RUNNING: str = "INFERENCE_RUNNING"
PROCESSED_VISION: str = "PROCESSED_VISION"
FAILED_VISION: str = "FAILED_VISION"

if INPUT_WIDTH <= 0 or INPUT_HEIGHT <= 0:
    raise ValueError("Vision model input dimensions must be greater than zero.")
if INPUT_LAYOUT not in {"NCHW", "NHWC"}:
    raise ValueError("The vision input layout must be NCHW or NHWC.")
if OUTPUT_FORMAT not in {"auto", "yolov8", "yolov5"}:
    raise ValueError("The vision output format must be auto, yolov8, or yolov5.")
if not 0 <= CONFIDENCE_THRESHOLD <= 1 or not 0 <= IOU_THRESHOLD <= 1:
    raise ValueError("Vision confidence and IoU thresholds must be between zero and one.")
if (
    INFERENCE_THREADS <= 0
    or INFERENCE_QUEUE_SIZE <= 0
    or POLLING_INTERVAL_SEC <= 0
    or INFERENCE_LEASE_SEC <= 0
):
    raise ValueError("Vision thread, queue, and polling settings must be greater than zero.")
if HERD_PROXIMITY_PIXELS < 0:
    raise ValueError("The herd proximity radius cannot be negative.")
if VIDEO_SAMPLE_INTERVAL_SEC <= 0 or VIDEO_MAX_FRAMES <= 0:
    raise ValueError("Vision video sampling interval and frame limit must be greater than zero.")

# This queue can also be supplied to an ingestion process running in the same event loop
VISION_INFERENCE_QUEUE: asyncio.Queue[dict[str, object]] = asyncio.Queue(
    maxsize=INFERENCE_QUEUE_SIZE
)
VISION_ALERT_QUEUE: asyncio.Queue[dict[str, object]] = asyncio.Queue(
    maxsize=INFERENCE_QUEUE_SIZE
)


def getModelDimensions(modelInput: ort.NodeArg) -> tuple[int, int]:
    inputShape = modelInput.shape
    if INPUT_LAYOUT == "NCHW":
        modelHeight = inputShape[2] if len(inputShape) == 4 else None
        modelWidth = inputShape[3] if len(inputShape) == 4 else None
    else:
        modelHeight = inputShape[1] if len(inputShape) == 4 else None
        modelWidth = inputShape[2] if len(inputShape) == 4 else None

    height = int(modelHeight) if isinstance(modelHeight, (int, np.integer)) and modelHeight > 0 else INPUT_HEIGHT
    width = int(modelWidth) if isinstance(modelWidth, (int, np.integer)) and modelWidth > 0 else INPUT_WIDTH
    return width, height


def createInferenceSession(modelPath: str = MODEL_PATH) -> ort.InferenceSession:
    if not os.path.isfile(modelPath):
        raise FileNotFoundError(
            f"Vision ONNX model not found at '{modelPath}'. Copy a compatible model there "
            "or update vision.modelPath in config.yaml."
        )

    sessionOptions = ort.SessionOptions()
    sessionOptions.intra_op_num_threads = INFERENCE_THREADS
    sessionOptions.inter_op_num_threads = 1
    sessionOptions.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    # ONNX Runtime's CPU execution provider uses optimized kernels available on the host ARM CPU.
    return ort.InferenceSession(
        modelPath,
        sess_options=sessionOptions,
        providers=["CPUExecutionProvider"],
    )


def prepareInputTensor(
    frame: np.ndarray,
    width: int,
    height: int,
    layout: str,
    inputType: str,
    inputScale: Optional[float] = INPUT_SCALE,
    zeroPoint: int = INPUT_ZERO_POINT,
) -> tuple[np.ndarray, tuple[float, int, int]]:
    if frame.ndim != 3 or frame.shape[2] != 3:
        raise ValueError("Vision input frames must be three-channel HWC images.")
    if frame.shape[0] <= 0 or frame.shape[1] <= 0:
        raise ValueError("Vision input frame dimensions must be greater than zero.")

    sourceHeight, sourceWidth = frame.shape[:2]
    resizeScale = min(width / sourceWidth, height / sourceHeight)
    resizedWidth = max(1, round(sourceWidth * resizeScale))
    resizedHeight = max(1, round(sourceHeight * resizeScale))
    resizedFrame = cv2.resize(frame, (resizedWidth, resizedHeight), interpolation=cv2.INTER_LINEAR)
    padX = (width - resizedWidth) // 2
    padY = (height - resizedHeight) // 2
    letterboxedFrame = np.full((height, width, 3), 114, dtype=np.uint8)
    letterboxedFrame[padY:padY + resizedHeight, padX:padX + resizedWidth] = resizedFrame

    imageTensor = letterboxedFrame.astype(np.float32) / 255.0
    if inputType in {"tensor(uint8)", "tensor(int8)"}:
        if inputScale is None or float(inputScale) <= 0:
            raise ValueError(
                "Quantized ONNX inputs require a positive vision.inputScale in config.yaml."
            )
        imageTensor = np.rint(imageTensor / float(inputScale) + zeroPoint)
        if inputType == "tensor(uint8)":
            imageTensor = np.clip(imageTensor, 0, 255).astype(np.uint8)
        else:
            imageTensor = np.clip(imageTensor, -128, 127).astype(np.int8)
    elif inputType == "tensor(float16)":
        imageTensor = imageTensor.astype(np.float16)
    elif inputType == "tensor(float)":
        imageTensor = imageTensor.astype(np.float32, copy=False)
    else:
        raise TypeError(f"Unsupported ONNX vision input type: {inputType}")

    if layout == "NCHW":
        imageTensor = np.transpose(imageTensor, (2, 0, 1))
    elif layout != "NHWC":
        raise ValueError("The vision input layout must be NCHW or NHWC.")

    return np.expand_dims(imageTensor, axis=0), (resizeScale, padX, padY)


def calculateIoU(firstBox: np.ndarray, secondBox: np.ndarray) -> float:
    left = max(float(firstBox[0]), float(secondBox[0]))
    top = max(float(firstBox[1]), float(secondBox[1]))
    right = min(float(firstBox[2]), float(secondBox[2]))
    bottom = min(float(firstBox[3]), float(secondBox[3]))
    overlap = max(0.0, right - left) * max(0.0, bottom - top)
    firstArea = max(0.0, float(firstBox[2] - firstBox[0])) * max(
        0.0, float(firstBox[3] - firstBox[1])
    )
    secondArea = max(0.0, float(secondBox[2] - secondBox[0])) * max(
        0.0, float(secondBox[3] - secondBox[1])
    )
    union = firstArea + secondArea - overlap
    return overlap / union if union > 0 else 0.0


def suppressOverlappingBoxes(
    boxes: list[list[float]],
    scores: list[float],
    classIDs: list[int],
    iouThreshold: float,
) -> list[int]:
    keptIndices: list[int] = []
    for classID in set(classIDs):
        classIndices = [index for index, value in enumerate(classIDs) if value == classID]
        classIndices.sort(key=lambda index: scores[index], reverse=True)
        while classIndices:
            bestIndex = classIndices.pop(0)
            keptIndices.append(bestIndex)
            classIndices = [
                index for index in classIndices
                if calculateIoU(np.asarray(boxes[bestIndex]), np.asarray(boxes[index])) <= iouThreshold
            ]
    return sorted(keptIndices, key=lambda index: scores[index], reverse=True)


def decodeDetections(
    outputs: list[np.ndarray],
    frameWidth: int,
    frameHeight: int,
    transform: tuple[float, int, int],
) -> list[dict[str, object]]:
    resizeScale, padX, padY = transform
    decodedBoxes: list[list[float]] = []
    decodedScores: list[float] = []
    decodedClassIDs: list[int] = []
    expectedClassCount = len(CLASS_NAMES)

    for output in outputs:
        predictions = np.asarray(output)
        if predictions.ndim == 3:
            predictions = predictions[0]
        if predictions.ndim != 2 or predictions.size == 0:
            continue
        if expectedClassCount:
            expectedColumns = {expectedClassCount + 4, expectedClassCount + 5}
            if predictions.shape[0] in expectedColumns and predictions.shape[1] not in expectedColumns:
                predictions = predictions.T
        elif predictions.shape[0] <= 256 and predictions.shape[1] > 256:
            predictions = predictions.T

        if predictions.shape[1] < 5:
            continue
        detectionFormat = OUTPUT_FORMAT
        if detectionFormat == "auto":
            if expectedClassCount and predictions.shape[1] == expectedClassCount + 5:
                detectionFormat = "yolov5"
            elif not expectedClassCount and predictions.shape[1] == 85:
                detectionFormat = "yolov5"
            else:
                detectionFormat = "yolov8"

        hasObjectness = detectionFormat == "yolov5"
        classOffset = 5 if hasObjectness else 4
        if predictions.shape[1] <= classOffset:
            continue

        for prediction in predictions:
            classProbabilities = prediction[classOffset:]
            if classProbabilities.size == 0:
                continue
            classID = int(np.argmax(classProbabilities))
            confidence = float(classProbabilities[classID])
            if hasObjectness:
                confidence *= float(prediction[4])
            if not np.isfinite(confidence) or confidence < CONFIDENCE_THRESHOLD:
                continue

            centerX, centerY, boxWidth, boxHeight = (float(value) for value in prediction[:4])
            if not np.isfinite([centerX, centerY, boxWidth, boxHeight]).all():
                continue
            if boxWidth <= 0 or boxHeight <= 0:
                continue
            left = (centerX - boxWidth / 2 - padX) / resizeScale
            top = (centerY - boxHeight / 2 - padY) / resizeScale
            right = (centerX + boxWidth / 2 - padX) / resizeScale
            bottom = (centerY + boxHeight / 2 - padY) / resizeScale
            left = min(max(left, 0.0), float(frameWidth))
            top = min(max(top, 0.0), float(frameHeight))
            right = min(max(right, 0.0), float(frameWidth))
            bottom = min(max(bottom, 0.0), float(frameHeight))
            if right <= left or bottom <= top:
                continue

            decodedBoxes.append([left, top, right, bottom])
            decodedScores.append(confidence)
            decodedClassIDs.append(classID)

    keptIndices = suppressOverlappingBoxes(
        decodedBoxes, decodedScores, decodedClassIDs, IOU_THRESHOLD
    )
    detections: list[dict[str, object]] = []
    for index in keptIndices:
        classID = decodedClassIDs[index]
        species = CLASS_NAMES[classID] if classID < len(CLASS_NAMES) else f"class_{classID}"
        left, top, right, bottom = decodedBoxes[index]
        detections.append({
            "class_id": classID,
            "species": species,
            "confidence": decodedScores[index],
            "box": [left, top, right, bottom],
            "centroid": [(left + right) / 2, (top + bottom) / 2],
        })

    return detections


def buildDetectionMetadata(detections: list[dict[str, object]], frameWidth: int, frameHeight: int) -> dict[str, object]:
    counts: dict[str, int] = {}
    speciesDetections: dict[str, list[dict[str, object]]] = {}
    for detection in detections:
        species = str(detection["species"])
        counts[species] = counts.get(species, 0) + 1
        speciesDetections.setdefault(species, []).append(detection)

    herdBehavior: dict[str, bool] = {}
    for species, matchingDetections in speciesDetections.items():
        isHerd = False
        for firstIndex, firstDetection in enumerate(matchingDetections):
            firstCenter = np.asarray(firstDetection["centroid"], dtype=np.float64)
            for secondDetection in matchingDetections[firstIndex + 1:]:
                secondCenter = np.asarray(secondDetection["centroid"], dtype=np.float64)
                if float(np.linalg.norm(firstCenter - secondCenter)) <= HERD_PROXIMITY_PIXELS:
                    isHerd = True
                    break
            if isHerd:
                break
        herdBehavior[species] = isHerd

    frameArea = max(1, frameWidth * frameHeight)
    boxArea = sum(
        max(0.0, float(detection["box"][2]) - float(detection["box"][0]))
        * max(0.0, float(detection["box"][3]) - float(detection["box"][1]))
        for detection in detections
    )
    return {
        "detections": detections,
        "species_counts": counts,
        "total_count": len(detections),
        "herd_behavior": herdBehavior,
        "herd_density": min(1.0, boxArea / frameArea),
        "highest_confidence": max(
            (float(detection["confidence"]) for detection in detections), default=0.0
        ),
        "threat_detected": any(
            str(detection["species"]).casefold() in THREAT_CLASSES
            for detection in detections
        ),
    }


def inferFrame(
    frame: np.ndarray,
    session: ort.InferenceSession,
) -> dict[str, object]:
    if frame.ndim != 3 or frame.shape[2] != 3:
        raise ValueError("Decoded camera frames must be three-channel images.")
    frameHeight, frameWidth = frame.shape[:2]
    # OpenCV decodes BGR; model input is standardized to RGB.
    rgbFrame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    modelInput = session.get_inputs()[0]
    modelWidth, modelHeight = getModelDimensions(modelInput)
    tensor, transform = prepareInputTensor(
        rgbFrame,
        modelWidth,
        modelHeight,
        INPUT_LAYOUT,
        modelInput.type,
    )
    rawOutputs = session.run(None, {modelInput.name: tensor})
    return buildDetectionMetadata(
        decodeDetections(
            rawOutputs,
            frameWidth,
            frameHeight,
            transform,
        ),
        frameWidth,
        frameHeight,
    )


def readFrame(sourcePath: str) -> np.ndarray:
    if not os.path.isfile(sourcePath):
        raise FileNotFoundError(f"Archived camera media not found: {sourcePath}")
    capture = cv2.VideoCapture(sourcePath)
    try:
        success, frame = capture.read()
    finally:
        capture.release()
    if not success or frame is None:
        raise ValueError(f"Could not decode an image or video frame from: {sourcePath}")
    return frame


def inferVideo(
    sourcePath: str,
    session: ort.InferenceSession,
) -> dict[str, object]:
    capture = cv2.VideoCapture(sourcePath)
    if not capture.isOpened():
        capture.release()
        raise ValueError(f"Could not open camera video: {sourcePath}")

    framesPerSecond = float(capture.get(cv2.CAP_PROP_FPS))
    if not np.isfinite(framesPerSecond) or framesPerSecond <= 0:
        frameInterval = 1
        framesPerSecond = 0.0
    else:
        frameInterval = max(1, round(framesPerSecond * VIDEO_SAMPLE_INTERVAL_SEC))
    frameResults: list[dict[str, object]] = []
    frameIndex = 0
    try:
        while len(frameResults) < VIDEO_MAX_FRAMES:
            success, frame = capture.read()
            if not success:
                break
            if frameIndex % frameInterval == 0:
                detections = inferFrame(frame, session)
                detections["frame_index"] = frameIndex
                detections["timestamp_offset_sec"] = (
                    frameIndex / framesPerSecond if framesPerSecond > 0 else None
                )
                frameResults.append(detections)
            frameIndex += 1
    finally:
        capture.release()

    if not frameResults:
        raise ValueError(f"Could not decode any frames from camera video: {sourcePath}")

    speciesCounts: dict[str, int] = {}
    herdBehavior: dict[str, bool] = {}
    for result in frameResults:
        for species, count in result["species_counts"].items():
            speciesCounts[species] = max(speciesCounts.get(species, 0), count)
        for species, isHerd in result["herd_behavior"].items():
            herdBehavior[species] = herdBehavior.get(species, False) or isHerd

    return {
        "frame_results": frameResults,
        "sampled_frame_count": len(frameResults),
        "total_count": max(int(result["total_count"]) for result in frameResults),
        "species_counts": speciesCounts,
        "herd_behavior": herdBehavior,
        "herd_density": max(float(result["herd_density"]) for result in frameResults),
        "highest_confidence": max(float(result["highest_confidence"]) for result in frameResults),
        "threat_detected": any(bool(result["threat_detected"]) for result in frameResults),
        "video_truncated": len(frameResults) == VIDEO_MAX_FRAMES,
    }


def ensureInferenceColumns(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute("PRAGMA table_info(camera_ingestion);")
    columns = {row[1] for row in cursor.fetchall()}
    if not columns:
        raise sqlite3.OperationalError("The camera_ingestion table does not exist.")
    for columnName, columnType in (
        ("detection_metadata", "TEXT"),
        ("inference_started_at", "TEXT"),
    ):
        if columnName not in columns:
            cursor.execute(
                f"ALTER TABLE camera_ingestion ADD COLUMN {columnName} {columnType};"
            )


def setRecordStatus(databaseID: int, status: str, metadata: Optional[dict[str, object]] = None) -> bool:
    with closing(sqlite3.connect(DB_FILE, timeout=30.0)) as connection:
        with connection:
            cursor = connection.cursor()
            ensureInferenceColumns(connection)
            if metadata is None:
                cursor.execute(
                    """
                    UPDATE camera_ingestion
                    SET status = ?, inference_started_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND (
                        status = ? OR (
                            status = ? AND (
                                inference_started_at IS NULL
                                OR inference_started_at <= datetime('now', ?)
                            )
                        )
                    );
                    """,
                    (
                        status,
                        databaseID,
                        QUEUED_FOR_INFERENCE,
                        INFERENCE_RUNNING,
                        f"-{INFERENCE_LEASE_SEC} seconds",
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE camera_ingestion
                    SET status = ?, detection_metadata = ?, inference_started_at = NULL
                    WHERE id = ? AND status = ?;
                    """,
                    (
                        status,
                        json.dumps(metadata, separators=(",", ":"), allow_nan=False),
                        databaseID,
                        INFERENCE_RUNNING,
                    ),
                )
            return cursor.rowcount > 0


def getQueuedRecords(limit: int) -> list[dict[str, object]]:
    with closing(sqlite3.connect(DB_FILE, timeout=30.0)) as connection:
        with connection:
            ensureInferenceColumns(connection)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT id, archive_path, source_path, source_type, file_hash, device_id,
                       location_id, capture_timestamp
                FROM camera_ingestion
                WHERE status = ? OR (
                    status = ? AND (
                        inference_started_at IS NULL
                        OR inference_started_at <= datetime('now', ?)
                    )
                )
                ORDER BY id
                LIMIT ?;
                """,
                (
                    QUEUED_FOR_INFERENCE,
                    INFERENCE_RUNNING,
                    f"-{INFERENCE_LEASE_SEC} seconds",
                    limit,
                ),
            )
            return [dict(row) for row in cursor.fetchall()]


async def pollDatabaseQueue(
    inferenceQueue: asyncio.Queue[dict[str, object]],
    pendingIDs: set[int],
) -> None:
    while True:
        queuedRecords = await asyncio.to_thread(getQueuedRecords, INFERENCE_QUEUE_SIZE)
        for record in queuedRecords:
            databaseID = int(record["id"])
            if databaseID in pendingIDs:
                continue
            pendingIDs.add(databaseID)
            record["database_id"] = databaseID
            try:
                await inferenceQueue.put(record)
            except BaseException:
                pendingIDs.discard(databaseID)
                raise
        await asyncio.sleep(POLLING_INTERVAL_SEC)


async def processQueueItem(
    item: dict[str, object],
    session: ort.InferenceSession,
) -> None:
    databaseID = item.get("database_id")
    if databaseID is None:
        raise ValueError("Vision queue items must include a database_id.")

    claimed = await asyncio.to_thread(setRecordStatus, int(databaseID), INFERENCE_RUNNING)
    if not claimed:
        return

    try:
        frameValue = item.get("frame")
        if isinstance(frameValue, np.ndarray):
            frame = frameValue
            metadata = await asyncio.to_thread(inferFrame, frame, session)
        else:
            sourcePath = str(item.get("archive_path") or item.get("source_path") or "")
            if not sourcePath:
                raise ValueError("Vision queue item has no frame or source_path.")
            if str(item.get("source_type", "")).upper() == "VIDEO":
                metadata = await asyncio.to_thread(inferVideo, sourcePath, session)
            else:
                frame = await asyncio.to_thread(readFrame, sourcePath)
                metadata = await asyncio.to_thread(inferFrame, frame, session)
        metadata.update({
            "database_id": int(databaseID),
            "device_id": str(item.get("device_id", "unknown")),
            "location_id": str(item.get("location_id", "unknown")),
            "capture_timestamp": str(item.get("capture_timestamp", "")),
            "file_hash": str(item.get("file_hash", "")),
        })
        updated = await asyncio.to_thread(
            setRecordStatus, int(databaseID), PROCESSED_VISION, metadata
        )
        if not updated:
            raise sqlite3.OperationalError(
                f"Camera record {databaseID} changed status before detections could be stored."
            )
        if metadata["threat_detected"]:
            alert = {
                "database_id": int(databaseID),
                "device_id": metadata["device_id"],
                "capture_timestamp": metadata["capture_timestamp"],
                "detections": metadata["detections"],
            }
            try:
                VISION_ALERT_QUEUE.put_nowait(alert)
            except asyncio.QueueFull:
                print(f"Vision alert queue is full; threat alert for record {databaseID} was not queued.")
            print(f"High-priority vision detection in camera record {databaseID}: {metadata['species_counts']}")
    except Exception as error:
        failureMetadata = {
            "error": str(error),
            "database_id": int(databaseID),
        }
        await asyncio.to_thread(
            setRecordStatus, int(databaseID), FAILED_VISION, failureMetadata
        )
        raise


async def runInferenceWorker(
    inferenceQueue: asyncio.Queue[dict[str, object]],
    session: ort.InferenceSession,
    pendingIDs: set[int],
) -> None:
    while True:
        item = await inferenceQueue.get()
        try:
            await processQueueItem(item, session)
        except Exception as error:
            # The failed record is already marked and logged by processQueueItem.
            print(f"Vision worker could not complete a queue item: {error}")
        finally:
            databaseID = item.get("database_id")
            if databaseID is not None:
                pendingIDs.discard(int(databaseID))
            inferenceQueue.task_done()


async def main() -> None:
    print("Loading the ONNX vision model...")
    session = await asyncio.to_thread(createInferenceSession)
    inferenceQueue = VISION_INFERENCE_QUEUE
    pendingIDs: set[int] = set()
    tasks = [
        asyncio.create_task(pollDatabaseQueue(inferenceQueue, pendingIDs)),
        asyncio.create_task(runInferenceWorker(inferenceQueue, session, pendingIDs)),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
