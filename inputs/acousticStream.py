import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from math import gcd
import numpy as np
import os
from pathlib import Path
import queue
from scipy import signal
import shutil
import sounddevice as sd
import soundfile as sf
import sqlite3
import subprocess
from typing import Optional
import uuid
import yaml

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

ACOUSTIC_CONFIG: dict = config["acoustic"]
DB_FILE: str = os.path.expanduser(config["storage"]["databasePath"])
SPATIALITE_EXT: str = config["storage"]["spatialiteExtension"]
INPUT_DIRECTORY: str = os.path.expanduser(ACOUSTIC_CONFIG["inputDirectory"])

# Define supported audio formats
AUDIO_EXTENSIONS: set[str] = {".wav", ".flac", ".mp3"}

# Define signal-processing settings
POLLING_INTERVAL_SEC: float = float(ACOUSTIC_CONFIG["pollingIntervalSec"])
TARGET_SAMPLE_RATE: int = int(ACOUSTIC_CONFIG["targetSampleRate"])
SEGMENT_DURATION_SEC: float = float(ACOUSTIC_CONFIG["segmentDurationSec"])
OVERLAP_SEC: float = float(ACOUSTIC_CONFIG["overlapSec"])
N_FFT: int = int(ACOUSTIC_CONFIG["nFft"])
HOP_LENGTH: int = int(ACOUSTIC_CONFIG["hopLength"])
MEL_BANDS: int = int(ACOUSTIC_CONFIG["melBands"])
WORK_QUEUE_SIZE: int = int(ACOUSTIC_CONFIG["workQueueSize"])
INFERENCE_QUEUE_SIZE: int = int(ACOUSTIC_CONFIG["inferenceQueueSize"])

LIVE_ENABLED: bool = bool(ACOUSTIC_CONFIG["liveEnabled"])
LIVE_DEVICE_ID: str = str(ACOUSTIC_CONFIG["liveDeviceID"])
LIVE_INPUT_SAMPLE_RATE: int = int(ACOUSTIC_CONFIG["liveInputSampleRate"])
LIVE_CHANNELS: int = int(ACOUSTIC_CONFIG["liveChannels"])
LIVE_BUFFER_CHUNKS: int = int(ACOUSTIC_CONFIG["liveBufferChunks"])
DEFAULT_DEVICE_ID: str = str(ACOUSTIC_CONFIG.get("defaultDeviceID", "unknown"))

VALID: str = "VALID"
MISSING_DEVICE_LOCATION: str = "MISSING_DEVICE_LOCATION"
QUEUED_FOR_INFERENCE: str = "QUEUED_FOR_INFERENCE"
QUEUE_OVERFLOW_DROPPED: str = "QUEUE_OVERFLOW_DROPPED"

if TARGET_SAMPLE_RATE <= 0:
    raise ValueError("The acoustic target sample rate must be greater than zero.")
if SEGMENT_DURATION_SEC <= 0 or not 0 <= OVERLAP_SEC < SEGMENT_DURATION_SEC:
    raise ValueError("The acoustic segment duration must be positive and overlap must be within the segment.")
if N_FFT < 2 or HOP_LENGTH <= 0 or HOP_LENGTH > N_FFT:
    raise ValueError("The acoustic FFT size and hop length are invalid.")
if MEL_BANDS <= 0 or WORK_QUEUE_SIZE <= 0 or INFERENCE_QUEUE_SIZE <= 0:
    raise ValueError("Acoustic mel-band and queue sizes must be greater than zero.")
if LIVE_INPUT_SAMPLE_RATE <= 0 or LIVE_CHANNELS <= 0:
    raise ValueError("Acoustic live input settings must be greater than zero.")

SEGMENT_SAMPLES: int = round(SEGMENT_DURATION_SEC * TARGET_SAMPLE_RATE)
STRIDE_SAMPLES: int = round((SEGMENT_DURATION_SEC - OVERLAP_SEC) * TARGET_SAMPLE_RATE)
if SEGMENT_SAMPLES < N_FFT or STRIDE_SAMPLES < 1:
    raise ValueError("The configured segment and overlap must provide enough samples for the selected FFT.")

# Keep spectrograms in memory for the downstream acoustic inference consumer
ACOUSTIC_INFERENCE_QUEUE: asyncio.Queue[dict[str, object]] = asyncio.Queue(maxsize=INFERENCE_QUEUE_SIZE)


@dataclass
class AudioWorkItem:
    workType: str
    sourceID: str
    deviceID: str
    originTimestamp: datetime
    sourcePath: Optional[str] = None
    samples: Optional[np.ndarray] = None
    sampleRate: Optional[int] = None
    discontinuity: bool = False
    sourceSampleRate: Optional[int] = None
    sourceChannels: Optional[int] = None
    sourceBitDepth: Optional[int] = None
    sourceSubtype: Optional[str] = None


@dataclass
class LiveBufferState:
    workItem: AudioWorkItem
    samples: np.ndarray
    startSample: int = 0
    windowIndex: int = 0


def ensureDirectory(directoryPath: str) -> None:
    os.makedirs(directoryPath, exist_ok=True)


def getFileHash(filePath: str) -> str:
    hasher = hashlib.sha256()
    with open(filePath, "rb") as fileHandle:
        for chunk in iter(lambda: fileHandle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def normalizePcm(samples: np.ndarray) -> np.ndarray:
    # Convert integer PCM to float32 in the standard -1.0 to 1.0 range
    if np.issubdtype(samples.dtype, np.integer):
        bitDepth: int = np.iinfo(samples.dtype).bits
        normalizedSamples = samples.astype(np.float32) / float(2 ** (bitDepth - 1))
    else:
        normalizedSamples = samples.astype(np.float32, copy=False)

    if not np.isfinite(normalizedSamples).all():
        raise ValueError("Audio contains NaN or infinite samples.")

    return np.clip(normalizedSamples, -1.0, 1.0)


def downmixToMono(samples: np.ndarray) -> np.ndarray:
    normalizedSamples = normalizePcm(samples)
    if normalizedSamples.ndim == 2:
        normalizedSamples = np.mean(normalizedSamples, axis=1, dtype=np.float32)
    elif normalizedSamples.ndim != 1:
        raise ValueError(f"Expected one- or two-dimensional audio, received {normalizedSamples.ndim} dimensions.")

    return np.asarray(normalizedSamples, dtype=np.float32)


def resampleAudio(samples: np.ndarray, originalSampleRate: int, targetSampleRate: int) -> np.ndarray:
    if originalSampleRate <= 0 or targetSampleRate <= 0:
        raise ValueError("Audio sample rates must be greater than zero.")
    if originalSampleRate == targetSampleRate:
        return np.asarray(samples, dtype=np.float32)

    divisor: int = gcd(originalSampleRate, targetSampleRate)
    upsampleFactor: int = targetSampleRate // divisor
    downsampleFactor: int = originalSampleRate // divisor
    resampledSamples = signal.resample_poly(samples, upsampleFactor, downsampleFactor)
    return np.asarray(resampledSamples, dtype=np.float32)


def getBitDepth(subtype: Optional[str]) -> Optional[int]:
    if subtype is None:
        return None
    if subtype.rpartition("_")[2].isdigit():
        return int(subtype.rpartition("_")[2])
    return {"FLOAT": 32, "DOUBLE": 64}.get(subtype.upper())


def runFfmpegDecode(filePath: str) -> tuple[np.ndarray, int, int, Optional[int], Optional[str]]:
    ffprobePath = shutil.which("ffprobe")
    ffmpegPath = shutil.which("ffmpeg")
    if ffprobePath is None or ffmpegPath is None:
        raise RuntimeError("FFmpeg and ffprobe are required to decode this audio format.")

    probe = subprocess.run(
        [
            ffprobePath,
            "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=sample_rate,channels,bits_per_sample,bits_per_raw_sample,codec_name",
            "-of", "json",
            filePath,
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    if probe.returncode != 0:
        raise RuntimeError(f"ffprobe could not read audio metadata: {probe.stderr.strip()}")

    audioStreams = json.loads(probe.stdout).get("streams", [])
    if not audioStreams:
        raise ValueError(f"No audio stream was found in {filePath}.")

    sampleRate: int = int(audioStreams[0]["sample_rate"])
    channels: int = int(audioStreams[0]["channels"])
    bitDepthValue = audioStreams[0].get("bits_per_raw_sample") or audioStreams[0].get("bits_per_sample")
    bitDepth = int(bitDepthValue) if bitDepthValue and int(bitDepthValue) > 0 else None
    subtype = audioStreams[0].get("codec_name")
    decoded = subprocess.run(
        [
            ffmpegPath,
            "-v", "error",
            "-i", filePath,
            "-map", "0:a:0",
            "-f", "f32le",
            "-acodec", "pcm_f32le",
            "pipe:1",
        ],
        capture_output=True,
        check=False,
    )
    if decoded.returncode != 0:
        errorText = decoded.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"FFmpeg could not decode {filePath}: {errorText}")

    samples = np.frombuffer(decoded.stdout, dtype="<f4")
    if samples.size == 0 or samples.size % channels != 0:
        raise ValueError(f"FFmpeg returned an invalid PCM stream for {filePath}.")

    return samples.reshape(-1, channels), sampleRate, channels, bitDepth, subtype


def readAudioFile(filePath: str) -> tuple[np.ndarray, int, int, Optional[int], Optional[str]]:
    try:
        with sf.SoundFile(filePath) as audioFile:
            sampleRate = audioFile.samplerate
            channels = audioFile.channels
            subtype = audioFile.subtype
            samples = audioFile.read(dtype="float32", always_2d=True)
            bitDepth = getBitDepth(subtype)
    except (RuntimeError, OSError, ValueError) as decodeError:
        try:
            samples, sampleRate, channels, bitDepth, subtype = runFfmpegDecode(filePath)
        except (OSError, RuntimeError, ValueError) as fallbackError:
            raise RuntimeError(f"Could not decode audio file {filePath}: {fallbackError}") from decodeError

    if sampleRate <= 0 or samples.size == 0:
        raise ValueError(f"Audio file {filePath} contains no samples or has an invalid sample rate.")

    monoSamples = downmixToMono(np.asarray(samples))
    return (
        resampleAudio(monoSamples, int(sampleRate), TARGET_SAMPLE_RATE),
        int(sampleRate),
        int(channels),
        bitDepth,
        subtype,
    )


def hzToMel(frequency: np.ndarray | float) -> np.ndarray | float:
    return 2595.0 * np.log10(1.0 + np.asarray(frequency) / 700.0)


def melToHz(mel: np.ndarray | float) -> np.ndarray | float:
    return 700.0 * (10.0 ** (np.asarray(mel) / 2595.0) - 1.0)


def createMelFilterbank(sampleRate: int, nFft: int, melBands: int) -> np.ndarray:
    maxFrequency: float = sampleRate / 2.0
    melEdges = np.linspace(hzToMel(0.0), hzToMel(maxFrequency), melBands + 2)
    frequencyEdges = np.asarray(melToHz(melEdges), dtype=np.float64)
    fftFrequencies = np.fft.rfftfreq(nFft, d=1.0 / sampleRate)
    filterbank = np.zeros((melBands, len(fftFrequencies)), dtype=np.float32)

    for bandIndex in range(melBands):
        lowerFrequency = frequencyEdges[bandIndex]
        centerFrequency = frequencyEdges[bandIndex + 1]
        upperFrequency = frequencyEdges[bandIndex + 2]
        risingSlope = (fftFrequencies - lowerFrequency) / max(centerFrequency - lowerFrequency, 1e-12)
        fallingSlope = (upperFrequency - fftFrequencies) / max(upperFrequency - centerFrequency, 1e-12)
        filterbank[bandIndex] = np.maximum(0.0, np.minimum(risingSlope, fallingSlope))

    if np.any(np.sum(filterbank, axis=1) == 0):
        raise ValueError("The selected FFT size is too small for the configured number of Mel bands.")

    return filterbank


def generateLogMelSpectrogram(samples: np.ndarray) -> np.ndarray:
    if len(samples) != SEGMENT_SAMPLES:
        raise ValueError(f"Expected {SEGMENT_SAMPLES} samples per segment, received {len(samples)}.")

    _, _, stftValues = signal.stft(
        samples,
        fs=TARGET_SAMPLE_RATE,
        window="hann",
        nperseg=N_FFT,
        noverlap=N_FFT - HOP_LENGTH,
        nfft=N_FFT,
        boundary=None,
        padded=False,
    )
    powerSpectrum = np.square(np.abs(stftValues), dtype=np.float32)
    melFilterbank = createMelFilterbank(TARGET_SAMPLE_RATE, N_FFT, MEL_BANDS)
    melPower = melFilterbank @ powerSpectrum
    epsilon: float = np.finfo(np.float32).eps
    return np.asarray(10.0 * np.log10(np.maximum(melPower, epsilon)), dtype=np.float32)


def createSpatialMetadata(cursor: sqlite3.Cursor) -> None:
    cursor.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'spatial_ref_sys';")
    if cursor.fetchone() is None:
        cursor.execute("SELECT InitSpatialMetadata(1);")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS acoustic_devices (
            device_id TEXT PRIMARY KEY,
            location_id TEXT,
            latitude REAL,
            longitude REAL,
            altitude REAL
        );
    """)
    cursor.execute("PRAGMA table_info(acoustic_devices);")
    deviceColumns = {row[1] for row in cursor.fetchall()}
    if "geom" not in deviceColumns:
        cursor.execute("SELECT AddGeometryColumn('acoustic_devices', 'geom', 4326, 'POINT', 'XYZ');")
        cursor.execute("SELECT CreateSpatialIndex('acoustic_devices', 'geom');")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS acoustic_windows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            source_path TEXT,
            window_index INTEGER NOT NULL,
            device_id TEXT NOT NULL,
            location_id TEXT,
            start_sample INTEGER NOT NULL,
            window_timestamp TEXT NOT NULL,
            sample_rate INTEGER NOT NULL,
            source_sample_rate INTEGER,
            source_channels INTEGER,
            source_bit_depth INTEGER,
            source_subtype TEXT,
            segment_duration_sec REAL NOT NULL,
            validation_status TEXT NOT NULL,
            processing_status TEXT NOT NULL,
            UNIQUE(source_id, window_index)
        );
    """)
    cursor.execute("PRAGMA table_info(acoustic_windows);")
    windowColumns = {row[1] for row in cursor.fetchall()}
    windowMetadataColumns: dict[str, str] = {
        "source_sample_rate": "INTEGER",
        "source_channels": "INTEGER",
        "source_bit_depth": "INTEGER",
        "source_subtype": "TEXT",
        "spectrogram": "BLOB",
        "inference_metadata": "TEXT",
        "inference_started_at": "TEXT",
    }
    for columnName, columnType in windowMetadataColumns.items():
        if columnName not in windowColumns:
            cursor.execute(f"ALTER TABLE acoustic_windows ADD COLUMN {columnName} {columnType};")
    if "geom" not in windowColumns:
        cursor.execute("SELECT AddGeometryColumn('acoustic_windows', 'geom', 4326, 'POINT', 'XYZ');")
        cursor.execute("SELECT CreateSpatialIndex('acoustic_windows', 'geom');")


def syncAcousticDevices(cursor: sqlite3.Cursor) -> None:
    devices = ACOUSTIC_CONFIG.get("devices", [])
    if not isinstance(devices, list):
        raise ValueError("The acoustic.devices setting must be a list.")

    for device in devices:
        if not isinstance(device, dict):
            raise ValueError("Each acoustic device entry must be a mapping.")

        deviceID = str(device.get("deviceID", "")).strip()
        if not deviceID:
            raise ValueError("Each acoustic device entry must have a non-empty deviceID.")

        locationID = str(device.get("locationID", "")).strip() or None
        latitude = device.get("latitude")
        longitude = device.get("longitude")
        altitude = device.get("altitude", 0.0)
        if (latitude is None) != (longitude is None):
            raise ValueError(f"Acoustic device {deviceID} must provide both latitude and longitude or neither.")

        if latitude is not None:
            latitude = float(latitude)
            longitude = float(longitude)
            altitude = float(altitude)
            if not np.isfinite([latitude, longitude, altitude]).all():
                raise ValueError(f"Acoustic device {deviceID} coordinates must be finite numbers.")
            if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
                raise ValueError(f"Acoustic device {deviceID} has coordinates outside valid bounds.")

        cursor.execute("""
            INSERT INTO acoustic_devices (device_id, location_id, latitude, longitude, altitude, geom)
            VALUES (?, ?, ?, ?, ?, CASE WHEN ? IS NULL THEN NULL ELSE MakePointZ(?, ?, ?, 4326) END)
            ON CONFLICT(device_id) DO UPDATE SET
                location_id = excluded.location_id,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                altitude = excluded.altitude,
                geom = excluded.geom;
        """, (
            deviceID, locationID, latitude, longitude, altitude,
            latitude, longitude, latitude, altitude,
        ))


def getDeviceLocation(cursor: sqlite3.Cursor, deviceID: str) -> Optional[dict[str, object]]:
    cursor.execute("""
        SELECT location_id, latitude, longitude, altitude
        FROM acoustic_devices
        WHERE device_id = ?;
    """, (deviceID,))
    row = cursor.fetchone()
    if row is None:
        return None

    return {
        "location_id": row[0],
        "latitude": row[1],
        "longitude": row[2],
        "altitude": row[3],
    }


def initializeDatabase() -> sqlite3.Connection:
    ensureDirectory(os.path.dirname(DB_FILE))
    connection = sqlite3.connect(DB_FILE, timeout=30.0)
    cursor = connection.cursor()
    connection.enable_load_extension(True)
    cursor.execute("SELECT load_extension(?);", (SPATIALITE_EXT,))
    connection.enable_load_extension(False)
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    createSpatialMetadata(cursor)
    syncAcousticDevices(cursor)
    connection.commit()
    return connection


def makeAudioWindow(
    connection: sqlite3.Connection,
    sourceID: str,
    sourcePath: Optional[str],
    windowIndex: int,
    deviceID: str,
    startSample: int,
    originTimestamp: datetime,
    sourceSampleRate: int,
    sourceChannels: int,
    sourceBitDepth: Optional[int],
    sourceSubtype: Optional[str],
) -> Optional[int]:
    cursor = connection.cursor()
    deviceLocation = getDeviceLocation(cursor, deviceID)
    hasLocation = (
        deviceLocation is not None
        and deviceLocation["latitude"] is not None
        and deviceLocation["longitude"] is not None
    )
    validationStatus: str = VALID if hasLocation else MISSING_DEVICE_LOCATION
    locationID = deviceLocation["location_id"] if deviceLocation is not None else None
    latitude = deviceLocation["latitude"] if deviceLocation is not None else None
    longitude = deviceLocation["longitude"] if deviceLocation is not None else None
    altitude = deviceLocation["altitude"] if deviceLocation is not None else None
    windowTimestamp = originTimestamp + timedelta(seconds=startSample / TARGET_SAMPLE_RATE)
    timestampText = windowTimestamp.astimezone(timezone.utc).isoformat(timespec="microseconds")

    if not hasLocation:
        cursor.execute("""
            INSERT OR IGNORE INTO acoustic_windows (
                source_id, source_path, window_index, device_id, location_id, start_sample,
                window_timestamp, sample_rate, source_sample_rate, source_channels,
                source_bit_depth, source_subtype, segment_duration_sec, validation_status,
                processing_status, geom
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL);
        """, (
            sourceID, sourcePath, windowIndex, deviceID, locationID, startSample,
            timestampText, TARGET_SAMPLE_RATE, sourceSampleRate, sourceChannels,
            sourceBitDepth, sourceSubtype, SEGMENT_DURATION_SEC, validationStatus, "PENDING",
        ))
    else:
        cursor.execute("""
            INSERT OR IGNORE INTO acoustic_windows (
                source_id, source_path, window_index, device_id, location_id, start_sample,
                window_timestamp, sample_rate, source_sample_rate, source_channels,
                source_bit_depth, source_subtype, segment_duration_sec, validation_status,
                processing_status, geom
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, MakePointZ(?, ?, ?, 4326));
        """, (
            sourceID, sourcePath, windowIndex, deviceID, locationID, startSample,
            timestampText, TARGET_SAMPLE_RATE, sourceSampleRate, sourceChannels,
            sourceBitDepth, sourceSubtype, SEGMENT_DURATION_SEC, validationStatus,
            "PENDING", longitude, latitude, altitude if altitude is not None else 0.0,
        ))

    if cursor.rowcount == 0:
        return None
    connection.commit()
    return int(cursor.lastrowid)


def updateQueueStatus(connection: sqlite3.Connection, recordID: int, status: str) -> None:
    cursor = connection.cursor()
    cursor.execute("UPDATE acoustic_windows SET processing_status = ? WHERE id = ?;", (status, recordID))
    connection.commit()


def encodeSpectrogram(spectrogram: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, np.asarray(spectrogram, dtype=np.float32), allow_pickle=False)
    return buffer.getvalue()


def persistSpectrogram(
    connection: sqlite3.Connection,
    recordID: int,
    spectrogram: np.ndarray,
) -> None:
    cursor = connection.cursor()
    cursor.execute("PRAGMA table_info(acoustic_windows);")
    existingColumns = {row[1] for row in cursor.fetchall()}
    if "spectrogram" not in existingColumns:
        cursor.execute("ALTER TABLE acoustic_windows ADD COLUMN spectrogram BLOB;")
    cursor.execute(
        "UPDATE acoustic_windows SET spectrogram = ? WHERE id = ?;",
        (sqlite3.Binary(encodeSpectrogram(spectrogram)), recordID),
    )
    connection.commit()


def enqueueInferencePayload(
    inferenceQueue: asyncio.Queue[dict[str, object]],
    payload: dict[str, object],
    connection: sqlite3.Connection,
) -> None:
    if inferenceQueue.full():
        droppedPayload = inferenceQueue.get_nowait()
        droppedRecordID = droppedPayload.get("database_record_id")
        if isinstance(droppedRecordID, int):
            updateQueueStatus(connection, droppedRecordID, QUEUE_OVERFLOW_DROPPED)
        inferenceQueue.task_done()
        print("Acoustic inference queue is full; dropped its oldest window.")

    inferenceQueue.put_nowait(payload)


async def persistAudioWindow(
    samples: np.ndarray,
    workItem: AudioWorkItem,
    windowIndex: int,
    startSample: int,
    connection: sqlite3.Connection,
    inferenceQueue: asyncio.Queue[dict[str, object]],
) -> None:
    logMelSpectrogram = await asyncio.to_thread(generateLogMelSpectrogram, samples)
    recordID = makeAudioWindow(
        connection,
        workItem.sourceID,
        workItem.sourcePath,
        windowIndex,
        workItem.deviceID,
        startSample,
        workItem.originTimestamp,
        workItem.sourceSampleRate or TARGET_SAMPLE_RATE,
        workItem.sourceChannels or LIVE_CHANNELS,
        workItem.sourceBitDepth,
        workItem.sourceSubtype,
    )
    if recordID is None:
        return

    persistSpectrogram(connection, recordID, logMelSpectrogram)
    deviceLocation = getDeviceLocation(connection.cursor(), workItem.deviceID)
    hasLocation = (
        deviceLocation is not None
        and deviceLocation["latitude"] is not None
        and deviceLocation["longitude"] is not None
    )
    validationStatus = VALID if hasLocation else MISSING_DEVICE_LOCATION
    payload: dict[str, object] = {
        "database_record_id": recordID,
        "source_id": workItem.sourceID,
        "source_path": workItem.sourcePath,
        "window_index": windowIndex,
        "device_id": workItem.deviceID,
        "location_id": deviceLocation["location_id"] if deviceLocation is not None else None,
        "latitude": deviceLocation["latitude"] if deviceLocation is not None else None,
        "longitude": deviceLocation["longitude"] if deviceLocation is not None else None,
        "altitude": deviceLocation["altitude"] if deviceLocation is not None else None,
        "window_timestamp": (
            workItem.originTimestamp + timedelta(seconds=startSample / TARGET_SAMPLE_RATE)
        ).astimezone(timezone.utc).isoformat(timespec="microseconds"),
        "start_sample": startSample,
        "sample_rate": TARGET_SAMPLE_RATE,
        "source_sample_rate": workItem.sourceSampleRate or TARGET_SAMPLE_RATE,
        "source_channels": workItem.sourceChannels or LIVE_CHANNELS,
        "source_bit_depth": workItem.sourceBitDepth,
        "source_subtype": workItem.sourceSubtype,
        "validation_status": validationStatus,
        "log_mel_spectrogram": logMelSpectrogram,
    }
    enqueueInferencePayload(inferenceQueue, payload, connection)
    updateQueueStatus(connection, recordID, QUEUED_FOR_INFERENCE)


async def processFileWork(
    workItem: AudioWorkItem,
    connection: sqlite3.Connection,
    inferenceQueue: asyncio.Queue[dict[str, object]],
) -> None:
    if workItem.sourcePath is None:
        raise ValueError("A file work item must have a source path.")

    samples, sourceSampleRate, sourceChannels, sourceBitDepth, sourceSubtype = await asyncio.to_thread(
        readAudioFile,
        workItem.sourcePath,
    )
    workItem.sourceSampleRate = sourceSampleRate
    workItem.sourceChannels = sourceChannels
    workItem.sourceBitDepth = sourceBitDepth
    workItem.sourceSubtype = sourceSubtype
    if len(samples) < SEGMENT_SAMPLES:
        print(f"Audio file is shorter than one segment; skipping: {workItem.sourcePath}")
        return

    windowIndex: int = 0
    for startSample in range(0, len(samples) - SEGMENT_SAMPLES + 1, STRIDE_SAMPLES):
        audioWindow = samples[startSample:startSample + SEGMENT_SAMPLES]
        await persistAudioWindow(
            audioWindow,
            workItem,
            windowIndex,
            startSample,
            connection,
            inferenceQueue,
        )
        windowIndex += 1


async def processLiveChunk(
    workItem: AudioWorkItem,
    liveBuffers: dict[str, LiveBufferState],
    connection: sqlite3.Connection,
    inferenceQueue: asyncio.Queue[dict[str, object]],
) -> None:
    if workItem.samples is None or workItem.sampleRate is None:
        raise ValueError("A live audio work item must contain samples and its source sample rate.")

    monoSamples = downmixToMono(workItem.samples)
    targetSamples = resampleAudio(monoSamples, workItem.sampleRate, TARGET_SAMPLE_RATE)
    if workItem.sourceID not in liveBuffers:
        liveBuffers[workItem.sourceID] = LiveBufferState(workItem, np.empty(0, dtype=np.float32))

    liveState = liveBuffers[workItem.sourceID]
    if workItem.discontinuity:
        liveState.workItem = workItem
        liveState.samples = np.empty(0, dtype=np.float32)
        liveState.startSample = 0
    pendingSamples = np.concatenate((liveState.samples, targetSamples))

    while len(pendingSamples) >= SEGMENT_SAMPLES:
        startSample = liveState.startSample
        windowIndex = liveState.windowIndex
        await persistAudioWindow(
            pendingSamples[:SEGMENT_SAMPLES],
            liveState.workItem,
            windowIndex,
            startSample,
            connection,
            inferenceQueue,
        )
        pendingSamples = pendingSamples[STRIDE_SAMPLES:]
        liveState.startSample = startSample + STRIDE_SAMPLES
        liveState.windowIndex = windowIndex + 1

    liveState.samples = pendingSamples


async def audioWorker(
    workQueue: asyncio.Queue[AudioWorkItem],
    connection: sqlite3.Connection,
    inferenceQueue: asyncio.Queue[dict[str, object]],
) -> None:
    liveBuffers: dict[str, LiveBufferState] = {}
    while True:
        workItem = await workQueue.get()
        try:
            if workItem.workType == "file":
                await processFileWork(workItem, connection, inferenceQueue)
            elif workItem.workType == "live":
                await processLiveChunk(workItem, liveBuffers, connection, inferenceQueue)
            else:
                raise ValueError(f"Unknown acoustic work type: {workItem.workType}")
        except (OSError, RuntimeError, ValueError, sqlite3.Error) as error:
            print(f"Error processing acoustic input {workItem.sourcePath or workItem.sourceID}: {error}")
        finally:
            workQueue.task_done()


async def fileIsStable(filePath: str, delaySeconds: float = 0.5) -> bool:
    try:
        initialStat = await asyncio.to_thread(os.stat, filePath)
    except FileNotFoundError:
        return False

    await asyncio.sleep(delaySeconds)
    try:
        finalStat = await asyncio.to_thread(os.stat, filePath)
    except FileNotFoundError:
        return False

    return initialStat.st_size > 0 and initialStat.st_size == finalStat.st_size and initialStat.st_mtime_ns == finalStat.st_mtime_ns


def getDeviceID(filePath: str) -> str:
    relativePath = os.path.relpath(filePath, INPUT_DIRECTORY)
    pathParts = Path(relativePath).parts
    if len(pathParts) > 1:
        return pathParts[0]
    return DEFAULT_DEVICE_ID


async def pollDirectory(workQueue: asyncio.Queue[AudioWorkItem]) -> None:
    ensureDirectory(INPUT_DIRECTORY)
    seenFiles: set[str] = set()

    while True:
        for root, _, fileNames in os.walk(INPUT_DIRECTORY):
            for fileName in fileNames:
                filePath = os.path.join(root, fileName)
                if os.path.splitext(fileName)[1].lower() not in AUDIO_EXTENSIONS or filePath in seenFiles:
                    continue
                if not await fileIsStable(filePath):
                    continue

                deviceID = getDeviceID(filePath)
                sourceID = f"{deviceID}:{await asyncio.to_thread(getFileHash, filePath)}"
                # Audio tags are inconsistent; use the stable file modification time as a UTC capture-time proxy.
                originTimestamp = datetime.fromtimestamp(
                    (await asyncio.to_thread(os.path.getmtime, filePath)),
                    tz=timezone.utc,
                )
                await workQueue.put(AudioWorkItem(
                    workType="file",
                    sourceID=sourceID,
                    deviceID=deviceID,
                    originTimestamp=originTimestamp,
                    sourcePath=filePath,
                ))
                seenFiles.add(filePath)

        await asyncio.sleep(POLLING_INTERVAL_SEC)


async def captureLiveAudio(workQueue: asyncio.Queue[AudioWorkItem]) -> None:
    rawAudioQueue: queue.Queue[tuple[np.ndarray, datetime, bool]] = queue.Queue(maxsize=LIVE_BUFFER_CHUNKS)
    streamID: str = f"live:{LIVE_DEVICE_ID}:{uuid.uuid4().hex}"
    droppedChunks: int = 0

    def audioCallback(indata: np.ndarray, _frames: int, _timeInfo: object, status: sd.CallbackFlags) -> None:
        nonlocal droppedChunks
        del _frames, _timeInfo
        if status:
            print(f"Live audio input warning: {status}")

        audioChunk = np.array(indata, dtype=np.float32, copy=True)
        captureTimestamp = datetime.now(timezone.utc)
        discontinuity = False
        try:
            rawAudioQueue.put_nowait((audioChunk, captureTimestamp, discontinuity))
        except queue.Full:
            try:
                while True:
                    rawAudioQueue.get_nowait()
                    droppedChunks += 1
            except queue.Empty:
                pass
            discontinuity = True
            rawAudioQueue.put_nowait((audioChunk, captureTimestamp, discontinuity))

    stream = sd.InputStream(
        samplerate=LIVE_INPUT_SAMPLE_RATE,
        channels=LIVE_CHANNELS,
        dtype="float32",
        callback=audioCallback,
    )
    try:
        stream.start()
        print(f"Capturing live audio for device {LIVE_DEVICE_ID} at {LIVE_INPUT_SAMPLE_RATE} Hz.")
        while True:
            try:
                audioChunk, captureTimestamp, discontinuity = rawAudioQueue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.01)
                continue

            if droppedChunks:
                print(f"Live audio ring buffer dropped {droppedChunks} old chunk(s).")
                droppedChunks = 0

            await workQueue.put(AudioWorkItem(
                workType="live",
                sourceID=streamID,
                deviceID=LIVE_DEVICE_ID,
                originTimestamp=captureTimestamp,
                samples=audioChunk,
                sampleRate=LIVE_INPUT_SAMPLE_RATE,
                discontinuity=discontinuity,
                sourceSampleRate=LIVE_INPUT_SAMPLE_RATE,
                sourceChannels=LIVE_CHANNELS,
                sourceBitDepth=32,
                sourceSubtype="FLOAT",
            ))
    finally:
        stream.stop()
        stream.close()


async def main(
    inferenceQueue: Optional[asyncio.Queue[dict[str, object]]] = None,
) -> None:
    print(f"Starting acoustic ingestion; watching {INPUT_DIRECTORY}...")
    ensureDirectory(INPUT_DIRECTORY)
    connection = initializeDatabase()
    audioWorkQueue: asyncio.Queue[AudioWorkItem] = asyncio.Queue(maxsize=WORK_QUEUE_SIZE)
    outputQueue = inferenceQueue if inferenceQueue is not None else ACOUSTIC_INFERENCE_QUEUE

    workerTask = asyncio.create_task(audioWorker(audioWorkQueue, connection, outputQueue))
    inputTasks = [asyncio.create_task(pollDirectory(audioWorkQueue))]
    if LIVE_ENABLED:
        inputTasks.append(asyncio.create_task(captureLiveAudio(audioWorkQueue)))

    try:
        await asyncio.gather(*inputTasks)
    finally:
        for task in inputTasks:
            task.cancel()
        await asyncio.gather(*inputTasks, return_exceptions=True)
        workerTask.cancel()
        await asyncio.gather(workerTask, return_exceptions=True)
        connection.close()


if __name__ == "__main__":
    asyncio.run(main())
