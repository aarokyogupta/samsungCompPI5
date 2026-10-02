import aiomqtt
import asyncio
from datetime import datetime, timedelta, timezone
import json
import math
import os
import sqlite3
from typing import Optional
import yaml

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)
    
# Define MQTT broker config
BROKER_HOST: str = config["mqtt"]["brokerHost"]
PORT: int = config["mqtt"]["port"]
TOPIC: str = config["mqtt"]["topics"]["telemetry"]

# Define SQLite database file location
DB_FILE: str = os.path.expanduser(config["storage"]["databasePath"])

# Timestamp format used for storing & parsing telemetry timestamps
TIMESTAMP_FORMAT: str = "%Y %m %d %H %M %S %f"

# Speed calculation constants
SEMI_MAJOR_AXIS: float = 6_378_137.0
SEMI_MINOR_AXIS: float = 6_356_752.3142
SEMI_MAJOR_AXIS_SQUARED: float = 40_680_631_590_769.0
SEMI_MINOR_AXIS_SQUARED: float = 40_408_299_984_087.055_521_64

VALID_FIX: str = "VALID_FIX"
EXCEEDED_SPEED_THRESHOLD: str = "EXCEEDED_SPEED_THRESHOLD"
FUTURE_TIMESTAMP_ERROR: str = "FUTURE_TIMESTAMP_ERROR"
INVALID_COORDINATE_BOUNDS: str = "INVALID_COORDINATE_BOUNDS"
SPATIALITE_EXT: str = config["storage"]["spatialiteExtension"]
BATTERY_LOW_THRESHOLD: float = float(config["validation"]["batteryLowThreshold"])
VALID_HARDWARE_STATES: set[str] = {"BATTERY_OK", "BATTERY_LOW"}
VALID_FIX_QUALITIES: set[str] = {"3D_FIX", "2D_FIX", "ARGOS_LOCATION_CLASS", "UNKNOWN"}

TELEMETRY_DATA_COLUMNS: dict[str, str] = {
    "body_temperature": "REAL",
    "heart_rate": "REAL",
    "target_species": "TEXT",
    "device_model": "TEXT",
    "device_make": "TEXT",
    "battery_percentage": "REAL",
    "validation_status": f"TEXT NOT NULL DEFAULT '{VALID_FIX}'",
    "fix_quality": "TEXT",
    "hardware_state": "TEXT",
}

def optionalFloat(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    
    return float(value)

def getField(payloadData: dict, fields: dict, fieldName: str, default: object = None) -> object:
    if fieldName in fields:
        return fields[fieldName]
    
    return payloadData.get(fieldName, default)

def getHardwareState(payloadData: dict, fields: dict, batteryPercentage: float) -> str:
    hardwareState = str(getField(payloadData, fields, "hardware_state", "")).strip().upper()
    
    if not hardwareState:
        return "BATTERY_LOW" if batteryPercentage < BATTERY_LOW_THRESHOLD else "BATTERY_OK"
    
    if hardwareState not in VALID_HARDWARE_STATES:
        raise ValueError(f"Hardware state ({hardwareState}) must be one of {sorted(VALID_HARDWARE_STATES)}.")
    
    return hardwareState

def getFixQuality(payloadData: dict, fields: dict) -> str:
    fixQuality = str(getField(payloadData, fields, "fix_quality", getField(
        payloadData, fields, "gps_fix_quality", "UNKNOWN"
    ))).strip().upper()
    if fixQuality not in VALID_FIX_QUALITIES:
        raise ValueError(f"Fix quality ({fixQuality}) must be one of {sorted(VALID_FIX_QUALITIES)}.")
    return fixQuality

def ensureTelemetryDataColumns() -> None:
    cursor.execute("PRAGMA table_info(telemetry_data);")
    existingColumns = {row[1] for row in cursor.fetchall()}
    
    for columnName, columnType in TELEMETRY_DATA_COLUMNS.items():
        if columnName not in existingColumns:
            cursor.execute(f"ALTER TABLE telemetry_data ADD COLUMN {columnName} {columnType};")

def telemetryDataTableExists() -> bool:
    cursor.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
            AND name = 'telemetry_data';
    """)
    return cursor.fetchone() is not None

# Asyncly extracts fields from JSON payload
async def processPayload(payloadBytes: bytes) -> None:
    try:
        # Decode bytes into dict
        payloadData: dict = json.loads(payloadBytes.decode("utf-8"))
        
        # Extract deviceID & fields from dict
        deviceID: str = payloadData.get("device_id", "unknown")
        maxSpeed: float = float(payloadData.get("max_speed", 400)) # (kmph)
        fields: dict = payloadData.get("fields", {})
        
        # Extract each field from fields
        animalID: str = fields.get("animal_id", "unknown")
        targetSpecies: str = str(getField(payloadData, fields, "target_species", "unknown"))
        deviceModel: str = str(getField(payloadData, fields, "device_model", "unknown"))
        deviceMake: str = str(getField(payloadData, fields, "device_make", "unknown"))
        timestamp: datetime = datetime.strptime(fields.get("timestamp", "1970 01 01 00 00 00 000000"), 
            TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)
        latitude: float = float(fields.get("latitude", 0.0)) # (degrees)
        longitude: float = float(fields.get("longitude", 0.0)) # (degrees)
        altitude: float = float(fields.get("altitude", 0.0)) # (meters)
        temperature: float = float(fields.get("temperature", 0.0)) # (celsius)
        bodyTemperature: Optional[float] = optionalFloat(fields.get("body_temperature")) # (celsius)
        heartRate: Optional[float] = optionalFloat(fields.get("heart_rate")) # (bpm)
        batteryPercentage: float = float(fields.get("battery_percentage", 100.0))
        fixQuality: str = getFixQuality(payloadData, fields)
        hardwareState: str = getHardwareState(payloadData, fields, batteryPercentage)
        validationStatus: str = VALID_FIX
        
        # Validate fields against appropriate bounds
        if timestamp > datetime.now(timezone.utc) + timedelta(days=1):
            validationStatus = FUTURE_TIMESTAMP_ERROR
        
        validCoordinates: bool = -90 <= latitude <= 90 and -180 <= longitude <= 180
        if not validCoordinates:
            validationStatus = INVALID_COORDINATE_BOUNDS
        
        if altitude < -11000:
            raise ValueError(f"Altitude ({altitude} meters) has to be greater than -11,000 meters.")
        
        if not( -100 <= temperature <= 100):
            raise ValueError(f"Temperature ({temperature}°C) has to be between -100°C & 100°C.")
        
        if bodyTemperature is not None and not( -100 <= bodyTemperature <= 100):
            raise ValueError(f"Body temperature ({bodyTemperature}°C) has to be between -100°C & 100°C.")
        
        if heartRate is not None and not(0 <= heartRate <= 1000):
            raise ValueError(f"Heart rate ({heartRate} bpm) has to be between 0 & 1000 bpm.")
        
        if not(0 <= batteryPercentage <= 100):
            raise ValueError(f"Battery percentage ({batteryPercentage}%) has to be between 0% & 100%.")
        
        if validCoordinates and validationStatus == VALID_FIX:
            cursor.execute("""
                SELECT 
                    ST_X(geom) AS longitude,
                    ST_Y(geom) AS latitude,
                    ST_Z(geom) AS altitude,
                    timestamp AS timestamp
                FROM telemetry_data
                WHERE animal_id = ?
                    AND validation_status = ?
                    AND geom IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 1;
            """, (animalID, VALID_FIX))
            
            row = cursor.fetchone()
            
            if row:
                previousLongitude, previousLatitude, previousAltitude, previousTimestamp = row
                deltaTime: timedelta = timestamp - datetime.strptime(
                    previousTimestamp, TIMESTAMP_FORMAT
                ).replace(tzinfo=timezone.utc)
                deltaSeconds: float = deltaTime.total_seconds()
                
                if deltaSeconds > 0:
                    previousPrimeVerticalRadiusOfCurvature: float = SEMI_MAJOR_AXIS_SQUARED / math.sqrt((SEMI_MAJOR_AXIS * 
                        math.cos(math.radians(previousLatitude)))**2 + 
                        (SEMI_MINOR_AXIS * math.sin(math.radians(previousLatitude)))**2)
                    
                    primeVerticalRadiusOfCurvature: float = SEMI_MAJOR_AXIS_SQUARED / math.sqrt((SEMI_MAJOR_AXIS \
                        * math.cos(math.radians(latitude)))**2 + (SEMI_MINOR_AXIS * math.sin(math.radians(latitude)))**2)
                    
                    previousPointX: float = (previousPrimeVerticalRadiusOfCurvature + previousAltitude) \
                        * math.cos(math.radians(previousLatitude)) * math.cos(math.radians(previousLongitude))
                        
                    previousPointY: float = (previousPrimeVerticalRadiusOfCurvature + previousAltitude) \
                        * math.cos(math.radians(previousLatitude)) * math.sin(math.radians(previousLongitude))
                        
                    previousPointZ: float = (SEMI_MINOR_AXIS_SQUARED * previousPrimeVerticalRadiusOfCurvature / 
                        SEMI_MAJOR_AXIS_SQUARED + previousAltitude) * math.sin(math.radians(previousLatitude))
                    
                    pointX: float = (primeVerticalRadiusOfCurvature + altitude) * math.cos(math.radians(latitude)) \
                        * math.cos(math.radians(longitude))
                        
                    pointY: float = (primeVerticalRadiusOfCurvature + altitude) * math.cos(math.radians(latitude)) \
                        * math.sin(math.radians(longitude))
                        
                    pointZ: float = (SEMI_MINOR_AXIS_SQUARED * primeVerticalRadiusOfCurvature / SEMI_MAJOR_AXIS_SQUARED + 
                        altitude) * math.sin(math.radians(latitude))
                    
                    speed: float = 3.6 * math.sqrt((previousPointX - pointX)**2 + (previousPointY - pointY)**2 + 
                        (previousPointZ - pointZ)**2) / deltaSeconds
                    
                    if speed > maxSpeed:
                        validationStatus = EXCEEDED_SPEED_THRESHOLD
        
        insertValues = (
            deviceID,
            animalID,
            targetSpecies,
            deviceMake,
            deviceModel,
            timestamp.strftime(TIMESTAMP_FORMAT),
            temperature,
            bodyTemperature,
            heartRate,
            batteryPercentage,
            validationStatus,
            fixQuality,
            hardwareState,
        )
        
        if validCoordinates:
            cursor.execute("""
                INSERT INTO telemetry_data (
                    device_id, animal_id, target_species, device_make, device_model, timestamp, temperature,
                    body_temperature, heart_rate, battery_percentage, validation_status, fix_quality, hardware_state, geom
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, MakePointZ(?, ?, ?, 4326));
            """, (*insertValues, longitude, latitude, altitude))
        else:
            cursor.execute("""
                INSERT INTO telemetry_data (
                    device_id, animal_id, target_species, device_make, device_model, timestamp, temperature,
                    body_temperature, heart_rate, battery_percentage, validation_status, fix_quality, hardware_state, geom
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL);
            """, insertValues)
        
        conn.commit()
        if validationStatus != VALID_FIX:
            print(f"Stored telemetry with validation status: {validationStatus}")
    except json.JSONDecodeError:
        print(f"Failed to decode JSON payload: {payloadBytes}")
    except Exception as e:
        print(f"Error processing message: {e}")

# Sequentially processes queued payloads
async def payloadWorker(queue: "asyncio.Queue[bytes]") -> None:
    while True:
        payloadBytes = await queue.get()
        try:
            await processPayload(payloadBytes)
        finally:
            queue.task_done()

# Main loop
async def main() -> None:
    print(f"Connecting to MQTT broker at {BROKER_HOST}...")
    queue: "asyncio.Queue[bytes]" = asyncio.Queue()
    worker = asyncio.create_task(payloadWorker(queue))
    
    # Establish async context manager connection
    async with aiomqtt.Client(hostname=BROKER_HOST, port=PORT) as client:
        print(f"Subscribed to topic: {TOPIC}")
        await client.subscribe(TOPIC)
        
        # Asynchronously iterate over incoming messages
        async for message in client.messages:
            # Enqueue payload so the next message can be detected instantly
            await queue.put(message.payload)
    
    worker.cancel()

if __name__ == "__main__":
    databaseExists = os.path.exists(DB_FILE)
    conn = sqlite3.connect(DB_FILE, timeout=30.0)
    cursor = conn.cursor()
    
    try:
        conn.enable_load_extension(True)
        cursor.execute("SELECT load_extension(?);", (SPATIALITE_EXT,))
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        
        if not databaseExists or not telemetryDataTableExists():
            cursor.execute("SELECT InitSpatialMetadata(1);")
            
            cursor.execute("""
                CREATE TABLE telemetry_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT,
                    animal_id TEXT,
                    target_species TEXT,
                    device_make TEXT,
                    device_model TEXT,
                    timestamp TEXT,
                    temperature REAL,
                    body_temperature REAL,
                    heart_rate REAL,
                    battery_percentage REAL,
                    validation_status TEXT NOT NULL DEFAULT 'VALID_FIX',
                    fix_quality TEXT,
                    hardware_state TEXT
                );
            """)
            
            cursor.execute("SELECT AddGeometryColumn('telemetry_data', 'geom', 4326, 'POINT', 'XYZ');")
            cursor.execute("SELECT CreateSpatialIndex('telemetry_data', 'geom');")
            conn.commit()
            print("Database initialized successfully.")
        else:
            ensureTelemetryDataColumns()
            conn.commit()
        
        asyncio.run(main())
    except sqlite3.OperationalError as e:
        print(f"\nDatabase Error: {e}")
    except KeyboardInterrupt:
        print("\nDisconnected gracefully.")
    finally:
        conn.close()