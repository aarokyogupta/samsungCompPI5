import aiomqtt
import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import os
import sqlite3
from statistics import median
import yaml

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

BROKER_HOST: str = config["mqtt"]["brokerHost"]
PORT: int = config["mqtt"]["port"]
TOPIC: str = config["mqtt"]["topics"]["environmental"]

DB_FILE = os.path.expanduser(config["storage"]["databasePath"])
SPATIALITE_EXT: str = config["storage"]["spatialiteExtension"]

# Timestamp format used for storing & parsing telemetry timestamps
TIMESTAMP_FORMAT: str = "%Y %m %d %H %M %S %f"

VALID_FIX: str = "VALID_FIX"
FUTURE_TIMESTAMP_ERROR: str = "FUTURE_TIMESTAMP_ERROR"
INVALID_COORDINATE_BOUNDS: str = "INVALID_COORDINATE_BOUNDS"
BATTERY_LOW_THRESHOLD: float = float(config["validation"]["batteryLowThreshold"])

VALID_HARDWARE_STATES: set[str] = {"BATTERY_OK", "BATTERY_LOW"}
VALID_FIX_QUALITIES: set[str] = {"3D_FIX", "2D_FIX", "ARGOS_LOCATION_CLASS", "UNKNOWN"}

METRICS_LIST: set[str] = {"water_turbidity", "water_velocity", "water_ph", "water_salinity", 
    "water_oxygen_concentration", "water_temperature", "temperature", "humidity", "air_quality_level", 
    "pollutant_concentration_level"}

HAMPEL_FILTER_STATE_STORE: dict[str, deque] = {metric: deque(maxlen=11) for metric in METRICS_LIST}

EMA_SMOOTHING_STATE_STORE: dict[str, float | None] = {metric: None for metric in METRICS_LIST}

def toCamelCase(fieldName: str) -> str:
    # Config keys are camelCase while sensor payload fields and DB columns stay snake_case
    parts = fieldName.split("_")
    return parts[0] + "".join(part[:1].upper() + part[1:] for part in parts[1:])

EMA_SMOOTHING_ALPHA_STATE_STORE: dict[str, float] = {metric: config["emaAlphas"][toCamelCase(metric)] 
    for metric in METRICS_LIST}

WELFORD_STATE_STORE: dict[tuple[str, int, str], WelfordState] = {}

ENVIRONMENTAL_DATA_COLUMNS: dict[str, str] = {
    "target_species": "TEXT",
    "device_model": "TEXT",
    "device_make": "TEXT",
    "air_quality_level": "REAL",
    "pollutant_concentration_level": "REAL",
    "rainfall": "REAL",
    "humidity": "REAL",
    "water_velocity": "REAL",
    "water_temperature": "REAL",
    "water_ph": "REAL",
    "water_salinity": "REAL",
    "water_oxygen_concentration": "REAL",
    "water_turbidity": "REAL",
    "battery_percentage": "REAL",
    "validation_status": f"TEXT NOT NULL DEFAULT '{VALID_FIX}'",
    "fix_quality": "TEXT",
    "hardware_state": "TEXT",
    "environmental_stress_flag": "INTEGER DEFAULT 0"
}

@dataclass
class WelfordState:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, x: float) -> None:
        self.count += 1
        
        delta1: float = x - self.mean
        self.mean += delta1 / self.count
        
        delta2: float = x - self.mean
        self.m2 += delta1 * delta2

    @property
    def sampleStandardDeviation(self) -> float:
        if self.count < 2:
            return 0.0
    
        return math.sqrt(self.m2 / (self.count - 1))

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
    fixQuality = str(getField(payloadData, fields, "fix_quality", getField(payloadData, fields, 
        "gps_fix_quality", "UNKNOWN"))).strip().upper()
    
    if fixQuality not in VALID_FIX_QUALITIES:
        raise ValueError(f"Fix quality ({fixQuality}) must be one of {sorted(VALID_FIX_QUALITIES)}.")
    
    return fixQuality

def ensureEnvironmentalDataColumns(cursor: sqlite3.Cursor) -> None:
    cursor.execute("PRAGMA table_info(environmental_data);")
    existingColumns = {row[1] for row in cursor.fetchall()}
    
    for columnName, columnType in ENVIRONMENTAL_DATA_COLUMNS.items():
        if columnName not in existingColumns:
            cursor.execute(f"ALTER TABLE environmental_data ADD COLUMN {columnName} {columnType};")

def environmentalDataTableExists(cursor: sqlite3.Cursor) -> bool:
    cursor.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
            AND name = 'environmental_data';
    """)
    
    return cursor.fetchone() is not None

def hampelFilter(stateStore: deque, newValue: float) -> float:
    stateStore.append(newValue)
    
    if len(stateStore) != stateStore.maxlen:
        return newValue
    
    rollingMedian: float = median(stateStore)
    medianAbsoluteDeviation: float = 1.4826 * median([abs(value - rollingMedian) for value in stateStore])
    
    if medianAbsoluteDeviation == 0:
        return rollingMedian
    
    if abs(newValue - rollingMedian) > 3 * medianAbsoluteDeviation:
        return rollingMedian
    
    return newValue

def emaSmoothing(previousSmoothed: float | None, newValue: float, alpha: float) -> float:
    if previousSmoothed is None:
        return newValue
    
    return alpha * newValue + (1 - alpha) * previousSmoothed

def computeZScore(smoothedValue: float, baselineMean: float, baselineStdDev: float) -> float:
    if baselineStdDev == 0.0:
        return 0.0
    
    return (smoothedValue - baselineMean) / baselineStdDev
        
# Asyncly extracts fields from JSON payload
async def processPayload(payloadBytes: bytes) -> None:
    try:
        databaseExists: bool = os.path.exists(DB_FILE)
        conn: sqlite3.Connection = sqlite3.connect(DB_FILE, timeout=30.0)
        cursor: sqlite3.Cursor = conn.cursor()
        
        conn.enable_load_extension(True)
        cursor.execute("SELECT load_extension(?);", (SPATIALITE_EXT,))
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        
        if not databaseExists or not environmentalDataTableExists(cursor):
            cursor.execute("SELECT InitSpatialMetadata(1);")
            
            cursor.execute("""
                CREATE TABLE environmental_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT,
                    location_id TEXT,
                    target_species TEXT,
                    device_make TEXT,
                    device_model TEXT,
                    timestamp TEXT,
                    temperature REAL,
                    temperature_filtered REAL,
                    air_quality_level REAL,
                    air_quality_level_filtered REAL, 
                    pollutant_concentration_level REAL,
                    pollutant_concentration_level_filtered REAL, 
                    rainfall REAL,
                    humidity REAL,
                    humidity_filtered REAL, 
                    water_velocity REAL,
                    water_velocity_filtered REAL,
                    water_temperature REAL,
                    water_temperature_filtered REAL,
                    water_ph REAL,
                    water_ph_filtered REAL,
                    water_salinity REAL,
                    water_salinity_filtered REAL, 
                    water_oxygen_concentration REAL,
                    water_oxygen_concentration_filtered REAL,
                    water_turbidity REAL,
                    water_turbidity_filtered REAL,
                    battery_percentage REAL,
                    validation_status TEXT NOT NULL DEFAULT 'VALID_FIX',
                    fix_quality TEXT,
                    hardware_state TEXT,
                    environmental_stress_flag INTEGER DEFAULT 0
                );
            """)
            
            cursor.execute("SELECT AddGeometryColumn('environmental_data', 'geom', 4326, 'POINT', 'XYZ');")
            cursor.execute("SELECT CreateSpatialIndex('environmental_data', 'geom');")
            conn.commit()
            print("Database initialized successfully.")
        else:
            ensureEnvironmentalDataColumns(cursor)
            conn.commit()
        
        # Decode bytes into dict
        payloadData: dict = json.loads(payloadBytes.decode("utf-8"))
        
        # Extract deviceID & fields from dict
        deviceID: str = payloadData.get("device_id", "unknown")
        fields: dict = payloadData.get("fields", {})
        
        # Extract each field from fields
        locationID: str = fields.get("location_id", "unknown")
        targetSpecies: str = str(getField(payloadData, fields, "target_species", "unknown"))
        deviceModel: str = str(getField(payloadData, fields, "device_model", "unknown"))
        deviceMake: str = str(getField(payloadData, fields, "device_make", "unknown"))
        timestamp: datetime = datetime.strptime(fields.get("timestamp", "1970 01 01 00 00 00 000000"), 
            TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)
        latitude: float = float(fields.get("latitude", 0.0)) # (degrees)
        longitude: float = float(fields.get("longitude", 0.0)) # (degrees)
        altitude: float = float(fields.get("altitude", 0.0)) # (meters)
        temperature: float = float(fields.get("temperature", 0.0)) # (celsius)
        airQualityLevel: float = float(fields.get("air_quality_level", 0.0)) # (1-10) - 1 IS GOOD
        pollutantConcentrationLevel: float = float(fields.get("pollutant_concentration_level", 0.0)) # (ppm)
        rainfall: float = float(fields.get("rainfall", 0.0)) # (mm)
        humidity: float = float(fields.get("humidity", 0.0)) # (%)  
        waterVelocity: float = float(fields.get("water_velocity", 0.0)) # (m/s)
        waterTemperature: float = float(fields.get("water_temperature", 0.0)) # (celsius)  
        waterPH: float = float(fields.get("water_ph", 7.0)) # (PH scale)   
        waterSalinity: float = float(fields.get("water_salinity", 0.0)) # (PSU)        
        waterOxygenConcentration: float = float(fields.get("water_oxygen_concentration", 0.0)) # (mg/L)     
        waterTurbidity: float = float(fields.get("water_turbidity", 0.0)) # (FNU)    
        batteryPercentage: float = float(fields.get("battery_percentage", 100.0))
        fixQuality: str = getFixQuality(payloadData, fields)
        hardwareState: str = getHardwareState(payloadData, fields, batteryPercentage)
        validationStatus: str = VALID_FIX
                
        waterTurbidityFiltered: float = hampelFilter(HAMPEL_FILTER_STATE_STORE["water_turbidity"], 
            waterTurbidity)
        waterVelocityFiltered: float = hampelFilter(HAMPEL_FILTER_STATE_STORE["water_velocity"], waterVelocity)
        waterPHFiltered: float = hampelFilter(HAMPEL_FILTER_STATE_STORE["water_ph"], waterPH)
        waterSalinityFiltered: float = hampelFilter(HAMPEL_FILTER_STATE_STORE["water_salinity"], waterSalinity)
        waterOxygenConcentrationFiltered: float = hampelFilter(HAMPEL_FILTER_STATE_STORE[
            "water_oxygen_concentration"], waterOxygenConcentration)
        waterTemperatureFiltered: float = hampelFilter(HAMPEL_FILTER_STATE_STORE["water_temperature"], 
            temperature)
        temperatureFiltered: float = hampelFilter(HAMPEL_FILTER_STATE_STORE["temperature"], 
            waterTemperature)
        humidityFiltered: float = hampelFilter(HAMPEL_FILTER_STATE_STORE["humidity"], humidity)
        airQualityLevelFiltered: float = hampelFilter(HAMPEL_FILTER_STATE_STORE["air_quality_level"],
            airQualityLevel)
        pollutantConcentrationLevelFiltered: float = hampelFilter(HAMPEL_FILTER_STATE_STORE[
            "pollutant_concentration_level"], pollutantConcentrationLevel)

        waterTurbidityFiltered = emaSmoothing(EMA_SMOOTHING_STATE_STORE["water_turbidity"], waterTurbidityFiltered, EMA_SMOOTHING_ALPHA_STATE_STORE["water_turbidity"])
        EMA_SMOOTHING_STATE_STORE["water_turbidity"] = waterTurbidityFiltered

        waterVelocityFiltered = emaSmoothing(EMA_SMOOTHING_STATE_STORE["water_velocity"], waterVelocityFiltered, EMA_SMOOTHING_ALPHA_STATE_STORE["water_velocity"])
        EMA_SMOOTHING_STATE_STORE["water_velocity"] = waterVelocityFiltered

        waterPHFiltered = emaSmoothing(EMA_SMOOTHING_STATE_STORE["water_ph"], waterPHFiltered, EMA_SMOOTHING_ALPHA_STATE_STORE["water_ph"])
        EMA_SMOOTHING_STATE_STORE["water_ph"] = waterPHFiltered

        waterSalinityFiltered = emaSmoothing(EMA_SMOOTHING_STATE_STORE["water_salinity"], waterSalinityFiltered, EMA_SMOOTHING_ALPHA_STATE_STORE["water_salinity"])
        EMA_SMOOTHING_STATE_STORE["water_salinity"] = waterSalinityFiltered

        waterOxygenConcentrationFiltered = emaSmoothing(EMA_SMOOTHING_STATE_STORE["water_oxygen_concentration"], waterOxygenConcentrationFiltered, EMA_SMOOTHING_ALPHA_STATE_STORE["water_oxygen_concentration"])
        EMA_SMOOTHING_STATE_STORE["water_oxygen_concentration"] = waterOxygenConcentrationFiltered

        waterTemperatureFiltered = emaSmoothing(EMA_SMOOTHING_STATE_STORE["water_temperature"], waterTemperatureFiltered, EMA_SMOOTHING_ALPHA_STATE_STORE["water_temperature"])
        EMA_SMOOTHING_STATE_STORE["water_temperature"] = waterTemperatureFiltered

        temperatureFiltered = emaSmoothing(EMA_SMOOTHING_STATE_STORE["temperature"], temperatureFiltered, EMA_SMOOTHING_ALPHA_STATE_STORE["temperature"])
        EMA_SMOOTHING_STATE_STORE["temperature"] = temperatureFiltered

        humidityFiltered = emaSmoothing(EMA_SMOOTHING_STATE_STORE["humidity"], humidityFiltered, EMA_SMOOTHING_ALPHA_STATE_STORE["humidity"])
        EMA_SMOOTHING_STATE_STORE["humidity"] = humidityFiltered

        airQualityLevelFiltered = emaSmoothing(EMA_SMOOTHING_STATE_STORE["air_quality_level"], airQualityLevelFiltered, EMA_SMOOTHING_ALPHA_STATE_STORE["air_quality_level"])
        EMA_SMOOTHING_STATE_STORE["air_quality_level"] = airQualityLevelFiltered

        pollutantConcentrationLevelFiltered = emaSmoothing(EMA_SMOOTHING_STATE_STORE["pollutant_concentration_level"], pollutantConcentrationLevelFiltered, EMA_SMOOTHING_ALPHA_STATE_STORE["pollutant_concentration_level"])
        EMA_SMOOTHING_STATE_STORE["pollutant_concentration_level"] = pollutantConcentrationLevelFiltered
        
        # Validate fields against appropriate bounds
        if timestamp > datetime.now(timezone.utc) + timedelta(days=1):
            validationStatus = FUTURE_TIMESTAMP_ERROR
        
        validCoordinates: bool = -90 <= latitude <= 90 and -180 <= longitude <= 180
        if not validCoordinates:
            validationStatus = INVALID_COORDINATE_BOUNDS
        
        if altitude < -11000:
            raise ValueError(f"Altitude ({altitude} meters) has to be greater than -11,000 meters.")
        
        if not( -100 <= temperatureFiltered <= 100):
            raise ValueError(f"Temperature ({temperatureFiltered}°C) has to be between -100°C & 100°C.")
        
        if not(1 <= airQualityLevelFiltered <= 10):
            raise ValueError(f"Air quality level ({airQualityLevelFiltered}) has to be between 1 & 10.")
        
        if not(0 <= pollutantConcentrationLevelFiltered <= 100000):
            raise ValueError(f"Pollutant concentration level ({pollutantConcentrationLevelFiltered} ppm) has to be between 0 & 100,000 ppm.")
        
        if not(0 <= rainfall <= 2000):
            raise ValueError(f"Rainfall ({rainfall} mm) has to be between 0 & 2,000 mm.")
        
        if not(0 <= humidityFiltered <= 100):
            raise ValueError(f"Humidity ({humidityFiltered}%) has to be between 0% & 100%.")
        
        if not(0 <= waterVelocityFiltered <= 50):
            raise ValueError(f"Water velocity ({waterVelocityFiltered} m/s) has to be between 0 & 50 m/s.")
        
        if not( -10 <= waterTemperatureFiltered <= 100):
            raise ValueError(f"Water temperature ({waterTemperatureFiltered}°C) has to be between -10°C & 100°C.")
        
        if not(0 <= waterPHFiltered <= 14):
            raise ValueError(f"Water pH ({waterPHFiltered}) has to be between 0 & 14.")
        
        if not(0 <= waterSalinityFiltered <= 50):
            raise ValueError(f"Water salinity ({waterSalinityFiltered} PSU) has to be between 0 & 50 PSU.")
        
        if not(0 <= waterOxygenConcentrationFiltered <= 20):
            raise ValueError(f"Water oxygen concentration ({waterOxygenConcentrationFiltered} mg/L) has to be between 0 & 20 mg/L.")
        
        if not(0 <= waterTurbidityFiltered <= 4000):
            raise ValueError(f"Water turbidity ({waterTurbidityFiltered} FNU) has to be between 0 & 4,000 FNU.")
        
        if not(0 <= batteryPercentage <= 100):
            raise ValueError(f"Battery percentage ({batteryPercentage}%) has to be between 0% & 100%.")
        
        currentMonth: int = timestamp.month
        triggeredMetrics: list[str] = []

        welfordValues: dict[str, float] = {
            "water_turbidity": waterTurbidityFiltered,
            "water_velocity": waterVelocityFiltered,
            "water_ph": waterPHFiltered,
            "water_salinity": waterSalinityFiltered,
            "water_oxygen_concentration": waterOxygenConcentrationFiltered,
            "water_temperature": waterTemperatureFiltered,
            "temperature": temperatureFiltered,
            "rainfall": rainfall,
            "humidity": humidityFiltered,
            "air_quality_level": airQualityLevelFiltered,
            "pollutant_concentration_level": pollutantConcentrationLevelFiltered,
        }

        for metricName, value in welfordValues.items():
            stateKey: tuple[str, int, str] = (locationID, currentMonth, metricName)
            
            if stateKey not in WELFORD_STATE_STORE:
                WELFORD_STATE_STORE[stateKey] = WelfordState()
            
            welford: WelfordState = WELFORD_STATE_STORE[stateKey]
            baselineMean: float = welford.mean
            baselineStdDev: float = welford.sampleStandardDeviation
            
            welford.update(value)
            
            zScore: float = computeZScore(value, baselineMean, baselineStdDev)
            
            if abs(zScore) > 2.5:
                triggeredMetrics.append(metricName)

        environmentalStressFlagInt: int = len(triggeredMetrics) > 0
        
        insertValues = (
            deviceID,
            locationID,
            targetSpecies,
            deviceMake,
            deviceModel,
            timestamp.strftime(TIMESTAMP_FORMAT),
            temperature,
            temperatureFiltered,
            airQualityLevel,
            airQualityLevelFiltered,
            pollutantConcentrationLevel,
            pollutantConcentrationLevelFiltered,
            rainfall,
            humidity,
            humidityFiltered,
            waterVelocity,
            waterVelocityFiltered,
            waterTemperature,
            waterTemperatureFiltered,
            waterPH,
            waterPHFiltered,
            waterSalinity,
            waterSalinityFiltered, 
            waterOxygenConcentration,
            waterOxygenConcentrationFiltered,
            waterTurbidity,
            waterTurbidityFiltered,
            batteryPercentage,
            validationStatus,
            fixQuality,
            hardwareState,
            environmentalStressFlagInt
        )
        
        if validCoordinates:
            cursor.execute("""
                INSERT INTO environmental_data (
                    device_id, location_id, target_species, device_make, device_model, timestamp, temperature, 
                    temperature_filtered, air_quality_level, air_quality_level_filtered, 
                    pollutant_concentration_level, pollutant_concentration_level_filtered, rainfall, humidity, 
                    humidity_filtered, water_velocity, water_velocity_filtered, water_temperature, 
                    water_temperature_filtered, water_ph, water_ph_filtered, water_salinity, 
                    water_salinity_filtered, water_oxygen_concentration, water_oxygen_concentration_filtered,
                    water_turbidity, water_turbidity_filtered, battery_percentage, validation_status, 
                    fix_quality, hardware_state, environmental_stress_flag, geom
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, MakePointZ(?, ?, ?, 4326));
            """, (*insertValues, longitude, latitude, altitude))
        else:
            cursor.execute("""
                INSERT INTO environmental_data (
                    device_id, location_id, target_species, device_make, device_model, timestamp, temperature, 
                    temperature_filtered, air_quality_level, air_quality_level_filtered, 
                    pollutant_concentration_level, pollutant_concentration_level_filtered, rainfall, humidity, 
                    humidity_filtered, water_velocity, water_velocity_filtered, water_temperature, 
                    water_temperature_filtered, water_ph, water_ph_filtered, water_salinity, 
                    water_salinity_filtered, water_oxygen_concentration, water_oxygen_concentration_filtered,
                    water_turbidity, water_turbidity_filtered, battery_percentage, validation_status, 
                    fix_quality, hardware_state, environmental_stress_flag, geom
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL);
            """, insertValues)
        
        conn.commit()
        
        if validationStatus != VALID_FIX:
            print(f"Stored telemetry with validation status: {validationStatus}")
    except sqlite3.OperationalError as e:
        print(f"\nDatabase Error: {e}")
    except json.JSONDecodeError:
        print(f"Failed to decode JSON payload: {payloadBytes}")
    except Exception as e:
        print(f"Error processing message: {e}")
    finally:
        conn.close()

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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDisconnected gracefully.")