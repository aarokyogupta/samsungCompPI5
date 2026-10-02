import aiosqlite
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import math
import os
import random
import re
import sqlite3
from typing import AsyncIterator, Awaitable, Callable, Optional, Sequence, TypeVar, Union
import yaml

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

DATABASE_CONFIG: dict = config.get("database", {})
DB_FILE: str = os.path.expanduser(config["storage"]["databasePath"])
SPATIALITE_EXT: str = config["storage"]["spatialiteExtension"]
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def resolveProjectPath(path: object) -> str:
    expandedPath = os.path.expanduser(str(path))
    if os.path.isabs(expandedPath):
        return expandedPath
    return os.path.join(PROJECT_ROOT, expandedPath)


# Define connection pool and write batching settings
READ_POOL_SIZE: int = int(DATABASE_CONFIG.get("readPoolSize", 3))
BUSY_TIMEOUT_MS: int = int(DATABASE_CONFIG.get("busyTimeoutMs", 5000))
WRITE_QUEUE_SIZE: int = int(DATABASE_CONFIG.get("writeQueueSize", 10000))
WRITE_BATCH_INTERVAL_SEC: float = float(DATABASE_CONFIG.get("writeBatchIntervalMs", 500)) / 1000.0
WRITE_BATCH_SIZE: int = int(DATABASE_CONFIG.get("writeBatchSize", 500))
LOCK_RETRY_INITIAL_DELAY_SEC: float = float(DATABASE_CONFIG.get("lockRetryInitialDelaySec", 0.05))
LOCK_RETRY_MAX_DELAY_SEC: float = float(DATABASE_CONFIG.get("lockRetryMaxDelaySec", 5.0))
LOCK_RETRY_ATTEMPTS: int = int(DATABASE_CONFIG.get("lockRetryAttempts", 10))

# Define baseline schema and migration settings
SCHEMA_FILE: str = resolveProjectPath(DATABASE_CONFIG.get("schemaFile", "database/schema.sql"))
MIGRATIONS_DIRECTORY: str = resolveProjectPath(
    DATABASE_CONFIG.get("migrationsDirectory", "database/schemaMigrations")
)
TARGET_SCHEMA_VERSION: Optional[int] = (
    int(DATABASE_CONFIG["targetSchemaVersion"])
    if DATABASE_CONFIG.get("targetSchemaVersion") is not None
    else None
)

# Define geospatial threat query settings
THREAT_RADIUS_METERS: float = float(DATABASE_CONFIG.get("threatRadiusMeters", 50000.0))
THREAT_LOOKBACK_DAYS: float = float(DATABASE_CONFIG.get("threatLookbackDays", 30.0))

if READ_POOL_SIZE <= 0 or WRITE_QUEUE_SIZE <= 0 or WRITE_BATCH_SIZE <= 0:
    raise ValueError("Database pool, write queue, and write batch sizes must be positive.")
if BUSY_TIMEOUT_MS < 0 or WRITE_BATCH_INTERVAL_SEC <= 0:
    raise ValueError("Database busy timeout must be non-negative and the batch interval positive.")
if LOCK_RETRY_INITIAL_DELAY_SEC <= 0 or LOCK_RETRY_MAX_DELAY_SEC < LOCK_RETRY_INITIAL_DELAY_SEC:
    raise ValueError("Database lock retry delays must be positive and ordered.")
if LOCK_RETRY_ATTEMPTS <= 0:
    raise ValueError("Database lockRetryAttempts must be positive.")
if TARGET_SCHEMA_VERSION is not None and TARGET_SCHEMA_VERSION < 0:
    raise ValueError("Database targetSchemaVersion must be non-negative.")
if THREAT_RADIUS_METERS <= 0 or THREAT_LOOKBACK_DAYS <= 0:
    raise ValueError("Threat radius and lookback period must be positive.")

# WGS84 radii used to build a search box that always contains the geodesic radius
WGS84_SEMI_MAJOR_AXIS: float = 6378137.0
WGS84_MINIMUM_MERIDIAN_RADIUS: float = 6335439.327
SEARCH_FRAME_MARGIN: float = 1.01

# Migration files are named like 0001acousticThreatGeometry.sql
MIGRATION_FILE_PATTERN = re.compile(r"^(\d+)([A-Za-z][A-Za-z0-9]*)\.sql$")
TRANSACTION_CONTROL_PATTERN = re.compile(
    r"^(BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE)\b", re.IGNORECASE
)
SPATIAL_MANAGEMENT_PATTERN = re.compile(
    r"\b(AddGeometryColumn|CreateSpatialIndex|RecoverGeometryColumn|"
    r"RecoverSpatialIndex|DiscardGeometryColumn|DisableSpatialIndex)\s*\(",
    re.IGNORECASE,
)
LOCKED_ERROR_MESSAGES: tuple[str, ...] = (
    "database is locked",
    "database table is locked",
    "database schema is locked",
    "database is busy",
)

SqlParameters = Union[Sequence[object], dict[str, object]]
ReturnType = TypeVar("ReturnType")


class MigrationError(RuntimeError):
    pass


class DatabaseClosedError(RuntimeError):
    pass


@dataclass(frozen=True)
class WriteResult:
    # executemany cannot report per-row counts, so batched rows use rowCount -1
    rowCount: int
    lastRowID: Optional[int]


@dataclass(eq=False)
class WriteRequest:
    statements: list[tuple[str, SqlParameters]]
    future: asyncio.Future
    isBatchable: bool
    isTransaction: bool
    results: list[WriteResult] = field(default_factory=list)
    error: Optional[BaseException] = None


def isDatabaseLocked(error: BaseException) -> bool:
    if not isinstance(error, sqlite3.OperationalError):
        return False
    message = str(error).casefold()
    return any(lockedMessage in message for lockedMessage in LOCKED_ERROR_MESSAGES)


def getRetryDelay(attempt: int) -> float:
    # Exponential backoff with jitter prevents several writers retrying in lockstep
    delay = min(LOCK_RETRY_MAX_DELAY_SEC, LOCK_RETRY_INITIAL_DELAY_SEC * (2 ** attempt))
    return delay * random.uniform(0.5, 1.0)


async def retryWhileLocked(
    operation: Callable[[], Awaitable[ReturnType]],
    description: str,
    maxAttempts: Optional[int] = LOCK_RETRY_ATTEMPTS,
) -> ReturnType:
    attempt = 0
    while True:
        try:
            return await operation()
        except sqlite3.OperationalError as error:
            if not isDatabaseLocked(error):
                raise
            attempt += 1
            if maxAttempts is not None and attempt >= maxAttempts:
                raise
            delay = getRetryDelay(attempt - 1)
            print(f"Database locked during {description}; retrying in {delay:.2f}s (attempt {attempt}).")
            await asyncio.sleep(delay)


def stripLeadingComments(statement: str) -> str:
    text = statement.lstrip()
    while True:
        if text.startswith("--"):
            newlineIndex = text.find("\n")
            text = "" if newlineIndex < 0 else text[newlineIndex + 1:].lstrip()
        elif text.startswith("/*"):
            commentEnd = text.find("*/")
            text = "" if commentEnd < 0 else text[commentEnd + 2:].lstrip()
        else:
            return text


def validateWriteStatement(sql: str) -> str:
    statement = stripLeadingComments(str(sql))
    if not statement:
        raise ValueError("Database write statements must not be empty.")
    if TRANSACTION_CONTROL_PATTERN.match(statement):
        raise ValueError("Transaction control is managed by the database write worker.")
    return str(sql)


def splitSqlStatements(script: str, fileName: str) -> list[str]:
    statements: list[str] = []
    currentStatement = ""
    # complete_statement keeps trigger bodies and quoted semicolons intact
    for line in script.splitlines(keepends=True):
        currentStatement += line
        if sqlite3.complete_statement(currentStatement):
            statement = currentStatement.strip()
            if stripLeadingComments(statement):
                statements.append(statement)
            currentStatement = ""

    if stripLeadingComments(currentStatement):
        raise MigrationError(f"Migration {fileName} ends with an incomplete SQL statement.")
    for statement in statements:
        if TRANSACTION_CONTROL_PATTERN.match(stripLeadingComments(statement)):
            raise MigrationError(
                f"Migration {fileName} must not contain transaction control; "
                "the migration engine wraps it in an exclusive transaction."
            )
    return statements


def loadMigrationFiles(directory: str = MIGRATIONS_DIRECTORY) -> list[dict[str, object]]:
    if not os.path.isdir(directory):
        raise MigrationError(f"Schema migration directory does not exist: {directory}.")

    migrations: list[dict[str, object]] = []
    versions: set[int] = set()
    for fileName in sorted(os.listdir(directory)):
        if not fileName.casefold().endswith(".sql"):
            continue
        match = MIGRATION_FILE_PATTERN.match(fileName)
        if match is None:
            raise MigrationError(
                f"Invalid migration file name {fileName}; use 0001descriptiveName.sql."
            )
        version = int(match.group(1))
        if version <= 0 or version in versions:
            raise MigrationError(f"Migration version {version} is invalid or duplicated.")
        versions.add(version)

        with open(os.path.join(directory, fileName), "r", encoding="utf-8") as migrationFile:
            # Normalizing line endings keeps checksums stable between Windows and the Pi
            script = migrationFile.read().replace("\r\n", "\n")
        migrations.append({
            "version": version,
            "name": match.group(2),
            "fileName": fileName,
            "checksum": hashlib.sha256(script.encode("utf-8")).hexdigest(),
            "statements": splitSqlStatements(script, fileName),
        })
    return sorted(migrations, key=lambda migration: int(migration["version"]))


async def runStatement(
    connection: aiosqlite.Connection,
    sql: str,
    parameters: SqlParameters = (),
) -> WriteResult:
    cursor = await connection.execute(sql, parameters)
    try:
        return WriteResult(rowCount=cursor.rowcount, lastRowID=cursor.lastrowid)
    finally:
        await cursor.close()


async def fetchOneValue(
    connection: aiosqlite.Connection,
    sql: str,
    parameters: SqlParameters = (),
) -> object:
    async with connection.execute(sql, parameters) as cursor:
        row = await cursor.fetchone()
    return None if row is None else row[0]


async def executeMigrationStatement(
    connection: aiosqlite.Connection,
    statement: str,
    fileName: str,
) -> None:
    async with connection.execute(statement) as cursor:
        rows = await cursor.fetchall()
    # SpatiaLite management functions report failure as 0 rather than raising
    if SPATIAL_MANAGEMENT_PATTERN.search(statement):
        if not rows or any(value in (0, None) for value in tuple(rows[0])):
            raise MigrationError(
                f"SpatiaLite rejected a geometry statement in migration {fileName}."
            )


async def applyMigrations(
    connection: aiosqlite.Connection,
    directory: str = MIGRATIONS_DIRECTORY,
    targetVersion: Optional[int] = TARGET_SCHEMA_VERSION,
) -> list[int]:
    migrations = loadMigrationFiles(directory)
    await runStatement(connection, """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            checksum TEXT NOT NULL,
            applied_at TEXT NOT NULL
        );
    """)
    async with connection.execute("SELECT version, checksum FROM schema_migrations;") as cursor:
        appliedChecksums = {int(row[0]): str(row[1]) for row in await cursor.fetchall()}

    availableMigrations = {int(migration["version"]): migration for migration in migrations}
    latestVersion = max(availableMigrations, default=0)
    resolvedTarget = latestVersion if targetVersion is None else targetVersion
    if resolvedTarget > latestVersion:
        raise MigrationError(
            f"Target schema version {resolvedTarget} has no migration file; latest is {latestVersion}."
        )

    # Compare the deployed database state with the software's target schema
    for version, checksum in sorted(appliedChecksums.items()):
        if version not in availableMigrations:
            raise MigrationError(f"Database has migration {version}, which this software does not include.")
        if version > resolvedTarget:
            raise MigrationError(
                f"Database schema version {version} is newer than target {resolvedTarget}."
            )
        if availableMigrations[version]["checksum"] != checksum:
            raise MigrationError(
                f"Applied migration {availableMigrations[version]['fileName']} has been modified."
            )

    pendingMigrations = [
        migration for migration in migrations
        if int(migration["version"]) <= resolvedTarget
        and int(migration["version"]) not in appliedChecksums
    ]
    if not pendingMigrations:
        return []
    if appliedChecksums and int(pendingMigrations[0]["version"]) < max(appliedChecksums):
        raise MigrationError(
            f"Migration {pendingMigrations[0]['fileName']} is older than the applied schema version."
        )

    async def runPendingMigrations() -> list[int]:
        # The whole pending batch is one exclusive transaction, so a failure leaves no hybrid schema
        await runStatement(connection, "BEGIN EXCLUSIVE TRANSACTION;")
        try:
            for migration in pendingMigrations:
                fileName = str(migration["fileName"])
                try:
                    for statement in migration["statements"]:
                        await executeMigrationStatement(connection, str(statement), fileName)
                except sqlite3.Error as error:
                    if isDatabaseLocked(error):
                        raise
                    raise MigrationError(f"Migration {fileName} failed: {error}") from error
                await runStatement(connection, """
                    INSERT INTO schema_migrations (version, name, checksum, applied_at)
                    VALUES (?, ?, ?, ?);
                """, (
                    migration["version"],
                    migration["name"],
                    migration["checksum"],
                    datetime.now(timezone.utc).isoformat(timespec="microseconds"),
                ))
            await runStatement(connection, "COMMIT;")
        except BaseException:
            if connection.in_transaction:
                await runStatement(connection, "ROLLBACK;")
            raise
        return [int(migration["version"]) for migration in pendingMigrations]

    return await retryWhileLocked(runPendingMigrations, "schema migration")


def loadSchemaFile(schemaPath: str = SCHEMA_FILE) -> dict[str, object]:
    if not os.path.isfile(schemaPath):
        raise MigrationError(f"Baseline schema file does not exist: {schemaPath}.")
    with open(schemaPath, "r", encoding="utf-8") as schemaFile:
        script = schemaFile.read().replace("\r\n", "\n")
    fileName = os.path.basename(schemaPath)
    return {
        "fileName": fileName,
        "checksum": hashlib.sha256(script.encode("utf-8")).hexdigest(),
        "statements": splitSqlStatements(script, fileName),
    }


async def applyBaseSchema(
    connection: aiosqlite.Connection,
    schemaPath: str = SCHEMA_FILE,
) -> bool:
    schema = loadSchemaFile(schemaPath)
    fileName = str(schema["fileName"])
    await runStatement(connection, """
        CREATE TABLE IF NOT EXISTS schema_baseline (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            checksum TEXT NOT NULL,
            applied_at TEXT NOT NULL
        );
    """)
    # The baseline is idempotent, so it is only re-run when schema.sql changes
    appliedChecksum = await fetchOneValue(connection, "SELECT checksum FROM schema_baseline WHERE id = 1;")
    if appliedChecksum == schema["checksum"]:
        return False

    async def runBaseSchema() -> bool:
        await runStatement(connection, "BEGIN EXCLUSIVE TRANSACTION;")
        try:
            try:
                for statement in schema["statements"]:
                    await executeMigrationStatement(connection, str(statement), fileName)
            except sqlite3.Error as error:
                if isDatabaseLocked(error):
                    raise
                raise MigrationError(f"Baseline schema {fileName} failed: {error}") from error
            await runStatement(connection, """
                INSERT INTO schema_baseline (id, checksum, applied_at) VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET checksum = excluded.checksum, applied_at = excluded.applied_at;
            """, (
                schema["checksum"],
                datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            ))
            await runStatement(connection, "COMMIT;")
        except BaseException:
            if connection.in_transaction:
                await runStatement(connection, "ROLLBACK;")
            raise
        return True

    return await retryWhileLocked(runBaseSchema, "baseline schema")


def getSearchFrames(
    latitude: float,
    longitude: float,
    radiusMeters: float,
) -> list[tuple[float, float, float, float]]:
    # Use the smallest Earth radii so the rectangular R-Tree frame never clips the true radius
    latitudeDelta = math.degrees(radiusMeters / WGS84_MINIMUM_MERIDIAN_RADIUS) * SEARCH_FRAME_MARGIN
    minLatitude = latitude - latitudeDelta
    maxLatitude = latitude + latitudeDelta
    if minLatitude <= -90.0 or maxLatitude >= 90.0:
        return [(-180.0, max(-90.0, minLatitude), 180.0, min(90.0, maxLatitude))]

    widestLatitude = max(abs(minLatitude), abs(maxLatitude))
    longitudeDelta = math.degrees(
        radiusMeters / (WGS84_SEMI_MAJOR_AXIS * math.cos(math.radians(widestLatitude)))
    ) * SEARCH_FRAME_MARGIN
    if longitudeDelta >= 180.0:
        return [(-180.0, minLatitude, 180.0, maxLatitude)]

    minLongitude = longitude - longitudeDelta
    maxLongitude = longitude + longitudeDelta
    # Split frames that cross the antimeridian into two valid longitude ranges
    if minLongitude < -180.0:
        return [
            (minLongitude + 360.0, minLatitude, 180.0, maxLatitude),
            (-180.0, minLatitude, maxLongitude, maxLatitude),
        ]
    if maxLongitude > 180.0:
        return [
            (minLongitude, minLatitude, 180.0, maxLatitude),
            (-180.0, minLatitude, maxLongitude - 360.0, maxLatitude),
        ]
    return [(minLongitude, minLatitude, maxLongitude, maxLatitude)]


def buildThreatRadiusQuery(
    latitude: float,
    longitude: float,
    radiusMeters: float = THREAT_RADIUS_METERS,
    lookbackDays: float = THREAT_LOOKBACK_DAYS,
    category: str = "immediate_threat",
    referenceTime: Optional[datetime] = None,
) -> tuple[str, list[object]]:
    latitude = float(latitude)
    longitude = float(longitude)
    radiusMeters = float(radiusMeters)
    lookbackDays = float(lookbackDays)
    if not math.isfinite(latitude) or not -90.0 <= latitude <= 90.0:
        raise ValueError("Threat query latitude must be between -90 and 90 degrees.")
    if not math.isfinite(longitude) or not -180.0 <= longitude <= 180.0:
        raise ValueError("Threat query longitude must be between -180 and 180 degrees.")
    if not math.isfinite(radiusMeters) or radiusMeters <= 0:
        raise ValueError("Threat query radius must be a positive number of meters.")
    if not math.isfinite(lookbackDays) or lookbackDays <= 0:
        raise ValueError("Threat query lookback must be a positive number of days.")

    currentTime = referenceTime or datetime.now(timezone.utc)
    if currentTime.tzinfo is None:
        currentTime = currentTime.replace(tzinfo=timezone.utc)
    # Match the acoustic worker's UTC ISO format so the B-Tree string range is exact
    cutoffTimestamp = (currentTime.astimezone(timezone.utc) - timedelta(days=lookbackDays)).isoformat(
        timespec="microseconds"
    )
    searchPoint = f"POINT({longitude:.9f} {latitude:.9f})"
    searchFrames = getSearchFrames(latitude, longitude, radiusMeters)
    frameQuery = " UNION ".join(
        """
                SELECT ROWID FROM SpatialIndex
                WHERE f_table_name = 'acoustic_events'
                    AND f_geometry_column = 'geom'
                    AND search_frame = BuildMbr(?, ?, ?, ?, 4326)"""
        for _ in searchFrames
    )

    # INDEXED BY forces the timestamp B-Tree; the SpatialIndex subquery drives the R-Tree lookup
    query = f"""
        SELECT events.id, events.device_id, events.location_id, events.latitude,
               events.longitude, events.altitude, events.class_id, events.class_name,
               events.category, events.confidence, events.start_timestamp,
               events.end_timestamp, events.window_count, events.metadata,
               AsText(events.geom) AS geometry_wkt,
               ST_Distance(CastToXY(events.geom), ST_GeomFromText(?, 4326), 1) AS distance_meters
        FROM acoustic_events AS events INDEXED BY idx_acoustic_events_category_start
        WHERE events.category = ?
            AND events.start_timestamp >= ?
            AND events.geom IS NOT NULL
            AND events.ROWID IN ({frameQuery}
            )
            AND ST_Distance(CastToXY(events.geom), ST_GeomFromText(?, 4326), 1) <= ?
        ORDER BY events.start_timestamp DESC;
    """
    parameters: list[object] = [searchPoint, category, cutoffTimestamp]
    for searchFrame in searchFrames:
        parameters.extend(searchFrame)
    parameters.extend([searchPoint, radiusMeters])
    return query, parameters


def groupWriteRequests(batch: list[WriteRequest]) -> list[list[WriteRequest]]:
    groups: list[list[WriteRequest]] = []
    # Only neighbouring identical statements are grouped, preserving write order
    for request in batch:
        if (
            groups
            and request.isBatchable
            and groups[-1][0].isBatchable
            and groups[-1][0].statements[0][0] == request.statements[0][0]
        ):
            groups[-1].append(request)
        else:
            groups.append([request])
    return groups


class DatabaseManager:
    def __init__(
        self,
        databasePath: str = DB_FILE,
        spatialiteExtension: Optional[str] = SPATIALITE_EXT,
        readPoolSize: int = READ_POOL_SIZE,
        migrationsDirectory: str = MIGRATIONS_DIRECTORY,
        targetSchemaVersion: Optional[int] = TARGET_SCHEMA_VERSION,
        schemaPath: Optional[str] = SCHEMA_FILE,
    ) -> None:
        self.databasePath = os.path.expanduser(databasePath)
        self.spatialiteExtension = spatialiteExtension
        self.readPoolSize = int(readPoolSize)
        self.migrationsDirectory = migrationsDirectory
        self.targetSchemaVersion = targetSchemaVersion
        self.schemaPath = schemaPath
        self.baseSchemaApplied = False
        self.writeConnection: Optional[aiosqlite.Connection] = None
        self.readerConnections: list[aiosqlite.Connection] = []
        self.readerPool: Optional[asyncio.Queue[aiosqlite.Connection]] = None
        self.writeQueue: asyncio.Queue[Optional[WriteRequest]] = asyncio.Queue(maxsize=WRITE_QUEUE_SIZE)
        self.writeTask: Optional[asyncio.Task] = None
        self.appliedMigrations: list[int] = []
        self.isClosing = False
        if self.readPoolSize <= 0:
            raise ValueError("Database read pool size must be positive.")

    async def __aenter__(self) -> "DatabaseManager":
        await self.start()
        return self

    async def __aexit__(self, *exceptionDetails: object) -> None:
        await self.close()

    async def openConnection(self, isReadOnly: bool) -> aiosqlite.Connection:
        connection = await aiosqlite.connect(
            self.databasePath,
            timeout=BUSY_TIMEOUT_MS / 1000.0,
            isolation_level=None,
        )
        try:
            connection.row_factory = sqlite3.Row
            if self.spatialiteExtension:
                await connection.enable_load_extension(True)
                await fetchOneValue(connection, "SELECT load_extension(?);", (self.spatialiteExtension,))
                await connection.enable_load_extension(False)
            await runStatement(connection, f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS};")
            # SQLite ignores the schema's foreign keys unless every connection enables them
            await runStatement(connection, "PRAGMA foreign_keys = ON;")
            if not isReadOnly:
                journalMode = await fetchOneValue(connection, "PRAGMA journal_mode = WAL;")
                if str(journalMode).casefold() != "wal":
                    raise sqlite3.OperationalError(
                        f"SQLite could not enable WAL mode; journal_mode is {journalMode}."
                    )
            await runStatement(connection, "PRAGMA synchronous = NORMAL;")
            if isReadOnly:
                await runStatement(connection, "PRAGMA query_only = ON;")
        except BaseException:
            await connection.close()
            raise
        return connection

    async def start(self) -> None:
        if self.writeTask is not None:
            return
        if self.isClosing:
            raise DatabaseClosedError("A closed database manager cannot be restarted.")
        databaseDirectory = os.path.dirname(self.databasePath)
        if databaseDirectory:
            os.makedirs(databaseDirectory, exist_ok=True)

        try:
            # The writer opens first so WAL is active before the reader pool connects
            writeConnection = await self.openConnection(isReadOnly=False)
            self.writeConnection = writeConnection
            if self.spatialiteExtension:
                hasSpatialMetadata = await fetchOneValue(
                    writeConnection,
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'spatial_ref_sys';",
                )
                if hasSpatialMetadata is None:
                    await retryWhileLocked(
                        lambda: fetchOneValue(writeConnection, "SELECT InitSpatialMetadata(1);"),
                        "spatial metadata initialization",
                    )
            # Baseline tables come first so numbered migrations can alter them later
            if self.schemaPath:
                self.baseSchemaApplied = await applyBaseSchema(writeConnection, self.schemaPath)
            self.appliedMigrations = await applyMigrations(
                writeConnection,
                self.migrationsDirectory,
                self.targetSchemaVersion,
            )

            self.readerPool = asyncio.Queue(maxsize=self.readPoolSize)
            for _ in range(self.readPoolSize):
                readerConnection = await self.openConnection(isReadOnly=True)
                self.readerConnections.append(readerConnection)
                self.readerPool.put_nowait(readerConnection)
            self.writeTask = asyncio.create_task(self.runWriteWorker())
        except BaseException:
            await self.closeConnections()
            raise

    async def closeConnections(self) -> None:
        for readerConnection in self.readerConnections:
            await readerConnection.close()
        self.readerConnections = []
        self.readerPool = None
        if self.writeConnection is not None:
            await self.writeConnection.close()
            self.writeConnection = None

    async def close(self) -> None:
        if self.isClosing:
            return
        self.isClosing = True
        if self.writeTask is not None:
            if not self.writeTask.done():
                # The sentinel lets the worker flush every write queued before shutdown
                await self.writeQueue.put(None)
            try:
                await self.writeTask
            finally:
                self.writeTask = None
        await self.closeConnections()

    async def enqueueRequest(self, request: WriteRequest) -> asyncio.Future:
        if self.isClosing or self.writeTask is None or self.writeTask.done():
            raise DatabaseClosedError("The database write worker is not running.")
        await self.writeQueue.put(request)
        return request.future

    async def enqueueWrite(
        self,
        sql: str,
        parameters: SqlParameters = (),
        requireLastRowID: bool = False,
    ) -> asyncio.Future:
        request = WriteRequest(
            statements=[(validateWriteStatement(sql), parameters)],
            future=asyncio.get_running_loop().create_future(),
            isBatchable=not requireLastRowID,
            isTransaction=False,
        )
        return await self.enqueueRequest(request)

    async def executeWrite(
        self,
        sql: str,
        parameters: SqlParameters = (),
        requireLastRowID: bool = False,
    ) -> WriteResult:
        return await (await self.enqueueWrite(sql, parameters, requireLastRowID))

    async def executeTransaction(
        self,
        statements: Sequence[tuple[str, SqlParameters]],
    ) -> list[WriteResult]:
        if not statements:
            return []
        request = WriteRequest(
            statements=[
                (validateWriteStatement(sql), parameters) for sql, parameters in statements
            ],
            future=asyncio.get_running_loop().create_future(),
            isBatchable=False,
            isTransaction=True,
        )
        return await (await self.enqueueRequest(request))

    async def runWriteWorker(self) -> None:
        loop = asyncio.get_running_loop()
        isStopping = False
        while not isStopping:
            request = await self.writeQueue.get()
            if request is None:
                break

            batch = [request]
            flushDeadline = loop.time() + WRITE_BATCH_INTERVAL_SEC
            # Flush on the configured interval or as soon as the volume threshold is reached
            while len(batch) < WRITE_BATCH_SIZE:
                try:
                    request = self.writeQueue.get_nowait()
                except asyncio.QueueEmpty:
                    remainingTime = flushDeadline - loop.time()
                    if remainingTime <= 0:
                        break
                    try:
                        request = await asyncio.wait_for(self.writeQueue.get(), remainingTime)
                    except TimeoutError:
                        break
                if request is None:
                    isStopping = True
                    break
                batch.append(request)
            await self.flushWriteBatch(batch)

    async def executeWriteGroup(self, group: list[WriteRequest]) -> None:
        connection = self.writeConnection
        if connection is None:
            raise DatabaseClosedError("The database write connection is closed.")
        await runStatement(connection, "SAVEPOINT write_group;")
        try:
            if len(group) > 1:
                sql = group[0].statements[0][0]
                cursor = await connection.executemany(
                    sql, [request.statements[0][1] for request in group]
                )
                await cursor.close()
                for request in group:
                    request.results = [WriteResult(rowCount=-1, lastRowID=None)]
            else:
                request = group[0]
                request.results = []
                for sql, parameters in request.statements:
                    request.results.append(await runStatement(connection, sql, parameters))
            await runStatement(connection, "RELEASE write_group;")
        except (sqlite3.Error, TypeError, ValueError) as error:
            if isDatabaseLocked(error):
                raise
            await runStatement(connection, "ROLLBACK TO write_group;")
            await runStatement(connection, "RELEASE write_group;")
            # Retry grouped rows one by one so a single bad row cannot drop valid writes
            if len(group) > 1:
                for request in group:
                    await self.executeWriteGroup([request])
            else:
                group[0].results = []
                group[0].error = error

    async def writeBatchOnce(self, batch: list[WriteRequest]) -> None:
        connection = self.writeConnection
        if connection is None:
            raise DatabaseClosedError("The database write connection is closed.")
        for request in batch:
            request.results = []
            request.error = None
        await runStatement(connection, "BEGIN IMMEDIATE;")
        try:
            for group in groupWriteRequests(batch):
                await self.executeWriteGroup(group)
            await runStatement(connection, "COMMIT;")
        except BaseException:
            if connection.in_transaction:
                await runStatement(connection, "ROLLBACK;")
            raise

    async def flushWriteBatch(self, batch: list[WriteRequest]) -> None:
        batchError: Optional[BaseException] = None
        try:
            # Lock errors are retried indefinitely so queued sensor data is never discarded
            await retryWhileLocked(
                lambda: self.writeBatchOnce(batch),
                f"batch write of {len(batch)} request(s)",
                maxAttempts=None,
            )
        except Exception as error:
            batchError = error
            print(f"Database batch write failed: {error}")

        for request in batch:
            if request.future.done():
                continue
            error = batchError or request.error
            if error is not None:
                request.future.set_exception(error)
            elif request.isTransaction:
                request.future.set_result(list(request.results))
            else:
                request.future.set_result(request.results[0])

    @asynccontextmanager
    async def acquireReader(self) -> AsyncIterator[aiosqlite.Connection]:
        if self.readerPool is None or self.isClosing:
            raise DatabaseClosedError("The database reader pool is not running.")
        readerPool = self.readerPool
        connection = await readerPool.get()
        try:
            yield connection
        finally:
            readerPool.put_nowait(connection)

    async def fetchAll(
        self,
        sql: str,
        parameters: SqlParameters = (),
    ) -> list[dict[str, object]]:
        async def readRows() -> list[dict[str, object]]:
            async with self.acquireReader() as connection:
                async with connection.execute(sql, parameters) as cursor:
                    rows = await cursor.fetchall()
            return [dict(row) for row in rows]

        return await retryWhileLocked(readRows, "database read")

    async def fetchThreatEventsWithinRadius(
        self,
        latitude: float,
        longitude: float,
        radiusMeters: float = THREAT_RADIUS_METERS,
        lookbackDays: float = THREAT_LOOKBACK_DAYS,
        category: str = "immediate_threat",
    ) -> list[dict[str, object]]:
        query, parameters = buildThreatRadiusQuery(
            latitude, longitude, radiusMeters, lookbackDays, category
        )
        return await self.fetchAll(query, parameters)


async def main() -> None:
    print("Connecting to the SpatiaLite database and applying schema migrations...")
    async with DatabaseManager() as databaseManager:
        if databaseManager.baseSchemaApplied:
            print("Applied the baseline schema from schema.sql.")
        if databaseManager.appliedMigrations:
            print(f"Applied schema migrations: {databaseManager.appliedMigrations}.")
        else:
            print("Database schema is already up to date.")
        versionRows = await databaseManager.fetchAll(
            "SELECT MAX(version) AS version FROM schema_migrations;"
        )
        print(f"Current schema version: {versionRows[0]['version'] or 0}.")


if __name__ == "__main__":
    asyncio.run(main())
