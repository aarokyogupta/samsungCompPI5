import asyncio
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from fastapi import APIRouter, Body, FastAPI, Query, Request, WebSocket, WebSocketDisconnect
import h3
import itertools
import json
import logging
import os
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator
import secrets
from starlette.exceptions import HTTPException
from starlette.websockets import WebSocketState
import sys
import time
from typing import Annotated, Any, AsyncGenerator, Optional
import uuid
import yaml

# Allow "python api/websocketManager.py" as well as loading through api/main.py
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database.models import formatUtcTimestamp  # noqa: E402

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

WEBSOCKET_CONFIG: dict = config.get("websocketApi", {}) or {}

# Define the socket path; it is absolute because this router is mounted without a prefix of its own
ALERT_PATH: str = str(WEBSOCKET_CONFIG.get("alertPath", "/ws/alerts/{client_id}"))

# Define connection limits
MAX_CONNECTIONS: int = int(WEBSOCKET_CONFIG.get("maxConnections", 100))
MAX_CONNECTIONS_PER_CLIENT: int = int(WEBSOCKET_CONFIG.get("maxConnectionsPerClient", 3))
CLIENT_TYPES: tuple[str, ...] = tuple(WEBSOCKET_CONFIG.get("clientTypes", ["web", "ios", "android"]) or ["web"])
DEFAULT_CLIENT_TYPE: str = str(WEBSOCKET_CONFIG.get("defaultClientType", CLIENT_TYPES[0]))

# Define per-socket delivery settings
SEND_QUEUE_SIZE: int = int(WEBSOCKET_CONFIG.get("sendQueueSize", 50))
SEND_TIMEOUT_SEC: float = float(WEBSOCKET_CONFIG.get("sendTimeoutSec", 10.0))
HEARTBEAT_INTERVAL_SEC: float = float(WEBSOCKET_CONFIG.get("heartbeatIntervalSec", 30.0))
HEARTBEAT_MISSES_ALLOWED: int = int(WEBSOCKET_CONFIG.get("heartbeatMissesAllowed", 2))

# Define zone (H3) settings
ZONE_RESOLUTION: int = int(WEBSOCKET_CONFIG.get("zoneResolution", (config.get("edna", {}) or {}).get("h3Resolution", 7)))
MIN_ZONE_RESOLUTION: int = int(WEBSOCKET_CONFIG.get("minZoneResolution", 0))
MAX_ZONES_PER_CLIENT: int = int(WEBSOCKET_CONFIG.get("maxZonesPerClient", 32))

# Define the priority ladder and the risk thresholds that promote an assessment onto it
PRIORITIES: tuple[str, ...] = tuple(WEBSOCKET_CONFIG.get("priorities", ["INFO", "ROUTINE", "ELEVATED", "HIGH", "CRITICAL"]))
PRIORITY_RANK: dict[str, int] = {name: index for index, name in enumerate(PRIORITIES)}
LOWEST_PRIORITY: str = PRIORITIES[0]
HIGHEST_PRIORITY: str = PRIORITIES[-1]
DEFAULT_MIN_PRIORITY: str = str(WEBSOCKET_CONFIG.get("defaultMinPriority", LOWEST_PRIORITY))
CRI_CRITICAL_THRESHOLD: float = float(WEBSOCKET_CONFIG.get("criCriticalThreshold", 80.0))
CRI_HIGH_THRESHOLD: float = float(WEBSOCKET_CONFIG.get("criHighThreshold", 60.0))
CRI_ELEVATED_THRESHOLD: float = float(WEBSOCKET_CONFIG.get("criElevatedThreshold", 40.0))

# Define fan-out behaviour
FORWARD_TELEMETRY: bool = bool(WEBSOCKET_CONFIG.get("forwardTelemetry", False))
DUPLICATE_WINDOW_SEC: float = float(WEBSOCKET_CONFIG.get("duplicateWindowSec", 30.0))
REPLAY_BUFFER_SIZE: int = int(WEBSOCKET_CONFIG.get("replayBufferSize", 50))
REPLAY_ON_CONNECT: int = int(WEBSOCKET_CONFIG.get("replayOnConnect", 10))

# Define handshake authentication; browsers cannot set headers on a WebSocket, so the key travels as a query parameter
TOKEN_QUERY_PARAMETER: str = str(WEBSOCKET_CONFIG.get("tokenQueryParameter", "token"))
REQUIRE_TOKEN: bool = bool(WEBSOCKET_CONFIG.get("requireToken", False))
SHUTDOWN_CLOSE_CODE: int = int(WEBSOCKET_CONFIG.get("shutdownCloseCode", 1001))

# WebSocket close codes used by the handshake and the heartbeat
CLOSE_POLICY_VIOLATION: int = 1008
CLOSE_TRY_AGAIN_LATER: int = 1013
CLOSE_GOING_AWAY: int = 1001

logger = logging.getLogger("icmis.api.websocket")


# Field types shared by the payload schema and the control messages

LabelText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=64)]
# Zones are H3 cells, but a human sector name such as "Sector_4_North" is accepted and matched literally
ZoneText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=64)]


# 4. Standardised JSON payload: web, iOS and Android all parse this exact shape

class AlertMetrics(BaseModel):
    # extra="allow" so a new model can attach its own score without a schema change on every client
    model_config = ConfigDict(extra="allow")

    cri_score: Optional[float] = Field(None, ge=0.0, le=100.0)
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)


class AlertLocation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lat: Optional[float] = Field(None, ge=-90.0, le=90.0)
    lng: Optional[float] = Field(None, ge=-180.0, le=180.0)
    zone: Optional[ZoneText] = None

    @model_validator(mode="after")
    def deriveZone(self) -> "AlertLocation":
        # A coordinate without a zone still has to reach the rangers subscribed to the cell it falls in
        if self.zone is None and self.lat is not None and self.lng is not None:
            self.zone = h3.latlng_to_cell(self.lat, self.lng, ZONE_RESOLUTION)
        return self


class AlertEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: f"evt_{uuid.uuid4().hex[:16]}", max_length=64)
    timestamp: str = Field(default_factory=lambda: formatUtcTimestamp(datetime.now(timezone.utc)))
    priority: LabelText = DEFAULT_MIN_PRIORITY
    event_type: LabelText
    classification: LabelText
    metrics: AlertMetrics = Field(default_factory=AlertMetrics)
    location: Optional[AlertLocation] = None

    @field_validator("priority")
    @classmethod
    def checkPriority(cls, value: str) -> str:
        priority = value.strip().upper()
        if priority not in PRIORITY_RANK:
            raise ValueError(f"priority must be one of {', '.join(PRIORITIES)}.")
        return priority

    @property
    def zone(self) -> Optional[str]:
        return self.location.zone if self.location is not None else None

    @property
    def rank(self) -> int:
        return PRIORITY_RANK[self.priority]

    def toFrame(self) -> dict[str, Any]:
        # "type" is added on the wire only, so the documented payload above stays exactly as specified
        return {"type": "alert", **self.model_dump(mode="json")}


class AlertPublishRequest(AlertEvent):
    # Same schema as the pushed payload; the identifier and timestamp are filled in when the caller omits them
    model_config = ConfigDict(extra="forbid")

    # A zone given here overrides the one derived from the coordinates
    zone_override: Optional[ZoneText] = None


class SubscriptionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    zones: list[ZoneText] = Field(default_factory=list, max_length=MAX_ZONES_PER_CLIENT)
    min_priority: Optional[LabelText] = None

    @field_validator("min_priority")
    @classmethod
    def checkMinPriority(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        priority = value.strip().upper()
        if priority not in PRIORITY_RANK:
            raise ValueError(f"min_priority must be one of {', '.join(PRIORITIES)}.")
        return priority


# 1. State and connection registry

@dataclass
class ClientConnection:
    connectionID: str
    clientID: str
    clientType: str
    websocket: WebSocket
    # Empty means "every zone"; a subscription also matches every H3 cell nested inside it
    zones: set[str] = field(default_factory=set)
    minPriority: str = DEFAULT_MIN_PRIORITY
    sendQueue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=SEND_QUEUE_SIZE))
    senderTask: Optional[asyncio.Task] = None
    connectedAt: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    lastSeenAt: float = field(default_factory=time.monotonic)
    missedPings: int = 0
    sentCount: int = 0
    droppedCount: int = 0
    isClosing: bool = False

    @property
    def minRank(self) -> int:
        return PRIORITY_RANK.get(self.minPriority, 0)

    def describe(self) -> dict[str, Any]:
        return {
            "connection_id": self.connectionID,
            "client_id": self.clientID,
            "client_type": self.clientType,
            "zones": sorted(self.zones),
            "min_priority": self.minPriority,
            "connected_at": formatUtcTimestamp(self.connectedAt),
            "queued": self.sendQueue.qsize(),
            "sent": self.sentCount,
            "dropped": self.droppedCount,
            "missed_pings": self.missedPings,
        }


def zoneMatches(subscription: str, alertZone: Optional[str]) -> bool:
    # A ranger watching a coarse cell still sees an anomaly reported against a finer cell inside it
    if alertZone is None:
        return False
    if subscription == alertZone:
        return True
    try:
        subscriptionResolution = h3.get_resolution(subscription)
        if not h3.is_valid_cell(subscription) or not h3.is_valid_cell(alertZone):
            return False
        if subscriptionResolution > h3.get_resolution(alertZone):
            return False
        return h3.cell_to_parent(alertZone, subscriptionResolution) == subscription
    except Exception:
        # Named sectors such as "Sector_4_North" are not H3 cells, so only the literal match above applies
        return False


class ConnectionManager:
    def __init__(self, maxConnections: int = MAX_CONNECTIONS, perClient: int = MAX_CONNECTIONS_PER_CLIENT) -> None:
        self.maxConnections = maxConnections
        self.perClient = perClient
        # Grouped by client identifier; one dashboard user may hold a web, an iOS and an Android socket at once
        self.activeConnections: dict[str, list[ClientConnection]] = {}
        # Zone index kept beside the registry so a targeted fan-out does not walk every socket
        self.zoneSubscriptions: dict[str, set[str]] = {}
        self.connectionSequence = itertools.count(1)
        self.replayBuffer: deque[AlertEvent] = deque(maxlen=max(REPLAY_BUFFER_SIZE, 0))
        self.recentAlerts: dict[tuple[str, str, str], float] = {}
        self.heartbeatTask: Optional[asyncio.Task] = None
        self.publishedAlerts = 0
        self.suppressedAlerts = 0
        self.droppedFrames = 0
        self.isClosed = False

    @property
    def connectionCount(self) -> int:
        return sum(len(connections) for connections in self.activeConnections.values())

    def allConnections(self) -> list[ClientConnection]:
        return [connection for connections in self.activeConnections.values() for connection in connections]

    # 2. Connection lifecycle

    async def connect(
        self,
        websocket: WebSocket,
        clientID: str,
        clientType: str = DEFAULT_CLIENT_TYPE,
        zones: Optional[list[str]] = None,
        minPriority: str = DEFAULT_MIN_PRIORITY,
    ) -> Optional[ClientConnection]:
        # Accepting first means a refusal arrives as a readable close frame instead of an opaque handshake failure
        await websocket.accept()
        if self.isClosed:
            await closeQuietly(websocket, CLOSE_GOING_AWAY, "Server is shutting down.")
            return None
        if self.connectionCount >= self.maxConnections:
            await refuse(websocket, CLOSE_TRY_AGAIN_LATER, f"Connection limit ({self.maxConnections}) reached.")
            return None
        if len(self.activeConnections.get(clientID, [])) >= self.perClient:
            await refuse(websocket, CLOSE_TRY_AGAIN_LATER, f"Client {clientID} already holds {self.perClient} sockets.")
            return None

        connection = ClientConnection(
            connectionID=f"con_{next(self.connectionSequence):06d}",
            clientID=clientID,
            clientType=clientType,
            websocket=websocket,
            minPriority=minPriority,
        )
        self.activeConnections.setdefault(clientID, []).append(connection)
        self.setZones(connection, zones or [])
        connection.senderTask = asyncio.create_task(self.senderLoop(connection))
        logger.info("WebSocket %s connected (client=%s, type=%s, zones=%s).",
                    connection.connectionID, clientID, clientType, sorted(connection.zones) or ["*"])
        await self.sendControl(connection, {
            "type": "welcome",
            "connection_id": connection.connectionID,
            "client_id": clientID,
            "client_type": clientType,
            "zones": sorted(connection.zones),
            "min_priority": connection.minPriority,
            "heartbeat_interval_sec": HEARTBEAT_INTERVAL_SEC,
            "priorities": list(PRIORITIES),
            "server_time": formatUtcTimestamp(datetime.now(timezone.utc)),
        })
        self.replayTo(connection)
        return connection

    async def disconnect(self, connection: ClientConnection, code: int = CLOSE_GOING_AWAY, reason: str = "") -> None:
        if connection.isClosing:
            return
        connection.isClosing = True
        connections = self.activeConnections.get(connection.clientID, [])
        if connection in connections:
            connections.remove(connection)
        if not connections:
            self.activeConnections.pop(connection.clientID, None)
        self.setZones(connection, [], deregisterOnly=True)
        # None wakes the sender so it finishes instead of waiting on a queue nobody will fill again
        connection.sendQueue.put_nowait(None)
        senderTask = connection.senderTask
        if senderTask is not None and senderTask is not asyncio.current_task():
            try:
                await asyncio.wait_for(asyncio.shield(senderTask), timeout=SEND_TIMEOUT_SEC)
            except (TimeoutError, asyncio.CancelledError):
                senderTask.cancel()
        await closeQuietly(connection.websocket, code, reason)
        logger.info("WebSocket %s disconnected (sent=%d, dropped=%d, reason=%s).",
                    connection.connectionID, connection.sentCount, connection.droppedCount, reason or code)

    def setZones(self, connection: ClientConnection, zones: list[str], deregisterOnly: bool = False) -> None:
        # Every zone change funnels through here so the registry and the index can never disagree
        for zone in connection.zones:
            subscribers = self.zoneSubscriptions.get(zone)
            if subscribers is not None:
                subscribers.discard(connection.connectionID)
                if not subscribers:
                    self.zoneSubscriptions.pop(zone, None)
        connection.zones = set() if deregisterOnly else set(zones[:MAX_ZONES_PER_CLIENT])
        for zone in connection.zones:
            self.zoneSubscriptions.setdefault(zone, set()).add(connection.connectionID)

    async def heartbeatMonitor(self) -> None:
        # A phone that drives out of cell range never sends a close frame, so silence is the only signal left
        try:
            while not self.isClosed:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
                deadline = time.monotonic() - HEARTBEAT_INTERVAL_SEC
                for connection in self.allConnections():
                    if connection.lastSeenAt > deadline:
                        connection.missedPings = 0
                    else:
                        connection.missedPings += 1
                    if connection.missedPings > HEARTBEAT_MISSES_ALLOWED:
                        await self.disconnect(connection, CLOSE_GOING_AWAY, "Heartbeat timed out.")
                        continue
                    await self.sendControl(connection, {
                        "type": "ping",
                        "server_time": formatUtcTimestamp(datetime.now(timezone.utc)),
                    })
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Heartbeat monitor stopped unexpectedly.")

    async def senderLoop(self, connection: ClientConnection) -> None:
        # One writer per socket: a stalled mobile link blocks only its own queue, never the fan-out
        try:
            while True:
                frame = await connection.sendQueue.get()
                if frame is None:
                    break
                try:
                    await asyncio.wait_for(connection.websocket.send_json(frame), timeout=SEND_TIMEOUT_SEC)
                except (TimeoutError, WebSocketDisconnect, RuntimeError) as error:
                    logger.info("WebSocket %s send failed (%s); dropping the connection.",
                                connection.connectionID, type(error).__name__)
                    asyncio.create_task(self.disconnect(connection, CLOSE_GOING_AWAY, "Send failed."))
                    break
                connection.sentCount += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Sender loop for %s stopped unexpectedly.", connection.connectionID)

    # 3. Broadcast engine

    def enqueue(self, connection: ClientConnection, frame: dict[str, Any]) -> bool:
        if connection.isClosing:
            return False
        if connection.sendQueue.full():
            # A backed-up socket loses its oldest alert rather than stalling every other client
            try:
                connection.sendQueue.get_nowait()
                connection.droppedCount += 1
                self.droppedFrames += 1
            except asyncio.QueueEmpty:
                pass
        connection.sendQueue.put_nowait(frame)
        return True

    async def sendControl(self, connection: ClientConnection, frame: dict[str, Any]) -> None:
        self.enqueue(connection, frame)

    def wants(self, connection: ClientConnection, alert: AlertEvent) -> bool:
        if alert.rank < connection.minRank:
            return False
        if not connection.zones:
            return True
        return any(zoneMatches(zone, alert.zone) for zone in connection.zones)

    async def broadcastGlobal(self, alert: AlertEvent) -> int:
        # Used for system-wide events; each socket still applies its own priority floor
        frame = alert.toFrame()
        delivered = 0
        for connection in self.allConnections():
            if alert.rank >= connection.minRank and self.enqueue(connection, frame):
                delivered += 1
        return delivered

    async def broadcastTargeted(self, alert: AlertEvent, zoneID: str) -> int:
        # Off-duty and out-of-area personnel are not woken for an anomaly in a sector they are not watching
        frame = alert.toFrame()
        delivered = 0
        for connection in self.allConnections():
            if not connection.zones:
                matches = True
            else:
                matches = any(zoneMatches(zone, zoneID) for zone in connection.zones)
            if matches and alert.rank >= connection.minRank and self.enqueue(connection, frame):
                delivered += 1
        return delivered

    async def dispatch(self, alert: AlertEvent) -> int:
        if self.isClosed:
            return 0
        if self.isDuplicate(alert):
            self.suppressedAlerts += 1
            return 0
        self.publishedAlerts += 1
        if self.replayBuffer.maxlen:
            self.replayBuffer.append(alert)
        if alert.zone:
            return await self.broadcastTargeted(alert, alert.zone)
        return await self.broadcastGlobal(alert)

    def isDuplicate(self, alert: AlertEvent) -> bool:
        # The same reading classified twice in one zone is one alarm, not two; a critical alert is never suppressed
        if DUPLICATE_WINDOW_SEC <= 0 or alert.priority == HIGHEST_PRIORITY:
            return False
        key = (alert.event_type, alert.classification, alert.zone or "*")
        now = time.monotonic()
        lastSeen = self.recentAlerts.get(key)
        self.recentAlerts = {seen: at for seen, at in self.recentAlerts.items() if now - at < DUPLICATE_WINDOW_SEC}
        self.recentAlerts[key] = now
        return lastSeen is not None and now - lastSeen < DUPLICATE_WINDOW_SEC

    def replayTo(self, connection: ClientConnection) -> int:
        # A dashboard opened seconds after an alarm still shows it instead of an empty panel
        if REPLAY_ON_CONNECT <= 0:
            return 0
        replayed = 0
        for alert in list(self.replayBuffer)[-REPLAY_ON_CONNECT:]:
            if self.wants(connection, alert):
                frame = alert.toFrame()
                frame["replayed"] = True
                self.enqueue(connection, frame)
                replayed += 1
        return replayed

    def describe(self) -> dict[str, Any]:
        byClientType: dict[str, int] = {}
        for connection in self.allConnections():
            byClientType[connection.clientType] = byClientType.get(connection.clientType, 0) + 1
        return {
            "connections": self.connectionCount,
            "max_connections": self.maxConnections,
            "clients": len(self.activeConnections),
            "by_client_type": byClientType,
            "zones_watched": sorted(self.zoneSubscriptions),
            "published_alerts": self.publishedAlerts,
            "suppressed_alerts": self.suppressedAlerts,
            "dropped_frames": self.droppedFrames,
            "buffered_alerts": len(self.replayBuffer),
            "heartbeat_interval_sec": HEARTBEAT_INTERVAL_SEC,
            "closed": self.isClosed,
        }

    async def start(self) -> None:
        self.isClosed = False
        if self.heartbeatTask is None and HEARTBEAT_INTERVAL_SEC > 0:
            self.heartbeatTask = asyncio.create_task(self.heartbeatMonitor())

    async def close(self) -> None:
        self.isClosed = True
        if self.heartbeatTask is not None:
            self.heartbeatTask.cancel()
            try:
                await self.heartbeatTask
            except (asyncio.CancelledError, Exception):
                pass
            self.heartbeatTask = None
        for connection in self.allConnections():
            await self.disconnect(connection, SHUTDOWN_CLOSE_CODE, "Server is shutting down.")


async def closeQuietly(websocket: WebSocket, code: int, reason: str) -> None:
    # A socket the client already abandoned raises on close; that is expected, not an error worth logging
    try:
        if websocket.client_state is not WebSocketState.DISCONNECTED:
            await websocket.close(code=code, reason=reason[:120])
    except (RuntimeError, WebSocketDisconnect):
        pass


async def refuse(websocket: WebSocket, code: int, reason: str) -> None:
    logger.warning("WebSocket refused: %s", reason)
    try:
        await websocket.send_json({"type": "error", "code": code, "detail": reason})
    except (RuntimeError, WebSocketDisconnect):
        pass
    await closeQuietly(websocket, code, reason)


# Event bridge: the in-process telemetry broker stands in for Redis, since the whole stack runs on one Pi

def priorityForRiskIndex(riskIndex: Optional[float]) -> str:
    if riskIndex is None:
        return DEFAULT_MIN_PRIORITY
    if riskIndex >= CRI_CRITICAL_THRESHOLD:
        return HIGHEST_PRIORITY
    if riskIndex >= CRI_HIGH_THRESHOLD:
        return PRIORITIES[max(len(PRIORITIES) - 2, 0)]
    if riskIndex >= CRI_ELEVATED_THRESHOLD:
        return PRIORITIES[max(len(PRIORITIES) - 3, 0)]
    return DEFAULT_MIN_PRIORITY


def buildLocation(data: dict[str, Any]) -> Optional[AlertLocation]:
    latitude = data.get("latitude")
    longitude = data.get("longitude")
    zone = data.get("h3_cell")
    if latitude is None and longitude is None and not zone:
        return None
    try:
        return AlertLocation(
            lat=float(latitude) if latitude is not None else None,
            lng=float(longitude) if longitude is not None else None,
            zone=str(zone) if zone else None,
        )
    except (TypeError, ValueError):
        return None


def alertFromTelemetryEvent(event: dict[str, Any]) -> Optional[AlertEvent]:
    eventType = event.get("event")
    data = event.get("data") or {}
    if eventType == "threat":
        # The fast path already decided this reading is a threat, so it goes out at the top of the ladder
        classification = data.get("class_name") or data.get("category") or data.get("reading_type") or "threat_detected"
        return AlertEvent(
            priority=HIGHEST_PRIORITY,
            event_type=f"{data.get('reading_type', 'sensor')}_anomaly",
            classification=str(classification),
            metrics=AlertMetrics(confidence=asOptionalFloat(data.get("confidence"))),
            location=buildLocation(data),
        )
    if eventType == "assessment":
        riskIndex = asOptionalFloat(data.get("conservation_risk_index"))
        classification = str(data.get("intervention_priority_tier") or "conservation_risk")
        return AlertEvent(
            priority=priorityForRiskIndex(riskIndex),
            event_type="risk_assessment",
            classification=classification,
            metrics=AlertMetrics(
                cri_score=riskIndex,
                escalation_level=data.get("escalation_level"),
                intervention_priority_index=asOptionalFloat(data.get("intervention_priority_index")),
                species_id=data.get("species_id"),
                run_id=data.get("run_id"),
            ),
            location=buildLocation(data),
        )
    if eventType == "telemetry" and FORWARD_TELEMETRY:
        return AlertEvent(
            priority=LOWEST_PRIORITY,
            event_type="telemetry_reading",
            classification=str(data.get("reading_type") or "reading"),
            metrics=AlertMetrics(confidence=asOptionalFloat(data.get("confidence"))),
            location=buildLocation(data),
        )
    return None


def asOptionalFloat(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


class AlertBridge:
    def __init__(self, app: FastAPI, manager: ConnectionManager) -> None:
        self.app = app
        self.manager = manager
        self.task: Optional[asyncio.Task] = None
        self.clientQueue: Optional[asyncio.Queue] = None
        self.status = "stopped"
        self.translated = 0
        self.ignored = 0

    async def start(self) -> None:
        service = getattr(self.app.state, "telemetryService", None)
        if service is None:
            # The alert path still works through publishAlert(); only the automatic bridge is missing
            self.status = "unavailable"
            logger.warning("Telemetry service not found; WebSocket alerts will only carry published events.")
            return
        try:
            self.clientQueue = service.broadcaster.subscribe()
        except HTTPException as error:
            self.status = f"unavailable: {error.detail}"
            logger.warning("Could not subscribe to the telemetry broker: %s", error.detail)
            return
        self.status = "running"
        self.task = asyncio.create_task(self.run())

    async def run(self) -> None:
        assert self.clientQueue is not None
        try:
            while True:
                event = await self.clientQueue.get()
                if event is None:
                    break
                alert = alertFromTelemetryEvent(event)
                if alert is None:
                    self.ignored += 1
                    continue
                self.translated += 1
                await self.manager.dispatch(alert)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Alert bridge stopped unexpectedly.")
        finally:
            self.status = "stopped"

    async def stop(self) -> None:
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except (asyncio.CancelledError, Exception):
                pass
            self.task = None
        service = getattr(self.app.state, "telemetryService", None)
        if service is not None and self.clientQueue is not None:
            service.broadcaster.unsubscribe(self.clientQueue)
        self.clientQueue = None
        self.status = "stopped"

    def describe(self) -> dict[str, Any]:
        return {"status": self.status, "translated": self.translated, "ignored": self.ignored}


router = APIRouter()


def getManager(request: Request) -> ConnectionManager:
    manager = getattr(request.app.state, "connectionManager", None)
    if manager is None:
        raise HTTPException(status_code=503, detail="WebSocket manager is not running.")
    return manager


async def publishAlert(app: FastAPI, alert: AlertEvent) -> int:
    # Entry point for any in-process component that needs to push an alert without going through the broker
    manager: Optional[ConnectionManager] = getattr(app.state, "connectionManager", None)
    if manager is None:
        return 0
    return await manager.dispatch(alert)


# Lifecycle hooks called by api/main.py (shutdown runs before the database is closed)

async def onStartup(app: FastAPI) -> None:
    manager = ConnectionManager()
    await manager.start()
    app.state.connectionManager = manager
    bridge = AlertBridge(app, manager)
    await bridge.start()
    app.state.alertBridge = bridge


async def onShutdown(app: FastAPI) -> None:
    bridge: Optional[AlertBridge] = getattr(app.state, "alertBridge", None)
    if bridge is not None:
        await bridge.stop()
        app.state.alertBridge = None
    manager: Optional[ConnectionManager] = getattr(app.state, "connectionManager", None)
    if manager is not None:
        await manager.close()
        app.state.connectionManager = None


# 1. Handshake authentication

def authenticate(websocket: WebSocket) -> Optional[str]:
    apiKey = getattr(getattr(websocket.app.state, "settings", None), "api_key", None)
    expected = apiKey.get_secret_value() if apiKey is not None else ""
    supplied = websocket.query_params.get(TOKEN_QUERY_PARAMETER, "")
    if not expected:
        # No key configured means an open LAN deployment, unless the config insists on one anyway
        return "A token is required but ICMIS_API_KEY is not configured." if REQUIRE_TOKEN else None
    if not secrets.compare_digest(supplied.encode(), expected.encode()):
        return "Missing or invalid token."
    return None


# 2. Connection lifecycle endpoint

@router.websocket(ALERT_PATH, name="alertSocket")
async def alertSocket(
    websocket: WebSocket,
    client_id: str,
    client_type: str = Query(DEFAULT_CLIENT_TYPE),
    zone: Optional[list[str]] = Query(None),
    min_priority: str = Query(DEFAULT_MIN_PRIORITY),
) -> None:
    manager: Optional[ConnectionManager] = getattr(websocket.app.state, "connectionManager", None)
    if manager is None:
        await websocket.accept()
        await refuse(websocket, CLOSE_TRY_AGAIN_LATER, "WebSocket manager is not running.")
        return

    rejection = authenticate(websocket)
    if rejection is not None:
        await websocket.accept()
        await refuse(websocket, CLOSE_POLICY_VIOLATION, rejection)
        return
    clientType = client_type.strip().lower()
    if clientType not in CLIENT_TYPES:
        await websocket.accept()
        await refuse(websocket, CLOSE_POLICY_VIOLATION, f"client_type must be one of {', '.join(CLIENT_TYPES)}.")
        return
    priority = min_priority.strip().upper()
    if priority not in PRIORITY_RANK:
        await websocket.accept()
        await refuse(websocket, CLOSE_POLICY_VIOLATION, f"min_priority must be one of {', '.join(PRIORITIES)}.")
        return

    connection = await manager.connect(websocket, client_id.strip(), clientType, zone or [], priority)
    if connection is None:
        return
    try:
        while True:
            message = await websocket.receive_text()
            # Any inbound frame proves the link is alive, so a chatty client never needs to answer the ping
            connection.lastSeenAt = time.monotonic()
            connection.missedPings = 0
            await handleClientMessage(manager, connection, message)
    except WebSocketDisconnect:
        await manager.disconnect(connection, CLOSE_GOING_AWAY, "Client closed the connection.")
    except Exception:
        logger.exception("WebSocket %s failed; closing.", connection.connectionID)
        await manager.disconnect(connection, CLOSE_GOING_AWAY, "Internal error.")


async def handleClientMessage(manager: ConnectionManager, connection: ClientConnection, message: str) -> None:
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        await manager.sendControl(connection, {"type": "error", "detail": "Frames must be JSON objects."})
        return
    if not isinstance(payload, dict):
        await manager.sendControl(connection, {"type": "error", "detail": "Frames must be JSON objects."})
        return

    messageType = str(payload.get("type", "")).strip().lower()
    if messageType == "pong":
        return
    if messageType == "ping":
        await manager.sendControl(connection, {
            "type": "pong",
            "server_time": formatUtcTimestamp(datetime.now(timezone.utc)),
        })
        return
    if messageType == "subscribe":
        # Filters may sit under "data" or be spread across the frame itself, minus its own routing key
        data = payload.get("data")
        if not isinstance(data, dict):
            data = {key: value for key, value in payload.items() if key != "type"}
        try:
            request = SubscriptionRequest.model_validate(data)
        except Exception as error:
            await manager.sendControl(connection, {"type": "error", "detail": str(error)[:300]})
            return
        manager.setZones(connection, request.zones)
        if request.min_priority is not None:
            connection.minPriority = request.min_priority
        await manager.sendControl(connection, {
            "type": "subscribed",
            "zones": sorted(connection.zones),
            "min_priority": connection.minPriority,
        })
        return
    if messageType == "unsubscribe":
        manager.setZones(connection, [])
        await manager.sendControl(connection, {"type": "subscribed", "zones": [], "min_priority": connection.minPriority})
        return
    if messageType == "status":
        await manager.sendControl(connection, {"type": "status", "connection": connection.describe()})
        return
    await manager.sendControl(connection, {"type": "error", "detail": f"Unknown frame type '{messageType}'."})


# 3. Broadcast engine exposed over REST, so any service can fan an alert out without holding a socket

@router.post("/ws/alerts", status_code=202, name="publishAlert")
async def publishAlertEndpoint(
    request: Request,
    payload: AlertPublishRequest = Body(...),
    zone_id: Optional[str] = Query(None, description="Restrict the fan-out to one zone instead of every client."),
) -> dict[str, Any]:
    manager = getManager(request)
    fields = payload.model_dump(exclude={"zone_override"})
    if payload.zone_override:
        location = fields.get("location") or {}
        location["zone"] = payload.zone_override
        fields["location"] = location
    alert = AlertEvent.model_validate(fields)
    delivered = await manager.broadcastTargeted(alert, zone_id) if zone_id else await manager.dispatch(alert)
    return {
        "event_id": alert.event_id,
        "priority": alert.priority,
        "zone": zone_id or alert.zone,
        "delivered": delivered,
        "connections": manager.connectionCount,
    }


@router.get("/ws/status", name="websocketStatus")
async def websocketStatus(request: Request, include_connections: bool = Query(False)) -> dict[str, Any]:
    manager = getManager(request)
    bridge: Optional[AlertBridge] = getattr(request.app.state, "alertBridge", None)
    status: dict[str, Any] = {
        "manager": manager.describe(),
        "bridge": bridge.describe() if bridge is not None else {"status": "not started"},
        "alert_path": ALERT_PATH,
    }
    if include_connections:
        status["connections_detail"] = [connection.describe() for connection in manager.allConnections()]
    return status


# In-process demo: serves this router on a loopback port and drives it with a real WebSocket client

async def main() -> None:
    import uvicorn
    import websockets

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    @asynccontextmanager
    async def demoLifespan(demoApp: FastAPI) -> AsyncGenerator[None]:
        await onStartup(demoApp)
        yield
        await onShutdown(demoApp)

    app = FastAPI(title="ICMIS WebSocket demo", lifespan=demoLifespan)
    app.state.settings = None
    app.include_router(router)

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=8765, log_level="warning"))
    serverTask = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)

    socketUrl = "ws://127.0.0.1:8765/ws/alerts/demoRanger?client_type=web&min_priority=ROUTINE"
    async with websockets.connect(socketUrl) as socket:
        print("Welcome:", json.loads(await socket.recv()))
        alert = AlertEvent(
            priority="CRITICAL",
            event_type="acoustic_anomaly",
            classification="gunshot_detected",
            metrics=AlertMetrics(cri_score=88.0, confidence=0.94),
            location=AlertLocation(lat=-2.334, lng=34.821),
        )
        delivered = await publishAlert(app, alert)
        print(f"Delivered to {delivered} connection(s).")
        print("Alert:", json.loads(await asyncio.wait_for(socket.recv(), timeout=5.0)))
        await socket.send(json.dumps({"type": "status"}))
        print("Status:", json.loads(await asyncio.wait_for(socket.recv(), timeout=5.0)))

    manager: ConnectionManager = app.state.connectionManager
    print("Manager:", json.dumps(manager.describe(), indent=2, default=str))
    server.should_exit = True
    await serverTask


if __name__ == "__main__":
    asyncio.run(main())