/**
 * Centralised network client for the ICMIS backend.
 *
 * Every REST call and every WebSocket frame in the dashboard flows through this module so that
 * authentication, error normalisation and reconnection logic live in exactly one place.
 */

import axios from "axios";

// Configuration
// Vite inlines import.meta.env at build time, so the Pi's address is baked into the bundle we ship
const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");
const BUILD_API_KEY = import.meta.env.VITE_API_KEY || "";
const API_KEY_STORAGE_KEY = "icmisApiKey";
const API_PREFIX = "/api/v1";
const REQUEST_TIMEOUT_MS = 20000;

// The backend heartbeats every 30s; we allow two misses before treating the socket as dead
const SOCKET_HEARTBEAT_GRACE_MS = 75000;
const SOCKET_BACKOFF_STEPS_MS = [1000, 2000, 4000, 8000, 16000];
const SOCKET_MAX_BACKOFF_MS = 30000;

export const PRIORITY_ORDER = ["INFO", "ROUTINE", "ELEVATED", "HIGH", "CRITICAL"];

// REST client
export const httpClient = axios.create({
    baseURL: `${API_BASE_URL}${API_PREFIX}`,
    timeout: REQUEST_TIMEOUT_MS,
    headers: { Accept: "application/json" },
});

/**
 * The key typed into the Settings page wins over the build-time VITE_API_KEY, so operators can pair a
 * browser with the Pi (or rotate the key) without rebuilding the bundle.
 */
export const getApiKey = () => {
    try {
        return window.localStorage.getItem(API_KEY_STORAGE_KEY) || BUILD_API_KEY;
    } catch (failure) {
        return BUILD_API_KEY;
    }
};

export const setApiKey = (apiKey) => {
    try {
        if (apiKey) {
            window.localStorage.setItem(API_KEY_STORAGE_KEY, apiKey);
        } else {
            window.localStorage.removeItem(API_KEY_STORAGE_KEY);
        }
    } catch (failure) {
        // Private browsing can refuse storage; the build-time key still applies
    }
};

export const hasStoredApiKey = () => {
    try {
        return Boolean(window.localStorage.getItem(API_KEY_STORAGE_KEY));
    } catch (failure) {
        return false;
    }
};

// The API authenticates with a single shared key rather than per-user JWTs, so one header covers every call
httpClient.interceptors.request.use((requestConfig) => {
    const apiKey = getApiKey();
    if (apiKey) {
        requestConfig.headers["X-API-Key"] = apiKey;
    }
    return requestConfig;
});

/**
 * Normalises every failure into one shape. The backend answers with {error: {code, message, request_id}},
 * but network drops and timeouts never reach that envelope, so they are synthesised here.
 */
export class ApiError extends Error {
    constructor({ code, message, requestID, status, detail, payload }) {
        super(message);
        this.name = "ApiError";
        this.code = code;
        this.requestID = requestID || null;
        this.status = status || null;
        this.detail = detail || null;
        // The reports poller answers 500 with the whole job body, so the raw payload is kept for it
        this.payload = payload || null;
    }
}

httpClient.interceptors.response.use(
    (response) => response,
    (failure) => {
        const status = failure.response?.status ?? null;
        const envelope = failure.response?.data?.error;

        if (envelope) {
            return Promise.reject(
                new ApiError({
                    code: envelope.code,
                    message: envelope.message,
                    requestID: envelope.request_id,
                    status,
                    detail: envelope.detail ?? null,
                    payload: failure.response?.data ?? null,
                }),
            );
        }

        if (status === 401 || status === 403) {
            return Promise.reject(
                new ApiError({
                    code: "unauthorised",
                    message: "The API key was rejected. Enter the Pi's key under Settings \u2192 Connection.",
                    status,
                }),
            );
        }

        return Promise.reject(
            new ApiError({
                code: status ? `http_${status}` : "network_unreachable",
                message: status
                    ? `The Pi returned HTTP ${status}.`
                    : "The Pi is unreachable. Verify the edge node is powered and on the same network.",
                status,
            }),
        );
    },
);

// Convenience wrappers so pages never repeat ".data"
const getJson = async (path, params, options = {}) => {
    const response = await httpClient.get(path, { params, ...options });
    return response.data;
};

// Telemetry
export const fetchIngestStatus = () => getJson("/telemetry/ingest/status");

/**
 * Cursor pagination: pass the previous response's next_cursor rather than an offset so that newly
 * ingested rows never shift the page boundary underneath the operator.
 */
export const fetchTelemetryData = ({ readingType, limit = 50, cursor = null, bounds = null, startTime = null, endTime = null } = {}) =>
    getJson("/telemetry/data", {
        reading_type: readingType,
        limit,
        cursor: cursor || undefined,
        start_time: startTime || undefined,
        end_time: endTime || undefined,
        min_lat: bounds?.minLatitude,
        max_lat: bounds?.maxLatitude,
        min_lon: bounds?.minLongitude,
        max_lon: bounds?.maxLongitude,
    });

export const fetchTelemetryStats = ({ readingType, bucket = "hour", startTime = null, endTime = null } = {}) =>
    getJson("/telemetry/stats", {
        reading_type: readingType,
        bucket,
        start_time: startTime || undefined,
        end_time: endTime || undefined,
    });

// Analytics
export const fetchAnalyticsSummary = ({ windowHours = 24 } = {}) =>
    getJson("/analytics/summary", { window_hours: windowHours });

export const fetchAnalyticsHistorical = ({ source = "risk", bucket = null, startTime = null, endTime = null, speciesID = null } = {}) =>
    getJson("/analytics/historical", {
        source,
        bucket: bucket || undefined,
        start_time: startTime || undefined,
        end_time: endTime || undefined,
        species_id: speciesID || undefined,
    });

export const fetchAnalyticsRadar = ({ limit = 6, startTime = null, endTime = null } = {}) =>
    getJson("/analytics/radar", { limit, start_time: startTime || undefined, end_time: endTime || undefined });

/**
 * Viewport culling: the map hands us its current bounding box so the Pi only clusters the points
 * that are physically on screen instead of shipping the whole movement history.
 */
export const fetchAnalyticsSpatial = ({ bounds, layer = "gps", startTime = null, endTime = null } = {}) =>
    getJson("/analytics/spatial", {
        north: bounds.north,
        south: bounds.south,
        east: bounds.east,
        west: bounds.west,
        layer,
        start_time: startTime || undefined,
        end_time: endTime || undefined,
    });

// Reports
export const fetchReports = ({ limit = 20, offset = 0, speciesID = null, startDate = null, endDate = null, threatTier = null } = {}) =>
    getJson("/reports", {
        limit,
        offset,
        species_id: speciesID || undefined,
        start_date: startDate || undefined,
        end_date: endDate || undefined,
        threat_tier: threatTier || undefined,
    });

export const fetchReportDetail = (reportID, { includeSources = true } = {}) =>
    getJson(`/reports/${reportID}`, { include_sources: includeSources });

export const triggerReport = async (payload) => {
    const response = await httpClient.post("/reports/generate", payload);
    return response.data;
};

// redirect=false keeps the poll as a plain JSON status check instead of a 303 the browser would chase
export const fetchReportJobStatus = (jobID) => getJson(`/reports/status/${jobID}`, { redirect: false });

export const fetchReportJobs = () => getJson("/reports/jobs");

// System
export const fetchHealth = () => getJson("/health");
export const fetchHardware = () => getJson("/system/hardware");
export const fetchSocketStatus = () => getJson("/ws/status");

// Settings (saved to config.yaml on the Pi, then applied by restarting only the affected services)
export const fetchSettingsSchema = () => getJson("/settings/schema");
export const fetchSettings = () => getJson("/settings");
export const fetchSettingsStatus = () => getJson("/settings/status");
export const fetchSettingsBackups = () => getJson("/settings/backups");

// The revision guards against two operators overwriting each other; a stale one answers 409
export const saveSettings = async (values, revision) => {
    const response = await httpClient.patch("/settings", { values, revision }, { timeout: 240000 });
    return response.data;
};

export const applySettings = async (services = null) => {
    const response = await httpClient.post("/settings/apply", services ? { services } : {});
    return response.data;
};

export const rollbackSettings = async (name) => {
    const response = await httpClient.post("/settings/rollback", { name }, { timeout: 240000 });
    return response.data;
};

export const fetchSecrets = () => getJson("/settings/secrets");

export const saveSecret = async (name, value) => {
    const response = await httpClient.put("/settings/secrets", { name, value });
    return response.data;
};

/**
 * Resilient wrapper around the browser WebSocket.
 *
 * Responsibilities beyond the raw socket:
 *   - attaches the API key as a query parameter, because browsers cannot set headers on the upgrade
 *   - answers the server's {"type": "ping"} with {"type": "pong"}; two missed pings flush the connection
 *   - reconnects with exponential backoff (1s, 2s, 4s, 8s, 16s, capped at 30s) when the Pi reboots
 *   - fans incoming frames out to any number of listeners so several components can share one socket
 */
export class AlertSocket {
    constructor({ clientID, clientType = "web", zones = [], minPriority = "INFO" } = {}) {
        this.clientID = clientID || `web-${Math.random().toString(16).slice(2, 10)}`;
        this.clientType = clientType;
        this.zones = zones;
        this.minPriority = minPriority;

        this.socket = null;
        this.listeners = new Set();
        this.stateListeners = new Set();
        this.attempt = 0;
        this.reconnectTimer = null;
        this.watchdogTimer = null;
        this.closedByCaller = false;
        this.state = "idle";
    }

    get url() {
        const httpOrigin = API_BASE_URL || window.location.origin;
        const socketOrigin = httpOrigin.replace(/^http/, "ws");
        const parameters = new URLSearchParams({ client_type: this.clientType, min_priority: this.minPriority });

        const apiKey = getApiKey();
        if (apiKey) {
            parameters.set("token", apiKey);
        }
        this.zones.forEach((zone) => parameters.append("zone", zone));

        return `${socketOrigin}${API_PREFIX}/ws/alerts/${encodeURIComponent(this.clientID)}?${parameters.toString()}`;
    }

    onFrame(listener) {
        this.listeners.add(listener);
        return () => this.listeners.delete(listener);
    }

    onStateChange(listener) {
        this.stateListeners.add(listener);
        listener(this.state);
        return () => this.stateListeners.delete(listener);
    }

    setState(state) {
        this.state = state;
        this.stateListeners.forEach((listener) => listener(state));
    }

    connect() {
        if (this.socket && (this.socket.readyState === WebSocket.OPEN || this.socket.readyState === WebSocket.CONNECTING)) {
            return;
        }

        this.closedByCaller = false;
        this.setState(this.attempt === 0 ? "connecting" : "reconnecting");

        try {
            this.socket = new WebSocket(this.url);
        } catch (failure) {
            this.scheduleReconnect();
            return;
        }

        this.socket.onopen = () => {
            this.attempt = 0;
            this.setState("open");
            this.armWatchdog();
        };

        this.socket.onmessage = (event) => this.handleMessage(event);

        this.socket.onclose = () => {
            this.clearWatchdog();
            if (!this.closedByCaller) {
                this.setState("offline");
                this.scheduleReconnect();
            }
        };

        // onerror always precedes onclose in every browser, so reconnection is driven from onclose alone
        this.socket.onerror = () => this.socket?.close();
    }

    handleMessage(event) {
        let frame;
        try {
            frame = JSON.parse(event.data);
        } catch (failure) {
            return;
        }

        // Any traffic proves the link is alive, so the missed-heartbeat watchdog restarts on every frame
        this.armWatchdog();

        if (frame.type === "ping") {
            this.send({ type: "pong" });
            return;
        }

        this.listeners.forEach((listener) => listener(frame));
    }

    send(payload) {
        if (this.socket?.readyState === WebSocket.OPEN) {
            this.socket.send(JSON.stringify(payload));
        }
    }

    subscribe({ zones = [], minPriority = null } = {}) {
        this.zones = zones;
        if (minPriority) {
            this.minPriority = minPriority;
        }
        this.send({ type: "subscribe", zones, min_priority: this.minPriority });
    }

    unsubscribe() {
        this.zones = [];
        this.send({ type: "unsubscribe" });
    }

    armWatchdog() {
        this.clearWatchdog();
        // A collar that drives out of cell range never sends a close frame, so silence is the only signal
        this.watchdogTimer = window.setTimeout(() => {
            this.socket?.close();
        }, SOCKET_HEARTBEAT_GRACE_MS);
    }

    clearWatchdog() {
        if (this.watchdogTimer) {
            window.clearTimeout(this.watchdogTimer);
            this.watchdogTimer = null;
        }
    }

    scheduleReconnect() {
        if (this.reconnectTimer) {
            return;
        }

        const delay = SOCKET_BACKOFF_STEPS_MS[Math.min(this.attempt, SOCKET_BACKOFF_STEPS_MS.length - 1)] || SOCKET_MAX_BACKOFF_MS;
        this.attempt += 1;

        this.reconnectTimer = window.setTimeout(() => {
            this.reconnectTimer = null;
            this.connect();
        }, Math.min(delay, SOCKET_MAX_BACKOFF_MS));
    }

    close() {
        this.closedByCaller = true;
        this.clearWatchdog();

        if (this.reconnectTimer) {
            window.clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }

        this.socket?.close();
        this.socket = null;
        this.setState("closed");
    }
}

export default httpClient;
