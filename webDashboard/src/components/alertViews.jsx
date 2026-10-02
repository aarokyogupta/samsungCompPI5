/**
 * Real-time alert presentation: the scrolling live feed and the global toast overlay.
 */

import { useAlerts } from "../context/alertContext.jsx";
import { formatClock, priorityStyle } from "./uiKit.jsx";

const EVENT_GLYPHS = {
    acoustic_anomaly: "\u266B",
    vision_detection: "\u25C9",
    gps_anomaly: "\u2316",
    risk_escalation: "\u26A0",
    system: "\u2699",
};

const eventGlyph = (eventType) => EVENT_GLYPHS[eventType] || "\u25CF";

export function AlertRow({ alert, compact = false }) {
    const metrics = alert.metrics || {};
    const location = alert.location || {};

    return (
        <li className={`flex gap-3 border-b border-hairline/60 py-3 last:border-0 ${compact ? "text-xs" : "text-sm"}`}>
            <span className={`pill h-fit shrink-0 ${priorityStyle(alert.priority)}`}>
                <span>{eventGlyph(alert.event_type)}</span>
                {alert.priority}
            </span>
            <div className="min-w-0 flex-1">
                <p className="truncate font-medium text-ink">{alert.classification || alert.event_type}</p>
                <p className="mt-0.5 text-xs text-muted">
                    {formatClock(alert.timestamp)}
                    {location.zone && <span className="ml-2 font-mono">{location.zone}</span>}
                    {alert.replayed && <span className="ml-2 text-caution">replayed</span>}
                </p>
                {(metrics.cri_score != null || metrics.confidence != null) && (
                    <p className="mt-1 font-mono text-[11px] text-muted">
                        {metrics.cri_score != null && <span className="mr-3">CRI {metrics.cri_score}</span>}
                        {metrics.confidence != null && <span>conf {(metrics.confidence * 100).toFixed(0)}%</span>}
                    </p>
                )}
            </div>
        </li>
    );
}

export function LiveFeed({ limit = 25 }) {
    const { alerts } = useAlerts();
    const visible = alerts.slice(0, limit);

    if (visible.length === 0) {
        return <p className="py-8 text-center text-sm text-muted">No events since this session opened. The edge network is quiet.</p>;
    }

    return (
        <ul className="max-h-[420px] overflow-y-auto pr-1">
            {visible.map((alert, index) => (
                <AlertRow key={`${alert.event_id}-${index}`} alert={alert} />
            ))}
        </ul>
    );
}

/**
 * Fixed overlay that survives navigation because it renders from the root-level alert context.
 * Only ELEVATED and above reach this layer; quieter traffic stays in the feed.
 */
export function ToastOverlay() {
    const { toasts, dismissToast } = useAlerts();

    if (toasts.length === 0) {
        return null;
    }

    return (
        <div className="pointer-events-none fixed right-6 top-6 z-50 flex w-80 flex-col gap-3">
            {toasts.map((alert) => (
                <div
                    key={alert.event_id}
                    className={`pointer-events-auto animate-slideIn rounded-xl border bg-panel p-4 shadow-panel ${priorityStyle(alert.priority)}`}
                >
                    <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                            <p className="eyebrow">{alert.priority} &middot; {alert.event_type}</p>
                            <p className="mt-1 truncate text-sm font-semibold text-ink">{alert.classification}</p>
                        </div>
                        <button
                            type="button"
                            onClick={() => dismissToast(alert.event_id)}
                            className="shrink-0 rounded-md px-1.5 text-muted hover:text-ink"
                            aria-label="Dismiss alert"
                        >
                            {"\u2715"}
                        </button>
                    </div>
                    <p className="mt-2 font-mono text-[11px] text-muted">
                        {alert.location?.zone || "unzoned"} &middot; {formatClock(alert.timestamp)}
                    </p>
                    {/* The CRI and confidence decide whether an operator acts now or triages later */}
                    {(alert.metrics?.cri_score != null || alert.metrics?.confidence != null) && (
                        <p className="mt-1 font-mono text-[11px] text-muted">
                            {alert.metrics?.cri_score != null && <span>CRI {alert.metrics.cri_score}</span>}
                            {alert.metrics?.cri_score != null && alert.metrics?.confidence != null && " · "}
                            {alert.metrics?.confidence != null && <span>{Math.round(alert.metrics.confidence * 100)}% confidence</span>}
                        </p>
                    )}
                </div>
            ))}
        </div>
    );
}
