/**
 * Shared presentational primitives used across all three operational views.
 */

import { Component } from "react";
import { priorityRank } from "../context/alertContext.jsx";

// Priority colouring is shared by the live feed, the toast overlay and the report tier badges
const PRIORITY_STYLES = {
    CRITICAL: "border-danger/60 bg-danger/15 text-danger",
    HIGH: "border-caution/60 bg-caution/15 text-caution",
    ELEVATED: "border-accentCool/60 bg-accentCool/15 text-accentCool",
    ROUTINE: "border-hairline bg-panelRaised text-muted",
    INFO: "border-hairline bg-panelRaised text-muted",
};

export const priorityStyle = (priority) => PRIORITY_STYLES[String(priority || "").toUpperCase()] || PRIORITY_STYLES.INFO;

export const formatNumber = (value, digits = 1) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return "\u2014";
    }
    const numeric = Number(value);
    return Number.isInteger(numeric) ? numeric.toLocaleString() : numeric.toFixed(digits);
};

export const formatClock = (isoTimestamp) => {
    if (!isoTimestamp) {
        return "\u2014";
    }
    const moment = new Date(isoTimestamp);
    return Number.isNaN(moment.getTime()) ? "\u2014" : moment.toLocaleString();
};

export function Panel({ eyebrow, title, description, actions, children, className = "" }) {
    return (
        <section className={`panel p-5 ${className}`}>
            {(eyebrow || title || actions) && (
                <header className="mb-4 flex items-start justify-between gap-4">
                    <div>
                        {eyebrow && <p className="eyebrow">{eyebrow}</p>}
                        {title && <h2 className="mt-1 text-lg font-semibold text-ink">{title}</h2>}
                        {description && <p className="mt-1 text-sm text-muted">{description}</p>}
                    </div>
                    {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
                </header>
            )}
            {children}
        </section>
    );
}

/**
 * A summary card straight from GET /analytics/summary. The backend already computed the delta against
 * the previous window, so the card only has to choose an arrow and a colour.
 */
export function MetricCard({ card }) {
    const trendStyle =
        card.trend === "up" ? "text-accent" : card.trend === "down" ? "text-danger" : "text-muted";
    const trendGlyph = card.trend === "up" ? "\u25B2" : card.trend === "down" ? "\u25BC" : "\u25CF";

    return (
        <article className="panelRaised flex flex-col justify-between p-4">
            <p className="eyebrow">{card.label}</p>
            <p className="metricValue mt-2">
                {formatNumber(card.value)}
                {card.unit && <span className="ml-1 text-sm font-normal text-muted">{card.unit}</span>}
            </p>
            <p className={`mt-2 text-xs ${trendStyle}`}>
                <span className="mr-1">{trendGlyph}</span>
                {card.delta_text || "No comparison available"}
            </p>
        </article>
    );
}

export function StatusPill({ state }) {
    const label = { open: "Live", connecting: "Connecting", reconnecting: "Reconnecting", offline: "Offline", closed: "Closed" }[state] || "Idle";
    const tone = state === "open" ? "border-accent/60 text-accent" : state === "offline" || state === "closed" ? "border-danger/60 text-danger" : "border-caution/60 text-caution";

    return (
        <span className={`pill ${tone}`}>
            <span className={`h-1.5 w-1.5 rounded-full ${state === "open" ? "bg-accent" : "bg-current"}`} />
            {label}
        </span>
    );
}

export function LoadingBlock({ label = "Loading" }) {
    return (
        <div className="flex items-center gap-3 py-8 text-sm text-muted">
            <span className="h-3 w-3 animate-ping rounded-full bg-accent" />
            {label}
            {"\u2026"}
        </div>
    );
}

export function EmptyState({ message }) {
    return <p className="rounded-xl border border-dashed border-hairline px-4 py-8 text-center text-sm text-muted">{message}</p>;
}

export function ErrorNotice({ error, onRetry }) {
    if (!error) {
        return null;
    }

    return (
        <div className="rounded-xl border border-danger/50 bg-danger/10 px-4 py-3 text-sm text-danger">
            <p className="font-semibold">{error.message}</p>
            {error.requestID && <p className="mt-1 font-mono text-[11px] opacity-80">request_id: {error.requestID}</p>}
            {onRetry && (
                <button type="button" onClick={onRetry} className="mt-2 rounded-lg border border-danger/50 px-3 py-1 text-xs hover:bg-danger/20">
                    Retry
                </button>
            )}
        </div>
    );
}

/**
 * A render failure inside one chart must not blank the whole command centre, so each view is wrapped.
 */
export class ViewBoundary extends Component {
    constructor(props) {
        super(props);
        this.state = { failure: null };
    }

    static getDerivedStateFromError(failure) {
        return { failure };
    }

    render() {
        if (this.state.failure) {
            return (
                <div className="panel m-6 p-6">
                    <p className="eyebrow text-danger">Render fault</p>
                    <h2 className="mt-1 text-lg font-semibold">This view could not be drawn.</h2>
                    <p className="mt-2 font-mono text-xs text-muted">{String(this.state.failure)}</p>
                </div>
            );
        }
        return this.props.children;
    }
}

export { priorityRank };
