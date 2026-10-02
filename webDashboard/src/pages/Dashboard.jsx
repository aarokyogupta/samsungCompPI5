/**
 * Dashboard - the command centre.
 *
 * Default route. Answers the macro question "what is happening across the reserve right now?" using
 * the pre-aggregated summary cards, Pi hardware health and the live WebSocket feed.
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { LiveFeed } from "../components/alertViews.jsx";
import { TimeSeriesChart, ZoneComparisonChart } from "../components/charts.jsx";
import { EmptyState, ErrorNotice, LoadingBlock, MetricCard, Panel, StatusPill, formatNumber } from "../components/uiKit.jsx";
import { useAlerts } from "../context/alertContext.jsx";
import {
    fetchAnalyticsHistorical,
    fetchAnalyticsRadar,
    fetchAnalyticsSummary,
    fetchHardware,
    fetchIngestStatus,
} from "../services/api.js";

// Configuration
const WINDOW_OPTIONS = [
    { label: "6h", hours: 6 },
    { label: "24h", hours: 24 },
    { label: "7d", hours: 168 },
    { label: "30d", hours: 720 },
];
// The summary endpoint caches for 5 minutes server-side; polling faster only burns Pi cycles
const SUMMARY_REFRESH_MS = 60000;
const HARDWARE_REFRESH_MS = 20000;
const PRIMARY_CARD_KEYS = ["total_detections", "threat_alerts", "mean_cri", "critical_cells"];

const cardsByDomain = (cards, domain) => cards.filter((card) => card.domain === domain);

export default function Dashboard() {
    const { connectionState, alerts } = useAlerts();
    const [windowHours, setWindowHours] = useState(24);
    const [summary, setSummary] = useState(null);
    const [hardware, setHardware] = useState(null);
    const [ingest, setIngest] = useState(null);
    const [historical, setHistorical] = useState(null);
    const [radar, setRadar] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);

    const loadSummary = useCallback(async () => {
        try {
            const [summaryPayload, historicalPayload, radarPayload] = await Promise.all([
                fetchAnalyticsSummary({ windowHours }),
                fetchAnalyticsHistorical({ source: "risk" }),
                fetchAnalyticsRadar({ limit: 6 }),
            ]);
            setSummary(summaryPayload);
            setHistorical(historicalPayload);
            setRadar(radarPayload);
            setError(null);
        } catch (failure) {
            setError(failure);
        } finally {
            setLoading(false);
        }
    }, [windowHours]);

    useEffect(() => {
        setLoading(true);
        loadSummary();
        const timer = window.setInterval(loadSummary, SUMMARY_REFRESH_MS);
        return () => window.clearInterval(timer);
    }, [loadSummary]);

    // Hardware and queue depth are cheap and volatile, so they poll on their own faster cadence
    useEffect(() => {
        const poll = async () => {
            // Settled rather than all: the hardware probe can time out on the LLM check while the
            // ingest counters are perfectly healthy, and one stale panel should not blank the other
            const [hardwareResult, ingestResult] = await Promise.allSettled([fetchHardware(), fetchIngestStatus()]);
            setHardware(hardwareResult.status === "fulfilled" ? hardwareResult.value : null);
            setIngest(ingestResult.status === "fulfilled" ? ingestResult.value : null);
        };

        poll();
        const timer = window.setInterval(poll, HARDWARE_REFRESH_MS);
        return () => window.clearInterval(timer);
    }, []);

    const primaryCards = useMemo(() => {
        if (!summary) {
            return [];
        }
        const lookup = new Map(summary.cards.map((card) => [card.key, card]));
        return PRIMARY_CARD_KEYS.map((key) => lookup.get(key)).filter(Boolean);
    }, [summary]);

    const criSeries = useMemo(() => {
        if (!historical) {
            return null;
        }
        return historical.datasets.filter((series) => series.key === "conservation_risk_index");
    }, [historical]);

    return (
        <div className="space-y-6">
            <header className="flex flex-wrap items-end justify-between gap-4">
                <div>
                    <p className="eyebrow">Operations overview</p>
                    <h1 className="mt-1 text-2xl font-semibold">Command Centre</h1>
                    <p className="mt-1 text-sm text-muted">
                        Edge network status for the last {windowHours >= 48 ? `${Math.round(windowHours / 24)} days` : `${windowHours} hours`}.
                    </p>
                </div>
                <div className="flex items-center gap-2">
                    <StatusPill state={connectionState} />
                    <div className="flex overflow-hidden rounded-xl border border-hairline">
                        {WINDOW_OPTIONS.map((option) => (
                            <button
                                key={option.hours}
                                type="button"
                                onClick={() => setWindowHours(option.hours)}
                                className={`px-3 py-1.5 text-xs font-semibold transition ${
                                    windowHours === option.hours ? "bg-accent text-canvas" : "text-muted hover:bg-panelRaised"
                                }`}
                            >
                                {option.label}
                            </button>
                        ))}
                    </div>
                </div>
            </header>

            <ErrorNotice error={error} onRetry={loadSummary} />

            {loading && !summary ? (
                <LoadingBlock label="Contacting the edge node" />
            ) : (
                <>
                    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                        {primaryCards.map((card) => (
                            <MetricCard key={card.key} card={card} />
                        ))}
                    </div>

                    <div className="grid gap-6 xl:grid-cols-3">
                        <Panel
                            className="xl:col-span-2"
                            eyebrow="Conservation risk"
                            title="Risk index trend"
                            description="Composite CRI across every assessed grid cell, downsampled by the backend to match the range."
                        >
                            {criSeries && criSeries.length > 0 && historical.labels.length > 0 ? (
                                <TimeSeriesChart labels={historical.labels} datasets={criSeries} />
                            ) : (
                                <EmptyState message="No risk assessments have been generated for this period yet." />
                            )}
                        </Panel>

                        <Panel eyebrow="Real time" title="Live event feed" description="Pushed over the WebSocket the moment the edge AI classifies an event.">
                            <LiveFeed limit={20} />
                        </Panel>
                    </div>

                    <div className="grid gap-6 xl:grid-cols-3">
                        <Panel
                            className="xl:col-span-2"
                            eyebrow="Zone comparison"
                            title="Normalised ecological metrics"
                            description="Every metric is min-max scaled to 0-100 so counts and degrees can share one axis."
                        >
                            {radar && radar.datasets.length > 0 ? (
                                <ZoneComparisonChart labels={radar.labels} datasets={radar.datasets} />
                            ) : (
                                <EmptyState message="Not enough zone activity to compare sectors yet." />
                            )}
                        </Panel>

                        <Panel eyebrow="Edge node" title="System health" description="Live Raspberry Pi 5 telemetry.">
                            {hardware || ingest ? (
                                <dl className="space-y-3 text-sm">
                                    {hardware && (
                                        <>
                                            <HealthRow label="CPU temperature" value={`${formatNumber(hardware.cpu_temperature_c)} \u00B0C`} alarm={hardware.thermal_throttled} />
                                            <HealthRow label="Free memory" value={`${formatNumber(hardware.free_ram_gb, 2)} GB`} alarm={!hardware.memory_sufficient} />
                                            <HealthRow label="Local model" value={hardware.local_model_resident ? "Resident" : "Cold"} alarm={!hardware.local_viable} />
                                            <HealthRow label="Network" value={hardware.network_available ? "Online" : "Isolated"} alarm={!hardware.network_available} />
                                        </>
                                    )}
                                    {ingest && (
                                        <>
                                            <HealthRow
                                                label="Ingest queue"
                                                value={`${ingest.queue_depth} / ${ingest.queue_capacity}`}
                                                alarm={ingest.queue_depth > ingest.queue_capacity * 0.8}
                                            />
                                            <HealthRow label="Readings stored" value={formatNumber(ingest.counters.stored)} />
                                            <HealthRow label="Threats dispatched" value={formatNumber(ingest.counters.threats)} alarm={ingest.counters.threats > 0} />
                                        </>
                                    )}
                                </dl>
                            ) : (
                                <EmptyState message="Hardware probe unavailable." />
                            )}
                        </Panel>
                    </div>

                    <Panel
                        eyebrow="Domain breakdown"
                        title="All indicators"
                        description="Every card the analytics engine computes, grouped by the subsystem that produced it."
                        actions={
                            <Link to="/reports" className="rounded-lg border border-hairline px-3 py-1.5 text-xs text-muted hover:border-accent/50 hover:text-ink">
                                AI diagnostics
                            </Link>
                        }
                    >
                        {summary ? (
                            <div className="space-y-6">
                                {["detections", "gps", "environmental", "risk"].map((domain) => {
                                    const domainCards = cardsByDomain(summary.cards, domain);
                                    if (domainCards.length === 0) {
                                        return null;
                                    }
                                    return (
                                        <div key={domain}>
                                            <p className="eyebrow mb-3">{domain}</p>
                                            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                                                {domainCards.map((card) => (
                                                    <MetricCard key={card.key} card={card} />
                                                ))}
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        ) : (
                            <LoadingBlock />
                        )}
                    </Panel>

                    <p className="text-center text-xs text-muted">
                        {alerts.length} event{alerts.length === 1 ? "" : "s"} received this session
                        {summary?.cached && <span className="ml-2">&middot; summary served from cache</span>}
                    </p>
                </>
            )}
        </div>
    );
}

function HealthRow({ label, value, alarm = false }) {
    return (
        <div className="flex items-center justify-between border-b border-hairline/60 pb-2 last:border-0">
            <dt className="text-muted">{label}</dt>
            <dd className={`font-mono ${alarm ? "text-danger" : "text-ink"}`}>{value}</dd>
        </div>
    );
}
