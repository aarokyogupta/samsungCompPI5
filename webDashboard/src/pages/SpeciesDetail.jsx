/**
 * SpeciesDetail - deep-dive analytics for one monitored population.
 *
 * Route: /species/:id
 * Combines the geospatial track, the historical CRI curve and the six-axis ecological balance so an
 * analyst can see where a population is, how its risk is moving and which domain is driving it.
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";
import { SubscoreRadarChart, TimeSeriesChart } from "../components/charts.jsx";
import { TrackingMap } from "../components/trackingMap.jsx";
import { EmptyState, ErrorNotice, LoadingBlock, Panel, formatClock, formatNumber } from "../components/uiKit.jsx";
import { fetchAnalyticsHistorical, fetchAnalyticsSpatial, fetchReports, fetchTelemetryData } from "../services/api.js";

// Configuration
const TRACK_FIX_LIMIT = 250;
// New coordinate telemetry is pulled on this cadence so the markers advance without a page reload
const TRACK_REFRESH_MS = 30000;
const SUBSCORE_AXES = [
    { key: "population_subscore", label: "Population" },
    { key: "habitat_subscore", label: "Habitat" },
    { key: "threat_subscore", label: "Threat" },
    { key: "climate_subscore", label: "Climate" },
    { key: "genetics_subscore", label: "Genetics" },
    { key: "behavior_subscore", label: "Behaviour" },
];

export default function SpeciesDetail() {
    const { id } = useParams();
    const speciesID = Number(id);

    const [historical, setHistorical] = useState(null);
    const [track, setTrack] = useState([]);
    const [clusters, setClusters] = useState([]);
    const [latestReport, setLatestReport] = useState(null);
    const [bounds, setBounds] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);

    const loadAnalytics = useCallback(async () => {
        try {
            const [historicalPayload, reportsPayload] = await Promise.all([
                fetchAnalyticsHistorical({ source: "risk", speciesID }),
                fetchReports({ limit: 1, speciesID }),
            ]);
            setHistorical(historicalPayload);
            setLatestReport(reportsPayload.items?.[0] || null);
            setError(null);
        } catch (failure) {
            setError(failure);
        } finally {
            setLoading(false);
        }
    }, [speciesID]);

    useEffect(() => {
        setLoading(true);
        loadAnalytics();
    }, [loadAnalytics]);

    const loadTrack = useCallback(async () => {
        try {
            const payload = await fetchTelemetryData({ readingType: "gps", limit: TRACK_FIX_LIMIT });
            // The species filter is applied client-side because /telemetry/data pages by reading type
            setTrack(payload.items.filter((fix) => fix.species_id === speciesID || !fix.species_id));
        } catch (failure) {
            setTrack([]);
        }
    }, [speciesID]);

    useEffect(() => {
        loadTrack();
        const timer = window.setInterval(loadTrack, TRACK_REFRESH_MS);
        return () => window.clearInterval(timer);
    }, [loadTrack]);

    // Viewport culling: the cluster query only re-runs when the operator actually moves the map
    useEffect(() => {
        if (!bounds) {
            return;
        }

        let cancelled = false;
        fetchAnalyticsSpatial({ bounds, layer: "gps" })
            .then((payload) => {
                if (!cancelled) {
                    setClusters(payload.features || []);
                }
            })
            .catch(() => {
                if (!cancelled) {
                    setClusters([]);
                }
            });

        return () => {
            cancelled = true;
        };
    }, [bounds]);

    const subscoreValues = useMemo(() => {
        if (latestReport?.scores?.subscores) {
            const scores = latestReport.scores.subscores;
            return [scores.population, scores.habitat, scores.threat, scores.climate, scores.genetics, scores.behavior];
        }

        if (!historical) {
            return null;
        }

        // Falling back to the newest non-null point of each historical series keeps the radar populated
        return SUBSCORE_AXES.map(({ key }) => {
            const series = historical.datasets.find((dataset) => dataset.key === key);
            if (!series) {
                return null;
            }
            const populated = series.data.filter((value) => value !== null && value !== undefined);
            return populated.length > 0 ? populated[populated.length - 1] : null;
        });
    }, [latestReport, historical]);

    const criSeries = useMemo(
        () => historical?.datasets.filter((series) => series.key === "conservation_risk_index") || [],
        [historical],
    );

    const subscoreSeries = useMemo(
        () => historical?.datasets.filter((series) => series.key.endsWith("_subscore")) || [],
        [historical],
    );

    if (loading) {
        return <LoadingBlock label="Aggregating population telemetry" />;
    }

    return (
        <div className="space-y-6">
            <header className="flex flex-wrap items-end justify-between gap-4">
                <div>
                    <p className="eyebrow">Population deep dive</p>
                    <h1 className="mt-1 text-2xl font-semibold">
                        {latestReport?.species?.common_name || `Species ${speciesID}`}
                    </h1>
                    <p className="mt-1 text-sm italic text-muted">{latestReport?.species?.scientific_name || "Scientific name unavailable"}</p>
                </div>
                {latestReport && (
                    <div className="flex gap-4 text-right">
                        <Headline label="CRI" value={formatNumber(latestReport.scores.conservation_risk_index)} />
                        <Headline label="Escalation" value={latestReport.scores.escalation_tier || `L${latestReport.scores.escalation_level}`} />
                        <Headline label="Ne" value={formatNumber(latestReport.scores.effective_population_size, 0)} />
                        <Headline label="Trend" value={latestReport.scores.population_trend || "\u2014"} />
                    </div>
                )}
            </header>

            <ErrorNotice error={error} onRetry={loadAnalytics} />

            <div className="grid gap-6 xl:grid-cols-3">
                <Panel
                    className="xl:col-span-2"
                    eyebrow="Geospatial tracking"
                    title="Movement vectors and activity clusters"
                    description="Green nodes are server-side clusters weighted by point count; the blue line is the recent collar track."
                >
                    <TrackingMap clusters={clusters} track={track} onViewportChange={setBounds} />
                    <p className="mt-3 text-xs text-muted">
                        {track.length} recent fixes &middot; {clusters.length} clusters in view
                        {track[0] && <span className="ml-2">&middot; last fix {formatClock(track[0].recorded_at)}</span>}
                    </p>
                </Panel>

                <Panel
                    eyebrow="Ecological balance"
                    title="Six-domain subscores"
                    description="Population, habitat, threat, climate, genetics and behaviour on a common 0-100 scale."
                >
                    {subscoreValues && subscoreValues.some((value) => value !== null) ? (
                        <SubscoreRadarChart labels={SUBSCORE_AXES.map((axis) => axis.label)} values={subscoreValues} />
                    ) : (
                        <EmptyState message="No scored assessment exists for this population yet." />
                    )}
                </Panel>
            </div>

            <Panel
                eyebrow="Historical risk"
                title="Conservation Risk Index over time"
                description="Hollow gaps mean the sensor was offline and the backend forward-filled the bucket."
            >
                {criSeries.length > 0 && historical.labels.length > 0 ? (
                    <TimeSeriesChart labels={historical.labels} datasets={criSeries} height={260} />
                ) : (
                    <EmptyState message="No risk history recorded for this population." />
                )}
            </Panel>

            <Panel eyebrow="Domain drivers" title="Subscore history" description="Which domain is pushing the composite index.">
                {subscoreSeries.length > 0 && historical.labels.length > 0 ? (
                    <TimeSeriesChart labels={historical.labels} datasets={subscoreSeries} height={300} />
                ) : (
                    <EmptyState message="No subscore history recorded." />
                )}
            </Panel>
        </div>
    );
}

function Headline({ label, value }) {
    return (
        <div>
            <p className="eyebrow">{label}</p>
            <p className="mt-1 font-mono text-xl font-semibold text-ink">{value}</p>
        </div>
    );
}
