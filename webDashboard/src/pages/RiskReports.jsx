/**
 * RiskReports - AI diagnostics library and detail view.
 *
 * Routes: /reports (library) and /reports/:reportID (single analysis).
 * The generate action follows the backend's trigger-and-poll contract: POST returns 202 with a
 * job_id, and this page polls /reports/status/{job_id} until the LangGraph chain finishes.
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { InterventionChecklist, NarrativeMarkdown } from "../components/narrative.jsx";
import { EmptyState, ErrorNotice, LoadingBlock, Panel, formatClock, formatNumber, priorityStyle } from "../components/uiKit.jsx";
import { fetchReportDetail, fetchReportJobStatus, fetchReports, triggerReport } from "../services/api.js";

// Configuration
const PAGE_SIZE = 20;
// The frontend contract in the spec is a 5 second poll while the chain assembles the report
const JOB_POLL_MS = 5000;
// A LangGraph diagnostic can legitimately run for minutes, but past this we stop nagging the Pi
const JOB_POLL_TIMEOUT_MS = 600000;
const THREAT_TIERS = ["", "LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL"];

const tierStyle = (tier) => priorityStyle(tier === "MODERATE" ? "ROUTINE" : tier === "LOW" ? "INFO" : tier);

export default function RiskReports() {
    const { reportID } = useParams();
    return reportID ? <ReportDetail reportID={reportID} /> : <ReportLibrary />;
}

function ReportLibrary() {
    const navigate = useNavigate();
    const [filters, setFilters] = useState({ speciesID: "", startDate: "", endDate: "", threatTier: "" });
    const [offset, setOffset] = useState(0);
    const [page, setPage] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);
    const [job, setJob] = useState(null);
    const [triggerOpen, setTriggerOpen] = useState(false);
    // The graph assesses one H3 cell, so a trigger needs a species and a point as well as a window
    const [trigger, setTrigger] = useState({ speciesID: "", latitude: "", longitude: "", lookbackDays: "30" });

    const loadPage = useCallback(async () => {
        setLoading(true);
        try {
            const payload = await fetchReports({
                limit: PAGE_SIZE,
                offset,
                speciesID: filters.speciesID || null,
                startDate: filters.startDate || null,
                endDate: filters.endDate || null,
                threatTier: filters.threatTier || null,
            });
            setPage(payload);
            setError(null);
        } catch (failure) {
            setError(failure);
        } finally {
            setLoading(false);
        }
    }, [offset, filters]);

    useEffect(() => {
        loadPage();
    }, [loadPage]);

    const triggerReady =
        Number(trigger.speciesID) > 0 &&
        trigger.latitude !== "" &&
        trigger.longitude !== "" &&
        Math.abs(Number(trigger.latitude)) <= 90 &&
        Math.abs(Number(trigger.longitude)) <= 180;

    const startGeneration = async () => {
        if (!triggerReady) {
            return;
        }
        try {
            // ReportTriggerRequest forbids extras, so only the resolved scope fields are sent
            const acknowledgement = await triggerReport({
                species_id: Number(trigger.speciesID),
                latitude: Number(trigger.latitude),
                longitude: Number(trigger.longitude),
                lookback_days: Number(trigger.lookbackDays) || 30,
            });
            setJob({ status: "queued", progress: 0, ...acknowledgement });
            setTriggerOpen(false);
            setError(null);
        } catch (failure) {
            setError(failure);
        }
    };

    // Trigger-and-poll: the POST only acknowledges, so completion is discovered by polling the job
    useEffect(() => {
        if (!job?.job_id || ["completed", "failed", "timed_out"].includes(job.status)) {
            return undefined;
        }

        const startedAt = Date.now();
        const timer = window.setInterval(async () => {
            if (Date.now() - startedAt > JOB_POLL_TIMEOUT_MS) {
                setJob((current) => ({ ...current, status: "timed_out" }));
                return;
            }

            try {
                const status = await fetchReportJobStatus(job.job_id);
                setJob((current) => ({ ...current, ...status }));

                if (status.status === "completed") {
                    if (status.report_id) {
                        navigate(`/reports/${status.report_id}`);
                    } else {
                        loadPage();
                    }
                }
            } catch (failure) {
                // A failed chain answers 500 with the full job body, so it is merged rather than discarded
                setJob((current) => ({ ...current, ...(failure.payload || {}), status: "failed", error: failure.message }));
            }
        }, JOB_POLL_MS);

        return () => window.clearInterval(timer);
    }, [job, navigate, loadPage]);

    const updateFilter = (key, value) => {
        setOffset(0);
        setFilters((current) => ({ ...current, [key]: value }));
    };

    return (
        <div className="space-y-6">
            <header className="flex flex-wrap items-end justify-between gap-4">
                <div>
                    <p className="eyebrow">Explainable AI</p>
                    <h1 className="mt-1 text-2xl font-semibold">Risk Diagnostics Library</h1>
                    <p className="mt-1 text-sm text-muted">Historical LangGraph analyses with root-cause narratives and intervention plans.</p>
                </div>
                <button
                    type="button"
                    onClick={() => setTriggerOpen((open) => !open)}
                    className="rounded-xl bg-accent px-4 py-2 text-sm font-semibold text-canvas transition hover:brightness-110"
                >
                    {triggerOpen ? "Cancel" : "Generate new analysis"}
                </button>
            </header>

            {triggerOpen && (
                <Panel eyebrow="LangGraph pipeline" title="Trigger a diagnostic run">
                    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
                        <FilterField label="Species ID">
                            <input
                                type="number"
                                min="1"
                                value={trigger.speciesID}
                                onChange={(event) => setTrigger((current) => ({ ...current, speciesID: event.target.value }))}
                                className="fieldInput"
                                placeholder="Required"
                            />
                        </FilterField>
                        <FilterField label="Latitude">
                            <input
                                type="number"
                                step="0.0001"
                                value={trigger.latitude}
                                onChange={(event) => setTrigger((current) => ({ ...current, latitude: event.target.value }))}
                                className="fieldInput"
                                placeholder="-2.3340"
                            />
                        </FilterField>
                        <FilterField label="Longitude">
                            <input
                                type="number"
                                step="0.0001"
                                value={trigger.longitude}
                                onChange={(event) => setTrigger((current) => ({ ...current, longitude: event.target.value }))}
                                className="fieldInput"
                                placeholder="34.8210"
                            />
                        </FilterField>
                        <FilterField label="Lookback (days)">
                            <input
                                type="number"
                                min="1"
                                value={trigger.lookbackDays}
                                onChange={(event) => setTrigger((current) => ({ ...current, lookbackDays: event.target.value }))}
                                className="fieldInput"
                            />
                        </FilterField>
                        <div className="flex items-end">
                            <button
                                type="button"
                                onClick={startGeneration}
                                disabled={!triggerReady}
                                className="w-full rounded-xl bg-accent px-4 py-2 text-sm font-semibold text-canvas transition hover:brightness-110 disabled:opacity-40"
                            >
                                Run diagnostic
                            </button>
                        </div>
                    </div>
                    <p className="mt-3 text-xs text-muted">
                        The point is snapped to an H3 cell server-side; a full chain typically takes 30 seconds to several minutes.
                    </p>
                </Panel>
            )}

            {job && <JobBanner job={job} />}
            <ErrorNotice error={error} onRetry={loadPage} />

            <Panel eyebrow="Filters" title="Narrow the library">
                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                    <FilterField label="Species ID">
                        <input
                            type="number"
                            min="1"
                            value={filters.speciesID}
                            onChange={(event) => updateFilter("speciesID", event.target.value)}
                            className="fieldInput"
                            placeholder="Any"
                        />
                    </FilterField>
                    <FilterField label="From">
                        <input type="date" value={filters.startDate} onChange={(event) => updateFilter("startDate", event.target.value)} className="fieldInput" />
                    </FilterField>
                    <FilterField label="To">
                        <input type="date" value={filters.endDate} onChange={(event) => updateFilter("endDate", event.target.value)} className="fieldInput" />
                    </FilterField>
                    <FilterField label="Threat tier">
                        <select value={filters.threatTier} onChange={(event) => updateFilter("threatTier", event.target.value)} className="fieldInput">
                            {THREAT_TIERS.map((tier) => (
                                <option key={tier || "any"} value={tier}>
                                    {tier || "Any tier"}
                                </option>
                            ))}
                        </select>
                    </FilterField>
                </div>
            </Panel>

            {loading && !page ? (
                <LoadingBlock label="Querying the diagnostics archive" />
            ) : page && page.items.length > 0 ? (
                <>
                    <div className="grid gap-4 xl:grid-cols-2">
                        {page.items.map((report) => (
                            <ReportCard key={report.id} report={report} />
                        ))}
                    </div>

                    <div className="flex items-center justify-between text-sm text-muted">
                        <span>
                            Showing {offset + 1}-{offset + page.count} of {page.total}
                        </span>
                        <div className="flex gap-2">
                            <button
                                type="button"
                                disabled={offset === 0}
                                onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                                className="rounded-lg border border-hairline px-3 py-1.5 text-xs disabled:opacity-40"
                            >
                                Previous
                            </button>
                            <button
                                type="button"
                                disabled={!page.has_more}
                                onClick={() => setOffset(offset + PAGE_SIZE)}
                                className="rounded-lg border border-hairline px-3 py-1.5 text-xs disabled:opacity-40"
                            >
                                Next
                            </button>
                        </div>
                    </div>
                </>
            ) : (
                <EmptyState message="No diagnostics match these filters. Generate a new analysis to populate the archive." />
            )}
        </div>
    );
}

function ReportDetail({ reportID }) {
    const [report, setReport] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);

    const load = useCallback(async () => {
        setLoading(true);
        try {
            // include_sources eagerly hydrates the sensor rows that triggered the assessment
            setReport(await fetchReportDetail(reportID, { includeSources: true }));
            setError(null);
        } catch (failure) {
            setError(failure);
        } finally {
            setLoading(false);
        }
    }, [reportID]);

    useEffect(() => {
        load();
    }, [load]);

    const subscoreRows = useMemo(() => {
        const scores = report?.scores?.subscores;
        return scores ? Object.entries(scores) : [];
    }, [report]);

    if (loading) {
        return <LoadingBlock label="Loading diagnostic" />;
    }

    if (error) {
        return <ErrorNotice error={error} onRetry={load} />;
    }

    if (!report) {
        return <EmptyState message="That report could not be found." />;
    }

    return (
        <div className="space-y-6">
            <header className="flex flex-wrap items-end justify-between gap-4">
                <div>
                    <Link to="/reports" className="eyebrow hover:text-accent">
                        {"\u2190"} Back to library
                    </Link>
                    <h1 className="mt-2 text-2xl font-semibold">Diagnostic #{report.id}</h1>
                    <p className="mt-1 text-sm text-muted">
                        {report.species?.common_name} &middot; <span className="italic">{report.species?.scientific_name}</span> &middot;{" "}
                        {formatClock(report.generated_at)}
                    </p>
                </div>
                <span className={`pill ${tierStyle(report.scores.escalation_tier)}`}>
                    Level {report.scores.escalation_level} &middot; {report.scores.escalation_tier}
                </span>
            </header>

            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                <ScoreTile label="Conservation Risk Index" value={formatNumber(report.scores.conservation_risk_index)} />
                <ScoreTile label="Inbreeding penalty" value={formatNumber(report.scores.inbreeding_penalty_index)} alarm={report.scores.critical_inbreeding_risk} />
                <ScoreTile label="Risk momentum / day" value={formatNumber(report.scores.risk_momentum_per_day, 2)} />
                <ScoreTile label="Effective population" value={formatNumber(report.scores.effective_population_size, 0)} />
            </div>

            <div className="grid gap-6 xl:grid-cols-3">
                <Panel className="xl:col-span-2" eyebrow="Narrative" title="Root-cause analysis" description={`Model ${report.model_version || "unknown"}`}>
                    <NarrativeMarkdown markdown={report.narrative_report} />
                </Panel>

                <div className="space-y-6">
                    <Panel eyebrow="Field actions" title="Intervention checklist" description="Ticks are stored locally on this workstation.">
                        <InterventionChecklist reportID={report.id} markdown={report.narrative_report} />
                    </Panel>

                    <Panel eyebrow="Scope" title="Assessment window">
                        <dl className="space-y-2 text-sm">
                            <DetailRow label="H3 cell" value={report.scope?.h3_cell} mono />
                            <DetailRow label="Period start" value={formatClock(report.scope?.period_start)} />
                            <DetailRow label="Period end" value={formatClock(report.scope?.period_end)} />
                            <DetailRow label="Population trend" value={report.scores.population_trend} />
                        </dl>
                    </Panel>
                </div>
            </div>

            <div className="grid gap-6 xl:grid-cols-2">
                <Panel eyebrow="Subscores" title="Domain breakdown">
                    {subscoreRows.length > 0 ? (
                        <ul className="space-y-3">
                            {subscoreRows.map(([domain, value]) => (
                                <li key={domain}>
                                    <div className="flex justify-between text-sm">
                                        <span className="capitalize text-muted">{domain}</span>
                                        <span className="font-mono text-ink">{formatNumber(value)}</span>
                                    </div>
                                    <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-panelRaised">
                                        <div className="h-full bg-accent" style={{ width: `${Math.min(100, Math.max(0, value || 0))}%` }} />
                                    </div>
                                </li>
                            ))}
                        </ul>
                    ) : (
                        <EmptyState message="No subscores recorded." />
                    )}
                </Panel>

                <Panel eyebrow="Evidence" title="Triggering inputs" description="The sensor context the LangGraph chain consumed.">
                    {report.input_summary && Object.keys(report.input_summary).length > 0 ? (
                        <dl className="space-y-2 text-sm">
                            {Object.entries(report.input_summary).map(([key, value]) => (
                                <DetailRow key={key} label={key.replace(/_/g, " ")} value={typeof value === "object" ? JSON.stringify(value) : String(value)} />
                            ))}
                        </dl>
                    ) : (
                        <EmptyState message="No input summary was attached." />
                    )}

                    {report.pipeline_errors?.length > 0 && (
                        <div className="mt-4 rounded-xl border border-caution/50 bg-caution/10 p-3 text-xs text-caution">
                            <p className="font-semibold">The pipeline reported {report.pipeline_errors.length} issue(s).</p>
                            <ul className="mt-1 list-disc pl-4">
                                {report.pipeline_errors.map((issue, index) => (
                                    <li key={index}>{typeof issue === "string" ? issue : JSON.stringify(issue)}</li>
                                ))}
                            </ul>
                        </div>
                    )}
                </Panel>
            </div>
        </div>
    );
}

function ReportCard({ report }) {
    return (
        <Link to={`/reports/${report.id}`} className="panelRaised block p-4 transition hover:border-accent/50">
            <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                    <p className="eyebrow">#{report.id} &middot; {formatClock(report.generated_at)}</p>
                    <p className="mt-1 truncate font-semibold text-ink">{report.species?.common_name || "Unassigned species"}</p>
                    <p className="truncate text-xs italic text-muted">{report.species?.scientific_name}</p>
                </div>
                <span className={`pill shrink-0 ${tierStyle(report.scores?.escalation_tier)}`}>
                    CRI {formatNumber(report.scores?.conservation_risk_index, 0)}
                </span>
            </div>
            <p className="mt-3 line-clamp-2 text-xs text-muted">{(report.narrative_excerpt || "").replace(/[#*]/g, "")}</p>
        </Link>
    );
}

function JobBanner({ job }) {
    const tone =
        job.status === "failed" || job.status === "timed_out"
            ? "border-danger/50 bg-danger/10 text-danger"
            : job.status === "completed"
              ? "border-accent/50 bg-accent/10 text-accent"
              : "border-caution/50 bg-caution/10 text-caution";

    return (
        <div className={`rounded-xl border px-4 py-3 text-sm ${tone}`}>
            <p className="font-semibold">
                Job {job.job_id} &middot; {job.status}
                {job.progress != null && ` (${job.progress}%)`}
            </p>
            {job.error && <p className="mt-1 font-mono text-xs">{job.error}</p>}
            {job.current_node && !["completed", "failed", "timed_out"].includes(job.status) && (
                <p className="mt-1 text-xs opacity-80">Running node: {job.current_node}</p>
            )}
            {Array.isArray(job.pipeline_errors) && job.pipeline_errors.length > 0 && (
                <ul className="mt-2 space-y-0.5 text-[11px] opacity-80">
                    {job.pipeline_errors.slice(0, 6).map((line) => (
                        <li key={line}>{line}</li>
                    ))}
                </ul>
            )}
            {!["completed", "failed", "timed_out"].includes(job.status) && (
                <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-panelRaised">
                    <div className="h-full bg-current transition-all" style={{ width: `${job.progress || 8}%` }} />
                </div>
            )}
        </div>
    );
}

function FilterField({ label, children }) {
    return (
        <label className="block">
            <span className="eyebrow">{label}</span>
            <div className="mt-1.5">{children}</div>
        </label>
    );
}

function ScoreTile({ label, value, alarm = false }) {
    return (
        <article className="panelRaised p-4">
            <p className="eyebrow">{label}</p>
            <p className={`metricValue mt-2 ${alarm ? "text-danger" : ""}`}>{value}</p>
        </article>
    );
}

function DetailRow({ label, value, mono = false }) {
    return (
        <div className="flex items-start justify-between gap-3 border-b border-hairline/60 pb-2 last:border-0">
            <dt className="capitalize text-muted">{label}</dt>
            <dd className={`text-right text-ink ${mono ? "font-mono text-xs" : ""}`}>{value || "\u2014"}</dd>
        </div>
    );
}
