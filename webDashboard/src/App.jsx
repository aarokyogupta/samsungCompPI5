/**
 * Root engine for the ICMIS Web Operations Dashboard.
 *
 * Owns three things that must outlive any single route: the router, the dark command-centre shell,
 * and the global alert provider whose WebSocket keeps running while the operator navigates.
 */

import { useEffect, useState } from "react";
import { NavLink, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { ToastOverlay } from "./components/alertViews.jsx";
import { StatusPill, ViewBoundary } from "./components/uiKit.jsx";
import { AlertProvider, useAlerts } from "./context/alertContext.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import RiskReports from "./pages/RiskReports.jsx";
import Settings from "./pages/Settings.jsx";
import SpeciesDetail from "./pages/SpeciesDetail.jsx";
import { fetchHealth } from "./services/api.js";

// Configuration
const HEALTH_REFRESH_MS = 30000;
const NAVIGATION = [
    { to: "/", label: "Command Centre", glyph: "\u25A6", end: true },
    { to: "/species/1", label: "Species Analytics", glyph: "\u25C9", match: "/species" },
    { to: "/reports", label: "Risk Diagnostics", glyph: "\u26A0", match: "/reports" },
    { to: "/settings", label: "Pi Settings", glyph: "\u2699", match: "/settings" },
];

function Sidebar() {
    const location = useLocation();
    const { connectionState } = useAlerts();
    const [health, setHealth] = useState(null);

    useEffect(() => {
        const poll = () => fetchHealth().then(setHealth).catch(() => setHealth(null));
        poll();
        const timer = window.setInterval(poll, HEALTH_REFRESH_MS);
        return () => window.clearInterval(timer);
    }, []);

    return (
        <aside className="flex w-60 shrink-0 flex-col border-r border-hairline bg-panel">
            <div className="border-b border-hairline px-5 py-6">
                <p className="text-lg font-semibold tracking-tight text-accent">ICMIS</p>
                <p className="mt-1 text-[11px] leading-snug text-muted">
                    Integrated Conservation Monitoring &amp; Intelligence System
                </p>
            </div>

            <nav className="flex-1 space-y-1 p-3">
                {NAVIGATION.map((item) => {
                    // NavLink's own matching cannot express "/species/:id is still the species tab"
                    const active = item.match ? location.pathname.startsWith(item.match) : location.pathname === item.to;
                    return (
                        <NavLink
                            key={item.to}
                            to={item.to}
                            end={item.end}
                            className={`flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm transition ${
                                active ? "bg-accent/15 font-semibold text-accent" : "text-muted hover:bg-panelRaised hover:text-ink"
                            }`}
                        >
                            <span className="w-4 text-center">{item.glyph}</span>
                            {item.label}
                        </NavLink>
                    );
                })}
            </nav>

            <div className="space-y-2 border-t border-hairline p-4 text-[11px] text-muted">
                <StatusPill state={connectionState} />
                {health && (
                    <>
                        <p>
                            API <span className="font-mono text-ink">{health.version}</span> &middot; schema v{health.schema_version}
                        </p>
                        <p>
                            Database <span className={health.database === "ok" ? "text-accent" : "text-danger"}>{health.database}</span>
                        </p>
                        <p>Uptime {Math.round((health.uptime_sec || 0) / 60)} min</p>
                    </>
                )}
            </div>
        </aside>
    );
}

function Shell() {
    return (
        <div className="flex h-full">
            <Sidebar />
            <main className="flex-1 overflow-y-auto">
                <div className="mx-auto max-w-[1480px] p-6 xl:p-8">
                    <ViewBoundary>
                        <Routes>
                            <Route path="/" element={<Dashboard />} />
                            <Route path="/species/:id" element={<SpeciesDetail />} />
                            <Route path="/reports" element={<RiskReports />} />
                            <Route path="/reports/:reportID" element={<RiskReports />} />
                            <Route path="/settings" element={<Settings />} />
                            <Route path="*" element={<Navigate to="/" replace />} />
                        </Routes>
                    </ViewBoundary>
                </div>
            </main>
            <ToastOverlay />
        </div>
    );
}

export default function App() {
    return (
        <AlertProvider>
            <Shell />
        </AlertProvider>
    );
}
