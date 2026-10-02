/**
 * Settings - remote configuration of the Raspberry Pi.
 *
 * Route: /settings
 * Every form is generated from GET /settings/schema, so a setting added to the backend catalogue shows
 * up here without a frontend change. Saving writes config.yaml on the Pi (after a pre-flight check);
 * "Apply" then asks the root helper to restart only the programs that read the changed values.
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import { EmptyState, ErrorNotice, LoadingBlock, Panel, formatClock } from "../components/uiKit.jsx";
import {
    applySettings,
    fetchHealth,
    fetchSettings,
    fetchSettingsBackups,
    fetchSettingsSchema,
    fetchSettingsStatus,
    getApiKey,
    hasStoredApiKey,
    rollbackSettings,
    saveSecret,
    saveSettings,
    setApiKey,
} from "../services/api.js";

// Configuration
const STATUS_REFRESH_MS = 10000;
// After an API restart the Pi needs a few seconds to import every module before /health answers
const RESTART_POLL_MS = 2000;
const RESTART_TIMEOUT_MS = 180000;

// Values travel as JSON; the form keeps text for anything the operator types so half-typed numbers survive
const toDraft = (field, value) => {
    if (value === null || value === undefined) {
        return field.valueType === "boolean" ? false : "";
    }
    if (field.valueType === "stringList") {
        return value.join("\n");
    }
    if (field.valueType === "json") {
        return JSON.stringify(value, null, 2);
    }
    if (field.valueType === "integer" || field.valueType === "number") {
        return String(value);
    }
    return value;
};

const fromDraft = (field, draft) => {
    if (field.valueType === "boolean") {
        return Boolean(draft);
    }
    if (field.valueType === "stringList") {
        return String(draft).split(/[\n,]/).map((item) => item.trim()).filter(Boolean);
    }
    if (field.valueType === "json") {
        // A parse error is raised to the caller so it can be shown next to the field
        return JSON.parse(draft || "null");
    }
    if (field.valueType === "integer" || field.valueType === "number") {
        if (String(draft).trim() === "") {
            return null;
        }
        const parsed = Number(draft);
        return Number.isNaN(parsed) ? draft : parsed;
    }
    if (field.nullable && String(draft).trim() === "") {
        return null;
    }
    return draft;
};

const sameValue = (left, right) => JSON.stringify(left ?? null) === JSON.stringify(right ?? null);

const waitForApi = async () => {
    const deadline = Date.now() + RESTART_TIMEOUT_MS;
    // Give systemd a moment to actually stop the old process before the first probe
    await new Promise((resolve) => window.setTimeout(resolve, 4000));
    while (Date.now() < deadline) {
        try {
            await fetchHealth();
            return true;
        } catch (failure) {
            await new Promise((resolve) => window.setTimeout(resolve, RESTART_POLL_MS));
        }
    }
    return false;
};

export default function Settings() {
    const [schema, setSchema] = useState(null);
    const [settings, setSettings] = useState(null);
    const [status, setStatus] = useState(null);
    const [backups, setBackups] = useState([]);
    const [drafts, setDrafts] = useState({});
    const [fieldErrors, setFieldErrors] = useState({});
    const [activeGroup, setActiveGroup] = useState("system");
    const [error, setError] = useState(null);
    const [notice, setNotice] = useState(null);
    const [busy, setBusy] = useState(null);

    const loadAll = useCallback(async () => {
        try {
            const [schemaPayload, settingsPayload, backupPayload] = await Promise.all([
                fetchSettingsSchema(),
                fetchSettings(),
                fetchSettingsBackups(),
            ]);
            setSchema(schemaPayload);
            setSettings(settingsPayload);
            setBackups(backupPayload.backups || []);
            setDrafts({});
            setFieldErrors({});
            setError(null);
        } catch (failure) {
            setError(failure);
        }
    }, []);

    const loadStatus = useCallback(async () => {
        try {
            setStatus(await fetchSettingsStatus());
        } catch (failure) {
            // Status is advisory; the main error banner is reserved for load and save failures
        }
    }, []);

    useEffect(() => {
        loadAll();
        loadStatus();
        const timer = window.setInterval(loadStatus, STATUS_REFRESH_MS);
        return () => window.clearInterval(timer);
    }, [loadAll, loadStatus]);

    const fieldsByGroup = useMemo(() => {
        const grouped = {};
        (schema?.fields || []).forEach((field) => {
            (grouped[field.group] = grouped[field.group] || []).push(field);
        });
        return grouped;
    }, [schema]);

    const fieldsByKey = useMemo(() => Object.fromEntries((schema?.fields || []).map((field) => [field.key, field])), [schema]);
    const dirtyKeys = Object.keys(drafts);
    const pending = status?.pending ?? settings?.pending ?? [];

    const updateDraft = (field, draft) => {
        setDrafts((current) => {
            const next = { ...current, [field.key]: draft };
            // Typing a value back to what the Pi already has should clear the unsaved marker
            try {
                if (sameValue(fromDraft(field, draft), settings.values[field.key])) {
                    delete next[field.key];
                }
            } catch (failure) {
                // Invalid JSON mid-edit is still a change
            }
            return next;
        });
        setFieldErrors((current) => {
            const next = { ...current };
            delete next[field.key];
            return next;
        });
    };

    const handleSave = async () => {
        const values = {};
        const localErrors = {};
        dirtyKeys.forEach((key) => {
            try {
                values[key] = fromDraft(fieldsByKey[key], drafts[key]);
            } catch (failure) {
                localErrors[key] = "Not valid JSON.";
            }
        });
        if (Object.keys(localErrors).length) {
            setFieldErrors(localErrors);
            return;
        }

        setBusy("save");
        setNotice(null);
        try {
            const result = await saveSettings(values, settings.revision);
            setSettings(result);
            setDrafts({});
            setFieldErrors({});
            setError(null);
            setNotice(
                result.changed?.length
                    ? `Saved ${result.changed.length} setting(s). Press Apply to restart: ${result.services.join(", ")}.`
                    : "Nothing changed.",
            );
            fetchSettingsBackups().then((payload) => setBackups(payload.backups || [])).catch(() => {});
            loadStatus();
        } catch (failure) {
            // 422 carries one {key, message} per bad field so each can be shown in place
            if (Array.isArray(failure.detail)) {
                const mapped = {};
                const general = [];
                failure.detail.forEach((item) => {
                    if (item?.key && fieldsByKey[item.key]) {
                        mapped[item.key] = item.message;
                    } else {
                        general.push(item?.message || String(item));
                    }
                });
                setFieldErrors(mapped);
                setError(general.length ? { message: general.join(" ") } : { message: "Some settings need fixing (marked in red)." });
                const firstKey = Object.keys(mapped)[0];
                if (firstKey) {
                    setActiveGroup(fieldsByKey[firstKey].group);
                }
            } else {
                setError(failure);
            }
        } finally {
            setBusy(null);
        }
    };

    const handleApply = async (services = null) => {
        setBusy("apply");
        setNotice(null);
        try {
            const result = await applySettings(services);
            if (!result.queued?.length) {
                setNotice(result.message || "Nothing to apply.");
                return;
            }
            setNotice(`Restarting ${result.queued.join(", ")}\u2026`);
            if (result.apiRestarting) {
                const back = await waitForApi();
                setNotice(back ? "Applied. The API restarted and is back online." : "The API has not come back yet; check `journalctl -u icmis-api` on the Pi.");
                await loadAll();
            }
            loadStatus();
        } catch (failure) {
            setError(failure);
        } finally {
            setBusy(null);
        }
    };

    const handleRollback = async (name) => {
        if (!window.confirm(`Restore config.yaml from ${name}? The current settings are backed up first.`)) {
            return;
        }
        setBusy("rollback");
        try {
            const result = await rollbackSettings(name);
            setSettings(result);
            setDrafts({});
            setNotice(`Restored ${result.restored}. Press Apply to restart the affected programs.`);
            fetchSettingsBackups().then((payload) => setBackups(payload.backups || [])).catch(() => {});
            loadStatus();
        } catch (failure) {
            setError(Array.isArray(failure.detail) ? { message: failure.detail.map((item) => item.message).join(" ") } : failure);
        } finally {
            setBusy(null);
        }
    };

    return (
        <div className="space-y-6">
            <header className="flex flex-wrap items-end justify-between gap-4">
                <div>
                    <p className="eyebrow">Edge node</p>
                    <h1 className="mt-1 text-2xl font-semibold">Pi Settings</h1>
                    <p className="mt-1 text-sm text-muted">
                        Changes are validated, written to config.yaml on the Pi and applied by restarting only the affected programs.
                    </p>
                </div>
                <div className="flex gap-2">
                    <button
                        type="button"
                        onClick={() => setDrafts({})}
                        disabled={!dirtyKeys.length || Boolean(busy)}
                        className="rounded-xl border border-hairline px-4 py-2 text-sm text-muted transition hover:text-ink disabled:opacity-40"
                    >
                        Discard
                    </button>
                    <button
                        type="button"
                        onClick={handleSave}
                        disabled={!dirtyKeys.length || Boolean(busy)}
                        className="rounded-xl bg-accent px-4 py-2 text-sm font-semibold text-canvas transition hover:brightness-110 disabled:opacity-40"
                    >
                        {busy === "save" ? "Checking & saving\u2026" : `Save ${dirtyKeys.length || ""} change${dirtyKeys.length === 1 ? "" : "s"}`}
                    </button>
                </div>
            </header>

            <ConnectionCard onChanged={loadAll} />

            {pending.length > 0 && (
                <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-caution/50 bg-caution/10 px-4 py-3 text-sm text-caution">
                    <p>
                        <span className="font-semibold">Saved but not applied:</span> {pending.join(", ")}
                    </p>
                    <button
                        type="button"
                        onClick={() => handleApply()}
                        disabled={Boolean(busy) || status?.applyInProgress}
                        className="rounded-lg bg-caution px-3 py-1.5 text-xs font-semibold text-canvas disabled:opacity-40"
                    >
                        {busy === "apply" || status?.applyInProgress ? "Applying\u2026" : "Apply now"}
                    </button>
                </div>
            )}

            {notice && <p className="rounded-xl border border-accent/40 bg-accent/10 px-4 py-3 text-sm text-accent">{notice}</p>}
            <ErrorNotice error={error} onRetry={error?.status ? loadAll : null} />

            {!schema || !settings ? (
                error ? null : <LoadingBlock label="Reading settings from the Pi" />
            ) : (
                <div className="grid gap-6 xl:grid-cols-[220px_1fr]">
                    <nav className="panel h-fit space-y-1 p-2">
                        {schema.groups.map((group) => {
                            const groupDirty = (fieldsByGroup[group.id] || []).some((field) => field.key in drafts);
                            const groupError = (fieldsByGroup[group.id] || []).some((field) => field.key in fieldErrors);
                            return (
                                <button
                                    key={group.id}
                                    type="button"
                                    onClick={() => setActiveGroup(group.id)}
                                    className={`flex w-full items-center justify-between rounded-lg px-3 py-2 text-left text-sm transition ${
                                        activeGroup === group.id ? "bg-accent/15 font-semibold text-accent" : "text-muted hover:bg-panelRaised hover:text-ink"
                                    }`}
                                >
                                    {group.label}
                                    {groupError ? <span className="text-danger">!</span> : groupDirty ? <span className="text-caution">{"\u25CF"}</span> : null}
                                </button>
                            );
                        })}
                    </nav>

                    <GroupForm
                        group={schema.groups.find((group) => group.id === activeGroup) || schema.groups[0]}
                        fields={fieldsByGroup[activeGroup] || []}
                        values={settings.values}
                        drafts={drafts}
                        fieldErrors={fieldErrors}
                        timezones={schema.timezones || []}
                        onChange={updateDraft}
                    />
                </div>
            )}

            <div className="grid gap-6 xl:grid-cols-2">
                <ServiceStatus status={status} onRestart={(service) => handleApply([service])} busy={Boolean(busy)} />
                <div className="space-y-6">
                    <SecretsPanel
                        onSaved={() => {
                            loadAll();
                            loadStatus();
                        }}
                    />
                    <BackupsPanel backups={backups} onRollback={handleRollback} busy={Boolean(busy)} />
                </div>
            </div>
        </div>
    );
}

function GroupForm({ group, fields, values, drafts, fieldErrors, timezones, onChange }) {
    return (
        <Panel eyebrow="Settings" title={group.label} description={group.description}>
            {fields.length === 0 ? (
                <EmptyState message="No settings in this group." />
            ) : (
                <div className="grid gap-4 lg:grid-cols-2">
                    {fields.map((field) => (
                        <SettingInput
                            key={field.key}
                            field={field}
                            draft={field.key in drafts ? drafts[field.key] : toDraft(field, values[field.key])}
                            dirty={field.key in drafts}
                            error={fieldErrors[field.key]}
                            timezones={timezones}
                            onChange={(draft) => onChange(field, draft)}
                        />
                    ))}
                </div>
            )}
        </Panel>
    );
}

function SettingInput({ field, draft, dirty, error, timezones, onChange }) {
    const wide = field.valueType === "json" || field.valueType === "stringList";
    const border = error ? "border-danger" : dirty ? "border-caution" : "";
    let control;

    if (field.valueType === "boolean") {
        control = (
            <button
                type="button"
                role="switch"
                aria-checked={Boolean(draft)}
                onClick={() => onChange(!draft)}
                className={`relative h-6 w-11 rounded-full transition ${draft ? "bg-accent" : "bg-hairline"}`}
            >
                <span className={`absolute top-0.5 h-5 w-5 rounded-full bg-canvas transition ${draft ? "left-[22px]" : "left-0.5"}`} />
            </button>
        );
    } else if (field.valueType === "enum") {
        control = (
            <select value={draft ?? ""} onChange={(event) => onChange(event.target.value)} className={`fieldInput ${border}`}>
                {field.nullable && <option value="">(not set)</option>}
                {field.options.map((option) => (
                    <option key={option} value={option}>
                        {option}
                    </option>
                ))}
            </select>
        );
    } else if (wide) {
        control = (
            <textarea
                value={draft}
                onChange={(event) => onChange(event.target.value)}
                rows={field.valueType === "json" ? 8 : 4}
                spellCheck={false}
                placeholder={field.valueType === "stringList" ? "One item per line" : "JSON"}
                className={`fieldInput font-mono text-xs ${border}`}
            />
        );
    } else {
        const numeric = field.valueType === "integer" || field.valueType === "number";
        const listID = field.validator === "timezone" ? `timezones-${field.key}` : undefined;
        control = (
            <>
                <input
                    type={numeric ? "number" : "text"}
                    step={field.valueType === "integer" ? "1" : "any"}
                    min={field.minimum ?? undefined}
                    max={field.maximum ?? undefined}
                    value={draft}
                    list={listID}
                    placeholder={field.nullable ? "(not set)" : ""}
                    onChange={(event) => onChange(event.target.value)}
                    className={`fieldInput ${field.valueType === "path" ? "font-mono text-xs" : ""} ${border}`}
                />
                {listID && (
                    <datalist id={listID}>
                        {timezones.map((zone) => (
                            <option key={zone} value={zone} />
                        ))}
                    </datalist>
                )}
            </>
        );
    }

    const range =
        field.minimum !== null && field.maximum !== null
            ? `${field.minimum} \u2013 ${field.maximum}`
            : field.minimum !== null
              ? `\u2265 ${field.minimum}`
              : field.maximum !== null
                ? `\u2264 ${field.maximum}`
                : null;

    return (
        <label className={`block space-y-1.5 ${wide ? "lg:col-span-2" : ""}`}>
            <span className="flex items-center justify-between gap-2 text-xs">
                <span className="font-semibold text-ink">
                    {field.label}
                    {dirty && <span className="ml-1 text-caution">{"\u25CF"}</span>}
                </span>
                <span className="font-mono text-[10px] text-muted">{field.key}</span>
            </span>
            {control}
            {error ? (
                <span className="block text-xs text-danger">{error}</span>
            ) : (
                <span className="block text-[11px] leading-snug text-muted">
                    {field.description}
                    {range && <span className="ml-1 opacity-70">({range})</span>}
                    <span className="ml-1 opacity-70">Restarts: {field.services.join(", ")}.</span>
                </span>
            )}
        </label>
    );
}

/**
 * The browser remembers the Pi's API key locally, so a fresh laptop can be paired by pasting the key
 * printed by installServices.sh instead of rebuilding the dashboard with VITE_API_KEY.
 */
function ConnectionCard({ onChanged }) {
    const [apiKey, setApiKeyDraft] = useState("");
    const [stored, setStored] = useState(hasStoredApiKey());

    const save = () => {
        setApiKey(apiKey.trim());
        setStored(hasStoredApiKey());
        setApiKeyDraft("");
        onChanged();
    };

    return (
        <Panel
            eyebrow="Connection"
            title="This browser's access key"
            description={
                stored
                    ? "A key is saved in this browser. Paste a new one to replace it."
                    : getApiKey()
                      ? "Using the key built into the dashboard (VITE_API_KEY)."
                      : "No key saved. If the Pi has ICMIS_API_KEY set, paste it here (it is printed by installServices.sh)."
            }
        >
            <div className="flex flex-wrap gap-2">
                <input
                    type="password"
                    autoComplete="off"
                    value={apiKey}
                    onChange={(event) => setApiKeyDraft(event.target.value)}
                    placeholder="ICMIS_API_KEY"
                    className="fieldInput max-w-md flex-1 font-mono text-xs"
                />
                <button
                    type="button"
                    onClick={save}
                    disabled={!apiKey.trim()}
                    className="rounded-xl bg-accent px-4 py-2 text-sm font-semibold text-canvas disabled:opacity-40"
                >
                    Save key
                </button>
                {stored && (
                    <button
                        type="button"
                        onClick={() => {
                            setApiKey("");
                            setStored(false);
                            onChanged();
                        }}
                        className="rounded-xl border border-hairline px-4 py-2 text-sm text-muted hover:text-ink"
                    >
                        Forget
                    </button>
                )}
            </div>
        </Panel>
    );
}

const stateTone = (state) =>
    state === "active" ? "text-accent" : state === "failed" ? "text-danger" : state === "activating" ? "text-caution" : "text-muted";

function ServiceStatus({ status, onRestart, busy }) {
    if (!status) {
        return (
            <Panel eyebrow="Programs" title="Service status">
                <LoadingBlock label="Asking systemd" />
            </Panel>
        );
    }

    return (
        <Panel
            eyebrow="Programs"
            title="Service status"
            description={
                status.systemdAvailable
                    ? "Everything here starts automatically at boot. Programs that are not ready are skipped until their setting is fixed."
                    : "systemd is not available on this machine, so live states are hidden (expected when running off the Pi)."
            }
        >
            <table className="w-full text-left text-xs">
                <thead className="text-muted">
                    <tr>
                        <th className="py-2 font-medium">Program</th>
                        <th className="py-2 font-medium">State</th>
                        <th className="py-2 font-medium">Readiness</th>
                        <th className="py-2" />
                    </tr>
                </thead>
                <tbody className="divide-y divide-hairline">
                    <tr>
                        <td className="py-2 font-semibold text-ink">API server</td>
                        <td className={`py-2 ${stateTone(status.api?.activeState)}`}>{status.api?.activeState || "\u2014"}</td>
                        <td className="py-2 text-muted">Serves this dashboard</td>
                        <td className="py-2 text-right">
                            <RestartButton disabled={busy || !status.applyAvailable} onClick={() => onRestart("api")} />
                        </td>
                    </tr>
                    {status.workers.map((worker) => (
                        <tr key={worker.service}>
                            <td className="py-2 text-ink">{worker.label}</td>
                            <td className={`py-2 ${stateTone(worker.activeState)}`}>
                                {worker.activeState ? `${worker.activeState} (${worker.subState})` : "\u2014"}
                            </td>
                            <td className={`py-2 ${worker.ready ? "text-muted" : "text-caution"}`}>{worker.reason}</td>
                            <td className="py-2 text-right">
                                <RestartButton disabled={busy || !status.applyAvailable} onClick={() => onRestart(worker.service)} />
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>

            <h3 className="mt-5 text-xs font-semibold uppercase tracking-wide text-muted">Schedules</h3>
            <ul className="mt-2 space-y-1 text-xs">
                {(status.schedules || []).map((schedule) => (
                    <li key={schedule.schedule} className="flex justify-between gap-3">
                        <span className="text-ink">
                            {schedule.label} <span className="font-mono text-muted">{schedule.onCalendar}</span>
                        </span>
                        <span className="text-muted">
                            {!schedule.configEnabled ? "disabled" : schedule.nextRun ? `next ${schedule.nextRun}` : schedule.activeState || "\u2014"}
                        </span>
                    </li>
                ))}
            </ul>

            {status.lastApply && (
                <p className="mt-4 text-[11px] text-muted">
                    Last apply: <span className={status.lastApply.state === "failed" ? "text-danger" : "text-ink"}>{status.lastApply.state}</span>{" "}
                    {formatClock(status.lastApply.finishedAt || status.lastApply.queuedAt)}
                    {status.lastApply.error ? ` \u2014 ${status.lastApply.error}` : ""}
                </p>
            )}
        </Panel>
    );
}

function RestartButton({ disabled, onClick }) {
    return (
        <button
            type="button"
            onClick={onClick}
            disabled={disabled}
            className="rounded-lg border border-hairline px-2 py-1 text-[11px] text-muted hover:text-ink disabled:opacity-30"
        >
            Restart
        </button>
    );
}

function SecretsPanel({ onSaved }) {
    const [secrets, setSecrets] = useState(null);
    const [drafts, setDrafts] = useState({});
    const [error, setError] = useState(null);

    useEffect(() => {
        fetchSettingsSchema()
            .then((payload) => setSecrets(payload.secrets))
            .catch(() => setSecrets(null));
    }, []);

    const save = async (name) => {
        const value = (drafts[name] || "").trim();
        try {
            const result = await saveSecret(name, value);
            setSecrets(result.secrets);
            setDrafts((current) => ({ ...current, [name]: "" }));
            setError(null);
            if (name === "ICMIS_API_KEY") {
                // The old key stays valid until the API restarts, so restart first and switch this browser over after
                setError({ message: "Restarting the API with the new key\u2026" });
                await applySettings(["api"]);
                await waitForApi();
                setApiKey(value);
                setError(null);
            }
            onSaved();
        } catch (failure) {
            setError(failure);
        }
    };

    return (
        <Panel eyebrow="Secrets" title="Keys stored in .env on the Pi" description="Values are write-only: the Pi never sends them back.">
            <ErrorNotice error={error} />
            {!secrets ? (
                <EmptyState message="Secrets are unavailable until the dashboard can reach the Pi." />
            ) : (
                <div className="space-y-3">
                    {Object.entries(secrets).map(([name, secret]) => (
                        <div key={name} className="space-y-1">
                            <p className="flex justify-between text-xs">
                                <span className="font-semibold text-ink">{secret.label}</span>
                                <span className={secret.isSet ? "text-accent" : "text-muted"}>{secret.isSet ? "set" : "not set"}</span>
                            </p>
                            <div className="flex gap-2">
                                <input
                                    type="password"
                                    autoComplete="new-password"
                                    value={drafts[name] || ""}
                                    onChange={(event) => setDrafts((current) => ({ ...current, [name]: event.target.value }))}
                                    placeholder={`${name} (min ${secret.minLength} chars)`}
                                    className="fieldInput flex-1 font-mono text-xs"
                                />
                                <button
                                    type="button"
                                    onClick={() => save(name)}
                                    disabled={!(drafts[name] || "").trim() && (name === "ICMIS_API_KEY" || !secret.isSet)}
                                    className="rounded-xl border border-hairline px-3 py-2 text-xs text-ink hover:border-accent disabled:opacity-40"
                                >
                                    {(drafts[name] || "").trim() ? "Save" : "Remove"}
                                </button>
                            </div>
                        </div>
                    ))}
                </div>
            )}
        </Panel>
    );
}

function BackupsPanel({ backups, onRollback, busy }) {
    return (
        <Panel eyebrow="History" title="config.yaml backups" description="A copy is taken before every save or restore.">
            {backups.length === 0 ? (
                <EmptyState message="No backups yet; one is made the first time you save." />
            ) : (
                <ul className="max-h-64 divide-y divide-hairline overflow-y-auto text-xs">
                    {backups.map((backup) => (
                        <li key={backup.name} className="flex items-center justify-between gap-3 py-2">
                            <span>
                                <span className="text-ink">{formatClock(backup.createdAt)}</span>
                                <span className="ml-2 font-mono text-[10px] text-muted">{backup.name}</span>
                            </span>
                            <button
                                type="button"
                                disabled={busy}
                                onClick={() => onRollback(backup.name)}
                                className="rounded-lg border border-hairline px-2 py-1 text-[11px] text-muted hover:text-ink disabled:opacity-30"
                            >
                                Restore
                            </button>
                        </li>
                    ))}
                </ul>
            )}
        </Panel>
    );
}
