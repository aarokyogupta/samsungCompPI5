/**
 * Narrative rendering for LangGraph output.
 *
 * The AI writes markdown containing LaTeX such as $N_e$ and $H_e$, so remark-math parses the maths
 * and rehype-katex typesets it. react-markdown escapes raw HTML by default, which keeps model output
 * from injecting script tags into the command centre.
 */

import { useEffect, useMemo, useState } from "react";
import Markdown from "react-markdown";
import rehypeKatex from "rehype-katex";
import remarkGfm from "remark-gfm";
import remarkMath from "remark-math";

// Checklist progress is a field note, not backend state, so it lives in the operator's own browser
const CHECKLIST_STORAGE_PREFIX = "icmis.interventions.";
// Recommendation bullets the AI emits look like "Level 4: Deploy anti-poaching drone to Sector 4"
const INTERVENTION_PATTERN = /^\s*[-*]\s*(?:\*\*)?Level\s*([1-5])(?:\*\*)?\s*[:.\u2014-]\s*(.+?)\s*$/gim;

const markdownComponents = {
    h1: ({ children }) => <h2 className="mt-6 text-xl font-semibold text-ink first:mt-0">{children}</h2>,
    h2: ({ children }) => <h3 className="mt-6 text-lg font-semibold text-ink first:mt-0">{children}</h3>,
    h3: ({ children }) => <h4 className="mt-5 text-base font-semibold text-accent first:mt-0">{children}</h4>,
    p: ({ children }) => <p className="mt-3 text-sm leading-relaxed text-muted">{children}</p>,
    ul: ({ children }) => <ul className="mt-3 list-disc space-y-1 pl-5 text-sm text-muted">{children}</ul>,
    ol: ({ children }) => <ol className="mt-3 list-decimal space-y-1 pl-5 text-sm text-muted">{children}</ol>,
    strong: ({ children }) => <strong className="font-semibold text-ink">{children}</strong>,
    code: ({ children }) => <code className="rounded bg-panelRaised px-1.5 py-0.5 font-mono text-[12px] text-accent">{children}</code>,
    a: ({ href, children }) => (
        <a href={href} className="text-accentCool underline underline-offset-2" target="_blank" rel="noreferrer">
            {children}
        </a>
    ),
    table: ({ children }) => (
        <div className="mt-4 overflow-x-auto">
            <table className="w-full border-collapse text-sm">{children}</table>
        </div>
    ),
    th: ({ children }) => <th className="border border-hairline bg-panelRaised px-3 py-2 text-left text-xs uppercase tracking-wide text-muted">{children}</th>,
    td: ({ children }) => <td className="border border-hairline px-3 py-2 text-muted">{children}</td>,
    blockquote: ({ children }) => <blockquote className="mt-4 border-l-2 border-accent/60 pl-4 italic text-muted">{children}</blockquote>,
};

export function NarrativeMarkdown({ markdown }) {
    if (!markdown) {
        return <p className="text-sm text-muted">This report has no narrative body.</p>;
    }

    return (
        <div className="max-w-none">
            <Markdown remarkPlugins={[remarkGfm, remarkMath]} rehypePlugins={[rehypeKatex]} components={markdownComponents}>
                {markdown}
            </Markdown>
        </div>
    );
}

/** Pulls the "Level N: action" bullets out of the narrative so they can become a tickable checklist. */
export const extractInterventions = (markdown) => {
    if (!markdown) {
        return [];
    }

    const matches = [...markdown.matchAll(INTERVENTION_PATTERN)];
    return matches.map((match, index) => ({
        id: `${index}-${match[2].slice(0, 40)}`,
        level: Number(match[1]),
        action: match[2].replace(/\*\*/g, "").trim(),
    }));
};

const LEVEL_TONE = {
    5: "border-danger/60 text-danger",
    4: "border-danger/50 text-danger",
    3: "border-caution/60 text-caution",
    2: "border-accentCool/60 text-accentCool",
    1: "border-hairline text-muted",
};

export function InterventionChecklist({ reportID, markdown }) {
    const interventions = useMemo(() => extractInterventions(markdown), [markdown]);
    const storageKey = `${CHECKLIST_STORAGE_PREFIX}${reportID}`;
    const [completed, setCompleted] = useState({});

    useEffect(() => {
        try {
            setCompleted(JSON.parse(window.localStorage.getItem(storageKey) || "{}"));
        } catch (failure) {
            setCompleted({});
        }
    }, [storageKey]);

    const toggle = (interventionID) => {
        setCompleted((current) => {
            const next = { ...current, [interventionID]: !current[interventionID] };
            window.localStorage.setItem(storageKey, JSON.stringify(next));
            return next;
        });
    };

    if (interventions.length === 0) {
        return <p className="text-sm text-muted">No Level 1-5 interventions were recommended for this assessment.</p>;
    }

    const doneCount = interventions.filter((item) => completed[item.id]).length;

    return (
        <div>
            <div className="mb-3 flex items-center justify-between text-xs text-muted">
                <span>
                    {doneCount} of {interventions.length} mitigation steps logged
                </span>
                <div className="h-1.5 w-32 overflow-hidden rounded-full bg-panelRaised">
                    <div className="h-full bg-accent transition-all" style={{ width: `${(doneCount / interventions.length) * 100}%` }} />
                </div>
            </div>
            <ul className="space-y-2">
                {interventions
                    .slice()
                    .sort((left, right) => right.level - left.level)
                    .map((item) => (
                        <li key={item.id}>
                            <label className="flex cursor-pointer items-start gap-3 rounded-xl border border-hairline bg-panelRaised p-3 transition hover:border-accent/50">
                                <input
                                    type="checkbox"
                                    checked={Boolean(completed[item.id])}
                                    onChange={() => toggle(item.id)}
                                    className="mt-0.5 h-4 w-4 accent-[#57d89b]"
                                />
                                <span className="min-w-0 flex-1">
                                    <span className={`pill mr-2 ${LEVEL_TONE[item.level] || LEVEL_TONE[1]}`}>Level {item.level}</span>
                                    <span className={`text-sm ${completed[item.id] ? "text-muted line-through" : "text-ink"}`}>{item.action}</span>
                                </span>
                            </label>
                        </li>
                    ))}
            </ul>
        </div>
    );
}
