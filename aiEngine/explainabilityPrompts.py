import h3
import json
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import PydanticOutputParser
import math
import os
from pydantic import BaseModel, ConfigDict, Field, field_validator
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional
import yaml

# Allow "python aiEngine/explainabilityPrompts.py" as well as "python -m aiEngine.explainabilityPrompts" from the project root
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from aiEngine.stateSchema import (
    CRITICAL_NE_THRESHOLD,
    DATABASE_SCORE_SCALE,
    RISK_DOMAINS,
    ROUTE_GENETIC_RESCUE,
    ROUTE_POACHING_RESPONSE,
    AssessmentScope,
    NormalizedSubscores,
    RawIndicators,
    RiskMetrics,
    StateValidationError,
    routeOnRiskTriggers,
)

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

PROMPT_CONFIG: dict = config.get("aiEngine", {}).get("explainabilityPrompts", {}) or {}

# Subscores (0-100) at or above this value unlock their domain's mitigation strategies
CRITICAL_SUBSCORE_THRESHOLD: float = float(PROMPT_CONFIG.get("criticalSubscoreThreshold", 70.0))
CONTRIBUTION_DECIMALS: int = int(PROMPT_CONFIG.get("contributionDecimals", 1))
# Allowed gap (percentage points) between LLM-reported and calculated contributions
CONTRIBUTION_TOLERANCE: float = float(PROMPT_CONFIG.get("contributionTolerance", 0.5))
MAX_THREAT_CONTEXT_CLASSES: int = int(PROMPT_CONFIG.get("maxThreatContextClasses", 10))
MAX_TARGET_CELLS: int = int(PROMPT_CONFIG.get("maxTargetCells", 5))
# Behaviour components from the Indicator Calculator that indicate lost ecological knowledge
KNOWLEDGE_LOSS_ELDER_ABSENCE: float = float(PROMPT_CONFIG.get("knowledgeLossElderAbsence", 0.5))

if not 0.0 < CRITICAL_SUBSCORE_THRESHOLD <= 100.0:
    raise ValueError("explainabilityPrompts criticalSubscoreThreshold is on the 0-100 scale and must be in (0, 100].")
if CONTRIBUTION_DECIMALS < 0 or CONTRIBUTION_TOLERANCE < 0.0:
    raise ValueError("explainabilityPrompts contributionDecimals and contributionTolerance must be non-negative.")
if MAX_THREAT_CONTEXT_CLASSES < 1 or MAX_TARGET_CELLS < 1:
    raise ValueError("explainabilityPrompts maxThreatContextClasses and maxTargetCells must be at least 1.")
if not 0.0 <= KNOWLEDGE_LOSS_ELDER_ABSENCE <= 1.0:
    raise ValueError("explainabilityPrompts knowledgeLossElderAbsence must be between 0 and 1.")

NULL_MARKER: str = "NULL"
INSUFFICIENT_DATA: str = "Insufficient data for assessment"
MARKER_PATTERN = re.compile(r"\{\{([A-Z0-9_]+)\}\}")
# Words the genetics breakdown may not use while the N_e < 50 flag is raised
FORBIDDEN_SAFE_PATTERN = re.compile(r"\b(safe|secure|stable|healthy|viable)\b", re.IGNORECASE)
SUBSCORE_MARKERS: dict[str, str] = {
    "population": "S_POP",
    "habitat": "S_HAB",
    "threat": "S_THR",
    "climate": "S_CLI",
    "genetics": "S_GEN",
    "behavior": "S_BEH",
}


class ReportValidationError(ValueError):
    pass


# System instructions (immutable analytical persona)

SYSTEM_PROMPT: str = (
    "You are an analytical conservation intelligence engine. You will be provided with pre-calculated metrics "
    "(CRI, IPI, subscores). Your singular role is to translate these exact mathematical figures into structured "
    "diagnostic narratives. You are strictly forbidden from hallucinating, interpolating, or guessing numerical "
    "figures that are not provided in your payload.\n\n"
    "Tone rules:\n"
    "- Write in a clinical, objective and biologically accurate register.\n"
    "- Do not use flowery language, alarming hyperbole, exclamation marks or rhetorical questions.\n"
    "- Do not speculate about sensor, network or off-grid hardware failures; describe only the data provided.\n"
    "- Quote numbers exactly as they appear in the payload, with the same decimal places.\n\n"
    "Data rules:\n"
    f"- If a metric is listed as {NULL_MARKER} in the payload, you must state '{INSUFFICIENT_DATA}' in that specific "
    "schema field. Do not attempt to guess.\n"
    "- Domains listed under MISSING DOMAINS were scored with a precautionary fallback, not field data; any field "
    f"that describes such a domain must state '{INSUFFICIENT_DATA}'.\n"
    "- Threat events exist only as listed in the THREAT CONTEXT block. Never add, merge or extrapolate events.\n"
    "- If CRITICAL NE FLAG is TRUE you must never describe the population as safe, secure, stable, healthy or viable.\n"
    "- Subscores and indices run from 0 (optimal baseline) to 100 (imminent ecological collapse).\n\n"
    "Mitigation rules:\n"
    "- Recommend only strategies listed in the AUTHORISED MITIGATIONS block, using their exact strategy_id.\n"
    "- Never invent new strategies. Only use H3 cells listed for that strategy.\n"
    "- If the block is empty, return an empty recommended_mitigations list.\n\n"
    "{{FORMAT_INSTRUCTIONS}}"
)


# Payload template (marker tags are filled by compilePromptTemplate)

HUMAN_PROMPT_TEMPLATE: str = (
    "ASSESSMENT SCOPE\n"
    "Species: {{SPECIES}}\n"
    "H3 cell: {{H3_CELL}}\n"
    "Period: {{PERIOD_START}} to {{PERIOD_END}}\n\n"
    "RISK INDICES\n"
    "Conservation Risk Index (CRI): {{CRI_SCORE}} (Level {{ESCALATION_LEVEL}} of 5)\n"
    "Momentum vector: {{MOMENTUM_VECTOR}}\n"
    "Intervention Priority Index (IPI): {{IPI_SCORE}} ({{IPI_TIER}})\n"
    "Inbreeding Penalty Index: {{INBREEDING_PENALTY_INDEX}}\n\n"
    "SUBSCORES (0-100)\n"
    "S_pop (population health): {{S_POP}}\n"
    "S_hab (habitat condition): {{S_HAB}}\n"
    "S_thr (threat pressure): {{S_THR}}\n"
    "S_cli (climate stress): {{S_CLI}}\n"
    "S_gen (genetic resilience): {{S_GEN}}\n"
    "S_beh (behavioural stability): {{S_BEH}}\n"
    "MISSING DOMAINS: {{MISSING_DOMAINS}}\n\n"
    "WEIGHTED CONTRIBUTIONS (w_j x S_j and exact percentage of CRI)\n"
    "{{CONTRIBUTION_CONTEXT}}\n"
    "Primary mathematical driver: {{PRIMARY_DRIVER}}\n\n"
    "GENETICS\n"
    "Expected heterozygosity (He): {{EXPECTED_HETEROZYGOSITY}}\n"
    "Effective population size (Ne): {{EFFECTIVE_POPULATION_SIZE}}\n"
    "CRITICAL NE FLAG (Ne < {{CRITICAL_NE_THRESHOLD}}): {{CRITICAL_NE_FLAG}}\n\n"
    "BEHAVIOUR CONTEXT\n"
    "{{BEHAVIOR_CONTEXT}}\n\n"
    "THREAT CONTEXT (exact event counts; trend = second half of window minus first half)\n"
    "{{THREAT_CONTEXT}}\n"
    "IMMEDIATE POACHING FLAG: {{IMMEDIATE_POACHING_FLAG}}\n\n"
    "AUTHORISED MITIGATIONS\n"
    "{{MITIGATION_MATRIX}}\n\n"
    "Focus: {{ROUTE_FOCUS}}"
)

ROUTE_FOCUS: dict[str, str] = {
    ROUTE_POACHING_RESPONSE: "An immediate poaching threat flag is raised; lead with the threat pressure evidence and locations.",
    ROUTE_GENETIC_RESCUE: "The critical Ne flag is raised; lead with inbreeding depression risk.",
}
DEFAULT_FOCUS: str = "Summarise the overall conservation risk and its primary mathematical driver."

# Deterministic fallback strings used when the LLM times out or returns malformed JSON
FALLBACK_REPORT_TEMPLATE: dict[str, str] = {
    "summary": "CRI is {{CRI_SCORE}}. Analysis failed.",
    "executive_summary": (
        "Escalation Level {{ESCALATION_LEVEL}} of 5 for {{SPECIES}} in cell {{H3_CELL}}, momentum "
        "{{MOMENTUM_VECTOR}}, IPI {{IPI_SCORE}} ({{IPI_TIER}}). Automated narrative analysis failed; figures are "
        "reported directly from the risk engine."
    ),
    "primary_threat_drivers": (
        "The {{PRIMARY_DRIVER}} domain is the highest weighted contributor to the CRI at {{PRIMARY_DRIVER_SHARE}}% "
        "of the index. {{THREAT_EVENT_TOTAL}} threat events were recorded in the assessment window."
    ),
    "genetic_status_breakdown": (
        "He = {{EXPECTED_HETEROZYGOSITY}}, Ne = {{EFFECTIVE_POPULATION_SIZE}}, S_gen = {{S_GEN}}, Inbreeding "
        "Penalty Index = {{INBREEDING_PENALTY_INDEX}}. Critical Ne flag (Ne < {{CRITICAL_NE_THRESHOLD}}): "
        "{{CRITICAL_NE_FLAG}}."
    ),
    "genetic_critical": (
        "Ne = {{EFFECTIVE_POPULATION_SIZE}} is below the critical threshold of {{CRITICAL_NE_THRESHOLD}}, indicating "
        "an inbreeding depression crisis. He = {{EXPECTED_HETEROZYGOSITY}}, Inbreeding Penalty Index = "
        "{{INBREEDING_PENALTY_INDEX}}."
    ),
    "behavioral_loss_impact": "S_beh = {{S_BEH}}. {{BEHAVIOR_SUMMARY}}",
}


# Mitigation recommendation matrix (the only strategies the LLM may select)

MITIGATION_MATRIX: dict[str, dict[str, str]] = {
    "GENETIC_RESCUE": {
        "domain": "genetics",
        "title": "Genetic Rescue",
        "tactic": (
            "Translocate breeding males from neighbouring, genetically distinct populations to restore "
            "heterozygosity and raise Ne."
        ),
        "trigger": f"S_gen >= {CRITICAL_SUBSCORE_THRESHOLD:g} or critical Ne flag",
    },
    "NEURAL_CONSERVATION_TRAINING": {
        "domain": "behavior",
        "title": "Neural Conservation Training",
        "tactic": (
            "Condition individuals to historical corridors and resources to replace ecological knowledge lost "
            "with elder or matriarch removal."
        ),
        "trigger": f"S_beh >= {CRITICAL_SUBSCORE_THRESHOLD:g} with elder loss or orphaned juvenile groups",
    },
    "HERD_REINTEGRATION": {
        "domain": "behavior",
        "title": "Targeted Herd Re-integration",
        "tactic": "Re-integrate isolated juvenile groups with mature adults to rebuild social structure.",
        "trigger": f"S_beh >= {CRITICAL_SUBSCORE_THRESHOLD:g} with elder loss or orphaned juvenile groups",
    },
    "ANTI_POACHING_DRONE_DEPLOYMENT": {
        "domain": "threat",
        "title": "Anti-Poaching Drone Deployment",
        "tactic": "Deploy drone overwatch and ranger interdiction to the listed H3 grid cells immediately.",
        "trigger": f"S_thr is the primary driver, S_thr >= {CRITICAL_SUBSCORE_THRESHOLD:g} or immediate poaching flag",
    },
    "CONTINUED_MONITORING": {
        "domain": "overall",
        "title": "Continued Monitoring",
        "tactic": "Maintain the current sensor, camera-trap and eDNA sampling schedule and reassess next window.",
        "trigger": "No domain meets a mitigation trigger",
    },
}
MitigationID = Literal[
    "GENETIC_RESCUE",
    "NEURAL_CONSERVATION_TRAINING",
    "HERD_REINTEGRATION",
    "ANTI_POACHING_DRONE_DEPLOYMENT",
    "CONTINUED_MONITORING",
]
RiskDomainName = Literal["population", "habitat", "threat", "climate", "genetics", "behavior"]


# Structured output schema

class ReportModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class DomainContribution(ReportModel):
    domain: RiskDomainName
    percentage: float = Field(ge=0.0, le=100.0, description="Exact percentage this subscore contributed to the CRI.")


class MitigationRecommendation(ReportModel):
    strategy_id: MitigationID = Field(description="strategy_id copied from the AUTHORISED MITIGATIONS block.")
    rationale: str = Field(min_length=1, description="One or two sentences linking the strategy to the provided metrics.")
    target_h3_cells: list[str] = Field(
        default_factory=list, description="H3 cells listed for this strategy in the payload; empty if none are listed."
    )


class ExplainabilityReport(ReportModel):
    executive_summary: str = Field(min_length=1, description="Two sentences stating the CRI, level, momentum and IPI.")
    primary_threat_drivers: str = Field(
        min_length=1, description="Two sentences identifying the highest weighted contributor to the CRI."
    )
    percentage_contributions: list[DomainContribution] = Field(
        description="Exactly six entries, one per domain, copying the percentages from the payload."
    )
    genetic_status_breakdown: str = Field(
        min_length=1, description="Localised risk assessment translating the provided He and Ne metrics."
    )
    behavioral_loss_impact: str = Field(
        min_length=1, description="Ecological consequence of the provided S_beh score, e.g. lost migration routes."
    )
    recommended_mitigations: list[MitigationRecommendation] = Field(
        default_factory=list, description="Strategies selected only from the AUTHORISED MITIGATIONS block."
    )

    @field_validator("percentage_contributions")
    @classmethod
    def requireEveryDomain(cls, contributions: list[DomainContribution]) -> list[DomainContribution]:
        domains = [entry.domain for entry in contributions]
        if sorted(domains) != sorted(RISK_DOMAINS):
            raise ValueError("percentage_contributions must list each of the six risk domains exactly once.")
        return contributions


REPORT_PARSER = PydanticOutputParser(pydantic_object=ExplainabilityReport)


# Template compilation

def compilePromptTemplate(template: str, variables: dict[str, Any]) -> str:
    # Plain marker substitution keeps JSON braces in the payload from being parsed as template syntax
    missingMarkers = sorted({marker for marker in MARKER_PATTERN.findall(template) if marker not in variables})
    if missingMarkers:
        raise KeyError(f"Prompt template markers have no value: {', '.join(missingMarkers)}.")
    return MARKER_PATTERN.sub(lambda match: str(variables[match.group(1)]), template)


def formatMetric(value: Optional[float], digits: int = 1) -> str:
    if value is None or not math.isfinite(value):
        return NULL_MARKER
    return f"{value:.{digits}f}"


def formatFlag(flag: bool) -> str:
    return "TRUE" if flag else "FALSE"


def formatTrend(delta: int) -> str:
    return f"+{delta}" if delta > 0 else str(delta)


def getWeightedContributions(riskMetrics: RiskMetrics, subscores: NormalizedSubscores) -> dict[str, float]:
    # Contribution of each domain to CRI on the 0-100 scale: w_j * S_j
    return {
        domain: riskMetrics.domain_weights[domain] * getattr(subscores, domain) * DATABASE_SCORE_SCALE
        for domain in RISK_DOMAINS
    }


def calculatePercentageContributions(contributions: dict[str, float]) -> dict[str, float]:
    total = math.fsum(contributions.values())
    if total <= 0.0:
        return {domain: 0.0 for domain in RISK_DOMAINS}
    return {domain: round(contributions[domain] / total * 100.0, CONTRIBUTION_DECIMALS) for domain in RISK_DOMAINS}


def summarizeThreatCounts(raw: RawIndicators) -> list[dict[str, Any]]:
    # Exact per-class counts with a half-window trend so the LLM never has to count or invent events
    midpoint = raw.window_start + (raw.window_end - raw.window_start) / 2
    classes: dict[str, dict[str, Any]] = {}
    for event in raw.threat_events:
        entry = classes.setdefault(event.class_name, {"type": event.class_name, "count": 0, "earlier": 0,
                                                      "later": 0, "max_confidence": 0.0, "latest": event.detected_at})
        entry["count"] += 1
        entry["later" if event.detected_at >= midpoint else "earlier"] += 1
        entry["max_confidence"] = max(entry["max_confidence"], event.confidence)
        entry["latest"] = max(entry["latest"], event.detected_at)
    ranked = sorted(classes.values(), key=lambda entry: (entry["count"], entry["latest"]), reverse=True)
    return [
        {
            "type": entry["type"],
            "count": entry["count"],
            "trend": formatTrend(entry["later"] - entry["earlier"]),
            "max_confidence": round(entry["max_confidence"], 2),
            "latest_detected_at": entry["latest"].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for entry in ranked[:MAX_THREAT_CONTEXT_CLASSES]
    ]


def getThreatTargetCells(raw: RawIndicators, scope: AssessmentScope) -> list[str]:
    # Threat coordinates are snapped to the scope's H3 resolution; the scope cell is the fallback
    resolution = h3.get_resolution(scope.h3_cell)
    cellCounts: dict[str, int] = {}
    for event in raw.threat_events:
        if event.latitude is not None and event.longitude is not None:
            cell = h3.latlng_to_cell(event.latitude, event.longitude, resolution)
            cellCounts[cell] = cellCounts.get(cell, 0) + 1
    ranked = sorted(cellCounts, key=lambda cell: cellCounts[cell], reverse=True)[:MAX_TARGET_CELLS]
    return ranked or [scope.h3_cell]


def detectKnowledgeLoss(subscores: NormalizedSubscores) -> bool:
    components = subscores.score_components.get("behavior", {})
    elderAbsence = components.get("elder_absence_fraction")
    orphanDays = components.get("orphan_group_days")
    return (elderAbsence is not None and elderAbsence >= KNOWLEDGE_LOSS_ELDER_ABSENCE) or bool(orphanDays)


def describeBehaviorContext(subscores: NormalizedSubscores) -> str:
    components = subscores.score_components.get("behavior", {})
    lines = [
        f"Migration corridor retention (0-1): {formatMetric(components.get('corridor_retention'), 2)}",
        f"Elder absence fraction (0-1): {formatMetric(components.get('elder_absence_fraction'), 2)}",
        f"Days with orphaned juvenile groups: {formatMetric(components.get('orphan_group_days'), 0)}",
        f"Knowledge loss indicated: {formatFlag(detectKnowledgeLoss(subscores))}",
    ]
    return "\n".join(lines)


def describeBehaviorSummary(subscores: NormalizedSubscores) -> str:
    if "behavior" in subscores.missing_domains:
        return f"{INSUFFICIENT_DATA}."
    components = subscores.score_components.get("behavior", {})
    parts: list[str] = []
    if components.get("corridor_retention") is not None:
        parts.append(f"migration corridor retention is {components['corridor_retention']:.2f}")
    if components.get("elder_absence_fraction") is not None:
        parts.append(f"elder absence fraction is {components['elder_absence_fraction']:.2f}")
    if components.get("orphan_group_days"):
        parts.append(f"orphaned juvenile groups were observed on {components['orphan_group_days']:.0f} days")
    return (f"Behavioural components: {'; '.join(parts)}." if parts else "No behavioural component detail was recorded.")


# Mitigation selection

def selectAuthorizedMitigations(state: dict[str, Any], primaryDriver: str) -> list[dict[str, Any]]:
    scope: AssessmentScope = state["assessment_scope"]
    subscores: NormalizedSubscores = state["normalized_subscores"]
    riskMetrics: RiskMetrics = state["risk_metrics"]
    raw: RawIndicators = state["raw_indicators"]
    scaled = {domain: getattr(subscores, domain) * DATABASE_SCORE_SCALE for domain in RISK_DOMAINS}
    selected: list[str] = []
    targetCells: dict[str, list[str]] = {}

    if riskMetrics.critical_inbreeding_flag or (
        scaled["genetics"] >= CRITICAL_SUBSCORE_THRESHOLD and "genetics" not in subscores.missing_domains
    ):
        selected.append("GENETIC_RESCUE")
    if (
        scaled["behavior"] >= CRITICAL_SUBSCORE_THRESHOLD
        and "behavior" not in subscores.missing_domains
        and detectKnowledgeLoss(subscores)
    ):
        selected.extend(["NEURAL_CONSERVATION_TRAINING", "HERD_REINTEGRATION"])
    if (
        riskMetrics.immediate_poaching_threat
        or primaryDriver == "threat"
        or scaled["threat"] >= CRITICAL_SUBSCORE_THRESHOLD
    ):
        selected.append("ANTI_POACHING_DRONE_DEPLOYMENT")
        targetCells["ANTI_POACHING_DRONE_DEPLOYMENT"] = getThreatTargetCells(raw, scope)
    if not selected:
        selected.append("CONTINUED_MONITORING")

    return [
        {
            "strategy_id": strategyID,
            "title": MITIGATION_MATRIX[strategyID]["title"],
            "domain": MITIGATION_MATRIX[strategyID]["domain"],
            "tactic": MITIGATION_MATRIX[strategyID]["tactic"],
            "trigger": MITIGATION_MATRIX[strategyID]["trigger"],
            "target_h3_cells": targetCells.get(strategyID, []),
        }
        for strategyID in selected
    ]


# Prompt context construction

def buildPromptContext(state: dict[str, Any]) -> dict[str, Any]:
    scope: Optional[AssessmentScope] = state.get("assessment_scope")
    subscores: Optional[NormalizedSubscores] = state.get("normalized_subscores")
    riskMetrics: Optional[RiskMetrics] = state.get("risk_metrics")
    raw: Optional[RawIndicators] = state.get("raw_indicators")
    if scope is None or subscores is None or riskMetrics is None or raw is None:
        raise StateValidationError("Explainability prompts require assessment_scope, raw_indicators, subscores and risk_metrics.")

    contributions = getWeightedContributions(riskMetrics, subscores)
    percentages = calculatePercentageContributions(contributions)
    primaryDriver = max(RISK_DOMAINS, key=lambda domain: contributions[domain])
    mitigations = selectAuthorizedMitigations(state, primaryDriver)
    threatCounts = summarizeThreatCounts(raw)
    momentum = riskMetrics.momentum_vector
    momentumText = (
        f"{momentum.trajectory}, slope {momentum.slope_per_day * DATABASE_SCORE_SCALE:+.3f} CRI points per day, "
        f"delta {momentum.delta_cri * DATABASE_SCORE_SCALE:+.2f} over {momentum.window_days:.1f} days"
        if momentum.previous_cri is not None
        else f"{momentum.trajectory} ({NULL_MARKER}: no previous assessment for this cell)"
    )
    variables: dict[str, Any] = {
        "SPECIES": scope.scientific_name or f"species {scope.species_id}",
        "H3_CELL": scope.h3_cell,
        "PERIOD_START": scope.period_start.date().isoformat(),
        "PERIOD_END": scope.period_end.date().isoformat(),
        "CRI_SCORE": formatMetric(riskMetrics.conservation_risk_index * DATABASE_SCORE_SCALE),
        "ESCALATION_LEVEL": riskMetrics.escalation_level,
        "MOMENTUM_VECTOR": momentumText,
        "IPI_SCORE": formatMetric(riskMetrics.intervention_priority_index),
        "IPI_TIER": riskMetrics.intervention_priority_tier or NULL_MARKER,
        "INBREEDING_PENALTY_INDEX": formatMetric(riskMetrics.inbreeding_penalty_index),
        "MISSING_DOMAINS": ", ".join(subscores.missing_domains) or "none",
        "CONTRIBUTION_CONTEXT": json.dumps([
            {
                "domain": domain,
                "weight": round(riskMetrics.domain_weights[domain], 3),
                "subscore": round(getattr(subscores, domain) * DATABASE_SCORE_SCALE, 1),
                "weighted_points": round(contributions[domain], 2),
                "percentage": percentages[domain],
            }
            for domain in RISK_DOMAINS
        ]),
        "PRIMARY_DRIVER": primaryDriver,
        "PRIMARY_DRIVER_SHARE": f"{percentages[primaryDriver]:.{CONTRIBUTION_DECIMALS}f}",
        "EXPECTED_HETEROZYGOSITY": formatMetric(raw.expected_heterozygosity, 3),
        "EFFECTIVE_POPULATION_SIZE": formatMetric(raw.effective_population_size),
        "CRITICAL_NE_THRESHOLD": f"{CRITICAL_NE_THRESHOLD:g}",
        "CRITICAL_NE_FLAG": formatFlag(riskMetrics.critical_inbreeding_flag),
        "BEHAVIOR_CONTEXT": describeBehaviorContext(subscores),
        "BEHAVIOR_SUMMARY": describeBehaviorSummary(subscores),
        "THREAT_CONTEXT": json.dumps(threatCounts),
        "THREAT_EVENT_TOTAL": len(raw.threat_events),
        "IMMEDIATE_POACHING_FLAG": formatFlag(riskMetrics.immediate_poaching_threat),
        "MITIGATION_MATRIX": json.dumps(mitigations, indent=1),
        "ROUTE_FOCUS": ROUTE_FOCUS.get(routeOnRiskTriggers(state), DEFAULT_FOCUS),
        "FORMAT_INSTRUCTIONS": REPORT_PARSER.get_format_instructions(),
    }
    for domain, marker in SUBSCORE_MARKERS.items():
        variables[marker] = formatMetric(getattr(subscores, domain) * DATABASE_SCORE_SCALE)

    return {
        "variables": variables,
        "percentages": percentages,
        "primary_driver": primaryDriver,
        "mitigations": mitigations,
        "critical_inbreeding_flag": riskMetrics.critical_inbreeding_flag,
        "genetics_missing": raw.expected_heterozygosity is None and raw.effective_population_size is None,
        "behavior_missing": "behavior" in subscores.missing_domains,
    }


def buildPromptMessages(promptContext: dict[str, Any]) -> list[BaseMessage]:
    variables = promptContext["variables"]
    return [
        SystemMessage(content=compilePromptTemplate(SYSTEM_PROMPT, variables)),
        HumanMessage(content=compilePromptTemplate(HUMAN_PROMPT_TEMPLATE, variables)),
    ]


# Post-parse validation against the payload

def validateReportAgainstContext(report: ExplainabilityReport, promptContext: dict[str, Any]) -> ExplainabilityReport:
    expected = promptContext["percentages"]
    for entry in report.percentage_contributions:
        if abs(entry.percentage - expected[entry.domain]) > CONTRIBUTION_TOLERANCE:
            raise ReportValidationError(
                f"{entry.domain} contribution {entry.percentage} does not match the calculated {expected[entry.domain]}."
            )
    if promptContext["critical_inbreeding_flag"] and FORBIDDEN_SAFE_PATTERN.search(report.genetic_status_breakdown):
        raise ReportValidationError("genetic_status_breakdown describes the population as safe while the Ne flag is raised.")
    if promptContext["genetics_missing"] and INSUFFICIENT_DATA.lower() not in report.genetic_status_breakdown.lower():
        raise ReportValidationError(f"genetic_status_breakdown must state '{INSUFFICIENT_DATA}' when He and Ne are NULL.")
    if promptContext["behavior_missing"] and INSUFFICIENT_DATA.lower() not in report.behavioral_loss_impact.lower():
        raise ReportValidationError(f"behavioral_loss_impact must state '{INSUFFICIENT_DATA}' when behaviour data is missing.")

    authorised = {entry["strategy_id"]: set(entry["target_h3_cells"]) for entry in promptContext["mitigations"]}
    for recommendation in report.recommended_mitigations:
        if recommendation.strategy_id not in authorised:
            raise ReportValidationError(f"{recommendation.strategy_id} is not an authorised mitigation for this payload.")
        unlistedCells = set(recommendation.target_h3_cells) - authorised[recommendation.strategy_id]
        if unlistedCells:
            raise ReportValidationError(f"{recommendation.strategy_id} targets unlisted H3 cells: {sorted(unlistedCells)}.")
    return report


# Fallback generation

def buildFallbackReport(promptContext: dict[str, Any]) -> ExplainabilityReport:
    variables = promptContext["variables"]
    fallback = {key: compilePromptTemplate(template, variables) for key, template in FALLBACK_REPORT_TEMPLATE.items()}
    if promptContext["genetics_missing"]:
        geneticText = f"{INSUFFICIENT_DATA}. {fallback['genetic_status_breakdown']}"
    elif promptContext["critical_inbreeding_flag"]:
        geneticText = fallback["genetic_critical"]
    else:
        geneticText = fallback["genetic_status_breakdown"]
    return ExplainabilityReport(
        executive_summary=f"{fallback['summary']} {fallback['executive_summary']}",
        primary_threat_drivers=fallback["primary_threat_drivers"],
        percentage_contributions=[
            DomainContribution(domain=domain, percentage=promptContext["percentages"][domain]) for domain in RISK_DOMAINS
        ],
        genetic_status_breakdown=geneticText,
        behavioral_loss_impact=fallback["behavioral_loss_impact"],
        recommended_mitigations=[
            MitigationRecommendation(
                strategy_id=entry["strategy_id"],
                rationale=f"{entry['tactic']} Trigger: {entry['trigger']}.",
                target_h3_cells=entry["target_h3_cells"],
            )
            for entry in promptContext["mitigations"]
        ],
    )


def formatMitigations(report: ExplainabilityReport) -> str:
    lines = []
    for recommendation in report.recommended_mitigations:
        cells = f" [{', '.join(recommendation.target_h3_cells)}]" if recommendation.target_h3_cells else ""
        lines.append(f"{MITIGATION_MATRIX[recommendation.strategy_id]['title']}{cells}: {recommendation.rationale}")
    return " ".join(lines)


# Demo

def buildDemoState() -> dict[str, Any]:
    from aiEngine.stateSchema import DOMAIN_WEIGHTS, MomentumVector, ThreatEvent, calculateConservationRiskIndex

    now = datetime.now(timezone.utc)
    h3Cell = h3.latlng_to_cell(-2.3333, 34.8333, 7)
    scope = AssessmentScope(
        species_id=1,
        scientific_name="Loxodonta africana",
        h3_cell=h3Cell,
        period_start=now - timedelta(days=30),
        period_end=now,
    )
    raw = RawIndicators(
        aggregated_at=now,
        window_start=scope.period_start,
        window_end=scope.period_end,
        effective_population_size=42.0,
        expected_heterozygosity=0.41,
        threat_events=tuple(
            ThreatEvent(detected_at=now - timedelta(days=days), class_name=className, confidence=0.9,
                        latitude=-2.3333 + offset, longitude=34.8333)
            for days, className, offset in [(1, "gunshot", 0.0), (3, "gunshot", 0.01), (20, "gunshot", 0.0),
                                            (2, "chainsaw", 0.02), (25, "vehicle_engine", 0.0)]
        ),
    )
    subscores = NormalizedSubscores(
        population=0.55, habitat=0.40, threat=0.88, climate=0.30, genetics=1.0, behavior=0.78,
        score_components={"behavior": {"corridor_retention": 0.45, "elder_absence_fraction": 0.7, "orphan_group_days": 4.0}},
    )
    cri = calculateConservationRiskIndex(subscores, DOMAIN_WEIGHTS)
    riskMetrics = RiskMetrics(
        conservation_risk_index=cri,
        inbreeding_penalty_index=100.0,
        intervention_priority_index=cri * 100.0 * 1.2,
        priority_components={"momentum_factor": 1.2, "feasibility_multiplier": 1.0},
        momentum_vector=MomentumVector.fromHistory(cri, cri - 0.06),
        domain_weights=dict(DOMAIN_WEIGHTS),
        effective_population_size=42.0,
        critical_inbreeding_flag=True,
        immediate_poaching_threat=True,
    )
    return {"assessment_scope": scope, "raw_indicators": raw, "normalized_subscores": subscores, "risk_metrics": riskMetrics}


def main() -> None:
    promptContext = buildPromptContext(buildDemoState())
    messages = buildPromptMessages(promptContext)
    print("=== System prompt ===")
    print(messages[0].content.split("The output should be formatted")[0].strip())
    print("\n=== Payload ===")
    print(messages[1].content)
    print("\n=== Fallback report ===")
    print(json.dumps(buildFallbackReport(promptContext).model_dump(mode="json"), indent=2))


if __name__ == "__main__":
    main()
