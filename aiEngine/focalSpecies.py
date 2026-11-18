LANGUR = "Semnopithecus ajax"
PRIMARY_DOMAINS = {
    "population": "Population Health",
    "habitat": "Habitat Destruction",
    "threat": "Human Threats",
    "genetics": "Genetic Resilience",
}


def primaryScores(
    subscores: dict, missingDomains: list, components: dict, evidenceKnown: bool
) -> list[dict]:
    return [
        {
            "domain": domain,
            "label": label,
            "risk": subscores.get(domain) if evidenceKnown and domain not in missingDomains else None,
            "status": (
                "unknown" if not evidenceKnown else
                "unavailable" if domain in missingDomains or subscores.get(domain) is None else
                "available"
            ),
            "components": components.get(domain, {}),
        }
        for domain, label in PRIMARY_DOMAINS.items()
    ]
