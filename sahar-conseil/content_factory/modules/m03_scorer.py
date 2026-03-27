"""
Module 03 — Scorer d'intérêt éditorial
Calcule un score 0-100 combinant SERP, tendances et pertinence SAHAR.
Décision : publier si score >= SEUIL (défaut 60).
"""

import logging, re
from typing import Optional

logger = logging.getLogger(__name__)

SEUIL_PUBLICATION = 60  # Score minimum pour déclencher la génération

# Mots-clés SAHAR à fort potentiel — bonus si présents
SAHAR_KEYWORDS = [
    "dvf", "dpe", "passoire", "thermique", "immobilier", "rénovation",
    "prospection", "scoring", "pipeline", "crm", "données publiques",
    "insee", "sirene", "open data", "artisan", "rge", "franchise",
    "recrutement", "tension", "marché immobilier",
]


def score(keyword: str, serp_data: dict, trends_data: dict) -> dict:
    """
    Score composite 0-100 :
      - Tendance Google Trends       : 35 pts
      - Richesse SERP / PAA          : 30 pts
      - Pertinence thématique SAHAR  : 25 pts
      - Concurrence SERP (inverse)   : 10 pts
    """
    kw_lower = keyword.lower()

    # ── 1. Score tendance (35 pts) ───────────────────────────────────────────
    s_trend = trends_data.get("score_tendance", 40)
    direction = trends_data.get("direction", "stable")
    score_tendance = (s_trend / 100) * 35
    if direction == "hausse":
        score_tendance = min(35, score_tendance * 1.2)

    # ── 2. Richesse SERP / PAA (30 pts) ─────────────────────────────────────
    nb_paa = serp_data.get("paa_count", 0)
    nb_organic = serp_data.get("organic_count", 0)
    nb_related = len(serp_data.get("related", []))

    # PAA = signal fort d'intention informationnelle = bon pour blog
    score_serp = min(30, (nb_paa * 4) + (nb_related * 1.5) + (nb_organic > 5) * 5)

    # ── 3. Pertinence thématique SAHAR (25 pts) ──────────────────────────────
    matches = sum(1 for kw in SAHAR_KEYWORDS if kw in kw_lower)
    score_pertinence = min(25, matches * 8)

    # Bonus si mot-clé contient termes très ciblés
    high_value = ["dvf", "dpe", "passoire", "scoring prospect", "pipeline crm"]
    if any(hv in kw_lower for hv in high_value):
        score_pertinence = min(25, score_pertinence + 10)

    # ── 4. Concurrence SERP (10 pts) — moins c'est concurrentiel, mieux c'est ─
    # Heuristique : gros sites (seloger, leboncoin, service-public) = forte concurrence
    big_domains = ["seloger.com", "leboncoin.fr", "service-public.fr", "legifrance.gouv.fr",
                   "meilleursagents.com", "pap.fr", "notaires.fr", "logic-immo.com"]
    organic = serp_data.get("organic", [])
    nb_big = sum(1 for r in organic if any(bd in r.get("domain","") for bd in big_domains))
    score_concurrence = max(0, 10 - nb_big * 2)

    # ── Total ────────────────────────────────────────────────────────────────
    total = score_tendance + score_serp + score_pertinence + score_concurrence
    total = round(min(100, max(0, total)), 1)

    decision = "PUBLIER" if total >= SEUIL_PUBLICATION else "IGNORER"

    result = {
        "keyword": keyword,
        "score_total": total,
        "decision": decision,
        "seuil": SEUIL_PUBLICATION,
        "detail": {
            "tendance": round(score_tendance, 1),
            "serp_richesse": round(score_serp, 1),
            "pertinence_sahar": round(score_pertinence, 1),
            "concurrence": round(score_concurrence, 1),
        },
        "meta": {
            "direction_tendance": direction,
            "nb_paa": nb_paa,
            "nb_related": nb_related,
            "nb_big_competitors": nb_big,
            "kw_matches": matches,
        }
    }

    logger.info(
        f"Score '{keyword}': {total}/100 → {decision} "
        f"(trend={score_tendance:.0f} serp={score_serp:.0f} "
        f"pertinence={score_pertinence:.0f} conc={score_concurrence:.0f})"
    )
    return result


if __name__ == "__main__":
    import sys, json, logging
    logging.basicConfig(level=logging.INFO)

    # Test avec données fictives
    fake_serp = {"organic_count": 8, "paa_count": 4, "related": ["a","b","c"], "organic": [
        {"domain": "seloger.com"}, {"domain": "blog-exemple.fr"}
    ]}
    fake_trends = {"score_tendance": 65, "direction": "hausse"}
    kw = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "DVF prix immobilier Bordeaux"
    print(json.dumps(score(kw, fake_serp, fake_trends), ensure_ascii=False, indent=2))
