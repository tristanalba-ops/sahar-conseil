"""
SAHAR Conseil — Content Factory
Module 03 : Scoring d'intérêt contenu

Score 0-100 basé sur 4 dimensions :
  - Tendance Google (40 pts)  → signal demande réelle
  - Richesse PAA    (25 pts)  → angle questions à répondre
  - Concurrence     (20 pts)  → inversé : faible concurrence = bonne opportunité
  - Pertinence SAHAR(15 pts)  → est-ce dans notre territoire éditorial ?

Seuil de publication : score ≥ 60

Usage :
  python 03_scorer.py --slug passoires-thermiques-interdites-location-2025
  python 03_scorer.py --all
  python 03_scorer.py --all --min-score 70
"""

import json
import argparse
from pathlib import Path
from datetime import datetime

HERE    = Path(__file__).parent.parent
OUT_DIR = HERE / "output" / "research"
CFG     = json.loads((HERE / "config" / "keywords.json").read_text())
SETTINGS = CFG.get("settings", {})
SCORE_MIN = SETTINGS.get("score_minimum", 60)

# Domaines éditoriaux SAHAR → pondération pertinence
SAHAR_TOPICS = {
    "dvf":                 15, "transaction":        12, "immobilier":         12,
    "prix m2":             12, "agent immobilier":   10, "mandat":             10,
    "investisseur":        10, "sous-valoris":       15,
    "dpe":                 15, "passoire":           15, "thermique":          12,
    "rénovation":          12, "artisan rge":        15, "maprimerenov":       12,
    "logement f":          15, "logement g":         15, "interdit location":  15,
    "crm":                 12, "pipeline":           12, "prospect":           12,
    "scoring":             12, "open data":          12, "données publiques":  12,
    "franchise":           10, "implantation":       10, "zone commerciale":   10,
    "recrutement":         10, "tension emploi":     10, "dares":              12,
    "insee":               10, "sirene":             10, "data.gouv":          10,
}


def score_trends(trends: dict) -> int:
    """40 pts max — signal demande réelle."""
    if not trends or trends.get("source", "").startswith("fallback"):
        return 20  # score neutre si pas de données

    ts   = trends.get("trend_score", 50)
    dir_ = trends.get("direction", "stable")
    near = trends.get("near_peak", False)

    # Base score sur la moyenne
    base = int(ts * 0.4)   # max 40

    # Bonus direction
    if dir_ == "montante": base = min(40, base + 6)
    if near:               base = min(40, base + 4)
    if dir_ == "descendante": base = max(0, base - 8)

    return base


def score_paa(paa: list) -> int:
    """25 pts max — richesse des questions PAA."""
    n = len(paa)
    if n == 0:  return 5
    if n == 1:  return 10
    if n == 2:  return 15
    if n <= 4:  return 18
    if n <= 6:  return 22
    return 25   # 7+ PAA = max


def score_competition(competition: dict) -> int:
    """
    20 pts max — INVERSÉ : faible concurrence = bon score.
    On cherche des mots-clés où on peut se positionner.
    """
    c = competition.get("score", 50) if competition else 50
    # Concurrence 0 → 20 pts / Concurrence 100 → 0 pts
    return int((1 - c / 100) * 20)


def score_relevance(keyword: str, secteur: str = "") -> int:
    """15 pts max — pertinence territoire éditorial SAHAR."""
    kw_lower = keyword.lower()
    sec_lower = secteur.lower()
    best = 0
    for topic, pts in SAHAR_TOPICS.items():
        if topic in kw_lower or topic in sec_lower:
            best = max(best, pts)
    return min(15, best)


def compute_score(data: dict) -> dict:
    """
    Calcule le score final et les sous-scores.
    Retourne un dict enrichi avec la décision publish/skip.
    """
    keyword    = data.get("keyword", "")
    secteur    = data.get("secteur", "")
    trends     = data.get("trends", {})
    paa        = data.get("paa", [])
    competition = data.get("competition", {})

    s_trends  = score_trends(trends)
    s_paa     = score_paa(paa)
    s_comp    = score_competition(competition)
    s_rel     = score_relevance(keyword, secteur)
    total     = s_trends + s_paa + s_comp + s_rel

    decision  = "publish" if total >= SCORE_MIN else "skip"

    # Recommandation format article
    paa_count = len(paa)
    if paa_count >= 6:
        format_rec = "guide_complet"   # long-form 1500+ mots, sections FAQ
    elif paa_count >= 3:
        format_rec = "article_moyen"   # 900-1200 mots
    else:
        format_rec = "article_court"   # 600-900 mots

    score_data = {
        "total":         total,
        "decision":      decision,
        "details": {
            "trends":      s_trends,
            "paa":         s_paa,
            "competition": s_comp,
            "relevance":   s_rel,
        },
        "format_rec":    format_rec,
        "seuil":         SCORE_MIN,
        "timestamp":     datetime.now().isoformat(),
    }

    print(f"  📊 Score total    : {total}/100 → {decision.upper()}")
    print(f"     Tendance       : {s_trends}/40")
    print(f"     PAA ({paa_count} q.)   : {s_paa}/25")
    print(f"     Concurrence    : {s_comp}/20 (niveau: {competition.get('level','?')})")
    print(f"     Pertinence     : {s_rel}/15")
    print(f"     Format recommandé : {format_rec}")

    return score_data


def score_file(slug: str) -> dict | None:
    """Charge, score et sauvegarde un fichier research."""
    path = OUT_DIR / f"{slug}.json"
    if not path.exists():
        print(f"❌ Fichier introuvable : {path}")
        return None

    data  = json.loads(path.read_text())
    kw    = data.get("keyword", slug)
    print(f"\n🎯 Scoring : {kw}")

    score = compute_score(data)
    data["score"] = score
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    return data


def score_all(min_score: int = 0) -> list:
    """Score tous les fichiers research. Retourne ceux qui passent le seuil."""
    files    = sorted(OUT_DIR.glob("*.json"))
    publish  = []
    skip     = []

    for f in files:
        data = json.loads(f.read_text())
        if "score" not in data:
            data = score_file(f.stem) or data

        total = data.get("score", {}).get("total", 0)
        if total >= max(min_score, SCORE_MIN):
            publish.append(data)
        else:
            skip.append(data)

    print(f"\n── RÉSUMÉ SCORING ──────────────────────────────")
    print(f"  ✅ À publier : {len(publish)}")
    for d in publish:
        s = d.get("score", {})
        print(f"     [{s.get('total','?'):>3}/100] {d.get('keyword','')}")
    print(f"  ⏭  Ignorés  : {len(skip)}")
    for d in skip:
        s = d.get("score", {})
        print(f"     [{s.get('total','?'):>3}/100] {d.get('keyword','')}")

    return publish


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug",      type=str)
    parser.add_argument("--all",       action="store_true")
    parser.add_argument("--min-score", type=int, default=0)
    args = parser.parse_args()

    if args.slug:
        score_file(args.slug)
    elif args.all:
        score_all(args.min_score)
    else:
        print("Usage: python 03_scorer.py --slug <slug> | --all [--min-score 70]")
