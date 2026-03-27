"""
SAHAR Conseil — Content Factory
Module 01 : SERP + PAA Research

Deux modes :
  - ValueSERP API (recommandé, ~2€/mois pour 50 articles)
  - Scraping direct DuckDuckGo (gratuit, fallback)

Usage :
  python 01_research.py --keyword "passoires thermiques interdites 2025"
  python 01_research.py --all          # tous les seeds keywords.json
  python 01_research.py --keyword "..." --no-api  # force scraping direct

Output : output/research/{slug}.json
"""

import os
import re
import json
import time
import hashlib
import argparse
import requests
from pathlib import Path
from datetime import datetime
from urllib.parse import quote_plus, urlparse

# ── Chemins ──────────────────────────────────────────────────────────────────
HERE     = Path(__file__).parent.parent
CONFIG   = HERE / "config" / "keywords.json"
OUT_DIR  = HERE / "output" / "research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────
VALUESERP_KEY = os.getenv("VALUESERP_KEY", "")   # optionnel
USER_AGENT    = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "fr-FR,fr;q=0.9"}


# ─────────────────────────────────────────────────────────────────────────────
# VALUESERP — mode principal
# ─────────────────────────────────────────────────────────────────────────────

def fetch_valueserp(keyword: str, country: str = "fr") -> dict:
    """
    Appelle l'API ValueSERP.
    Retourne SERP top10 + PAA + related_searches.
    Docs : https://www.valueserp.com/docs
    """
    url = "https://api.valueserp.com/search"
    params = {
        "api_key":  VALUESERP_KEY,
        "q":        keyword,
        "location": "France",
        "gl":       country,
        "hl":       "fr",
        "num":      10,
        "output":   "json",
        "include_answer_box": True,
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return parse_valueserp(data, keyword)
    except Exception as e:
        print(f"  ⚠️  ValueSERP error: {e}")
        return {}


def parse_valueserp(data: dict, keyword: str) -> dict:
    """Extrait les infos utiles d'une réponse ValueSERP."""

    # SERP top 10
    organic = []
    for item in data.get("organic_results", [])[:10]:
        organic.append({
            "position": item.get("position"),
            "title":    item.get("title", ""),
            "url":      item.get("link", ""),
            "domain":   urlparse(item.get("link", "")).netloc,
            "snippet":  item.get("snippet", ""),
        })

    # People Also Ask
    paa = []
    for item in data.get("related_questions", []):
        paa.append({
            "question": item.get("question", ""),
            "answer":   item.get("answer", {}).get("snippet", ""),
        })

    # Related searches
    related = [
        item.get("query", "")
        for item in data.get("related_searches", [])
    ]

    # Answer box si présent
    answer_box = data.get("answer_box", {}).get("snippet", "")

    # Featured snippet
    featured = data.get("knowledge_graph", {}).get("description", "")

    return {
        "keyword":      keyword,
        "organic":      organic,
        "paa":          paa,
        "related":      related,
        "answer_box":   answer_box,
        "featured":     featured,
        "source":       "valueserp",
        "total_results": data.get("search_information", {}).get("total_results", 0),
    }


# ─────────────────────────────────────────────────────────────────────────────
# DUCKDUCKGO — fallback sans API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_duckduckgo(keyword: str) -> dict:
    """
    Scraping DuckDuckGo HTML (pas d'API key requise).
    Moins de données que ValueSERP mais suffisant pour le scoring.
    """
    print(f"  → Fallback DuckDuckGo pour : {keyword}")
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(keyword)}&kl=fr-fr"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return parse_duckduckgo(r.text, keyword)
    except Exception as e:
        print(f"  ⚠️  DuckDuckGo error: {e}")
        return {}


def parse_duckduckgo(html: str, keyword: str) -> dict:
    """Parse basique du HTML DuckDuckGo."""
    organic = []

    # Titres
    titles = re.findall(r'class="result__a"[^>]*>([^<]+)</a>', html)
    # URLs
    urls   = re.findall(r'class="result__url"[^>]*>\s*([^\s<]+)', html)
    # Snippets
    snips  = re.findall(r'class="result__snippet"[^>]*>([^<]+)', html)

    for i, title in enumerate(titles[:10]):
        organic.append({
            "position": i + 1,
            "title":    title.strip(),
            "url":      urls[i].strip() if i < len(urls) else "",
            "domain":   urls[i].strip().split("/")[0] if i < len(urls) else "",
            "snippet":  snips[i].strip() if i < len(snips) else "",
        })

    # PAA non disponible sur DDG — on génère depuis le keyword
    paa_seeds = _generate_paa_from_keyword(keyword)

    return {
        "keyword": keyword,
        "organic": organic,
        "paa":     paa_seeds,
        "related": [],
        "answer_box": "",
        "featured": "",
        "source":  "duckduckgo",
        "total_results": 0,
    }


def _generate_paa_from_keyword(keyword: str) -> list:
    """
    Génère des questions PAA vraisemblables depuis le keyword.
    Utilisé quand le scraping PAA n'est pas disponible.
    """
    prefixes = ["Comment", "Pourquoi", "Qu'est-ce que", "Quand", "Qui", "Combien"]
    paa = []
    words = keyword.split()
    for p in prefixes[:4]:
        paa.append({
            "question": f"{p} {' '.join(words[:4])} ?",
            "answer": "",
        })
    return paa


# ─────────────────────────────────────────────────────────────────────────────
# ANALYSE CONCURRENCE SERP
# ─────────────────────────────────────────────────────────────────────────────

STRONG_DOMAINS = {
    "seloger.com", "leboncoin.fr", "logic-immo.com", "bienici.com",
    "lemonde.fr", "lefigaro.fr", "service-public.fr", "legifrance.gouv.fr",
    "gouvernement.fr", "ademe.fr", "data.gouv.fr", "insee.fr",
    "wikipedia.org", "notaires.fr", "particulier.fr",
}

def analyse_competition(organic: list) -> dict:
    """
    Évalue la difficulté de la SERP.
    Retourne un score de concurrence 0-100 (plus c'est haut, plus c'est dur).
    """
    if not organic:
        return {"score": 50, "level": "inconnu", "dominant_domains": []}

    strong_count = 0
    domains      = []
    for r in organic[:10]:
        d = r.get("domain", "").lower().replace("www.", "")
        domains.append(d)
        if any(s in d for s in STRONG_DOMAINS):
            strong_count += 1

    # Score concurrence : % de domains forts × 100
    competition_score = min(100, int((strong_count / min(len(organic), 10)) * 100))

    level = (
        "faible"  if competition_score < 30 else
        "moyen"   if competition_score < 60 else
        "élevé"   if competition_score < 80 else
        "très élevé"
    )

    return {
        "score":            competition_score,
        "level":            level,
        "strong_count":     strong_count,
        "dominant_domains": list(set(domains))[:5],
    }


def extract_title_patterns(organic: list) -> list:
    """Extrait les patterns de titres les plus fréquents en SERP."""
    patterns = []
    for r in organic[:10]:
        t = r.get("title", "")
        # Détecter si le titre est un guide, une liste, une question…
        if re.search(r"\d+\s*(raisons|conseils|étapes|façons|astuces)", t, re.I):
            patterns.append("liste_numerotee")
        elif t.endswith("?") or t.startswith(("Comment", "Pourquoi", "Quand", "Qu'")):
            patterns.append("question")
        elif re.search(r"guide|tout savoir|comprendre", t, re.I):
            patterns.append("guide_complet")
        elif re.search(r"2024|2025|2026", t):
            patterns.append("date_recente")
        else:
            patterns.append("informatif")

    # Top pattern
    from collections import Counter
    top = Counter(patterns).most_common(3)
    return [p for p, _ in top]


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────

def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[àáâãäå]", "a", text)
    text = re.sub(r"[èéêë]",   "e", text)
    text = re.sub(r"[ìíîï]",   "i", text)
    text = re.sub(r"[òóôõö]",  "o", text)
    text = re.sub(r"[ùúûü]",   "u", text)
    text = re.sub(r"[ç]",      "c", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")[:60]


def research_keyword(keyword: str, use_api: bool = True) -> dict:
    """
    Lance la recherche complète pour un mot-clé.
    Retourne un dict enrichi prêt pour le scorer.
    """
    print(f"\n🔍 Recherche : {keyword}")
    print(f"   Mode : {'ValueSERP API' if (use_api and VALUESERP_KEY) else 'DuckDuckGo fallback'}")

    # Fetch SERP
    if use_api and VALUESERP_KEY:
        data = fetch_valueserp(keyword)
    else:
        data = fetch_duckduckgo(keyword)

    if not data:
        print(f"  ❌ Aucune donnée récupérée")
        return {}

    # Enrichissement
    organic     = data.get("organic", [])
    paa         = data.get("paa", [])
    competition = analyse_competition(organic)
    patterns    = extract_title_patterns(organic)

    # Métriques pour le scorer
    result = {
        # Identification
        "keyword":      keyword,
        "slug":         slugify(keyword),
        "timestamp":    datetime.now().isoformat(),
        "source":       data.get("source", "unknown"),

        # SERP
        "organic":      organic,
        "paa":          paa,
        "related":      data.get("related", []),
        "answer_box":   data.get("answer_box", ""),
        "featured":     data.get("featured", ""),
        "total_results": data.get("total_results", 0),

        # Analyse
        "competition":  competition,
        "title_patterns": patterns,
        "paa_count":    len(paa),
        "serp_count":   len(organic),

        # Méta pour le scorer
        "serp_metrics": {
            "competition_score": competition["score"],
            "competition_level": competition["level"],
            "paa_richness":      min(100, len(paa) * 12),   # 8 PAA = score max
            "has_answer_box":    bool(data.get("answer_box")),
            "top_titles":        [r.get("title", "") for r in organic[:3]],
        },
    }

    # Sauvegarde
    out_path = OUT_DIR / f"{slugify(keyword)}.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"  ✅ Sauvegardé : {out_path.name}")
    print(f"  📊 SERP : {len(organic)} résultats | PAA : {len(paa)} questions")
    print(f"  🏆 Concurrence : {competition['level']} ({competition['score']}/100)")

    return result


def research_all(use_api: bool = True, max_keywords: int = 10) -> list:
    """Lance la recherche sur tous les seeds du fichier keywords.json."""
    cfg = json.loads(CONFIG.read_text())
    keywords = sorted(cfg["keywords"], key=lambda x: x.get("priorite", 99))
    results  = []

    print(f"📋 {len(keywords)} mots-clés à traiter (max: {max_keywords})")
    for i, kw in enumerate(keywords[:max_keywords]):
        seed = kw["seed"]
        # Check si déjà traité aujourd'hui
        out  = OUT_DIR / f"{slugify(seed)}.json"
        if out.exists():
            age = (datetime.now() - datetime.fromtimestamp(out.stat().st_mtime)).seconds
            if age < 86400:
                print(f"  ⏭  Déjà traité aujourd'hui : {seed}")
                results.append(json.loads(out.read_text()))
                continue

        result = research_keyword(seed, use_api=use_api)
        if result:
            # Ajouter les métadonnées du seed
            result["secteur"]   = kw.get("secteur", "")
            result["cible"]     = kw.get("cible", "")
            result["url_cible"] = kw.get("url_cible", "")
            result["priorite"]  = kw.get("priorite", 99)
            # Resauvegarder avec les métadonnées
            out = OUT_DIR / f"{slugify(seed)}.json"
            out.write_text(json.dumps(result, ensure_ascii=False, indent=2))
            results.append(result)

        # Rate limiting
        if i < len(keywords) - 1:
            time.sleep(1.5)

    print(f"\n✅ {len(results)} recherches terminées → {OUT_DIR}")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAHAR Content Factory — Module 01 Research")
    parser.add_argument("--keyword",  type=str, help="Mot-clé unique à analyser")
    parser.add_argument("--all",      action="store_true", help="Traiter tous les seeds")
    parser.add_argument("--no-api",   action="store_true", help="Forcer DuckDuckGo (sans ValueSERP)")
    parser.add_argument("--max",      type=int, default=10, help="Max keywords (mode --all)")
    args = parser.parse_args()

    use_api = not args.no_api

    if args.keyword:
        research_keyword(args.keyword, use_api=use_api)
    elif args.all:
        research_all(use_api=use_api, max_keywords=args.max)
    else:
        # Mode démo — keyword de test
        print("Mode démo — keyword test")
        research_keyword("passoires thermiques interdites location 2025", use_api=False)
