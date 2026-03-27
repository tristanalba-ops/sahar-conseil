"""
Module 01 — SERP + PAA Research
Scrape les 10 premiers résultats SERP + les PAA (People Also Ask)
pour un mot-clé donné.

Stratégie :
  - ValueSERP si SERP_API_KEY configuré (~0.00125$/req)
  - Fallback : scraping direct DuckDuckGo (gratuit, moins complet)
"""

import os, json, time, re, logging
from pathlib import Path
from typing import Optional
import requests

logger = logging.getLogger(__name__)


# ── ValueSERP ────────────────────────────────────────────────────────────────

def fetch_serp_valueserp(keyword: str, api_key: str, gl="fr", hl="fr") -> dict:
    """Requête ValueSERP API — retourne résultats organiques + PAA."""
    url = "https://api.valueserp.com/search"
    params = {
        "api_key": api_key,
        "q": keyword,
        "gl": gl,
        "hl": hl,
        "num": 10,
        "output": "json",
        "include_answer_box": True,
        "include_related_searches": True,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def parse_valueserp(data: dict) -> dict:
    """Extrait résultats organiques, PAA, recherches associées."""
    organic = []
    for item in data.get("organic_results", []):
        organic.append({
            "position": item.get("position"),
            "title": item.get("title", ""),
            "url": item.get("link", ""),
            "snippet": item.get("snippet", ""),
            "domain": item.get("domain", ""),
        })

    paa = []
    for item in data.get("people_also_ask", []):
        paa.append({
            "question": item.get("question", ""),
            "answer": item.get("answer", ""),
        })

    related = [r.get("query", "") for r in data.get("related_searches", [])]

    return {"organic": organic, "paa": paa, "related": related}


# ── Fallback DuckDuckGo ──────────────────────────────────────────────────────

def fetch_serp_duckduckgo(keyword: str) -> dict:
    """Fallback gratuit via DuckDuckGo HTML scraping."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; SAHARBot/1.0)"}
    url = "https://html.duckduckgo.com/html/"
    data = {"q": keyword, "kl": "fr-fr"}

    try:
        r = requests.post(url, data=data, headers=headers, timeout=20)
        html = r.text

        # Extraire les résultats
        from html.parser import HTMLParser

        class DDGParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.results = []
                self._in_result = False
                self._in_title = False
                self._current = {}
                self._depth = 0

            def handle_starttag(self, tag, attrs):
                attrs = dict(attrs)
                cls = attrs.get("class", "")
                if "result__title" in cls:
                    self._in_title = True
                    self._current = {}
                if "result__snippet" in cls:
                    self._in_snippet = True
                if tag == "a" and self._in_title:
                    self._current["url"] = attrs.get("href", "")

            def handle_data(self, data):
                if self._in_title and data.strip():
                    self._current["title"] = data.strip()
                    self._in_title = False
                    self.results.append(self._current)

        parser = DDGParser()
        parser.feed(html)

        organic = [
            {"position": i+1, "title": r.get("title",""), "url": r.get("url",""), "snippet": "", "domain": ""}
            for i, r in enumerate(parser.results[:10])
        ]

        return {"organic": organic, "paa": [], "related": []}

    except Exception as e:
        logger.warning(f"DuckDuckGo fallback failed: {e}")
        return {"organic": [], "paa": [], "related": []}


# ── Interface publique ────────────────────────────────────────────────────────

def research(keyword: str, api_key: Optional[str] = None) -> dict:
    """
    Point d'entrée principal.
    Retourne dict avec : organic, paa, related, keyword, source.
    """
    api_key = api_key or os.getenv("SERP_API_KEY", "")

    logger.info(f"Research: '{keyword}' (api={'valueserp' if api_key else 'duckduckgo'})")

    if api_key:
        try:
            raw = fetch_serp_valueserp(keyword, api_key)
            result = parse_valueserp(raw)
            result["source"] = "valueserp"
        except Exception as e:
            logger.warning(f"ValueSERP error: {e} — fallback DDG")
            result = fetch_serp_duckduckgo(keyword)
            result["source"] = "duckduckgo_fallback"
    else:
        result = fetch_serp_duckduckgo(keyword)
        result["source"] = "duckduckgo"

    result["keyword"] = keyword
    result["organic_count"] = len(result.get("organic", []))
    result["paa_count"] = len(result.get("paa", []))

    logger.info(f"  → {result['organic_count']} résultats, {result['paa_count']} PAA, source={result['source']}")
    return result


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    kw = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "DVF prix immobilier"
    r = research(kw)
    print(json.dumps(r, ensure_ascii=False, indent=2)[:2000])
