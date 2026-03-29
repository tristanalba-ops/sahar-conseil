#!/usr/bin/env python3
"""
SAHAR Conseil — MCP Server (FastMCP)
Expose les outils d'enrichissement géographique et d'analyse DPE comme tools MCP,
utilisables directement depuis Claude Desktop ou tout client MCP.

Prérequis :
  pip install fastmcp anthropic requests --break-system-packages

Lancement :
  python mcp_sahar.py           # mode stdio (Claude Desktop)
  python mcp_sahar.py --http    # mode HTTP SSE sur :8000

Config Claude Desktop (~/.config/claude/claude_desktop_config.json) :
  {
    "mcpServers": {
      "sahar": {
        "command": "python",
        "args": ["/chemin/vers/mcp_sahar.py"],
        "env": {
          "ANTHROPIC_API_KEY": "sk-ant-...",
          "SUPABASE_KEY": "..."
        }
      }
    }
  }
"""

import os, sys, json, io, csv, logging
from typing import Optional

# ── FastMCP ───────────────────────────────────────────────────────────────────
try:
    from fastmcp import FastMCP
except ImportError:
    print("FastMCP non installé : pip install fastmcp --break-system-packages", file=sys.stderr)
    sys.exit(1)

import requests
from anthropic import Anthropic

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL  = os.getenv("SUPABASE_URL", "https://ylrrcbklufshebcizgus.supabase.co")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlscnJjYmtsdWZzaGViY2l6Z3VzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ1NjQzNTEsImV4cCI6MjA5MDE0MDM1MX0.KQjvB5aePbmCcrAu9yYKoIblDG0ui90LXa-DcL7HAEA")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

GEO_API = "https://geo.api.gouv.fr"
BAN_API = "https://api-adresse.data.gouv.fr"

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

logging.basicConfig(level=logging.WARNING)

# ── MCP Server ────────────────────────────────────────────────────────────────
mcp = FastMCP(
    name="sahar-conseil",
    description="SAHAR Conseil — Enrichissement géographique open data (DPE ADEME, DVF, geo.api.gouv.fr, BAN)",
    version="1.0.0",
)

# ── Helpers internes ──────────────────────────────────────────────────────────
def _geo(code: str) -> dict:
    r = requests.get(f"{GEO_API}/communes/{code}", params={
        "fields": "nom,code,codeDepartement,codeRegion,codesPostaux,population,surface,centre"
    }, timeout=10)
    r.raise_for_status()
    return r.json()

def _dpe_commune(code: str) -> dict:
    r = requests.get(f"{SUPABASE_URL}/rest/v1/dpe_communes", headers=SB_HEADERS,
        params={"code_insee": f"eq.{code}", "select": "*", "limit": "1"}, timeout=10)
    rows = r.json()
    return rows[0] if isinstance(rows, list) and rows else {}

def _dpe_logements(code: str, limit: int = 10) -> list:
    r = requests.get(f"{SUPABASE_URL}/rest/v1/dpe_logements", headers=SB_HEADERS, params={
        "code_insee": f"eq.{code}", "etiquette_dpe": "in.(F,G)",
        "select": "etiquette_dpe,adresse,conso_par_m2,type_batiment,periode_construction",
        "order": "conso_par_m2.desc.nullslast", "limit": str(limit),
    }, timeout=15)
    result = r.json()
    return result if isinstance(result, list) else []

def _ban_commune(code: str, nom: str) -> dict:
    r = requests.get(f"{BAN_API}/search/", params={
        "q": f"mairie {nom}", "citycode": code, "limit": 1
    }, timeout=10)
    features = r.json().get("features", [])
    if not features:
        return {}
    f = features[0]
    return {
        "label": f["properties"]["label"],
        "score": round(f["properties"]["score"], 4),
        "lat": f["geometry"]["coordinates"][1],
        "lon": f["geometry"]["coordinates"][0],
    }

def _geo_enriched_row(code: str) -> Optional[dict]:
    r = requests.get(f"{SUPABASE_URL}/rest/v1/geo_enriched", headers=SB_HEADERS,
        params={"code_insee": f"eq.{code}", "select": "*", "limit": "1"}, timeout=10)
    rows = r.json()
    return rows[0] if isinstance(rows, list) and rows else None

def _score_energie(pct_fg: float, nb_fg: int, conso: float) -> int:
    s = min(50, int(pct_fg * 1.5))
    s += 30 if nb_fg > 500 else 20 if nb_fg > 200 else 10 if nb_fg > 50 else 0
    s += 20 if conso > 400 else 12 if conso > 300 else 6 if conso > 200 else 0
    return min(100, s)

# ── MCP Tools ─────────────────────────────────────────────────────────────────

@mcp.tool()
def get_commune_info(code_insee: str) -> str:
    """
    Retourne les informations officielles d'une commune française.
    Sources : geo.api.gouv.fr + DPE ADEME (Supabase).

    Args:
        code_insee: Code INSEE de la commune (ex: '31555' pour Toulouse, '75056' pour Paris)

    Returns:
        JSON avec population, superficie, département, région, coordonnées GPS et résumé DPE.
    """
    try:
        geo = _geo(code_insee)
        dpe = _dpe_commune(code_insee)
        ban = _ban_commune(code_insee, geo.get("nom", ""))

        nb_fg = (dpe.get("nb_f", 0) or 0) + (dpe.get("nb_g", 0) or 0)
        sup = round(geo.get("surface", 0) / 100, 2)
        pop = geo.get("population", 0)

        result = {
            "code_insee": code_insee,
            "nom": geo.get("nom"),
            "code_postal": geo.get("codesPostaux", [""])[0],
            "departement_code": geo.get("codeDepartement"),
            "region_code": geo.get("codeRegion"),
            "population": pop,
            "superficie_km2": sup,
            "densite_hab_km2": round(pop / sup, 1) if sup else None,
            "coordonnees": {
                "lat": ban.get("lat") or (geo.get("centre", {}).get("coordinates", [None, None])[1]),
                "lon": ban.get("lon") or (geo.get("centre", {}).get("coordinates", [None, None])[0]),
                "label_ban": ban.get("label"),
                "score_geocodage": ban.get("score"),
            },
            "dpe_resume": {
                "nb_diagnostics_total": dpe.get("nb_dpe_efg", 0),
                "nb_passoires_fg": nb_fg,
                "pct_passoires_fg": dpe.get("pct_fg", 0),
                "conso_moy_kwh_m2": dpe.get("conso_moy"),
                "periode_construction_dominante": dpe.get("periode_dominante"),
                "score_energie": _score_energie(
                    dpe.get("pct_fg", 0), nb_fg, dpe.get("conso_moy", 0) or 0
                ),
            },
        }
        return json.dumps(result, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"erreur": str(e)}, ensure_ascii=False)


@mcp.tool()
def get_dpe_logements_fg(code_insee: str, limit: int = 15) -> str:
    """
    Liste les logements classés F ou G (passoires thermiques) dans une commune.
    Source : registre DPE ADEME via Supabase (875k diagnostics).

    Args:
        code_insee: Code INSEE de la commune
        limit: Nombre max de logements à retourner (défaut 15, max 50)

    Returns:
        Liste JSON des logements F/G avec adresse, consommation et type de bâtiment.
    """
    try:
        limit = min(limit, 50)
        logements = _dpe_logements(code_insee, limit)
        return json.dumps({
            "commune_code": code_insee,
            "nb_resultats": len(logements),
            "note": "Triés par consommation décroissante (logements les plus énergivores en premier)",
            "source": "ADEME — Registre national DPE",
            "logements": logements,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"erreur": str(e)}, ensure_ascii=False)


@mcp.tool()
def get_geo_enriched(code_insee: str) -> str:
    """
    Récupère le profil enrichi complet d'une commune (si déjà traité par le pipeline).
    Inclut : synthèse IA, tags, signaux, score énergie, article SEO, coordonnées BAN.

    Args:
        code_insee: Code INSEE de la commune

    Returns:
        Profil enrichi JSON ou message si la commune n'a pas encore été traitée.
    """
    try:
        row = _geo_enriched_row(code_insee)
        if not row:
            return json.dumps({
                "status": "non_enrichi",
                "message": f"La commune {code_insee} n'a pas encore été enrichie. Utilisez enrich_commune() pour la traiter.",
            }, ensure_ascii=False)

        # Retourner sans l'article SEO (trop long pour le contexte MCP)
        return json.dumps({
            "code_insee": row.get("code_insee"),
            "nom_commune": row.get("nom_commune"),
            "departement": row.get("departement"),
            "region": row.get("region"),
            "population": row.get("population"),
            "superficie_km2": row.get("superficie_km2"),
            "densite": row.get("densite"),
            "lat_centre": row.get("lat_centre"),
            "lon_centre": row.get("lon_centre"),
            "dpe": {
                "nb_fg": row.get("nb_fg"),
                "pct_fg": row.get("pct_fg"),
                "conso_moy": row.get("conso_moy"),
                "score_energie": row.get("score_energie"),
            },
            "claude": {
                "synthese": row.get("synthese"),
                "tags": row.get("tags"),
                "signaux": json.loads(row.get("signaux", "{}")),
                "titre_seo": row.get("titre_seo"),
                "meta_description": row.get("meta_description"),
            },
            "statut": row.get("statut"),
            "enrichi_at": row.get("enrichi_at"),
            "note_article": "L'article SEO complet est disponible dans le champ article_seo — demandez-le explicitement si besoin.",
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"erreur": str(e)}, ensure_ascii=False)


@mcp.tool()
def enrich_commune(code_insee: str, force: bool = False) -> str:
    """
    Lance le pipeline d'enrichissement complet pour une commune :
    geo.api.gouv.fr + DPE ADEME + BAN + Claude Haiku (synthèse + article SEO) → Supabase.

    Nécessite ANTHROPIC_API_KEY dans l'environnement.
    Durée estimée : 15-30 secondes par commune.

    Args:
        code_insee: Code INSEE de la commune à enrichir
        force: Si True, réenrichit même si la commune est déjà en base (défaut: False)

    Returns:
        Résultat de l'enrichissement avec score, titre SEO et statut Supabase.
    """
    if not ANTHROPIC_KEY:
        return json.dumps({
            "erreur": "ANTHROPIC_API_KEY manquant. Ajoutez-le dans l'environnement MCP.",
            "aide": "Dans claude_desktop_config.json : env: { ANTHROPIC_API_KEY: 'sk-ant-...' }",
        }, ensure_ascii=False)

    # Vérifier si déjà enrichi (sauf si force=True)
    if not force:
        existing = _geo_enriched_row(code_insee)
        if existing and existing.get("statut") == "done":
            return json.dumps({
                "status": "deja_enrichi",
                "message": f"{existing.get('nom_commune')} est déjà enrichie (statut: done). Utilisez force=True pour réenrichir.",
                "enrichi_at": existing.get("enrichi_at"),
                "score_energie": existing.get("score_energie"),
            }, ensure_ascii=False)

    try:
        # Import de la logique principale du pipeline
        # (en production, on importe directement les fonctions de enrich_commune.py)
        import importlib.util, pathlib
        pipeline_path = pathlib.Path(__file__).parent / "enrich_commune.py"
        spec = importlib.util.spec_from_file_location("pipeline", pipeline_path)
        pipeline = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(pipeline)

        ok = pipeline.enrich_commune(code_insee)

        if ok:
            row = _geo_enriched_row(code_insee)
            return json.dumps({
                "status": "ok",
                "code_insee": code_insee,
                "nom_commune": row.get("nom_commune") if row else None,
                "score_energie": row.get("score_energie") if row else None,
                "titre_seo": row.get("titre_seo") if row else None,
                "tags": row.get("tags") if row else None,
                "message": "Commune enrichie et enregistrée en base Supabase.",
            }, ensure_ascii=False, indent=2)
        else:
            return json.dumps({"status": "erreur", "message": "L'enrichissement a échoué. Vérifiez les logs."}, ensure_ascii=False)

    except Exception as e:
        return json.dumps({"status": "erreur", "message": str(e)}, ensure_ascii=False)


@mcp.tool()
def search_communes_by_dep(departement: str, limit: int = 10, min_score: int = 0) -> str:
    """
    Liste les communes d'un département déjà enrichies dans la base SAHAR,
    triées par score énergie décroissant.

    Args:
        departement: Code département (ex: '69', '31', '75')
        limit: Nombre max de communes (défaut 10)
        min_score: Score énergie minimum (0-100, défaut 0 = toutes)

    Returns:
        Liste JSON des communes enrichies avec leurs scores et signaux.
    """
    try:
        params = {
            "code_dep": f"eq.{departement}",
            "select": "code_insee,nom_commune,score_energie,pct_fg,nb_fg,conso_moy,synthese,tags,signaux,titre_seo",
            "order": "score_energie.desc",
            "limit": str(min(limit, 100)),
        }
        if min_score > 0:
            params["score_energie"] = f"gte.{min_score}"

        r = requests.get(f"{SUPABASE_URL}/rest/v1/geo_enriched", headers=SB_HEADERS,
                         params=params, timeout=10)
        rows = r.json()

        if not isinstance(rows, list):
            return json.dumps({"erreur": str(rows)}, ensure_ascii=False)

        return json.dumps({
            "departement": departement,
            "nb_communes_enrichies": len(rows),
            "communes": [{
                "code_insee": row.get("code_insee"),
                "nom": row.get("nom_commune"),
                "score_energie": row.get("score_energie"),
                "pct_fg": row.get("pct_fg"),
                "nb_fg": row.get("nb_fg"),
                "conso_moy": row.get("conso_moy"),
                "synthese": (row.get("synthese") or "")[:200] + "…" if row.get("synthese") else None,
                "tags": row.get("tags"),
                "signaux": json.loads(row.get("signaux", "{}")),
                "titre_seo": row.get("titre_seo"),
            } for row in rows],
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"erreur": str(e)}, ensure_ascii=False)


@mcp.tool()
def geocode_address(adresse: str, commune: Optional[str] = None, code_insee: Optional[str] = None) -> str:
    """
    Géocode une adresse française via la Base Adresse Nationale (BAN).

    Args:
        adresse: Adresse à géocoder (ex: '12 rue de la Paix')
        commune: Nom de la commune pour affiner (ex: 'Paris')
        code_insee: Code INSEE pour ancrer dans une commune précise (ex: '75056')

    Returns:
        Coordonnées GPS, adresse normalisée et score de confiance.
    """
    try:
        params = {"q": adresse, "limit": "3"}
        if code_insee:
            params["citycode"] = code_insee
        elif commune:
            params["q"] = f"{adresse} {commune}"

        r = requests.get(f"{BAN_API}/search/", params=params, timeout=10)
        data = r.json()
        features = data.get("features", [])

        if not features:
            return json.dumps({"erreur": "Adresse non trouvée", "query": adresse}, ensure_ascii=False)

        results = []
        for f in features[:3]:
            p = f["properties"]
            c = f["geometry"]["coordinates"]
            results.append({
                "label": p.get("label"),
                "score": round(p.get("score", 0), 4),
                "type": p.get("type"),
                "lat": c[1],
                "lon": c[0],
                "postcode": p.get("postcode"),
                "city": p.get("city"),
                "citycode": p.get("citycode"),
                "housenumber": p.get("housenumber"),
                "street": p.get("street"),
            })

        return json.dumps({
            "query": adresse,
            "nb_resultats": len(results),
            "source": "Base Adresse Nationale (data.gouv.fr)",
            "resultats": results,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"erreur": str(e)}, ensure_ascii=False)


@mcp.tool()
def get_article_seo(code_insee: str) -> str:
    """
    Récupère l'article SEO HTML complet généré par Claude pour une commune enrichie.

    Args:
        code_insee: Code INSEE de la commune

    Returns:
        Article HTML complet (1500+ mots) ou message si commune non enrichie.
    """
    try:
        row = _geo_enriched_row(code_insee)
        if not row:
            return f"Commune {code_insee} non enrichie. Utilisez enrich_commune('{code_insee}') d'abord."
        article = row.get("article_seo", "")
        if not article:
            return f"Article SEO non disponible pour {row.get('nom_commune', code_insee)}."
        return article
    except Exception as e:
        return f"Erreur : {e}"


# ── Ressources MCP (lecture) ──────────────────────────────────────────────────

@mcp.resource("sahar://stats")
def get_stats() -> str:
    """Statistiques globales de la base SAHAR Conseil."""
    try:
        r_enriched = requests.get(f"{SUPABASE_URL}/rest/v1/geo_enriched",
            headers={**SB_HEADERS, "Prefer": "count=exact"},
            params={"select": "count", "statut": "eq.done"}, timeout=10)
        r_dpe = requests.get(f"{SUPABASE_URL}/rest/v1/dpe_communes",
            headers={**SB_HEADERS, "Prefer": "count=exact"},
            params={"select": "count"}, timeout=10)

        enriched_count = r_enriched.headers.get("content-range", "?/?").split("/")[-1]
        dpe_count = r_dpe.headers.get("content-range", "?/?").split("/")[-1]

        return json.dumps({
            "communes_enrichies": enriched_count,
            "communes_dpe_disponibles": dpe_count,
            "sources": ["DGFiP DVF", "ADEME DPE", "geo.api.gouv.fr", "Base Adresse Nationale"],
            "modele_ia": "claude-haiku-4-5-20251001",
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"erreur": str(e)}, ensure_ascii=False)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SAHAR Conseil MCP Server")
    parser.add_argument("--http", action="store_true", help="Mode HTTP SSE (défaut: stdio)")
    parser.add_argument("--port", type=int, default=8000, help="Port HTTP (défaut: 8000)")
    args = parser.parse_args()

    if args.http:
        print(f"SAHAR MCP Server → HTTP SSE sur http://localhost:{args.port}", file=sys.stderr)
        mcp.run(transport="sse", host="0.0.0.0", port=args.port)
    else:
        print("SAHAR MCP Server → stdio (Claude Desktop)", file=sys.stderr)
        mcp.run(transport="stdio")
