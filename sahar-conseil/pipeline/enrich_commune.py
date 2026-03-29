#!/usr/bin/env python3
"""
SAHAR Conseil — Pipeline d'enrichissement géographique par commune
Usage:
  python enrich_commune.py --insee 69123          # enrichit Lyon
  python enrich_commune.py --dep 69 --limit 10    # enrichit 10 communes du Rhône
  python enrich_commune.py --all --limit 50        # batch sur toutes les communes
  python enrich_commune.py --dry --insee 31555     # dry-run sans Claude ni Supabase

Sources :
  geo.api.gouv.fr        → métadonnées officelles (pop, surface, codes)
  Supabase dpe_communes  → agrégats DPE par commune (nb F/G, conso, période)
  Supabase dpe_logements → échantillon logements individuels F/G
  api-adresse.data.gouv.fr (BAN) → géocodage : centre commune + adresses F/G
  Claude API (haiku)     → synthèse + tags + article SEO
"""

import io, os, sys, json, time, argparse, logging, csv
from datetime import datetime, timezone
from typing import Optional

import requests
from anthropic import Anthropic

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL  = os.getenv("SUPABASE_URL", "https://ylrrcbklufshebcizgus.supabase.co")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlscnJjYmtsdWZzaGViY2l6Z3VzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ1NjQzNTEsImV4cCI6MjA5MDE0MDM1MX0.KQjvB5aePbmCcrAu9yYKoIblDG0ui90LXa-DcL7HAEA")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

GEO_API = "https://geo.api.gouv.fr"
BAN_API = "https://api-adresse.data.gouv.fr"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sahar.enrich")

client = Anthropic(api_key=ANTHROPIC_KEY)

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

# ── Sources : geo.api.gouv.fr ──────────────────────────────────────────────

def fetch_geo_commune(code_insee: str) -> dict:
    """geo.api.gouv.fr — infos officielles de la commune."""
    url = f"{GEO_API}/communes/{code_insee}"
    params = {"fields": "nom,code,codeDepartement,codeRegion,codesPostaux,population,surface,centre", "format": "json"}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def fetch_departement_info(code_dep: str) -> dict:
    """geo.api.gouv.fr — nom et région du département."""
    r = requests.get(f"{GEO_API}/departements/{code_dep}", params={"fields": "nom,code,codeRegion"}, timeout=10)
    r.raise_for_status()
    return r.json()

def fetch_region_info(code_region: str) -> dict:
    """geo.api.gouv.fr — nom de la région."""
    r = requests.get(f"{GEO_API}/regions/{code_region}", params={"fields": "nom,code"}, timeout=10)
    r.raise_for_status()
    return r.json()

# ── Sources : Supabase DPE ─────────────────────────────────────────────────

def fetch_dpe_commune(code_insee: str) -> dict:
    """Supabase dpe_communes — agrégats DPE locaux."""
    url = f"{SUPABASE_URL}/rest/v1/dpe_communes"
    params = {"code_insee": f"eq.{code_insee}", "select": "*", "limit": "1"}
    r = requests.get(url, headers=SB_HEADERS, params=params, timeout=10)
    r.raise_for_status()
    rows = r.json()
    return rows[0] if rows else {}

def fetch_dpe_logements_sample(code_insee: str, limit: int = 15) -> list:
    """Supabase dpe_logements — échantillon de logements F/G triés par conso décroissante.
    Index requis : idx_dpe_logements_commune_fg_conso (code_insee, conso_par_m2 DESC) WHERE etiquette_dpe IN ('F','G')
    """
    url = f"{SUPABASE_URL}/rest/v1/dpe_logements"
    params = {
        "code_insee": f"eq.{code_insee}",
        "etiquette_dpe": "in.(F,G)",
        "select": "etiquette_dpe,type_batiment,periode_construction,adresse,conso_par_m2",
        "order": "conso_par_m2.desc.nullslast",
        "limit": str(limit),
    }
    r = requests.get(url, headers=SB_HEADERS, params=params, timeout=15)
    r.raise_for_status()
    result = r.json()
    if isinstance(result, dict):  # Supabase error object
        log.warning(f"[{code_insee}] dpe_logements returned error: {result}")
        return []
    return result

# ── Sources : BAN (Base Adresse Nationale) ────────────────────────────────

def fetch_ban_commune(code_insee: str, nom_commune: str) -> dict:
    """BAN — point de référence géocodé de la commune (mairie/place centrale).

    Retourne : {label, score, lat, lon, postcode, city}
    """
    r = requests.get(
        f"{BAN_API}/search/",
        params={"q": f"mairie {nom_commune}", "citycode": code_insee, "limit": 1, "type": "street"},
        timeout=10,
    )
    r.raise_for_status()
    features = r.json().get("features", [])
    if not features:
        # Fallback : juste le nom de la commune sans "mairie"
        r2 = requests.get(
            f"{BAN_API}/search/",
            params={"q": nom_commune, "citycode": code_insee, "limit": 1},
            timeout=10,
        )
        r2.raise_for_status()
        features = r2.json().get("features", [])
    if not features:
        return {}
    f = features[0]
    props = f["properties"]
    coords = f["geometry"]["coordinates"]
    return {
        "label":    props.get("label", ""),
        "score":    round(props.get("score", 0), 4),
        "lat":      coords[1],
        "lon":      coords[0],
        "postcode": props.get("postcode", ""),
        "city":     props.get("city", ""),
    }

def fetch_ban_logements(adresses: list[str], code_insee: str) -> list[dict]:
    """BAN — géocodage par lot des adresses F/G via endpoint /search/csv/.

    Envoie un fichier CSV (avec colonne citycode) à l'API BAN.
    Retourne uniquement les adresses géocodées dans la bonne commune (result_citycode == code_insee).
    """
    if not adresses:
        return []

    # Nettoyage et déduplication (max 20)
    adresses_clean = list(dict.fromkeys(
        a.strip().replace("\n", " ") for a in adresses if a and a.strip()
    ))[:20]

    # Construction du CSV avec colonne citycode pour ancrer dans la commune
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["adresse", "citycode"])
    for a in adresses_clean:
        writer.writerow([a, code_insee])
    buf.seek(0)

    try:
        r = requests.post(
            f"{BAN_API}/search/csv/",
            files={"data": ("adresses.csv", buf.getvalue().encode("utf-8"), "text/csv")},
            data={"columns": "adresse", "citycode": "citycode"},   # "citycode" = nom de la colonne CSV
            timeout=30,
        )
        r.raise_for_status()
    except Exception as e:
        log.warning(f"BAN CSV batch error: {e}")
        return []

    # Parse la réponse CSV — filtre sur result_citycode pour éviter les faux positifs
    results = []
    reader = csv.DictReader(io.StringIO(r.text))
    for row in reader:
        try:
            score = float(row.get("result_score", 0) or 0)
            # Garde seulement les adresses dans la bonne commune avec un score acceptable
            if row.get("result_citycode", "") != code_insee:
                continue
            results.append({
                "adresse_input":       row.get("adresse", ""),
                "adresse_normalisee":  row.get("result_label", ""),
                "score":               round(score, 4),
                "lat":                 float(row.get("latitude", 0) or 0),
                "lon":                 float(row.get("longitude", 0) or 0),
                "type":                row.get("result_type", ""),
            })
        except (ValueError, KeyError):
            continue
    return results

# ── Collecte globale ──────────────────────────────────────────────────────

def collect_commune_data(code_insee: str) -> dict:
    """Agrège toutes les sources disponibles pour une commune."""
    data = {"code_insee": code_insee, "sources": {}, "erreurs": []}

    # 1. geo.api.gouv.fr — commune
    try:
        geo = fetch_geo_commune(code_insee)
        data["sources"]["geo"] = geo
        data["nom_commune"]    = geo.get("nom", "")
        data["code_postal"]    = geo.get("codesPostaux", [""])[0]
        data["population"]     = geo.get("population", 0)
        data["superficie_km2"] = round(geo.get("surface", 0) / 100, 2)  # hectares → km²
        data["code_dep"]       = geo.get("codeDepartement", "")
        data["code_region"]    = geo.get("codeRegion", "")
        # Coordonnées du centroïde depuis l'API geo (fallback si BAN échoue)
        centre = geo.get("centre", {})
        if centre and centre.get("coordinates"):
            data["lat_centre_geo"] = centre["coordinates"][1]
            data["lon_centre_geo"] = centre["coordinates"][0]
    except Exception as e:
        data["erreurs"].append(f"geo: {e}")
        log.warning(f"[{code_insee}] geo API error: {e}")

    # 2. Département + Région
    try:
        dep = fetch_departement_info(data.get("code_dep", ""))
        data["departement"]       = dep.get("nom", "")
        data["sources"]["departement"] = dep
    except Exception as e:
        data["erreurs"].append(f"dep: {e}")

    try:
        reg = fetch_region_info(data.get("code_region", ""))
        data["region"]          = reg.get("nom", "")
        data["sources"]["region"] = reg
    except Exception as e:
        data["erreurs"].append(f"region: {e}")

    # 3. DPE agrégé
    try:
        dpe = fetch_dpe_commune(code_insee)
        data["sources"]["dpe_communes"] = dpe
        if dpe:
            data["nb_dpe_total"]       = dpe.get("nb_dpe_efg", 0) or 0
            data["nb_fg"]              = (dpe.get("nb_f", 0) or 0) + (dpe.get("nb_g", 0) or 0)
            data["pct_fg"]             = dpe.get("pct_fg", 0) or 0
            data["conso_moy"]          = dpe.get("conso_moy", 0)
            data["periode_dominante"]  = dpe.get("periode_dominante", "")
    except Exception as e:
        data["erreurs"].append(f"dpe_communes: {e}")
        log.warning(f"[{code_insee}] DPE communes error: {e}")

    # 4. Échantillon logements F/G
    logements = []
    try:
        logements = fetch_dpe_logements_sample(code_insee, limit=15)
        data["sources"]["dpe_logements_fg_sample"] = logements
        data["nb_fg_sample"] = len(logements)
    except Exception as e:
        data["erreurs"].append(f"dpe_logements: {e}")

    # 5. BAN — centre commune
    try:
        nom = data.get("nom_commune", "")
        ban = fetch_ban_commune(code_insee, nom)
        data["sources"]["ban_commune"] = ban
        if ban:
            data["lat_centre"] = ban["lat"]
            data["lon_centre"] = ban["lon"]
            data["ban_label"]  = ban["label"]
            data["ban_score"]  = ban["score"]
            log.info(f"  BAN centre: {ban['label']} (score={ban['score']})")
    except Exception as e:
        # Fallback sur centroïde geo API
        data["lat_centre"] = data.get("lat_centre_geo")
        data["lon_centre"] = data.get("lon_centre_geo")
        data["erreurs"].append(f"ban_commune: {e}")
        log.warning(f"[{code_insee}] BAN commune error: {e}")

    # 6. BAN — géocodage des adresses F/G (pour enrichir l'analyse spatiale)
    try:
        adresses_fg = [l.get("adresse", "") for l in logements if l.get("adresse")]
        if adresses_fg:
            ban_log = fetch_ban_logements(adresses_fg, code_insee)
            data["sources"]["ban_logements"] = ban_log
            # Score moyen de géocodage (indicateur qualité)
            scores = [b["score"] for b in ban_log if b["score"] > 0]
            data["ban_geocoding_score_moy"] = round(sum(scores) / len(scores), 3) if scores else None
            # Nb d'adresses bien géocodées (score > 0.7)
            data["ban_geocoding_ok"] = sum(1 for b in ban_log if b["score"] > 0.7)
            log.info(f"  BAN logements: {len(ban_log)} adresses, score moy={data['ban_geocoding_score_moy']}, ok={data['ban_geocoding_ok']}")
    except Exception as e:
        data["erreurs"].append(f"ban_logements: {e}")
        log.warning(f"[{code_insee}] BAN logements error: {e}")

    return data

# ── Scoring interne ───────────────────────────────────────────────────────

def compute_score_energie(pct_fg: float, nb_fg: int, conso_moy: float) -> int:
    """Score opportunité rénovation 0-100 (haut = opportunité forte)."""
    score = 0
    # % passoires thermiques (poids 50)
    score += min(50, int(pct_fg * 1.5))
    # Volume absolu (poids 30)
    if nb_fg > 500:   score += 30
    elif nb_fg > 200: score += 20
    elif nb_fg > 50:  score += 10
    # Conso moyenne (poids 20)
    if conso_moy and conso_moy > 400: score += 20
    elif conso_moy and conso_moy > 300: score += 12
    elif conso_moy and conso_moy > 200: score += 6
    return min(100, score)

# ── Appel Claude API ──────────────────────────────────────────────────────

SYSTEM_PROMPT = """Tu es un expert en données immobilières et énergétiques françaises.
Tu analyses des données publiques open data (DPE ADEME, geo.api.gouv.fr, Base Adresse Nationale) pour générer :
1. Une synthèse narrative structurée sur la situation énergétique et immobilière d'une commune
2. Des tags intelligents
3. Un article SEO complet de minimum 1500 mots destiné au blog de SAHAR Conseil

Ton ton est professionnel, factuel, utile pour des professionnels (artisans RGE, agents immo, investisseurs).
Tu cites toujours les sources (données ADEME, DGFiP, INSEE, BAN) et tu indiques les limites des données."""

def build_claude_prompt(data: dict) -> str:
    nom     = data.get("nom_commune", "cette commune")
    dep     = data.get("departement", "")
    region  = data.get("region", "")
    pop     = data.get("population", 0)
    sup     = data.get("superficie_km2", 0)
    pct_fg  = data.get("pct_fg", 0)
    nb_fg   = data.get("nb_fg", 0)
    nb_tot  = data.get("nb_dpe_total", 0)
    conso   = data.get("conso_moy", 0)
    periode = data.get("periode_dominante", "")
    lat     = data.get("lat_centre", "")
    lon     = data.get("lon_centre", "")
    ban_label = data.get("ban_label", "")
    geo_ok  = data.get("ban_geocoding_ok", 0)
    geo_score = data.get("ban_geocoding_score_moy", "")
    sample  = data.get("sources", {}).get("dpe_logements_fg_sample", [])[:5]
    ban_log = data.get("sources", {}).get("ban_logements", [])[:5]

    prompt = f"""Analyse la commune de **{nom}** ({dep}, {region}).

DONNÉES GÉOGRAPHIQUES :
- Population : {pop:,} habitants
- Superficie : {sup} km²
- Densité : {round(pop/sup, 1) if sup else 'N/A'} hab/km²
- Coordonnées centre (BAN) : lat={lat}, lon={lon}
- Référence adresse : {ban_label}

DONNÉES DPE (source ADEME) :
- Passoires thermiques (F+G) : {nb_fg} logements ({pct_fg}% du parc diagnostiqué)
- Total DPE dans la base : {nb_tot}
- Consommation énergétique moyenne : {conso} kWh/m²/an
- Période de construction dominante : {periode}

GÉOCODAGE ADRESSES F/G (Base Adresse Nationale) :
- Adresses vérifiées correctement : {geo_ok} / 15 (score seuil > 0.70)
- Score de géocodage moyen : {geo_score}
- Échantillon adresses normalisées : {json.dumps(ban_log, ensure_ascii=False)}

EXEMPLES DE LOGEMENTS F/G :
{json.dumps(sample, ensure_ascii=False, indent=2)}

MISSION — Génère une réponse JSON structurée avec exactement ces clés :

{{
  "synthese": "Paragraphe synthétique de 3-4 phrases sur la situation énergétique et le potentiel rénovation de {nom}",

  "tags": ["tag1", "tag2", ...],

  "signaux": {{
    "urgence_renovation": "haute|moyenne|faible",
    "potentiel_marche": "fort|modéré|faible",
    "profil_bati": "description courte du parc",
    "opportunite_artisan": "description courte"
  }},

  "titre_seo": "Titre SEO accrocheur pour un article de blog (60-70 caractères max)",

  "meta_description": "Meta description SEO (150-160 caractères)",

  "article_seo": "Article complet de minimum 1500 mots en HTML sémantique (h2, h3, p, ul, strong). Structure : Introduction avec contexte géographique et économique — Le parc résidentiel de {nom} : état des lieux DPE — Les passoires thermiques F/G : enjeux et données — Opportunités pour les professionnels (artisans RGE, agents immo, investisseurs) — MaPrimeRénov' et aides disponibles — Perspective et tendances du marché local — Conclusion avec CTA vers SAHAR Conseil. Cite les sources (ADEME, BAN, DGFiP). Inclure les données chiffrées. Ton professionnel et utile."
}}

IMPORTANT : Réponds UNIQUEMENT avec le JSON valide, sans markdown, sans commentaires."""
    return prompt

def call_claude(data: dict) -> dict:
    prompt = build_claude_prompt(data)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=8192,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw)

# ── Stockage Supabase ──────────────────────────────────────────────────────

def upsert_geo_enriched(code_insee: str, data: dict, claude: dict) -> bool:
    """Upsert dans geo_enriched avec résolution de conflits sur code_insee."""
    score = compute_score_energie(
        data.get("pct_fg", 0),
        data.get("nb_fg", 0),
        data.get("conso_moy", 0) or 0,
    )

    # Sources à stocker (sans l'échantillon verbeux de logements individuels)
    sources_light = {
        k: v for k, v in data.get("sources", {}).items()
        if k not in ("dpe_logements_fg_sample", "ban_logements")
    }

    row = {
        "code_insee":       code_insee,
        "nom_commune":      data.get("nom_commune", ""),
        "code_postal":      data.get("code_postal", ""),
        "departement":      data.get("departement", ""),
        "code_dep":         data.get("code_dep", ""),
        "region":           data.get("region", ""),
        "population":       data.get("population", 0),
        "superficie_km2":   float(data.get("superficie_km2", 0) or 0),
        "densite":          round(data["population"] / data["superficie_km2"], 1)
                            if data.get("population") and data.get("superficie_km2") else None,
        # Coordonnées BAN
        "lat_centre":       data.get("lat_centre"),
        "lon_centre":       data.get("lon_centre"),
        "ban_data":         json.dumps({
            "label":          data.get("ban_label", ""),
            "score":          data.get("ban_score"),
            "geocoding_ok":   data.get("ban_geocoding_ok"),
            "geocoding_score_moy": data.get("ban_geocoding_score_moy"),
        }, ensure_ascii=False),
        # DPE
        "nb_dpe_total":     data.get("nb_dpe_total", 0),
        "nb_fg":            data.get("nb_fg", 0),
        "pct_fg":           float(data.get("pct_fg", 0) or 0),
        "conso_moy":        float(data.get("conso_moy", 0) or 0),
        "score_energie":    score,
        "donnees_brutes":   json.dumps(sources_light, ensure_ascii=False),
        # Claude
        "synthese":         claude.get("synthese", ""),
        "tags":             claude.get("tags", []),
        "score_global":     score,
        "signaux":          json.dumps(claude.get("signaux", {}), ensure_ascii=False),
        "titre_seo":        claude.get("titre_seo", ""),
        "meta_description": claude.get("meta_description", ""),
        "article_seo":      claude.get("article_seo", ""),
        "statut":           "done" if not data.get("erreurs") else "partial",
        "erreur":           "; ".join(data.get("erreurs", [])) or None,
        "enrichi_at":       datetime.now(timezone.utc).isoformat(),
        "updated_at":       datetime.now(timezone.utc).isoformat(),
    }

    url = f"{SUPABASE_URL}/rest/v1/geo_enriched"
    headers = {**SB_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"}
    r = requests.post(url, headers=headers, json=row, timeout=15)
    if r.status_code not in (200, 201, 204):
        log.error(f"Supabase upsert error {r.status_code}: {r.text[:300]}")
        return False
    return True

# ── Orchestration ─────────────────────────────────────────────────────────

def enrich_commune(code_insee: str) -> bool:
    log.info(f"→ Enrichissement {code_insee}")
    try:
        data = collect_commune_data(code_insee)
        if not data.get("nom_commune"):
            log.warning(f"  Commune {code_insee} introuvable via geo API")
            return False
        log.info(f"  {data['nom_commune']} — {data.get('nb_fg', 0)} logements F/G ({data.get('pct_fg', 0)}%) "
                 f"| BAN: {data.get('ban_label', 'N/A')} ({data.get('lat_centre', '?')}, {data.get('lon_centre', '?')})")

        claude = call_claude(data)
        score  = compute_score_energie(data.get("pct_fg", 0), data.get("nb_fg", 0), data.get("conso_moy", 0) or 0)
        log.info(f"  Claude OK — score={score} tags={claude.get('tags', [])}")

        ok = upsert_geo_enriched(code_insee, data, claude)
        log.info(f"  Supabase {'OK ✓' if ok else 'ERREUR ✗'}")
        return ok

    except Exception as e:
        log.error(f"  ERREUR {code_insee}: {e}", exc_info=True)
        return False

def get_communes_from_supabase(dep: Optional[str] = None, limit: int = 10) -> list[str]:
    """Récupère les codes INSEE depuis dpe_communes, triés par volume DPE décroissant."""
    url = f"{SUPABASE_URL}/rest/v1/dpe_communes"
    params = {
        "select": "code_insee",
        "code_insee": "not.is.null",
        "order": "nb_dpe_efg.desc",
        "limit": str(limit),
    }
    if dep:
        params["departement"] = f"eq.{dep}"
    r = requests.get(url, headers=SB_HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return [row["code_insee"] for row in r.json() if row.get("code_insee")]

# ── CLI ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pipeline enrichissement géo SAHAR")
    parser.add_argument("--insee",  help="Code INSEE d'une commune spécifique")
    parser.add_argument("--dep",    help="Département (ex: 69)")
    parser.add_argument("--limit",  type=int, default=5, help="Nombre max de communes")
    parser.add_argument("--dry",    action="store_true", help="Affiche les données sans appeler Claude ni Supabase")
    args = parser.parse_args()

    if not ANTHROPIC_KEY and not args.dry:
        log.error("ANTHROPIC_API_KEY manquant. Export: export ANTHROPIC_API_KEY=sk-ant-...")
        sys.exit(1)

    if args.insee:
        communes = [args.insee]
    else:
        communes = get_communes_from_supabase(dep=args.dep, limit=args.limit)
        log.info(f"{len(communes)} communes à enrichir")

    if args.dry:
        for c in communes[:3]:
            data = collect_commune_data(c)
            # Masquer les sources volumineuses pour le dry-run
            out = {k: v for k, v in data.items() if k != "sources"}
            out["sources_keys"] = list(data.get("sources", {}).keys())
            print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
        return

    ok = err = 0
    for code in communes:
        success = enrich_commune(code)
        if success: ok += 1
        else: err += 1
        time.sleep(0.5)  # Rate limit Claude API

    log.info(f"\n✓ {ok} communes enrichies | ✗ {err} erreurs")

if __name__ == "__main__":
    main()
