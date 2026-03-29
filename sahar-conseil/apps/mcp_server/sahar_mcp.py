#!/usr/bin/env python3
"""
SAHAR Conseil — MCP Server v2
Conforme au cahier des charges CLAUDE.md Phase 4

Tools :
  query_commune        → KPIs complets d'une commune (kpi_commune view)
  enrich_address       → Normalisation BAN + KPIs commune
  search_communes      → Recherche multicritères
  batch_enrich         → Enrichissement CSV en lot
  health_check         → État du système

KPI Engines (4 domaines) :
  ImmoKPIs             → Valorisation, tendance marché
  RenoKPIs             → Énergie, passoires, aides
  FinKPIs              → Mensualités, accessibilité
  EvalKPIs             → Score global, verdict

Lancement :
  python mcp_sahar.py              # stdio — Claude Desktop
  python mcp_sahar.py --http 8000  # HTTP SSE — Cloud Run / tests

Config Claude Desktop :
  {
    "mcpServers": {
      "sahar": {
        "command": "python3",
        "args": ["/path/to/mcp_sahar.py"],
        "env": { "SUPABASE_KEY": "eyJ..." }
      }
    }
  }
"""

import os, sys, json, logging
from dataclasses import dataclass, asdict
from typing import Optional

try:
    from fastmcp import FastMCP
except ImportError:
    print("pip install fastmcp --break-system-packages", file=sys.stderr)
    sys.exit(1)

import requests

# ── Config ─────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://wwvdpixzfaviaapixarb.supabase.co")
SUPABASE_KEY = os.getenv(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind3dmRwaXh6ZmF2aWFhcGl4YXJiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ3Mzk0NDgsImV4cCI6MjA5MDMxNTQ0OH0"
    ".NjOeWzUCo2BJPcxnkEJNm215GJBr1RAHba1eL_EF758",
)
BAN_API  = "https://api-adresse.data.gouv.fr"
GEO_API  = "https://geo.api.gouv.fr"

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

logging.basicConfig(level=logging.WARNING)

# ── KPI Engines ────────────────────────────────────────────────────────────────

@dataclass
class ImmoKPIs:
    prix_m2_median: float
    prix_m2_moyen: float
    evolution_12m_pct: float
    trend: str                      # hot / warm / cold
    nb_ventes_12m: int
    vitesse_marche: str             # rapide / normal / lent
    walkability_score: float
    poi_score_global: float
    confidence: int                 # 0-100
    warning_flags: list

    @classmethod
    def compute(cls, r: dict) -> "ImmoKPIs":
        prix  = float(r.get("prix_median_m2") or 0)
        evo   = float(r.get("evolution_12m") or 0)
        nb    = int(float(r.get("nb_ventes") or 0))
        trend = str(r.get("trend") or "cold").lower()
        poi   = float(r.get("poi_score_global") or 0)

        vitesse = "rapide" if nb > 30 else "normal" if nb > 10 else "lent"

        flags = []
        if nb < 5:    flags.append("Faible volume de transactions (< 5 ventes/an)")
        if prix == 0: flags.append("Prix médian indisponible")

        conf = 0
        conf += 40 if nb >= 50 else 30 if nb >= 20 else 20 if nb >= 10 else 10 if nb >= 5 else 0
        conf += 30 if prix > 0 else 0
        conf += 20 if poi > 0 else 0
        conf = min(100, conf)

        return cls(
            prix_m2_median=round(prix, 0),
            prix_m2_moyen=round(float(r.get("prix_moyen_m2") or prix), 0),
            evolution_12m_pct=round(evo, 2),
            trend=trend,
            nb_ventes_12m=nb,
            vitesse_marche=vitesse,
            walkability_score=round(poi, 1),
            poi_score_global=round(poi, 1),
            confidence=conf,
            warning_flags=flags,
        )


@dataclass
class RenoKPIs:
    score_energie: int
    pct_passoire: float
    conso_moy_kwh_m2: float
    dpe_dominant: str
    urgency_level: str
    nb_dpe: int
    maprimenov_eligible: bool
    maprimenov_montant_estime: int
    total_aides_estimees: int
    cout_net_apres_aides: int
    economie_euros_estimee: int
    confidence: int
    warning_flags: list

    @classmethod
    def compute(cls, r: dict) -> "RenoKPIs":
        score  = int(float(r.get("score_energie") or 0))
        pct_pp = float(r.get("pct_passoire") or 0)
        conso  = float(r.get("conso_moy_kwh_m2") or 250)
        nb_dpe = int(float(r.get("nb_dpe") or 0))

        classes = {c: float(r.get(f"pct_{c.lower()}") or 0) for c in "ABCDEFG"}
        dominant = max(classes, key=classes.get) if any(v > 0 for v in classes.values()) else "D"

        urgency = (
            "critical" if pct_pp > 30 else
            "high"     if pct_pp > 20 else
            "medium"   if pct_pp > 10 else
            "low"
        )

        maprenovmontant = 4500 if pct_pp > 15 else 2500
        total_aides = int(maprenovmontant * 1.3)
        cout_net = max(0, 15000 - total_aides)
        economie_kwh = pct_pp / 100 * nb_dpe * 75 * max(0, conso - 150) if nb_dpe else 0
        economie_eur = int(economie_kwh * 0.15)

        flags = []
        if nb_dpe < 10: flags.append("Peu de DPE disponibles (< 10)")
        if score == 0:  flags.append("Score énergie non calculé")

        return cls(
            score_energie=score,
            pct_passoire=round(pct_pp, 1),
            conso_moy_kwh_m2=round(conso, 1),
            dpe_dominant=dominant,
            urgency_level=urgency,
            nb_dpe=nb_dpe,
            maprimenov_eligible=True,
            maprimenov_montant_estime=maprenovmontant,
            total_aides_estimees=total_aides,
            cout_net_apres_aides=cout_net,
            economie_euros_estimee=economie_eur,
            confidence=70 if nb_dpe >= 50 else 40 if nb_dpe >= 10 else 20,
            warning_flags=flags,
        )


@dataclass
class FinKPIs:
    prix_m2_median: float
    estimation_60m2: int
    estimation_100m2: int
    taux_ref_pct: float
    mensualite_60m2_25ans: int
    mensualite_100m2_25ans: int
    revenu_min_60m2: int
    revenu_min_100m2: int
    indice_accessibilite: str
    ratio_prix_revenu: float
    revenu_median_local: Optional[int]   # iris_revenu_median — None si données IRIS absentes
    source_revenu: str                   # "local_iris" | "national_estime"
    confidence: int

    @classmethod
    def compute(cls, r: dict, taux: float = 3.5) -> "FinKPIs":
        prix   = float(r.get("prix_median_m2") or 0)
        est60  = int(prix * 60)
        est100 = int(prix * 100)

        tm = taux / 100 / 12
        n  = 25 * 12
        if tm > 0 and prix > 0:
            factor = tm / (1 - (1 + tm) ** -n)
            m60  = int(est60  * factor)
            m100 = int(est100 * factor)
        else:
            m60 = m100 = 0

        rev60  = int(m60  / 0.35) if m60  else 0
        rev100 = int(m100 / 0.35) if m100 else 0

        # Revenu local IRIS (annuel) — fallback sur la moyenne nationale 25 200 €/an
        iris_revenu = r.get("iris_revenu_median")
        if iris_revenu is not None:
            try:
                revenu_annuel = float(iris_revenu)
                revenu_local_int = int(revenu_annuel) if revenu_annuel > 0 else None
            except (ValueError, TypeError):
                revenu_annuel = 0
                revenu_local_int = None
        else:
            revenu_annuel = 0
            revenu_local_int = None

        # Revenu mensuel = annuel / 12 ; sinon médiane nationale estimée 2 100 €/mois
        revenu_mensuel = (revenu_annuel / 12) if revenu_annuel > 0 else 2100
        source_rev = "local_iris" if revenu_annuel > 0 else "national_estime"

        ratio = round(prix / revenu_mensuel, 2) if prix and revenu_mensuel else 0
        acc = (
            "très accessible" if ratio < 2   else
            "accessible"      if ratio < 3.5 else
            "tendu"           if ratio < 5   else
            "très tendu"
        )

        return cls(
            prix_m2_median=round(prix, 0),
            estimation_60m2=est60,
            estimation_100m2=est100,
            taux_ref_pct=taux,
            mensualite_60m2_25ans=m60,
            mensualite_100m2_25ans=m100,
            revenu_min_60m2=rev60,
            revenu_min_100m2=rev100,
            indice_accessibilite=acc,
            ratio_prix_revenu=ratio,
            revenu_median_local=revenu_local_int,
            source_revenu=source_rev,
            confidence=int(float(r.get("confidence") or 50)),
        )


@dataclass
class EvalKPIs:
    score_immobilier: int
    score_renovation: int
    score_financement: int
    score_localisation: int
    score_global: int
    is_good_deal: bool
    verdict: str
    recommendation: str
    points_forts: list
    points_faibles: list
    deal_confidence: int

    @classmethod
    def compute(cls, row: dict, immo: ImmoKPIs, reno: RenoKPIs, fin: FinKPIs) -> "EvalKPIs":
        s_immo = 50
        s_immo += 20 if immo.trend == "hot" else -20 if immo.trend == "cold" else 0
        s_immo += 15 if immo.nb_ventes_12m > 20 else 5 if immo.nb_ventes_12m > 5 else 0
        evo = immo.evolution_12m_pct
        s_immo += 15 if evo > 3 else 5 if evo > 0 else -15 if evo < -3 else 0
        s_immo = max(0, min(100, s_immo))

        s_reno = reno.score_energie
        acc_map = {"très accessible": 90, "accessible": 70, "tendu": 40, "très tendu": 20}
        s_fin  = acc_map.get(fin.indice_accessibilite, 50)
        s_loc  = int(immo.poi_score_global)

        # Global pondéré CLAUDE.md : 30/20/25/25
        s_global = int(s_immo * 0.30 + s_reno * 0.20 + s_fin * 0.25 + s_loc * 0.25)
        # Priorité à la valeur pré-calculée par la vue si disponible
        vue_score = int(float(row.get("score_global") or 0))
        if vue_score > 0:
            s_global = vue_score

        verdict = "🟢" if s_global > 75 else "🟡" if s_global > 55 else "🟠" if s_global > 35 else "🔴"

        if s_global > 75:
            reco = "Commune très attractive. Forte recommandation pour investissement ou installation."
        elif s_global > 55:
            reco = "Commune correcte avec un bon rapport qualité/prix. Potentiel intéressant."
        elif s_global > 35:
            reco = "Commune avec des points d'attention. Analyser les détails avant décision."
        else:
            reco = "Commune à risque. Vigilance recommandée sur les fondamentaux."

        forts, faibles = [], []
        if s_immo > 70: forts.append("Marché immobilier dynamique")
        if s_immo < 40: faibles.append("Marché immobilier atone")
        if s_reno > 70: forts.append("Bon parc énergétique")
        if s_reno < 40: faibles.append("Fort besoin de rénovation")
        if s_fin > 70:  forts.append("Immobilier accessible")
        if s_fin < 40:  faibles.append("Marché immobilier tendu")
        if s_loc > 70:  forts.append("Bonne desserte en services")
        if s_loc < 40:  faibles.append("Manque d'équipements de proximité")

        return cls(
            score_immobilier=s_immo,
            score_renovation=s_reno,
            score_financement=s_fin,
            score_localisation=s_loc,
            score_global=s_global,
            is_good_deal=s_global > 65,
            verdict=verdict,
            recommendation=reco,
            points_forts=forts,
            points_faibles=faibles,
            deal_confidence=min(immo.confidence, reno.confidence, fin.confidence),
        )


# ── Helpers Supabase ───────────────────────────────────────────────────────────

def _supabase_get(table: str, params: dict) -> list:
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=SB_HEADERS,
        params=params,
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def _fetch_commune_row(code_postal: str) -> Optional[dict]:
    rows = _supabase_get("kpi_commune", {
        "code_postal": f"eq.{code_postal}",
        "select": "*",
        "limit": "1",
    })
    return rows[0] if rows else None


def _build_kpis(row: dict) -> dict:
    immo  = ImmoKPIs.compute(row)
    reno  = RenoKPIs.compute(row)
    fin   = FinKPIs.compute(row)
    eval_ = EvalKPIs.compute(row, immo, reno, fin)
    return {
        "immobilier":  asdict(immo),
        "renovation":  asdict(reno),
        "financement": asdict(fin),
        "evaluation":  asdict(eval_),
    }


# ── Helper BAN — réconciliation INSEE ─────────────────────────────────────────

def _reconcile_insee(
    adresse: str,
    code_postal: str = None,
    lat: float = None,
    lon: float = None,
) -> dict:
    """
    Résolution INSEE en 4 tentatives (CLAUDE.md §1.4).
    Règle : confidence < 0.80 → avertissement, < 0.50 → rejet.
    """
    _ban_partial = None

    # T1 — BAN exact
    try:
        params = {"q": adresse, "limit": "1"}
        if code_postal:
            params["postcode"] = code_postal
        r = requests.get(f"{BAN_API}/search/", params=params, timeout=10)
        features = r.json().get("features", [])
        if features:
            f   = features[0]
            p   = f["properties"]
            c   = f["geometry"]["coordinates"]
            scr = float(p.get("score", 0))
            base = {"code_insee": p.get("citycode"), "confidence": scr,
                    "lat": c[1], "lon": c[0], "label": p.get("label"),
                    "code_postal_ban": p.get("postcode")}
            if scr >= 0.95:
                return {**base, "method": "ban_exact"}
            if scr >= 0.80:
                _ban_partial = {**base, "method": "ban_partial"}
    except Exception:
        pass

    # T2 — reverse geocode
    if lat and lon:
        try:
            r2 = requests.get(f"{GEO_API}/communes",
                               params={"lat": lat, "lon": lon, "fields": "code,nom"}, timeout=8)
            communes = r2.json()
            if communes:
                return {"code_insee": communes[0]["code"], "confidence": 0.90,
                        "method": "reverse_geocode", "lat": lat, "lon": lon,
                        "label": communes[0]["nom"], "code_postal_ban": None}
        except Exception:
            pass

    # T3 — BAN partiel
    if _ban_partial:
        return _ban_partial

    # T4 — CP unique
    if code_postal:
        try:
            r3 = requests.get(f"{GEO_API}/communes",
                               params={"codePostal": code_postal, "fields": "code,nom"}, timeout=8)
            communes = r3.json()
            if communes:
                conf = 0.75 if len(communes) == 1 else 0.50
                return {"code_insee": communes[0]["code"], "confidence": conf,
                        "method": "postal_code_unique" if len(communes) == 1 else "postal_code_ambiguous",
                        "lat": None, "lon": None, "label": communes[0]["nom"],
                        "code_postal_ban": code_postal}
        except Exception:
            pass

    return {"code_insee": None, "confidence": 0.0, "method": "failed",
            "lat": None, "lon": None, "label": None, "code_postal_ban": None}


# ── MCP Server ─────────────────────────────────────────────────────────────────

mcp = FastMCP(
    name="sahar-conseil",
    description=(
        "SAHAR Conseil — Plateforme d'aide à la décision immobilière. "
        "Open data français : DVF DGFiP, DPE ADEME, BAN, BPE INSEE, SIRENE. "
        "4 domaines : immobilier, rénovation énergétique, financement, évaluation."
    ),
    version="2.0.0",
)


@mcp.tool()
def query_commune(code_postal: str, domaine: str = "all") -> str:
    """
    Retourne les KPIs complets d'une commune française identifiée par son code postal.

    Domaines :
    - "immobilier"  → prix m², tendance, volume, vitesse de vente
    - "renovation"  → DPE, % passoires F+G, aides MaPrimeRénov estimées
    - "financement" → mensualités sur 25 ans, revenu minimum requis, accessibilité
    - "evaluation"  → score global 0-100, verdict, points forts/faibles, recommandation
    - "all"         → les 4 domaines (défaut)

    Args:
        code_postal : Code postal (ex: "33000" Bordeaux, "75001" Paris 1er)
        domaine     : immobilier / renovation / financement / evaluation / all

    Returns:
        JSON avec métadonnées commune + KPIs du domaine demandé.
    """
    try:
        row = _fetch_commune_row(code_postal)
        if not row:
            return json.dumps({
                "status": "not_found",
                "message": (
                    f"Aucune donnée pour le code postal {code_postal}. "
                    "Vérifiez le code ou essayez un code postal voisin."
                ),
                "code_postal": code_postal,
            }, ensure_ascii=False)

        kpis = _build_kpis(row)
        kpis_out = {domaine: kpis[domaine]} if (domaine != "all" and domaine in kpis) else kpis

        def _safe_float(v, dec=1):
            try:
                return round(float(v), dec) if v is not None else None
            except (ValueError, TypeError):
                return None

        def _safe_int(v):
            try:
                f = float(v)
                return int(f) if f == f else None  # NaN check
            except (ValueError, TypeError):
                return None

        iris_meta = {}
        if row.get("nb_iris"):
            iris_meta = {
                "nb_iris":              _safe_int(row.get("nb_iris")),
                "revenu_median":        _safe_int(row.get("iris_revenu_median")),
                "taux_pauvrete_pct":    _safe_float(row.get("iris_taux_pauvrete")),
                "taux_chomage_pct":     _safe_float(row.get("iris_taux_chomage")),
                "part_proprietaires":   _safe_float(row.get("iris_part_proprietaires")),
                "part_locataires":      _safe_float(row.get("iris_part_locataires")),
                "part_seniors":         _safe_float(row.get("iris_part_seniors")),
                "taille_menage_moy":    _safe_float(row.get("iris_taille_menage"), 2),
                "part_cadres":          _safe_float(row.get("iris_part_cadres")),
                "part_ouvriers":        _safe_float(row.get("iris_part_ouvriers")),
            }

        commune_meta = {
            "code_postal": row.get("code_postal"),
            "nom":         row.get("nom_commune"),
            "departement": row.get("code_departement"),
            "population":  _safe_int(row.get("population")),
        }
        if iris_meta:
            commune_meta["iris"] = iris_meta

        sources = ["DVF DGFiP", "DPE ADEME", "BPE INSEE", "SIRENE INSEE"]
        if iris_meta:
            sources.append("RP2020 INSEE / Filosofi 2020")

        return json.dumps({
            "status": "ok",
            "commune": commune_meta,
            "kpis": kpis_out,
            "sources": sources,
            "fraicheur": "Données 2024-2025",
        }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False)


@mcp.tool()
def enrich_address(adresse: str, code_postal: str = None, domaine: str = "all") -> str:
    """
    Normalise une adresse française (BAN) et retourne l'enrichissement complet de sa commune.

    Pipeline :
    1. Géocodage BAN — 4 tentatives de réconciliation INSEE (BAN exact → reverse → BAN partiel → CP unique)
    2. Vérification confidence ≥ 0.80 (sinon avertissement)
    3. Récupération des KPIs de la commune identifiée (kpi_commune view)

    Seuils de confiance :
    - ≥ 0.95 🟢 Match exact BAN — utiliser directement
    - 0.80-0.94 🟡 Match partiel — fiable avec contexte
    - 0.50-0.79 🟠 Faible confiance — résultat approximatif
    - < 0.50 🔴 Rejet — adresse non reconnue

    Args:
        adresse    : Adresse complète (ex: "12 rue de Rivoli Paris")
        code_postal: Code postal pour affiner (recommandé)
        domaine    : Domaine KPI (immobilier/renovation/financement/evaluation/all)

    Returns:
        JSON avec adresse normalisée (lat/lon, code INSEE, confidence) + KPIs commune.
    """
    try:
        rec = _reconcile_insee(adresse, code_postal)

        if rec["confidence"] < 0.50:
            return json.dumps({
                "status": "not_found",
                "message": "Adresse non reconnue. Vérifiez l'orthographe ou ajoutez le code postal.",
                "confidence": rec["confidence"],
                "method": rec["method"],
            }, ensure_ascii=False)

        warn = rec["confidence"] < 0.80

        code_insee = rec["code_insee"]
        cp_ban     = rec.get("code_postal_ban")

        # Recherche dans kpi_commune
        rows = _supabase_get("kpi_commune", {
            "code_commune": f"eq.{code_insee}",
            "select": "*", "limit": "1",
        }) if code_insee else []

        if not rows and cp_ban:
            rows = _supabase_get("kpi_commune", {
                "code_postal": f"eq.{cp_ban}",
                "select": "*", "limit": "1",
            })

        if not rows and code_postal:
            rows = _supabase_get("kpi_commune", {
                "code_postal": f"eq.{code_postal}",
                "select": "*", "limit": "1",
            })

        if not rows:
            return json.dumps({
                "status": "no_kpi",
                "message": f"Adresse géocodée (INSEE {code_insee}) mais aucun KPI disponible pour cette commune.",
                "address": rec,
                "confidence_warning": warn,
            }, ensure_ascii=False)

        row  = rows[0]
        kpis = _build_kpis(row)
        kpis_out = {domaine: kpis[domaine]} if (domaine != "all" and domaine in kpis) else kpis

        result = {
            "status": "ok",
            "confidence_warning": warn,
            "address": {
                "original":        adresse,
                "label_ban":       rec.get("label"),
                "code_insee":      code_insee,
                "lat":             rec.get("lat"),
                "lon":             rec.get("lon"),
                "confidence":      round(rec["confidence"], 3),
                "method":          rec["method"],
            },
            "commune": {
                "code_postal": row.get("code_postal"),
                "nom":         row.get("nom_commune"),
                "departement": row.get("departement"),
                "population":  int(float(row.get("population") or 0)),
            },
            "kpis": kpis_out,
        }
        if warn:
            result["warning"] = (
                f"Confiance BAN faible ({rec['confidence']:.0%}). "
                "Résultat approximatif — précisez le code postal pour améliorer la précision."
            )

        return json.dumps(result, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False)


@mcp.tool()
def search_communes(
    prix_max_m2: float = None,
    prix_min_m2: float = None,
    min_poi_score: int = None,
    max_pct_passoire: float = None,
    departement: str = None,
    trend: str = None,
    min_score_global: int = None,
    limit: int = 20,
) -> str:
    """
    Recherche des communes selon des critères multicritères.
    Tous les paramètres sont optionnels — combinez-les librement.

    Args:
        prix_max_m2      : Prix plafond au m² (ex: 3000)
        prix_min_m2      : Prix plancher au m² (ex: 1500)
        min_poi_score    : Score équipements minimum 0-100 (ex: 60)
        max_pct_passoire : % max passoires F+G (ex: 20)
        departement      : Code département (ex: "33", "69", "75")
        trend            : Tendance marché — "hot", "warm" ou "cold"
        min_score_global : Score global minimum 0-100 (ex: 60)
        limit            : Nombre max de résultats (défaut 20, max 100)

    Returns:
        Liste de communes triées par score global décroissant, avec KPIs synthétiques.
        Utilisez query_commune(code_postal) pour le détail complet.
    """
    try:
        params: dict = {
            "select": (
                "code_postal,nom_commune,departement,prix_median_m2,evolution_12m,"
                "trend,nb_ventes,score_energie,pct_passoire,poi_score_global,"
                "score_global,population"
            ),
            "order": "score_global.desc.nullslast",
            "limit": str(min(int(limit), 100)),
        }

        # Filtres Supabase PostgREST
        if prix_max_m2 is not None:       params["prix_median_m2"]  = f"lte.{prix_max_m2}"
        if prix_min_m2 is not None:       params["prix_median_m2"]  = f"gte.{prix_min_m2}"
        if min_poi_score is not None:     params["poi_score_global"] = f"gte.{min_poi_score}"
        if max_pct_passoire is not None:  params["pct_passoire"]     = f"lte.{max_pct_passoire}"
        if departement:                   params["departement"]      = f"eq.{departement}"
        if trend:                         params["trend"]            = f"eq.{trend}"
        if min_score_global is not None:  params["score_global"]     = f"gte.{min_score_global}"

        rows = _supabase_get("kpi_commune", params)

        communes = [
            {
                "code_postal":   row.get("code_postal"),
                "nom":           row.get("nom_commune"),
                "departement":   row.get("departement"),
                "prix_m2":       int(float(row.get("prix_median_m2") or 0)),
                "evolution_12m": round(float(row.get("evolution_12m") or 0), 1),
                "trend":         row.get("trend"),
                "nb_ventes":     int(float(row.get("nb_ventes") or 0)),
                "score_energie": int(float(row.get("score_energie") or 0)),
                "pct_passoire":  round(float(row.get("pct_passoire") or 0), 1),
                "poi_score":     int(float(row.get("poi_score_global") or 0)),
                "score_global":  int(float(row.get("score_global") or 0)),
                "population":    int(float(row.get("population") or 0)),
            }
            for row in rows
        ]

        criteres = {k: v for k, v in {
            "prix_max_m2": prix_max_m2, "prix_min_m2": prix_min_m2,
            "min_poi_score": min_poi_score, "max_pct_passoire": max_pct_passoire,
            "departement": departement, "trend": trend, "min_score_global": min_score_global,
        }.items() if v is not None}

        return json.dumps({
            "status": "ok",
            "nb_resultats": len(communes),
            "criteres": criteres,
            "communes": communes,
            "tip": "Utilisez query_commune(code_postal) pour le détail complet d'une commune.",
        }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False)


@mcp.tool()
def batch_enrich(addresses: list) -> str:
    """
    Enrichit un lot d'adresses en un seul appel (max 50).

    Chaque adresse est un dict avec "adresse" et optionnellement "code_postal".
    Retourne uniquement le domaine "evaluation" (score global + verdict) pour garder la réponse concise.

    Args:
        addresses: [{"adresse": "12 rue de Rivoli", "code_postal": "75001"}, ...]

    Returns:
        Dict avec résultats enrichis + stats (succès/échec/faible confiance).

    Exemple :
        batch_enrich([
            {"adresse": "place du Capitole", "code_postal": "31000"},
            {"adresse": "15 rue de la Paix", "code_postal": "75002"},
        ])
    """
    try:
        if not isinstance(addresses, list):
            return json.dumps({"status": "error", "message": "addresses doit être une liste"}, ensure_ascii=False)

        addresses = addresses[:50]
        results = []
        stats = {"total": len(addresses), "success": 0, "failed": 0, "low_confidence": 0}

        for addr in addresses:
            if not isinstance(addr, dict):
                stats["failed"] += 1
                results.append({"status": "error", "message": "Format invalide — attendu dict"})
                continue

            res_json = enrich_address(
                adresse=str(addr.get("adresse", "")),
                code_postal=str(addr.get("code_postal", "")) or None,
                domaine="evaluation",
            )
            res = json.loads(res_json)

            if res.get("status") == "ok":
                stats["success"] += 1
                if res.get("confidence_warning"):
                    stats["low_confidence"] += 1
            else:
                stats["failed"] += 1

            results.append({
                "input":     addr,
                "status":    res.get("status"),
                "commune":   res.get("commune"),
                "address":   res.get("address"),
                "evaluation": res.get("kpis", {}).get("evaluation"),
                "warning":   res.get("warning"),
            })

        return json.dumps({
            "status": "ok",
            "stats": stats,
            "taux_succes": f"{stats['success']}/{stats['total']}",
            "results": results,
        }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False)


@mcp.tool()
def health_check() -> str:
    """
    Vérifie l'état de santé du système SAHAR Conseil.

    Contrôles :
    - Supabase   : connectivité + nombre de communes disponibles dans kpi_commune
    - BAN API    : disponibilité du géocodage
    - Données    : dernière année DVF disponible

    Returns:
        JSON avec statut global (healthy / degraded) et détail par check.
    """
    checks = {}
    details = {}

    # Supabase kpi_commune
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/kpi_commune",
            headers={**SB_HEADERS, "Prefer": "count=exact"},
            params={"select": "code_postal"},
            timeout=10,
        )
        count = r.headers.get("content-range", "?/?").split("/")[-1]
        checks["supabase"] = "ok"
        details["communes_avec_kpis"] = count
    except Exception as e:
        checks["supabase"] = f"error: {e}"

    # BAN API
    try:
        r2 = requests.get(f"{BAN_API}/search/", params={"q": "Paris", "limit": "1"}, timeout=8)
        r2.raise_for_status()
        checks["ban_api"] = "ok"
    except Exception as e:
        checks["ban_api"] = f"error: {e}"

    # DVF fraîcheur
    try:
        rows = _supabase_get("market_stats", {
            "select": "annee", "order": "annee.desc", "limit": "1",
        })
        details["dvf_annee_max"] = rows[0]["annee"] if rows else "unknown"
        checks["data"] = "ok"
    except Exception as e:
        checks["data"] = f"error: {e}"

    all_ok = all(v == "ok" for v in checks.values())

    return json.dumps({
        "status": "healthy" if all_ok else "degraded",
        "version": "2.0.0",
        "checks": checks,
        "details": details,
        "endpoints": {
            "supabase": SUPABASE_URL,
            "ban_api": BAN_API,
            "geo_api": GEO_API,
        },
    }, ensure_ascii=False, indent=2)


# ── Resource stats ─────────────────────────────────────────────────────────────

@mcp.resource("sahar://stats")
def get_stats() -> str:
    """Statistiques globales de la base SAHAR Conseil."""
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/kpi_commune",
            headers={**SB_HEADERS, "Prefer": "count=exact"},
            params={"select": "code_postal"},
            timeout=10,
        )
        count = r.headers.get("content-range", "?/?").split("/")[-1]
        return json.dumps({
            "communes_avec_kpis": count,
            "tables_supabase": [
                "dvf_mutations (20 687 transactions)",
                "ban_adresses (142 000 adresses)",
                "dpe_diagnostics (DPE ADEME)",
                "poi_equipements (6 644 POI)",
                "poi_scores_commune (467 communes)",
                "sirene_stats (SIRENE INSEE)",
                "commune_wiki (35 625 communes)",
                "market_stats (agrégats DVF)",
                "dpe_commune_agg (agrégats DPE)",
            ],
            "domaines_kpi": ["immobilier", "renovation", "financement", "evaluation"],
            "version": "2.0.0",
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"erreur": str(e)}, ensure_ascii=False)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SAHAR Conseil MCP Server v2")
    parser.add_argument("--http", action="store_true", help="Mode HTTP SSE (défaut: stdio)")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.http:
        print(f"🏠 SAHAR MCP Server v2 → HTTP SSE ::{args.port}", file=sys.stderr)
        mcp.run(transport="sse", host="0.0.0.0", port=args.port)
    else:
        print("🏠 SAHAR MCP Server v2 → stdio", file=sys.stderr)
        mcp.run(transport="stdio")
