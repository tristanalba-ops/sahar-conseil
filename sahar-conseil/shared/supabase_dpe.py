"""
SAHAR Conseil — supabase_dpe.py
Accès aux données DPE depuis Supabase.

Les données DPE (125 000+ logements E/F/G sur 7 départements) sont
stockées dans Supabase. Ce module fournit un accès simple et cacheable.

Tables utilisées :
  - dpe_logements : données individuelles (140k lignes)
  - dpe_communes  : agrégation par commune (2346 lignes)

Usage :
    from shared.supabase_dpe import get_dpe_communes, get_dpe_logements, get_passoires

    # Agrégation par commune (rapide, ~2300 lignes)
    df = get_dpe_communes(departement="33")

    # Logements individuels d'un département
    df = get_dpe_logements(departement="33", etiquettes=["F", "G"])

    # Passoires thermiques F+G
    df = get_passoires(departement="33")
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from typing import Optional, List

# Cache local pour l'agrégation communes (fichier Parquet)
_AGG_CACHE = Path(__file__).parent.parent / "data" / "processed" / "dpe_communes_agg.parquet"

# ── Supabase Config ──────────────────────────────────────────────────────────

SUPABASE_URL = "https://ylrrcbklufshebcizgus.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlscnJjYmtsdWZzaGViY2l6Z3VzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ1NjQzNTEsImV4cCI6MjA5MDE0MDM1MX0.KQjvB5aePbmCcrAu9yYKoIblDG0ui90LXa-DcL7HAEA"


def _get_supabase():
    """Retourne le client Supabase (singleton via st.secrets ou config par défaut)."""
    try:
        url = st.secrets.get("SUPABASE_URL", SUPABASE_URL)
        key = st.secrets.get("SUPABASE_KEY", SUPABASE_ANON_KEY)
        from supabase import create_client
        return create_client(url, key)
    except Exception:
        return None


# ── Agrégation communes ──────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_dpe_communes(departement: Optional[str] = None) -> pd.DataFrame:
    """
    Retourne l'agrégation DPE par commune.
    Colonnes : departement, commune, code_postal, code_insee, nb_dpe_efg,
               nb_e, nb_f, nb_g, pct_fg, conso_moy, ges_moy, periode_dominante

    Essaye d'abord le cache Parquet local, sinon requête Supabase.
    """
    # 1. Cache Parquet local
    if _AGG_CACHE.exists():
        df = pd.read_parquet(_AGG_CACHE)
        if departement:
            df = df[df["departement"] == departement].copy()
        return df

    # 2. Fallback Supabase
    client = _get_supabase()
    if not client:
        return pd.DataFrame()

    query = client.table("dpe_communes").select("*")
    if departement:
        query = query.eq("departement", departement)
    resp = query.execute()

    if not resp.data:
        return pd.DataFrame()

    df = pd.DataFrame(resp.data)
    drop_cols = [c for c in ["id", "created_at"] if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df


# ── Logements individuels ────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_dpe_logements(
    departement: str,
    etiquettes: Optional[List[str]] = None,
    commune: Optional[str] = None,
    score_min: int = 0,
    limit: int = 5000,
) -> pd.DataFrame:
    """
    Retourne les logements DPE individuels depuis Supabase.

    Args:
        departement: Code département (obligatoire)
        etiquettes: Filtrer par étiquettes DPE (ex: ["F", "G"])
        commune: Filtrer par commune
        score_min: Score urgence minimum (0-100)
        limit: Nombre max de résultats

    Returns:
        DataFrame avec colonnes : numero_dpe, etiquette_dpe, etiquette_ges,
        departement, code_postal, commune, code_insee, adresse, longitude,
        latitude, conso_ef, conso_par_m2, emission_ges, type_batiment,
        periode_construction, surface_immeuble, energie_principale,
        date_dpe, score_urgence
    """
    client = _get_supabase()
    if not client:
        return pd.DataFrame()

    all_rows = []
    offset = 0
    batch = min(1000, limit)

    while len(all_rows) < limit:
        query = client.table("dpe_logements").select(
            "numero_dpe,etiquette_dpe,etiquette_ges,departement,code_postal,"
            "commune,code_insee,adresse,longitude,latitude,conso_ef,conso_par_m2,"
            "emission_ges,type_batiment,periode_construction,surface_immeuble,"
            "energie_principale,date_dpe,score_urgence"
        ).eq("departement", departement)

        if etiquettes:
            query = query.in_("etiquette_dpe", etiquettes)
        if commune:
            query = query.eq("commune", commune)
        if score_min > 0:
            query = query.gte("score_urgence", score_min)

        query = query.order("score_urgence", desc=True)
        query = query.range(offset, offset + batch - 1)

        resp = query.execute()
        rows = resp.data or []
        if not rows:
            break

        all_rows.extend(rows)
        offset += batch

        if len(rows) < batch:
            break

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    if "date_dpe" in df.columns:
        df["date_dpe"] = pd.to_datetime(df["date_dpe"], errors="coerce")
    for col in ["conso_ef", "conso_par_m2", "emission_ges", "surface_immeuble",
                "longitude", "latitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_passoires(departement: str, limit: int = 5000) -> pd.DataFrame:
    """Raccourci pour obtenir les logements F et G (passoires thermiques)."""
    return get_dpe_logements(departement, etiquettes=["F", "G"], limit=limit)


# ── Stats rapides ────────────────────────────────────────────────────────────

def get_dpe_stats(departement: str) -> dict:
    """Retourne des stats rapides pour un département."""
    df = get_dpe_communes(departement)
    if df.empty:
        return {}

    return {
        "nb_communes": len(df),
        "nb_dpe_total": int(df["nb_dpe_efg"].sum()),
        "nb_f": int(df["nb_f"].sum()),
        "nb_g": int(df["nb_g"].sum()),
        "nb_fg": int(df["nb_f"].sum() + df["nb_g"].sum()),
        "pct_fg_moyen": round(df["pct_fg"].mean(), 1),
        "top_communes_fg": df.nlargest(10, "pct_fg")[
            ["commune", "code_postal", "nb_dpe_efg", "nb_f", "nb_g", "pct_fg"]
        ].to_dict("records"),
    }
