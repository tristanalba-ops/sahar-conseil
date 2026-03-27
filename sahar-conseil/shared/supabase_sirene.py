"""
SAHAR Conseil — supabase_sirene.py
Accès aux données SIRENE depuis Supabase.

Tables :
  - sirene_etablissements : 39 codes NAF cibles, tous départements
  - sirene_naf_mapping    : correspondance NAF → secteur SAHAR
  - sirene_stats (vue)    : agrégation par département × secteur

Usage :
    from shared.supabase_sirene import get_etablissements, get_stats, search_entreprises
"""

import streamlit as st
import pandas as pd
from typing import Optional, List

SUPABASE_URL = "https://ylrrcbklufshebcizgus.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlscnJjYmtsdWZzaGViY2l6Z3VzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ1NjQzNTEsImV4cCI6MjA5MDE0MDM1MX0.KQjvB5aePbmCcrAu9yYKoIblDG0ui90LXa-DcL7HAEA"


def _get_client():
    try:
        url = st.secrets.get("SUPABASE_URL", SUPABASE_URL)
        key = st.secrets.get("SUPABASE_KEY", SUPABASE_ANON_KEY)
        from supabase import create_client
        return create_client(url, key)
    except Exception:
        return None


@st.cache_data(ttl=3600)
def get_naf_mapping() -> pd.DataFrame:
    """Retourne le mapping NAF → secteur SAHAR."""
    client = _get_client()
    if not client:
        return pd.DataFrame()
    resp = client.table("sirene_naf_mapping").select("*").execute()
    return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()


@st.cache_data(ttl=1800)
def get_etablissements(
    departement: str,
    secteur: Optional[str] = None,
    naf: Optional[str] = None,
    commune: Optional[str] = None,
    score_min: int = 0,
    tags: Optional[List[str]] = None,
    limit: int = 2000,
) -> pd.DataFrame:
    """
    Requête les établissements SIRENE filtrés.
    """
    client = _get_client()
    if not client:
        return pd.DataFrame()

    all_rows = []
    offset = 0
    batch = min(1000, limit)

    while len(all_rows) < limit:
        query = client.table("sirene_etablissements").select(
            "siret,siren,denomination,nom_commercial,prenom,nom,"
            "naf,naf_libelle,tranche_effectifs,"
            "adresse,code_postal,commune,departement,"
            "longitude,latitude,date_creation,date_debut_activite,"
            "secteur_sahar,score_potentiel,tags"
        ).eq("departement", departement).eq("etat_administratif", "A")

        if secteur:
            query = query.eq("secteur_sahar", secteur)
        if naf:
            query = query.eq("naf", naf)
        if commune:
            query = query.ilike("commune", f"%{commune}%")
        if score_min > 0:
            query = query.gte("score_potentiel", score_min)
        if tags:
            query = query.contains("tags", tags)

        query = query.order("score_potentiel", desc=True)
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
    if "date_creation" in df.columns:
        df["date_creation"] = pd.to_datetime(df["date_creation"], errors="coerce")
    for col in ["longitude", "latitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


@st.cache_data(ttl=3600)
def get_stats(departement: Optional[str] = None) -> pd.DataFrame:
    """Retourne les stats agrégées (vue sirene_stats)."""
    client = _get_client()
    if not client:
        return pd.DataFrame()

    query = client.table("sirene_stats").select("*")
    if departement:
        query = query.eq("departement", departement)

    resp = query.execute()
    return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()


@st.cache_data(ttl=600)
def search_entreprises(
    terme: str,
    departement: Optional[str] = None,
    limit: int = 50,
) -> pd.DataFrame:
    """Recherche textuelle sur dénomination/adresse."""
    client = _get_client()
    if not client:
        return pd.DataFrame()

    query = client.table("sirene_etablissements").select(
        "siret,siren,denomination,nom_commercial,prenom,nom,"
        "naf,naf_libelle,adresse,code_postal,commune,departement,"
        "secteur_sahar,score_potentiel,tags,date_creation"
    ).eq("etat_administratif", "A").or_(
        f"denomination.ilike.%{terme}%,nom_commercial.ilike.%{terme}%,"
        f"adresse.ilike.%{terme}%,commune.ilike.%{terme}%"
    )

    if departement:
        query = query.eq("departement", departement)

    query = query.limit(limit)
    resp = query.execute()
    return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()


def get_secteurs() -> list:
    """Liste des secteurs SAHAR disponibles."""
    return ["immobilier", "energie", "retail", "auto", "rh", "sante"]


def get_secteur_label(secteur: str) -> str:
    labels = {
        "immobilier": "🏠 Immobilier",
        "energie": "⚡ Énergie / Rénovation",
        "retail": "🏪 Retail / Franchise",
        "auto": "🚗 Automobile",
        "rh": "👥 RH / Recrutement",
        "sante": "🏥 Santé",
    }
    return labels.get(secteur, secteur)
