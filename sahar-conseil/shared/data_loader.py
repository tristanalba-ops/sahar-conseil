"""
SAHAR Conseil — data_loader.py v3
Chargement optimisé : parquet prioritaire, CSV fallback.
"""

import streamlit as st
import pandas as pd
import requests
from pathlib import Path


def _find_dir(relative: str) -> Path:
    candidates = [
        Path(__file__).resolve().parents[1] / relative,
        Path(__file__).resolve().parents[2] / relative,
        Path("/mount/src/sahar-conseil/sahar-conseil") / relative,
        Path("/mount/src/sahar-conseil") / relative,
    ]
    for p in candidates:
        if p.exists():
            return p
    default = Path(__file__).resolve().parents[1] / relative
    default.mkdir(parents=True, exist_ok=True)
    return default


DATA_RAW = _find_dir("data/raw")
DATA_PROCESSED = _find_dir("data/processed")
DPE_API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines"


@st.cache_data(ttl=3600, show_spinner="Chargement DVF...")
def load_dvf(departement: str) -> pd.DataFrame:
    # 1. Chercher parquet dans tous les emplacements
    for base in [Path(__file__).resolve().parents[1],
                 Path(__file__).resolve().parents[2],
                 Path("/mount/src/sahar-conseil/sahar-conseil"),
                 Path("/mount/src/sahar-conseil")]:
        p = base / "data" / "processed" / f"dvf_{departement}.parquet"
        if p.exists() and p.stat().st_size > 1000:
            return pd.read_parquet(p)

    # 2. Chercher CSV
    csv_path = None
    for base in [Path(__file__).resolve().parents[1],
                 Path(__file__).resolve().parents[2],
                 Path("/mount/src/sahar-conseil/sahar-conseil"),
                 Path("/mount/src/sahar-conseil")]:
        c = base / "data" / "raw" / f"dvf_{departement}.csv"
        if c.exists() and c.stat().st_size > 1000:
            csv_path = c
            break

    if csv_path is None:
        st.error(f"Fichier DVF introuvable pour le département {departement}.")
        st.stop()

    return _load_and_clean_csv(csv_path)


def _load_and_clean_csv(path: Path) -> pd.DataFrame:
    cols = ['id_mutation','date_mutation','nature_mutation','valeur_fonciere',
            'adresse_numero','adresse_nom_voie','code_postal','code_commune',
            'nom_commune','type_local','surface_reelle_bati','nombre_pieces_principales',
            'surface_terrain','lot1_surface_carrez','longitude','latitude','id_parcelle']
    dtypes = {'code_commune':'str','code_postal':'str',
              'nature_mutation':'category','type_local':'category','nom_commune':'category'}

    header = pd.read_csv(path, nrows=0).columns.tolist()
    usecols = [c for c in cols if c in header]

    df = pd.read_csv(path, usecols=usecols, dtype=dtypes, low_memory=False)
    df['date_mutation'] = pd.to_datetime(df['date_mutation'], errors='coerce')
    for col in ['valeur_fonciere','surface_reelle_bati','surface_terrain',
                'lot1_surface_carrez','longitude','latitude']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df[df['type_local'].isin(['Appartement','Maison'])]
    df = df[df['nature_mutation'] == 'Vente']
    df = df.dropna(subset=['valeur_fonciere','surface_reelle_bati'])
    df = df[(df['surface_reelle_bati'] > 5) & (df['valeur_fonciere'] > 1000)]

    df['surface_utile'] = df['surface_reelle_bati']
    if 'lot1_surface_carrez' in df.columns:
        m = df['lot1_surface_carrez'].notna() & (df['lot1_surface_carrez'] > 0)
        df.loc[m, 'surface_utile'] = df.loc[m, 'lot1_surface_carrez']

    df['prix_m2'] = (df['valeur_fonciere'] / df['surface_utile']).round(0)
    df = df[df['prix_m2'].between(500, 25000)]
    df['adresse'] = (df.get('adresse_numero', pd.Series('')).fillna('').astype(str).str.strip() + ' ' +
                     df.get('adresse_nom_voie', pd.Series('')).fillna('').astype(str)).str.strip()
    df['annee'] = df['date_mutation'].dt.year
    df['mois'] = df['date_mutation'].dt.to_period('M').astype(str)
    return df.reset_index(drop=True)


@st.cache_data(ttl=86400)
def load_dpe(code_postal: str, nb_resultats: int = 500) -> pd.DataFrame:
    params = {
        "q": code_postal, "q_fields": "code_postal_ban", "size": nb_resultats,
        "select": "numero_dpe,date_etablissement_dpe,etiquette_dpe,etiquette_ges,"
                  "conso_5_usages_e_finale,adresse_ban,code_postal_ban,nom_commune_ban,"
                  "latitude,longitude,type_batiment,annee_construction,surface_habitable_logement"
    }
    try:
        r = requests.get(DPE_API_URL, params=params, timeout=30)
        r.raise_for_status()
        df = pd.DataFrame(r.json().get("results", []))
        if not df.empty:
            df = df.rename(columns={"latitude": "lat", "longitude": "lon"})
            df["date_etablissement_dpe"] = pd.to_datetime(df["date_etablissement_dpe"], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Erreur API DPE : {e}")
        return pd.DataFrame()


def liste_departements() -> list:
    return sorted([str(i).zfill(2) for i in range(1, 96) if i != 20] + ["2A", "2B"])


def filtrer_par_periode(df: pd.DataFrame, col_date: str, mois: int = 24) -> pd.DataFrame:
    cutoff = pd.Timestamp.now() - pd.DateOffset(months=mois)
    return df[df[col_date] >= cutoff].copy()
