"""
SAHAR Conseil — data_loader.py
Chargement et cache des données open data.
"""

import streamlit as st
import pandas as pd
import requests
from pathlib import Path


def _find_dir(relative: str) -> Path:
    """Trouve un dossier data/ quel que soit l'environnement."""
    base = Path(__file__).resolve()
    candidates = [
        base.parents[1] / relative,
        base.parents[2] / relative,
        Path("/mount/src/sahar-conseil/sahar-conseil") / relative,
        Path("/mount/src/sahar-conseil") / relative,
    ]
    for p in candidates:
        if p.exists():
            return p
    fallback = base.parents[1] / relative
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


DATA_RAW = _find_dir("data/raw")
DATA_PROCESSED = _find_dir("data/processed")

DPE_API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines"


@st.cache_data(ttl=86400, show_spinner="Chargement des données DVF...")
def load_dvf(departement: str) -> pd.DataFrame:
    """
    Charge les transactions DVF pour un département.
    Cherche d'abord un fichier local, sinon télécharge depuis data.gouv.fr.
    """
    # Chercher le fichier dans tous les emplacements possibles
    base_paths = [
        Path(__file__).resolve().parents[1],
        Path(__file__).resolve().parents[2],
        Path("/mount/src/sahar-conseil/sahar-conseil"),
        Path("/mount/src/sahar-conseil"),
    ]

    raw_path = None
    for base in base_paths:
        candidate = base / "data" / "raw" / f"dvf_{departement}.csv"
        if candidate.exists() and candidate.stat().st_size > 1000:
            raw_path = candidate
            break

    if raw_path is None:
        st.info(f"Fichier local introuvable — téléchargement DVF département {departement}...")
        DATA_RAW.mkdir(parents=True, exist_ok=True)
        raw_path = DATA_RAW / f"dvf_{departement}.csv"

        urls = [
            f"https://files.data.gouv.fr/geo-dvf/latest/csv/{departement}/full.csv",
            f"https://files.data.gouv.fr/geo-dvf/latest/csv/{departement}/communes/{departement}.csv",
        ]

        ok = False
        for url in urls:
            try:
                r = requests.get(url, timeout=120)
                if r.status_code == 200 and len(r.content) > 1000:
                    raw_path.write_bytes(r.content)
                    ok = True
                    break
            except Exception:
                continue

        if not ok:
            st.error(
                f"Impossible de télécharger les données DVF pour le département {departement}. "
                "Télécharger depuis https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/ "
                f"et uploader dans data/raw/ sous le nom dvf_{departement}.csv"
            )
            st.stop()

    return _clean_dvf(pd.read_csv(raw_path, low_memory=False))


def _clean_dvf(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyage et standardisation des colonnes DVF brutes."""
    colonnes_utiles = [
        "id_mutation", "date_mutation", "nature_mutation",
        "valeur_fonciere", "adresse_numero", "adresse_nom_voie",
        "code_postal", "code_commune", "nom_commune",
        "code_departement", "type_local",
        "surface_reelle_bati", "nombre_pieces_principales",
        "longitude", "latitude"
    ]
    cols = [c for c in colonnes_utiles if c in df.columns]
    df = df[cols].copy()

    df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
    df["valeur_fonciere"] = pd.to_numeric(df["valeur_fonciere"], errors="coerce")
    df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")

    df = df[df["type_local"].isin(["Appartement", "Maison"])]
    df = df[df["nature_mutation"] == "Vente"]
    df = df.dropna(subset=["valeur_fonciere", "surface_reelle_bati"])
    df = df[df["surface_reelle_bati"] > 5]
    df = df[df["valeur_fonciere"] > 1000]

    df["prix_m2"] = (df["valeur_fonciere"] / df["surface_reelle_bati"]).round(0)
    df = df.rename(columns={"longitude": "lon", "latitude": "lat"})

    return df.reset_index(drop=True)


def load_dvf_depuis_fichier(chemin_fichier: str) -> pd.DataFrame:
    """Charge le DVF depuis un fichier CSV local."""
    return _clean_dvf(pd.read_csv(chemin_fichier, low_memory=False))


@st.cache_data(ttl=86400, show_spinner="Chargement des données DPE...")
def load_dpe(code_postal: str, nb_resultats: int = 500) -> pd.DataFrame:
    """Charge les DPE via l'API ADEME."""
    params = {
        "q": code_postal,
        "q_fields": "code_postal_ban",
        "size": nb_resultats,
        "select": (
            "numero_dpe,date_etablissement_dpe,etiquette_dpe,etiquette_ges,"
            "conso_5_usages_e_finale,adresse_ban,code_postal_ban,nom_commune_ban,"
            "latitude,longitude,type_batiment,annee_construction,surface_habitable_logement"
        )
    }
    try:
        r = requests.get(DPE_API_URL, params=params, timeout=30)
        r.raise_for_status()
        df = pd.DataFrame(r.json().get("results", []))
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"latitude": "lat", "longitude": "lon"})
        df["date_etablissement_dpe"] = pd.to_datetime(df["date_etablissement_dpe"], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Erreur API DPE : {e}")
        return pd.DataFrame()


def liste_departements() -> list:
    """Liste des codes département métropolitains."""
    deps = [str(i).zfill(2) for i in range(1, 96) if i != 20]
    deps += ["2A", "2B"]
    return sorted(deps)


def filtrer_par_periode(df: pd.DataFrame, col_date: str, mois: int = 24) -> pd.DataFrame:
    """Filtre sur les N derniers mois."""
    cutoff = pd.Timestamp.now() - pd.DateOffset(months=mois)
    return df[df[col_date] >= cutoff].copy()
