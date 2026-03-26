"""
SAHAR Conseil — data_loader.py
Chargement, téléchargement et cache des données open data.
Utilise @st.cache_data pour éviter les rechargements inutiles.
"""

import streamlit as st
import pandas as pd
import requests
import os
from pathlib import Path

# Dossiers de données relatifs à la racine du repo
DATA_RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
DATA_PROCESSED = Path(__file__).resolve().parents[1] / "data" / "processed"

# URLs des fichiers DVF par département (data.gouv.fr)
DVF_BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"

# URL API DPE ADEME
DPE_API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines"


# ─────────────────────────────────────────────
# DVF — Demandes de Valeurs Foncières
# ─────────────────────────────────────────────

@st.cache_data(ttl=86400, show_spinner="Chargement des données DVF...")
def load_dvf(departement: str) -> pd.DataFrame:
    """
    Charge les transactions DVF pour un département donné.
    Télécharge le CSV depuis data.gouv.fr si absent en local.

    Args:
        departement: Code département sur 2-3 caractères (ex: "75", "69", "2A")

    Returns:
        DataFrame nettoyé avec colonnes standardisées.

    Exemple:
        df = load_dvf("75")
    """
    cache_path = DATA_PROCESSED / f"dvf_{departement}.parquet"

    # Retourner le cache parquet si disponible
    if cache_path.exists():
        return pd.read_parquet(cache_path)

    # Télécharger si absent
    url = f"{DVF_BASE_URL}/{departement}/communes/{departement}.csv"
    raw_path = DATA_RAW / f"dvf_{departement}.csv"

    if not raw_path.exists():
        st.info(f"Téléchargement DVF département {departement}...")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        DATA_RAW.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(response.content)

    # Charger et nettoyer
    df = _clean_dvf(pd.read_csv(raw_path, low_memory=False))

    # Sauvegarder en parquet pour les prochains chargements
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)

    return df


def _clean_dvf(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyage et standardisation des colonnes DVF brutes."""
    colonnes_utiles = [
        "id_mutation", "date_mutation", "nature_mutation",
        "valeur_fonciere", "adresse_numero", "adresse_nom_voie",
        "adresse_code_voie", "code_postal", "code_commune",
        "nom_commune", "code_departement", "type_local",
        "surface_reelle_bati", "nombre_pieces_principales",
        "longitude", "latitude"
    ]

    # Garder seulement les colonnes existantes
    cols = [c for c in colonnes_utiles if c in df.columns]
    df = df[cols].copy()

    # Conversions types
    df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
    df["valeur_fonciere"] = pd.to_numeric(df["valeur_fonciere"], errors="coerce")
    df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")

    # Filtrer les lignes utiles (appartements et maisons, transactions réelles)
    df = df[df["type_local"].isin(["Appartement", "Maison"])]
    df = df[df["nature_mutation"] == "Vente"]
    df = df.dropna(subset=["valeur_fonciere", "surface_reelle_bati"])
    df = df[df["surface_reelle_bati"] > 5]      # exclure les surfaces aberrantes
    df = df[df["valeur_fonciere"] > 1000]        # exclure les prix aberrants

    # Calcul prix/m²
    df["prix_m2"] = (df["valeur_fonciere"] / df["surface_reelle_bati"]).round(0)

    # Renommer pour la lisibilité
    df = df.rename(columns={
        "longitude": "lon",
        "latitude": "lat",
    })

    return df.reset_index(drop=True)


def load_dvf_depuis_fichier(chemin_fichier: str) -> pd.DataFrame:
    """
    Charge le DVF depuis un fichier CSV local (utile si vous avez
    téléchargé manuellement le fichier depuis data.gouv.fr).

    Args:
        chemin_fichier: Chemin vers le fichier CSV DVF.

    Exemple:
        df = load_dvf_depuis_fichier("data/raw/dvf_75.csv")
    """
    df = pd.read_csv(chemin_fichier, low_memory=False)
    return _clean_dvf(df)


# ─────────────────────────────────────────────
# DPE — Diagnostics de Performance Énergétique
# ─────────────────────────────────────────────

@st.cache_data(ttl=86400, show_spinner="Chargement des données DPE...")
def load_dpe(code_postal: str, nb_resultats: int = 500) -> pd.DataFrame:
    """
    Charge les DPE via l'API ADEME pour un code postal.

    Args:
        code_postal: Code postal sur 5 chiffres (ex: "75015")
        nb_resultats: Nombre max de résultats à récupérer (défaut 500)

    Returns:
        DataFrame avec étiquettes DPE/GES, adresse, coordonnées.

    Exemple:
        df = load_dpe("69001")
    """
    params = {
        "q": code_postal,
        "q_fields": "code_postal_ban",
        "size": nb_resultats,
        "select": (
            "numero_dpe,date_etablissement_dpe,"
            "etiquette_dpe,etiquette_ges,"
            "conso_5_usages_e_finale,conso_5_usages_ep,"
            "adresse_ban,code_postal_ban,nom_commune_ban,"
            "latitude,longitude,type_batiment,"
            "annee_construction,surface_habitable_logement"
        )
    }
    try:
        response = requests.get(DPE_API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data.get("results", []))
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"latitude": "lat", "longitude": "lon"})
        df["date_etablissement_dpe"] = pd.to_datetime(
            df["date_etablissement_dpe"], errors="coerce"
        )
        return df
    except requests.RequestException as e:
        st.error(f"Erreur API DPE : {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────
# INSEE — Données socio-économiques par commune
# ─────────────────────────────────────────────

@st.cache_data(ttl=604800, show_spinner="Chargement données INSEE...")
def load_insee_communes() -> pd.DataFrame:
    """
    Charge le référentiel des communes INSEE (code, nom, population, département).
    Fichier léger inclus dans le repo.
    """
    path = DATA_RAW / "communes_insee.csv"
    if path.exists():
        return pd.read_csv(path, dtype={"code_commune": str, "code_departement": str})
    # Fallback : données minimales codées en dur pour les 10 plus grandes villes
    return pd.DataFrame({
        "code_commune": ["75056", "13055", "69123", "31555", "06088"],
        "nom_commune": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice"],
        "code_departement": ["75", "13", "69", "31", "06"],
        "population": [2161000, 873000, 522000, 493000, 342000],
    })


@st.cache_data(ttl=604800)
def load_bpe(type_equipement: str = None) -> pd.DataFrame:
    """
    Charge la Base Permanente des Équipements INSEE.
    Permet de mesurer l'attractivité d'une zone (commerces, médecins, écoles...).

    Args:
        type_equipement: Filtre optionnel sur le type (ex: "A" pour commerces)

    Returns:
        DataFrame avec code_commune, type_equipement, nb_equipements.
    """
    path = DATA_RAW / "bpe.csv"
    if not path.exists():
        st.warning(
            "Fichier BPE absent. Télécharger depuis : "
            "https://www.insee.fr/fr/statistiques/3568638"
        )
        return pd.DataFrame()
    df = pd.read_csv(path, sep=";", dtype={"CODGEO": str}, low_memory=False)
    df = df.rename(columns={"CODGEO": "code_commune", "TYPEQU": "type_equipement"})
    if type_equipement:
        df = df[df["type_equipement"].str.startswith(type_equipement)]
    return df


# ─────────────────────────────────────────────
# Utilitaires
# ─────────────────────────────────────────────

def liste_departements() -> list:
    """Retourne la liste des codes département métropolitains."""
    deps = [str(i).zfill(2) for i in range(1, 96) if i != 20]
    deps += ["2A", "2B"]
    return sorted(deps)


def filtrer_par_periode(df: pd.DataFrame, col_date: str, mois: int = 24) -> pd.DataFrame:
    """Filtre un DataFrame pour ne garder que les N derniers mois."""
    cutoff = pd.Timestamp.now() - pd.DateOffset(months=mois)
    return df[df[col_date] >= cutoff].copy()
