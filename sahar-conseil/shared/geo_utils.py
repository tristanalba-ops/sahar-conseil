"""
SAHAR Conseil — geo_utils.py
Utilitaires géographiques : geocoding BAN, jointures IRIS, filtres par zone.
"""

import requests
import pandas as pd
import streamlit as st
from typing import Optional


# ─────────────────────────────────────────────
# GEOCODING — Base Adresse Nationale (BAN)
# ─────────────────────────────────────────────

BAN_API_URL = "https://api-adresse.data.gouv.fr/search/"
BAN_BATCH_URL = "https://api-adresse.data.gouv.fr/search/csv/"


def geocoder_adresse(adresse: str, code_postal: str = None) -> Optional[dict]:
    """
    Géocode une adresse via l'API BAN (Base Adresse Nationale).
    Retourne latitude, longitude et score de confiance.

    Args:
        adresse: Adresse complète (ex: "10 rue de la Paix")
        code_postal: Code postal pour affiner la recherche

    Returns:
        dict {"lat": float, "lon": float, "score": float, "label": str}
        ou None si introuvable.

    Exemple:
        coords = geocoder_adresse("10 rue de la Paix", "75002")
    """
    params = {"q": adresse, "limit": 1}
    if code_postal:
        params["postcode"] = code_postal

    try:
        response = requests.get(BAN_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("features"):
            feat = data["features"][0]
            coords = feat["geometry"]["coordinates"]
            return {
                "lat": coords[1],
                "lon": coords[0],
                "score": feat["properties"].get("score", 0),
                "label": feat["properties"].get("label", adresse),
            }
    except requests.RequestException:
        pass
    return None


@st.cache_data(ttl=86400, show_spinner="Géocodage des adresses...")
def geocoder_dataframe(
    df: pd.DataFrame,
    col_adresse: str = "adresse",
    col_cp: str = "code_postal",
    limite: int = 200,
) -> pd.DataFrame:
    """
    Géocode un DataFrame en masse via l'API BAN.
    Ajoute les colonnes lat et lon.

    Args:
        df: DataFrame avec une colonne adresse
        col_adresse: Nom de la colonne adresse
        col_cp: Nom de la colonne code postal
        limite: Nombre max de lignes à géocoder (API BAN = 50 req/s)

    Returns:
        DataFrame avec colonnes lat et lon ajoutées.
    """
    df = df.copy()
    df["lat"] = None
    df["lon"] = None

    for i, (idx, row) in enumerate(df.head(limite).iterrows()):
        if i % 50 == 0:
            st.write(f"Géocodage {i}/{min(limite, len(df))}...")

        adresse = str(row.get(col_adresse, ""))
        cp = str(row.get(col_cp, "")) if col_cp in df.columns else None

        if adresse and adresse != "nan":
            result = geocoder_adresse(adresse, cp)
            if result:
                df.at[idx, "lat"] = result["lat"]
                df.at[idx, "lon"] = result["lon"]

    return df


# ─────────────────────────────────────────────
# UTILITAIRES COMMUNES / DÉPARTEMENTS
# ─────────────────────────────────────────────

def code_commune_vers_departement(code_commune: str) -> str:
    """
    Extrait le code département depuis un code commune INSEE.

    Exemples:
        "75056" → "75"
        "2A004" → "2A"
        "97100" → "971"
    """
    code = str(code_commune).strip()
    if code.startswith("2A") or code.startswith("2B"):
        return code[:2]
    elif code.startswith("97"):
        return code[:3]
    else:
        return code[:2]


def filtrer_par_rayon(
    df: pd.DataFrame,
    lat_centre: float,
    lon_centre: float,
    rayon_km: float,
) -> pd.DataFrame:
    """
    Filtre un DataFrame pour ne garder que les points dans un rayon donné.
    Utilise la formule de Haversine approximée (suffisant pour <100 km).

    Args:
        df: DataFrame avec colonnes lat et lon
        lat_centre: Latitude du centre
        lon_centre: Longitude du centre
        rayon_km: Rayon de recherche en kilomètres

    Returns:
        DataFrame filtré.
    """
    import numpy as np

    df = df.dropna(subset=["lat", "lon"]).copy()

    # Approximation : 1° lat ≈ 111 km, 1° lon ≈ 111 * cos(lat) km
    lat_r = lat_centre * (3.14159 / 180)
    dlat = (df["lat"] - lat_centre) * 111
    dlon = (df["lon"] - lon_centre) * 111 * abs(pd.Series([lat_r]).map(lambda x: __import__("math").cos(x)).iloc[0])

    distance = (dlat ** 2 + dlon ** 2) ** 0.5
    return df[distance <= rayon_km].copy()


def agréger_par_commune(df: pd.DataFrame, col_valeur: str = "prix_m2") -> pd.DataFrame:
    """
    Agrège un DataFrame à l'échelle commune avec statistiques descriptives.

    Args:
        df: DataFrame avec code_commune et col_valeur
        col_valeur: Colonne numérique à agréger

    Returns:
        DataFrame agrégé par commune avec count, median, mean, min, max.
    """
    return (
        df.groupby(["code_commune", "nom_commune"])[col_valeur]
        .agg(["count", "median", "mean", "min", "max"])
        .round(0)
        .reset_index()
        .rename(columns={
            "count": "nb_transactions",
            "median": f"{col_valeur}_median",
            "mean": f"{col_valeur}_moyenne",
            "min": f"{col_valeur}_min",
            "max": f"{col_valeur}_max",
        })
        .sort_values("nb_transactions", ascending=False)
    )
