"""
SAHAR Conseil — scoring.py
Moteur de scoring 0–100 pour chaque secteur.
Chaque fonction retourne une pd.Series de scores entiers entre 0 et 100.
"""

import pandas as pd
import numpy as np
from typing import Optional


def _normaliser_0_100(serie: pd.Series) -> pd.Series:
    """Normalise une série numérique entre 0 et 100."""
    min_val = serie.min()
    max_val = serie.max()
    if max_val == min_val:
        return pd.Series(50, index=serie.index)
    return ((serie - min_val) / (max_val - min_val) * 100).round(0).astype(int)


# ─────────────────────────────────────────────
# IMMOBILIER — Scoring opportunités DVF
# ─────────────────────────────────────────────

def score_opportunite_immo(
    df: pd.DataFrame,
    poids_prix: float = 0.40,
    poids_volume: float = 0.30,
    poids_dynamisme: float = 0.30,
    mois_dynamisme: int = 12,
) -> pd.Series:
    """
    Calcule un score d'opportunité immobilière 0–100 pour chaque transaction.

    Critères :
    - Sous-valorisation vs médiane IRIS (40%) : bien moins cher que la médiane = opportunité
    - Volume de transactions (30%) : zone liquide = moins risquée
    - Dynamisme prix 12 mois (30%) : zone en croissance récente

    Args:
        df: DataFrame DVF nettoyé (doit contenir prix_m2, code_commune, date_mutation)
        poids_prix: Poids de la sous-valorisation (défaut 40%)
        poids_volume: Poids du volume de transactions (défaut 30%)
        poids_dynamisme: Poids du dynamisme récent (défaut 30%)
        mois_dynamisme: Nombre de mois pour définir "récent" (défaut 12)

    Returns:
        pd.Series d'entiers 0–100 alignée sur l'index de df.

    Interprétation :
        > 70 : Opportunité forte
        40–70 : Zone à surveiller
        < 40 : Marché tendu ou mature
    """
    assert abs(poids_prix + poids_volume + poids_dynamisme - 1.0) < 0.01, \
        "La somme des poids doit être égale à 1.0"

    scores = pd.DataFrame(index=df.index)

    # 1. Sous-valorisation par rapport à la médiane communale
    mediane = df.groupby("code_commune")["prix_m2"].transform("median")
    ecart = (mediane - df["prix_m2"]) / mediane.replace(0, np.nan)
    scores["s_prix"] = ecart.clip(0, None).fillna(0)

    # 2. Volume de transactions par commune (liquidité marché)
    volume = df.groupby("code_commune")["prix_m2"].transform("count")
    scores["s_volume"] = volume

    # 3. Dynamisme : part de transactions dans les N derniers mois
    cutoff = pd.Timestamp.now() - pd.DateOffset(months=mois_dynamisme)
    recent = (df["date_mutation"] >= cutoff).astype(float)
    scores["s_dynamisme"] = recent

    # Normaliser chaque composante entre 0 et 100
    for col in scores.columns:
        scores[col] = _normaliser_0_100(scores[col])

    # Score composite pondéré
    score_final = (
        scores["s_prix"] * poids_prix
        + scores["s_volume"] * poids_volume
        + scores["s_dynamisme"] * poids_dynamisme
    ).round(0).astype(int)

    return score_final.clip(0, 100)


def label_score(score: int) -> str:
    """Retourne un label lisible pour un score."""
    if score >= 70:
        return "🟢 Opportunité forte"
    elif score >= 40:
        return "🟡 À surveiller"
    else:
        return "🔴 Marché tendu"


# ─────────────────────────────────────────────
# ÉNERGIE — Scoring priorité rénovation DPE
# ─────────────────────────────────────────────

def score_priorite_renovation(df: pd.DataFrame) -> pd.Series:
    """
    Score de priorité de rénovation 0–100 pour chaque logement DPE.

    Critères :
    - Étiquette DPE (F=60, G=100, E=40, D=20, C=5, B=2, A=0)
    - Consommation énergétique finale normalisée (30%)
    - Ancienneté du bâtiment (bâtiment ancien = plus urgent) (20%)

    Args:
        df: DataFrame DPE avec colonnes etiquette_dpe, conso_5_usages_e_finale,
            annee_construction

    Returns:
        pd.Series de scores 0–100.
    """
    scores = pd.DataFrame(index=df.index)

    # 1. Score par étiquette DPE
    mapping_dpe = {"G": 100, "F": 75, "E": 50, "D": 30, "C": 15, "B": 5, "A": 0}
    scores["s_etiquette"] = df["etiquette_dpe"].map(mapping_dpe).fillna(30)

    # 2. Consommation énergétique (kWh/m²/an) — plus élevée = plus urgent
    if "conso_5_usages_e_finale" in df.columns:
        conso = pd.to_numeric(df["conso_5_usages_e_finale"], errors="coerce").fillna(0)
        scores["s_conso"] = _normaliser_0_100(conso.clip(0, 800))
    else:
        scores["s_conso"] = 50

    # 3. Ancienneté (bâtiments construits avant 1975 = isolation quasi-nulle)
    if "annee_construction" in df.columns:
        annee = pd.to_numeric(df["annee_construction"], errors="coerce").fillna(1990)
        anciennete = (2024 - annee).clip(0, 100)
        scores["s_anciennete"] = _normaliser_0_100(anciennete)
    else:
        scores["s_anciennete"] = 50

    score_final = (
        scores["s_etiquette"] * 0.50
        + scores["s_conso"] * 0.30
        + scores["s_anciennete"] * 0.20
    ).round(0).astype(int)

    return score_final.clip(0, 100)


# ─────────────────────────────────────────────
# RETAIL — Scoring attractivité zone commerciale
# ─────────────────────────────────────────────

def score_attractivite_zone(
    df_commune: pd.DataFrame,
    poids_equipements: float = 0.40,
    poids_population: float = 0.35,
    poids_concurrence: float = 0.25,
) -> pd.Series:
    """
    Score d'attractivité commerciale 0–100 pour chaque commune.

    Critères :
    - Densité d'équipements BPE (40%) : écoles, commerces, services
    - Population et revenus (35%) : bassin de consommation
    - Niveau de concurrence SIRENE (25%) : inversé, moins = mieux

    Args:
        df_commune: DataFrame avec code_commune, nb_equipements,
                    population, nb_concurrents

    Returns:
        pd.Series de scores 0–100.
    """
    scores = pd.DataFrame(index=df_commune.index)

    if "nb_equipements" in df_commune.columns:
        scores["s_equip"] = _normaliser_0_100(
            pd.to_numeric(df_commune["nb_equipements"], errors="coerce").fillna(0)
        )
    else:
        scores["s_equip"] = 50

    if "population" in df_commune.columns:
        scores["s_pop"] = _normaliser_0_100(
            pd.to_numeric(df_commune["population"], errors="coerce").fillna(0)
        )
    else:
        scores["s_pop"] = 50

    # Concurrence : inversée — moins de concurrents = score plus élevé
    if "nb_concurrents" in df_commune.columns:
        conc = pd.to_numeric(df_commune["nb_concurrents"], errors="coerce").fillna(0)
        scores["s_conc"] = 100 - _normaliser_0_100(conc)
    else:
        scores["s_conc"] = 50

    score_final = (
        scores["s_equip"] * poids_equipements
        + scores["s_pop"] * poids_population
        + scores["s_conc"] * poids_concurrence
    ).round(0).astype(int)

    return score_final.clip(0, 100)


# ─────────────────────────────────────────────
# RH — Scoring tension recrutement
# ─────────────────────────────────────────────

def score_tension_rh(df: pd.DataFrame) -> pd.Series:
    """
    Score de tension de recrutement 0–100 par bassin d'emploi et métier.
    100 = très difficile à recruter (opportunité pour cabinet RH).

    Args:
        df: DataFrame DARES avec colonnes indicateur_tension, taux_chomage,
            offres_emploi, demandes_emploi

    Returns:
        pd.Series de scores 0–100.
    """
    scores = pd.DataFrame(index=df.index)

    if "indicateur_tension" in df.columns:
        tension = pd.to_numeric(df["indicateur_tension"], errors="coerce").fillna(1.0)
        scores["s_tension"] = _normaliser_0_100(tension)
    else:
        scores["s_tension"] = 50

    if "taux_chomage" in df.columns:
        # Faible chômage = tension élevée
        chomage = pd.to_numeric(df["taux_chomage"], errors="coerce").fillna(7.0)
        scores["s_chomage"] = 100 - _normaliser_0_100(chomage)
    else:
        scores["s_chomage"] = 50

    score_final = (
        scores["s_tension"] * 0.70
        + scores["s_chomage"] * 0.30
    ).round(0).astype(int)

    return score_final.clip(0, 100)
