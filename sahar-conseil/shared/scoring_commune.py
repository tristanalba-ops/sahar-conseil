"""
SAHAR Conseil — Scoring tension marché par commune
Calcule 5 indicateurs par commune à partir des données DVF filtrées.
"""

import pandas as pd
import numpy as np


def compute_score_commune(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule un score de tension marché par commune (0–100).

    Indicateurs :
      - prix_median     : prix médian €/m²
      - evolution_12m   : évolution prix médian sur 12 mois (%)
      - volume          : nombre de transactions (12 derniers mois)
      - tension         : ratio transactions récentes / anciennes
      - signal          : indicateur acheteur (bas) / vendeur (haut)

    Retourne un DataFrame indexé par nom_commune.
    """
    now = pd.Timestamp.now()
    periode_recente = now - pd.DateOffset(months=12)
    periode_ancienne_debut = now - pd.DateOffset(months=24)
    periode_ancienne_fin = now - pd.DateOffset(months=12)

    df_recent = df[df["date_mutation"] >= periode_recente]
    df_ancien = df[
        (df["date_mutation"] >= periode_ancienne_debut) &
        (df["date_mutation"] < periode_ancienne_fin)
    ]

    # ── Agrégations de base ──────────────────────────────────────────────
    agg_recent = df_recent.groupby("nom_commune").agg(
        prix_median=("prix_m2", "median"),
        volume_recent=("prix_m2", "count"),
        prix_moyen=("prix_m2", "mean"),
        prix_min=("prix_m2", "min"),
        prix_max=("prix_m2", "max"),
        surface_med=("surface_utile", "median"),
        valeur_med=("valeur_fonciere", "median"),
    ).reset_index()

    agg_ancien = df_ancien.groupby("nom_commune").agg(
        prix_median_N1=("prix_m2", "median"),
        volume_ancien=("prix_m2", "count"),
    ).reset_index()

    # ── Fusion ───────────────────────────────────────────────────────────
    result = agg_recent.merge(agg_ancien, on="nom_commune", how="left")

    # ── Évolution prix 12 mois ───────────────────────────────────────────
    result["evolution_12m"] = (
        (result["prix_median"] - result["prix_median_N1"])
        / result["prix_median_N1"].replace(0, np.nan)
        * 100
    ).round(1)

    # ── Tension marché (ratio récent/ancien) ────────────────────────────
    result["tension"] = (
        result["volume_recent"] / result["volume_ancien"].replace(0, np.nan)
    ).round(2)

    # ── Signal acheteur / vendeur ────────────────────────────────────────
    # Basé sur : évolution prix + tension combinés
    result["signal_score"] = (
        result["evolution_12m"].fillna(0).clip(-20, 20) / 20 * 50 +
        result["tension"].fillna(1).clip(0.5, 2).map(lambda x: (x - 0.5) / 1.5 * 50)
    ).round(0).clip(0, 100)

    result["signal"] = result["signal_score"].apply(
        lambda x: "🔴 Marché vendeur (prix hauts)" if x >= 65
        else ("🟡 Marché équilibré" if x >= 40
              else "🟢 Opportunité acheteur")
    )

    # ── Normalisation scores (0–100) ─────────────────────────────────────
    def norm(s, inverse=False):
        mn, mx = s.min(), s.max()
        if mx == mn:
            return pd.Series(50, index=s.index)
        n = (s - mn) / (mx - mn) * 100
        return (100 - n) if inverse else n

    # Score = combo volume (40%) + évolution (30%) + tension (30%)
    s_volume    = norm(result["volume_recent"])
    s_evolution = norm(result["evolution_12m"].fillna(0))
    s_tension   = norm(result["tension"].fillna(1))

    result["score_marche"] = (
        s_volume * 0.40 + s_evolution * 0.30 + s_tension * 0.30
    ).round(0).clip(0, 100).astype(int)

    # ── Tri et nettoyage ─────────────────────────────────────────────────
    result = result[result["volume_recent"] >= 5].copy()
    result = result.sort_values("score_marche", ascending=False).reset_index(drop=True)

    result = result.rename(columns={
        "nom_commune":       "Commune",
        "prix_median":       "Prix médian €/m²",
        "prix_moyen":        "Prix moyen €/m²",
        "prix_min":          "Prix min €/m²",
        "prix_max":          "Prix max €/m²",
        "surface_med":       "Surface médiane m²",
        "valeur_med":        "Prix médian total €",
        "volume_recent":     "Transactions 12m",
        "prix_median_N1":    "Prix médian N-1 €/m²",
        "volume_ancien":     "Transactions N-1",
        "evolution_12m":     "Évolution 12m (%)",
        "tension":           "Ratio tension",
        "signal":            "Signal marché",
        "score_marche":      "Score marché",
    })

    cols_order = [
        "Commune", "Score marché", "Signal marché",
        "Prix médian €/m²", "Évolution 12m (%)",
        "Transactions 12m", "Transactions N-1", "Ratio tension",
        "Surface médiane m²", "Prix médian total €",
        "Prix moyen €/m²", "Prix min €/m²", "Prix max €/m²",
        "Prix médian N-1 €/m²",
    ]
    return result[[c for c in cols_order if c in result.columns]]
