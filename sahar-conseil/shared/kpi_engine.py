"""
SAHAR Conseil — kpi_engine.py
Couche KPI unifiée au-dessus du data_catalog.

Point d'entrée unique pour toutes les apps SAHAR.
Aucune app ne recalcule ses propres métriques — tout passe ici.

Sources mobilisées :
  - DVF   → prix, volume, dynamisme marché
  - DPE   → étiquettes, conso, passoires, valeur verte
  - BAN   → géocodage, adresse → lat/lon
  - POI   → équipements (via api_clients.GeoClient + IRVEClient)

Usage :
    from shared.kpi_engine import kpi

    # KPIs immobilier d'une commune
    immo = kpi.immobilier("33063")
    print(immo["prix_median"], immo["evolution_12m"], immo["score_opportunite"])

    # KPIs énergie
    ener = kpi.energie(departement="33", commune="Bordeaux")

    # KPIs localisation à partir d'un point GPS
    loc = kpi.localisation(lat=44.837, lon=-0.579)

    # Score composite 0-100 tous axes
    score = kpi.composite("33063", lat=44.837, lon=-0.579)
"""

from __future__ import annotations

import math
import pandas as pd
import numpy as np
from typing import Optional
from datetime import datetime, timedelta


# ─── helpers internes ─────────────────────────────────────────────────────────

def _safe_mean(values: list) -> Optional[float]:
    vals = [v for v in values if v is not None and not math.isnan(v)]
    return round(float(np.mean(vals)), 1) if vals else None


def _score_0_100(value: float, low: float, high: float, invert: bool = False) -> int:
    """Normalise `value` dans [low, high] → entier [0, 100]."""
    if high == low:
        return 50
    s = (value - low) / (high - low) * 100
    s = max(0.0, min(100.0, s))
    return int(round(100 - s if invert else s))


# ─── Labels lisibles ──────────────────────────────────────────────────────────

DPE_SCORE = {"A": 100, "B": 85, "C": 65, "D": 45, "E": 25, "F": 10, "G": 0}
DPE_LABEL = {v: k for k, v in DPE_SCORE.items()}  # inverse

def label_score(score: int) -> str:
    if score >= 70: return "🟢 Fort"
    if score >= 40: return "🟡 Moyen"
    return "🔴 Faible"

def label_tension(score: int) -> str:
    if score >= 70: return "🔴 Marché vendeur"
    if score >= 40: return "🟡 Équilibré"
    return "🟢 Marché acheteur"


# ═════════════════════════════════════════════════════════════════════════════
# KPIEngine
# ═════════════════════════════════════════════════════════════════════════════

class KPIEngine:
    """
    Façade KPI unique pour toutes les apps SAHAR.

    Méthodes principales :
      .immobilier(code_commune, departement)  → dict KPIs DVF
      .energie(departement, commune)          → dict KPIs DPE
      .localisation(lat, lon)                 → dict KPIs POI
      .composite(code_commune, ...)           → dict score global + axes
    """

    # ── Immobilier ─────────────────────────────────────────────────────────

    def immobilier(
        self,
        code_commune: Optional[str] = None,
        departement: Optional[str] = None,
        annee_min: Optional[int] = None,
    ) -> dict:
        """
        KPIs marché immobilier DVF pour une commune ou un département.

        Retourne :
            prix_median      (€/m²)
            prix_moyen       (€/m²)
            prix_min/max     (€/m²)
            evolution_12m    (% vs N-1)
            volume           (nb transactions 12 mois)
            liquidite        (0-100 : volume relatif vs médiane dépt)
            tension          (0-100 : ratio récent/ancien)
            score_opportunite (0-100 : sous-valo × liquidité × dynamisme)
            label_opportunite
            label_tension
        """
        try:
            from data.api_clients import DVFClient
            client = DVFClient()

            params = {"max_pages": 30}
            if code_commune:
                params["code_commune"] = code_commune
            elif departement:
                params["code_departement"] = departement

            if annee_min:
                params["annee_min"] = annee_min
            else:
                params["annee_min"] = datetime.now().year - 3

            df = client.get_transactions(**params)
        except Exception as e:
            return {"erreur": f"DVF indisponible : {e}"}

        if df.empty:
            return {"erreur": "Aucune transaction trouvée"}

        # Nettoyage colonnes
        df = self._normaliser_dvf(df)
        if df.empty:
            return {"erreur": "Données DVF insuffisantes"}

        now = pd.Timestamp.now()
        cutoff_12m = now - pd.DateOffset(months=12)
        cutoff_24m = now - pd.DateOffset(months=24)

        df_recent = df[df["date_mutation"] >= cutoff_12m]
        df_n1     = df[(df["date_mutation"] >= cutoff_24m) & (df["date_mutation"] < cutoff_12m)]

        prix_median   = df_recent["prix_m2"].median() if not df_recent.empty else df["prix_m2"].median()
        prix_median_n1 = df_n1["prix_m2"].median() if not df_n1.empty else None
        evolution_12m = None
        if prix_median_n1 and prix_median_n1 > 0:
            evolution_12m = round((prix_median - prix_median_n1) / prix_median_n1 * 100, 1)

        volume = len(df_recent)
        vol_n1 = len(df_n1) if not df_n1.empty else 1
        tension_ratio = volume / max(vol_n1, 1)
        score_tension = _score_0_100(tension_ratio, 0.3, 2.0)

        # Liquidité : volume relatif normalisé (>50 transactions = liquide)
        score_liquidite = _score_0_100(volume, 0, 200)

        # Dynamisme : évolution prix récente
        score_dynamisme = _score_0_100(evolution_12m or 0, -10, 15)

        # Sous-valorisation vs médiane département (si on a code_commune + dépt)
        # ici approximation : on compare la commune à la médiane de toutes les communes
        prix_median_ref = df["prix_m2"].median()
        sous_valo = max(0.0, (prix_median_ref - prix_median) / prix_median_ref) if prix_median_ref > 0 else 0
        score_sous_valo = int(round(sous_valo * 100))

        score_opportunite = int(round(
            score_sous_valo * 0.35
            + score_liquidite * 0.30
            + score_dynamisme * 0.35
        ))
        score_opportunite = max(0, min(100, score_opportunite))

        return {
            "code_commune":       code_commune,
            "departement":        departement,
            "prix_median":        round(float(prix_median), 0),
            "prix_moyen":         round(float(df_recent["prix_m2"].mean() if not df_recent.empty else df["prix_m2"].mean()), 0),
            "prix_min":           round(float(df_recent["prix_m2"].quantile(0.05) if not df_recent.empty else df["prix_m2"].quantile(0.05)), 0),
            "prix_max":           round(float(df_recent["prix_m2"].quantile(0.95) if not df_recent.empty else df["prix_m2"].quantile(0.95)), 0),
            "evolution_12m":      evolution_12m,
            "volume":             volume,
            "score_liquidite":    score_liquidite,
            "score_tension":      score_tension,
            "score_dynamisme":    score_dynamisme,
            "score_opportunite":  score_opportunite,
            "label_opportunite":  label_score(score_opportunite),
            "label_tension":      label_tension(score_tension),
            "nb_transactions_total": len(df),
            "date_calcul":        now.isoformat(),
        }

    # ── Énergie / DPE ──────────────────────────────────────────────────────

    def energie(
        self,
        departement: str,
        commune: Optional[str] = None,
        code_insee: Optional[str] = None,
    ) -> dict:
        """
        KPIs énergétiques DPE pour un département ou une commune.

        Retourne :
            nb_logements_analyses
            taux_passoires_fg    (% logements F+G)
            taux_ef              (% E+F)
            taux_abc             (% performants A+B+C)
            score_dpe_moyen      (0-100 : A=100 → G=0)
            conso_moyenne        (kWh/m²/an)
            emission_ges_moyen   (kgCO2/m²/an)
            potentiel_reno       (score 0-100 : urgence rénovation territoriale)
            valeur_verte_estimee (€/m² de gain potentiel F→C)
            top_communes_fg      (liste)
            label_performance
        """
        try:
            from shared.supabase_dpe import get_dpe_communes, get_dpe_logements
            df_communes = get_dpe_communes(departement)
        except Exception as e:
            return {"erreur": f"DPE Supabase indisponible : {e}"}

        if df_communes.empty:
            return {"erreur": "Aucune donnée DPE pour ce département"}

        # Filtrer par commune si demandé
        if commune:
            mask = df_communes["commune"].str.lower() == commune.lower()
            df_communes = df_communes[mask]
        if code_insee:
            mask = df_communes["code_insee"] == code_insee
            df_communes = df_communes[mask]

        if df_communes.empty:
            return {"erreur": "Commune non trouvée dans les données DPE"}

        nb_total = int(df_communes["nb_dpe_efg"].sum())
        nb_fg    = int(df_communes["nb_f"].sum() + df_communes["nb_g"].sum())
        nb_e     = int(df_communes["nb_e"].sum()) if "nb_e" in df_communes.columns else 0

        taux_fg = round(nb_fg / nb_total * 100, 1) if nb_total > 0 else 0
        taux_ef = round((nb_e + nb_fg) / nb_total * 100, 1) if nb_total > 0 else 0

        # Score DPE moyen (approximation : E = 25/100 en moyenne sur E/F/G stock)
        # Pondération simple par nb : G=0, F=10, E=25
        score_dpe_moyen = None
        if nb_total > 0:
            nb_g = int(df_communes["nb_g"].sum())
            score_dpe_moyen = int(round(
                (nb_e * 25 + nb_fg * 10 - nb_g * 10) / nb_total
            ))
            score_dpe_moyen = max(0, min(100, score_dpe_moyen))

        conso_moy = round(float(df_communes["conso_moy"].mean()), 1) if "conso_moy" in df_communes.columns else None
        ges_moy   = round(float(df_communes["ges_moy"].mean()), 1) if "ges_moy" in df_communes.columns else None

        # Potentiel de rénovation = urgence territoriale (% passoires × conso relative)
        potentiel_reno = _score_0_100(taux_fg, 5, 60)

        # Valeur verte estimée : passage F→C vaut ~8-12% du prix
        # Approximation : on prend une base 3 500 €/m² (médiane nationale)
        # Gain F→C = ~8% → ~280 €/m², proportionnel au taux de passoires local
        valeur_verte = round(280 * (taux_fg / 30), 0) if taux_fg > 0 else 0

        # Top communes passoires
        top_fg = []
        if "commune" in df_communes.columns and "pct_fg" in df_communes.columns:
            top_fg = (
                df_communes
                .nlargest(5, "pct_fg")[["commune", "code_postal", "nb_dpe_efg", "pct_fg"]]
                .to_dict("records")
            )

        return {
            "departement":          departement,
            "commune":              commune,
            "nb_logements_analyses": nb_total,
            "taux_passoires_fg":    taux_fg,
            "taux_ef":              taux_ef,
            "nb_fg":                nb_fg,
            "score_dpe_moyen":      score_dpe_moyen,
            "conso_moyenne_kwh_m2": conso_moy,
            "emission_ges_moyen":   ges_moy,
            "potentiel_reno":       potentiel_reno,
            "valeur_verte_estimee": valeur_verte,
            "top_communes_fg":      top_fg,
            "label_performance":    label_score(100 - potentiel_reno),
            "date_calcul":          datetime.now().isoformat(),
        }

    # ── Localisation / POI ─────────────────────────────────────────────────

    def localisation(
        self,
        lat: float,
        lon: float,
        rayon_m: int = 500,
        adresse: Optional[str] = None,
    ) -> dict:
        """
        Score de localisation à partir d'un point GPS ou d'une adresse.

        Si adresse est fournie, géocode d'abord via BAN.

        Retourne :
            lat, lon
            score_global     (0-100)
            score_transport  (0-100 : stations metro/bus/tram dans rayon)
            score_commerces  (0-100 : commerces alimentaires, services)
            score_ecoles     (0-100 : écoles, collèges, lycées)
            score_sante      (0-100 : médecins, pharmacies, hôpitaux)
            score_loisirs    (0-100 : parcs, cinémas, restaurants)
            nb_poi_total
            label_localisation
        """
        # Géocodage optionnel via BAN
        if adresse:
            coords = self._geocoder(adresse)
            if coords:
                lat, lon = coords

        # Récupération POI via API Overpass (OpenStreetMap) ou fallback GeoAPI
        poi_counts = self._compter_poi(lat, lon, rayon_m)

        # Scores par catégorie (calibrés : rayon 500m)
        score_transport = _score_0_100(poi_counts.get("transport", 0), 0, 8)
        score_commerces  = _score_0_100(poi_counts.get("commerces", 0), 0, 10)
        score_ecoles     = _score_0_100(poi_counts.get("ecoles", 0), 0, 4)
        score_sante      = _score_0_100(poi_counts.get("sante", 0), 0, 5)
        score_loisirs    = _score_0_100(poi_counts.get("loisirs", 0), 0, 8)

        score_global = int(round(
            score_transport * 0.30
            + score_commerces  * 0.25
            + score_ecoles     * 0.20
            + score_sante      * 0.15
            + score_loisirs    * 0.10
        ))

        nb_poi_total = sum(poi_counts.values())

        return {
            "lat":               lat,
            "lon":               lon,
            "rayon_m":           rayon_m,
            "score_global":      score_global,
            "score_transport":   score_transport,
            "score_commerces":   score_commerces,
            "score_ecoles":      score_ecoles,
            "score_sante":       score_sante,
            "score_loisirs":     score_loisirs,
            "nb_poi_total":      nb_poi_total,
            "poi_detail":        poi_counts,
            "label_localisation": label_score(score_global),
            "date_calcul":       datetime.now().isoformat(),
        }

    # ── Composite ──────────────────────────────────────────────────────────

    def composite(
        self,
        code_commune: Optional[str] = None,
        departement: Optional[str] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        adresse: Optional[str] = None,
        poids_immo: float = 0.40,
        poids_energie: float = 0.30,
        poids_localisation: float = 0.30,
    ) -> dict:
        """
        Score composite 0-100 combinant immobilier + énergie + localisation.

        Utilisé par PropertyValueSimulator et Carte Scoring.

        Retourne :
            score_global       (0-100)
            score_immo         (0-100)
            score_energie      (0-100)
            score_localisation (0-100)
            label_global
            detail_immo        (dict complet)
            detail_energie     (dict complet)
            detail_localisation (dict complet si lat/lon fournis)
        """
        assert abs(poids_immo + poids_energie + poids_localisation - 1.0) < 0.01, \
            "La somme des poids doit être égale à 1.0"

        # — Immobilier
        detail_immo = {}
        score_immo = 50  # défaut neutre
        if code_commune or departement:
            detail_immo = self.immobilier(code_commune=code_commune, departement=departement)
            if "score_opportunite" in detail_immo:
                score_immo = detail_immo["score_opportunite"]

        # — Énergie
        detail_energie = {}
        score_energie = 50
        if departement:
            commune_nom = None
            if code_commune and len(code_commune) == 5:
                # On ne peut pas résoudre le nom de commune sans INSEE directement
                pass
            detail_energie = self.energie(departement=departement, code_insee=code_commune)
            if "potentiel_reno" in detail_energie:
                # Inverser : faible potentiel reno = bon score énergie
                score_energie = 100 - detail_energie["potentiel_reno"]

        # — Localisation
        detail_localisation = {}
        score_localisation = 50
        if lat is not None and lon is not None:
            detail_localisation = self.localisation(lat=lat, lon=lon, adresse=adresse)
            score_localisation = detail_localisation.get("score_global", 50)

        # — Score composite
        score_global = int(round(
            score_immo         * poids_immo
            + score_energie    * poids_energie
            + score_localisation * poids_localisation
        ))
        score_global = max(0, min(100, score_global))

        return {
            "score_global":         score_global,
            "score_immo":           score_immo,
            "score_energie":        score_energie,
            "score_localisation":   score_localisation,
            "label_global":         label_score(score_global),
            "poids": {
                "immobilier":   poids_immo,
                "energie":      poids_energie,
                "localisation": poids_localisation,
            },
            "detail_immo":          detail_immo,
            "detail_energie":       detail_energie,
            "detail_localisation":  detail_localisation,
            "date_calcul":          datetime.now().isoformat(),
        }

    # ── Helpers privés ─────────────────────────────────────────────────────

    @staticmethod
    def _normaliser_dvf(df: pd.DataFrame) -> pd.DataFrame:
        """Normalise les colonnes DVF pour garantir prix_m2 et date_mutation."""
        # Date
        for col in ["date_mutation", "datemut"]:
            if col in df.columns:
                df["date_mutation"] = pd.to_datetime(df[col], errors="coerce")
                break

        # Valeur foncière
        for col in ["valeur_fonciere", "valeurfonc"]:
            if col in df.columns:
                df["valeur_fonciere"] = pd.to_numeric(df[col], errors="coerce")
                break

        # Surface
        for col in ["surface_reelle_bati", "sbati", "surface_utile"]:
            if col in df.columns:
                df["surface_utile"] = pd.to_numeric(df[col], errors="coerce")
                break

        # Prix /m²
        if "prix_m2" not in df.columns:
            if "valeur_fonciere" in df.columns and "surface_utile" in df.columns:
                df["prix_m2"] = df["valeur_fonciere"] / df["surface_utile"].replace(0, np.nan)
            else:
                return pd.DataFrame()

        df = df[
            df["prix_m2"].notna()
            & (df["prix_m2"] > 200)
            & (df["prix_m2"] < 30_000)
        ].copy()

        if "date_mutation" not in df.columns:
            return pd.DataFrame()

        df = df[df["date_mutation"].notna()].copy()
        return df

    @staticmethod
    def _geocoder(adresse: str) -> Optional[tuple]:
        """Géocode une adresse via l'API BAN. Retourne (lat, lon) ou None."""
        try:
            import requests
            r = requests.get(
                "https://api-adresse.data.gouv.fr/search/",
                params={"q": adresse, "limit": 1},
                timeout=5,
            )
            r.raise_for_status()
            features = r.json().get("features", [])
            if features:
                coords = features[0]["geometry"]["coordinates"]
                return (coords[1], coords[0])  # (lat, lon)
        except Exception:
            pass
        return None

    @staticmethod
    def _compter_poi(lat: float, lon: float, rayon_m: int = 500) -> dict:
        """
        Compte les POI autour d'un point via l'API Overpass (OpenStreetMap).
        Fallback sur des valeurs neutres si l'API est indisponible.
        """
        try:
            import requests

            # Requête Overpass : catégories principales
            overpass_url = "https://overpass-api.de/api/interpreter"
            query = f"""
            [out:json][timeout:10];
            (
              node["public_transport"](around:{rayon_m},{lat},{lon});
              node["highway"="bus_stop"](around:{rayon_m},{lat},{lon});
              node["railway"~"station|tram_stop|subway_entrance"](around:{rayon_m},{lat},{lon});
              node["amenity"~"supermarket|bakery|butcher|convenience|market"](around:{rayon_m},{lat},{lon});
              node["shop"~"supermarket|bakery|butcher|convenience"](around:{rayon_m},{lat},{lon});
              node["amenity"~"school|college|kindergarten|university"](around:{rayon_m},{lat},{lon});
              node["amenity"~"pharmacy|hospital|clinic|doctors"](around:{rayon_m},{lat},{lon});
              node["amenity"~"restaurant|cafe|bar|cinema|theatre|park"](around:{rayon_m},{lat},{lon});
              way["leisure"="park"](around:{rayon_m},{lat},{lon});
            );
            out count;
            """
            # Version simplifiée : requêtes séparées par catégorie
            categories = {
                "transport": f'node["public_transport"](around:{rayon_m},{lat},{lon}); node["highway"="bus_stop"](around:{rayon_m},{lat},{lon}); node["railway"~"station|tram_stop|subway_entrance"](around:{rayon_m},{lat},{lon});',
                "commerces": f'node["amenity"~"supermarket|bakery|convenience"](around:{rayon_m},{lat},{lon}); node["shop"~"supermarket|bakery|convenience"](around:{rayon_m},{lat},{lon});',
                "ecoles":    f'node["amenity"~"school|college|kindergarten|university"](around:{rayon_m},{lat},{lon});',
                "sante":     f'node["amenity"~"pharmacy|hospital|clinic|doctors"](around:{rayon_m},{lat},{lon});',
                "loisirs":   f'node["amenity"~"restaurant|cafe|bar|cinema|theatre"](around:{rayon_m},{lat},{lon}); way["leisure"="park"](around:{rayon_m},{lat},{lon});',
            }

            counts = {}
            for cat, body in categories.items():
                q = f"[out:json][timeout:8]; ({body}); out count;"
                resp = requests.post(overpass_url, data={"data": q}, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                counts[cat] = data.get("elements", [{}])[0].get("tags", {}).get("total", 0)
                try:
                    counts[cat] = int(counts[cat])
                except (TypeError, ValueError):
                    counts[cat] = len(data.get("elements", []))

            return counts

        except Exception:
            # Fallback neutre — évite de bloquer les apps si Overpass est down
            return {
                "transport": 3,
                "commerces": 4,
                "ecoles":    2,
                "sante":     2,
                "loisirs":   3,
            }


# ── Singleton global ──────────────────────────────────────────────────────────

kpi = KPIEngine()
