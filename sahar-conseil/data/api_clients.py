"""
SAHAR Conseil — api_clients.py
Connecteurs API unifiés pour toutes les sources open data.

Sources couvertes :
  - DVF (api-dvf.datagouvfr.fr) — transactions immobilières
  - DPE ADEME (data.ademe.fr) — diagnostics énergétiques
  - INSEE / SIRENE (api.insee.fr) — entreprises + données locales
  - BAN (api-adresse.data.gouv.fr) — géocodage adresses
  - Geo API (geo.api.gouv.fr) — communes, départements, EPCI
  - Data.gouv.fr — datasets génériques

Usage :
    from data.api_clients import DVFClient, DPEClient, SIRENEClient, BANClient, GeoClient

    dvf = DVFClient()
    df = dvf.get_transactions(code_commune="33063", annee_min=2022)

    sirene = SIRENEClient(token="votre_token_insee")
    df = sirene.search(activite="6820A", departement="33")
"""

import requests
import pandas as pd
import time
import json
from pathlib import Path
from typing import Optional, List
from datetime import datetime

# ── CACHE LOCAL ──────────────────────────────────────────────────────────────

CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(source: str, key: str) -> Path:
    return CACHE_DIR / f"{source}_{key}.parquet"


def _cache_get(source: str, key: str, ttl_hours: int = 24) -> Optional[pd.DataFrame]:
    """Retourne le DataFrame caché s'il existe et n'est pas expiré."""
    p = _cache_path(source, key)
    if p.exists():
        age_h = (time.time() - p.stat().st_mtime) / 3600
        if age_h < ttl_hours:
            try:
                return pd.read_parquet(p)
            except Exception:
                pass
    return None


def _cache_set(source: str, key: str, df: pd.DataFrame):
    """Sauvegarde le DataFrame en cache Parquet."""
    if not df.empty:
        try:
            df.to_parquet(_cache_path(source, key), index=False, compression="snappy")
        except Exception:
            pass


def _request(url: str, params: dict = None, headers: dict = None,
             retries: int = 3, delay: float = 1.0, timeout: int = 60) -> Optional[dict]:
    """GET avec retry et gestion d'erreurs."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 429:  # Rate limit
                wait = float(r.headers.get("Retry-After", delay * attempt * 2))
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.Timeout:
            if attempt < retries:
                time.sleep(delay * attempt)
        except requests.RequestException as e:
            if attempt < retries:
                time.sleep(delay * attempt)
            else:
                print(f"[API] Erreur après {retries} essais : {e}")
    return None


# ═════════════════════════════════════════════════════════════════════════════
# DVF — Transactions immobilières
# ═════════════════════════════════════════════════════════════════════════════

class DVFClient:
    """
    API DVF officielle — api-dvf.datagouvfr.fr
    Données de valeurs foncières (ventes immobilières).
    Limites : 1000 résultats par requête, pagination par curseur.
    """

    BASE = "https://apidf-preprod.cerema.fr/dvf_opendata/mutations"

    def get_transactions(
        self,
        code_commune: str = None,
        code_departement: str = None,
        annee_min: int = None,
        annee_max: int = None,
        type_local: str = None,
        valeur_min: float = None,
        valeur_max: float = None,
        page_size: int = 500,
        max_pages: int = 20,
    ) -> pd.DataFrame:
        """
        Récupère les transactions DVF via l'API.

        Args:
            code_commune: Code INSEE (ex: "33063" pour Bordeaux)
            code_departement: Code département (ex: "33")
            annee_min/max: Filtrer par année de mutation
            type_local: "Appartement" ou "Maison"
            valeur_min/max: Filtrer par valeur foncière
            page_size: Nb résultats par page (max 500)
            max_pages: Limite de pages à récupérer

        Returns:
            DataFrame des transactions.
        """
        cache_key = f"dvf_{code_commune or code_departement}_{annee_min}_{annee_max}_{type_local}"
        cached = _cache_get("dvf", cache_key, ttl_hours=72)
        if cached is not None:
            return cached

        params = {"page_size": min(page_size, 500)}
        if code_commune:
            params["code_commune"] = code_commune
        if code_departement:
            params["code_departement"] = code_departement
        if annee_min:
            params["anneemut_min"] = annee_min
        if annee_max:
            params["anneemut_max"] = annee_max
        if type_local:
            params["codtypbien"] = {"Appartement": "2", "Maison": "1"}.get(type_local, "")
        if valeur_min:
            params["valeurmin"] = valeur_min
        if valeur_max:
            params["valeurmax"] = valeur_max

        all_rows = []
        page = 1

        while page <= max_pages:
            params["page"] = page
            data = _request(self.BASE, params=params)
            if not data:
                break

            results = data.get("results", [])
            if not results:
                break

            all_rows.extend(results)
            print(f"\r[DVF API] Page {page} — {len(all_rows)} transactions", end="", flush=True)

            if not data.get("next"):
                break
            page += 1
            time.sleep(0.3)

        print()

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)

        # Nettoyage standard
        if "datemut" in df.columns:
            df["date_mutation"] = pd.to_datetime(df["datemut"], errors="coerce")
        if "valeurfonc" in df.columns:
            df.rename(columns={"valeurfonc": "valeur_fonciere"}, inplace=True)
        if "sbati" in df.columns:
            df.rename(columns={"sbati": "surface_reelle_bati"}, inplace=True)

        _cache_set("dvf", cache_key, df)
        return df

    def get_communes_stats(self, code_departement: str, annee: int = None) -> pd.DataFrame:
        """Statistiques agrégées par commune pour un département."""
        cache_key = f"dvf_stats_{code_departement}_{annee}"
        cached = _cache_get("dvf", cache_key, ttl_hours=168)  # 1 semaine
        if cached is not None:
            return cached

        df = self.get_transactions(
            code_departement=code_departement,
            annee_min=annee or datetime.now().year - 2,
            max_pages=50,
        )
        if df.empty:
            return pd.DataFrame()

        # Agrégation
        if "l_codinsee" in df.columns:
            df["code_commune"] = df["l_codinsee"].apply(
                lambda x: x[0] if isinstance(x, list) and x else ""
            )
        stats = df.groupby("code_commune").agg(
            nb_transactions=("valeur_fonciere", "count"),
            prix_median=("valeur_fonciere", "median"),
            prix_moyen=("valeur_fonciere", "mean"),
        ).reset_index()

        _cache_set("dvf", cache_key, stats)
        return stats


# ═════════════════════════════════════════════════════════════════════════════
# DPE — Diagnostics de Performance Énergétique
# ═════════════════════════════════════════════════════════════════════════════

class DPEClient:
    """
    API ADEME — data.ademe.fr
    Diagnostics de performance énergétique des logements.
    """

    URLS = [
        "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines",
        "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines",
    ]

    SELECT = (
        "numero_dpe,date_etablissement_dpe,etiquette_dpe,etiquette_ges,"
        "conso_5_usages_e_finale,emission_ges_5_usages,"
        "adresse_ban,code_postal_ban,nom_commune_ban,"
        "latitude,longitude,type_batiment,annee_construction,"
        "surface_habitable_logement,type_energie_principale_chauffage"
    )

    def get_logements(
        self,
        code_departement: str,
        etiquettes: List[str] = None,
        max_results: int = 10000,
    ) -> pd.DataFrame:
        """
        Récupère les DPE d'un département.

        Args:
            code_departement: Code département (ex: "33")
            etiquettes: Filtrer par étiquettes DPE (ex: ["F", "G"])
            max_results: Nombre max de résultats

        Returns:
            DataFrame des DPE.
        """
        cache_key = f"dpe_{code_departement}_{'_'.join(etiquettes or ['all'])}"
        cached = _cache_get("dpe", cache_key, ttl_hours=168)
        if cached is not None:
            return cached

        all_rows = []
        after = None

        for url in self.URLS:
            try:
                while len(all_rows) < max_results:
                    params = {
                        "q": code_departement,
                        "q_fields": "code_departement_insee",
                        "size": 1000,
                        "select": self.SELECT,
                    }
                    if after:
                        params["after"] = after

                    data = _request(url, params=params)
                    if not data:
                        break

                    results = data.get("results", [])
                    if not results:
                        break

                    all_rows.extend(results)
                    print(f"\r[DPE API] {len(all_rows)} logements", end="", flush=True)

                    after = data.get("next")
                    if not after:
                        break
                    time.sleep(0.2)

                if all_rows:
                    break
            except Exception:
                continue

        print()

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)

        # Typage
        if "date_etablissement_dpe" in df.columns:
            df["date_etablissement_dpe"] = pd.to_datetime(
                df["date_etablissement_dpe"], errors="coerce"
            )
        for col in ["conso_5_usages_e_finale", "emission_ges_5_usages",
                     "surface_habitable_logement", "latitude", "longitude"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "annee_construction" in df.columns:
            df["annee_construction"] = pd.to_numeric(
                df["annee_construction"], errors="coerce"
            ).astype("Int64")

        # Filtrer étiquettes si demandé
        if etiquettes and "etiquette_dpe" in df.columns:
            df = df[df["etiquette_dpe"].isin(etiquettes)].copy()

        # Score urgence rénovation
        def _score(row):
            s = 0
            etiq = row.get("etiquette_dpe", "")
            if etiq == "G":
                s += 50
            elif etiq == "F":
                s += 35
            elif etiq == "E":
                s += 20
            conso = row.get("conso_5_usages_e_finale", 0) or 0
            if conso > 450:
                s += 30
            elif conso > 330:
                s += 20
            elif conso > 250:
                s += 10
            annee = row.get("annee_construction", 0) or 0
            if annee and annee < 1975:
                s += 20
            elif annee and annee < 1990:
                s += 10
            return min(100, s)

        df["score_urgence"] = df.apply(_score, axis=1)

        _cache_set("dpe", cache_key, df)
        return df

    def get_passoires(self, code_departement: str, max_results: int = 10000) -> pd.DataFrame:
        """Raccourci pour obtenir les logements F et G uniquement."""
        return self.get_logements(code_departement, etiquettes=["F", "G"], max_results=max_results)


# ═════════════════════════════════════════════════════════════════════════════
# SIRENE — Entreprises (INSEE)
# ═════════════════════════════════════════════════════════════════════════════

class SIRENEClient:
    """
    API SIRENE INSEE — api.insee.fr
    Répertoire des entreprises et établissements.
    Nécessite un token INSEE (gratuit sur api.insee.fr).
    """

    BASE = "https://api.insee.fr/entreprises/sirene/V3.11"

    def __init__(self, token: str = None):
        """
        Args:
            token: Bearer token INSEE. Obtenir sur https://api.insee.fr
                   (créer une application, activer API SIRENE)
        """
        self.token = token
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}

    def search(
        self,
        activite: str = None,
        departement: str = None,
        commune: str = None,
        tranche_effectif: str = None,
        nombre: int = 1000,
        curseur: str = "*",
    ) -> pd.DataFrame:
        """
        Recherche d'établissements SIRENE.

        Args:
            activite: Code APE/NAF (ex: "6820A" pour agences immobilières,
                      "4511Z" pour concessionnaires auto)
            departement: Code département
            commune: Code commune INSEE
            tranche_effectif: Filtre effectif (ex: "11" pour 10-19 salariés)
            nombre: Nb résultats max
            curseur: Curseur pagination (défaut "*" = début)

        Returns:
            DataFrame des établissements.

        Codes APE utiles pour SAHAR :
            6820A - Agences immobilières
            6820B - Location de terrains
            4511Z - Commerce voitures
            4519Z - Commerce autres véhicules
            8690A - Ambulances
            4399C - Travaux couverture
            4391B - Travaux charpente
            4322A - Plomberie chauffage
            4321A - Travaux installation électrique
            4329A - Travaux isolation
            5610A - Restauration
            4711B - Commerce alimentaire
        """
        if not self.token:
            print("[SIRENE] Token INSEE requis. Obtenir sur https://api.insee.fr")
            return pd.DataFrame()

        cache_key = f"sirene_{activite}_{departement}_{commune}"
        cached = _cache_get("sirene", cache_key, ttl_hours=168)
        if cached is not None:
            return cached

        # Construction du filtre
        criteres = []
        if activite:
            criteres.append(f'activitePrincipaleEtablissement:"{activite}"')
        if departement:
            criteres.append(f'codePostalEtablissement:"{departement}*"')
        if commune:
            criteres.append(f'codeCommuneEtablissement:"{commune}"')
        if tranche_effectif:
            criteres.append(f'trancheEffectifsEtablissement:"{tranche_effectif}"')

        # Seulement les établissements actifs
        criteres.append('etatAdministratifEtablissement:"A"')

        q = " AND ".join(criteres)

        all_rows = []
        pages = 0
        max_pages = (nombre // 100) + 1

        while pages < max_pages:
            params = {"q": q, "nombre": min(100, nombre), "curseur": curseur}
            data = _request(
                f"{self.BASE}/siret",
                params=params,
                headers=self.headers,
            )
            if not data:
                break

            etabs = data.get("etablissements", [])
            if not etabs:
                break

            for e in etabs:
                row = {
                    "siret": e.get("siret", ""),
                    "siren": e.get("siren", ""),
                    "denomination": (
                        e.get("uniteLegale", {}).get("denominationUniteLegale", "")
                        or e.get("uniteLegale", {}).get("nomUniteLegale", "")
                    ),
                    "ape": e.get("uniteLegale", {}).get("activitePrincipaleUniteLegale", ""),
                    "effectif": e.get("trancheEffectifsEtablissement", ""),
                    "date_creation": e.get("dateCreationEtablissement", ""),
                    "adresse_numero": e.get("adresseEtablissement", {}).get("numeroVoieEtablissement", ""),
                    "adresse_voie": e.get("adresseEtablissement", {}).get("libelleVoieEtablissement", ""),
                    "code_postal": e.get("adresseEtablissement", {}).get("codePostalEtablissement", ""),
                    "commune": e.get("adresseEtablissement", {}).get("libelleCommuneEtablissement", ""),
                    "code_commune": e.get("adresseEtablissement", {}).get("codeCommuneEtablissement", ""),
                }
                all_rows.append(row)

            print(f"\r[SIRENE] {len(all_rows)} établissements", end="", flush=True)

            curseur = data.get("header", {}).get("curseurSuivant", "")
            if not curseur or len(all_rows) >= nombre:
                break
            pages += 1
            time.sleep(0.5)

        print()

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        _cache_set("sirene", cache_key, df)
        return df

    def agences_immo(self, departement: str) -> pd.DataFrame:
        """Raccourci : agences immobilières d'un département."""
        return self.search(activite="6820A", departement=departement)

    def artisans_renovation(self, departement: str) -> pd.DataFrame:
        """Raccourci : artisans rénovation (plomberie, isolation, électricité)."""
        codes = ["4322A", "4329A", "4321A", "4391B", "4399C"]
        frames = []
        for code in codes:
            df = self.search(activite=code, departement=departement, nombre=500)
            if not df.empty:
                frames.append(df)
            time.sleep(1)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def concessionnaires_auto(self, departement: str) -> pd.DataFrame:
        """Raccourci : concessionnaires automobiles."""
        return self.search(activite="4511Z", departement=departement)


# ═════════════════════════════════════════════════════════════════════════════
# BAN — Base Adresse Nationale (Géocodage)
# ═════════════════════════════════════════════════════════════════════════════

class BANClient:
    """
    API BAN — api-adresse.data.gouv.fr
    Géocodage et reverse-géocodage d'adresses françaises.
    Gratuit, sans token, limité à ~50 req/s.
    """

    BASE = "https://api-adresse.data.gouv.fr"

    def geocode(self, adresse: str, code_postal: str = None, limit: int = 1) -> dict:
        """
        Géocode une adresse en coordonnées lat/lon.

        Returns:
            {"lat": float, "lon": float, "label": str, "score": float} ou {}
        """
        params = {"q": adresse, "limit": limit}
        if code_postal:
            params["postcode"] = code_postal

        data = _request(f"{self.BASE}/search/", params=params, retries=2)
        if not data or not data.get("features"):
            return {}

        f = data["features"][0]
        return {
            "lat": f["geometry"]["coordinates"][1],
            "lon": f["geometry"]["coordinates"][0],
            "label": f["properties"].get("label", ""),
            "score": f["properties"].get("score", 0),
            "code_commune": f["properties"].get("citycode", ""),
        }

    def geocode_batch(self, addresses: List[str], code_postaux: List[str] = None) -> pd.DataFrame:
        """
        Géocode une liste d'adresses (séquentiellement, ~50 req/s max).
        Pour du vrai batch (>1000), utiliser l'endpoint /search/csv/ avec upload CSV.

        Returns:
            DataFrame avec colonnes: adresse, lat, lon, label, score, code_commune
        """
        results = []
        for i, addr in enumerate(addresses):
            cp = code_postaux[i] if code_postaux and i < len(code_postaux) else None
            r = self.geocode(addr, cp)
            r["adresse_input"] = addr
            results.append(r)
            if (i + 1) % 50 == 0:
                print(f"\r[BAN] {i+1}/{len(addresses)} géocodées", end="", flush=True)
                time.sleep(1)  # respect rate limit

        print()
        return pd.DataFrame(results)

    def reverse(self, lat: float, lon: float) -> dict:
        """Reverse géocode : coordonnées → adresse."""
        data = _request(f"{self.BASE}/reverse/", params={"lat": lat, "lon": lon})
        if not data or not data.get("features"):
            return {}
        f = data["features"][0]
        return {
            "label": f["properties"].get("label", ""),
            "code_commune": f["properties"].get("citycode", ""),
            "commune": f["properties"].get("city", ""),
            "code_postal": f["properties"].get("postcode", ""),
        }


# ═════════════════════════════════════════════════════════════════════════════
# GEO API — Communes, départements, EPCI
# ═════════════════════════════════════════════════════════════════════════════

class GeoClient:
    """
    API Geo — geo.api.gouv.fr
    Référentiel géographique officiel.
    Gratuit, sans token.
    """

    BASE = "https://geo.api.gouv.fr"

    def get_communes(self, code_departement: str, fields: str = None) -> pd.DataFrame:
        """
        Liste les communes d'un département.

        Args:
            code_departement: Ex: "33"
            fields: Champs à retourner (ex: "nom,code,population,surface,centre")
        """
        cache_key = f"communes_{code_departement}"
        cached = _cache_get("geo", cache_key, ttl_hours=720)  # 30 jours
        if cached is not None:
            return cached

        params = {}
        if fields:
            params["fields"] = fields
        else:
            params["fields"] = "nom,code,codesPostaux,population,surface,centre,departement"

        data = _request(f"{self.BASE}/departements/{code_departement}/communes", params=params)
        if not data:
            return pd.DataFrame()

        rows = []
        for c in data:
            row = {
                "code_commune": c.get("code", ""),
                "nom_commune": c.get("nom", ""),
                "population": c.get("population", 0),
                "surface_ha": c.get("surface", 0) / 100 if c.get("surface") else 0,
                "codes_postaux": ",".join(c.get("codesPostaux", [])),
            }
            centre = c.get("centre", {}).get("coordinates", [])
            if centre:
                row["longitude"] = centre[0]
                row["latitude"] = centre[1]
            rows.append(row)

        df = pd.DataFrame(rows)
        _cache_set("geo", cache_key, df)
        return df

    def get_departements(self) -> pd.DataFrame:
        """Liste tous les départements français."""
        data = _request(f"{self.BASE}/departements", params={"fields": "nom,code,codeRegion"})
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)

    def search_commune(self, nom: str, code_departement: str = None) -> dict:
        """Recherche une commune par nom."""
        params = {"nom": nom, "limit": 1}
        if code_departement:
            params["codeDepartement"] = code_departement
        data = _request(f"{self.BASE}/communes", params=params)
        if data and len(data) > 0:
            return data[0]
        return {}


# ═════════════════════════════════════════════════════════════════════════════
# DATA.GOUV.FR — Datasets génériques
# ═════════════════════════════════════════════════════════════════════════════

class DataGouvClient:
    """
    API data.gouv.fr — accès aux datasets publics.
    Utile pour IRVE (bornes de recharge), mobilités, etc.
    """

    BASE = "https://www.data.gouv.fr/api/1"

    def search_datasets(self, query: str, page_size: int = 10) -> list:
        """Recherche des datasets par mots-clés."""
        data = _request(f"{self.BASE}/datasets/", params={"q": query, "page_size": page_size})
        if not data:
            return []
        return [
            {
                "id": d["id"],
                "title": d["title"],
                "description": d.get("description", "")[:200],
                "organization": d.get("organization", {}).get("name", ""),
                "last_update": d.get("last_update", ""),
                "nb_resources": len(d.get("resources", [])),
                "url": d.get("page", ""),
            }
            for d in data.get("data", [])
        ]

    def get_resource_url(self, dataset_id: str, format_pref: str = "csv") -> Optional[str]:
        """Retourne l'URL de la première ressource au format demandé."""
        data = _request(f"{self.BASE}/datasets/{dataset_id}/")
        if not data:
            return None
        for r in data.get("resources", []):
            if format_pref in r.get("format", "").lower():
                return r.get("url", "")
        # Fallback : première ressource
        resources = data.get("resources", [])
        return resources[0]["url"] if resources else None

    def download_csv(self, dataset_id: str, **read_csv_kwargs) -> pd.DataFrame:
        """Télécharge un dataset CSV directement en DataFrame."""
        url = self.get_resource_url(dataset_id, "csv")
        if not url:
            return pd.DataFrame()
        try:
            return pd.read_csv(url, **read_csv_kwargs)
        except Exception as e:
            print(f"[data.gouv] Erreur téléchargement : {e}")
            return pd.DataFrame()


# ═════════════════════════════════════════════════════════════════════════════
# IRVE — Bornes de recharge véhicules électriques
# ═════════════════════════════════════════════════════════════════════════════

class IRVEClient:
    """
    Données IRVE (Infrastructures de Recharge Véhicules Électriques).
    Source : data.gouv.fr — consolidation transport.data.gouv.fr
    """

    DATASET_ID = "64fb1c23bc1e4a7e24c50033"

    def get_bornes(self, departement: str = None) -> pd.DataFrame:
        """
        Récupère les bornes IRVE, optionnellement filtrées par département.
        """
        cache_key = f"irve_{departement or 'all'}"
        cached = _cache_get("irve", cache_key, ttl_hours=168)
        if cached is not None:
            return cached

        dg = DataGouvClient()
        url = dg.get_resource_url(self.DATASET_ID, "csv")
        if not url:
            print("[IRVE] Dataset introuvable")
            return pd.DataFrame()

        try:
            df = pd.read_csv(url, low_memory=False, sep=",")
        except Exception as e:
            print(f"[IRVE] Erreur : {e}")
            return pd.DataFrame()

        # Filtrage département
        if departement and "code_commune_INSEE" in df.columns:
            df = df[df["code_commune_INSEE"].astype(str).str.startswith(departement)].copy()

        _cache_set("irve", cache_key, df)
        return df


# ═════════════════════════════════════════════════════════════════════════════
# RACCOURCIS
# ═════════════════════════════════════════════════════════════════════════════

def liste_sources() -> dict:
    """Retourne la liste de toutes les sources disponibles avec description."""
    return {
        "dvf": {
            "nom": "DVF — Demandes de Valeurs Foncières",
            "source": "api-dvf.datagouvfr.fr",
            "description": "Transactions immobilières (ventes) sur tout le territoire",
            "token_requis": False,
            "client": "DVFClient",
            "secteurs": ["immobilier"],
        },
        "dpe": {
            "nom": "DPE — Diagnostics de Performance Énergétique",
            "source": "data.ademe.fr",
            "description": "Diagnostics énergétiques des logements (étiquettes A–G)",
            "token_requis": False,
            "client": "DPEClient",
            "secteurs": ["énergie", "immobilier"],
        },
        "sirene": {
            "nom": "SIRENE — Répertoire des entreprises",
            "source": "api.insee.fr",
            "description": "Établissements actifs : agences immo, artisans, commerce...",
            "token_requis": True,
            "client": "SIRENEClient",
            "secteurs": ["immobilier", "retail", "auto", "énergie", "rh"],
        },
        "ban": {
            "nom": "BAN — Base Adresse Nationale",
            "source": "api-adresse.data.gouv.fr",
            "description": "Géocodage et reverse-géocodage d'adresses",
            "token_requis": False,
            "client": "BANClient",
            "secteurs": ["tous"],
        },
        "geo": {
            "nom": "Geo API — Référentiel géographique",
            "source": "geo.api.gouv.fr",
            "description": "Communes, départements, EPCI, population, surface",
            "token_requis": False,
            "client": "GeoClient",
            "secteurs": ["tous"],
        },
        "irve": {
            "nom": "IRVE — Bornes de recharge VE",
            "source": "data.gouv.fr",
            "description": "Localisation des bornes de recharge véhicules électriques",
            "token_requis": False,
            "client": "IRVEClient",
            "secteurs": ["auto", "énergie"],
        },
        "data_gouv": {
            "nom": "data.gouv.fr — Datasets génériques",
            "source": "data.gouv.fr",
            "description": "Accès à tous les datasets publics français",
            "token_requis": False,
            "client": "DataGouvClient",
            "secteurs": ["tous"],
        },
    }
