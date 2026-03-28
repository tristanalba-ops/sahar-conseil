"""
SAHAR Conseil — api_connectors.py
Connecteurs vers toutes les API gratuites exploitables.

Chaque connecteur est une classe avec des méthodes simples.
Toutes les API sont gratuites et sans clé (sauf INSEE qui nécessite OAuth2).

Usage :
    from shared.api_connectors import (
        GeoAPI, BanAPI, DvfAPI, DpeAPI, SireneAPI,
        CadastreAPI, UrbanismeAPI, AnnuaireSanteAPI,
        PappersAPI, GoogleTrendsLocal, DataGouvAPI
    )
"""

import requests
import pandas as pd
from typing import Optional, List, Dict
from functools import lru_cache
import time


# ── 1. API Géo (communes, départements, régions) ────────────────────────────

class GeoAPI:
    """
    API Géo — geo.api.gouv.fr
    Gratuite, sans clé, sans limite raisonnable.
    Données : communes, départements, régions, codes postaux.
    """
    BASE = "https://geo.api.gouv.fr"

    @staticmethod
    def communes(departement: str, fields: str = "nom,code,codesPostaux,population,centre") -> pd.DataFrame:
        """Liste des communes d'un département."""
        r = requests.get(f"{GeoAPI.BASE}/departements/{departement}/communes",
                         params={"fields": fields}, timeout=15)
        r.raise_for_status()
        return pd.DataFrame(r.json())

    @staticmethod
    def search_commune(nom: str, limit: int = 10) -> pd.DataFrame:
        """Recherche de commune par nom."""
        r = requests.get(f"{GeoAPI.BASE}/communes",
                         params={"nom": nom, "limit": limit, "fields": "nom,code,departement,population"}, timeout=10)
        r.raise_for_status()
        return pd.DataFrame(r.json())

    @staticmethod
    def departements() -> pd.DataFrame:
        """Liste de tous les départements."""
        r = requests.get(f"{GeoAPI.BASE}/departements", timeout=10)
        r.raise_for_status()
        return pd.DataFrame(r.json())

    @staticmethod
    def regions() -> pd.DataFrame:
        """Liste de toutes les régions."""
        r = requests.get(f"{GeoAPI.BASE}/regions", timeout=10)
        r.raise_for_status()
        return pd.DataFrame(r.json())

    @staticmethod
    def commune_by_code(code_insee: str) -> dict:
        """Détail d'une commune par code INSEE."""
        r = requests.get(f"{GeoAPI.BASE}/communes/{code_insee}",
                         params={"fields": "nom,code,codesPostaux,population,surface,centre,contour"}, timeout=10)
        r.raise_for_status()
        return r.json()


# ── 2. API Adresse / BAN (géocodage) ────────────────────────────────────────

class BanAPI:
    """
    Base Adresse Nationale — api-adresse.data.gouv.fr
    Gratuite, 50 req/s par IP.
    Géocodage, reverse geocoding, autocomplétion.
    """
    BASE = "https://api-adresse.data.gouv.fr"

    @staticmethod
    def geocode(address: str, limit: int = 5) -> pd.DataFrame:
        """Géocode une adresse → lat/lon."""
        r = requests.get(f"{BanAPI.BASE}/search",
                         params={"q": address, "limit": limit}, timeout=10)
        r.raise_for_status()
        features = r.json().get("features", [])
        rows = []
        for f in features:
            props = f["properties"]
            coords = f["geometry"]["coordinates"]
            rows.append({
                "label": props.get("label"),
                "score": props.get("score"),
                "housenumber": props.get("housenumber"),
                "street": props.get("street"),
                "postcode": props.get("postcode"),
                "city": props.get("city"),
                "citycode": props.get("citycode"),
                "longitude": coords[0],
                "latitude": coords[1],
            })
        return pd.DataFrame(rows)

    @staticmethod
    def reverse(lat: float, lon: float) -> dict:
        """Reverse geocoding : coordonnées → adresse."""
        r = requests.get(f"{BanAPI.BASE}/reverse",
                         params={"lat": lat, "lon": lon}, timeout=10)
        r.raise_for_status()
        features = r.json().get("features", [])
        if features:
            return features[0]["properties"]
        return {}

    @staticmethod
    def geocode_csv(df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """Géocodage en masse via CSV (POST)."""
        import io
        csv_data = df.to_csv(index=False)
        r = requests.post(f"{BanAPI.BASE}/search/csv",
                          files={"data": ("data.csv", csv_data)},
                          data={"columns": columns}, timeout=60)
        r.raise_for_status()
        return pd.read_csv(io.StringIO(r.text))


# ── 3. API DVF (Demandes de Valeurs Foncières) ──────────────────────────────

class DvfAPI:
    """
    API DVF — api.cquest.org/dvf ou data.gouv.fr
    Gratuite, sans clé.
    Transactions immobilières depuis 2014.
    """
    BASE = "https://apidf-preprod.cerema.fr/dvf_opendata"

    @staticmethod
    def mutations(code_commune: str, annee_min: int = 2020, limit: int = 500) -> pd.DataFrame:
        """Transactions immobilières d'une commune."""
        # API DVF du CEREMA
        params = {
            "code_commune": code_commune,
            "annee_mutation_min": annee_min,
            "page_size": min(limit, 500),
        }
        r = requests.get(f"{DvfAPI.BASE}/mutations/",
                         params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return pd.DataFrame(data.get("results", []))

    @staticmethod
    def mutations_geo(lat: float, lon: float, dist: int = 500) -> pd.DataFrame:
        """Transactions autour d'un point GPS (rayon en mètres)."""
        params = {
            "lat": lat, "lon": lon, "dist_max": dist,
            "page_size": 100,
        }
        r = requests.get(f"{DvfAPI.BASE}/mutations/",
                         params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return pd.DataFrame(data.get("results", []))

    @staticmethod
    def prix_median_commune(code_commune: str) -> dict:
        """Prix médian au m² d'une commune (calcul depuis mutations)."""
        df = DvfAPI.mutations(code_commune, annee_min=2022)
        if df.empty:
            return {}
        if "valeur_fonciere" in df.columns and "surface_reelle_bati" in df.columns:
            df = df[df["surface_reelle_bati"] > 0].copy()
            df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
            return {
                "nb_transactions": len(df),
                "prix_m2_median": round(df["prix_m2"].median(), 0),
                "prix_m2_moyen": round(df["prix_m2"].mean(), 0),
            }
        return {}


# ── 4. API DPE (ADEME) ──────────────────────────────────────────────────────

class DpeAPI:
    """
    API DPE — data.ademe.fr
    Gratuite, sans clé. Rate limit modéré.
    Diagnostics de Performance Énergétique.
    """
    BASE = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants"

    @staticmethod
    def logements(code_postal: str, etiquettes: Optional[List[str]] = None, limit: int = 100) -> pd.DataFrame:
        """DPE par code postal."""
        qs = f'N°_département:"{code_postal[:2]}" AND Code_postal_(BAN):"{code_postal}"'
        if etiquettes:
            etiq_filter = " OR ".join([f'Etiquette_DPE:"{e}"' for e in etiquettes])
            qs += f" AND ({etiq_filter})"
        params = {"qs": qs, "size": limit, "select": (
            "N°DPE,Etiquette_DPE,Etiquette_GES,"
            "Code_postal_(BAN),Commune_(BAN),Adresse_(BAN),"
            "Conso_5_usages_é_finale,Surface_habitable_logement,"
            "Année_construction,Date_réception_DPE"
        )}
        r = requests.get(f"{DpeAPI.BASE}/lines", params=params, timeout=30)
        r.raise_for_status()
        return pd.DataFrame(r.json().get("results", []))


# ── 5. API SIRENE (INSEE) ───────────────────────────────────────────────────

class SireneAPI:
    """
    API SIRENE — api.insee.fr
    Gratuite avec inscription (OAuth2). 30 req/min.
    25M entreprises, 36M établissements.
    """
    BASE = "https://api.insee.fr/api-sirene/3.11"
    TOKEN_URL = "https://auth.insee.net/auth/realms/apim/protocol/openid-connect/token"

    def __init__(self, client_id: str = "", client_secret: str = ""):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = None
        self._token_expires = 0

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expires:
            return self._token
        r = requests.post(self.TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }, timeout=10)
        r.raise_for_status()
        data = r.json()
        self._token = data["access_token"]
        self._token_expires = time.time() + data.get("expires_in", 3600) - 60
        return self._token

    def search(self, query: str, limit: int = 20) -> pd.DataFrame:
        """Recherche d'entreprises par nom/SIREN/SIRET."""
        headers = {"Authorization": f"Bearer {self._get_token()}"}
        params = {"q": query, "nombre": limit}
        r = requests.get(f"{self.BASE}/siret", headers=headers, params=params, timeout=15)
        r.raise_for_status()
        return pd.DataFrame(r.json().get("etablissements", []))


# ── 6. API Cadastre ──────────────────────────────────────────────────────────

class CadastreAPI:
    """
    API Carto Cadastre — apicarto.ign.fr
    Gratuite, sans clé.
    Parcelles cadastrales, sections, communes.
    """
    BASE = "https://apicarto.ign.fr/api/cadastre"

    @staticmethod
    def parcelles(code_insee: str, section: Optional[str] = None) -> list:
        """Parcelles cadastrales d'une commune."""
        params = {"code_insee": code_insee}
        if section:
            params["section"] = section
        r = requests.get(f"{CadastreAPI.BASE}/parcelle",
                         params=params, timeout=30)
        r.raise_for_status()
        return r.json().get("features", [])

    @staticmethod
    def parcelle_by_ref(code_insee: str, section: str, numero: str) -> dict:
        """Détail d'une parcelle par référence."""
        params = {"code_insee": code_insee, "section": section, "numero": numero}
        r = requests.get(f"{CadastreAPI.BASE}/parcelle", params=params, timeout=15)
        r.raise_for_status()
        features = r.json().get("features", [])
        return features[0] if features else {}


# ── 7. API Géoportail Urbanisme ──────────────────────────────────────────────

class UrbanismeAPI:
    """
    API Géoportail de l'Urbanisme — apicarto.ign.fr
    Gratuite, sans clé.
    PLU, zones urbaines, règlements.
    """
    BASE = "https://apicarto.ign.fr/api/gpu"

    @staticmethod
    def zone_urba(lat: float, lon: float) -> list:
        """Zone d'urbanisme à un point GPS."""
        geom = {"type": "Point", "coordinates": [lon, lat]}
        params = {"geom": str(geom).replace("'", '"')}
        r = requests.get(f"{UrbanismeAPI.BASE}/zone-urba",
                         params=params, timeout=15)
        r.raise_for_status()
        return r.json().get("features", [])

    @staticmethod
    def document(code_insee: str) -> list:
        """Documents d'urbanisme d'une commune."""
        r = requests.get(f"{UrbanismeAPI.BASE}/document",
                         params={"partition": f"DU_{code_insee}"}, timeout=15)
        r.raise_for_status()
        return r.json().get("features", [])


# ── 8. API Annuaire Santé (FHIR) ────────────────────────────────────────────

class AnnuaireSanteAPI:
    """
    Annuaire Santé FHIR — gateway.api.esante.gouv.fr
    Gratuite, sans clé.
    Professionnels et structures de santé.
    """
    BASE = "https://gateway.api.esante.gouv.fr/fhir/v1"

    @staticmethod
    def search_practitioners(city: str, specialty: str = "", limit: int = 50) -> pd.DataFrame:
        """Recherche de professionnels de santé."""
        params = {"_count": limit, "address-city": city}
        if specialty:
            params["specialty"] = specialty
        r = requests.get(f"{AnnuaireSanteAPI.BASE}/Practitioner",
                         params=params, timeout=20,
                         headers={"Accept": "application/json"})
        r.raise_for_status()
        entries = r.json().get("entry", [])
        rows = []
        for e in entries:
            res = e.get("resource", {})
            name = res.get("name", [{}])[0]
            rows.append({
                "id": res.get("id"),
                "nom": name.get("family", ""),
                "prenom": " ".join(name.get("given", [])),
                "active": res.get("active"),
            })
        return pd.DataFrame(rows)

    @staticmethod
    def search_organizations(city: str, type_code: str = "", limit: int = 50) -> pd.DataFrame:
        """Recherche de structures de santé (hôpitaux, cliniques, etc.)."""
        params = {"_count": limit, "address-city": city}
        if type_code:
            params["type"] = type_code
        r = requests.get(f"{AnnuaireSanteAPI.BASE}/Organization",
                         params=params, timeout=20,
                         headers={"Accept": "application/json"})
        r.raise_for_status()
        entries = r.json().get("entry", [])
        rows = []
        for e in entries:
            res = e.get("resource", {})
            rows.append({
                "id": res.get("id"),
                "nom": res.get("name", ""),
                "active": res.get("active"),
            })
        return pd.DataFrame(rows)


# ── 9. Pappers (freemium) ───────────────────────────────────────────────────

class PappersAPI:
    """
    Pappers — api.pappers.fr
    10 000 consultations/mois gratuites. Clé API requise.
    Données légales, financières, dirigeants.
    """
    BASE = "https://api.pappers.fr/v2"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    def search(self, query: str, limit: int = 10) -> pd.DataFrame:
        """Recherche d'entreprises."""
        params = {"api_token": self.api_key, "q": query, "par_page": limit}
        r = requests.get(f"{self.BASE}/recherche", params=params, timeout=15)
        r.raise_for_status()
        return pd.DataFrame(r.json().get("resultats", []))

    def entreprise(self, siren: str) -> dict:
        """Fiche entreprise complète."""
        params = {"api_token": self.api_key, "siren": siren}
        r = requests.get(f"{self.BASE}/entreprise", params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    def dirigeants(self, siren: str) -> list:
        """Liste des dirigeants."""
        data = self.entreprise(siren)
        return data.get("dirigeants", [])


# ── 10. Google Trends (local via pytrends) ───────────────────────────────────

class GoogleTrendsLocal:
    """
    Google Trends via pytrends (pseudo-API locale).
    Gratuit, sans clé. Rate limit souple.
    pip install pytrends
    """

    @staticmethod
    def trending_searches(country: str = "france") -> pd.DataFrame:
        """Tendances du jour."""
        try:
            from pytrends.request import TrendReq
            pytrends = TrendReq(hl="fr-FR", tz=60)
            return pytrends.trending_searches(pn=country)
        except ImportError:
            return pd.DataFrame({"error": ["pip install pytrends"]})

    @staticmethod
    def interest_over_time(keywords: List[str], timeframe: str = "today 3-m", geo: str = "FR") -> pd.DataFrame:
        """Évolution de l'intérêt dans le temps."""
        try:
            from pytrends.request import TrendReq
            pytrends = TrendReq(hl="fr-FR", tz=60)
            pytrends.build_payload(keywords, cat=0, timeframe=timeframe, geo=geo)
            return pytrends.interest_over_time()
        except ImportError:
            return pd.DataFrame({"error": ["pip install pytrends"]})

    @staticmethod
    def related_queries(keyword: str, geo: str = "FR") -> dict:
        """Requêtes associées à un mot-clé."""
        try:
            from pytrends.request import TrendReq
            pytrends = TrendReq(hl="fr-FR", tz=60)
            pytrends.build_payload([keyword], geo=geo, timeframe="today 3-m")
            return pytrends.related_queries()
        except ImportError:
            return {"error": "pip install pytrends"}


# ── 11. data.gouv.fr (catalogue) ────────────────────────────────────────────

class DataGouvAPI:
    """
    API data.gouv.fr — catalogue des données ouvertes.
    Gratuite, sans clé.
    Recherche et téléchargement de datasets.
    """
    BASE = "https://www.data.gouv.fr/api/1"

    @staticmethod
    def search_datasets(query: str, limit: int = 10) -> pd.DataFrame:
        """Recherche de jeux de données."""
        r = requests.get(f"{DataGouvAPI.BASE}/datasets/",
                         params={"q": query, "page_size": limit}, timeout=15)
        r.raise_for_status()
        results = r.json().get("data", [])
        rows = []
        for ds in results:
            rows.append({
                "id": ds.get("id"),
                "title": ds.get("title"),
                "organization": ds.get("organization", {}).get("name", ""),
                "description": (ds.get("description") or "")[:200],
                "last_update": ds.get("last_update"),
                "nb_resources": len(ds.get("resources", [])),
                "url": ds.get("page"),
            })
        return pd.DataFrame(rows)

    @staticmethod
    def download_resource(dataset_id: str, resource_idx: int = 0) -> str:
        """Retourne l'URL de téléchargement d'une ressource."""
        r = requests.get(f"{DataGouvAPI.BASE}/datasets/{dataset_id}/", timeout=10)
        r.raise_for_status()
        resources = r.json().get("resources", [])
        if resource_idx < len(resources):
            return resources[resource_idx].get("url", "")
        return ""


# ── 12. API Entreprise (BODACC / RNCS) ──────────────────────────────────────

class BodaccAPI:
    """
    BODACC — bodacc-datadila.opendatasoft.com
    Gratuite, sans clé.
    Bulletin Officiel des Annonces Civiles et Commerciales.
    Créations, modifications, radiations, procédures collectives.
    """
    BASE = "https://bodacc-datadila.opendatasoft.com/api/records/1.0/search"

    @staticmethod
    def search(query: str = "", departement: str = "", type_annonce: str = "", limit: int = 50) -> pd.DataFrame:
        """Recherche d'annonces BODACC."""
        params = {"rows": limit, "dataset": "annonces-commerciales"}
        filters = []
        if query:
            params["q"] = query
        if departement:
            filters.append(f'departement_code:"{departement}"')
        if type_annonce:
            filters.append(f'fampicolle_lib:"{type_annonce}"')
        if filters:
            params["refine"] = filters
        r = requests.get(BodaccAPI.BASE, params=params, timeout=15)
        r.raise_for_status()
        records = r.json().get("records", [])
        return pd.DataFrame([rec.get("fields", {}) for rec in records])


# ── REGISTRE DES CONNECTEURS ─────────────────────────────────────────────────

CONNECTORS = {
    "geo": {
        "name": "API Géo",
        "class": "GeoAPI",
        "url": "https://geo.api.gouv.fr",
        "auth": "Aucune",
        "rate_limit": "Illimité (raisonnable)",
        "data": "Communes, départements, régions, codes postaux, populations",
        "sectors": ["all"],
    },
    "ban": {
        "name": "API Adresse (BAN)",
        "class": "BanAPI",
        "url": "https://api-adresse.data.gouv.fr",
        "auth": "Aucune",
        "rate_limit": "50 req/s par IP",
        "data": "Géocodage, reverse geocoding, autocomplétion adresses",
        "sectors": ["all"],
    },
    "dvf": {
        "name": "API DVF (CEREMA)",
        "class": "DvfAPI",
        "url": "https://apidf-preprod.cerema.fr",
        "auth": "Aucune",
        "rate_limit": "Modéré",
        "data": "Transactions immobilières depuis 2014, prix, surfaces",
        "sectors": ["immobilier"],
    },
    "dpe": {
        "name": "API DPE (ADEME)",
        "class": "DpeAPI",
        "url": "https://data.ademe.fr",
        "auth": "Aucune",
        "rate_limit": "Modéré",
        "data": "Diagnostics énergétiques, étiquettes, consommations",
        "sectors": ["immobilier", "energie"],
    },
    "sirene": {
        "name": "API SIRENE (INSEE)",
        "class": "SireneAPI",
        "url": "https://api.insee.fr",
        "auth": "OAuth2 (inscription gratuite)",
        "rate_limit": "30 req/min",
        "data": "25M entreprises, 36M établissements, NAF, effectifs",
        "sectors": ["all"],
    },
    "cadastre": {
        "name": "API Cadastre (IGN)",
        "class": "CadastreAPI",
        "url": "https://apicarto.ign.fr",
        "auth": "Aucune",
        "rate_limit": "Modéré",
        "data": "Parcelles cadastrales, sections, géométries",
        "sectors": ["immobilier"],
    },
    "urbanisme": {
        "name": "API Urbanisme (GPU)",
        "class": "UrbanismeAPI",
        "url": "https://apicarto.ign.fr/api/gpu",
        "auth": "Aucune",
        "rate_limit": "Modéré",
        "data": "PLU, zones urbaines, règlements, servitudes",
        "sectors": ["immobilier"],
    },
    "sante": {
        "name": "Annuaire Santé (FHIR)",
        "class": "AnnuaireSanteAPI",
        "url": "https://gateway.api.esante.gouv.fr",
        "auth": "Aucune",
        "rate_limit": "Modéré",
        "data": "Professionnels de santé, structures, spécialités",
        "sectors": ["sante"],
    },
    "pappers": {
        "name": "Pappers",
        "class": "PappersAPI",
        "url": "https://api.pappers.fr",
        "auth": "Clé API (10K req/mois gratuit)",
        "rate_limit": "10 000/mois",
        "data": "Données légales, financières, dirigeants, bilans",
        "sectors": ["all"],
    },
    "trends": {
        "name": "Google Trends (pytrends)",
        "class": "GoogleTrendsLocal",
        "url": "https://trends.google.com",
        "auth": "Aucune (scraping local)",
        "rate_limit": "Souple (~10 req/min)",
        "data": "Tendances, intérêt temporel, requêtes associées",
        "sectors": ["all"],
    },
    "datagouv": {
        "name": "data.gouv.fr",
        "class": "DataGouvAPI",
        "url": "https://www.data.gouv.fr",
        "auth": "Aucune",
        "rate_limit": "Illimité (raisonnable)",
        "data": "Catalogue national des données ouvertes (90K+ datasets)",
        "sectors": ["all"],
    },
    "bodacc": {
        "name": "BODACC",
        "class": "BodaccAPI",
        "url": "https://bodacc-datadila.opendatasoft.com",
        "auth": "Aucune",
        "rate_limit": "Modéré",
        "data": "Annonces légales : créations, radiations, procédures collectives",
        "sectors": ["all"],
    },
}


def list_connectors(sector: Optional[str] = None) -> pd.DataFrame:
    """Liste tous les connecteurs disponibles, filtrable par secteur."""
    rows = []
    for key, info in CONNECTORS.items():
        if sector and sector not in info["sectors"] and "all" not in info["sectors"]:
            continue
        rows.append({
            "id": key,
            "nom": info["name"],
            "auth": info["auth"],
            "rate_limit": info["rate_limit"],
            "données": info["data"],
            "secteurs": ", ".join(info["sectors"]),
            "url": info["url"],
        })
    return pd.DataFrame(rows)


def test_connector(connector_id: str) -> dict:
    """Teste la connectivité d'un connecteur."""
    info = CONNECTORS.get(connector_id)
    if not info:
        return {"status": "error", "message": "Connecteur inconnu"}

    try:
        if connector_id == "geo":
            GeoAPI.departements()
        elif connector_id == "ban":
            BanAPI.geocode("1 rue de Rivoli Paris")
        elif connector_id == "dvf":
            DvfAPI.mutations("33063", limit=1)
        elif connector_id == "dpe":
            DpeAPI.logements("75001", limit=1)
        elif connector_id == "cadastre":
            CadastreAPI.parcelles("75101", section="AB")
        elif connector_id == "sante":
            AnnuaireSanteAPI.search_practitioners("Paris", limit=1)
        elif connector_id == "datagouv":
            DataGouvAPI.search_datasets("immobilier", limit=1)
        elif connector_id == "bodacc":
            BodaccAPI.search(departement="75", limit=1)
        elif connector_id == "trends":
            GoogleTrendsLocal.trending_searches()
        else:
            return {"status": "skip", "message": "Nécessite une clé API"}
        return {"status": "ok", "message": f"{info['name']} connecté"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:200]}
