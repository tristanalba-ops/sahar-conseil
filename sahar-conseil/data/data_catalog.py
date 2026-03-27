"""
SAHAR Conseil — data_catalog.py
Catalogue central de données : inventaire, accès unifié, métadonnées.

Ce module est le point d'entrée unique pour toutes les apps SAHAR.
Il gère :
  - L'inventaire de ce qui est disponible (local + API)
  - Le chargement intelligent (cache Parquet > API > CSV brut)
  - Les métadonnées (date, taille, source, fraîcheur)

Usage dans une app Streamlit :
    from data.data_catalog import catalog

    # Charger les transactions DVF Gironde
    df = catalog.load("dvf", departement="33")

    # Charger les DPE passoires F/G
    df = catalog.load("dpe", departement="33", etiquettes=["F", "G"])

    # Lister ce qui est disponible
    catalog.inventory()
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, List

HERE = Path(__file__).parent
RAW = HERE / "raw"
PROCESSED = HERE / "processed"
CACHE = HERE / "cache"

for d in [RAW, PROCESSED, CACHE]:
    d.mkdir(parents=True, exist_ok=True)


class DataCatalog:
    """Point d'accès unifié à toutes les données SAHAR."""

    def __init__(self):
        self._clients = {}

    def _get_client(self, source: str):
        """Lazy-load des clients API."""
        if source not in self._clients:
            from data.api_clients import (
                DVFClient, DPEClient, SIRENEClient,
                BANClient, GeoClient, IRVEClient, DataGouvClient,
            )
            mapping = {
                "dvf": DVFClient,
                "dpe": DPEClient,
                "sirene": SIRENEClient,
                "ban": BANClient,
                "geo": GeoClient,
                "irve": IRVEClient,
                "data_gouv": DataGouvClient,
            }
            cls = mapping.get(source)
            if cls:
                self._clients[source] = cls()
        return self._clients.get(source)

    # ── CHARGEMENT INTELLIGENT ───────────────────────────────────────────

    def load(self, source: str, departement: str = None, **kwargs) -> pd.DataFrame:
        """
        Charge un dataset avec la meilleure stratégie disponible.

        Ordre de priorité :
          1. Cache Parquet local (data/cache/) — si frais
          2. Fichier Parquet processé (data/processed/)
          3. API en direct
          4. CSV brut (data/raw/) — fallback

        Args:
            source: "dvf", "dpe", "sirene", "ban", "geo", "irve"
            departement: Code département
            **kwargs: Paramètres spécifiques à la source

        Returns:
            DataFrame
        """
        loaders = {
            "dvf": self._load_dvf,
            "dpe": self._load_dpe,
            "dpe_communes": self._load_dpe_communes,
            "sirene": self._load_sirene,
            "geo_communes": self._load_communes,
            "irve": self._load_irve,
        }

        loader = loaders.get(source)
        if loader:
            return loader(departement=departement, **kwargs)

        print(f"[Catalog] Source inconnue : {source}")
        return pd.DataFrame()

    def _load_dvf(self, departement: str = "33", **kwargs) -> pd.DataFrame:
        """Charge DVF : Parquet > API > CSV."""
        # 1. Parquet processé
        parquet = PROCESSED / f"dvf_{departement}.parquet"
        if parquet.exists() and parquet.stat().st_size > 1000:
            print(f"[DVF] Chargement Parquet local : {parquet.name}")
            return pd.read_parquet(parquet)

        # 2. API
        client = self._get_client("dvf")
        if client:
            print(f"[DVF] Chargement via API pour dept {departement}...")
            df = client.get_transactions(
                code_departement=departement,
                annee_min=kwargs.get("annee_min", 2022),
                max_pages=kwargs.get("max_pages", 30),
            )
            if not df.empty:
                return df

        # 3. CSV brut
        csv = RAW / f"dvf_{departement}.csv"
        if csv.exists():
            print(f"[DVF] Fallback CSV brut : {csv.name}")
            return pd.read_csv(csv, low_memory=False)

        print(f"[DVF] Aucune donnée pour le département {departement}")
        return pd.DataFrame()

    def _load_dpe(self, departement: str = "33", etiquettes: List[str] = None, **kwargs) -> pd.DataFrame:
        """Charge DPE : Parquet local > Supabase > API ADEME."""
        # 1. Parquet processé local
        parquet = PROCESSED / f"dpe_{departement}.parquet"
        if parquet.exists() and parquet.stat().st_size > 1000:
            print(f"[DPE] Chargement Parquet local : {parquet.name}")
            df = pd.read_parquet(parquet)
            if etiquettes and "etiquette_dpe" in df.columns:
                df = df[df["etiquette_dpe"].isin(etiquettes)]
            return df

        # 2. Supabase (125k+ logements E/F/G en base)
        try:
            from shared.supabase_dpe import get_dpe_logements
            print(f"[DPE] Chargement depuis Supabase pour dept {departement}...")
            df = get_dpe_logements(
                departement=departement,
                etiquettes=etiquettes,
                limit=kwargs.get("max_results", 10000),
            )
            if not df.empty:
                return df
        except Exception as e:
            print(f"[DPE] Supabase indisponible : {e}")

        # 3. API ADEME directe
        client = self._get_client("dpe")
        if client:
            print(f"[DPE] Chargement via API ADEME pour dept {departement}...")
            return client.get_logements(
                departement,
                etiquettes=etiquettes,
                max_results=kwargs.get("max_results", 10000),
            )

        return pd.DataFrame()

    def _load_dpe_communes(self, departement: str = None, **kwargs) -> pd.DataFrame:
        """Charge l'agrégation DPE par commune : Parquet local > Supabase."""
        try:
            from shared.supabase_dpe import get_dpe_communes
            return get_dpe_communes(departement=departement)
        except Exception:
            # Fallback Parquet direct
            agg = PROCESSED / "dpe_communes_agg.parquet"
            if agg.exists():
                df = pd.read_parquet(agg)
                if departement:
                    df = df[df["departement"] == departement]
                return df
            return pd.DataFrame()

    def _load_sirene(self, departement: str = "33", activite: str = None,
                     token: str = None, **kwargs) -> pd.DataFrame:
        """Charge SIRENE via API (token requis)."""
        client = self._get_client("sirene")
        if not client and token:
            from data.api_clients import SIRENEClient
            client = SIRENEClient(token=token)
            self._clients["sirene"] = client

        if not client or not getattr(client, 'token', None):
            print("[SIRENE] Token INSEE requis. Passer token= dans load()")
            return pd.DataFrame()

        return client.search(activite=activite, departement=departement,
                             nombre=kwargs.get("nombre", 1000))

    def _load_communes(self, departement: str = "33", **kwargs) -> pd.DataFrame:
        """Charge les communes via Geo API."""
        client = self._get_client("geo")
        if client:
            return client.get_communes(departement)
        return pd.DataFrame()

    def _load_irve(self, departement: str = None, **kwargs) -> pd.DataFrame:
        """Charge les bornes IRVE."""
        client = self._get_client("irve")
        if client:
            return client.get_bornes(departement)
        return pd.DataFrame()

    # ── INVENTAIRE ───────────────────────────────────────────────────────

    def inventory(self) -> pd.DataFrame:
        """
        Inventaire complet de ce qui est disponible localement.

        Returns:
            DataFrame avec : source, type, fichier, taille, nb_lignes, date_modif
        """
        items = []

        # Fichiers raw
        for f in sorted(RAW.glob("*")):
            if f.is_file() and f.suffix in [".csv", ".parquet", ".gz"]:
                items.append(self._file_info(f, "raw"))

        # Fichiers processed
        for f in sorted(PROCESSED.glob("*")):
            if f.is_file() and f.suffix in [".parquet", ".csv"]:
                items.append(self._file_info(f, "processed"))

        # Cache
        for f in sorted(CACHE.glob("*")):
            if f.is_file():
                items.append(self._file_info(f, "cache"))

        df = pd.DataFrame(items) if items else pd.DataFrame(
            columns=["source", "type", "fichier", "taille_mo", "date_modif"]
        )

        return df.sort_values(["type", "source"]).reset_index(drop=True)

    def _file_info(self, path: Path, data_type: str) -> dict:
        """Métadonnées d'un fichier."""
        size = path.stat().st_size / 1024 / 1024
        mtime = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")

        # Deviner la source
        name = path.stem.lower()
        if "dvf" in name:
            source = "dvf"
        elif "dpe" in name:
            source = "dpe"
        elif "sirene" in name:
            source = "sirene"
        elif "irve" in name:
            source = "irve"
        elif "commune" in name or "geo" in name:
            source = "geo"
        elif "enrichi" in name:
            source = "dvf+dpe"
        else:
            source = "autre"

        # Nb lignes (parquet rapide, CSV échantillon)
        nb_lignes = None
        try:
            if path.suffix == ".parquet":
                nb_lignes = len(pd.read_parquet(path, columns=[pd.read_parquet(path, columns=None).columns[0]]))
            elif path.suffix == ".csv" and size < 200:
                nb_lignes = sum(1 for _ in open(path)) - 1
        except Exception:
            pass

        return {
            "source": source,
            "type": data_type,
            "fichier": path.name,
            "taille_mo": round(size, 1),
            "nb_lignes": nb_lignes,
            "date_modif": mtime,
            "path": str(path),
        }

    def freshness(self) -> dict:
        """Vérifie la fraîcheur des données par source."""
        inv = self.inventory()
        if inv.empty:
            return {}

        result = {}
        for source in inv["source"].unique():
            src_files = inv[inv["source"] == source]
            latest = src_files["date_modif"].max()
            total_size = src_files["taille_mo"].sum()
            result[source] = {
                "derniere_maj": latest,
                "nb_fichiers": len(src_files),
                "taille_totale_mo": round(total_size, 1),
            }

        return result

    # ── NETTOYAGE CACHE ──────────────────────────────────────────────────

    def clear_cache(self, source: str = None):
        """Vide le cache (tout ou par source)."""
        for f in CACHE.glob("*"):
            if source is None or f.stem.startswith(source):
                f.unlink()
                print(f"[Cache] Supprimé : {f.name}")

    def clear_all(self):
        """Vide tout le cache."""
        self.clear_cache()


# ── SINGLETON ────────────────────────────────────────────────────────────────

catalog = DataCatalog()
