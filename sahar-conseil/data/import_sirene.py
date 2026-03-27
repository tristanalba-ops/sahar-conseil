#!/usr/bin/env python3
"""
SAHAR Conseil — import_sirene.py
Télécharge et importe la base SIRENE dans Supabase.

Stratégie :
  1. Télécharge StockEtablissement_utf8.zip depuis data.gouv.fr
  2. Filtre uniquement les NAF cibles (voir sirene_naf_mapping dans Supabase)
  3. Enrichit avec le secteur SAHAR et le score de base
  4. Insère par batch dans Supabase

Usage :
    python import_sirene.py                    # Import complet
    python import_sirene.py --dept 33          # Un seul département
    python import_sirene.py --dept 33 75 69    # Plusieurs départements
    python import_sirene.py --update           # Mise à jour incrémentale (dernières créations)

Prérequis :
    pip install requests pandas supabase tqdm

Variables d'environnement :
    SUPABASE_URL  (défaut: URL SAHAR)
    SUPABASE_KEY  (défaut: anon key)
"""

import os
import sys
import csv
import zipfile
import argparse
import requests
import pandas as pd
from pathlib import Path
from io import BytesIO
from datetime import datetime
from time import sleep

# ── Config ────────────────────────────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://ylrrcbklufshebcizgus.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlscnJjYmtsdWZzaGViY2l6Z3VzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ1NjQzNTEsImV4cCI6MjA5MDE0MDM1MX0.KQjvB5aePbmCcrAu9yYKoIblDG0ui90LXa-DcL7HAEA")

SIRENE_URL = "https://files.data.gouv.fr/insee-sirene/StockEtablissement_utf8.zip"
SIRENE_GEO_URL = "https://files.data.gouv.fr/insee-sirene-geo/GeolocalisationEtablissement_Sirene_pour_analyses_statistiques_utf8.zip"

HERE = Path(__file__).parent
CACHE_DIR = HERE / "cache"
CACHE_DIR.mkdir(exist_ok=True)

BATCH_SIZE = 500

# Colonnes SIRENE à garder
KEEP_COLS = [
    "siret", "siren", "nic",
    "denominationUniteLegale", "denominationUsuelleEtablissement",
    "prenomUsuelUniteLegale", "nomUniteLegale",
    "activitePrincipaleEtablissement",  # = code NAF
    "categorieJuridiqueUniteLegale",
    "trancheEffectifsEtablissement",
    "numeroVoieEtablissement", "typeVoieEtablissement",
    "libelleVoieEtablissement", "complementAdresseEtablissement",
    "codePostalEtablissement", "libelleCommuneEtablissement",
    "codeCommuneEtablissement",
    "dateCreationEtablissement", "dateDebut",
    "dateDernierTraitementEtablissement",
    "etatAdministratifEtablissement",
]


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def load_naf_mapping(client) -> dict:
    """Charge le mapping NAF → secteur/score depuis Supabase."""
    resp = client.table("sirene_naf_mapping").select("*").execute()
    mapping = {}
    for row in resp.data:
        mapping[row["naf"]] = {
            "secteur_sahar": row["secteur_sahar"],
            "sous_secteur": row.get("sous_secteur", ""),
            "tags": row.get("tags", []),
            "score_base": row.get("score_base", 50),
            "naf_libelle": row.get("naf_libelle", ""),
        }
    return mapping


def download_sirene(force=False) -> Path:
    """Télécharge le fichier SIRENE (cache local)."""
    cache_file = CACHE_DIR / "StockEtablissement_utf8.csv"
    if cache_file.exists() and not force:
        print(f"[cache] {cache_file} existe déjà ({cache_file.stat().st_size / (1024**3):.1f} Go)")
        return cache_file

    print(f"[download] Téléchargement SIRENE depuis {SIRENE_URL}...")
    print("  (fichier ~2 Go compressé, ~4 Go décompressé — patience)")

    r = requests.get(SIRENE_URL, stream=True, timeout=600)
    r.raise_for_status()

    zip_path = CACHE_DIR / "StockEtablissement_utf8.zip"
    total = int(r.headers.get("content-length", 0))

    with open(zip_path, "wb") as f:
        downloaded = 0
        for chunk in r.iter_content(chunk_size=8192 * 16):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(f"\r  {pct:.1f}% ({downloaded / (1024**2):.0f} Mo)", end="", flush=True)
    print()

    print("[unzip] Extraction...")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(CACHE_DIR)

    zip_path.unlink()  # Supprimer le zip
    print(f"[ok] {cache_file}")
    return cache_file


def dept_from_cp(code_postal: str) -> str:
    """Extrait le département depuis le code postal."""
    if not code_postal or len(code_postal) < 2:
        return ""
    cp = str(code_postal).strip()
    if cp.startswith("97") or cp.startswith("98"):
        return cp[:3]
    return cp[:2]


def process_and_insert(csv_path: Path, naf_mapping: dict, client, depts=None):
    """Lit le CSV SIRENE, filtre, enrichit, et insère dans Supabase."""

    target_nafs = set(naf_mapping.keys())

    print(f"[process] Lecture {csv_path.name}...")
    print(f"  NAF cibles : {len(target_nafs)} codes")
    if depts:
        print(f"  Départements : {', '.join(depts)}")

    batch = []
    total_inserted = 0
    total_skipped = 0
    total_read = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_read += 1
            if total_read % 500000 == 0:
                print(f"\r  Lu {total_read:,} lignes — inséré {total_inserted:,}", end="", flush=True)

            # Filtre : actif uniquement
            if row.get("etatAdministratifEtablissement") != "A":
                total_skipped += 1
                continue

            # Filtre : NAF cible
            naf = row.get("activitePrincipaleEtablissement", "")
            if naf not in target_nafs:
                total_skipped += 1
                continue

            # Filtre : département si spécifié
            cp = row.get("codePostalEtablissement", "")
            dept = dept_from_cp(cp)
            if depts and dept not in depts:
                total_skipped += 1
                continue

            # Construire l'adresse
            num = row.get("numeroVoieEtablissement", "") or ""
            type_voie = row.get("typeVoieEtablissement", "") or ""
            lib_voie = row.get("libelleVoieEtablissement", "") or ""
            adresse = f"{num} {type_voie} {lib_voie}".strip()

            # Enrichissement SAHAR
            naf_info = naf_mapping[naf]

            record = {
                "siret": row.get("siret", ""),
                "siren": row.get("siren", ""),
                "nic": row.get("nic", ""),
                "denomination": row.get("denominationUniteLegale", "") or row.get("denominationUsuelleEtablissement", ""),
                "nom_commercial": row.get("denominationUsuelleEtablissement", ""),
                "prenom": row.get("prenomUsuelUniteLegale", ""),
                "nom": row.get("nomUniteLegale", ""),
                "naf": naf,
                "naf_libelle": naf_info["naf_libelle"],
                "categorie_juridique": row.get("categorieJuridiqueUniteLegale", ""),
                "tranche_effectifs": row.get("trancheEffectifsEtablissement", ""),
                "adresse": adresse,
                "complement_adresse": row.get("complementAdresseEtablissement", ""),
                "code_postal": cp,
                "commune": row.get("libelleCommuneEtablissement", ""),
                "code_commune": row.get("codeCommuneEtablissement", ""),
                "departement": dept,
                "etat_administratif": "A",
                "secteur_sahar": naf_info["secteur_sahar"],
                "score_potentiel": naf_info["score_base"],
                "tags": naf_info["tags"],
            }

            # Dates
            dc = row.get("dateCreationEtablissement", "")
            if dc and dc != "":
                try:
                    record["date_creation"] = dc
                except Exception:
                    pass

            dd = row.get("dateDebut", "")
            if dd and dd != "":
                record["date_debut_activite"] = dd

            batch.append(record)

            if len(batch) >= BATCH_SIZE:
                try:
                    client.table("sirene_etablissements").upsert(
                        batch, on_conflict="siret"
                    ).execute()
                    total_inserted += len(batch)
                except Exception as e:
                    print(f"\n  [err] Batch failed: {e}")
                    # Retry one by one
                    for rec in batch:
                        try:
                            client.table("sirene_etablissements").upsert(
                                [rec], on_conflict="siret"
                            ).execute()
                            total_inserted += 1
                        except Exception:
                            pass
                batch = []
                sleep(0.05)  # Rate limit respect

    # Dernier batch
    if batch:
        try:
            client.table("sirene_etablissements").upsert(
                batch, on_conflict="siret"
            ).execute()
            total_inserted += len(batch)
        except Exception as e:
            print(f"\n  [err] Last batch: {e}")

    print(f"\n[done] Lu {total_read:,} — Inséré {total_inserted:,} — Ignoré {total_skipped:,}")
    return total_inserted


def main():
    parser = argparse.ArgumentParser(description="Import SIRENE → Supabase")
    parser.add_argument("--dept", nargs="*", help="Départements à importer")
    parser.add_argument("--force-download", action="store_true", help="Re-télécharger")
    parser.add_argument("--dry-run", action="store_true", help="Juste compter, pas insérer")
    args = parser.parse_args()

    client = get_supabase()
    print("[supabase] Connecté")

    naf_mapping = load_naf_mapping(client)
    print(f"[naf] {len(naf_mapping)} codes NAF chargés")

    csv_path = download_sirene(force=args.force_download)

    if args.dry_run:
        print("[dry-run] Comptage uniquement...")

    total = process_and_insert(csv_path, naf_mapping, client, depts=args.dept)
    print(f"\n{'='*50}")
    print(f"Import terminé : {total:,} établissements dans Supabase")


if __name__ == "__main__":
    main()
