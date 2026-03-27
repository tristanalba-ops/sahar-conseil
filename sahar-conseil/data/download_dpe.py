"""
SAHAR Conseil — download_dpe.py
Télécharge les données DPE ADEME par département.

Usage :
    python download_dpe.py --dept 33
    python download_dpe.py --dept 33 75 69
    python download_dpe.py --all   (tous les départements)

Les fichiers sont sauvegardés dans data/processed/dpe_{dept}.parquet
"""

import requests
import pandas as pd
import argparse
import sys
from pathlib import Path
from time import sleep

HERE = Path(__file__).parent
PROCESSED = HERE / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

ADEME_URLS = [
    "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines",
    "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines",
]

SELECT = (
    "numero_dpe,date_etablissement_dpe,etiquette_dpe,etiquette_ges,"
    "conso_5_usages_e_finale,emission_ges_5_usages,"
    "adresse_ban,code_postal_ban,nom_commune_ban,"
    "latitude,longitude,type_batiment,annee_construction,"
    "surface_habitable_logement,type_energie_principale_chauffage"
)

DEPTS = [str(i).zfill(2) for i in range(1, 96) if i != 20] + ["2A", "2B"]


def download_dept(dept: str, max_results: int = 50000) -> pd.DataFrame:
    """Télécharge tous les DPE d'un département via l'API ADEME."""
    print(f"[{dept}] Téléchargement DPE...", end="", flush=True)

    all_rows = []
    after = None
    page = 0

    for url in ADEME_URLS:
        try:
            while True:
                params = {
                    "q": dept,
                    "q_fields": "code_departement_insee",
                    "size": 1000,
                    "select": SELECT,
                }
                if after:
                    params["after"] = after

                r = requests.get(url, params=params, timeout=60)
                if r.status_code != 200:
                    break

                data = r.json()
                results = data.get("results", [])
                if not results:
                    break

                all_rows.extend(results)
                page += 1
                print(f"\r[{dept}] Page {page} — {len(all_rows)} lignes", end="", flush=True)

                # Pagination
                after = data.get("next", {})
                if not after or len(all_rows) >= max_results:
                    break

                sleep(0.2)  # Respect rate limit

            if all_rows:
                break  # URL fonctionnelle trouvée

        except Exception as e:
            print(f"\r[{dept}] Erreur URL {url}: {e}")
            continue

    if not all_rows:
        print(f"\r[{dept}] ✗ Aucune donnée")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    print(f"\r[{dept}] ✓ {len(df):,} DPE téléchargés    ")
    return df


def clean_and_save(dept: str, df: pd.DataFrame) -> Path:
    """Nettoie et sauvegarde en parquet."""
    if df.empty:
        return None

    # Nettoyage types
    df["date_etablissement_dpe"] = pd.to_datetime(
        df.get("date_etablissement_dpe", pd.Series(dtype=str)), errors="coerce"
    )
    for col in ["conso_5_usages_e_finale", "emission_ges_5_usages",
                "surface_habitable_logement", "latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "annee_construction" in df.columns:
        df["annee_construction"] = pd.to_numeric(
            df["annee_construction"], errors="coerce"
        ).astype("Int64")

    # Garder seulement F et G + E pour anticipation
    if "etiquette_dpe" in df.columns:
        df_fg = df[df["etiquette_dpe"].isin(["F", "G", "E"])].copy()
        print(f"[{dept}] Filtre F/G/E : {len(df_fg):,} logements sur {len(df):,}")
    else:
        df_fg = df

    # Score urgence
    def score(row):
        s = 0
        etiq = row.get("etiquette_dpe", "")
        if etiq == "G": s += 50
        elif etiq == "F": s += 35
        elif etiq == "E": s += 20
        conso = row.get("conso_5_usages_e_finale", 0) or 0
        if conso > 450: s += 30
        elif conso > 330: s += 20
        elif conso > 250: s += 10
        annee = row.get("annee_construction", 0) or 0
        if annee and annee < 1975: s += 20
        elif annee and annee < 1990: s += 10
        return min(100, s)

    df_fg["score_urgence"] = df_fg.apply(score, axis=1)

    out = PROCESSED / f"dpe_{dept}.parquet"
    df_fg.to_parquet(out, index=False, compression="snappy")
    size = out.stat().st_size / 1024 / 1024
    print(f"[{dept}] → {out.name} ({size:.1f} Mo)")
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dept", nargs="+", help="Codes département ex: 33 75")
    parser.add_argument("--all", action="store_true", help="Tous les départements")
    parser.add_argument("--max", type=int, default=50000, help="Max résultats par dept")
    args = parser.parse_args()

    if args.all:
        depts = DEPTS
    elif args.dept:
        depts = args.dept
    else:
        # Par défaut : Gironde
        depts = ["33"]

    print(f"\nTéléchargement DPE — {len(depts)} département(s)\n")

    for dept in depts:
        df = download_dept(dept, args.max)
        if not df.empty:
            clean_and_save(dept, df)
        print()

    print(f"\n✓ Terminé — fichiers dans {PROCESSED}")


if __name__ == "__main__":
    main()
