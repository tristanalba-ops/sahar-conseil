"""
SAHAR Conseil — convert_dpe_csv.py
Convertit les CSV DPE téléchargés en Parquet avec scoring urgence.

Usage :
    python convert_dpe_csv.py                    # Tous les CSV dpe_*.csv dans data/raw/
    python convert_dpe_csv.py --dept 33 75       # Départements spécifiques
    python convert_dpe_csv.py --input ~/Downloads # Chercher dans un dossier spécifique

Les CSV doivent être nommés dpe_XX.csv (ex: dpe_33.csv)
"""

import pandas as pd
import argparse
import sys
from pathlib import Path

HERE = Path(__file__).parent
RAW = HERE / "raw"
PROCESSED = HERE / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

# Mapping des nouveaux noms de champs ADEME vers noms internes SAHAR
RENAME_COLS = {
    "coordonnee_cartographique_x_ban": "longitude",
    "coordonnee_cartographique_y_ban": "latitude",
    "conso_5_usages_ef": "conso_5_usages_e_finale",
    "surface_habitable_immeuble": "surface_habitable_logement",
}


def score_urgence(row):
    """Calcule un score d'urgence rénovation 0-100."""
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

    periode = str(row.get("periode_construction", "") or "").lower()
    if "avant" in periode or "1948" in periode or "1974" in periode:
        s += 20
    elif "1975" in periode or "1988" in periode or "1990" in periode:
        s += 10

    return min(100, s)


def process_dpe_csv(csv_path: Path) -> Path:
    """Convertit un CSV DPE en Parquet optimisé."""
    dept = csv_path.stem.replace("dpe_", "")
    out = PROCESSED / f"dpe_{dept}.parquet"

    print(f"[{dept}] Lecture {csv_path.name} ({csv_path.stat().st_size / 1024 / 1024:.1f} Mo)...")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"[{dept}] {len(df):,} lignes chargées")

    # Renommage pour compatibilité
    df = df.rename(columns=RENAME_COLS)

    # Typage
    if "date_etablissement_dpe" in df.columns:
        df["date_etablissement_dpe"] = pd.to_datetime(
            df["date_etablissement_dpe"], errors="coerce"
        )
    for col in ["conso_5_usages_e_finale", "conso_5_usages_par_m2_ef",
                 "emission_ges_5_usages", "surface_habitable_logement",
                 "longitude", "latitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filtrer E/F/G si pas déjà filtré
    if "etiquette_dpe" in df.columns:
        before = len(df)
        df = df[df["etiquette_dpe"].isin(["E", "F", "G"])].copy()
        print(f"[{dept}] Filtre E/F/G : {len(df):,} / {before:,}")

    # Score urgence
    df["score_urgence"] = df.apply(score_urgence, axis=1)

    # Sauvegarde Parquet
    df.to_parquet(out, index=False, compression="snappy")
    size = out.stat().st_size / 1024 / 1024
    print(f"[{dept}] -> {out.name} ({size:.1f} Mo, {len(df):,} lignes)")
    return out


def main():
    parser = argparse.ArgumentParser(description="Convertit les CSV DPE en Parquet")
    parser.add_argument("--dept", nargs="+", help="Codes département (ex: 33 75)")
    parser.add_argument("--input", type=str, default=None,
                        help="Dossier source des CSV (défaut: data/raw/)")
    args = parser.parse_args()

    input_dir = Path(args.input) if args.input else RAW

    if args.dept:
        fichiers = [input_dir / f"dpe_{d}.csv" for d in args.dept]
        fichiers = [f for f in fichiers if f.exists()]
    else:
        fichiers = sorted(input_dir.glob("dpe_*.csv"))

    if not fichiers:
        print(f"Aucun fichier CSV DPE trouvé dans {input_dir}")
        print("Les fichiers doivent être nommés dpe_XX.csv (ex: dpe_33.csv)")
        sys.exit(1)

    print(f"\nConversion de {len(fichiers)} fichier(s) DPE...\n")
    for f in fichiers:
        try:
            process_dpe_csv(f)
        except Exception as e:
            print(f"[ERREUR] {f.name}: {e}")
    print(f"\nTerminé — fichiers Parquet dans {PROCESSED}")


if __name__ == "__main__":
    main()
