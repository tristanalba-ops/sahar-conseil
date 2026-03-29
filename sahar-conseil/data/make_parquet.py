"""
SAHAR Conseil — make_parquet.py
Convertit tous les CSV DVF en parquet optimisé.

Usage :
    python make_parquet.py           # tous les CSV dans data/raw/
    python make_parquet.py --dept 33 # département spécifique
"""

import pandas as pd
import argparse
import sys
from pathlib import Path

HERE = Path(__file__).parent
RAW = HERE / "raw"
PROCESSED = HERE / "processed"

COLS = ['id_mutation','date_mutation','nature_mutation','valeur_fonciere',
        'adresse_numero','adresse_nom_voie','code_postal','code_commune',
        'nom_commune','type_local','surface_reelle_bati','nombre_pieces_principales',
        'surface_terrain','lot1_surface_carrez','longitude','latitude','id_parcelle']

DTYPES = {'code_commune':'str','code_postal':'str',
          'nature_mutation':'category','type_local':'category','nom_commune':'category'}


def process(csv_path: Path) -> Path:
    dept = csv_path.stem.replace('dvf_', '')
    out = PROCESSED / f"dvf_{dept}.parquet"

    print(f"[{dept}] Lecture {csv_path.name} ({csv_path.stat().st_size/1024/1024:.1f} Mo)...")
    header = pd.read_csv(csv_path, nrows=0).columns.tolist()
    usecols = [c for c in COLS if c in header]

    df = pd.read_csv(csv_path, usecols=usecols, dtype=DTYPES, low_memory=False)

    df['date_mutation'] = pd.to_datetime(df['date_mutation'], errors='coerce')
    for col in ['valeur_fonciere','surface_reelle_bati','surface_terrain',
                'lot1_surface_carrez','longitude','latitude']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df[df['type_local'].isin(['Appartement','Maison'])]
    df = df[df['nature_mutation'] == 'Vente']
    df = df.dropna(subset=['valeur_fonciere','surface_reelle_bati'])
    df = df[(df['surface_reelle_bati'] > 5) & (df['valeur_fonciere'] > 1000)]

    df['surface_utile'] = df['surface_reelle_bati']
    if 'lot1_surface_carrez' in df.columns:
        m = df['lot1_surface_carrez'].notna() & (df['lot1_surface_carrez'] > 0)
        df.loc[m, 'surface_utile'] = df.loc[m, 'lot1_surface_carrez']

    df['prix_m2'] = (df['valeur_fonciere'] / df['surface_utile']).round(0)
    df = df[df['prix_m2'].between(500, 25000)]
    df['adresse'] = (df.get('adresse_numero', pd.Series('')).fillna('').astype(str).str.strip() + ' ' +
                     df.get('adresse_nom_voie', pd.Series('')).fillna('').astype(str)).str.strip()
    df['annee'] = df['date_mutation'].dt.year.astype('Int16')
    df['mois'] = df['date_mutation'].dt.to_period('M').astype(str)

    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False, compression='snappy')

    print(f"[{dept}] ✅ {len(df):,} lignes → {out.name} ({out.stat().st_size/1024/1024:.1f} Mo)")
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dept', nargs='+', help='Codes département ex: 33 75')
    args = parser.parse_args()

    if args.dept:
        fichiers = [RAW / f"dvf_{d}.csv" for d in args.dept]
        fichiers = [f for f in fichiers if f.exists()]
    else:
        fichiers = sorted(RAW.glob("dvf_*.csv"))

    if not fichiers:
        print("Aucun fichier CSV DVF trouvé dans data/raw/")
        sys.exit(1)

    print(f"\nConversion de {len(fichiers)} fichier(s)...\n")
    for f in fichiers:
        process(f)
    print(f"\n✅ Terminé — {len(fichiers)} parquet(s) dans data/processed/")


if __name__ == "__main__":
    main()
