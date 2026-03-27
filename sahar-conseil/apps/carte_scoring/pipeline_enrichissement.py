"""
SAHAR Conseil — Pipeline d'enrichissement DVF + BAN + DPE
==========================================================
Ce script tourne UNE FOIS en batch (sur ta machine ou GitHub Actions).
Il produit un fichier parquet enrichi par département, chargé
instantanément par l'app carte Streamlit.

Usage :
    python pipeline_enrichissement.py --dept 33
    python pipeline_enrichissement.py --dept 33 --limit 5000   # test rapide

Sorties :
    data/processed/enrichi_{dept}.parquet

Pipeline :
    1. Charger DVF (parquet existant)
    2. Géocoder les adresses manquantes via API BAN (batch 100 adresses/req)
    3. Télécharger DPE ADEME pour le département (API data.ademe.fr)
    4. Matcher DVF ↔ DPE par coordonnées GPS (rayon 15m)
    5. Calculer Score Opportunité Acheteur + Score Probabilité Vente
    6. Sauvegarder en parquet enrichi
"""

import argparse
import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# ── Chemins ──────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[2]
RAW  = ROOT / "data" / "raw"
PROC = ROOT / "data" / "processed"
PROC.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# 1. CHARGEMENT DVF
# ─────────────────────────────────────────────────────────────────────────────

def load_dvf(dept: str, limit: int = None) -> pd.DataFrame:
    """Charge le DVF depuis le parquet existant ou le CSV brut."""
    parquet = PROC / f"dvf_{dept}.parquet"
    csv     = RAW  / f"dvf_{dept}.csv"

    if parquet.exists():
        print(f"[DVF] Chargement parquet {parquet.name}...")
        df = pd.read_parquet(parquet)
    elif csv.exists():
        print(f"[DVF] Chargement CSV {csv.name}...")
        df = pd.read_csv(csv, low_memory=False)
        df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
        df = df[df["type_local"].isin(["Appartement", "Maison"])]
        df = df[df["nature_mutation"] == "Vente"]
        df = df.dropna(subset=["valeur_fonciere", "surface_reelle_bati"])
        df["surface_utile"] = df["surface_reelle_bati"]
        if "lot1_surface_carrez" in df.columns:
            m = df["lot1_surface_carrez"].notna() & (df["lot1_surface_carrez"] > 0)
            df.loc[m, "surface_utile"] = df.loc[m, "lot1_surface_carrez"]
        df["prix_m2"] = (df["valeur_fonciere"] / df["surface_utile"]).round(0)
        df = df[df["prix_m2"].between(500, 25000)]
        adresse_num = df.get("adresse_numero", pd.Series("")).fillna("").astype(str).str.strip()
        adresse_voie = df.get("adresse_nom_voie", pd.Series("")).fillna("").astype(str)
        df["adresse"] = (adresse_num + " " + adresse_voie).str.strip()
        df["annee"] = pd.to_datetime(df["date_mutation"], errors="coerce").dt.year
    else:
        raise FileNotFoundError(f"Pas de fichier DVF pour le département {dept}")

    # Filtrer sur les 5 dernières années max
    cutoff = pd.Timestamp.now() - pd.DateOffset(years=5)
    df = df[df["date_mutation"] >= cutoff].copy()

    if limit:
        df = df.head(limit)

    print(f"[DVF] {len(df):,} transactions chargées")
    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# 2. GÉOCODAGE BAN (API officielle)
# ─────────────────────────────────────────────────────────────────────────────

BAN_URL = "https://api-adresse.data.gouv.fr/search/csv/"

def geocoder_ban_batch(df: pd.DataFrame, dept: str) -> pd.DataFrame:
    """
    Géocode les adresses sans coordonnées via l'API BAN batch.
    L'API accepte un CSV, retourne un CSV avec lat/lon ajoutés.
    On traite par blocs de 5000 lignes.
    """
    # Lignes sans coords valides
    mask_missing = df["latitude"].isna() | df["longitude"].isna()
    n_missing = mask_missing.sum()

    if n_missing == 0:
        print("[BAN] Toutes les coords sont présentes.")
        return df

    print(f"[BAN] Géocodage de {n_missing:,} adresses manquantes...")

    df_miss = df[mask_missing].copy()
    df_miss["adresse_ban"] = df_miss["adresse"] + ", " + df_miss.get("code_postal", "").fillna("").astype(str)

    resultats = []
    bloc = 5000

    for i in range(0, len(df_miss), bloc):
        chunk = df_miss.iloc[i:i+bloc][["id_mutation", "adresse_ban"]].copy()
        chunk = chunk.rename(columns={"adresse_ban": "adresse"})

        csv_data = chunk.to_csv(index=False)
        try:
            resp = requests.post(
                BAN_URL,
                files={"data": ("adresses.csv", csv_data, "text/csv")},
                data={"columns": "adresse", "citycode": "code_commune" if "code_commune" in df.columns else ""},
                timeout=60
            )
            if resp.status_code == 200:
                from io import StringIO
                result_df = pd.read_csv(StringIO(resp.text))
                resultats.append(result_df)
                print(f"[BAN] Bloc {i//bloc + 1} OK — {len(chunk)} adresses")
            else:
                print(f"[BAN] Erreur bloc {i//bloc + 1}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"[BAN] Erreur bloc {i//bloc + 1}: {e}")

        time.sleep(0.5)  # Rate limit

    if resultats:
        df_geo = pd.concat(resultats, ignore_index=True)
        # Colonnes BAN : latitude, longitude, result_score
        if "latitude" in df_geo.columns and "longitude" in df_geo.columns:
            geo_map = df_geo.set_index("id_mutation")[["latitude", "longitude", "result_score"]].to_dict("index")
            for idx, row in df.iterrows():
                if mask_missing[idx] and row["id_mutation"] in geo_map:
                    entry = geo_map[row["id_mutation"]]
                    if entry.get("result_score", 0) > 0.5:
                        df.at[idx, "latitude"]  = entry["latitude"]
                        df.at[idx, "longitude"] = entry["longitude"]

    n_resolved = df["latitude"].notna().sum()
    print(f"[BAN] Résultat : {n_resolved:,} / {len(df):,} biens géocodés")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. TÉLÉCHARGEMENT DPE ADEME
# ─────────────────────────────────────────────────────────────────────────────

DPE_API = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines"

def telecharger_dpe(dept: str, max_lignes: int = 200000) -> pd.DataFrame:
    """
    Télécharge les DPE ADEME pour un département via l'API data.ademe.fr.
    Retourne un DataFrame avec : latitude, longitude, classe_energie,
    surface_habitable, annee_construction, adresse_ban.
    """
    cache = PROC / f"dpe_{dept}.parquet"
    if cache.exists():
        print(f"[DPE] Cache trouvé : {cache.name}")
        return pd.read_parquet(cache)

    print(f"[DPE] Téléchargement DPE département {dept}...")

    cols = [
        "N°DPE", "Adresse_(BAN)", "Coordonnée_cartographique_X_(BAN)",
        "Coordonnée_cartographique_Y_(BAN)", "Classe_énergie", "Etiquette_GES",
        "Surface_habitable_logement", "Année_construction",
        "Date_réception_DPE", "Code_postal_(BAN)", "Commune_(BAN)"
    ]

    resultats = []
    taille_page = 10000
    total = 0

    for page in range(0, max_lignes, taille_page):
        try:
            params = {
                "size": taille_page,
                "from": page,
                "select": ",".join(cols),
                "qs": f"Code_postal_(BAN):{dept}*",  # Filtre par code postal
            }
            resp = requests.get(DPE_API, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"[DPE] HTTP {resp.status_code} à la page {page}")
                break

            data = resp.json()
            results = data.get("results", [])
            if not results:
                break

            resultats.extend(results)
            total += len(results)
            print(f"[DPE] {total:,} DPE récupérés...")

            if total >= data.get("total", 0):
                break

            time.sleep(0.3)

        except Exception as e:
            print(f"[DPE] Erreur page {page}: {e}")
            break

    if not resultats:
        print("[DPE] Aucun DPE récupéré — vérifier la connexion à data.ademe.fr")
        return pd.DataFrame()

    df_dpe = pd.DataFrame(resultats)

    # Renommage colonnes
    rename = {
        "N°DPE": "dpe_id",
        "Adresse_(BAN)": "adresse_dpe",
        "Coordonnée_cartographique_X_(BAN)": "lon_dpe",
        "Coordonnée_cartographique_Y_(BAN)": "lat_dpe",
        "Classe_énergie": "classe_energie",
        "Etiquette_GES": "classe_ges",
        "Surface_habitable_logement": "surface_dpe",
        "Année_construction": "annee_construction",
        "Date_réception_DPE": "date_dpe",
        "Code_postal_(BAN)": "cp_dpe",
        "Commune_(BAN)": "commune_dpe",
    }
    df_dpe = df_dpe.rename(columns={k: v for k, v in rename.items() if k in df_dpe.columns})

    # Nettoyage coords (Lambert93 → WGS84 si nécessaire, sinon direct)
    for col in ["lat_dpe", "lon_dpe", "surface_dpe"]:
        if col in df_dpe.columns:
            df_dpe[col] = pd.to_numeric(df_dpe[col], errors="coerce")

    # Les coords ADEME sont en Lambert93 (EPSG:2154) → convertir en WGS84
    # Si les valeurs sont > 180, c'est du Lambert93
    if df_dpe["lon_dpe"].notna().any() and df_dpe["lon_dpe"].abs().max() > 180:
        try:
            import pyproj
            transformer = pyproj.Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
            valid = df_dpe["lon_dpe"].notna() & df_dpe["lat_dpe"].notna()
            lons, lats = transformer.transform(
                df_dpe.loc[valid, "lon_dpe"].values,
                df_dpe.loc[valid, "lat_dpe"].values
            )
            df_dpe.loc[valid, "lon_dpe"] = lons
            df_dpe.loc[valid, "lat_dpe"] = lats
            print("[DPE] Conversion Lambert93 → WGS84 OK")
        except ImportError:
            print("[DPE] pyproj non disponible — coords Lambert93 non converties")

    # Garder seulement les DPE récents (< 10 ans)
    if "date_dpe" in df_dpe.columns:
        df_dpe["date_dpe"] = pd.to_datetime(df_dpe["date_dpe"], errors="coerce")
        cutoff = pd.Timestamp.now() - pd.DateOffset(years=10)
        df_dpe = df_dpe[df_dpe["date_dpe"] >= cutoff]

    df_dpe = df_dpe.dropna(subset=["lat_dpe", "lon_dpe"])
    df_dpe = df_dpe[
        df_dpe["lat_dpe"].between(40, 52) &
        df_dpe["lon_dpe"].between(-5, 10)
    ]

    print(f"[DPE] {len(df_dpe):,} DPE géolocalisés valides")
    df_dpe.to_parquet(cache, index=False)
    return df_dpe


# ─────────────────────────────────────────────────────────────────────────────
# 4. MATCHING DVF ↔ DPE PAR GPS (rayon 15m)
# ─────────────────────────────────────────────────────────────────────────────

def matcher_dvf_dpe(df_dvf: pd.DataFrame, df_dpe: pd.DataFrame) -> pd.DataFrame:
    """
    Matche chaque transaction DVF avec le DPE le plus proche (rayon 15m).
    Utilise un KD-Tree pour la performance.
    """
    if df_dpe.empty:
        print("[MATCH] Pas de DPE — scoring sans données énergie")
        df_dvf["classe_energie"] = None
        df_dvf["classe_ges"] = None
        df_dvf["surface_dpe"] = None
        df_dvf["annee_construction"] = None
        return df_dvf

    from scipy.spatial import cKDTree

    # DVF avec coords valides
    mask_dvf = df_dvf["latitude"].notna() & df_dvf["longitude"].notna()
    df_dvf_geo = df_dvf[mask_dvf].copy()

    # Convertir en radians pour distance haversine approx
    # Pour de petites distances, on peut approximer en degrés → mètres
    # 1° latitude ≈ 111,111m, 1° longitude ≈ 111,111 * cos(lat)m
    lat_ref = df_dvf_geo["latitude"].mean()
    lon_scale = np.cos(np.radians(lat_ref))

    coords_dvf = np.column_stack([
        df_dvf_geo["latitude"].values,
        df_dvf_geo["longitude"].values * lon_scale
    ])
    coords_dpe = np.column_stack([
        df_dpe["lat_dpe"].values,
        df_dpe["lon_dpe"].values * lon_scale
    ])

    tree = cKDTree(coords_dpe)

    # Rayon 15m en degrés ≈ 15/111111
    rayon_deg = 15 / 111111

    distances, indices = tree.query(coords_dvf, k=1, distance_upper_bound=rayon_deg)

    # Colonnes à ajouter
    for col in ["classe_energie", "classe_ges", "surface_dpe", "annee_construction"]:
        df_dvf[col] = None

    matched = 0
    for i, (dist, idx) in enumerate(zip(distances, indices)):
        if dist < rayon_deg and idx < len(df_dpe):
            orig_idx = df_dvf_geo.index[i]
            row_dpe = df_dpe.iloc[idx]
            df_dvf.at[orig_idx, "classe_energie"]    = row_dpe.get("classe_energie")
            df_dvf.at[orig_idx, "classe_ges"]        = row_dpe.get("classe_ges")
            df_dvf.at[orig_idx, "surface_dpe"]       = row_dpe.get("surface_dpe")
            df_dvf.at[orig_idx, "annee_construction"] = row_dpe.get("annee_construction")
            matched += 1

    taux = matched / len(df_dvf_geo) * 100 if len(df_dvf_geo) > 0 else 0
    print(f"[MATCH] {matched:,} / {len(df_dvf_geo):,} biens matchés avec DPE ({taux:.1f}%)")
    return df_dvf


# ─────────────────────────────────────────────────────────────────────────────
# 5. SCORING
# ─────────────────────────────────────────────────────────────────────────────

CLASSES_ENERGIE = {"A": 7, "B": 6, "C": 5, "D": 4, "E": 3, "F": 2, "G": 1}
CLASSES_PASSOIRE = {"F", "G"}

def calculer_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule deux scores par bien (0–100) :

    SCORE OPPORTUNITÉ ACHETEUR
    - Prix/m² sous la médiane commune  → 40%
    - Classe DPE F/G → décote implicite → 30%
    - Écart surface DVF vs DPE          → 15%
    - Ancienneté transaction (>3 ans)   → 15%

    SCORE PROBABILITÉ DE MISE EN VENTE
    - Durée détention (achat il y a 7-15 ans) → 35%
    - DPE F/G + obligation rénovation          → 35%
    - Plus-value latente (prix marché vs achat)→ 20%
    - Commune en tension (volume élevé)        → 10%
    """

    def norm(s, inverse=False):
        mn, mx = s.min(), s.max()
        if mx == mn:
            return pd.Series(50.0, index=s.index)
        n = (s - mn) / (mx - mn) * 100.0
        return (100.0 - n) if inverse else n

    now = pd.Timestamp.now()

    # ── Médiane prix/m² par commune ──────────────────────────────────────────
    med_commune = df.groupby("code_commune")["prix_m2"].transform("median")

    # ── SCORE OPPORTUNITÉ ACHETEUR ───────────────────────────────────────────

    # 1. Sous-évaluation prix (plus c'est bas vs médiane, mieux c'est)
    ratio_prix = ((med_commune - df["prix_m2"]) / med_commune.replace(0, np.nan)).clip(0, 1).fillna(0)
    s_sous_eval = norm(ratio_prix) * 0.40

    # 2. DPE F/G = décote potentielle (bon pour acheteur avisé)
    s_dpe_acheteur = df["classe_energie"].map(
        lambda x: 100 if x in CLASSES_PASSOIRE else (50 if pd.isna(x) else 0)
    ).astype(float) * 0.30

    # 3. Écart surface DVF vs DPE (>15% = anomalie = opportunité)
    ecart_surface = pd.Series(0.0, index=df.index)
    mask_surf = df["surface_dpe"].notna() & (df["surface_dpe"] > 0)
    ecart_surface[mask_surf] = (
        (df.loc[mask_surf, "surface_utile"] - df.loc[mask_surf, "surface_dpe"]).abs()
        / df.loc[mask_surf, "surface_dpe"]
    ).clip(0, 0.5) * 200  # normalise 0-100
    s_ecart_surf = ecart_surface * 0.15

    # 4. Transaction ancienne (>3 ans → prix potentiellement obsolète)
    anciennete = (now - df["date_mutation"]).dt.days.fillna(0)
    s_anciennete = norm(anciennete.clip(0, 365*5)) * 0.15

    df["score_acheteur"] = (
        s_sous_eval + s_dpe_acheteur + s_ecart_surf + s_anciennete
    ).round(0).clip(0, 100).astype(int)

    # ── SCORE PROBABILITÉ DE MISE EN VENTE ───────────────────────────────────

    # 1. Durée de détention optimale (7-15 ans = pic de probabilité de vente)
    duree_detention = anciennete / 365.25
    # Pic à 10 ans, décroissant avant et après
    s_detention = (1 - ((duree_detention - 10).abs() / 10).clip(0, 1)) * 100 * 0.35

    # 2. DPE F/G = pression réglementaire (loyers F interdits 2025, G ventes 2028)
    s_dpe_vente = df["classe_energie"].map(
        lambda x: 100 if x == "G" else (80 if x == "F" else (40 if pd.isna(x) else 0))
    ).astype(float) * 0.35

    # 3. Plus-value latente (prix marché actuel vs prix d'achat)
    prix_actuel_estime = med_commune
    plus_value = ((prix_actuel_estime - df["prix_m2"]) / df["prix_m2"].replace(0, np.nan)).clip(0, 2).fillna(0)
    s_plus_value = norm(plus_value) * 0.20

    # 4. Commune en tension (volume récent élevé = marché actif = motivation vendeur)
    vol_commune = df.groupby("code_commune")["prix_m2"].transform("count")
    s_tension_commune = norm(vol_commune.astype(float)) * 0.10

    df["score_vente"] = (
        s_detention + s_dpe_vente + s_plus_value + s_tension_commune
    ).round(0).clip(0, 100).astype(int)

    # ── SCORE GLOBAL (combiné) ────────────────────────────────────────────────
    df["score_global"] = (
        df["score_acheteur"] * 0.5 + df["score_vente"] * 0.5
    ).round(0).clip(0, 100).astype(int)

    # ── LABELS ───────────────────────────────────────────────────────────────
    df["label_acheteur"] = df["score_acheteur"].apply(
        lambda x: "🟢 Opportunité forte" if x >= 70
        else ("🟡 Intéressant" if x >= 45 else "⚪ Standard")
    )
    df["label_vente"] = df["score_vente"].apply(
        lambda x: "🔴 Probable" if x >= 70
        else ("🟠 Possible" if x >= 45 else "⚪ Faible")
    )
    df["passoire"] = df["classe_energie"].isin(["F", "G"])

    print(f"[SCORE] Score acheteur médian : {df['score_acheteur'].median():.0f}")
    print(f"[SCORE] Score vente médian    : {df['score_vente'].median():.0f}")
    print(f"[SCORE] Passoires thermiques  : {df['passoire'].sum():,} biens")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 6. MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pipeline enrichissement SAHAR")
    parser.add_argument("--dept", default="33", help="Code département (ex: 33)")
    parser.add_argument("--limit", type=int, default=None, help="Limiter le nb de lignes (test)")
    parser.add_argument("--skip-ban",  action="store_true", help="Ignorer le géocodage BAN")
    parser.add_argument("--skip-dpe",  action="store_true", help="Ignorer le téléchargement DPE")
    args = parser.parse_args()

    dept = args.dept
    print(f"\n{'='*60}")
    print(f"SAHAR — Pipeline enrichissement département {dept}")
    print(f"{'='*60}\n")

    # 1. DVF
    df = load_dvf(dept, limit=args.limit)

    # 2. Géocodage BAN
    if not args.skip_ban:
        df = geocoder_ban_batch(df, dept)
    else:
        print("[BAN] Ignoré")

    # 3. DPE
    if not args.skip_dpe:
        df_dpe = telecharger_dpe(dept)
        df = matcher_dvf_dpe(df, df_dpe)
    else:
        print("[DPE] Ignoré")
        for col in ["classe_energie", "classe_ges", "surface_dpe", "annee_construction"]:
            df[col] = None

    # 4. Scoring
    df = calculer_scores(df)

    # 5. Sauvegarde
    # Garder uniquement les colonnes utiles pour l'app
    cols_export = [
        "id_mutation", "date_mutation", "annee", "type_local",
        "adresse", "nom_commune", "code_commune", "code_postal",
        "latitude", "longitude",
        "valeur_fonciere", "surface_utile", "prix_m2",
        "nombre_pieces_principales",
        "classe_energie", "classe_ges", "surface_dpe", "annee_construction",
        "score_acheteur", "score_vente", "score_global",
        "label_acheteur", "label_vente", "passoire",
    ]
    cols_present = [c for c in cols_export if c in df.columns]
    df_out = df[cols_present].copy()

    out_path = PROC / f"enrichi_{dept}.parquet"
    df_out.to_parquet(out_path, index=False)

    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"\n[OK] Fichier sauvegardé : {out_path}")
    print(f"     {len(df_out):,} biens  |  {size_mb:.1f} MB")
    print(f"\nLance l'app carte avec :")
    print(f"  streamlit run apps/carte_scoring/app.py")


if __name__ == "__main__":
    main()
