#!/usr/bin/env python3
"""
SAHAR Conseil — Collecteur IRIS démographique
Sources : INSEE RP2020 (population, logement, activité) + Filosofi 2020 (revenus)
Fichiers CSV open data — aucune clé API requise

Données chargées :
  - Population par tranche d'âge (RP2020)
  - Part propriétaires / locataires / logements vacants (RP2020)
  - Taille des ménages (RP2020)
  - Taux de chômage + CSP (RP2020)
  - Revenu médian + taux de pauvreté (Filosofi 2020)

Usage :
  pip install requests pandas --break-system-packages
  python collect_iris.py

Durée : ~3-5 minutes (téléchargement ~60 Mo de CSV)
"""

import os, sys, io, zipfile, time
import requests
import pandas as pd

# ── Config ─────────────────────────────────────────────────────────────────────
SUPABASE_URL = "https://wwvdpixzfaviaapixarb.supabase.co"
SUPABASE_KEY = os.getenv(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind3dmRwaXh6ZmF2aWFhcGl4YXJiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ3Mzk0NDgsImV4cCI6MjA5MDMxNTQ0OH0"
    ".NjOeWzUCo2BJPcxnkEJNm215GJBr1RAHba1eL_EF758",
)

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}

# INSEE open data — RP2020 IRIS (bases infracommunales agrégées)
# Population  : https://www.insee.fr/fr/statistiques/7704076
# Logement    : https://www.insee.fr/fr/statistiques/7704078
# Activité    : https://www.insee.fr/fr/statistiques/7704089
# Filosofi    : https://www.insee.fr/fr/statistiques/7233950
RP_POP_URL = "https://www.insee.fr/fr/statistiques/fichier/7704076/base-ic-evol-struct-pop-2020_csv.zip"
RP_LOG_URL = "https://www.insee.fr/fr/statistiques/fichier/7704078/base-ic-logement-2020_csv.zip"
RP_ACT_URL = "https://www.insee.fr/fr/statistiques/fichier/7704089/base-ic-activite-residents-2020_csv.zip"
# Filosofi 2020 IRIS
FIL_URL    = "https://www.insee.fr/fr/statistiques/fichier/7233950/indic-struct-distrib-revenu-2020-IRIS_csv.zip"


# ── Helpers ────────────────────────────────────────────────────────────────────

def download_csv(url: str, sep: str = ";") -> pd.DataFrame:
    """Télécharge un CSV (direct ou dans un ZIP) depuis l'INSEE."""
    print(f"  ↓ {url.split('/')[-1]} ...", end=" ", flush=True)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    print(f"{len(r.content)//1024:,} Ko")

    if url.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            csv_files = [f for f in z.namelist() if f.endswith(".csv") or f.endswith(".CSV")]
            if not csv_files:
                raise ValueError(f"Aucun CSV dans le ZIP : {z.namelist()}")
            fname = csv_files[0]
            print(f"    Lecture {fname}")
            with z.open(fname) as f:
                return pd.read_csv(f, sep=sep, dtype=str, encoding="utf-8", low_memory=False)
    else:
        return pd.read_csv(io.BytesIO(r.content), sep=sep, dtype=str, low_memory=False)


def safe_float(s) -> float | None:
    try:
        v = float(str(s).replace(",", ".").strip())
        return None if pd.isna(v) else v
    except (ValueError, TypeError):
        return None


def safe_int(s) -> int | None:
    try:
        v = float(str(s).replace(",", ".").strip())
        return None if pd.isna(v) else int(v)
    except (ValueError, TypeError):
        return None


def pct(num, denom) -> float | None:
    """Calcule num/denom * 100 avec garde-fou."""
    try:
        n, d = float(num), float(denom)
        return round(n / d * 100, 2) if d > 0 else None
    except (ValueError, TypeError):
        return None


def upsert_batch(rows: list[dict], table: str = "iris_demographics") -> bool:
    """Upsert en batch via Supabase REST (on conflict code_iris).

    Règles PostgREST :
    - ?on_conflict=code_iris → résoudre le conflit sur la colonne unique
    - Toutes les lignes d'un batch doivent avoir exactement les mêmes clés
    """
    if not rows:
        return True

    # Normaliser : toutes les lignes doivent avoir le même ensemble de clés
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())
    normalized = [{k: row.get(k, None) for k in all_keys} for row in rows]

    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=code_iris",
        headers=SB_HEADERS,
        json=normalized,
        timeout=30,
    )
    if r.status_code not in (200, 201):
        print(f"    ⚠ Supabase {r.status_code}: {r.text[:200]}")
        return False
    return True


# ── Étape 1 — Charger les IRIS existants en Supabase ──────────────────────────

def get_existing_iris() -> set:
    """Récupère les codes IRIS déjà en base."""
    existing = set()
    offset = 0
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/iris_demographics",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            params={"select": "code_iris", "limit": "1000", "offset": str(offset)},
            timeout=20,
        )
        rows = r.json()
        if not rows:
            break
        existing.update(row["code_iris"] for row in rows)
        if len(rows) < 1000:
            break
        offset += 1000
    return existing


# ── Étape 2 — RP2020 Population ───────────────────────────────────────────────

def process_pop(df_pop: pd.DataFrame, iris_set: set) -> dict:
    """
    Extrait depuis base-ic-evol-struct-pop-2020 :
    population totale + tranches d'âge.

    Colonnes clés (RP2020) :
      IRIS, COM, POP (pop totale)
      P20_POP0014, P20_POP1529, P20_POP3044,
      P20_POP4559, P20_POP6074, P20_POP75P
    """
    col_iris = next((c for c in df_pop.columns if c.upper() in ("IRIS", "CODE_IRIS")), None)
    if not col_iris:
        print("  ⚠ Colonne IRIS introuvable dans pop CSV")
        return {}

    df_pop = df_pop[df_pop[col_iris].isin(iris_set)].copy()
    print(f"  → {len(df_pop)} IRIS matchés sur population")

    # Normaliser les noms de colonnes
    df_pop.columns = [c.upper() for c in df_pop.columns]
    col_iris = col_iris.upper()

    data = {}
    for _, row in df_pop.iterrows():
        code = str(row[col_iris])
        pop  = safe_int(row.get("POP") or row.get("P20_POP"))
        if not pop:
            continue

        p0   = safe_float(row.get("P20_POP0014"))
        p15  = safe_float(row.get("P20_POP1529"))
        p30  = safe_float(row.get("P20_POP3044"))
        p45  = safe_float(row.get("P20_POP4559"))
        p60  = safe_float(row.get("P20_POP6074"))
        p75  = safe_float(row.get("P20_POP75P"))

        # Part 65+ (seniors, utile pour rénovation énergétique)
        p65  = safe_float(row.get("P20_POP6074"))  # approx 65+
        p65p = (p65 or 0) + (p75 or 0)

        data[code] = {
            "code_iris":   code,
            "population":  pop,
            "part_0_14":   pct(p0,  pop),
            "part_15_29":  pct(p15, pop),
            "part_30_44":  pct(p30, pop),
            "part_45_59":  pct(p45, pop),
            "part_60_74":  pct(p60, pop),
            "part_75_plus": pct(p75, pop),
            "part_seniors": pct(p65p, pop),
            "annee_ref":   2020,
        }
    return data


# ── Étape 3 — RP2020 Logement ─────────────────────────────────────────────────

def process_log(df_log: pd.DataFrame, iris_set: set, pop_data: dict) -> dict:
    """
    Extrait depuis base-ic-logement-2020 :
    résidences principales, propriétaires, locataires, vacants, taille ménage.

    Colonnes clés :
      IRIS, P20_LOG (total logements)
      P20_RP (résidences principales)
      P20_PROP (propriétaires occ.)
      P20_LOC (locataires)
      P20_LVAC (logements vacants)
      P20_NPER (nb personnes en rés. princ.)
    """
    col_iris = next((c for c in df_log.columns if c.upper() in ("IRIS", "CODE_IRIS")), None)
    if not col_iris:
        print("  ⚠ Colonne IRIS introuvable dans logement CSV")
        return {}

    df_log = df_log[df_log[col_iris].isin(iris_set)].copy()
    df_log.columns = [c.upper() for c in df_log.columns]
    col_iris = col_iris.upper()
    print(f"  → {len(df_log)} IRIS matchés sur logement")

    data = {}
    for _, row in df_log.iterrows():
        code = str(row[col_iris])
        log_total = safe_float(row.get("P20_LOG"))
        rp        = safe_float(row.get("P20_RP"))
        prop      = safe_float(row.get("P20_PROP"))
        loc       = safe_float(row.get("P20_LOC"))
        vac       = safe_float(row.get("P20_LVAC"))
        nper      = safe_float(row.get("P20_NPER"))  # personnes en RP

        taille_menage = round(nper / rp, 2) if (nper and rp and rp > 0) else None

        d = {
            "code_iris":                  code,
            "nb_residences_principales":  safe_int(rp),
            "part_proprietaires":         pct(prop, rp),
            "part_locataires":            pct(loc, rp),
            "part_logements_vacants":     pct(vac, log_total),
            "taille_menage_moy":          taille_menage,
        }
        # Fusionner avec pop_data si dispo
        if code in pop_data:
            data[code] = {**pop_data[code], **{k: v for k, v in d.items() if v is not None}}
        else:
            data[code] = d
    return data


# ── Étape 4 — RP2020 Activité ─────────────────────────────────────────────────

def process_act(df_act: pd.DataFrame, iris_set: set, merged: dict) -> dict:
    """
    Extrait depuis base-ic-activite-residents-2020 :
    taux de chômage + CSP (cadres, ouvriers).

    Colonnes clés :
      IRIS, P20_ACT1564 (actifs 15-64)
      P20_CHOM1564 (chômeurs)
      P20_CS3 (cadres)
      P20_CS6 (ouvriers)
    """
    col_iris = next((c for c in df_act.columns if c.upper() in ("IRIS", "CODE_IRIS")), None)
    if not col_iris:
        print("  ⚠ Colonne IRIS introuvable dans activité CSV")
        return merged

    df_act = df_act[df_act[col_iris].isin(iris_set)].copy()
    df_act.columns = [c.upper() for c in df_act.columns]
    col_iris = col_iris.upper()
    print(f"  → {len(df_act)} IRIS matchés sur activité")

    for _, row in df_act.iterrows():
        code = str(row[col_iris])
        act   = safe_float(row.get("P20_ACT1564"))
        chom  = safe_float(row.get("P20_CHOM1564"))
        cs3   = safe_float(row.get("P20_CS3"))   # cadres
        cs6   = safe_float(row.get("P20_CS6"))   # ouvriers

        d = {
            "code_iris":     code,
            "taux_chomage":  pct(chom, act),
            "part_cadres":   pct(cs3, act),
            "part_ouvriers": pct(cs6, act),
        }
        if code in merged:
            merged[code].update({k: v for k, v in d.items() if v is not None})
        else:
            merged[code] = d
    return merged


# ── Étape 5 — Filosofi Revenus ────────────────────────────────────────────────

def process_filosofi(df_fil: pd.DataFrame, iris_set: set, merged: dict) -> dict:
    """
    Extrait depuis indic-struct-distrib-revenu-2020-IRIS :
    revenu médian par UC + taux de pauvreté.

    Colonnes clés :
      IRIS, MED20 (revenu médian €/UC)
      TP6020 (taux pauvreté %)
    """
    col_iris = next((c for c in df_fil.columns if c.upper() in ("IRIS", "LIBIRIS", "CODE_IRIS")), None)
    if not col_iris:
        # Filosofi peut avoir un format différent
        print(f"  ⚠ Colonnes disponibles Filosofi : {list(df_fil.columns[:10])}")
        return merged

    df_fil = df_fil[df_fil[col_iris].isin(iris_set)].copy()
    df_fil.columns = [c.upper() for c in df_fil.columns]
    col_iris = col_iris.upper()
    print(f"  → {len(df_fil)} IRIS matchés sur Filosofi")

    for _, row in df_fil.iterrows():
        code = str(row[col_iris])

        # Filosofi 2020 : MED20 ou Q220 pour médiane
        rev = safe_float(row.get("MED20") or row.get("Q220") or row.get("RFMED20"))
        pauv = safe_float(row.get("TP6020") or row.get("TAUX_PAUVRETE"))

        d = {
            "code_iris":     code,
            "revenu_median": rev,
            "taux_pauvrete": pauv,
        }
        if code in merged:
            merged[code].update({k: v for k, v in d.items() if v is not None})
        else:
            merged[code] = d
    return merged


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("\n🏘  SAHAR — Collecteur IRIS démographique (RP2020 + Filosofi)\n")

    # 1. Récupérer les IRIS existants
    print("1. Chargement des IRIS Supabase...")
    iris_set = get_existing_iris()
    print(f"   {len(iris_set)} IRIS en base\n")

    if not iris_set:
        print("❌ Aucun IRIS en base. Vérifiez la connexion Supabase.")
        sys.exit(1)

    # 2. Téléchargement et traitement
    print("2. Téléchargement CSV INSEE (RP2020)...\n")

    merged = {}

    # Population
    try:
        df_pop = download_csv(RP_POP_URL)
        pop_data = process_pop(df_pop, iris_set)
        merged.update(pop_data)
        print(f"   ✓ Population : {len(pop_data)} IRIS traités")
    except Exception as e:
        print(f"   ⚠ Erreur population : {e}")

    print()

    # Logement
    try:
        df_log = download_csv(RP_LOG_URL)
        merged = process_log(df_log, iris_set, merged)
        print(f"   ✓ Logement traité")
    except Exception as e:
        print(f"   ⚠ Erreur logement : {e}")

    print()

    # Activité
    try:
        df_act = download_csv(RP_ACT_URL)
        merged = process_act(df_act, iris_set, merged)
        print(f"   ✓ Activité traitée")
    except Exception as e:
        print(f"   ⚠ Erreur activité : {e}")

    print()

    # Filosofi
    try:
        df_fil = download_csv(FIL_URL)
        merged = process_filosofi(df_fil, iris_set, merged)
        print(f"   ✓ Filosofi (revenus) traité")
    except Exception as e:
        print(f"   ⚠ Erreur Filosofi : {e}")

    print(f"\n3. Enrichissement total : {len(merged)} IRIS à mettre à jour\n")

    if not merged:
        print("❌ Aucune donnée collectée. Vérifiez les URLs et votre connexion.")
        sys.exit(1)

    # 3. Upsert Supabase par batch de 100
    rows = list(merged.values())
    batch_size = 100
    total_ok = 0

    print(f"4. Upsert Supabase ({len(rows)} lignes, batch={batch_size})...")
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        ok = upsert_batch(batch)
        if ok:
            total_ok += len(batch)
        pct_done = (i + len(batch)) / len(rows) * 100
        print(f"   [{pct_done:5.1f}%] {i + len(batch)}/{len(rows)}", end="\r")
        time.sleep(0.1)  # Rate limiting poli

    print(f"\n\n✅ Terminé — {total_ok}/{len(rows)} IRIS enrichis")

    # 4. Vérification rapide
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/iris_demographics",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        params={
            "select": "code_iris,population,revenu_median,taux_chomage,part_proprietaires",
            "population": "not.is.null",
            "limit": "3",
        },
        timeout=10,
    )
    if r.ok:
        print("\nSample Supabase :")
        for row in r.json():
            print(f"  {row['code_iris']} — pop={row.get('population')} "
                  f"rev_med={row.get('revenu_median')} "
                  f"chôm={row.get('taux_chomage')}%")


if __name__ == "__main__":
    main()
