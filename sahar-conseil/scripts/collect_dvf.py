#!/usr/bin/env python3
"""
SAHAR Conseil — Collecteur DVF national
Source : https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz

Stratégie :
  - 16 départements prioritaires (IDF + grandes métropoles)
  - Années 2022, 2023, 2024 (3 ans glissants)
  - Filtre : Vente + Appartement/Maison + surface > 0 + prix > 0
  - Upsert Supabase sur id_mutation (pas de doublon)
  - Recalcul market_stats via SQL après chargement

Usage :
  pip install requests pandas --break-system-packages
  python collect_dvf.py
  python collect_dvf.py --dept 33        # un seul département
  python collect_dvf.py --year 2024      # une seule année
  python collect_dvf.py --dry-run        # compte sans insérer

Durée estimée : 15-25 min (16 depts × 3 ans ≈ 400-600k lignes filtrées)
"""

import os, sys, io, gzip, csv, json, time, argparse
import requests

# ── Config ─────────────────────────────────────────────────────────────────────
SUPABASE_URL = "https://wwvdpixzfaviaapixarb.supabase.co"
SUPABASE_KEY = os.getenv(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind3dmRwaXh6ZmF2aWFhcGl4YXJiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ3Mzk0NDgsImV4cCI6MjA5MDMxNTQ0OH0"
    ".NjOeWzUCo2BJPcxnkEJNm215GJBr1RAHba1eL_EF758",
)

SB_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "resolution=merge-duplicates,return=minimal",
}

DVF_BASE  = "https://files.data.gouv.fr/geo-dvf/latest/csv"
YEARS     = [2022, 2023, 2024]

# 16 départements prioritaires CLAUDE.md
PRIORITY_DEPTS = [
    "75", "92", "93", "94", "91", "78", "95", "77",   # IDF
    "69", "13", "31", "33", "06", "44", "67", "59",   # Métropoles
]

# Filtre : uniquement les ventes résidentielles
NATURES_OK = {"Vente"}
TYPES_OK   = {"Appartement", "Maison"}

BATCH_SIZE = 200   # lignes par upsert Supabase


# ── Helpers ────────────────────────────────────────────────────────────────────

def sf(v):
    """safe float"""
    try:
        f = float(str(v).replace(",", ".").strip())
        return f if f == f else None
    except (ValueError, TypeError):
        return None

def si(v):
    """safe int"""
    f = sf(v)
    return int(f) if f is not None else None


def download_dept_year(dept: str, year: int) -> list[dict]:
    """Télécharge et filtre le CSV.GZ DVF pour un département/année."""
    url = f"{DVF_BASE}/{year}/departements/{dept}.csv.gz"
    print(f"  ↓ {dept}/{year} ...", end=" ", flush=True)

    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
    except Exception as e:
        print(f"✗ {e}")
        return []

    print(f"{len(r.content)//1024:,} Ko", end=" → ", flush=True)

    rows = []
    with gzip.open(io.BytesIO(r.content), "rt", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Filtre nature
            if row.get("nature_mutation", "").strip() not in NATURES_OK:
                continue
            # Filtre type local
            type_local = row.get("type_local", "").strip()
            if type_local not in TYPES_OK:
                continue
            # Filtre surface et prix
            surface = sf(row.get("surface_reelle_bati"))
            prix    = sf(row.get("valeur_fonciere"))
            if not surface or surface <= 0:
                continue
            if not prix or prix <= 0:
                continue

            # Construire l'id_mutation
            id_mut = row.get("id_mutation", "").strip()
            if not id_mut:
                # Fallback : département + commune + date + numéro disposition
                id_mut = f"DVF-{dept}-{row.get('code_commune','')}-{row.get('date_mutation','')}-{row.get('numero_disposition','0')}"

            rows.append({
                "id_mutation":              id_mut,
                "date_mutation":            row.get("date_mutation") or None,
                "nature_mutation":          "Vente",
                "valeur_fonciere":          prix,
                "code_postal":              row.get("code_postal")  or None,
                "commune":                  row.get("nom_commune")  or None,
                "code_commune":             row.get("code_commune") or None,
                "code_departement":         dept,
                "adresse_numero":           row.get("adresse_numero")   or None,
                "adresse_suffixe":          row.get("adresse_suffixe")  or None,
                "adresse_nom_voie":         row.get("adresse_nom_voie") or None,
                "type_local":               type_local,
                "surface_reelle_bati":      surface,
                "nombre_pieces_principales": si(row.get("nombre_pieces_principales")),
                "surface_terrain":          sf(row.get("surface_terrain")),
                "longitude":                sf(row.get("longitude")),
                "latitude":                 sf(row.get("latitude")),
            })

    print(f"{len(rows):,} mutations résidentielles")
    return rows


def upsert_batch(rows: list[dict]) -> bool:
    """Upsert en batch Supabase (on conflict id_mutation)."""
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/dvf_mutations?on_conflict=id_mutation",
        headers=SB_HEADERS,
        json=rows,
        timeout=60,
    )
    if r.status_code not in (200, 201):
        print(f"\n    ⚠ Supabase {r.status_code}: {r.text[:300]}")
        return False
    return True


def push_rows(rows: list[dict], dry_run: bool) -> int:
    """Insère rows en batches, affiche la progression. Retourne nb insérés."""
    if dry_run:
        return len(rows)
    ok = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        if upsert_batch(batch):
            ok += len(batch)
        pct = (i + len(batch)) / len(rows) * 100
        print(f"    [{pct:5.1f}%] {ok:,}/{len(rows):,}", end="\r")
        time.sleep(0.05)
    print(f"    [100.0%] {ok:,}/{len(rows):,} insérées          ")
    return ok


# ── Recalcul market_stats ──────────────────────────────────────────────────────

RECALC_SQL = """
-- Recalcul complet de market_stats depuis dvf_mutations
-- Minimum 3 ventes pour être fiable
INSERT INTO market_stats (
    code_postal, code_commune, code_departement, annee,
    prix_median_m2, prix_moyen_m2, prix_min_m2, prix_max_m2,
    volume, evolution_12m, updated_at
)
SELECT
    code_postal,
    code_commune,
    code_departement,
    EXTRACT(YEAR FROM date_mutation)::int        AS annee,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY ROUND(valeur_fonciere / surface_reelle_bati, 2)
    )::numeric(10,2)                             AS prix_median_m2,
    ROUND(AVG(valeur_fonciere / surface_reelle_bati), 2) AS prix_moyen_m2,
    ROUND(MIN(valeur_fonciere / surface_reelle_bati), 2) AS prix_min_m2,
    ROUND(MAX(valeur_fonciere / surface_reelle_bati), 2) AS prix_max_m2,
    COUNT(*)                                     AS volume,
    NULL::numeric                                AS evolution_12m,
    NOW()                                        AS updated_at
FROM dvf_mutations
WHERE nature_mutation = 'Vente'
  AND surface_reelle_bati > 0
  AND valeur_fonciere    > 0
  AND code_postal IS NOT NULL
  AND code_commune IS NOT NULL
GROUP BY code_postal, code_commune, code_departement,
         EXTRACT(YEAR FROM date_mutation)
HAVING COUNT(*) >= 3
ON CONFLICT (code_commune, annee)
DO UPDATE SET
    prix_median_m2 = EXCLUDED.prix_median_m2,
    prix_moyen_m2  = EXCLUDED.prix_moyen_m2,
    prix_min_m2    = EXCLUDED.prix_min_m2,
    prix_max_m2    = EXCLUDED.prix_max_m2,
    volume         = EXCLUDED.volume,
    updated_at     = EXCLUDED.updated_at;
"""

EVOLUTION_SQL = """
-- Calcul evolution_12m : (prix_median annee N - annee N-1) / annee N-1 * 100
UPDATE market_stats curr
SET evolution_12m = ROUND(
    (curr.prix_median_m2 - prev.prix_median_m2) / NULLIF(prev.prix_median_m2, 0) * 100,
    2
)
FROM market_stats prev
WHERE prev.code_commune = curr.code_commune
  AND prev.annee        = curr.annee - 1
  AND prev.prix_median_m2 > 0;
"""


def recalc_market_stats(dry_run: bool):
    """Recalcule market_stats via Supabase RPC (SQL direct)."""
    if dry_run:
        print("\n  [dry-run] Recalcul market_stats ignoré")
        return

    print("\n🔄 Recalcul market_stats...")
    for label, sql in [("agrégats", RECALC_SQL), ("évolution", EVOLUTION_SQL)]:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
            headers=SB_HEADERS,
            json={"query": sql},
            timeout=120,
        )
        # RPC execute_sql n'existe pas forcément — on utilise le endpoint SQL direct
        # Via l'API Supabase, il faut passer par la Management API
        # Fallback : afficher le SQL pour exécution manuelle
        if r.status_code == 404:
            print(f"  ℹ Exécute ce SQL manuellement dans Supabase SQL Editor :")
            print(f"  → market_stats {label}")
            return

        if r.status_code not in (200, 201, 204):
            print(f"  ⚠ {label} : {r.status_code} {r.text[:200]}")
        else:
            print(f"  ✓ {label} recalculé")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Collecteur DVF SAHAR")
    parser.add_argument("--dept",    help="Département unique (ex: 33)")
    parser.add_argument("--year",    type=int, help="Année unique (ex: 2024)")
    parser.add_argument("--dry-run", action="store_true", help="Compte sans insérer")
    args = parser.parse_args()

    depts = [args.dept] if args.dept else PRIORITY_DEPTS
    years = [args.year] if args.year else YEARS
    dry   = args.dry_run

    print(f"\n🏡 SAHAR — Collecteur DVF {'[DRY RUN]' if dry else ''}")
    print(f"   {len(depts)} départements × {len(years)} années")
    print(f"   Filtre : Vente + Appartement/Maison + surface>0 + prix>0\n")

    grand_total = 0
    grand_ok    = 0
    dept_stats  = {}

    for dept in depts:
        print(f"\n📍 Département {dept}")
        dept_rows = []

        for year in years:
            rows = download_dept_year(dept, year)
            dept_rows.extend(rows)

        if not dept_rows:
            print(f"  → Aucune donnée")
            continue

        # Dédupliquer sur id_mutation (au cas où)
        seen = {}
        for r in dept_rows:
            seen[r["id_mutation"]] = r
        dept_rows = list(seen.values())

        print(f"  Total {dept} : {len(dept_rows):,} lignes uniques ({len(years)} ans)")

        ok = push_rows(dept_rows, dry)
        grand_total += len(dept_rows)
        grand_ok    += ok
        dept_stats[dept] = {"total": len(dept_rows), "inserted": ok}

    print(f"\n{'─'*50}")
    print(f"✅ Total : {grand_ok:,}/{grand_total:,} mutations insérées")
    print(f"\nPar département :")
    for dept, s in dept_stats.items():
        print(f"  {dept} : {s['inserted']:,}/{s['total']:,}")

    # Recalcul market_stats
    recalc_market_stats(dry)

    print(f"""
{'─'*50}
📊 Prochaine étape — recalcul market_stats dans Supabase :
   Ouvre le SQL Editor de Supabase et exécute :

   1) Agrégats (prix, volume) :
      → Fichier : recalc_market_stats.sql (généré à côté)

   2) Évolution 12m :
      UPDATE market_stats curr
      SET evolution_12m = ROUND(
          (curr.prix_median_m2 - prev.prix_median_m2) /
          NULLIF(prev.prix_median_m2, 0) * 100, 2)
      FROM market_stats prev
      WHERE prev.code_commune = curr.code_commune
        AND prev.annee = curr.annee - 1
        AND prev.prix_median_m2 > 0;
""")

    # Sauvegarde locale du SQL de recalcul
    sql_path = os.path.join(os.path.dirname(__file__), "recalc_market_stats.sql")
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write(RECALC_SQL + "\n\n" + EVOLUTION_SQL)
    print(f"💾 SQL sauvegardé : {sql_path}")


if __name__ == "__main__":
    main()
