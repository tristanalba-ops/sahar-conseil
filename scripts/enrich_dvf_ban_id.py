#!/usr/bin/env python3
"""
enrich_dvf_ban_id.py
--------------------
Enrichit ban_id dans dvf_mutations via jointure texte BAN.
Stratégie : code_commune + adresse_numero + nom_voie normalisé (immutable_unaccent).
Tourne département par département pour éviter les timeouts.

Prérequis Supabase (déjà créés) :
  - Index idx_dvf_mutations_geom       (GiST sur dvf_mutations.geom)
  - Index idx_ban_commune_voie_norm    (btree sur ban_adresses)
  - Fonction immutable_unaccent(text)

Usage :
  export SUPABASE_DB_URL="postgresql://postgres.[ref]:[pwd]@aws-0-eu-west-3.pooler.supabase.com:6543/postgres"
  python enrich_dvf_ban_id.py --dry-run        # audit sans modifier
  python enrich_dvf_ban_id.py --dept 33        # tester sur la Gironde
  python enrich_dvf_ban_id.py                  # tous les départements
  python enrich_dvf_ban_id.py --start-dept 44  # reprendre à partir du 44
"""

import os, sys, time, argparse
import psycopg2

DB_URL = os.environ.get("SUPABASE_DB_URL", "")

UPDATE_SQL = """
UPDATE dvf_mutations d
SET ban_id = b.ban_id
FROM ban_adresses b
WHERE d.code_departement = %(dept)s
  AND d.ban_id IS NULL
  AND d.adresse_nom_voie IS NOT NULL
  AND d.adresse_numero IS NOT NULL
  AND b.code_commune = d.code_commune
  AND b.numero = d.adresse_numero
  AND lower(immutable_unaccent(b.nom_voie)) = lower(immutable_unaccent(d.adresse_nom_voie))
"""

COUNT_SQL = """
SELECT COUNT(*) FROM dvf_mutations d
JOIN ban_adresses b
  ON b.code_commune = d.code_commune
  AND b.numero = d.adresse_numero
  AND lower(immutable_unaccent(b.nom_voie)) = lower(immutable_unaccent(d.adresse_nom_voie))
WHERE d.code_departement = %(dept)s
  AND d.ban_id IS NULL
  AND d.adresse_nom_voie IS NOT NULL
  AND d.adresse_numero IS NOT NULL
"""

DEPTS_SQL = """
SELECT DISTINCT code_departement FROM dvf_mutations
WHERE ban_id IS NULL AND adresse_nom_voie IS NOT NULL
  AND adresse_numero IS NOT NULL AND code_departement IS NOT NULL
ORDER BY 1
"""

PROGRESS_SQL = """
SELECT
  COUNT(*) FILTER (WHERE ban_id IS NOT NULL) as avec_ban_id,
  COUNT(*) as total,
  ROUND(COUNT(*) FILTER (WHERE ban_id IS NOT NULL)::numeric / COUNT(*) * 100, 1) as pct
FROM dvf_mutations
WHERE adresse_nom_voie IS NOT NULL AND adresse_numero IS NOT NULL
"""

def get_conn():
    if not DB_URL:
        print("ERREUR : SUPABASE_DB_URL non définie")
        print("  Dashboard Supabase > Settings > Database > Connection string (Transaction pooler, port 6543)")
        print("  export SUPABASE_DB_URL='postgresql://postgres.[ref]:[pwd]@...'")
        sys.exit(1)
    return psycopg2.connect(DB_URL, connect_timeout=30, options="-c statement_timeout=300000")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dept", help="Un seul département (ex: 33)")
    parser.add_argument("--dry-run", action="store_true", help="Compter sans modifier")
    parser.add_argument("--start-dept", help="Reprendre à partir d'un département")
    args = parser.parse_args()

    conn = get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # État initial
            cur.execute(PROGRESS_SQL)
            row = cur.fetchone()
            print(f"État initial : {row[0]:,} / {row[1]:,} avec ban_id ({row[2]}%)")
            print()

            # Liste des départements
            if args.dept:
                depts = [args.dept]
            else:
                cur.execute(DEPTS_SQL)
                depts = [r[0] for r in cur.fetchall()]

            if args.start_dept and args.start_dept in depts:
                idx = depts.index(args.start_dept)
                depts = depts[idx:]
                print(f"Reprise à partir du département {args.start_dept} ({len(depts)} restants)")

            mode = "DRY RUN" if args.dry_run else "UPDATE"
            print(f"[{mode}] {len(depts)} département(s)")
            print("=" * 55)

            total_enrichi = 0
            for i, dept in enumerate(depts, 1):
                print(f"  [{i:3}/{len(depts)}] dept={dept:>3} ...", end=" ", flush=True)
                t0 = time.time()
                try:
                    if args.dry_run:
                        cur.execute(COUNT_SQL, {"dept": dept})
                        nb = cur.fetchone()[0]
                    else:
                        cur.execute(UPDATE_SQL, {"dept": dept})
                        nb = cur.rowcount
                        conn.commit()
                    elapsed = time.time() - t0
                    total_enrichi += nb
                    print(f"{nb:>7,} lignes  ({elapsed:.1f}s)")
                except Exception as e:
                    conn.rollback()
                    print(f"ERREUR : {e}")

            print("=" * 55)
            print(f"TOTAL : {total_enrichi:,} lignes {'potentielles' if args.dry_run else 'enrichies'}")

            if not args.dry_run:
                cur.execute(PROGRESS_SQL)
                row = cur.fetchone()
                print(f"État final  : {row[0]:,} / {row[1]:,} avec ban_id ({row[2]}%)")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
