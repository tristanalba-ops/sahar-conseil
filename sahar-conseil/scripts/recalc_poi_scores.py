"""
recalc_poi_scores.py
====================
Recalcule les scores POI de `poi_scores_commune` avec une normalisation
percentile nationale (scipy.stats.percentileofscore).

Problème corrigé :
  L'ancienne normalisation utilisait _score_0_100(count, 0, max_dept) — les
  communes en haut de leur département obtenaient 100/100 quel que soit leur
  niveau absolu.

Nouvelle logique :
  Pour chaque catégorie (transport, education, sante, commerce, loisir) :
    score = percentileofscore(all_counts_national, commune_count, kind='mean')
  → score 0-100 où 50 = médiane nationale.
  → Bruges (20K hab, bien équipée) devrait se retrouver autour de 75-85.
  → Paris > 95, commune rurale isolée < 20.

Usage :
    python scripts/recalc_poi_scores.py           # recalcul + upsert
    python scripts/recalc_poi_scores.py --dry-run  # diagnostique seulement
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
from scipy.stats import percentileofscore

# ── Config ────────────────────────────────────────────────────────────────────

SUPABASE_URL = "https://wwvdpixzfaviaapixarb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind3dmRwaXh6ZmF2aWFhcGl4YXJiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDczOTQ0OCwiZXhwIjoyMDkwMzE1NDQ4fQ.gkPYtHK4JhXMzxjbnJOdVfVT8M3oT_meP23f4v5Mdt8"
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

# Poids pour le score_global
# Note : la colonne score_loisir n'existe pas dans poi_scores_commune.
# Les equipements "loisir" sont absents des donnees BPE actuelles (loisir=0 partout).
# Le poids loisir (10%) est redistribue sur transport et education.
WEIGHTS = {
    "transport":  0.35,
    "education":  0.25,
    "sante":      0.20,
    "commerce":   0.20,
}

CATEGORIES = list(WEIGHTS.keys())

# Toutes les communes sans équipements dans une catégorie reçoivent score=0
# (pas 50, car "pas d'équipements" est une réalité, pas une donnée manquante)

# ── Helpers ───────────────────────────────────────────────────────────────────

def supa_get(path: str, params: dict | None = None, page_size: int = 1000) -> list[dict]:
    """Paginate through a Supabase REST endpoint and return all rows.
    Uses page_size=1000 (Supabase default max_rows) to paginate safely.
    """
    rows = []
    offset = 0
    while True:
        p = {"limit": page_size, "offset": offset, **(params or {})}
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{path}", headers=HEADERS, params=p, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        # Stop if last page (fewer rows than requested)
        if len(batch) < page_size:
            break
        print(f"  fetched {offset}...", end="\r", flush=True)
    return rows


def supa_upsert(table: str, rows: list[dict], batch_size: int = 500) -> int:
    """Upsert rows in batches. Returns total upserted."""
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=batch,
            timeout=30,
        )
        if r.status_code not in (200, 201):
            print(f"\n  UPSERT ERROR {r.status_code}: {r.text[:200]}", file=sys.stderr)
            r.raise_for_status()
        total += len(batch)
        print(f"  upserted {total}/{len(rows)}...", end="\r", flush=True)
    print()
    return total


# ── Core logic ────────────────────────────────────────────────────────────────

def fetch_raw_counts() -> dict[str, dict[str, int]]:
    """
    Retourne counts[code_commune][categorie] = nb_equipements.
    Toutes les communes et catégories absentes → implicitement 0.
    """
    print("Chargement de poi_equipements...")
    rows = supa_get(
        "poi_equipements",
        params={"select": "code_commune,categorie"},
    )
    print(f"\n  {len(rows)} lignes chargees.")

    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        cc = row.get("code_commune")
        cat = row.get("categorie")
        if cc and cat:
            counts[cc][cat] += 1

    return counts


def fetch_commune_meta() -> dict[str, dict]:
    """
    Retourne meta[code_commune] = {nom_commune, code_departement} depuis poi_scores_commune.
    """
    print("Chargement metadonnees communes...")
    rows = supa_get(
        "poi_scores_commune",
        params={"select": "code_commune,nom_commune,code_departement"},
    )
    print(f"\n  {len(rows)} communes.")
    return {r["code_commune"]: r for r in rows}


def compute_national_percentiles(
    counts: dict[str, dict[str, int]],
    all_communes: list[str],
) -> dict[str, list[float]]:
    """
    Pour chaque catégorie, construit le vecteur national de counts
    (inclut les 0 pour les communes sans équipements).
    Retourne national_vectors[cat] = sorted list (pour percentileofscore).
    """
    vectors: dict[str, list[float]] = {}
    for cat in CATEGORIES:
        vec = [float(counts.get(cc, {}).get(cat, 0)) for cc in all_communes]
        vectors[cat] = vec
    return vectors


def score_commune(
    cc: str,
    counts: dict[str, dict[str, int]],
    vectors: dict[str, list[float]],
) -> dict:
    """Calcule les scores percentile pour une commune."""
    cat_scores = {}
    for cat in CATEGORIES:
        raw = float(counts.get(cc, {}).get(cat, 0))
        # percentileofscore : % de communes avec un count INFÉRIEUR au nôtre
        # kind='mean' : gère les ex-aequo avec la moyenne des rangs
        pct = percentileofscore(vectors[cat], raw, kind="mean")
        # Si count == 0, forcer à 0 (pas de POI = vraiment défavorisé)
        cat_scores[cat] = round(pct) if raw > 0 else 0

    nb_total = sum(counts.get(cc, {}).get(cat, 0) for cat in CATEGORIES)

    score_global = round(sum(cat_scores[cat] * WEIGHTS[cat] for cat in CATEGORIES))
    score_global = max(0, min(100, score_global))

    return {
        "score_transport": cat_scores["transport"],
        "score_education": cat_scores["education"],
        "score_sante":     cat_scores["sante"],
        "score_commerce":  cat_scores["commerce"],
        "score_global":    score_global,
        "nb_equipements":  nb_total,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main(dry_run: bool = False):
    t0 = time.time()
    print("=" * 60)
    print("RECALCUL SCORES POI — normalisation percentile nationale")
    print("=" * 60)

    # 1. Données brutes
    counts = fetch_raw_counts()
    meta   = fetch_commune_meta()

    # Union : toutes les communes connues (avec ou sans equipements)
    all_communes = sorted(set(counts.keys()) | set(meta.keys()))
    print(f"\nCommunes totales (union) : {len(all_communes)}")
    print(f"Communes avec équipements : {len(counts)}")
    print(f"Communes sans équipements : {len(all_communes) - len(counts)}")

    # 2. Vecteurs nationaux
    print("\nCalcul des vecteurs de percentile nationaux...")
    vectors = compute_national_percentiles(counts, all_communes)
    for cat in CATEGORIES:
        vec = vectors[cat]
        nz = sum(1 for v in vec if v > 0)
        print(f"  {cat:12s} : max={max(vec):.0f}, nonzero={nz}/{len(vec)}")

    # 3. Score de chaque commune
    print("\nCalcul des scores...")
    new_rows = []
    for cc in all_communes:
        scores = score_commune(cc, counts, vectors)
        m = meta.get(cc, {})
        new_rows.append({
            "code_commune":    cc,
            "nom_commune":     m.get("nom_commune", ""),
            "code_departement": m.get("code_departement") or (cc[:2] if len(cc) >= 2 else ""),
            "updated_at":      datetime.now(timezone.utc).isoformat(),
            **scores,
        })

    # 4. Stats de validation
    print("\n— Distribution des nouveaux scores —")
    for col in ["score_transport", "score_education", "score_sante", "score_commerce", "score_global"]:
        vals = [r[col] for r in new_rows if r[col] is not None]
        n100 = sum(1 for v in vals if v == 100)
        med  = sorted(vals)[len(vals)//2]
        mean = sum(vals)/len(vals)
        print(f"  {col:18s} : median={med:3d}, mean={mean:5.1f}, score=100: {n100}/{len(vals)} ({100*n100/len(vals):.1f}%)")

    # 5. Vérification Bruges + quelques villes de référence
    ref_communes = {
        "33075": "Bruges (33075, ~20K hab)",
        "33063": "Bordeaux (33063)",
        "75056": "Paris (75056)",
        "69123": "Lyon (69123)",
        "13055": "Marseille (13055)",
        "31555": "Toulouse (31555)",
    }
    print("\n— Villes de référence —")
    for cc, label in ref_communes.items():
        r = next((x for x in new_rows if x["code_commune"] == cc), None)
        if r:
            print(f"  {label:35s} -> global={r['score_global']:3d} "
                  f"(transport={r['score_transport']}, educ={r['score_education']}, "
                  f"sante={r['score_sante']}, commerce={r['score_commerce']}, "
                  f"nb={r['nb_equipements']})")
        else:
            print(f"  {label:35s} -> non trouvee")

    if dry_run:
        print("\n[DRY RUN] Aucun upsert effectué.")
        return

    # 6. Upsert
    print(f"\nUpsert de {len(new_rows)} lignes vers poi_scores_commune...")
    upserted = supa_upsert("poi_scores_commune", new_rows, batch_size=500)
    print(f"  {upserted} lignes upserted en {time.time()-t0:.1f}s")
    print("\nTerminé.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recalcul scores POI avec normalisation percentile nationale")
    parser.add_argument("--dry-run", action="store_true", help="Calcule sans écrire en base")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
