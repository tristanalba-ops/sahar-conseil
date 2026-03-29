"""
SAHAR Conseil — download_all.py
Orchestrateur de collecte multi-sources via API.

Usage :
    python download_all.py --dept 33                    # DVF + DPE + Geo dept 33
    python download_all.py --dept 33 75 69 13 31        # Plusieurs départements
    python download_all.py --dept 33 --sources dvf dpe  # Sources spécifiques
    python download_all.py --dept 33 --all-sources      # Toutes les sources
    python download_all.py --dept 33 --sirene-token XXX  # Avec token SIRENE
    python download_all.py --inventory                   # Voir l'inventaire local

Sources disponibles :
    dvf     — Transactions immobilières (API DVF)
    dpe     — Diagnostics énergétiques (API ADEME)
    geo     — Communes, population (Geo API)
    sirene  — Entreprises (API INSEE, token requis)
    irve    — Bornes de recharge VE (data.gouv.fr)
"""

import argparse
import time
import json
import sys
from pathlib import Path
from datetime import datetime

HERE = Path(__file__).parent
PROCESSED = HERE / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

LOG_FILE = HERE / "collect_log.json"


def ts():
    return datetime.now().strftime("%H:%M:%S")


def log(msg, level="info"):
    icons = {"ok": "✅", "err": "❌", "info": "ℹ️ ", "warn": "⚠️ ", "skip": "⏭️ "}
    print(f"[{ts()}] {icons.get(level, '  ')} {msg}")


def save_log(journal):
    LOG_FILE.write_text(json.dumps(journal, indent=2, ensure_ascii=False), encoding="utf-8")


# ── COLLECTEURS ──────────────────────────────────────────────────────────────

def collect_dvf(dept: str, journal: dict, annee_min: int = 2022, max_pages: int = 40):
    """Collecte DVF via API et sauvegarde en Parquet."""
    from data.api_clients import DVFClient

    log(f"DVF dept {dept} — via API (années ≥ {annee_min})")
    client = DVFClient()
    df = client.get_transactions(
        code_departement=dept,
        annee_min=annee_min,
        max_pages=max_pages,
    )

    if df.empty:
        log(f"DVF dept {dept} — aucune donnée API, vérification fichier local...", "warn")
        # Fallback : check si le CSV brut existe et n'a pas de parquet
        raw_csv = HERE / "raw" / f"dvf_{dept}.csv"
        parquet = PROCESSED / f"dvf_{dept}.parquet"
        if raw_csv.exists() and not parquet.exists():
            log(f"DVF dept {dept} — conversion CSV → Parquet via make_parquet.py", "info")
            from data.make_parquet import process
            process(raw_csv)
            journal["ok"].append(f"dvf_{dept}_csv2parquet")
        elif parquet.exists():
            log(f"DVF dept {dept} — Parquet déjà existant ({parquet.stat().st_size/1024/1024:.1f} Mo)", "skip")
        else:
            journal["err"].append(f"dvf_{dept}")
        return

    # Sauvegarder en Parquet
    out = PROCESSED / f"dvf_{dept}_api.parquet"
    df.to_parquet(out, index=False, compression="snappy")
    size = out.stat().st_size / 1024 / 1024
    log(f"DVF dept {dept} — {len(df):,} transactions → {out.name} ({size:.1f} Mo)", "ok")
    journal["ok"].append(f"dvf_{dept}")


def collect_dpe(dept: str, journal: dict, max_results: int = 20000):
    """Collecte DPE via API ADEME."""
    from data.api_clients import DPEClient

    log(f"DPE dept {dept} — via API ADEME (F/G/E)")
    client = DPEClient()
    df = client.get_logements(dept, etiquettes=["E", "F", "G"], max_results=max_results)

    if df.empty:
        log(f"DPE dept {dept} — aucune donnée", "warn")
        journal["err"].append(f"dpe_{dept}")
        return

    out = PROCESSED / f"dpe_{dept}.parquet"
    df.to_parquet(out, index=False, compression="snappy")
    size = out.stat().st_size / 1024 / 1024
    log(f"DPE dept {dept} — {len(df):,} logements → {out.name} ({size:.1f} Mo)", "ok")
    journal["ok"].append(f"dpe_{dept}")


def collect_geo(dept: str, journal: dict):
    """Collecte communes via Geo API."""
    from data.api_clients import GeoClient

    log(f"GEO dept {dept} — communes via Geo API")
    client = GeoClient()
    df = client.get_communes(dept)

    if df.empty:
        log(f"GEO dept {dept} — aucune donnée", "warn")
        journal["err"].append(f"geo_{dept}")
        return

    out = PROCESSED / f"communes_{dept}.parquet"
    df.to_parquet(out, index=False, compression="snappy")
    log(f"GEO dept {dept} — {len(df)} communes → {out.name}", "ok")
    journal["ok"].append(f"geo_{dept}")


def collect_sirene(dept: str, journal: dict, token: str = None, activites: list = None):
    """Collecte SIRENE via API INSEE."""
    if not token:
        log("SIRENE — token INSEE requis (--sirene-token)", "warn")
        journal["err"].append(f"sirene_{dept}_no_token")
        return

    from data.api_clients import SIRENEClient

    client = SIRENEClient(token=token)

    # Codes APE par défaut : immobilier + énergie + auto
    if not activites:
        activites = [
            ("6820A", "agences_immo"),
            ("4322A", "plomberie_chauffage"),
            ("4329A", "isolation"),
            ("4321A", "electricite"),
            ("4511Z", "concessionnaires_auto"),
            ("5610A", "restauration"),
        ]

    all_frames = []
    for code_ape, label in activites:
        log(f"SIRENE dept {dept} — {label} ({code_ape})")
        df = client.search(activite=code_ape, departement=dept, nombre=500)
        if not df.empty:
            df["categorie"] = label
            all_frames.append(df)
            log(f"  → {len(df)} établissements", "ok")
        time.sleep(1)  # Rate limit INSEE

    if not all_frames:
        journal["err"].append(f"sirene_{dept}")
        return

    df_all = pd.concat(all_frames, ignore_index=True)
    out = PROCESSED / f"sirene_{dept}.parquet"

    import pandas as pd
    df_all.to_parquet(out, index=False, compression="snappy")
    log(f"SIRENE dept {dept} — {len(df_all):,} total → {out.name}", "ok")
    journal["ok"].append(f"sirene_{dept}")


def collect_irve(dept: str, journal: dict):
    """Collecte bornes IRVE."""
    from data.api_clients import IRVEClient

    log(f"IRVE dept {dept} — bornes de recharge VE")
    client = IRVEClient()
    df = client.get_bornes(dept)

    if df.empty:
        log(f"IRVE dept {dept} — aucune donnée", "warn")
        journal["err"].append(f"irve_{dept}")
        return

    out = PROCESSED / f"irve_{dept}.parquet"
    df.to_parquet(out, index=False, compression="snappy")
    log(f"IRVE dept {dept} — {len(df):,} bornes → {out.name}", "ok")
    journal["ok"].append(f"irve_{dept}")


# ── INVENTAIRE ───────────────────────────────────────────────────────────────

def show_inventory():
    """Affiche l'inventaire local."""
    from data.data_catalog import catalog
    inv = catalog.inventory()
    if inv.empty:
        print("\n📭 Aucune donnée locale. Lancez une collecte :")
        print("   python download_all.py --dept 33\n")
        return

    print(f"\n{'='*70}")
    print(f"  📦 INVENTAIRE DONNÉES SAHAR")
    print(f"{'='*70}\n")

    for dtype in ["processed", "cache", "raw"]:
        subset = inv[inv["type"] == dtype]
        if not subset.empty:
            print(f"  [{dtype.upper()}]")
            for _, row in subset.iterrows():
                lignes = f" — {row['nb_lignes']:,} lignes" if row.get("nb_lignes") else ""
                print(f"    {row['source']:8s}  {row['fichier']:40s}  {row['taille_mo']:6.1f} Mo{lignes}  ({row['date_modif']})")
            print()

    total = inv["taille_mo"].sum()
    print(f"  Total : {len(inv)} fichiers — {total:.1f} Mo")

    fresh = catalog.freshness()
    if fresh:
        print(f"\n  Fraîcheur :")
        for src, info in fresh.items():
            print(f"    {src:8s} — dernière MAJ : {info['derniere_maj']} — {info['nb_fichiers']} fichier(s)")

    print(f"{'='*70}\n")


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="SAHAR — Collecte multi-sources via API")
    p.add_argument("--dept", nargs="+", help="Codes département (ex: 33 75 69)")
    p.add_argument("--sources", nargs="+",
                   choices=["dvf", "dpe", "geo", "sirene", "irve"],
                   help="Sources à collecter (défaut: dvf dpe geo)")
    p.add_argument("--all-sources", action="store_true", help="Toutes les sources")
    p.add_argument("--sirene-token", help="Token API INSEE pour SIRENE")
    p.add_argument("--annee-min", type=int, default=2022, help="Année min DVF (défaut 2022)")
    p.add_argument("--dpe-max", type=int, default=20000, help="Max résultats DPE (défaut 20000)")
    p.add_argument("--inventory", action="store_true", help="Afficher l'inventaire local")
    args = p.parse_args()

    if args.inventory:
        show_inventory()
        return

    depts = args.dept or ["33"]
    if args.all_sources:
        sources = ["dvf", "dpe", "geo", "sirene", "irve"]
    else:
        sources = args.sources or ["dvf", "dpe", "geo"]

    journal = {
        "ok": [],
        "err": [],
        "debut": str(datetime.now()),
        "departements": depts,
        "sources": sources,
    }

    print(f"""
╔══════════════════════════════════════════════════╗
║   SAHAR Conseil — Collecte données API          ║
╠══════════════════════════════════════════════════╣
║  Départements : {', '.join(depts):32s} ║
║  Sources      : {', '.join(sources):32s} ║
║  DVF année ≥  : {str(args.annee_min):32s} ║
╚══════════════════════════════════════════════════╝
""")

    debut = time.time()

    collectors = {
        "dvf": lambda d: collect_dvf(d, journal, annee_min=args.annee_min),
        "dpe": lambda d: collect_dpe(d, journal, max_results=args.dpe_max),
        "geo": lambda d: collect_geo(d, journal),
        "sirene": lambda d: collect_sirene(d, journal, token=args.sirene_token),
        "irve": lambda d: collect_irve(d, journal),
    }

    for i, dept in enumerate(depts, 1):
        print(f"\n{'─'*50}")
        print(f"  [{i}/{len(depts)}] Département {dept}")
        print(f"{'─'*50}")

        for source in sources:
            collector = collectors.get(source)
            if collector:
                try:
                    collector(dept)
                except Exception as e:
                    log(f"{source} dept {dept} — ERREUR : {e}", "err")
                    journal["err"].append(f"{source}_{dept}_{str(e)[:50]}")
                print()

        if i < len(depts):
            time.sleep(2)

    journal["fin"] = str(datetime.now())
    journal["duree_min"] = round((time.time() - debut) / 60, 1)
    save_log(journal)

    print(f"""
╔══════════════════════════════════════════════════╗
║  ✅ COLLECTE TERMINÉE                            ║
╠══════════════════════════════════════════════════╣
║  Durée       : {journal['duree_min']:.1f} min{' '*(26-len(f"{journal['duree_min']:.1f}"))}║
║  Réussites   : {len(journal['ok'])}{' '*(33-len(str(len(journal['ok']))))}║
║  Erreurs     : {len(journal['err'])}{' '*(33-len(str(len(journal['err']))))}║
║  Log         : collect_log.json                  ║
╚══════════════════════════════════════════════════╝
""")

    if journal["err"]:
        log(f"Erreurs détaillées : {journal['err']}", "warn")

    # Afficher l'inventaire à la fin
    show_inventory()


if __name__ == "__main__":
    main()
