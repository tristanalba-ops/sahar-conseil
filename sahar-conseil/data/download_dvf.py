"""
SAHAR Conseil — download_dvf.py v3
Téléchargement DVF depuis files.data.gouv.fr/geo-dvf

Structure réelle de l'API (2025) :
  https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/departements/{dept}.csv.gz
  https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/communes/{dept}/{commune}.csv

Années disponibles : 2020, 2021, 2022, 2023, 2024, 2025

UTILISATION :
  python download_dvf.py --dept 33
  python download_dvf.py --dept 33 75 69 13 31
  python download_dvf.py --all
  python download_dvf.py --dept 33 --annees 2023 2024
  python download_dvf.py --resume
"""

import requests
import gzip
import time
import argparse
import json
import sys
import io
from pathlib import Path
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────

BASE = "https://files.data.gouv.fr/geo-dvf/latest/csv"
ANNEES = [2020, 2021, 2022, 2023, 2024, 2025]
DELAI = 2.0  # secondes entre requêtes
RETRIES = 3

DEPARTEMENTS = (
    [str(i).zfill(2) for i in range(1, 96) if i != 20]
    + ["2A", "2B"]
)

# Dossiers
HERE = Path(__file__).parent
RAW = HERE / "raw"
LOG = HERE / "download_log.json"

# ── UTILITAIRES ───────────────────────────────────────────────────────────────

def ts():
    return datetime.now().strftime("%H:%M:%S")

def log(msg, niveau=""):
    icones = {"ok": "✅", "err": "❌", "skip": "⏭ ", "warn": "⚠️ ", "": "ℹ️ "}
    print(f"[{ts()}] {icones.get(niveau, '')} {msg}")

def taille(path):
    s = path.stat().st_size
    for u in ["o", "Ko", "Mo", "Go"]:
        if s < 1024: return f"{s:.0f} {u}"
        s /= 1024
    return f"{s:.1f} Go"

def charger_log():
    if LOG.exists():
        return json.loads(LOG.read_text(encoding="utf-8"))
    return {"ok": [], "err": [], "debut": str(datetime.now())}

def sauver_log(j):
    LOG.write_text(json.dumps(j, indent=2, ensure_ascii=False), encoding="utf-8")

def get(url, retries=RETRIES):
    for i in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=120, stream=True)
            if r.status_code == 404:
                return None, 404
            r.raise_for_status()
            return r, r.status_code
        except requests.Timeout:
            log(f"Timeout tentative {i}/{retries}", "warn")
        except requests.RequestException as e:
            log(f"Erreur {e} tentative {i}/{retries}", "warn")
        if i < retries:
            time.sleep(DELAI * i)
    return None, 0

# ── TÉLÉCHARGEMENT ────────────────────────────────────────────────────────────

def telecharger_dept_annee(dept, annee, journal, force=False):
    """
    Télécharge le fichier .csv.gz d'un département pour une année,
    le décompresse et l'ajoute au CSV consolidé dvf_{dept}.csv
    """
    cle = f"{dept}_{annee}"
    if not force and cle in journal["ok"]:
        log(f"Déjà fait : {dept} {annee}", "skip")
        return True

    url = f"{BASE}/{annee}/departements/{dept}.csv.gz"
    log(f"Téléchargement dept {dept} — {annee} ...")

    r, status = get(url)

    if status == 404 or r is None:
        log(f"Introuvable : dept {dept} {annee} (HTTP {status})", "warn")
        return False

    # Décompresser le .gz en mémoire
    try:
        contenu_gz = r.content
        contenu_csv = gzip.decompress(contenu_gz).decode("utf-8", errors="replace")
    except Exception as e:
        log(f"Erreur décompression {dept} {annee} : {e}", "err")
        return False

    # Écrire / consolider dans dvf_{dept}.csv
    dest = RAW / f"dvf_{dept}.csv"
    RAW.mkdir(parents=True, exist_ok=True)

    lignes = contenu_csv.splitlines()
    header = lignes[0] if lignes else ""
    data_lignes = lignes[1:] if len(lignes) > 1 else []

    if not dest.exists():
        # Première année : écrire avec header
        dest.write_text(contenu_csv, encoding="utf-8")
    else:
        # Années suivantes : ajouter sans header
        with open(dest, "a", encoding="utf-8") as f:
            f.write("\n" + "\n".join(data_lignes))

    nb = len(data_lignes)
    log(f"OK dept {dept} {annee} — {nb:,} lignes ajoutées ({taille(dest)} total)", "ok")
    journal["ok"].append(cle)
    return True


def traiter_departement(dept, annees, journal, force=False):
    """Traite toutes les années pour un département."""
    log(f"\n{'='*50}")
    log(f"Département {dept} — {len(annees)} années")
    log(f"{'='*50}")

    # Supprimer le fichier consolidé si force
    dest = RAW / f"dvf_{dept}.csv"
    if force and dest.exists():
        dest.unlink()
        log(f"Fichier existant supprimé : {dest.name}", "warn")

    succes = 0
    for annee in annees:
        ok = telecharger_dept_annee(dept, annee, journal, force)
        if ok:
            succes += 1
        sauver_log(journal)
        time.sleep(DELAI)

    if dest.exists():
        log(f"Fichier final : {dest.name} ({taille(dest)})", "ok")
    return succes

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    global DELAI
    p = argparse.ArgumentParser(description="Télécharge les DVF depuis data.gouv.fr")
    p.add_argument("--dept", nargs="+", help="Codes département ex: 33 75 69")
    p.add_argument("--all", action="store_true", help="Tous les départements")
    p.add_argument("--annees", nargs="+", type=int, help=f"Années ex: 2023 2024. Défaut: {ANNEES}")
    p.add_argument("--resume", action="store_true", help="Reprendre sans re-télécharger")
    p.add_argument("--force", action="store_true", help="Forcer le re-téléchargement")
    p.add_argument("--delai", type=float, default=DELAI, help=f"Délai entre requêtes (défaut {DELAI}s)")
    args = p.parse_args()

    DELAI = args.delai

    depts = DEPARTEMENTS if args.all else (args.dept or ["33"])
    annees = args.annees or ANNEES

    invalides = [d for d in depts if d not in DEPARTEMENTS]
    if invalides:
        print(f"Départements invalides : {invalides}")
        sys.exit(1)

    journal = charger_log() if args.resume else {"ok": [], "err": [], "debut": str(datetime.now())}

    print(f"""
╔══════════════════════════════════════════╗
║   SAHAR Conseil — Téléchargement DVF    ║
╠══════════════════════════════════════════╣
║  Départements : {str(depts)[:30]:<26}║
║  Années       : {str(annees):<26}║
║  Délai        : {args.delai}s{'':<23}║
║  Destination  : data/raw/               ║
╚══════════════════════════════════════════╝
""")

    debut = time.time()
    total_ok = 0

    for i, dept in enumerate(depts, 1):
        print(f"\n[{i}/{len(depts)}]", end=" ")
        ok = traiter_departement(dept, annees, journal, args.force)
        total_ok += ok
        if i < len(depts):
            log(f"Pause 5s avant prochain département...")
            time.sleep(5)

    duree = (time.time() - debut) / 60
    print(f"""
╔══════════════════════════════════════════╗
║   TERMINÉ en {duree:.1f} min{'':<27}║
║   Fichiers OK : {total_ok:<26}║
║   Journal : download_log.json           ║
╚══════════════════════════════════════════╝
""")
    if journal["err"]:
        log(f"Erreurs : {journal['err']}", "warn")
        log("Relancer avec --resume pour réessayer.")

if __name__ == "__main__":
    main()
