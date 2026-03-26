"""
SAHAR Conseil — download_dvf.py v4
Téléchargement DVF depuis files.data.gouv.fr/geo-dvf

URLs :
  https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/departements/{dept}.csv.gz

UTILISATION :
  python download_dvf.py --dept 33
  python download_dvf.py --dept 33 75 69 13 31
  python download_dvf.py --dept 33 --annees 2023 2024
  python download_dvf.py --all
  python download_dvf.py --resume
"""

import requests
import gzip
import time
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

# ── CONSTANTES ────────────────────────────────────────────────────────────────

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"
ANNEES_DISPO = [2020, 2021, 2022, 2023, 2024, 2025]
RETRIES = 3

DEPARTEMENTS = (
    [str(i).zfill(2) for i in range(1, 96) if i != 20]
    + ["2A", "2B"]
)

HERE = Path(__file__).parent
RAW  = HERE / "raw"
LOG  = HERE / "download_log.json"


# ── UTILITAIRES ───────────────────────────────────────────────────────────────

def ts():
    return datetime.now().strftime("%H:%M:%S")

def afficher(msg, niveau=""):
    icones = {"ok": "OK ", "err": "ERR", "skip": "---", "warn": "!!!"}
    prefixe = icones.get(niveau, "   ")
    print(f"[{ts()}] {prefixe} {msg}")

def taille_fichier(path):
    s = path.stat().st_size
    for u in ["o", "Ko", "Mo", "Go"]:
        if s < 1024:
            return f"{s:.0f} {u}"
        s /= 1024
    return f"{s:.1f} Go"

def charger_log():
    if LOG.exists():
        return json.loads(LOG.read_text(encoding="utf-8"))
    return {"ok": [], "err": [], "debut": str(datetime.now())}

def sauver_log(journal):
    LOG.write_text(json.dumps(journal, indent=2, ensure_ascii=False), encoding="utf-8")


def telecharger_url(url, nb_essais=RETRIES):
    """Télécharge une URL avec retry. Retourne (contenu_bytes, status)."""
    for essai in range(1, nb_essais + 1):
        try:
            r = requests.get(url, timeout=120, stream=True)
            if r.status_code == 404:
                return None, 404
            r.raise_for_status()
            contenu = b""
            for chunk in r.iter_content(chunk_size=65536):
                contenu += chunk
            return contenu, 200
        except requests.Timeout:
            afficher(f"Timeout essai {essai}/{nb_essais}", "warn")
        except requests.RequestException as e:
            afficher(f"Erreur réseau essai {essai}/{nb_essais} : {e}", "warn")
        if essai < nb_essais:
            time.sleep(3 * essai)
    return None, 0


# ── TÉLÉCHARGEMENT ────────────────────────────────────────────────────────────

def telecharger_dept_annee(dept, annee, journal, delai, force):
    """Télécharge et consolide une année pour un département."""
    cle = f"{dept}_{annee}"

    if not force and cle in journal["ok"]:
        afficher(f"Deja fait : dept {dept} annee {annee}", "skip")
        return True

    url = f"{BASE_URL}/{annee}/departements/{dept}.csv.gz"
    afficher(f"Telechargement dept {dept} annee {annee} ...")
    afficher(f"URL : {url}")

    contenu_gz, status = telecharger_url(url)

    if status == 404 or contenu_gz is None:
        afficher(f"Fichier introuvable dept {dept} {annee} (HTTP {status})", "warn")
        return False

    # Décompresser
    try:
        contenu_csv = gzip.decompress(contenu_gz).decode("utf-8", errors="replace")
    except Exception as e:
        afficher(f"Erreur decompression : {e}", "err")
        return False

    # Consolider dans dvf_{dept}.csv
    RAW.mkdir(parents=True, exist_ok=True)
    dest = RAW / f"dvf_{dept}.csv"

    lignes = contenu_csv.splitlines()
    nb_lignes = len(lignes) - 1  # sans header

    if not dest.exists():
        dest.write_text(contenu_csv, encoding="utf-8")
    else:
        with open(dest, "a", encoding="utf-8") as f:
            f.write("\n")
            f.write("\n".join(lignes[1:]))

    afficher(f"dept {dept} {annee} : {nb_lignes:,} lignes -> {dest.name} ({taille_fichier(dest)})", "ok")
    journal["ok"].append(cle)
    time.sleep(delai)
    return True


def traiter_departement(dept, annees, journal, delai, force):
    """Traite toutes les années pour un département."""
    print(f"\n{'='*55}")
    print(f"  Departement {dept} — {len(annees)} annee(s) : {annees}")
    print(f"{'='*55}")

    dest = RAW / f"dvf_{dept}.csv"
    if force and dest.exists():
        dest.unlink()
        afficher(f"Fichier existant supprime : {dest.name}")

    succes = 0
    for annee in annees:
        ok = telecharger_dept_annee(dept, annee, journal, delai, force)
        if ok:
            succes += 1
        sauver_log(journal)

    if dest.exists():
        afficher(f"Fichier final : {dest.name} — {taille_fichier(dest)}", "ok")

    return succes


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Telecharge les DVF depuis data.gouv.fr"
    )
    p.add_argument("--dept",   nargs="+", help="Codes departement ex: 33 75 69")
    p.add_argument("--all",    action="store_true", help="Tous les departements")
    p.add_argument("--annees", nargs="+", type=int,  help="Annees ex: 2023 2024")
    p.add_argument("--resume", action="store_true",  help="Reprendre sans re-telecharger")
    p.add_argument("--force",  action="store_true",  help="Forcer le re-telechargement")
    p.add_argument("--delai",  type=float, default=2.0, help="Secondes entre requetes (defaut 2)")
    args = p.parse_args()

    delai  = args.delai
    depts  = DEPARTEMENTS if args.all else (args.dept or ["33"])
    annees = args.annees or ANNEES_DISPO

    invalides = [d for d in depts if d not in DEPARTEMENTS]
    if invalides:
        print(f"Departements invalides : {invalides}")
        sys.exit(1)

    journal = charger_log() if args.resume else {
        "ok": [], "err": [], "debut": str(datetime.now())
    }

    print(f"""
+------------------------------------------+
|   SAHAR Conseil - Telechargement DVF     |
+------------------------------------------+
  Departements : {depts}
  Annees       : {annees}
  Delai        : {delai}s
  Destination  : data/raw/
+------------------------------------------+
""")

    debut = time.time()
    total_ok = 0

    for i, dept in enumerate(depts, 1):
        print(f"\n[{i}/{len(depts)}]", end=" ")
        ok = traiter_departement(dept, annees, journal, delai, args.force)
        total_ok += ok
        if i < len(depts):
            afficher("Pause 5s ...")
            time.sleep(5)

    duree = (time.time() - debut) / 60
    print(f"""
+------------------------------------------+
  TERMINE en {duree:.1f} min
  Telechargements OK : {total_ok}
  Journal : download_log.json
+------------------------------------------+
""")
    if journal["err"]:
        afficher(f"Erreurs : {journal['err']}", "warn")


if __name__ == "__main__":
    main()
