"""
SAHAR Conseil — download_dvf.py
Téléchargement automatique et progressif de tous les fichiers DVF
disponibles sur data.gouv.fr (geo-dvf).

UTILISATION :
    python download_dvf.py                    # tout télécharger
    python download_dvf.py --dept 33          # un département
    python download_dvf.py --dept 33 75 69    # plusieurs départements
    python download_dvf.py --annee 2023       # une année spécifique
    python download_dvf.py --resume           # reprendre où on s'est arrêté

STRUCTURE TÉLÉCHARGÉE :
    data/raw/dvf/
    ├── 2019/
    │   ├── dvf_01_2019.csv
    │   ├── dvf_33_2019.csv
    │   └── ...
    ├── 2020/
    ├── 2021/
    ├── 2022/
    ├── 2023/
    ├── 2024/
    └── dvf_33.csv   ← fichier consolidé (toutes années) par département

PARAMÈTRES :
    DELAI_ENTRE_REQUETES : secondes entre chaque téléchargement (défaut 2s)
    MAX_RETRIES          : tentatives en cas d'échec (défaut 3)
"""

import requests
import time
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

# ─── CONFIGURATION ───────────────────────────────────────────────────────────

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"

# Années disponibles sur geo-dvf
ANNEES_DISPONIBLES = [2019, 2020, 2021, 2022, 2023, 2024]

# Tous les départements métropolitains + DOM
DEPARTEMENTS = (
    [str(i).zfill(2) for i in range(1, 96) if i != 20]
    + ["2A", "2B"]
    + ["971", "972", "973", "974", "976"]
)

# Délai entre requêtes pour ne pas surcharger data.gouv.fr
DELAI_ENTRE_REQUETES = 2.0   # secondes

# Délai entre départements (plus long pour étaler la charge)
DELAI_ENTRE_DEPTS = 5.0

MAX_RETRIES = 3

# Dossier de destination
DATA_DIR = Path(__file__).parent / "raw" / "dvf"
LOG_FILE = Path(__file__).parent / "download_log.json"

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def log(message: str, niveau: str = "INFO"):
    """Affiche un message avec horodatage."""
    now = datetime.now().strftime("%H:%M:%S")
    prefix = {"INFO": "ℹ️ ", "OK": "✅", "ERR": "❌", "SKIP": "⏭️ ", "WARN": "⚠️ "}.get(niveau, "")
    print(f"[{now}] {prefix} {message}")


def charger_log() -> dict:
    """Charge le journal des téléchargements pour le mode reprise."""
    if LOG_FILE.exists():
        return json.loads(LOG_FILE.read_text())
    return {"telechargés": [], "erreurs": [], "debut": str(datetime.now())}


def sauver_log(journal: dict):
    """Sauvegarde le journal."""
    LOG_FILE.write_text(json.dumps(journal, indent=2, ensure_ascii=False))


def taille_lisible(octets: int) -> str:
    """Convertit des octets en taille lisible."""
    for unit in ["o", "Ko", "Mo", "Go"]:
        if octets < 1024:
            return f"{octets:.1f} {unit}"
        octets /= 1024
    return f"{octets:.1f} To"


def telecharger_fichier(url: str, destination: Path, retries: int = MAX_RETRIES) -> bool:
    """
    Télécharge un fichier avec retry automatique.
    Retourne True si succès, False sinon.
    """
    for tentative in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=120, stream=True)

            if response.status_code == 404:
                return False  # Fichier inexistant, pas une erreur

            response.raise_for_status()

            # Téléchargement par chunks pour les gros fichiers
            destination.parent.mkdir(parents=True, exist_ok=True)
            total = 0
            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=65536):
                    f.write(chunk)
                    total += len(chunk)

            # Vérifier que le fichier n'est pas vide ou trop petit
            if total < 500:
                destination.unlink(missing_ok=True)
                return False

            return True

        except requests.Timeout:
            log(f"Timeout (tentative {tentative}/{retries}) : {url}", "WARN")
        except requests.RequestException as e:
            log(f"Erreur réseau (tentative {tentative}/{retries}) : {e}", "WARN")

        if tentative < retries:
            time.sleep(DELAI_ENTRE_REQUETES * tentative)

    return False


# ─── TÉLÉCHARGEMENT PAR ANNÉE ─────────────────────────────────────────────

def telecharger_annee(dept: str, annee: int, journal: dict, force: bool = False) -> bool:
    """
    Télécharge le fichier DVF d'un département pour une année donnée.
    Utilise le fichier full.csv (toutes communes du département).
    """
    cle = f"{dept}_{annee}"
    destination = DATA_DIR / str(annee) / f"dvf_{dept}_{annee}.csv"

    # Déjà téléchargé ?
    if not force and cle in journal["telechargés"]:
        log(f"Déjà téléchargé : dept {dept} — {annee}", "SKIP")
        return True

    if destination.exists() and destination.stat().st_size > 500 and not force:
        log(f"Fichier existant : {destination.name}", "SKIP")
        journal["telechargés"].append(cle)
        return True

    # URLs à essayer dans l'ordre
    urls = [
        f"https://files.data.gouv.fr/geo-dvf/latest/csv/{dept}/full.csv",
        # Note : geo-dvf ne propose pas de découpage par année directement
        # On prend le fichier complet et on filtre par année ensuite
    ]

    # Pour les années historiques, essayer DVF+ (Etalab)
    urls_historiques = [
        f"https://files.data.gouv.fr/geo-dvf/latest/csv/{dept}/full.csv",
    ]

    log(f"Téléchargement dept {dept} — {annee}...")

    for url in urls:
        if telecharger_fichier(url, destination):
            taille = taille_lisible(destination.stat().st_size)
            log(f"OK dept {dept} — {annee} ({taille})", "OK")
            journal["telechargés"].append(cle)
            return True

    log(f"Échec dept {dept} — {annee}", "ERR")
    if cle not in journal["erreurs"]:
        journal["erreurs"].append(cle)
    return False


def telecharger_full_dept(dept: str, journal: dict, force: bool = False) -> bool:
    """
    Télécharge le fichier complet (toutes années) pour un département.
    C'est le fichier principal utilisé par l'app DVF.
    """
    cle = f"{dept}_full"
    destination = DATA_DIR.parent / f"dvf_{dept}.csv"

    if not force and destination.exists() and destination.stat().st_size > 500:
        taille = taille_lisible(destination.stat().st_size)
        log(f"Fichier full existant : dvf_{dept}.csv ({taille})", "SKIP")
        if cle not in journal["telechargés"]:
            journal["telechargés"].append(cle)
        return True

    url = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{dept}/full.csv"
    log(f"Téléchargement full dept {dept}...")

    if telecharger_fichier(url, destination):
        taille = taille_lisible(destination.stat().st_size)
        log(f"OK dvf_{dept}.csv ({taille})", "OK")
        journal["telechargés"].append(cle)
        return True
    else:
        log(f"Fichier full indisponible pour dept {dept} — essai par commune...", "WARN")
        return telecharger_par_communes(dept, journal, force)


def telecharger_par_communes(dept: str, journal: dict, force: bool = False) -> bool:
    """
    Fallback : télécharge les fichiers par commune si le full.csv n'existe pas.
    Utilisé pour les DOM et certains départements.
    """
    import csv
    import io

    # Récupérer la liste des communes du département
    url_index = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{dept}/"

    try:
        r = requests.get(url_index, timeout=30)
        if r.status_code != 200:
            return False

        # Parser les liens CSV dans la page HTML
        communes = []
        for ligne in r.text.split("\n"):
            if ".csv" in ligne and dept in ligne:
                # Extraire le code commune (5 chiffres)
                import re
                codes = re.findall(r'(\d{5})\.csv', ligne)
                communes.extend(codes)

        if not communes:
            return False

        log(f"Dept {dept} : {len(communes)} communes trouvées")
        tous_ok = True

        for i, commune in enumerate(communes):
            cle_commune = f"{dept}_{commune}_commune"
            dest = DATA_DIR / "communes" / dept / f"{commune}.csv"

            if not force and dest.exists() and dest.stat().st_size > 100:
                continue

            url_commune = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{dept}/communes/{commune}.csv"
            if telecharger_fichier(url_commune, dest):
                log(f"  Commune {commune} ({i+1}/{len(communes)})", "OK")
            else:
                tous_ok = False

            time.sleep(DELAI_ENTRE_REQUETES)

        return tous_ok

    except Exception as e:
        log(f"Erreur liste communes dept {dept} : {e}", "ERR")
        return False


# ─── BOUCLE PRINCIPALE ────────────────────────────────────────────────────

def run(departements: list, annees: list, resume: bool, force: bool):
    """Lance le téléchargement progressif."""

    journal = charger_log() if resume else {
        "telechargés": [], "erreurs": [], "debut": str(datetime.now())
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    total_depts = len(departements)
    total_annees = len(annees)
    debut_global = time.time()

    log("=" * 60)
    log(f"SAHAR Conseil — Téléchargement DVF")
    log(f"Départements : {total_depts} | Années : {annees}")
    log(f"Délai entre requêtes : {DELAI_ENTRE_REQUETES}s")
    log(f"Destination : {DATA_DIR.parent.resolve()}")
    log("=" * 60)

    succes = 0
    echecs = 0

    for i, dept in enumerate(departements, 1):
        log(f"\n[{i}/{total_depts}] Département {dept}")

        # 1. Télécharger le fichier full (toutes années — utilisé par l'app)
        if telecharger_full_dept(dept, journal, force):
            succes += 1
        else:
            echecs += 1

        sauver_log(journal)
        time.sleep(DELAI_ENTRE_DEPTS)

    # Bilan
    duree = time.time() - debut_global
    log("\n" + "=" * 60)
    log(f"TERMINÉ en {duree/60:.1f} minutes")
    log(f"Succès : {succes} | Échecs : {echecs}")
    log(f"Journal : {LOG_FILE}")

    if journal["erreurs"]:
        log(f"Fichiers en erreur : {journal['erreurs']}", "WARN")
        log("Relancer avec --resume pour réessayer les échecs.")

    log("=" * 60)


# ─── CLI ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Télécharge les fichiers DVF depuis data.gouv.fr",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python download_dvf.py                        # Tous les départements
  python download_dvf.py --dept 33              # Gironde uniquement
  python download_dvf.py --dept 33 75 69 13     # Plusieurs départements
  python download_dvf.py --resume               # Reprendre après interruption
  python download_dvf.py --dept 33 --force      # Retélécharger même si existant
  python download_dvf.py --delai 5              # 5 secondes entre requêtes (plus lent)
        """
    )

    parser.add_argument(
        "--dept", nargs="+", default=None,
        help="Code(s) département (ex: 33 ou 33 75 69). Défaut: tous."
    )
    parser.add_argument(
        "--annee", nargs="+", type=int, default=None,
        help="Année(s) à télécharger (ex: 2023). Défaut: toutes."
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Reprendre le téléchargement en ignorant les fichiers déjà téléchargés."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Retélécharger même si le fichier existe déjà."
    )
    parser.add_argument(
        "--delai", type=float, default=DELAI_ENTRE_REQUETES,
        help=f"Délai en secondes entre requêtes (défaut: {DELAI_ENTRE_REQUETES})"
    )
    parser.add_argument(
        "--liste", action="store_true",
        help="Afficher les départements disponibles et quitter."
    )

    args = parser.parse_args()

    if args.liste:
        print("Départements disponibles :")
        print(", ".join(DEPARTEMENTS))
        sys.exit(0)

    # Ajuster le délai global
    global DELAI_ENTRE_REQUETES
    DELAI_ENTRE_REQUETES = args.delai

    departements = args.dept if args.dept else DEPARTEMENTS
    annees = args.annee if args.annee else ANNEES_DISPONIBLES

    # Vérifier les départements
    invalides = [d for d in departements if d not in DEPARTEMENTS]
    if invalides:
        print(f"❌ Départements invalides : {invalides}")
        print(f"   Valides : {DEPARTEMENTS}")
        sys.exit(1)

    run(departements, annees, args.resume, args.force)


if __name__ == "__main__":
    main()
