"""
SAHAR Conseil — Content Factory
Pipeline complet : research → trends → score → write → check → audio → publish

Usage :
  python run_pipeline.py                    # traite tous les seeds
  python run_pipeline.py --keyword "dvf"    # un seul mot-clé
  python run_pipeline.py --skip-audio       # sans génération podcast
  python run_pipeline.py --dry-run          # jusqu'au score, sans écrire

Variables d'environnement requises :
  ANTHROPIC_API_KEY     (modules 04, 05, 06)
  VALUESERP_KEY         (module 01 — optionnel, fallback DDG)
  ELEVENLABS_API_KEY    (module 06 — optionnel)
  GITHUB_TOKEN          (module 07)
  GA4_API_SECRET        (module 07 — optionnel)
"""

import sys
import json
import argparse
import time
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE / "modules"))

import importlib
m01 = importlib.import_module("01_research")
m02 = importlib.import_module("02_trends")
m03 = importlib.import_module("03_scorer")
m04 = importlib.import_module("04_writer")
m05 = importlib.import_module("05_checker")
m06 = importlib.import_module("06_audio")
m07 = importlib.import_module("07_publisher")

CFG      = json.loads((HERE / "config" / "keywords.json").read_text())
SETTINGS = CFG.get("settings", {})
MAX_ART  = SETTINGS.get("max_articles_par_run", 3)


def run_single(keyword: str, secteur: str = "", cible: str = "",
               url_cible: str = "index.html", skip_audio: bool = False,
               dry_run: bool = False) -> dict | None:
    """Pipeline complet pour un mot-clé."""

    print(f"\n{'='*60}")
    print(f"  PIPELINE : {keyword}")
    print(f"{'='*60}")

    # 01 — Research
    data = m01.research_keyword(keyword, use_api=True)
    if not data:
        print("  ❌ Research échoué — abandon")
        return None

    # Ajouter métadonnées seed
    data.update({"secteur": secteur, "cible": cible, "url_cible": url_cible})
    slug = data.get("slug", "")

    # 02 — Trends
    m02.enrich_research_file(slug)
    time.sleep(1.5)

    # Recharger après enrichissement
    out = HERE / "output" / "research" / f"{slug}.json"
    data = json.loads(out.read_text())

    # 03 — Score
    score = m03.compute_score(data)
    data["score"] = score
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2))

    if score["decision"] == "skip":
        print(f"\n  ⏭  Score {score['total']}/100 < {score['seuil']} — article ignoré")
        return None

    if dry_run:
        print(f"\n  🧪 Dry run — arrêt avant génération (score: {score['total']}/100)")
        return data

    # 04 — Write
    article = m04.generate_article(data)
    if not article:
        return None
    time.sleep(2)

    # 05 — Check IA
    article = m05.check_article(article["meta"]["slug"])
    if not article:
        return None

    # 06 — Audio (optionnel)
    if not skip_audio:
        m06.generate_podcast(article["meta"]["slug"])

    # 07 — Publish
    m07.publish_article(article["meta"]["slug"])

    print(f"\n  ✅ Pipeline terminé : {keyword}")
    return article


def run_all(skip_audio: bool = False, dry_run: bool = False,
            max_articles: int = MAX_ART) -> list:
    """Lance le pipeline sur tous les seeds."""

    keywords = sorted(
        CFG.get("keywords", []),
        key=lambda x: x.get("priorite", 99)
    )

    print(f"\n🚀 SAHAR Content Factory — {len(keywords)} seeds")
    print(f"   Max articles : {max_articles}")
    print(f"   Audio        : {'non' if skip_audio else 'oui'}")
    print(f"   Dry run      : {'oui' if dry_run else 'non'}")

    done = []
    for kw in keywords:
        if len(done) >= max_articles:
            break

        result = run_single(
            keyword   = kw["seed"],
            secteur   = kw.get("secteur", ""),
            cible     = kw.get("cible", ""),
            url_cible = kw.get("url_cible", "index.html"),
            skip_audio = skip_audio,
            dry_run   = dry_run,
        )
        if result:
            done.append(result)
        time.sleep(3)

    print(f"\n{'='*60}")
    print(f"  TERMINÉ : {len(done)}/{len(keywords)} articles publiés")
    print(f"{'='*60}\n")
    return done


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAHAR Content Factory — Pipeline complet")
    parser.add_argument("--keyword",    type=str, help="Mot-clé unique")
    parser.add_argument("--secteur",    type=str, default="")
    parser.add_argument("--all",        action="store_true")
    parser.add_argument("--skip-audio", action="store_true")
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--max",        type=int, default=MAX_ART)
    args = parser.parse_args()

    if args.keyword:
        run_single(
            keyword    = args.keyword,
            secteur    = args.secteur,
            skip_audio = args.skip_audio,
            dry_run    = args.dry_run,
        )
    else:
        run_all(
            skip_audio  = args.skip_audio,
            dry_run     = args.dry_run,
            max_articles = args.max,
        )
