"""
SAHAR Conseil — Content Factory Pipeline
Usage :
  python run_pipeline.py                    # Tous les seeds
  python run_pipeline.py --kw "DVF prix"   # Mot-clé spécifique
  python run_pipeline.py --dry-run         # Sans publication GitHub
  python run_pipeline.py --limit 3         # Max 3 mots-clés
"""
import argparse, json, logging, os, sys, time
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
log_file = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")],
)
logger = logging.getLogger("pipeline")

sys.path.insert(0, str(Path(__file__).parent))
from modules.m01_research import research
from modules.m02_trends import trends
from modules.m03_scorer import score
from modules.m04_writer import write
from modules.m05_checker import check_and_fix
from modules.m06_audio import generate_audio
from modules.m07_publisher import publish

KEYWORDS_FILE = Path(__file__).parent / "keywords.json"
OUTPUT_DIR = Path(__file__).parent / "output"


def load_keywords():
    return json.loads(KEYWORDS_FILE.read_text())


def run_single(seed, dry_run=False):
    kw = seed["kw"]
    logger.info(f"\n{'='*60}\nPIPELINE : {kw}\n{'='*60}")
    result = {"keyword": kw, "seed": seed, "steps": {}, "published": False}

    # 01 Research
    logger.info("[01] Research SERP + PAA...")
    try:
        serp = research(kw, api_key=os.getenv("SERP_API_KEY"))
        result["steps"]["research"] = {"ok": True, "organic": serp["organic_count"], "paa": serp["paa_count"]}
    except Exception as e:
        logger.error(f"Research: {e}")
        serp = {"organic": [], "paa": [], "related": [], "organic_count": 0, "paa_count": 0}
        result["steps"]["research"] = {"ok": False}
    time.sleep(1)

    # 02 Trends
    logger.info("[02] Tendances...")
    try:
        trends_data = trends(kw)
        result["steps"]["trends"] = {"ok": True, "score": trends_data["score_tendance"], "direction": trends_data["direction"]}
    except Exception as e:
        logger.warning(f"Trends: {e}")
        trends_data = {"score_tendance": 40, "direction": "stable"}
        result["steps"]["trends"] = {"ok": False}
    time.sleep(1)

    # 03 Score
    logger.info("[03] Score éditorial...")
    score_data = score(kw, serp, trends_data)
    result["score"] = score_data["score_total"]
    result["steps"]["score"] = {"ok": True, "total": score_data["score_total"], "decision": score_data["decision"]}

    if score_data["decision"] == "IGNORER":
        logger.info(f"  ⏭  Score {score_data['score_total']}/100 < seuil → IGNORÉ")
        result["skipped"] = True
        return result

    logger.info(f"  ✅ Score {score_data['score_total']}/100 → PUBLIER")

    # 04 Writer
    logger.info("[04] Génération article (Claude API)...")
    kw_config = load_keywords()
    try:
        article = write(kw, serp, trends_data, score_data, seed, kw_config)
        if "error" in article:
            raise ValueError(article["error"])
        result["steps"]["write"] = {"ok": True, "words": article.get("word_count_actual", 0), "slug": article.get("slug")}
        logger.info(f"  → '{article.get('h1')}' ({article.get('word_count_actual')} mots)")
    except Exception as e:
        logger.error(f"Write: {e}")
        result["steps"]["write"] = {"ok": False, "error": str(e)}
        return result

    # 05 Checker
    logger.info("[05] Checker naturalité...")
    try:
        article = check_and_fix(article)
        q = article.get("_quality", {})
        result["steps"]["check"] = {"ok": True, "score_naturalite": q.get("score_naturalite"), "passes": q.get("passes_reformulation")}
    except Exception as e:
        logger.warning(f"Checker: {e}")
        result["steps"]["check"] = {"ok": False}

    # 06 Audio
    logger.info("[06] Audio podcast...")
    slug = article.get("slug", "article")
    try:
        audio_data = generate_audio(article, slug)
        result["steps"]["audio"] = {"ok": True, "generated": audio_data.get("audio_generated")}
    except Exception as e:
        logger.warning(f"Audio: {e}")
        audio_data = {}
        result["steps"]["audio"] = {"ok": False}

    # 07 Publish
    if dry_run:
        logger.info("[07] DRY RUN — publication ignorée")
        result["steps"]["publish"] = {"ok": True, "dry_run": True}
        result["url"] = f"[dry-run] blog/{slug}.html"
    else:
        logger.info("[07] Publication GitHub...")
        try:
            pub = publish(article, audio_data)
            result["steps"]["publish"] = {"ok": True, "url": pub["url"], "github": pub["github_published"]}
            result["published"] = pub["github_published"]
            result["url"] = pub["url"]
        except Exception as e:
            logger.error(f"Publish: {e}")
            result["steps"]["publish"] = {"ok": False, "error": str(e)}

    return result


def run_all(limit=None, dry_run=False, specific_kw=None):
    config = load_keywords()
    seeds = config["seeds"]

    if specific_kw:
        seeds = [s for s in seeds if specific_kw.lower() in s["kw"].lower()]
        if not seeds:
            seeds = [{"kw": specific_kw, "secteur": "transversal", "url_cible": "index.html"}]
    if limit:
        seeds = seeds[:limit]

    logger.info(f"\n{'#'*60}\nSAHAR Content Factory — {len(seeds)} mot(s)-clé(s) | {'DRY RUN' if dry_run else 'PRODUCTION'}\n{'#'*60}\n")

    results, published, skipped, errors = [], 0, 0, 0
    for i, seed in enumerate(seeds):
        logger.info(f"\n[{i+1}/{len(seeds)}] {seed['kw']}")
        try:
            r = run_single(seed, dry_run=dry_run)
            results.append(r)
            if r.get("published"): published += 1
            elif r.get("skipped"): skipped += 1
            else: errors += 1
        except Exception as e:
            logger.error(f"Pipeline error '{seed['kw']}': {e}")
            errors += 1
        if i < len(seeds) - 1:
            time.sleep(3)

    logger.info(f"\n{'='*60}\nRAPPORT : {len(seeds)} traités | {published} publiés | {skipped} ignorés | {errors} erreurs\n{'='*60}\n")

    OUTPUT_DIR.mkdir(exist_ok=True)
    report = OUTPUT_DIR / f"report_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    report.write_text(json.dumps({"run_at": datetime.now().isoformat(), "dry_run": dry_run,
        "published": published, "skipped": skipped, "errors": errors, "results": results},
        ensure_ascii=False, indent=2, default=str))
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kw", type=str)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    run_all(limit=args.limit, dry_run=args.dry_run, specific_kw=args.kw)
