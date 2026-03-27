"""
SAHAR Conseil — Content Factory
Pipeline orchestrateur : lance la chaîne complète

Usage :
  python run_pipeline.py                  # lance tout le pipeline
  python run_pipeline.py --keyword "DVF"  # un seul mot-clé
  python run_pipeline.py --from-step 3   # reprendre depuis une étape
  python run_pipeline.py --dry-run        # simulation sans API
  python run_pipeline.py --report         # rapport seul

Étapes :
  1. Research (SERP + PAA)
  2. Trends (Google Trends)
  3. Score (intérêt éditorial)
  4. Write (Claude API)
  5. Check (naturalité anti-IA)
  6. Audio (ElevenLabs)
  7. Publish (GitHub Pages + GA4)

Variables d'environnement requises selon les étapes :
  VALUESERP_KEY         (optionnel — fallback DuckDuckGo)
  ANTHROPIC_API_KEY     (requis étapes 4 et 5)
  ELEVENLABS_API_KEY    (requis étape 6)
  GITHUB_TOKEN          (requis étape 7)
  GA4_API_SECRET        (optionnel étape 7)
"""

import sys
import json
import argparse
import time
from pathlib import Path
from datetime import datetime

HERE = Path(__file__).parent

# Ajouter le dossier modules au path
sys.path.insert(0, str(HERE / "modules"))

import importlib

def load_module(name):
    return importlib.import_module(name)


def run_pipeline(
    keyword:    str  = None,
    from_step:  int  = 1,
    dry_run:    bool = False,
    script_only: bool = False,
    max_articles: int = 3,
) -> dict:
    """Lance le pipeline complet ou partiel."""

    start = datetime.now()
    print(f"\n{'='*60}")
    print(f"SAHAR Content Factory — Pipeline")
    print(f"Démarrage : {start.strftime('%d/%m/%Y %H:%M')}")
    print(f"Mode : {'dry-run' if dry_run else 'production'}")
    if keyword:
        print(f"Keyword : {keyword}")
    print(f"{'='*60}\n")

    results = {
        "start":     start.isoformat(),
        "keyword":   keyword,
        "steps":     {},
        "errors":    [],
        "published": [],
    }

    # ── Étape 1 : Research ───────────────────────────────────────────────────
    if from_step <= 1:
        print("📡 Étape 1 / 7 — Research SERP + PAA")
        try:
            m01 = load_module("01_research")
            if keyword:
                r = m01.research_keyword(keyword)
                research_results = [r] if r else []
            else:
                research_results = m01.research_all(max_keywords=max_articles * 2)
            results["steps"]["research"] = {"ok": True, "count": len(research_results)}
            print(f"   ✅ {len(research_results)} keywords recherchés\n")
        except Exception as e:
            print(f"   ❌ Research error : {e}")
            results["errors"].append(f"research: {e}")
            research_results = []

        time.sleep(1)

    # ── Étape 2 : Trends ─────────────────────────────────────────────────────
    if from_step <= 2:
        print("📈 Étape 2 / 7 — Tendances Google Trends")
        try:
            m02 = load_module("02_trends")
            m02.enrich_all()
            results["steps"]["trends"] = {"ok": True}
            print(f"   ✅ Enrichissement tendances terminé\n")
        except Exception as e:
            print(f"   ⚠️  Trends error (non bloquant) : {e}")
            results["errors"].append(f"trends: {e}")

        time.sleep(2)

    # ── Étape 3 : Scoring ────────────────────────────────────────────────────
    if from_step <= 3:
        print("🎯 Étape 3 / 7 — Scoring")
        try:
            m03 = load_module("03_scorer")
            scored = m03.score_all()
            to_publish = [s for s in scored if s["decision"] == "PUBLIER"]
            results["steps"]["scoring"] = {
                "ok":           True,
                "total":        len(scored),
                "to_publish":   len(to_publish),
                "avg_score":    int(sum(s["score"] for s in scored) / max(len(scored), 1)),
            }
            print(f"   ✅ {len(to_publish)}/{len(scored)} keywords retenus\n")
        except Exception as e:
            print(f"   ❌ Scoring error : {e}")
            results["errors"].append(f"scoring: {e}")

    # ── Étape 4 : Writing ────────────────────────────────────────────────────
    if from_step <= 4:
        print("✍️  Étape 4 / 7 — Rédaction articles (Claude API)")
        if dry_run:
            print("   ⏭  Dry-run — rédaction ignorée\n")
        else:
            try:
                m04 = load_module("04_writer")
                m04.generate_all(dry_run=False)
                results["steps"]["writing"] = {"ok": True}
                print(f"   ✅ Rédaction terminée\n")
            except Exception as e:
                print(f"   ❌ Writing error : {e}")
                results["errors"].append(f"writing: {e}")

        time.sleep(1)

    # ── Étape 5 : Check naturalité ───────────────────────────────────────────
    if from_step <= 5:
        print("🔍 Étape 5 / 7 — Check naturalité (anti-IA)")
        if dry_run:
            print("   ⏭  Dry-run — check ignoré\n")
        else:
            try:
                m05 = load_module("05_checker")
                m05.check_all()
                results["steps"]["checker"] = {"ok": True}
                print(f"   ✅ Vérification terminée\n")
            except Exception as e:
                print(f"   ⚠️  Checker error (non bloquant) : {e}")
                results["errors"].append(f"checker: {e}")

    # ── Étape 6 : Audio ──────────────────────────────────────────────────────
    if from_step <= 6:
        print("🎙️  Étape 6 / 7 — Génération audio podcast (ElevenLabs)")
        if dry_run:
            print("   ⏭  Dry-run — audio ignoré\n")
        else:
            try:
                m06 = load_module("06_audio")
                m06.process_all(script_only=script_only)
                results["steps"]["audio"] = {"ok": True}
                print(f"   ✅ Audio terminé\n")
            except Exception as e:
                print(f"   ⚠️  Audio error (non bloquant) : {e}")
                results["errors"].append(f"audio: {e}")

    # ── Étape 7 : Publication ────────────────────────────────────────────────
    if from_step <= 7:
        print("🚀 Étape 7 / 7 — Publication GitHub Pages")
        if dry_run:
            print("   ⏭  Dry-run — publication ignorée\n")
        else:
            try:
                m07 = load_module("07_publisher")
                m07.publish_all()
                results["steps"]["publisher"] = {"ok": True}
                print(f"   ✅ Publication terminée\n")
            except Exception as e:
                print(f"   ❌ Publisher error : {e}")
                results["errors"].append(f"publisher: {e}")

    # ── Résumé ───────────────────────────────────────────────────────────────
    duration = (datetime.now() - start).seconds
    results["duration_seconds"] = duration
    results["end"] = datetime.now().isoformat()

    print(f"{'='*60}")
    print(f"Pipeline terminé en {duration}s")
    print(f"Étapes OK  : {len([s for s in results['steps'].values() if s.get('ok')])}")
    print(f"Erreurs    : {len(results['errors'])}")
    if results["errors"]:
        for e in results["errors"]:
            print(f"  ⚠️  {e}")
    print(f"{'='*60}\n")

    # Sauvegarder le rapport de run
    report_path = HERE / "output" / f"run_{start.strftime('%Y%m%d_%H%M')}.json"
    report_path.parent.mkdir(exist_ok=True)
    report_path.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    print(f"Rapport : {report_path.name}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAHAR Content Factory — Pipeline complet")
    parser.add_argument("--keyword",     type=str, help="Mot-clé unique")
    parser.add_argument("--from-step",   type=int, default=1, help="Reprendre depuis étape N (1-7)")
    parser.add_argument("--dry-run",     action="store_true", help="Simulation sans API")
    parser.add_argument("--script-only", action="store_true", help="Script podcast sans audio")
    parser.add_argument("--max",         type=int, default=3, help="Max articles par run")
    parser.add_argument("--report",      action="store_true", help="Afficher rapport sans lancer")
    args = parser.parse_args()

    if args.report:
        # Afficher le dernier rapport
        reports = sorted((HERE / "output").glob("run_*.json"), reverse=True)
        if reports:
            data = json.loads(reports[0].read_text())
            print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            print("Aucun rapport trouvé. Lancez le pipeline d'abord.")
    else:
        run_pipeline(
            keyword=    args.keyword,
            from_step=  args.from_step,
            dry_run=    args.dry_run,
            script_only=args.script_only,
            max_articles=args.max,
        )
