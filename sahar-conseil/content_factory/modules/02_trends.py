"""
SAHAR Conseil — Content Factory
Module 02 : Google Trends via PyTrends

Enrichit les données de research avec :
  - score tendance 0-100 (moyenne 12 mois)
  - direction : montante / stable / descendante
  - pic récent : est-ce qu'on est proche du pic ?
  - keywords associés tendance

Usage :
  python 02_trends.py --slug passoires-thermiques-interdites-location-2025
  python 02_trends.py --all
"""

import json
import time
import argparse
from pathlib import Path
from datetime import datetime

HERE    = Path(__file__).parent.parent
OUT_DIR = HERE / "output" / "research"

def get_trend_score(keyword: str) -> dict:
    """
    Récupère les tendances Google pour un mot-clé.
    Retourne un dict avec score, direction, pic, keywords associés.
    """
    try:
        from pytrends.request import TrendReq
        pt = TrendReq(hl='fr-FR', tz=60, timeout=(15, 45), retries=2, backoff_factor=0.5)
        pt.build_payload([keyword], cat=0, timeframe='today 12-m', geo='FR')

        interest = pt.interest_over_time()
        if interest.empty:
            return _empty_trend(keyword, "no_data")

        series   = interest[keyword].tolist()
        avg      = sum(series) / len(series)
        peak     = max(series)
        last_4   = series[-4:]   # ~dernier mois
        prev_4   = series[-8:-4] # ~mois précédent
        avg_last = sum(last_4) / len(last_4)
        avg_prev = sum(prev_4) / len(prev_4) if prev_4 else avg_last

        # Direction
        if avg_last > avg_prev * 1.15:
            direction = "montante"
        elif avg_last < avg_prev * 0.85:
            direction = "descendante"
        else:
            direction = "stable"

        # Proximité pic : sommes-nous dans les 20% hauts ?
        near_peak = avg_last >= peak * 0.8

        # Score final : moyenne pondérée (tendance récente compte plus)
        trend_score = min(100, int(avg * 0.6 + avg_last * 0.4))

        # Related queries
        related_kws = []
        try:
            time.sleep(1)
            related = pt.related_queries()
            top = related.get(keyword, {}).get("top")
            if top is not None and not top.empty:
                related_kws = top["query"].tolist()[:5]
        except Exception:
            pass

        return {
            "keyword":      keyword,
            "trend_score":  trend_score,
            "avg_12m":      round(avg, 1),
            "avg_last_4w":  round(avg_last, 1),
            "peak":         peak,
            "near_peak":    near_peak,
            "direction":    direction,
            "series":       series,
            "related_kws":  related_kws,
            "source":       "google_trends",
            "timestamp":    datetime.now().isoformat(),
        }

    except ImportError:
        return _empty_trend(keyword, "pytrends_not_installed")
    except Exception as e:
        print(f"  ⚠️  Trends erreur : {e}")
        return _empty_trend(keyword, str(e)[:80])


def _empty_trend(keyword: str, reason: str = "") -> dict:
    """Retourne un score neutre quand les trends ne sont pas disponibles."""
    return {
        "keyword":      keyword,
        "trend_score":  50,   # score neutre
        "avg_12m":      50,
        "avg_last_4w":  50,
        "peak":         100,
        "near_peak":    False,
        "direction":    "inconnu",
        "series":       [],
        "related_kws":  [],
        "source":       f"fallback:{reason}",
        "timestamp":    datetime.now().isoformat(),
    }


def enrich_research_file(slug: str) -> bool:
    """
    Charge le fichier research/{slug}.json et l'enrichit avec les données trends.
    """
    path = OUT_DIR / f"{slug}.json"
    if not path.exists():
        print(f"  ❌ Fichier introuvable : {path}")
        return False

    data    = json.loads(path.read_text())
    keyword = data.get("keyword", slug)

    print(f"\n📈 Trends : {keyword}")
    trend   = get_trend_score(keyword)
    data["trends"] = trend

    print(f"   Score     : {trend['trend_score']}/100")
    print(f"   Direction : {trend['direction']}")
    print(f"   Near peak : {trend['near_peak']}")
    if trend["related_kws"]:
        print(f"   Related   : {', '.join(trend['related_kws'][:3])}")

    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    return True


def enrich_all() -> int:
    """Enrichit tous les fichiers research/*.json sans données trends."""
    files = list(OUT_DIR.glob("*.json"))
    done  = 0
    for f in sorted(files):
        data = json.loads(f.read_text())
        if "trends" not in data:
            if enrich_research_file(f.stem):
                done += 1
                time.sleep(2)  # respecter le rate limit Google Trends
    print(f"\n✅ {done} fichiers enrichis")
    return done


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug", type=str)
    parser.add_argument("--all",  action="store_true")
    args = parser.parse_args()

    if args.slug:
        enrich_research_file(args.slug)
    elif args.all:
        enrich_all()
    else:
        print("Usage: python 02_trends.py --slug <slug> | --all")
