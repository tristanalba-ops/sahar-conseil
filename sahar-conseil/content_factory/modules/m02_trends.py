"""
Module 02 — Google Trends via PyTrends
Retourne : tendance 12 mois, intérêt moyen, pic, région top, topics associés.
"""

import logging, time
from typing import Optional

logger = logging.getLogger(__name__)


def trends(keyword: str, geo: str = "FR", timeframe: str = "today 12-m") -> dict:
    """
    Analyse tendance Google pour un mot-clé.
    Retourne score_tendance 0-100, direction (hausse/baisse/stable), données brutes.
    """
    try:
        from pytrends.request import TrendReq

        pt = TrendReq(hl="fr-FR", tz=60, timeout=(10, 30), retries=2, backoff_factor=0.5)
        pt.build_payload([keyword], cat=0, timeframe=timeframe, geo=geo, gprop="")
        time.sleep(1)  # Respect rate limit

        # Intérêt dans le temps
        df_time = pt.interest_over_time()
        if df_time.empty or keyword not in df_time.columns:
            return _no_data(keyword)

        series = df_time[keyword].dropna()
        if len(series) < 4:
            return _no_data(keyword)

        avg = float(series.mean())
        peak = float(series.max())
        recent_3m = float(series.tail(12).mean())  # ~3 mois (semaines)
        older_3m = float(series.head(12).mean())

        # Direction tendance
        if recent_3m > older_3m * 1.1:
            direction = "hausse"
        elif recent_3m < older_3m * 0.9:
            direction = "baisse"
        else:
            direction = "stable"

        # Score tendance 0-100
        # Pondération : intérêt moyen (60%) + direction (40%)
        score_base = min(100, avg)
        bonus_direction = 20 if direction == "hausse" else (-10 if direction == "baisse" else 0)
        score_tendance = min(100, max(0, score_base + bonus_direction))

        # Régions top
        try:
            df_region = pt.interest_by_region(resolution="REGION", inc_low_vol=True)
            top_regions = df_region[keyword].nlargest(3).index.tolist() if not df_region.empty else []
        except Exception:
            top_regions = []

        # Topics associés
        try:
            related = pt.related_topics()
            rising = related.get(keyword, {}).get("rising")
            top_topics = []
            if rising is not None and not rising.empty:
                top_topics = rising["topic_title"].head(5).tolist()
        except Exception:
            top_topics = []

        return {
            "keyword": keyword,
            "score_tendance": round(score_tendance, 1),
            "direction": direction,
            "avg_interest": round(avg, 1),
            "peak_interest": round(peak, 1),
            "recent_vs_older": round(recent_3m / max(older_3m, 1), 2),
            "top_regions": top_regions,
            "top_topics": top_topics,
            "data_points": len(series),
            "source": "pytrends",
        }

    except ImportError:
        logger.warning("pytrends non installé — pip install pytrends")
        return _no_data(keyword, reason="pytrends_missing")
    except Exception as e:
        logger.warning(f"Trends error for '{keyword}': {e}")
        return _no_data(keyword, reason=str(e))


def _no_data(keyword: str, reason: str = "no_data") -> dict:
    return {
        "keyword": keyword,
        "score_tendance": 40.0,  # Score neutre si pas de données
        "direction": "inconnu",
        "avg_interest": 0,
        "peak_interest": 0,
        "recent_vs_older": 1.0,
        "top_regions": [],
        "top_topics": [],
        "data_points": 0,
        "source": f"fallback_{reason}",
    }


if __name__ == "__main__":
    import sys, json, logging
    logging.basicConfig(level=logging.INFO)
    kw = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "passoires thermiques"
    print(json.dumps(trends(kw), ensure_ascii=False, indent=2))
