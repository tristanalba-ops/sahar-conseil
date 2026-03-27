"""
SAHAR Conseil — Content Factory
Module 05 : Détection & correction ton IA

Analyse le HTML généré et détecte les patterns IA trop visibles.
Si score IA > seuil, envoie une passe de reformulation ciblée.
Maximum 2 passes pour éviter les boucles infinies.

Score IA basé sur :
  - Patterns lexicaux IA (liste empirique FR)
  - Densité de formules intro génériques
  - Ratio adjectifs superlatifs
  - Répétitions structurelles

Usage :
  python 05_checker.py --slug mon-article
  python 05_checker.py --all
"""

import os
import re
import json
import requests
import argparse
from pathlib import Path
from datetime import datetime

HERE         = Path(__file__).parent.parent
OUT_ARTICLES = HERE / "output" / "articles"
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL         = "claude-sonnet-4-20250514"

# Seuil : si score IA > 40, reformuler
AI_SCORE_THRESHOLD = 40

# ─── Patterns IA en français ─────────────────────────────────────────────────
AI_PATTERNS = [
    # Introductions
    r"dans (un monde|notre monde|notre société) (où|qui|moderne)",
    r"(il est|il devient) (important|crucial|essentiel|primordial) de",
    r"(de plus en plus|toujours plus)",
    r"n'(est|a) (jamais été|plus été) aussi",
    r"à l'(ère|heure|époque) (de|du|des|où)",
    r"(face à|confronté à) (ces|les|de nombreux) (défis|enjeux|challenges)",
    # Transitions
    r"en (effet|outre|conclusion|résumé|somme)",
    r"(il convient de|il est à noter que|notons que|soulignons que)",
    r"(ainsi|par conséquent|de ce fait),? (il|on|nous|vous)",
    r"(dans (ce|cet|cette) (contexte|cadre|optique|perspective))",
    # Conclusions IA
    r"(en (conclusion|résumé|définitive|fin de compte))",
    r"(pour (conclure|résumer|récapituler))",
    r"(au final|au bout du compte)",
    r"n'hésitez pas à",
    # Adjectifs creux
    r"\b(incontournable|indispensable|essentiel|crucial|fondamental|primordial)\b",
    r"\b(innovant|révolutionnaire|disruptif|game.changer)\b",
    r"\b(optimal|optimiser|optimisation)\b",
    r"\b(robuste|efficace|performant|efficient)\b",
    # Structures IA
    r"(que ce soit|qu'il s'agisse) (pour|de)",
    r"il (faut|convient|est recommandé) (de|d')",
]

# Patterns qui signalent du BON contenu (baisser le score si présents)
GOOD_PATTERNS = [
    r"\d+[\s]*(€|%|m²|km|ans?|jours?|mois|semaines?)",  # chiffres concrets
    r"(par exemple|prenons|imaginons|concrètement)",       # exemples
    r"(selon|d'après|source|données?|chiffre)",            # sources
]


def compute_ai_score(html: str) -> dict:
    """
    Analyse le HTML et retourne un score IA 0-100
    et la liste des passages problématiques.
    """
    # Extraire le texte brut
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()
    words = text.split()
    total_words = max(len(words), 1)

    hits       = []
    hit_count  = 0

    for pattern in AI_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            hit_count += len(matches)
            hits.append({
                "pattern": pattern,
                "count":   len(matches),
                "example": re.search(pattern, text, re.IGNORECASE).group(0) if re.search(pattern, text, re.IGNORECASE) else ""
            })

    # Bonus "good content" : réduire le score si beaucoup de concret
    good_count = sum(
        len(re.findall(p, text, re.IGNORECASE))
        for p in GOOD_PATTERNS
    )

    # Score : hits / (mots/100) - bonus bon contenu
    raw_score = (hit_count / (total_words / 100)) * 15
    bonus     = min(20, good_count * 2)
    ai_score  = max(0, min(100, int(raw_score - bonus)))

    return {
        "ai_score":   ai_score,
        "hit_count":  hit_count,
        "good_count": good_count,
        "hits":       hits[:10],  # top 10 patterns trouvés
        "total_words": total_words,
    }


def call_claude_rewrite(html: str, hits: list) -> str:
    """Demande à Claude de reformuler les passages trop IA."""
    if not ANTHROPIC_KEY:
        raise ValueError("ANTHROPIC_API_KEY non définie")

    patterns_str = "\n".join(f"  - {h['example']!r} (×{h['count']})" for h in hits[:8])

    prompt = f"""Voici un article HTML qui contient des formulations trop typiques d'un texte généré par IA.

PASSAGES PROBLÉMATIQUES DÉTECTÉS :
{patterns_str}

INSTRUCTIONS DE CORRECTION :
1. Remplace les formulations listées par des alternatives directes et concrètes
2. Supprime les transitions inutiles ("en effet", "ainsi", "par conséquent")
3. Raccourcis les phrases longues en 2 phrases courtes
4. Remplace les adjectifs superlatifs par des chiffres ou des faits
5. Garde le HTML intact — modifie uniquement le texte
6. Ne change PAS : les liens, les classes CSS, les balises structurelles
7. Retourne UNIQUEMENT le HTML modifié, rien d'autre

HTML À CORRIGER :
{html[:8000]}"""

    headers = {
        "Content-Type":      "application/json",
        "x-api-key":         ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
    }
    body = {
        "model":      MODEL,
        "max_tokens": 4000,
        "system":     "Tu es un éditeur qui humanise les textes IA. Tu corriges le style sans changer la structure HTML. Tu retournes uniquement le HTML corrigé.",
        "messages":   [{"role": "user", "content": prompt}],
    }

    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=headers,
        json=body,
        timeout=90,
    )
    r.raise_for_status()
    return r.json()["content"][0]["text"]


def check_article(slug: str) -> dict | None:
    """Charge, analyse et si nécessaire reformule un article."""
    path = OUT_ARTICLES / f"{slug}.json"
    if not path.exists():
        print(f"❌ Article introuvable : {path}")
        return None

    data = json.loads(path.read_text())
    html = data.get("html", "")
    passes = data.get("ai_check_passes", 0)

    print(f"\n🔍 Check IA : {data.get('keyword', slug)}")

    result = compute_ai_score(html)
    score  = result["ai_score"]

    print(f"   Score IA : {score}/100 (seuil : {AI_SCORE_THRESHOLD})")
    print(f"   Hits     : {result['hit_count']} patterns IA")
    print(f"   Good     : {result['good_count']} signaux positifs")

    if score <= AI_SCORE_THRESHOLD:
        print(f"   ✅ Article OK — pas de reformulation nécessaire")
        data["ai_check"] = {**result, "passed": True, "passes": passes, "timestamp": datetime.now().isoformat()}
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        return data

    if passes >= 2:
        print(f"   ⚠️  Max passes atteint (2) — article accepté tel quel")
        data["ai_check"] = {**result, "passed": False, "passes": passes, "force_accepted": True, "timestamp": datetime.now().isoformat()}
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        return data

    # Reformulation
    print(f"   → Reformulation en cours (passe {passes + 1}/2)...")
    try:
        new_html = call_claude_rewrite(html, result["hits"])
        # Vérifier qu'on a bien du HTML en retour
        if "<article" in new_html or "<section" in new_html or "<p" in new_html:
            data["html"] = new_html
            data["ai_check_passes"] = passes + 1
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
            print(f"   → Re-check après reformulation...")
            return check_article(slug)  # récursion max 2 passes
        else:
            print(f"   ⚠️  Reformulation invalide — HTML non retourné")
    except Exception as e:
        print(f"   ❌ Erreur reformulation : {e}")

    data["ai_check"] = {**result, "passed": False, "passes": passes, "timestamp": datetime.now().isoformat()}
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    return data


def check_all() -> list:
    """Vérifie tous les articles non encore checkés."""
    files = sorted(OUT_ARTICLES.glob("*.json"))
    done  = []
    for f in files:
        data = json.loads(f.read_text())
        if "ai_check" not in data:
            result = check_article(f.stem)
            if result:
                done.append(result)
    print(f"\n✅ {len(done)} articles vérifiés")
    return done


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug", type=str)
    parser.add_argument("--all",  action="store_true")
    args = parser.parse_args()

    if args.slug:
        check_article(args.slug)
    elif args.all:
        check_all()
    else:
        print("Usage: python 05_checker.py --slug <slug> | --all")
