"""
Module 05 — AI Checker & Reformulator
Détecte les patterns trop "IA" dans le texte et reformule si nécessaire.
Max 2 passes. Score de naturalité 0-100.
"""

import os, re, logging, requests
from typing import Optional

logger = logging.getLogger(__name__)

SEUIL_NATURALITE = 70  # En dessous → reformulation
MAX_PASSES = 2

# Patterns typiques IA à détecter
AI_PATTERNS = [
    r"\bil est (important|essentiel|crucial) de\b",
    r"\bdans (un monde|notre société|notre ère) (où|actuelle)",
    r"\ben (conclusion|résumé|somme)\b",
    r"\bil convient de noter\b",
    r"\bn'oubliez pas que\b",
    r"\nen (effet|outre)\b",
    r"\bpar ailleurs\b",
    r"\btoutefois\b",
    r"\bnéanmoins\b",
    r"\bcependant\b",
    r"\bainsi que\b",
    r"\ben ce qui concerne\b",
    r"\bà cet égard\b",
    r"\bdans ce contexte\b",
    r"\bforce est de constater\b",
    r"\bil va sans dire\b",
    r"\bau fil du temps\b",
    r"\bla question se pose\b",
    r"\ben définitive\b",
    r"\bsans plus attendre\b",
    r"\bvous l'aurez compris\b",
]

# Phrases de clôture typiques IA
AI_CLOSINGS = [
    r"n'hésitez pas à (nous contacter|contacter|partager)",
    r"(nous espérons|j'espère) que (cet article|ce guide|cette)",
    r"pour aller plus loin",
    r"comme nous (avons vu|venons de voir)",
    r"pour conclure (cet article|ce guide|cette)",
]


def _extract_all_text(article: dict) -> str:
    """Extrait tout le texte HTML de l'article en plain text."""
    parts = [
        article.get("intro", ""),
        article.get("conclusion", ""),
    ]
    for s in article.get("sections", []):
        parts.append(s.get("content", ""))
        parts.append(s.get("h2", ""))

    text = " ".join(parts)
    # Nettoyer balises HTML
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _score_naturalite(text: str) -> dict:
    """Calcule le score de naturalité du texte."""
    text_lower = text.lower()
    total_words = len(text.split())

    # Compter les patterns IA
    ai_hits = []
    for pattern in AI_PATTERNS + AI_CLOSINGS:
        matches = re.findall(pattern, text_lower)
        if matches:
            ai_hits.extend(matches)

    nb_hits = len(ai_hits)

    # Ratio hits / 1000 mots
    hits_per_1k = (nb_hits / max(total_words, 1)) * 1000

    # Score : 100 = aucun pattern, 0 = très chargé
    score = max(0, min(100, 100 - (hits_per_1k * 15)))

    # Pénalités supplémentaires
    if re.search(r"en (conclusion|résumé)", text_lower):
        score -= 10
    if re.search(r"il est (important|essentiel|crucial)", text_lower):
        score -= 8

    score = max(0, round(score))

    return {
        "score": score,
        "ai_hits": ai_hits[:10],
        "nb_hits": nb_hits,
        "total_words": total_words,
        "hits_per_1k": round(hits_per_1k, 2),
    }


def _reformulate_with_claude(article: dict, check_result: dict) -> dict:
    """Demande à Claude de reformuler les passages trop IA."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY manquant — pas de reformulation")
        return article

    hits_str = ", ".join(f'"{h}"' for h in check_result.get("ai_hits", [])[:8])

    import json
    article_json = json.dumps(article, ensure_ascii=False)

    prompt = f"""Reformule cet article de blog pour le rendre plus naturel et humain.

PROBLÈMES DÉTECTÉS (expressions trop "IA" à éliminer) :
{hits_str}

RÈGLES DE REFORMULATION :
- Remplace ces formules par des alternatives directes et concrètes
- Garde le sens exact et le HTML
- Conserve le maillage interne (balises <a>)
- Conserve les balises <strong> sur les chiffres et faits importants
- Ton : direct, terrain, professionnel
- Phrases plus courtes si possible

ARTICLE À REFORMULER :
{article_json[:6000]}

RÉPONDS UNIQUEMENT avec le JSON reformulé, même structure exacte, sans markdown."""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 4000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=120,
        )
        r.raise_for_status()
        raw = r.json()["content"][0]["text"]

        # Parser JSON
        raw = re.sub(r"```json\s*", "", raw)
        raw = re.sub(r"```\s*", "", raw).strip()
        reformulated = json.loads(raw)
        logger.info("  → Reformulation appliquée")
        return reformulated

    except Exception as e:
        logger.warning(f"Reformulation failed: {e}")
        return article


def check_and_fix(article: dict) -> dict:
    """
    Point d'entrée principal.
    Vérifie la naturalité, reformule si besoin (max 2 passes).
    Retourne l'article avec métadonnées de qualité.
    """
    text = _extract_all_text(article)
    result = _score_naturalite(text)

    logger.info(
        f"Checker: score naturalité = {result['score']}/100 "
        f"({result['nb_hits']} patterns IA détectés)"
    )

    passes = 0
    current_article = article
    scores = [result["score"]]

    while result["score"] < SEUIL_NATURALITE and passes < MAX_PASSES:
        logger.info(f"  → Reformulation (passe {passes + 1}/{MAX_PASSES})...")
        current_article = _reformulate_with_claude(current_article, result)

        # Réévaluer
        text = _extract_all_text(current_article)
        result = _score_naturalite(text)
        scores.append(result["score"])
        passes += 1

        logger.info(f"  → Score après passe {passes} : {result['score']}/100")

    # Ajouter méta qualité à l'article
    current_article["_quality"] = {
        "score_naturalite": result["score"],
        "score_initial": scores[0],
        "passes_reformulation": passes,
        "scores_history": scores,
        "ai_hits_final": result["ai_hits"],
        "approved": result["score"] >= SEUIL_NATURALITE,
    }

    status = "✅ OK" if result["score"] >= SEUIL_NATURALITE else "⚠️ Approuvé malgré score bas"
    logger.info(f"Checker final: {result['score']}/100 {status}")

    return current_article


if __name__ == "__main__":
    import json, logging
    logging.basicConfig(level=logging.INFO)

    # Test avec article factice
    fake_article = {
        "h1": "Test article",
        "intro": "Il est important de noter que dans notre société actuelle, les passoires thermiques représentent un enjeu crucial. En effet, il convient de noter que cependant, par ailleurs, néanmoins...",
        "sections": [{"h2": "Section test", "content": "<p>En conclusion, n'oubliez pas que cet aspect est essentiel.</p>"}],
        "conclusion": "Pour aller plus loin, nous espérons que cet article vous a été utile. N'hésitez pas à nous contacter.",
    }
    result = check_and_fix(fake_article)
    print(f"Score initial  : {result['_quality']['score_initial']}/100")
    print(f"Score final    : {result['_quality']['score_naturalite']}/100")
    print(f"Passes         : {result['_quality']['passes_reformulation']}")
