"""
Module 04 — Writer
Génère un article SEO complet via Claude API (Sonnet).
Intègre : H1/H2/H3, maillage interne SAHAR, CTAs contextuels,
données PAA, structure sémantique.
"""

import os, json, logging, re
from typing import Optional
import requests

logger = logging.getLogger(__name__)

CLAUDE_MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 4000


def _call_claude(prompt: str, system: str) -> str:
    """Appel direct à l'API Anthropic."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY manquant")

    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": CLAUDE_MODEL,
            "max_tokens": MAX_TOKENS,
            "system": system,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=120,
    )
    r.raise_for_status()
    data = r.json()
    return data["content"][0]["text"]


def _build_system_prompt() -> str:
    return """Tu es un rédacteur SEO expert pour SAHAR Conseil.
SAHAR transforme les données publiques françaises (DVF, DPE, INSEE, SIRENE) en outils commerciaux pour les professionnels.

Ton style :
- Ton direct, terrain, professionnel. Pas de jargon inutile.
- Phrases courtes. Paragraphes aérés.
- Gras sur les chiffres, les faits importants, les termes techniques clés.
- Pas de formules creuses ("il est important de noter que...", "en conclusion...", "dans un monde où...")
- Pas de liste à puces excessive — préférer les paragraphes structurés
- Vocabulaire varié, naturel, humain

Contraintes techniques :
- Répondre UNIQUEMENT en JSON valide, sans markdown, sans backticks
- Structure imposée dans le prompt
- Maillage interne SAHAR obligatoire (liens fournis)
- CTAs contextuels intégrés dans le corps de l'article"""


def _build_article_prompt(
    keyword: str,
    serp_data: dict,
    trends_data: dict,
    score_data: dict,
    seed_info: dict,
    internal_links: list,
) -> str:
    paa_list = "\n".join(f"- {p['question']}" for p in serp_data.get("paa", [])[:6])
    related_list = ", ".join(serp_data.get("related", [])[:6])
    top_titles = "\n".join(
        f"- {r['title']} ({r['domain']})"
        for r in serp_data.get("organic", [])[:5]
    )
    links_str = "\n".join(f"- {l['anchor']} → {l['url']}" for l in internal_links[:5])
    direction = trends_data.get("direction", "stable")
    secteur = seed_info.get("secteur", "immobilier")

    return f"""Génère un article de blog SEO complet pour SAHAR Conseil.

MOT-CLÉ PRINCIPAL : {keyword}
SECTEUR : {secteur}
DIRECTION TENDANCE : {direction}
SCORE ÉDITORIAL : {score_data['score_total']}/100

QUESTIONS PAA À COUVRIR :
{paa_list or "Aucune PAA disponible — génère des questions pertinentes"}

RECHERCHES ASSOCIÉES (à intégrer naturellement) :
{related_list or "N/A"}

CONCURRENTS SERP (ne pas copier, s'en différencier) :
{top_titles or "N/A"}

LIENS INTERNES SAHAR À INTÉGRER (obligatoire — au moins 3) :
{links_str}

CTA À INTÉGRER (1 CTA en milieu d'article, 1 en fin) :
- CTA principal : "Accéder à l'outil SAHAR →" → lien vers {seed_info.get('url_cible', 'index.html')}
- CTA secondaire : "Demander une démo gratuite" → index.html#contact

CONSIGNES DE RÉDACTION :
- Longueur cible : 900 à 1200 mots
- H1 = le titre principal (intègre le mot-clé exactement)
- 4 à 6 H2 structurants (intègrent des variantes du mot-clé)
- H3 pour les sous-points si besoin
- Premier paragraphe : accroche avec fait chiffré ou stat réelle
- Couvre les questions PAA dans le corps de l'article
- Données sources : DVF data.gouv.fr, DPE ADEME, INSEE, DARES (officielles françaises)
- Intègre des chiffres concrets (même approximatifs mais sourcés)
- Maillage naturel — pas de "cliquez ici", utiliser des ancres descriptives

RÉPONDS EN JSON STRICT avec cette structure :
{{
  "slug": "url-slug-seo-en-minuscules-avec-tirets",
  "title_seo": "Titre SEO complet (60-65 chars)",
  "meta_description": "Description meta (150-160 chars)",
  "h1": "Titre H1 de l'article",
  "intro": "Paragraphe d'introduction (150-200 mots)",
  "sections": [
    {{
      "h2": "Titre de section",
      "content": "Contenu HTML de la section (p, strong, a href, h3 si besoin)",
      "has_cta": false
    }}
  ],
  "conclusion": "Paragraphe de conclusion avec CTA final",
  "tags": ["tag1", "tag2", "tag3"],
  "secteur": "{secteur}",
  "word_count_estimate": 1000
}}"""


def write(
    keyword: str,
    serp_data: dict,
    trends_data: dict,
    score_data: dict,
    seed_info: dict,
    keywords_config: dict,
) -> dict:
    """
    Génère l'article complet.
    Retourne dict avec toutes les métadonnées + contenu.
    """
    secteur = seed_info.get("secteur", "transversal")

    # Construire les liens internes depuis config
    link_urls = keywords_config.get("internal_links", {}).get(secteur, [])
    internal_links = _make_internal_links(keyword, link_urls)

    system = _build_system_prompt()
    prompt = _build_article_prompt(
        keyword, serp_data, trends_data, score_data, seed_info, internal_links
    )

    logger.info(f"Writer: génération article '{keyword}'...")

    raw = _call_claude(prompt, system)

    # Parser JSON
    article = _parse_json(raw)
    if not article:
        logger.error("Impossible de parser le JSON de l'article")
        return {"error": "json_parse_failed", "raw": raw[:500]}

    article["keyword"] = keyword
    article["internal_links_used"] = internal_links
    article["seed_info"] = seed_info

    wc = _estimate_word_count(article)
    article["word_count_actual"] = wc
    logger.info(f"  → Article généré : '{article.get('h1', '')}' (~{wc} mots)")

    return article


def _make_internal_links(keyword: str, urls: list) -> list:
    """Génère des suggestions d'ancres pour les liens internes."""
    anchors = {
        "immobilier.html": "DVF Analyse Pro",
        "energie-renovation.html": "DPE Scanner",
        "crm.html": "CRM Pipeline SAHAR",
        "scoring-prospects.html": "scoring de prospects",
        "trouver-prospects-immobiliers.html": "trouver des prospects immobiliers",
        "passoires-thermiques-interdites.html": "passoires thermiques interdites",
        "passoires-thermiques-bordeaux.html": "logements F/G à Bordeaux",
        "prospecter-donnees-publiques.html": "prospecter avec les données publiques",
        "kpis-commerciaux.html": "KPIs commerciaux",
        "automatisation-prospection.html": "automatisation de la prospection",
        "conversion-pipeline.html": "conversion pipeline",
        "rh-recrutement.html": "tension recrutement",
        "retail-franchise.html": "attractivité commerciale",
        "lead-magnet-immobilier.html": "lead magnet immobilier",
    }
    return [
        {"url": url, "anchor": anchors.get(url, url.replace(".html", "").replace("-", " "))}
        for url in urls
    ]


def _parse_json(text: str) -> Optional[dict]:
    """Extrait et parse le JSON de la réponse Claude."""
    # Nettoyer les backticks markdown éventuels
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()

    # Trouver le JSON
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Essayer d'extraire le JSON entre {}
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
    return None


def _estimate_word_count(article: dict) -> int:
    """Estime le nombre de mots dans l'article généré."""
    text = article.get("intro", "") + " " + article.get("conclusion", "")
    for s in article.get("sections", []):
        text += " " + s.get("content", "")
    # Nettoyer HTML
    text = re.sub(r"<[^>]+>", " ", text)
    return len(text.split())


if __name__ == "__main__":
    import sys, logging
    logging.basicConfig(level=logging.INFO)
    print("Module writer OK — nécessite ANTHROPIC_API_KEY pour exécution complète")
