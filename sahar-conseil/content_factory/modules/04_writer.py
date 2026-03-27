"""
SAHAR Conseil — Content Factory
Module 04 : Génération d'article SEO via Claude API

Génère un article HTML complet :
  - Structure H1/H2/H3 optimisée SEO
  - Intro accrocheuse (PAS formula)
  - Corps dense basé sur PAA + SERP
  - Maillage interne automatique depuis keywords.json
  - CTA contextuels intégrés
  - Balises meta title + description
  - Schema.org Article

Coût estimé : ~0.05€/article (Claude Sonnet)

Usage :
  python 04_writer.py --slug passoires-thermiques-interdites-location-2025
  python 04_writer.py --all   # traite tous les fichiers scorés "publish"
"""

import os
import json
import re
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime

HERE        = Path(__file__).parent.parent
OUT_RESEARCH = HERE / "output" / "research"
OUT_ARTICLES = HERE / "output" / "articles"
OUT_ARTICLES.mkdir(parents=True, exist_ok=True)

CFG          = json.loads((HERE / "config" / "keywords.json").read_text())
SETTINGS     = CFG.get("settings", {})
INTERNAL_LINKS = CFG.get("internal_links", {})
CTAS         = CFG.get("ctas", {})

ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL         = "claude-sonnet-4-20250514"  # toujours Sonnet 4
BASE_URL      = SETTINGS.get("base_url", "https://sahar-conseil.fr")
BLOG_DIR      = SETTINGS.get("blog_dir", "blog")
MOT_CIBLES    = SETTINGS.get("longueur_article_mots", 1200)


# ─────────────────────────────────────────────────────────────────────────────
# PROMPT ENGINEERING
# ─────────────────────────────────────────────────────────────────────────────

def build_brief(data: dict) -> str:
    """Construit le brief éditorial complet pour Claude."""
    keyword   = data.get("keyword", "")
    secteur   = data.get("secteur", "")
    cible     = data.get("cible", "")
    url_cible = data.get("url_cible", "index.html")
    paa       = data.get("paa", [])[:8]
    organic   = data.get("organic", [])[:5]
    related   = data.get("related", [])[:6]
    trends    = data.get("trends", {})
    score     = data.get("score", {})
    fmt       = score.get("format_rec", "article_moyen")

    # Links internes disponibles pour ce secteur
    links = INTERNAL_LINKS.get(secteur, []) + INTERNAL_LINKS.get("methode", [])
    links_str = "\n".join(f'  - texte="{l["texte"]}" url="{l["url"]}"' for l in links[:6])

    # CTA contextuel selon secteur
    cta = CTAS.get(f"outil_{secteur}", CTAS.get("demo", {"texte": "Voir une démo →", "url": "index.html#contact"}))

    # SERP context
    serp_titles = "\n".join(f"  {i+1}. {r.get('title','')}" for i, r in enumerate(organic))

    # PAA
    paa_str = "\n".join(f"  Q: {p['question']}" for p in paa if p.get("question"))

    # Format
    format_instructions = {
        "guide_complet":  f"Guide complet long-form de {MOT_CIBLES+400} mots minimum. 6-8 sections H2. Section FAQ en fin.",
        "article_moyen":  f"Article informatif de {MOT_CIBLES} mots. 4-5 sections H2.",
        "article_court":  f"Article focus de {MOT_CIBLES-300} mots. 3 sections H2 principales.",
    }.get(fmt, f"Article de {MOT_CIBLES} mots.")

    direction = trends.get("direction", "stable")
    trend_note = {
        "montante":    "Le sujet est EN HAUSSE — insiste sur l'urgence et l'actualité.",
        "descendante": "Le sujet est en légère baisse — focus sur la valeur durable, pas l'urgence.",
        "stable":      "Sujet stable — focus sur la valeur pratique et les cas d'usage.",
    }.get(direction, "")

    return f"""BRIEF ARTICLE SAHAR CONSEIL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MOT-CLÉ PRINCIPAL : {keyword}
SECTEUR : {secteur}
CIBLE LECTEUR : {cible}
FORMAT : {format_instructions}
TENDANCE : {trend_note}

CONCURRENTS SERP (titres des 5 premiers résultats) :
{serp_titles}

QUESTIONS PAA À COUVRIR (intégrer en H2 ou corps) :
{paa_str if paa_str else "  (pas de PAA disponible — générer 4 questions pertinentes)"}

MOTS-CLÉS ASSOCIÉS À INTÉGRER NATURELLEMENT :
{", ".join(related) if related else "  (générer depuis le contexte)"}

LIENS INTERNES DISPONIBLES (utiliser 3-4 dans l'article) :
{links_str if links_str else "  - url='index.html#contact' texte='demander une démo'"}

CTA PRINCIPAL :
  Texte : {cta['texte']}
  URL   : {cta['url']}
  (Intégrer 2 fois : milieu et fin d'article)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INSTRUCTIONS DE RÉDACTION :

1. TON : Direct, terrain, professionnel. Pas de jargon académique.
   Le lecteur est un professionnel qui cherche une info actionnable.
   Utilise "vous". Pas de formules creuses ("dans un monde où...").

2. STRUCTURE HTML attendue (retourner UNIQUEMENT du HTML, pas de markdown) :
   - <article> wrapper avec class="blog-article"
   - <header class="article-header"> avec h1, meta-info (date, temps lecture, secteur)
   - Sections <section> avec id= pour chaque H2
   - <strong> sur tous les chiffres clés et termes importants
   - Listes <ul> pour les énumérations (pas de tirets en prose)
   - 2 blocs <div class="article-cta"> avec le CTA principal
   - <div class="article-faq"> si guide_complet
   - Liens internes avec <a href="../../{url_cible}">texte</a>

3. SEO :
   - H1 doit contenir le mot-clé principal
   - Premiers 100 mots doivent contenir le mot-clé
   - Alt text sur toutes les images (décrire image suggérée entre [crochets])
   - Densité naturelle : mot-clé et variantes ~1.5%

4. LONGUEUR : {format_instructions}

5. MÉTA À GÉNÉRER (en JSON en dehors du HTML, dans un bloc <meta-seo>) :
   {{
     "title": "titre SEO 55-60 chars",
     "description": "meta desc 150-160 chars",
     "slug": "slug-url-de-l-article",
     "reading_time": "X min",
     "tags": ["tag1", "tag2", "tag3"]
   }}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
IMPORTANT : Retourner UNIQUEMENT :
1. Le bloc <meta-seo>{{...}}</meta-seo> avec le JSON
2. Le HTML de l'article complet (de <article> à </article>)
Rien d'autre. Pas d'explication, pas de markdown.
"""


# ─────────────────────────────────────────────────────────────────────────────
# APPEL API CLAUDE
# ─────────────────────────────────────────────────────────────────────────────

def call_claude(prompt: str, system: str = "") -> str:
    """Appelle l'API Claude et retourne le texte généré."""
    if not ANTHROPIC_KEY:
        raise ValueError("ANTHROPIC_API_KEY non définie")

    headers = {
        "Content-Type":    "application/json",
        "x-api-key":       ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
    }
    body = {
        "model":      MODEL,
        "max_tokens": 4000,
        "messages":   [{"role": "user", "content": prompt}],
    }
    if system:
        body["system"] = system

    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=headers,
        json=body,
        timeout=120,
    )
    r.raise_for_status()
    data = r.json()
    return data["content"][0]["text"]


SYSTEM_WRITER = """Tu es un rédacteur SEO expert en prospection commerciale B2B, données publiques françaises,
immobilier, énergie et retail. Tu rédiges pour SAHAR Conseil — un outil qui transforme DVF, DPE,
INSEE et SIRENE en pipeline commercial pour les professionnels.

Ton style : direct, terrain, dense en information, sans jargon creux. Tu utilises des chiffres réels
et des exemples concrets. Tu n'uses pas de formules d'introduction génériques.

Tu génères du HTML propre et sémantique. Jamais de markdown dans ta réponse."""


# ─────────────────────────────────────────────────────────────────────────────
# PARSING RÉPONSE
# ─────────────────────────────────────────────────────────────────────────────

def parse_response(raw: str) -> tuple[dict, str]:
    """
    Extrait le JSON meta-seo et le HTML de l'article depuis la réponse brute.
    Retourne (meta_dict, article_html).
    """
    # Extraire meta-seo
    meta = {}
    meta_match = re.search(r'<meta-seo>(.*?)</meta-seo>', raw, re.DOTALL)
    if meta_match:
        try:
            meta = json.loads(meta_match.group(1).strip())
        except json.JSONDecodeError:
            # Essayer de récupérer les champs manuellement
            for field in ["title", "description", "slug", "reading_time"]:
                m = re.search(rf'"{field}"\s*:\s*"([^"]+)"', meta_match.group(1))
                if m:
                    meta[field] = m.group(1)

    # Extraire le HTML article
    html_match = re.search(r'(<article[\s\S]*?</article>)', raw, re.DOTALL)
    article_html = html_match.group(1) if html_match else raw

    return meta, article_html


# ─────────────────────────────────────────────────────────────────────────────
# GÉNÉRATION
# ─────────────────────────────────────────────────────────────────────────────

def generate_article(data: dict) -> dict | None:
    """
    Génère l'article complet pour un keyword data.
    Retourne un dict avec meta + html, ou None si erreur.
    """
    keyword = data.get("keyword", "")
    slug    = data.get("slug", "article")
    print(f"\n✍️  Génération : {keyword}")
    print(f"   Score : {data.get('score', {}).get('total', '?')}/100")

    brief = build_brief(data)

    try:
        print("   → Appel Claude API...")
        raw = call_claude(brief, system=SYSTEM_WRITER)
        print(f"   → Réponse reçue ({len(raw)} chars)")
    except Exception as e:
        print(f"   ❌ Erreur API : {e}")
        return None

    meta, article_html = parse_response(raw)

    # Compléter les méta manquantes
    if not meta.get("slug"):
        meta["slug"] = slug
    if not meta.get("title"):
        meta["title"] = keyword[:60]

    # Assembler le résultat
    result = {
        "keyword":      keyword,
        "slug":         meta.get("slug", slug),
        "secteur":      data.get("secteur", ""),
        "cible":        data.get("cible", ""),
        "url_cible":    data.get("url_cible", "index.html"),
        "score":        data.get("score", {}),
        "meta": {
            "title":        meta.get("title", ""),
            "description":  meta.get("description", ""),
            "reading_time": meta.get("reading_time", "5 min"),
            "tags":         meta.get("tags", []),
            "published_at": datetime.now().strftime("%Y-%m-%d"),
            "slug":         meta.get("slug", slug),
        },
        "html":         article_html,
        "generated_at": datetime.now().isoformat(),
        "model":        MODEL,
        "needs_review": False,  # sera mis à True par le module 05
    }

    # Sauvegarder
    out_path = OUT_ARTICLES / f"{meta.get('slug', slug)}.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"   ✅ Article sauvegardé : {out_path.name}")

    return result


def generate_all(max_articles: int = None) -> list:
    """Génère tous les articles des fichiers research scorés 'publish'."""
    max_a   = max_articles or SETTINGS.get("max_articles_par_run", 3)
    files   = sorted(OUT_RESEARCH.glob("*.json"))
    done    = []

    for f in files:
        if len(done) >= max_a:
            print(f"\n⏹  Limite atteinte ({max_a} articles)")
            break

        data   = json.loads(f.read_text())
        score  = data.get("score", {})

        # Ne traiter que les "publish" non encore générés
        if score.get("decision") != "publish":
            continue

        art_path = OUT_ARTICLES / f"{data.get('slug', f.stem)}.json"
        if art_path.exists():
            print(f"  ⏭  Déjà généré : {f.stem}")
            continue

        result = generate_article(data)
        if result:
            done.append(result)
            time.sleep(2)  # pause entre les articles

    print(f"\n✅ {len(done)} articles générés → {OUT_ARTICLES}")
    return done


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug",  type=str, help="Slug du fichier research à traiter")
    parser.add_argument("--all",   action="store_true", help="Traiter tous les publish")
    parser.add_argument("--max",   type=int, default=3, help="Max articles (mode --all)")
    args = parser.parse_args()

    if args.slug:
        path = OUT_RESEARCH / f"{args.slug}.json"
        if path.exists():
            generate_article(json.loads(path.read_text()))
        else:
            print(f"❌ {path} introuvable")
    elif args.all:
        generate_all(args.max)
    else:
        print("Usage: python 04_writer.py --slug <slug> | --all [--max 3]")
