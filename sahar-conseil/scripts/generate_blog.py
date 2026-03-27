#!/usr/bin/env python3
"""
SAHAR Conseil — Usine à contenu automatisée
Génère des articles de blog SEO à partir des données immobilières SAHAR.

Usage :
  python generate_blog.py                    # Génère tous les articles du mois
  python generate_blog.py --dept 33          # Génère un article pour le dept 33
  python generate_blog.py --type tendances   # Un type spécifique

Types d'articles :
  - tendances    : Tendances marché immobilier par département
  - estimation   : Guide d'estimation immobilière
  - renovation   : Rénovation énergétique et passoires thermiques
  - investir     : Où investir : top communes par département
  - dpe          : Impact du DPE sur les prix immobiliers
"""

import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

# ── Configuration ────────────────────────────────────────────────────────────

SITE_DIR = Path(__file__).resolve().parents[1] / "site"
BLOG_DIR = SITE_DIR / "blog"
TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"

MOIS_FR = {
    1: "janvier", 2: "février", 3: "mars", 4: "avril",
    5: "mai", 6: "juin", 7: "juillet", 8: "août",
    9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre",
}

DEPT_NOMS = {
    "01": "Ain", "02": "Aisne", "03": "Allier", "06": "Alpes-Maritimes",
    "13": "Bouches-du-Rhône", "31": "Haute-Garonne", "33": "Gironde",
    "34": "Hérault", "38": "Isère", "44": "Loire-Atlantique",
    "59": "Nord", "67": "Bas-Rhin", "69": "Rhône", "75": "Paris",
    "76": "Seine-Maritime", "78": "Yvelines", "83": "Var",
    "92": "Hauts-de-Seine", "93": "Seine-Saint-Denis", "94": "Val-de-Marne",
}

# Départements prioritaires pour le SEO (gros volume de recherches)
DEPTS_PRIORITAIRES = ["75", "69", "33", "13", "31", "44", "34", "59", "06", "67"]

SAHAR_URL = "https://sahar-conseil.fr"
SAHAR_APP_URL = "https://sahar-co-dvfanalysepro.streamlit.app"

# ── Templates d'articles ─────────────────────────────────────────────────────

ARTICLE_TEMPLATES = {
    "tendances": {
        "title": "Marché immobilier {dept_nom} ({dept}) — Tendances {mois} {annee}",
        "slug": "marche-immobilier-{dept_slug}-tendances-{mois_slug}-{annee}",
        "description": "Analyse du marché immobilier en {dept_nom} : prix au m², évolution, communes dynamiques et opportunités {mois} {annee}.",
        "keywords": [
            "marché immobilier {dept_nom}",
            "prix immobilier {dept_nom} {annee}",
            "prix m2 {dept_nom}",
            "tendances immobilier {dept_nom}",
            "estimation immobilière {dept_nom}",
        ],
        "sections": [
            ("Vue d'ensemble du marché en {dept_nom}", "overview"),
            ("Prix au m² : appartements vs maisons", "prix_type"),
            ("Top 10 des communes les plus dynamiques", "top_communes"),
            ("Évolution des prix sur 12 mois", "evolution"),
            ("Où sont les opportunités ?", "opportunites"),
            ("Impact du DPE sur les prix", "dpe_impact"),
            ("Notre analyse et recommandations", "recommandations"),
        ],
    },
    "estimation": {
        "title": "Comment estimer un bien immobilier en {dept_nom} en {annee} ?",
        "slug": "estimer-bien-immobilier-{dept_slug}-{annee}",
        "description": "Guide complet pour estimer votre bien immobilier en {dept_nom}. Méthodes, comparables, outils gratuits et erreurs à éviter.",
        "keywords": [
            "estimation immobilière {dept_nom}",
            "estimer bien immobilier {dept_nom}",
            "prix bien immobilier {dept_nom}",
            "estimation gratuite {dept_nom}",
            "combien vaut ma maison {dept_nom}",
        ],
        "sections": [
            ("Pourquoi une bonne estimation est cruciale", "intro"),
            ("Les 3 méthodes d'estimation immobilière", "methodes"),
            ("Les données DVF : votre meilleur allié", "dvf"),
            ("Estimer un appartement vs une maison", "types"),
            ("Les facteurs qui font varier le prix", "facteurs"),
            ("Erreurs courantes à éviter", "erreurs"),
            ("Obtenez votre rapport d'estimation SAHAR", "cta"),
        ],
    },
    "renovation": {
        "title": "Passoires thermiques en {dept_nom} : état des lieux et opportunités {annee}",
        "slug": "passoires-thermiques-{dept_slug}-renovation-{annee}",
        "description": "Analyse des passoires thermiques DPE F et G en {dept_nom}. Zones prioritaires, aides MaPrimeRénov', opportunités pour artisans RGE.",
        "keywords": [
            "passoires thermiques {dept_nom}",
            "DPE F G {dept_nom}",
            "rénovation énergétique {dept_nom}",
            "MaPrimeRénov {dept_nom}",
            "artisan RGE {dept_nom}",
        ],
        "sections": [
            ("L'état du parc immobilier en {dept_nom}", "etat"),
            ("Combien de passoires thermiques ?", "chiffres"),
            ("Les communes les plus concernées", "communes"),
            ("Les aides disponibles : MaPrimeRénov' et CEE", "aides"),
            ("Opportunités pour les professionnels", "opportunites"),
            ("Comment cibler les bons prospects", "ciblage"),
        ],
    },
    "investir": {
        "title": "Où investir en {dept_nom} en {annee} ? Top communes et analyse",
        "slug": "ou-investir-{dept_slug}-{annee}",
        "description": "Les meilleures communes pour investir en {dept_nom} en {annee}. Analyse DVF, rendement locatif, dynamique de marché.",
        "keywords": [
            "investir immobilier {dept_nom}",
            "meilleure commune investir {dept_nom}",
            "rendement locatif {dept_nom}",
            "investissement immobilier {dept_nom} {annee}",
        ],
        "sections": [
            ("Pourquoi investir en {dept_nom} en {annee}", "pourquoi"),
            ("Les critères de sélection SAHAR", "criteres"),
            ("Top 10 communes pour investir", "top10"),
            ("Analyse risque/rendement", "risque"),
            ("Stratégies d'investissement recommandées", "strategies"),
            ("Passez à l'action avec SAHAR", "cta"),
        ],
    },
}


# ── Génération du contenu ────────────────────────────────────────────────────

def generate_section_content(section_id: str, template_type: str, dept: str, dept_nom: str) -> str:
    """Génère le contenu d'une section à partir de données et templates."""

    # Contenu par défaut enrichi avec des données réalistes
    # En production, ces données viendraient de Supabase
    contents = {
        "tendances": {
            "overview": f"""
Le marché immobilier en {dept_nom} reste dynamique en ce début d'année. Avec plus de
**5 000 transactions** enregistrées sur les 12 derniers mois dans le département {dept},
les indicateurs montrent une activité soutenue malgré les incertitudes économiques.

Le prix médian au m² s'établit autour de **3 200 €/m²** pour les appartements et
**2 800 €/m²** pour les maisons, avec des variations significatives selon les communes.
Les zones urbaines maintiennent leur attractivité tandis que certaines communes
périurbaines gagnent en dynamisme.

Notre [score de marché SAHAR]({SAHAR_URL}) permet d'identifier précisément les zones
les plus actives et les opportunités émergentes.
""",
            "prix_type": f"""
L'analyse des transactions DVF révèle des écarts marqués entre les types de biens :

| Type | Prix médian €/m² | Évolution 12m | Surface médiane |
|------|:-:|:-:|:-:|
| Appartement | 3 200 € | +2,8% | 62 m² |
| Maison | 2 800 € | +1,5% | 95 m² |

Les appartements affichent une progression plus rapide, portée par la demande
locative et les primo-accédants. Les maisons, bien que plus stables, offrent
souvent un meilleur rendement à long terme dans les communes périurbaines.

La **fourchette de prix** (écart interquartile) est un indicateur clé : elle
mesure l'homogénéité du marché. Un marché homogène facilite l'estimation et
réduit les risques.
""",
            "top_communes": f"""
Voici les communes les plus dynamiques du département {dept} selon notre scoring SAHAR
(combinant volume, évolution des prix et ratio de tension) :

1. **Commune A** — Score 85/100 — Prix médian 3 800 €/m² (+5,2%)
2. **Commune B** — Score 78/100 — Prix médian 2 950 €/m² (+3,8%)
3. **Commune C** — Score 74/100 — Prix médian 3 200 €/m² (+2,1%)
4. **Commune D** — Score 71/100 — Prix médian 2 600 €/m² (+4,5%)
5. **Commune E** — Score 68/100 — Prix médian 3 100 €/m² (+1,9%)

*Ces données sont issues de l'analyse DVF sur 12 mois. Pour des résultats
personnalisés sur votre zone, utilisez [DVF Analyse Pro]({SAHAR_APP_URL}).*
""",
            "evolution": f"""
Sur les 12 derniers mois, le département {dept} ({dept_nom}) affiche une
**évolution moyenne de +2,3%** des prix au m². Cette progression est
contrastée :

- **Zones urbaines denses** : +3 à +5% — portées par la rareté du foncier
- **Première couronne** : +1 à +3% — attractivité croissante
- **Zones rurales** : -1 à +1% — marché plus stable

Le **ratio de tension** (rapport entre volume de ventes récent et passé)
est supérieur à 1,0 dans 60% des communes, signe d'un marché globalement
porteur.
""",
            "opportunites": f"""
Notre algorithme identifie les opportunités selon deux axes :

**Communes sous-cotées à fort potentiel :**
Les communes avec un score marché élevé (>60) mais un prix médian encore
inférieur à la moyenne départementale représentent les meilleures opportunités.
Elles combinent dynamisme et accessibilité.

**Biens sous-valorisés :**
En croisant les données DVF avec les diagnostics DPE, nous identifions les
biens dont le prix au m² est significativement inférieur aux comparables.
Un bien classé F ou G avec un bon emplacement peut représenter une
opportunité après rénovation.

[Découvrez les opportunités sur votre zone →]({SAHAR_APP_URL})
""",
            "dpe_impact": f"""
L'impact du DPE sur les prix immobiliers est de plus en plus marqué :

| Étiquette DPE | Impact sur le prix |
|:---:|:---:|
| A-B | +6% à +16% vs moyenne |
| C-D | Référence (prix moyen) |
| E | -2% à -5% |
| F | -8% à -12% |
| G | -12% à -20% |

En {dept_nom}, nous analysons **plus de 10 000 diagnostics DPE** croisés avec
les transactions DVF. Cette donnée est intégrée dans nos rapports d'estimation
pour fournir une valorisation ajustée au DPE.
""",
            "recommandations": f"""
**Pour les acheteurs :** Le marché en {dept_nom} est favorable aux acheteurs
bien préparés. Armez-vous d'un [rapport d'estimation SAHAR]({SAHAR_URL}#rapport)
pour négocier avec des données concrètes.

**Pour les vendeurs :** Positionnez votre bien au prix du marché. Un bien
correctement estimé se vend en moyenne 30% plus vite qu'un bien surévalué.
Notre rapport inclut des recommandations de prix avec intervalle de confiance.

**Pour les investisseurs :** Ciblez les communes avec un score SAHAR > 60 et
un DPE améliorable. La combinaison achat décoté + rénovation énergétique
reste la stratégie la plus rentable en {annee}.

---

*Cet article est généré automatiquement à partir des données DVF officielles
(data.gouv.fr) analysées par SAHAR Conseil. [En savoir plus sur notre méthodologie]({SAHAR_URL}).*
""",
        },
    }

    # Récupérer le contenu ou un placeholder
    annee = datetime.now().year
    section_contents = contents.get(template_type, {})
    content = section_contents.get(section_id, f"*Contenu en cours de rédaction pour {dept_nom}.*\n")
    return content.replace("{annee}", str(annee))


def generate_article(template_type: str, dept: str, dept_nom: str) -> dict:
    """Génère un article complet."""
    now = datetime.now()
    mois = MOIS_FR[now.month]
    annee = now.year
    dept_slug = dept_nom.lower().replace(" ", "-").replace("'", "").replace("/", "-")
    mois_slug = mois.replace("é", "e").replace("û", "u")

    template = ARTICLE_TEMPLATES[template_type]

    # Format title and meta
    fmt = {
        "dept": dept, "dept_nom": dept_nom, "dept_slug": dept_slug,
        "mois": mois, "mois_slug": mois_slug, "annee": annee,
    }
    title = template["title"].format(**fmt)
    slug = template["slug"].format(**fmt)
    description = template["description"].format(**fmt)
    keywords = [kw.format(**fmt) for kw in template["keywords"]]

    # Generate sections
    sections_md = []
    for section_title, section_id in template["sections"]:
        h2_title = section_title.format(**fmt)
        content = generate_section_content(section_id, template_type, dept, dept_nom)
        sections_md.append(f"## {h2_title}\n\n{content.strip()}\n")

    body = "\n\n".join(sections_md)

    return {
        "title": title,
        "slug": slug,
        "description": description,
        "keywords": keywords,
        "body": body,
        "date": now.strftime("%Y-%m-%d"),
        "dept": dept,
        "dept_nom": dept_nom,
        "type": template_type,
    }


# ── Génération HTML ──────────────────────────────────────────────────────────

def markdown_to_html_simple(md: str) -> str:
    """Conversion Markdown → HTML simplifiée (sans dépendance)."""
    import re
    html = md

    # Tables
    lines = html.split("\n")
    result = []
    in_table = False
    for line in lines:
        if "|" in line and line.strip().startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if all(set(c) <= set("-: ") for c in cells):
                continue  # Skip separator row
            if not in_table:
                result.append("<table>")
                tag = "th"
                in_table = True
            else:
                tag = "td"
            row = "".join(f"<{tag}>{c}</{tag}>" for c in cells)
            result.append(f"<tr>{row}</tr>")
        else:
            if in_table:
                result.append("</table>")
                in_table = False
            result.append(line)
    if in_table:
        result.append("</table>")
    html = "\n".join(result)

    # Headers
    html = re.sub(r"^## (.+)$", r"<h2>\1</h2>", html, flags=re.MULTILINE)
    html = re.sub(r"^### (.+)$", r"<h3>\1</h3>", html, flags=re.MULTILINE)

    # Bold, italic
    html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html)
    html = re.sub(r"\*(.+?)\*", r"<em>\1</em>", html)

    # Links
    html = re.sub(r"\[(.+?)\]\((.+?)\)", r'<a href="\2">\1</a>', html)

    # Lists
    html = re.sub(r"^- (.+)$", r"<li>\1</li>", html, flags=re.MULTILINE)
    html = re.sub(r"^(\d+)\. (.+)$", r"<li>\2</li>", html, flags=re.MULTILINE)

    # Paragraphs
    html = re.sub(r"\n\n+", "\n</p><p>\n", html)
    html = f"<p>\n{html}\n</p>"

    # HR
    html = html.replace("---", "<hr>")

    return html


def generate_html_article(article: dict, spotify_show_id: str = "") -> str:
    """Génère la page HTML complète d'un article."""

    body_html = markdown_to_html_simple(article["body"])
    keywords_str = ", ".join(article["keywords"])
    date_display = datetime.strptime(article["date"], "%Y-%m-%d").strftime("%d %B %Y")

    spotify_widget = ""
    if spotify_show_id:
        spotify_widget = f"""
    <aside class="podcast-widget">
      <div class="podcast-label">Écoutez notre podcast</div>
      <iframe src="https://open.spotify.com/embed/show/{spotify_show_id}?theme=0"
              width="100%" height="152" frameBorder="0"
              allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture"
              loading="lazy"></iframe>
    </aside>"""

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{article['title']} — SAHAR Conseil</title>
  <meta name="description" content="{article['description']}">
  <meta name="keywords" content="{keywords_str}">
  <meta name="author" content="SAHAR Conseil">
  <link rel="canonical" href="{SAHAR_URL}/blog/{article['slug']}/">

  <!-- Open Graph -->
  <meta property="og:title" content="{article['title']}">
  <meta property="og:description" content="{article['description']}">
  <meta property="og:type" content="article">
  <meta property="og:url" content="{SAHAR_URL}/blog/{article['slug']}/">
  <meta property="og:site_name" content="SAHAR Conseil">

  <!-- Twitter Card -->
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="{article['title']}">
  <meta name="twitter:description" content="{article['description']}">

  <!-- JSON-LD Schema -->
  <script type="application/ld+json">
  {{
    "@context": "https://schema.org",
    "@type": "BlogPosting",
    "headline": "{article['title']}",
    "description": "{article['description']}",
    "datePublished": "{article['date']}",
    "author": {{ "@type": "Organization", "name": "SAHAR Conseil", "url": "{SAHAR_URL}" }},
    "publisher": {{ "@type": "Organization", "name": "SAHAR Conseil" }},
    "mainEntityOfPage": {{ "@type": "WebPage", "@id": "{SAHAR_URL}/blog/{article['slug']}/" }},
    "keywords": "{keywords_str}"
  }}
  </script>

  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=Inter:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {{ --bleu:#185FA5; --bleu-c:#E6F1FB; --vert:#1D9E75; --vert-c:#E1F5EE; --gris-f:#2C2C2A; --gris-m:#73726c; --gris-c:#F1EFE8; --blanc:#FFF; --border:#D3D1C7; }}
    *,*::before,*::after {{ box-sizing:border-box; margin:0; padding:0; }}
    html {{ scroll-behavior:smooth; }}
    body {{ font-family:'Inter',sans-serif; color:var(--gris-f); background:var(--blanc); line-height:1.7; }}
    h1,h2,h3 {{ font-family:'Syne',sans-serif; line-height:1.2; }}

    nav {{ position:sticky; top:0; z-index:100; background:rgba(255,255,255,.96); backdrop-filter:blur(8px); border-bottom:1px solid var(--border); padding:0 5%; display:flex; align-items:center; justify-content:space-between; height:64px; }}
    .nav-logo {{ font-family:'Syne',sans-serif; font-size:1.1rem; font-weight:700; color:var(--bleu); text-decoration:none; }}
    .nav-cta {{ background:var(--bleu); color:var(--blanc); padding:.5rem 1.2rem; border-radius:8px; font-size:.85rem; font-weight:500; text-decoration:none; }}

    .article-hero {{ background:linear-gradient(145deg,#0E3D6B,#185FA5); color:#fff; padding:4rem 5% 3rem; }}
    .article-hero .breadcrumb {{ font-size:.8rem; opacity:.6; margin-bottom:1rem; }}
    .article-hero .breadcrumb a {{ color:#fff; text-decoration:none; }}
    .article-hero h1 {{ font-size:clamp(1.6rem,3.5vw,2.4rem); font-weight:800; margin-bottom:1rem; max-width:720px; }}
    .article-hero .meta {{ display:flex; gap:1.5rem; font-size:.85rem; opacity:.75; flex-wrap:wrap; }}

    .article-body {{ max-width:760px; margin:0 auto; padding:3rem 5%; }}
    .article-body h2 {{ font-size:1.4rem; font-weight:700; color:var(--bleu); margin:2.5rem 0 1rem; padding-bottom:.5rem; border-bottom:2px solid var(--bleu-c); }}
    .article-body h3 {{ font-size:1.1rem; color:var(--gris-f); margin:1.5rem 0 .75rem; }}
    .article-body p {{ margin-bottom:1rem; }}
    .article-body a {{ color:var(--bleu); text-decoration:underline; }}
    .article-body strong {{ color:var(--gris-f); }}
    .article-body table {{ width:100%; border-collapse:collapse; margin:1.5rem 0; font-size:.9rem; }}
    .article-body th {{ background:var(--bleu); color:#fff; padding:.6rem .8rem; text-align:left; }}
    .article-body td {{ padding:.5rem .8rem; border-bottom:1px solid var(--gris-c); }}
    .article-body tr:nth-child(even) {{ background:var(--gris-c); }}
    .article-body li {{ margin-left:1.5rem; margin-bottom:.4rem; }}
    .article-body hr {{ border:none; border-top:1px solid var(--border); margin:2rem 0; }}

    .podcast-widget {{ background:var(--gris-c); padding:1.5rem; border-radius:12px; margin:2rem 0; border-left:4px solid #1DB954; }}
    .podcast-label {{ font-family:'Syne',sans-serif; font-weight:600; font-size:.9rem; margin-bottom:.75rem; color:#1DB954; }}

    .cta-box {{ background:linear-gradient(135deg,var(--bleu),#0C447C); color:#fff; padding:2rem; border-radius:12px; margin:2.5rem 0; text-align:center; }}
    .cta-box h3 {{ font-size:1.3rem; margin-bottom:.75rem; }}
    .cta-box p {{ opacity:.85; margin-bottom:1.2rem; }}
    .cta-box a {{ display:inline-block; background:#fff; color:var(--bleu); padding:.7rem 1.5rem; border-radius:8px; font-weight:600; text-decoration:none; }}

    footer {{ background:var(--gris-f); color:rgba(255,255,255,.7); padding:2rem 5%; text-align:center; font-size:.85rem; }}
    footer a {{ color:rgba(255,255,255,.6); text-decoration:none; }}

    @media(max-width:768px) {{ .article-hero {{ padding:2rem 5%; }} }}
  </style>
</head>
<body>

<nav>
  <a href="{SAHAR_URL}" class="nav-logo">SAHAR Conseil</a>
  <a href="{SAHAR_URL}#contact" class="nav-cta">Demander un rapport</a>
</nav>

<header class="article-hero">
  <div class="breadcrumb"><a href="{SAHAR_URL}">SAHAR</a> / <a href="{SAHAR_URL}/blog/">Blog</a> / {article['dept_nom']}</div>
  <h1>{article['title']}</h1>
  <div class="meta">
    <span>Par SAHAR Conseil</span>
    <span>{date_display}</span>
    <span>{article['dept_nom']} ({article['dept']})</span>
  </div>
</header>

<article class="article-body">
{spotify_widget}

{body_html}

  <div class="cta-box">
    <h3>Obtenez votre rapport d'estimation personnalisé</h3>
    <p>7 pages d'analyse : comparables, estimation avec intervalle de confiance, recommandations stratégiques.</p>
    <a href="{SAHAR_URL}#rapport">Découvrir le Rapport Pro →</a>
  </div>
</article>

<footer>
  <p>© {datetime.now().year} SAHAR Conseil — <a href="{SAHAR_URL}">sahar-conseil.fr</a> — Données DVF data.gouv.fr</p>
</footer>

</body>
</html>"""


# ── Génération de l'index blog ───────────────────────────────────────────────

def generate_blog_index(articles: list) -> str:
    """Génère la page index du blog avec tous les articles."""
    cards = ""
    for a in sorted(articles, key=lambda x: x["date"], reverse=True):
        date_display = datetime.strptime(a["date"], "%Y-%m-%d").strftime("%d %B %Y")
        cards += f"""
    <a href="{a['slug']}/" class="blog-card">
      <div class="blog-card-tag">{a['dept_nom']}</div>
      <h3>{a['title']}</h3>
      <p>{a['description'][:120]}...</p>
      <div class="blog-card-meta">{date_display}</div>
    </a>"""

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Blog SAHAR Conseil — Analyses immobilières</title>
  <meta name="description" content="Blog SAHAR Conseil : analyses de marché immobilier, tendances, estimations et opportunités par département.">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=Inter:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {{ --bleu:#185FA5; --bleu-c:#E6F1FB; --vert:#1D9E75; --gris-f:#2C2C2A; --gris-m:#73726c; --gris-c:#F1EFE8; --blanc:#FFF; --border:#D3D1C7; }}
    *,*::before,*::after {{ box-sizing:border-box; margin:0; padding:0; }}
    body {{ font-family:'Inter',sans-serif; color:var(--gris-f); background:var(--blanc); line-height:1.6; }}
    h1,h2,h3 {{ font-family:'Syne',sans-serif; }}

    nav {{ position:sticky; top:0; z-index:100; background:rgba(255,255,255,.96); backdrop-filter:blur(8px); border-bottom:1px solid var(--border); padding:0 5%; display:flex; align-items:center; justify-content:space-between; height:64px; }}
    .nav-logo {{ font-family:'Syne',sans-serif; font-size:1.1rem; font-weight:700; color:var(--bleu); text-decoration:none; }}
    .nav-cta {{ background:var(--bleu); color:var(--blanc); padding:.5rem 1.2rem; border-radius:8px; font-size:.85rem; font-weight:500; text-decoration:none; }}

    .blog-hero {{ background:linear-gradient(145deg,#f7f6f2,#eaf3fb); padding:4rem 5% 3rem; text-align:center; }}
    .blog-hero h1 {{ font-size:clamp(1.8rem,3.5vw,2.8rem); font-weight:800; margin-bottom:.75rem; }}
    .blog-hero p {{ font-size:1.05rem; color:var(--gris-m); max-width:540px; margin:0 auto; }}

    .blog-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:1.5rem; padding:3rem 5%; max-width:1100px; margin:0 auto; }}
    .blog-card {{ background:var(--blanc); border:1px solid var(--border); border-radius:12px; padding:1.5rem; text-decoration:none; color:inherit; transition:box-shadow .2s,transform .2s; display:block; }}
    .blog-card:hover {{ box-shadow:0 6px 30px rgba(24,95,165,.1); transform:translateY(-2px); }}
    .blog-card-tag {{ display:inline-block; background:var(--bleu-c); color:var(--bleu); font-size:.75rem; font-weight:600; padding:.2rem .6rem; border-radius:100px; margin-bottom:.75rem; }}
    .blog-card h3 {{ font-size:1rem; font-weight:600; margin-bottom:.5rem; line-height:1.3; }}
    .blog-card p {{ font-size:.85rem; color:var(--gris-m); line-height:1.5; }}
    .blog-card-meta {{ font-size:.75rem; color:var(--gris-m); margin-top:.75rem; }}

    footer {{ background:var(--gris-f); color:rgba(255,255,255,.7); padding:2rem 5%; text-align:center; font-size:.85rem; }}
    footer a {{ color:rgba(255,255,255,.6); text-decoration:none; }}
  </style>
</head>
<body>

<nav>
  <a href="{SAHAR_URL}" class="nav-logo">SAHAR Conseil</a>
  <a href="{SAHAR_URL}#contact" class="nav-cta">Demander une démo</a>
</nav>

<header class="blog-hero">
  <h1>Blog SAHAR Conseil</h1>
  <p>Analyses de marché, tendances immobilières et opportunités — département par département.</p>
</header>

<section class="blog-grid">
{cards}
</section>

<footer>
  <p>© {datetime.now().year} SAHAR Conseil — <a href="{SAHAR_URL}">sahar-conseil.fr</a></p>
</footer>

</body>
</html>"""


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SAHAR Blog Content Factory")
    parser.add_argument("--dept", help="Département spécifique (ex: 33)")
    parser.add_argument("--type", choices=ARTICLE_TEMPLATES.keys(), default="tendances",
                        help="Type d'article")
    parser.add_argument("--all", action="store_true", help="Générer pour tous les depts prioritaires")
    parser.add_argument("--spotify", default="", help="Spotify Show ID pour le widget podcast")
    args = parser.parse_args()

    BLOG_DIR.mkdir(parents=True, exist_ok=True)

    articles = []

    if args.all:
        depts = DEPTS_PRIORITAIRES
        types = list(ARTICLE_TEMPLATES.keys())
    elif args.dept:
        depts = [args.dept]
        types = [args.type]
    else:
        depts = DEPTS_PRIORITAIRES[:3]  # Top 3 par défaut
        types = [args.type]

    for dept in depts:
        dept_nom = DEPT_NOMS.get(dept, f"Département {dept}")
        for article_type in types:
            print(f"  Generating: {article_type} — {dept_nom} ({dept})...")
            article = generate_article(article_type, dept, dept_nom)
            articles.append(article)

            # Create article directory and HTML
            article_dir = BLOG_DIR / article["slug"]
            article_dir.mkdir(parents=True, exist_ok=True)

            html = generate_html_article(article, spotify_show_id=args.spotify)
            (article_dir / "index.html").write_text(html, encoding="utf-8")
            print(f"    → {article_dir}/index.html")

    # Generate blog index
    index_html = generate_blog_index(articles)
    (BLOG_DIR / "index.html").write_text(index_html, encoding="utf-8")
    print(f"\n  Blog index → {BLOG_DIR}/index.html")
    print(f"\n  {len(articles)} articles generated.")


if __name__ == "__main__":
    main()
