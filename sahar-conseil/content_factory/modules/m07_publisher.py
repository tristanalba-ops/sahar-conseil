"""
Module 07 — Publisher
- Génère le HTML final de l'article avec le design system SAHAR
- Intègre le widget Spotify si disponible
- Push sur GitHub Pages via API
- Ping GA4 Measurement Protocol
- Met à jour index du blog + sitemap
"""

import os, re, json, logging, requests, hashlib
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

logger = logging.getLogger(__name__)

GITHUB_REPO = "tristanalba-ops/sahar-conseil"
GITHUB_BRANCH = "main"
DOCS_PATH = "sahar-conseil/docs"
BLOG_PATH = f"{DOCS_PATH}/blog"
BASE_URL = "https://tristanalba-ops.github.io/sahar-conseil"
GA4_MEASUREMENT_ID = "G-XV2P0YPJK0"

# ── CSS partagé (injecté depuis index.html au runtime, fallback inline) ──────

def _read_shared_css() -> str:
    """Lire le CSS depuis index.html existant."""
    idx = Path(__file__).resolve().parents[2] / "docs" / "index.html"
    if idx.exists():
        content = idx.read_text()
        start = content.find("<style>") + 7
        end = content.find("</style>")
        if start > 7 and end > start:
            return content[start:end]
    return ""


# ── NAV / FOOTER identiques au site ─────────────────────────────────────────

NAV = """<a href="../index.html" class="skip">Aller au contenu</a>
<nav class="nav" aria-label="Navigation principale">
  <a href="../index.html" class="nav-logo"><span class="nav-logo-dot"></span>SAHAR Conseil</a>
  <ul class="nav-menu">
    <li><a href="../immobilier.html">Immobilier</a></li>
    <li><a href="../energie-renovation.html">Énergie</a></li>
    <li><a href="../retail-franchise.html">Retail</a></li>
    <li><a href="../rh-recrutement.html">RH</a></li>
    <li><a href="../crm.html">CRM</a></li>
    <li><a href="../index.html#tarifs">Tarifs</a></li>
  </ul>
  <div class="nav-actions"><a href="../index.html#contact" class="btn btn-primary btn-sm">Démo gratuite</a></div>
</nav>"""

FOOTER = """<footer class="footer">
  <div class="container">
    <div class="footer-grid">
      <div class="footer-brand"><strong style="font-size:.95rem;font-weight:700">SAHAR Conseil</strong><p>Open data au service des professionnels. DVF, DPE, INSEE, SIRENE transformés en pipeline commercial.</p></div>
      <div class="footer-col"><h4>Secteurs</h4><ul><li><a href="../immobilier.html">Immobilier DVF</a></li><li><a href="../energie-renovation.html">Énergie &amp; DPE</a></li><li><a href="../retail-franchise.html">Retail</a></li><li><a href="../rh-recrutement.html">RH</a></li></ul></div>
      <div class="footer-col"><h4>Outils</h4><ul><li><a href="../crm.html">CRM Pipeline</a></li><li><a href="../scoring-prospects.html">Scoring</a></li><li><a href="../automatisation-prospection.html">Automation</a></li></ul></div>
      <div class="footer-col"><h4>Blog</h4><ul><li><a href="index.html">Tous les articles</a></li></ul></div>
    </div>
    <div class="footer-bottom"><p>© 2024 SAHAR Conseil</p><div class="footer-legal"><a href="../index.html#contact">Contact</a><a href="../sitemap.xml">Sitemap</a></div></div>
  </div>
</footer>"""


def _spotify_widget(spotify_episode_url: str) -> str:
    """Génère le widget Spotify embed."""
    if not spotify_episode_url:
        return ""
    # Extraire l'ID épisode
    match = re.search(r"episode/([A-Za-z0-9]+)", spotify_episode_url)
    if not match:
        return ""
    ep_id = match.group(1)
    return f"""<div style="margin:2rem 0;border:1px solid var(--bd);border-radius:12px;overflow:hidden">
  <iframe style="border-radius:12px" src="https://open.spotify.com/embed/episode/{ep_id}?utm_source=generator&theme=0"
    width="100%" height="152" frameBorder="0" allowfullscreen=""
    allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture"
    loading="lazy"></iframe>
</div>"""


def _build_article_html(article: dict, audio_data: dict, date_str: str) -> str:
    """Génère le HTML complet de l'article."""
    css = _read_shared_css()
    slug = article.get("slug", "article")
    h1 = article.get("h1", "")
    title_seo = article.get("title_seo", h1)
    meta_desc = article.get("meta_description", "")
    intro = article.get("intro", "")
    conclusion = article.get("conclusion", "")
    sections = article.get("sections", [])
    tags = article.get("tags", [])
    secteur = article.get("secteur", "")
    quality = article.get("_quality", {})
    spotify_url = audio_data.get("spotify_url", "") if audio_data else ""
    word_count = article.get("word_count_actual", 0)
    canonical = f"{BASE_URL}/blog/{slug}.html"

    # Schema Article
    schema = json.dumps({
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": h1,
        "description": meta_desc,
        "author": {"@type": "Organization", "name": "SAHAR Conseil"},
        "publisher": {"@type": "Organization", "name": "SAHAR Conseil", "url": BASE_URL},
        "datePublished": date_str,
        "dateModified": date_str,
        "url": canonical,
        "keywords": ", ".join(tags),
        "articleSection": secteur,
    }, ensure_ascii=False)

    # TOC depuis les H2
    toc_items = ""
    for i, s in enumerate(sections):
        anchor = re.sub(r"[^a-z0-9]", "-", s.get("h2", "").lower())[:40]
        toc_items += f'<a href="#{anchor}">{s.get("h2","")}</a>\n'

    # Corps de l'article
    body_sections = ""
    mid = len(sections) // 2
    for i, s in enumerate(sections):
        anchor = re.sub(r"[^a-z0-9]", "-", s.get("h2", "").lower())[:40]
        body_sections += f'<h2 id="{anchor}">{s["h2"]}</h2>\n{s.get("content","")}\n'

        # CTA en milieu d'article
        if i == mid:
            url_cible = article.get("seed_info", {}).get("url_cible", "index.html")
            body_sections += f"""<div class="callout blue" style="text-align:center;margin:2rem 0">
  <strong>Testez SAHAR sur vos données réelles</strong>
  <p style="margin:.5rem 0 1rem">Démo en 20 minutes sur votre secteur.</p>
  <a href="../{url_cible}" class="btn btn-blue btn-sm">Accéder à l'outil →</a>
  <a href="../index.html#contact" class="btn btn-outline btn-sm" style="margin-left:.5rem">Demander une démo</a>
</div>\n"""

    # Widget Spotify
    spotify_html = _spotify_widget(spotify_url)
    if not spotify_html and audio_data and audio_data.get("script"):
        spotify_html = f"""<div class="callout" style="margin:2rem 0">
  <strong>🎙️ Version podcast disponible</strong>
  <p style="margin:.3rem 0 0;font-size:.88rem">Cet article existe en version audio. <a href="../index.html#contact">Demandez l'accès au podcast SAHAR →</a></p>
</div>"""

    # Tags HTML
    tags_html = " ".join(f'<span class="badge badge-gray">{t}</span>' for t in tags)

    # Related links depuis seed_info
    internal_links = article.get("internal_links_used", [])
    related_html = ""
    for lnk in internal_links[:3]:
        related_html += f"""<a href="../{lnk['url']}" class="related-card">
  <span class="tag">Ressource</span>
  <h4>{lnk['anchor'].title()}</h4>
  <p>Outil SAHAR</p>
</a>\n"""

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>{title_seo}</title>
  <meta name="description" content="{meta_desc}">
  <meta name="robots" content="index, follow">
  <meta name="author" content="SAHAR Conseil">
  <link rel="canonical" href="{canonical}">
  <meta property="og:type" content="article">
  <meta property="og:title" content="{title_seo}">
  <meta property="og:description" content="{meta_desc}">
  <meta property="og:url" content="{canonical}">
  <script type="application/ld+json">{schema}</script>
  <!-- GTM -->
  <script>(function(w,d,s,l,i){{w[l]=w[l]||[];w[l].push({{'gtm.start':new Date().getTime(),event:'gtm.js'}});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);}})(window,document,'script','dataLayer','GTM-5WSR4DK5');</script>
  <script async src="https://www.googletagmanager.com/gtag/js?id={GA4_MEASUREMENT_ID}"></script>
  <script>window.dataLayer=window.dataLayer||[];function gtag(){{dataLayer.push(arguments)}}gtag('js',new Date());gtag('config','{GA4_MEASUREMENT_ID}');
  gtag('event','article_view',{{article_slug:'{slug}',article_secteur:'{secteur}',article_score:'{quality.get("score_naturalite",0)}'}});</script>
  <style>{css}</style>
</head>
<body>
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-5WSR4DK5" height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
{NAV}
<main id="main">

<section class="section" style="padding-top:4.5rem;padding-bottom:3rem;background:var(--bg2)">
  <div class="container-text" style="max-width:720px;margin:0 auto;padding:0 5%">
    <div class="breadcrumb">
      <a href="../index.html">Accueil</a><span class="breadcrumb-sep">→</span>
      <a href="index.html">Blog</a><span class="breadcrumb-sep">→</span>
      <span>{secteur.title()}</span>
    </div>
    <div style="margin-bottom:.75rem">{tags_html}</div>
    <h1 style="margin-bottom:1rem">{h1}</h1>
    <p class="lead">{intro[:200]}...</p>
    <p style="font-size:.78rem;color:var(--ink3);margin-top:1rem">
      Publié le {date_str} · {word_count} mots · SAHAR Conseil
    </p>
  </div>
</section>

<section class="section">
  <div style="max-width:720px;margin:0 auto;padding:0 5%">

    {f'<div class="toc"><p class="toc-title">Dans cet article</p>{toc_items}</div>' if toc_items else ''}

    <p>{intro}</p>

    {spotify_html}

    {body_sections}

    <div style="border-top:1px solid var(--bd);margin:2.5rem 0;padding-top:2rem">
      <p>{conclusion}</p>
    </div>

    <!-- CTA final -->
    <div class="cta-box">
      <h3>Tester SAHAR sur vos données</h3>
      <p>Démonstration en 20 minutes — données DVF ou DPE de votre secteur.</p>
      <div style="display:flex;gap:.75rem;justify-content:center;flex-wrap:wrap">
        <a href="../index.html#contact" class="btn btn-primary">Demander une démo →</a>
        <a href="../{article.get('seed_info',{{}}).get('url_cible','index.html')}" class="btn btn-outline">Voir l'outil</a>
      </div>
    </div>

  </div>
</section>

<section class="section">
  <div class="container">
    <div class="related">
      <span class="related-label">Ressources associées</span>
      <div class="related-grid">{related_html}</div>
    </div>
  </div>
</section>

</main>
{FOOTER}
</body>
</html>"""


def _github_push(file_path: str, content: str, message: str) -> bool:
    """Push un fichier sur GitHub via l'API REST."""
    token = os.getenv("GITHUB_TOKEN", "")
    if not token:
        logger.warning("GITHUB_TOKEN manquant — sauvegarde locale uniquement")
        return False

    import base64
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Vérifier si le fichier existe (pour récupérer le SHA)
    sha = None
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            sha = r.json().get("sha")
    except Exception:
        pass

    payload = {
        "message": message,
        "content": base64.b64encode(content.encode()).decode(),
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    try:
        r = requests.put(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        logger.info(f"  → GitHub push OK : {file_path}")
        return True
    except Exception as e:
        logger.error(f"GitHub push failed: {e}")
        return False


def _ping_ga4(slug: str, title: str) -> bool:
    """Ping GA4 Measurement Protocol pour tracker la publication."""
    api_secret = os.getenv("GA4_API_SECRET", "")
    if not api_secret:
        return False

    payload = {
        "client_id": f"content_factory_{hashlib.md5(slug.encode()).hexdigest()[:8]}",
        "events": [{
            "name": "article_published",
            "params": {
                "article_slug": slug,
                "article_title": title[:100],
                "source": "content_factory",
                "engagement_time_msec": "1",
            }
        }]
    }
    try:
        r = requests.post(
            f"https://www.google-analytics.com/mp/collect?measurement_id={GA4_MEASUREMENT_ID}&api_secret={api_secret}",
            json=payload, timeout=10,
        )
        return r.status_code == 204
    except Exception:
        return False


def _update_blog_index(articles: list, github_token: str = None) -> bool:
    """Met à jour la page index du blog."""
    # Trier par date décroissante
    articles_sorted = sorted(articles, key=lambda a: a.get("date", ""), reverse=True)

    cards = ""
    for a in articles_sorted[:20]:
        tags_html = " ".join(f'<span class="badge badge-gray">{t}</span>' for t in a.get("tags", [])[:3])
        cards += f"""<a href="{a['slug']}.html" class="card" style="display:block">
  <div style="margin-bottom:.5rem">{tags_html}</div>
  <h3 style="margin-bottom:.4rem">{a.get('h1','')}</h3>
  <p style="font-size:.84rem;color:var(--ink3);margin:0">{a.get('meta_description','')[:120]}...</p>
  <p style="font-size:.75rem;color:var(--ink4);margin-top:.5rem">{a.get('date','')}</p>
</a>\n"""

    css = _read_shared_css()
    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Blog SAHAR Conseil — Open Data & Prospection Commerciale</title>
  <meta name="description" content="Guides, analyses et ressources sur l'open data, le scoring prospect, le CRM et la prospection commerciale pour les professionnels.">
  <link rel="canonical" href="{BASE_URL}/blog/index.html">
  <script>(function(w,d,s,l,i){{w[l]=w[l]||[];w[l].push({{'gtm.start':new Date().getTime(),event:'gtm.js'}});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);}})(window,document,'script','dataLayer','GTM-5WSR4DK5');</script>
  <script async src="https://www.googletagmanager.com/gtag/js?id={GA4_MEASUREMENT_ID}"></script>
  <script>window.dataLayer=window.dataLayer||[];function gtag(){{dataLayer.push(arguments)}}gtag('js',new Date());gtag('config','{GA4_MEASUREMENT_ID}');</script>
  <style>{css}</style>
</head>
<body>
{NAV}
<main id="main">
<section class="section" style="padding-top:4.5rem">
  <div class="container">
    <div class="breadcrumb"><a href="../index.html">Accueil</a><span class="breadcrumb-sep">→</span><span>Blog</span></div>
    <span class="overline">Ressources</span>
    <h1 style="margin-bottom:.75rem">Blog SAHAR Conseil</h1>
    <p class="lead">Open data, scoring prospect, CRM, prospection terrain.<br>{len(articles)} articles publiés.</p>
  </div>
</section>
<section class="section">
  <div class="container">
    <div class="grid-3">{cards}</div>
  </div>
</section>
</main>
{FOOTER}
</body>
</html>"""

    return html


def publish(article: dict, audio_data: dict) -> dict:
    """
    Point d'entrée principal.
    Génère HTML, push GitHub, ping GA4.
    """
    slug = article.get("slug", "article-" + datetime.now().strftime("%Y%m%d"))
    date_str = datetime.now().strftime("%d/%m/%Y")
    token = os.getenv("GITHUB_TOKEN", "")

    # 1. Générer HTML article
    html = _build_article_html(article, audio_data, date_str)

    # 2. Sauvegarder localement
    output_dir = Path(__file__).resolve().parents[1] / "output" / "articles"
    output_dir.mkdir(parents=True, exist_ok=True)
    local_path = output_dir / f"{slug}.html"
    local_path.write_text(html)
    logger.info(f"  → Sauvegarde locale : {local_path}")

    # 3. Push GitHub
    github_ok = _github_push(
        f"{BLOG_PATH}/{slug}.html",
        html,
        f"blog: {article.get('h1', slug)} [{date_str}]",
    )

    # 4. Mettre à jour index blog (lecture du manifest existant)
    manifest_path = Path(__file__).resolve().parents[1] / "output" / "blog_manifest.json"
    manifest = []
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception:
            pass

    manifest.append({
        "slug": slug,
        "h1": article.get("h1", ""),
        "meta_description": article.get("meta_description", ""),
        "tags": article.get("tags", []),
        "secteur": article.get("secteur", ""),
        "date": date_str,
        "word_count": article.get("word_count_actual", 0),
        "score_naturalite": article.get("_quality", {}).get("score_naturalite", 0),
    })
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

    # Générer et push le blog index
    blog_index_html = _update_blog_index(manifest)
    blog_index_path = Path(__file__).resolve().parents[1] / "output" / "blog_index.html"
    blog_index_path.write_text(blog_index_html)
    _github_push(f"{BLOG_PATH}/index.html", blog_index_html, f"blog: index mis à jour ({len(manifest)} articles)")

    # 5. Ping GA4
    ga4_ok = _ping_ga4(slug, article.get("h1", ""))

    result = {
        "slug": slug,
        "url": f"{BASE_URL}/blog/{slug}.html",
        "local_path": str(local_path),
        "github_published": github_ok,
        "ga4_pinged": ga4_ok,
        "date": date_str,
        "word_count": article.get("word_count_actual", 0),
        "score_naturalite": article.get("_quality", {}).get("score_naturalite", 0),
    }

    logger.info(
        f"Publication '{slug}': GitHub={'✅' if github_ok else '⚠️ local only'} "
        f"GA4={'✅' if ga4_ok else '—'}"
    )
    return result


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    print("Module publisher OK — nécessite GITHUB_TOKEN pour push")
