"""
SAHAR Conseil — Content Factory
Module 07 : Publisher (GitHub Pages + GA4)

Génère le fichier HTML final de l'article, le publie sur GitHub Pages
via l'API GitHub, et enregistre l'événement dans GA4 via Measurement Protocol.

Met à jour :
  - docs/blog/{slug}.html         (article publié)
  - docs/blog/index.html          (liste des articles)
  - docs/sitemap.xml              (nouvelle URL indexée)

Usage :
  python 07_publisher.py --slug "passoires-thermiques-interdites-2025"
  python 07_publisher.py --all    # publie tous les reviewed

Variables d'environnement requises :
  GITHUB_TOKEN           — Personal Access Token (repo scope)
  GA4_MEASUREMENT_ID     — G-XXXXXXXXXX
  GA4_API_SECRET         — secret Measurement Protocol
"""

import os
import re
import json
import base64
import argparse
import requests
from pathlib import Path
from datetime import datetime

HERE     = Path(__file__).parent.parent
ARTICLES = HERE / "output" / "articles"
CONFIG   = HERE / "config" / "keywords.json"
DOCS     = HERE.parent / "docs"

CFG      = json.loads(CONFIG.read_text())
BASE_URL = CFG["settings"].get("base_url", "https://sahar-conseil.fr")
BLOG_DIR = CFG["settings"].get("blog_dir", "blog")

GITHUB_TOKEN       = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO        = os.getenv("GITHUB_REPO", "tristanalba-ops/sahar-conseil")
GITHUB_BRANCH      = os.getenv("GITHUB_BRANCH", "main")
GA4_MEASUREMENT_ID = os.getenv("GA4_MEASUREMENT_ID", "G-XV2P0YPJK0")
GA4_API_SECRET     = os.getenv("GA4_API_SECRET", "")


# ─────────────────────────────────────────────────────────────────────────────
# TEMPLATE HTML ARTICLE
# ─────────────────────────────────────────────────────────────────────────────

def build_article_html(article_data: dict) -> str:
    """Génère le fichier HTML complet de l'article."""
    slug        = article_data["slug"]
    h1          = article_data.get("h1", article_data["keyword"])
    meta_desc   = article_data.get("meta_desc", "")
    secteur     = article_data.get("secteur", "")
    tags        = article_data.get("tags", [])
    article_body = article_data.get("article_html", "")
    word_count  = article_data.get("word_count", 0)
    date_str    = article_data.get("date_published",
                  datetime.now().strftime("%Y-%m-%d"))
    date_display = datetime.strptime(date_str[:10], "%Y-%m-%d").strftime("%d %B %Y")

    read_time  = max(3, round(word_count / 200))
    podcast_embed = article_data.get("podcast_embed", "")
    audio_url  = article_data.get("audio_url", "")

    # Bloc audio / podcast
    audio_block = ""
    if podcast_embed and "PLACEHOLDER" not in podcast_embed:
        audio_block = f"""
  <div class="podcast-widget" style="margin:2rem 0;padding:1.5rem;border:1px solid var(--bd);border-radius:12px;background:var(--bg2)">
    <p style="font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--ink3);margin-bottom:.75rem">🎙️ Écouter cet article</p>
    {podcast_embed}
  </div>"""
    elif audio_url:
        audio_block = f"""
  <div class="podcast-widget" style="margin:2rem 0;padding:1.5rem;border:1px solid var(--bd);border-radius:12px;background:var(--bg2)">
    <p style="font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--ink3);margin-bottom:.75rem">🎙️ Écouter cet article</p>
    <audio controls style="width:100%">
      <source src="{audio_url}" type="audio/mpeg">
    </audio>
  </div>"""

    # Tags HTML
    tags_html = " ".join(
        f'<a href="{BASE_URL}/{BLOG_DIR}/index.html?tag={t.lower().replace(" ","-")}" '
        f'class="badge badge-gray" style="text-decoration:none">{t}</a>'
        for t in tags
    ) if tags else ""

    # Schema Article
    schema = json.dumps({
        "@context": "https://schema.org",
        "@type":    "Article",
        "headline": h1,
        "description": meta_desc,
        "datePublished": date_str,
        "author": {"@type": "Organization", "name": "SAHAR Conseil"},
        "publisher": {
            "@type": "Organization",
            "name":  "SAHAR Conseil",
            "url":   BASE_URL,
        },
        "mainEntityOfPage": f"{BASE_URL}/{BLOG_DIR}/{slug}.html",
        "keywords": ", ".join(tags),
    }, ensure_ascii=False)

    # Lire le CSS depuis index.html
    index_path = DOCS / "index.html"
    css = ""
    if index_path.exists():
        idx = index_path.read_text()
        css_s = idx.find('<style>') + 7
        css_e = idx.find('</style>')
        if css_s > 6 and css_e > 0:
            css = idx[css_s:css_e]

    # CSS spécifique blog
    blog_css = """
.blog-article{max-width:720px;margin:0 auto}
.article-header{padding-bottom:2rem;border-bottom:1px solid var(--bd);margin-bottom:2.5rem}
.article-category{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:var(--blue);display:block;margin-bottom:.5rem}
.article-meta{font-size:.8rem;color:var(--ink3);margin:.5rem 0 1rem}
.article-intro{font-size:1.05rem;line-height:1.75;color:var(--ink2);margin-top:.75rem}
.article-body h2{font-size:1.2rem;font-weight:700;margin:2rem 0 .75rem;padding-top:.5rem;border-top:1px solid var(--bg3)}
.article-body h3{font-size:1rem;font-weight:600;margin:1.5rem 0 .5rem}
.article-body p{margin-bottom:1rem;line-height:1.75;color:var(--ink2)}
.article-body ul,.article-body ol{margin-bottom:1rem;padding-left:1.5rem}
.article-body li{margin-bottom:.4rem;color:var(--ink2);line-height:1.65}
.article-body table{width:100%;border-collapse:collapse;margin:1.5rem 0;font-size:.88rem}
.article-body table th{background:var(--bg2);padding:.6rem 1rem;text-align:left;font-weight:600;font-size:.78rem;border-bottom:2px solid var(--bd);color:var(--ink3);text-transform:uppercase;letter-spacing:.05em}
.article-body table td{padding:.6rem 1rem;border-bottom:1px solid var(--bg3);color:var(--ink2)}
.article-cta-box{background:var(--bg2);border:1px solid var(--bd);border-radius:12px;padding:2rem;text-align:center;margin:2.5rem 0}
.article-cta-box h3{margin-bottom:.5rem}
.article-cta-box p{color:var(--ink3);margin-bottom:1.25rem}
.article-footer{border-top:1px solid var(--bd);margin-top:2.5rem;padding-top:1.5rem}
.callout{background:var(--bg2);border-left:3px solid var(--ink);padding:1rem 1.25rem;border-radius:0 8px 8px 0;margin:1.5rem 0;font-size:.93rem}
.callout.blue{border-color:var(--blue);background:#e8f1fb}
"""

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>{h1} | SAHAR Conseil</title>
  <meta name="description" content="{meta_desc}">
  <meta name="robots" content="index, follow">
  <meta name="author" content="SAHAR Conseil">
  <meta property="og:type" content="article">
  <meta property="og:title" content="{h1}">
  <meta property="og:description" content="{meta_desc}">
  <meta property="og:url" content="{BASE_URL}/{BLOG_DIR}/{slug}.html">
  <link rel="canonical" href="{BASE_URL}/{BLOG_DIR}/{slug}.html">
  <script type="application/ld+json">{schema}</script>
  <!-- Google Tag Manager -->
  <script>(function(w,d,s,l,i){{w[l]=w[l]||[];w[l].push({{'gtm.start':new Date().getTime(),event:'gtm.js'}});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);}})(window,document,'script','dataLayer','GTM-5WSR4DK5');</script>
  <script async src="https://www.googletagmanager.com/gtag/js?id={GA4_MEASUREMENT_ID}"></script>
  <script>window.dataLayer=window.dataLayer||[];function gtag(){{dataLayer.push(arguments)}}gtag('js',new Date());gtag('config','{GA4_MEASUREMENT_ID}');gtag('event','page_view',{{page_title:'{h1}',page_location:'{BASE_URL}/{BLOG_DIR}/{slug}.html',content_type:'blog_article',content_category:'{secteur}'}});</script>
  <style>{css}{blog_css}</style>
</head>
<body>
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-5WSR4DK5" height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>

<a href="#main" class="skip">Aller au contenu</a>
<nav class="nav" aria-label="Navigation principale">
  <a href="{BASE_URL}/index.html" class="nav-logo"><span class="nav-logo-dot"></span>SAHAR Conseil</a>
  <ul class="nav-menu">
    <li><a href="{BASE_URL}/immobilier.html">Immobilier</a></li>
    <li><a href="{BASE_URL}/energie-renovation.html">Énergie</a></li>
    <li><a href="{BASE_URL}/retail-franchise.html">Retail</a></li>
    <li><a href="{BASE_URL}/rh-recrutement.html">RH</a></li>
    <li><a href="{BASE_URL}/crm.html">CRM</a></li>
    <li><a href="{BASE_URL}/{BLOG_DIR}/index.html" class="active">Blog</a></li>
  </ul>
  <div class="nav-actions"><a href="{BASE_URL}/index.html#contact" class="btn btn-primary btn-sm">Démo gratuite</a></div>
</nav>

<main id="main" style="padding:4rem 0">
  <div class="container">
    {audio_block}
    <div class="blog-article">
      {article_body}
      <div class="article-footer">
        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.75rem">
          <div style="display:flex;gap:.5rem;flex-wrap:wrap">{tags_html}</div>
          <a href="{BASE_URL}/{BLOG_DIR}/index.html" style="font-size:.82rem;color:var(--ink3)">← Tous les articles</a>
        </div>
      </div>
    </div>
  </div>
</main>

<footer class="footer">
  <div class="container">
    <div class="footer-grid">
      <div class="footer-brand"><strong style="font-size:.95rem;font-weight:700">SAHAR Conseil</strong><p>Open data au service des professionnels. DVF, DPE, INSEE, SIRENE transformés en pipeline commercial.</p></div>
      <div class="footer-col"><h4>Secteurs</h4><ul><li><a href="{BASE_URL}/immobilier.html">Immobilier DVF</a></li><li><a href="{BASE_URL}/energie-renovation.html">Énergie &amp; DPE</a></li><li><a href="{BASE_URL}/retail-franchise.html">Retail &amp; Franchise</a></li></ul></div>
      <div class="footer-col"><h4>Outils</h4><ul><li><a href="{BASE_URL}/crm.html">CRM Pipeline</a></li><li><a href="{BASE_URL}/scoring-prospects.html">Scoring Prospects</a></li></ul></div>
      <div class="footer-col"><h4>Blog</h4><ul><li><a href="{BASE_URL}/{BLOG_DIR}/index.html">Tous les articles</a></li><li><a href="{BASE_URL}/sitemap.xml">Sitemap</a></li></ul></div>
    </div>
    <div class="footer-bottom"><p>© 2024 SAHAR Conseil — Données DVF DGFiP, ADEME, INSEE</p></div>
  </div>
</footer>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# INDEX BLOG
# ─────────────────────────────────────────────────────────────────────────────

def build_blog_index(articles: list) -> str:
    """Génère la page index du blog."""
    cards = ""
    for a in sorted(articles, key=lambda x: x.get("date_published", ""), reverse=True)[:20]:
        date_str = a.get("date_published", "")[:10]
        try:
            date_disp = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
        except Exception:
            date_disp = date_str

        tags_html = " ".join(
            f'<span class="badge badge-gray">{t}</span>'
            for t in (a.get("tags", [])[:2])
        )
        cards += f"""
    <a href="{BASE_URL}/{BLOG_DIR}/{a['slug']}.html" style="display:block;border:1px solid var(--bd);border-radius:8px;padding:1.25rem;transition:border-color .15s;text-decoration:none" onmouseover="this.style.borderColor='#185FA5'" onmouseout="this.style.borderColor='var(--bd)'">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:.5rem">
        <span style="font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--blue)">{a.get('secteur','').upper()}</span>
        <span style="font-size:.75rem;color:var(--ink3)">{date_disp}</span>
      </div>
      <h3 style="font-size:1rem;font-weight:600;color:var(--ink);margin-bottom:.4rem;line-height:1.35">{a.get('h1', a['keyword'])}</h3>
      <p style="font-size:.83rem;color:var(--ink3);margin-bottom:.75rem;line-height:1.55">{a.get('meta_desc','')[:120]}...</p>
      <div style="display:flex;gap:.4rem;flex-wrap:wrap">{tags_html}</div>
    </a>"""

    # Lire le CSS depuis index.html
    index_path = DOCS / "index.html"
    css = ""
    if index_path.exists():
        idx = index_path.read_text()
        css_s = idx.find('<style>') + 7
        css_e = idx.find('</style>')
        if css_s > 6 and css_e > 0:
            css = idx[css_s:css_e]

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Blog SAHAR Conseil — Open Data, Prospection, Immobilier, Énergie</title>
  <meta name="description" content="Guides et analyses pour les professionnels qui prospectent avec les données publiques françaises. DVF, DPE, INSEE, CRM, scoring.">
  <meta name="robots" content="index, follow">
  <link rel="canonical" href="{BASE_URL}/{BLOG_DIR}/index.html">
  <script>(function(w,d,s,l,i){{w[l]=w[l]||[];w[l].push({{'gtm.start':new Date().getTime(),event:'gtm.js'}});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);}})(window,document,'script','dataLayer','GTM-5WSR4DK5');</script>
  <script async src="https://www.googletagmanager.com/gtag/js?id={GA4_MEASUREMENT_ID}"></script>
  <script>window.dataLayer=window.dataLayer||[];function gtag(){{dataLayer.push(arguments)}}gtag('js',new Date());gtag('config','{GA4_MEASUREMENT_ID}');</script>
  <style>{css}</style>
</head>
<body>
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-5WSR4DK5" height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
<nav class="nav">
  <a href="{BASE_URL}/index.html" class="nav-logo"><span class="nav-logo-dot"></span>SAHAR Conseil</a>
  <ul class="nav-menu">
    <li><a href="{BASE_URL}/immobilier.html">Immobilier</a></li>
    <li><a href="{BASE_URL}/energie-renovation.html">Énergie</a></li>
    <li><a href="{BASE_URL}/retail-franchise.html">Retail</a></li>
    <li><a href="{BASE_URL}/crm.html">CRM</a></li>
    <li><a href="{BASE_URL}/{BLOG_DIR}/index.html" class="active">Blog</a></li>
  </ul>
  <div class="nav-actions"><a href="{BASE_URL}/index.html#contact" class="btn btn-primary btn-sm">Démo gratuite</a></div>
</nav>
<main id="main">
  <section class="section" style="padding-top:4.5rem">
    <div class="container">
      <span class="overline">Blog</span>
      <h1 style="margin-bottom:.75rem">Prospection, données, terrain.</h1>
      <p class="lead" style="margin-bottom:2.5rem">Guides et analyses pour les professionnels qui utilisent les <strong>données publiques françaises</strong> pour prospecter.</p>
      <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:1rem">
        {cards}
      </div>
    </div>
  </section>
</main>
<footer class="footer">
  <div class="container">
    <div class="footer-bottom"><p>© 2024 SAHAR Conseil</p><div class="footer-legal"><a href="{BASE_URL}/sitemap.xml">Sitemap</a></div></div>
  </div>
</footer>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# GITHUB API
# ─────────────────────────────────────────────────────────────────────────────

def github_get_file_sha(path: str) -> str | None:
    """Récupère le SHA d'un fichier GitHub (nécessaire pour le mettre à jour)."""
    if not GITHUB_TOKEN:
        return None
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    r = requests.get(url, headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    })
    if r.status_code == 200:
        return r.json().get("sha")
    return None


def github_push_file(path: str, content: str, message: str) -> bool:
    """
    Crée ou met à jour un fichier sur GitHub.
    path = chemin relatif dans le repo (ex: "sahar-conseil/docs/blog/article.html")
    """
    if not GITHUB_TOKEN:
        print(f"   ⚠️  GITHUB_TOKEN absent — écriture locale uniquement")
        # Écriture locale en fallback
        local = DOCS.parent.parent / path
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(content)
        print(f"   💾 Écrit localement : {local}")
        return True

    url     = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    sha     = github_get_file_sha(path)
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")

    payload = {
        "message": message,
        "content": encoded,
        "branch":  GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, json=payload, headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github.v3+json",
    })

    if r.status_code in (200, 201):
        print(f"   ✅ GitHub : {path}")
        return True
    else:
        print(f"   ❌ GitHub error {r.status_code} : {r.text[:200]}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# GA4 MEASUREMENT PROTOCOL
# ─────────────────────────────────────────────────────────────────────────────

def track_publication_ga4(article_data: dict) -> None:
    """Envoie un événement 'article_published' à GA4."""
    if not GA4_API_SECRET:
        return

    url = f"https://www.google-analytics.com/mp/collect?measurement_id={GA4_MEASUREMENT_ID}&api_secret={GA4_API_SECRET}"

    payload = {
        "client_id":  "content_factory_bot",
        "events": [{
            "name": "article_published",
            "params": {
                "article_slug":    article_data["slug"],
                "article_keyword": article_data["keyword"],
                "article_score":   article_data.get("score", 0),
                "article_secteur": article_data.get("secteur", ""),
                "word_count":      article_data.get("word_count", 0),
                "has_audio":       bool(article_data.get("audio_url")),
            }
        }]
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 204:
            print("   📊 GA4 : événement publié")
    except Exception as e:
        print(f"   ⚠️  GA4 tracking error : {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SITEMAP
# ─────────────────────────────────────────────────────────────────────────────

def update_sitemap(new_urls: list) -> str:
    """Ajoute les nouvelles URLs au sitemap existant."""
    sitemap_path = DOCS / "sitemap.xml"
    existing_urls = []

    if sitemap_path.exists():
        content = sitemap_path.read_text()
        existing_urls = re.findall(r'<loc>(.*?)</loc>', content)

    all_urls = list(dict.fromkeys(existing_urls + new_urls))  # déduplique
    today    = datetime.now().strftime("%Y-%m-%d")

    entries = ""
    for url in all_urls:
        priority = "0.9" if "/blog/" in url else "0.7"
        entries += f"""  <url>
    <loc>{url}</loc>
    <lastmod>{today}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>{priority}</priority>
  </url>\n"""

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{entries}</urlset>"""


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATION PRINCIPALE
# ─────────────────────────────────────────────────────────────────────────────

def publish_article(article_data: dict) -> bool:
    """Pipeline complet de publication d'un article."""
    slug    = article_data["slug"]
    keyword = article_data["keyword"]

    print(f"\n🚀 Publication : {keyword}")

    # 1. Générer le HTML
    html = build_article_html(article_data)
    print(f"   → HTML généré ({len(html):,} chars)")

    # 2. Créer le dossier blog local
    blog_docs = DOCS / BLOG_DIR
    blog_docs.mkdir(exist_ok=True)

    # 3. Écrire localement
    local_path = blog_docs / f"{slug}.html"
    local_path.write_text(html)
    print(f"   → Écrit localement : {local_path}")

    # 4. Pousser sur GitHub
    github_path = f"sahar-conseil/docs/{BLOG_DIR}/{slug}.html"
    success = github_push_file(
        github_path, html,
        f"blog: publier article '{keyword[:50]}' (score {article_data.get('score', 0)})"
    )

    if not success:
        return False

    # 5. Mettre à jour l'index blog
    _refresh_blog_index()

    # 6. Mettre à jour le sitemap
    _refresh_sitemap(f"{BASE_URL}/{BLOG_DIR}/{slug}.html")

    # 7. Tracker dans GA4
    article_data["date_published"] = datetime.now().strftime("%Y-%m-%d")
    track_publication_ga4(article_data)

    # 8. Marquer comme publié
    article_data["status"] = "published"
    article_path = ARTICLES / f"{slug}.json"
    article_path.write_text(json.dumps(article_data, ensure_ascii=False, indent=2))

    print(f"   ✅ Publié : {BASE_URL}/{BLOG_DIR}/{slug}.html")
    return True


def _refresh_blog_index() -> None:
    """Régénère et publie l'index blog."""
    # Charger tous les articles publiés
    articles = []
    for f in ARTICLES.glob("*.json"):
        data = json.loads(f.read_text())
        if data.get("status") == "published":
            articles.append(data)

    if not articles:
        return

    html = build_blog_index(articles)

    # Écriture locale
    blog_docs = DOCS / BLOG_DIR
    blog_docs.mkdir(exist_ok=True)
    (blog_docs / "index.html").write_text(html)

    # GitHub
    github_push_file(
        f"sahar-conseil/docs/{BLOG_DIR}/index.html",
        html,
        f"blog: mettre à jour index ({len(articles)} articles)"
    )
    print(f"   📋 Index blog mis à jour ({len(articles)} articles)")


def _refresh_sitemap(new_url: str) -> None:
    """Met à jour le sitemap avec la nouvelle URL."""
    sitemap_content = update_sitemap([new_url])

    # Écriture locale
    (DOCS / "sitemap.xml").write_text(sitemap_content)

    # GitHub
    github_push_file(
        "sahar-conseil/docs/sitemap.xml",
        sitemap_content,
        f"seo: sitemap mis à jour — +{new_url.split('/')[-1]}"
    )
    print(f"   🗺️  Sitemap mis à jour")


def publish_all() -> None:
    """Publie tous les articles en statut reviewed."""
    files = [f for f in ARTICLES.glob("*.json")
             if json.loads(f.read_text()).get("status") == "reviewed"]

    print(f"📋 {len(files)} articles à publier\n")
    published = 0

    for f in files:
        data = json.loads(f.read_text())
        if publish_article(data):
            published += 1

    print(f"\n✅ {published}/{len(files)} articles publiés")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAHAR Content Factory — Module 07 Publisher")
    parser.add_argument("--slug",  type=str, help="Slug de l'article")
    parser.add_argument("--all",   action="store_true", help="Publier tous les reviewed")
    parser.add_argument("--index-only", action="store_true", help="Régénérer l'index seul")
    args = parser.parse_args()

    if args.slug:
        path = ARTICLES / f"{args.slug}.json"
        if not path.exists():
            print(f"Fichier non trouvé : {path}")
        else:
            data = json.loads(path.read_text())
            publish_article(data)
    elif args.all:
        publish_all()
    elif args.index_only:
        _refresh_blog_index()
    else:
        print("Usage : python 07_publisher.py --slug <slug> | --all | --index-only")
