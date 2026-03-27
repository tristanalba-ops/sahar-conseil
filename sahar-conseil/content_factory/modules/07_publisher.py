"""
SAHAR Conseil — Content Factory
Module 07 : Publication GitHub Pages + tracking GA4

Actions :
  1. Génère le HTML final de l'article (template blog SAHAR)
  2. Intègre le widget Spotify si disponible
  3. Push vers GitHub via API (commit direct, pas besoin de git local)
  4. Met à jour /docs/blog/index.html (listing articles)
  5. Met à jour sitemap.xml
  6. Ping GA4 Measurement Protocol (hit publication)

Usage :
  python 07_publisher.py --slug mon-article
  python 07_publisher.py --all
"""

import os
import re
import json
import base64
import hashlib
import argparse
import requests
from pathlib import Path
from datetime import datetime

HERE         = Path(__file__).parent.parent
OUT_ARTICLES = HERE / "output" / "articles"
OUT_AUDIO    = HERE / "output" / "audio"

# Config GitHub
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO  = os.getenv("GITHUB_REPO",  "tristanalba-ops/sahar-conseil")
GITHUB_BRANCH = "main"
DOCS_PREFIX  = "sahar-conseil/docs"  # préfixe dans le repo

# Config GA4
GA4_MEASUREMENT_ID = os.getenv("GA4_MEASUREMENT_ID", "G-XV2P0YPJK0")
GA4_API_SECRET     = os.getenv("GA4_API_SECRET", "")

# URLs
CFG      = json.loads((HERE / "config" / "keywords.json").read_text())
BASE_URL = CFG.get("settings", {}).get("base_url", "https://sahar-conseil.fr")
BLOG_DIR = CFG.get("settings", {}).get("blog_dir", "blog")


# ─────────────────────────────────────────────────────────────────────────────
# CSS BLOG (inline dans chaque article)
# ─────────────────────────────────────────────────────────────────────────────
BLOG_CSS = """
.blog-article{max-width:720px;margin:0 auto;padding:0 5%}
.article-header{padding:3.5rem 0 2.5rem;border-bottom:1px solid #e5e5e5;margin-bottom:2.5rem}
.article-header .overline{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#888;display:block;margin-bottom:.75rem}
.article-header h1{font-size:clamp(1.75rem,3.5vw,2.5rem);font-weight:700;letter-spacing:-.03em;line-height:1.1;color:#1a1a1a;margin-bottom:1rem}
.article-meta{display:flex;align-items:center;gap:1rem;flex-wrap:wrap;font-size:.8rem;color:#888}
.article-meta strong{color:#444}
.article-body{font-size:1.05rem;line-height:1.75;color:#333}
.article-body h2{font-size:1.35rem;font-weight:700;letter-spacing:-.02em;color:#1a1a1a;margin:2.5rem 0 .75rem;padding-top:1.5rem;border-top:1px solid #f0f0f0}
.article-body h2:first-of-type{border-top:none;padding-top:0}
.article-body h3{font-size:1.05rem;font-weight:600;color:#1a1a1a;margin:1.75rem 0 .5rem}
.article-body p{margin-bottom:1.1rem}
.article-body ul,.article-body ol{padding-left:1.4rem;margin-bottom:1.1rem}
.article-body li{margin-bottom:.4rem}
.article-body strong{font-weight:600;color:#1a1a1a}
.article-body a{color:#185FA5;border-bottom:1px solid rgba(24,95,165,.2);transition:border-color .12s}
.article-body a:hover{border-color:#185FA5}
.article-cta{background:#f8f8f8;border:1px solid #e5e5e5;border-radius:10px;padding:1.75rem;text-align:center;margin:2.5rem 0}
.article-cta p{color:#666;margin-bottom:1rem;font-size:.95rem}
.article-cta .btn{display:inline-flex;align-items:center;gap:.4rem;padding:.65rem 1.35rem;background:#1a1a1a;color:#fff;border-radius:7px;font-size:.9rem;font-weight:600;border:none;cursor:pointer;text-decoration:none}
.article-faq{margin:2.5rem 0}
.article-faq h2{font-size:1.2rem;margin-bottom:1.25rem}
.faq-item{border:1px solid #e5e5e5;border-radius:8px;margin-bottom:.75rem;overflow:hidden}
.faq-q{padding:.875rem 1rem;font-weight:600;font-size:.9rem;color:#1a1a1a;cursor:pointer;display:flex;justify-content:space-between;align-items:center}
.faq-q:after{content:"+";font-size:1.1rem;color:#888}
.faq-a{padding:0 1rem .875rem;font-size:.9rem;color:#444;line-height:1.65;display:none}
.podcast-widget{background:#1a1a1a;border-radius:10px;padding:1.25rem;margin:2.5rem 0;color:#fff}
.podcast-widget p{color:rgba(255,255,255,.7);font-size:.83rem;margin-bottom:.75rem}
.article-tags{display:flex;flex-wrap:wrap;gap:.4rem;margin-top:2.5rem;padding-top:1.5rem;border-top:1px solid #e5e5e5}
.article-tags a{font-size:.75rem;font-weight:600;padding:.2rem .6rem;border-radius:100px;background:#f2f2f2;color:#666;transition:background .12s}
.article-tags a:hover{background:#e5e5e5;border:none}
<script>
document.querySelectorAll('.faq-q').forEach(function(q){
  q.addEventListener('click',function(){
    var a=this.nextElementSibling;
    a.style.display=a.style.display==='block'?'none':'block';
    this.style.setProperty('--after-content',a.style.display==='block'?'"−"':'"+"');
  });
});
</script>
"""


# ─────────────────────────────────────────────────────────────────────────────
# TEMPLATE HTML ARTICLE
# ─────────────────────────────────────────────────────────────────────────────

def build_article_page(data: dict) -> str:
    """Génère le HTML complet de la page article pour GitHub Pages."""

    meta        = data.get("meta", {})
    title       = meta.get("title", data.get("keyword", "Article"))
    description = meta.get("description", "")
    pub_date    = meta.get("published_at", datetime.now().strftime("%Y-%m-%d"))
    reading_time= meta.get("reading_time", "5 min")
    tags        = meta.get("tags", [])
    slug        = meta.get("slug", data.get("slug", "article"))
    secteur     = data.get("secteur", "")
    article_html = data.get("html", "")

    # Date formatée FR
    from datetime import datetime as dt
    try:
        d = dt.strptime(pub_date, "%Y-%m-%d")
        date_fr = d.strftime("%-d %B %Y").replace(
            "January","janvier").replace("February","février").replace(
            "March","mars").replace("April","avril").replace("May","mai").replace(
            "June","juin").replace("July","juillet").replace("August","août").replace(
            "September","septembre").replace("October","octobre").replace(
            "November","novembre").replace("December","décembre")
    except Exception:
        date_fr = pub_date

    # Tags HTML
    tags_html = ""
    if tags:
        tags_html = '<div class="article-tags">' + "".join(
            f'<a href="../../blog/index.html?tag={t.lower().replace(" ","-")}">{t}</a>'
            for t in tags
        ) + "</div>"

    # Podcast widget
    podcast_html = ""
    podcast = data.get("podcast", {})
    spotify_id = podcast.get("spotify_episode_id", "")
    if spotify_id:
        podcast_html = f"""<div class="podcast-widget">
      <p>🎙️ Écouter cet article en podcast</p>
      <iframe style="border-radius:12px" src="https://open.spotify.com/embed/episode/{spotify_id}?utm_source=generator&theme=0"
        width="100%" height="152" frameBorder="0" allowfullscreen=""
        allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture"
        loading="lazy"></iframe>
    </div>"""
    elif podcast.get("status") == "ready":
        # Audio local sans ID Spotify encore
        podcast_html = """<div class="podcast-widget">
      <p>🎙️ Version podcast — disponible prochainement sur Spotify</p>
    </div>"""

    # Canonical URL
    canonical = f"{BASE_URL}/{BLOG_DIR}/{slug}.html"

    # Schema.org Article
    schema = json.dumps({
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": title,
        "description": description,
        "datePublished": pub_date,
        "author": {"@type": "Organization", "name": "SAHAR Conseil"},
        "publisher": {
            "@type": "Organization",
            "name": "SAHAR Conseil",
            "url": BASE_URL
        },
        "mainEntityOfPage": {"@type": "WebPage", "@id": canonical},
    })

    # Lire le CSS du site pour le nav/footer (si dispo)
    nav_html = """<a href="#main" class="skip">Aller au contenu</a>
<nav class="nav" aria-label="Navigation principale">
  <a href="../../index.html" class="nav-logo"><span class="nav-logo-dot"></span>SAHAR Conseil</a>
  <ul class="nav-menu">
    <li><a href="../../immobilier.html">Immobilier</a></li>
    <li><a href="../../energie-renovation.html">Énergie</a></li>
    <li><a href="../../retail-franchise.html">Retail</a></li>
    <li><a href="../../rh-recrutement.html">RH</a></li>
    <li><a href="../../crm.html">CRM</a></li>
    <li><a href="index.html" class="active">Blog</a></li>
  </ul>
  <div class="nav-actions"><a href="../../index.html#contact" class="btn btn-primary btn-sm">Démo gratuite</a></div>
</nav>"""

    footer_html = """<footer class="footer">
  <div class="container">
    <div class="footer-grid">
      <div class="footer-brand"><strong style="font-size:.95rem;font-weight:700">SAHAR Conseil</strong>
        <p>Open data au service des professionnels. DVF, DPE, INSEE, SIRENE transformés en pipeline commercial.</p></div>
      <div class="footer-col"><h4>Secteurs</h4><ul>
        <li><a href="../../immobilier.html">Immobilier DVF</a></li>
        <li><a href="../../energie-renovation.html">Énergie &amp; DPE</a></li>
        <li><a href="../../retail-franchise.html">Retail</a></li></ul></div>
      <div class="footer-col"><h4>Blog</h4><ul>
        <li><a href="index.html">Tous les articles</a></li>
        <li><a href="../../prospecter-donnees-publiques.html">Open data</a></li>
        <li><a href="../../scoring-prospects.html">Scoring</a></li></ul></div>
      <div class="footer-col"><h4>Contact</h4><ul>
        <li><a href="../../index.html#contact">Demander une démo</a></li>
        <li><a href="../../index.html#tarifs">Tarifs</a></li></ul></div>
    </div>
    <div class="footer-bottom"><p>© 2024-2025 SAHAR Conseil</p>
      <div class="footer-legal"><a href="../../index.html#contact">Contact</a><a href="../../sitemap.xml">Sitemap</a></div>
    </div>
  </div>
</footer>"""

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>{title} | SAHAR Conseil</title>
  <meta name="description" content="{description}">
  <meta name="robots" content="index, follow">
  <link rel="canonical" href="{canonical}">
  <meta property="og:type" content="article">
  <meta property="og:title" content="{title}">
  <meta property="og:description" content="{description}">
  <meta property="og:url" content="{canonical}">
  <meta property="article:published_time" content="{pub_date}">
  <script type="application/ld+json">{schema}</script>
  <!-- GTM -->
  <script>(function(w,d,s,l,i){{w[l]=w[l]||[];w[l].push({{'gtm.start':new Date().getTime(),event:'gtm.js'}});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);}})(window,document,'script','dataLayer','GTM-5WSR4DK5');</script>
  <script async src="https://www.googletagmanager.com/gtag/js?id={GA4_MEASUREMENT_ID}"></script>
  <script>window.dataLayer=window.dataLayer||[];function gtag(){{dataLayer.push(arguments)}}gtag('js',new Date());gtag('config','{GA4_MEASUREMENT_ID}');</script>
  <style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  html{{scroll-behavior:smooth}}
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;font-size:16px;line-height:1.65;color:#1a1a1a;background:#fff;-webkit-font-smoothing:antialiased}}
  a{{color:inherit;text-decoration:none}}
  :root{{--blue:#185FA5;--blue-l:#e8f1fb;--green:#1D9E75;--ink:#1a1a1a;--ink2:#444;--ink3:#888;--bd:#e5e5e5;--bg2:#f8f8f8;--r:8px;--r2:12px;--mw:900px}}
  .container{{max-width:var(--mw);margin:0 auto;padding:0 5%}}
  .section{{padding:5rem 0;border-bottom:1px solid var(--bd)}}
  .nav{{border-bottom:1px solid #e5e5e5;height:58px;display:flex;align-items:center;justify-content:space-between;padding:0 5%;position:sticky;top:0;background:rgba(255,255,255,.97);backdrop-filter:blur(8px);z-index:200}}
  .nav-logo{{display:flex;align-items:center;gap:.5rem;font-weight:700;font-size:.95rem;letter-spacing:-.01em;color:#1a1a1a}}
  .nav-logo-dot{{width:8px;height:8px;background:#185FA5;border-radius:50%;display:inline-block}}
  .nav-menu{{display:flex;list-style:none;gap:0}}
  .nav-menu a{{font-size:.84rem;font-weight:500;color:#444;padding:.4rem .8rem;border-radius:6px;transition:color .12s,background .12s;display:block}}
  .nav-menu a:hover,.nav-menu a.active{{color:#1a1a1a;background:#f8f8f8}}
  .nav-actions{{display:flex;align-items:center;gap:.5rem}}
  .btn{{display:inline-flex;align-items:center;gap:.45rem;padding:.65rem 1.35rem;border-radius:7px;font-size:.9rem;font-weight:600;cursor:pointer;border:none;transition:opacity .12s;line-height:1;white-space:nowrap}}
  .btn-primary{{background:#1a1a1a;color:#fff}}.btn-sm{{padding:.45rem .9rem;font-size:.82rem}}
  .skip{{position:absolute;left:-999px;top:0;padding:.5rem 1rem;background:#1a1a1a;color:#fff;font-size:.85rem;z-index:999}}.skip:focus{{left:0}}
  .overline{{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#888;display:block;margin-bottom:.75rem}}
  .breadcrumb{{display:flex;align-items:center;gap:.4rem;flex-wrap:wrap;font-size:.77rem;color:#888;margin-bottom:2rem}}
  .breadcrumb a{{color:#888}}.breadcrumb a:hover{{color:#1a1a1a}}.breadcrumb-sep{{color:#bbb}}
  .footer{{border-top:1px solid #e5e5e5;padding:3rem 0 2rem}}
  .footer-grid{{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:2.5rem;padding-bottom:2.5rem;border-bottom:1px solid #e5e5e5;margin-bottom:1.5rem}}
  .footer-brand p{{font-size:.82rem;color:#888;line-height:1.65;max-width:220px;margin-top:.5rem}}
  .footer-col h4{{font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#888;margin-bottom:.75rem}}
  .footer-col ul{{list-style:none;padding:0;display:flex;flex-direction:column;gap:.3rem}}
  .footer-col a{{font-size:.82rem;color:#888}}.footer-col a:hover{{color:#1a1a1a}}
  .footer-bottom{{display:flex;justify-content:space-between;flex-wrap:wrap;gap:.5rem}}
  .footer-bottom p,.footer-legal a{{font-size:.77rem;color:#bbb}}
  .footer-legal{{display:flex;gap:1.5rem}}
  @media(max-width:700px){{.nav-menu{{display:none}}.footer-grid{{grid-template-columns:1fr 1fr}}}}
  {BLOG_CSS}
  </style>
</head>
<body>
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-5WSR4DK5" height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
{nav_html}
<main id="main">
<div style="background:#f8f8f8;border-bottom:1px solid #e5e5e5;padding:1.25rem 0">
  <div class="container">
    <div class="breadcrumb">
      <a href="../../index.html">Accueil</a><span class="breadcrumb-sep">→</span>
      <a href="index.html">Blog</a><span class="breadcrumb-sep">→</span>
      <span>{secteur.capitalize()}</span>
    </div>
  </div>
</div>
<div style="padding:3rem 0 5rem">
  <div class="blog-article">
    {podcast_html}
    {article_html}
    {tags_html}
  </div>
</div>
</main>
{footer_html}
<script>
// FAQ accordion
document.querySelectorAll('.faq-q').forEach(function(q){{
  q.addEventListener('click',function(){{
    var a=this.nextElementSibling;
    if(a){{a.style.display=a.style.display==='block'?'none':'block'}}
  }});
}});
// GA4 scroll tracking
var s25=false,s50=false,s75=false;
window.addEventListener('scroll',function(){{
  var pct=window.scrollY/(document.body.scrollHeight-window.innerHeight)*100;
  if(!s25&&pct>=25){{s25=true;gtag('event','scroll',{{percent_scrolled:25}})}}
  if(!s50&&pct>=50){{s50=true;gtag('event','scroll',{{percent_scrolled:50}})}}
  if(!s75&&pct>=75){{s75=true;gtag('event','scroll',{{percent_scrolled:75}})}}
}});
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# GITHUB API
# ─────────────────────────────────────────────────────────────────────────────

def github_get_file_sha(path: str) -> str | None:
    """Récupère le SHA d'un fichier existant (nécessaire pour la mise à jour)."""
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    r = requests.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}, timeout=15)
    if r.status_code == 200:
        return r.json().get("sha")
    return None


def github_push_file(path: str, content: str, message: str) -> bool:
    """Push un fichier sur GitHub via l'API. Crée ou met à jour."""
    if not GITHUB_TOKEN:
        raise ValueError("GITHUB_TOKEN non défini")

    url     = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    sha     = github_get_file_sha(path)

    body = {
        "message": message,
        "content": encoded,
        "branch":  GITHUB_BRANCH,
    }
    if sha:
        body["sha"] = sha

    r = requests.put(
        url, json=body,
        headers={"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"},
        timeout=30,
    )

    if r.status_code in (200, 201):
        action = "mis à jour" if sha else "créé"
        print(f"   ✅ GitHub : {path} {action}")
        return True
    else:
        print(f"   ❌ GitHub {r.status_code} : {r.text[:200]}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# INDEX BLOG
# ─────────────────────────────────────────────────────────────────────────────

def build_blog_index(articles: list) -> str:
    """Génère la page index.html du blog avec la liste des articles."""

    cards = ""
    for a in sorted(articles, key=lambda x: x.get("meta", {}).get("published_at", ""), reverse=True):
        m    = a.get("meta", {})
        slug = m.get("slug", a.get("slug", ""))
        title = m.get("title", a.get("keyword", ""))
        desc  = m.get("description", "")
        date  = m.get("published_at", "")
        time_ = m.get("reading_time", "5 min")
        sect  = a.get("secteur", "")
        tags  = m.get("tags", [])
        score = a.get("score", {}).get("total", 0)

        tags_html = " ".join(
            f'<span style="font-size:.68rem;font-weight:700;padding:.15rem .5rem;background:#f2f2f2;color:#666;border-radius:100px">{t}</span>'
            for t in tags[:3]
        )

        cards += f"""
<a href="{slug}.html" style="display:block;border:1px solid #e5e5e5;border-radius:10px;padding:1.25rem;transition:border-color .15s" onmouseover="this.style.borderColor='#185FA5'" onmouseout="this.style.borderColor='#e5e5e5'">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:.5rem">
    <span style="font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#185FA5">{sect}</span>
    <span style="font-size:.72rem;color:#bbb">{date} · {time_}</span>
  </div>
  <h3 style="font-size:1rem;font-weight:600;color:#1a1a1a;line-height:1.35;margin-bottom:.4rem">{title}</h3>
  <p style="font-size:.83rem;color:#666;line-height:1.5;margin-bottom:.75rem">{desc[:120]}{"..." if len(desc)>120 else ""}</p>
  <div style="display:flex;gap:.35rem;flex-wrap:wrap">{tags_html}</div>
</a>"""

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Blog SAHAR Conseil — Prospection, Open Data, DVF, DPE</title>
  <meta name="description" content="Articles sur la prospection avec les données publiques françaises. DVF, DPE, INSEE, SIRENE. Méthodes, outils, cas d'usage pour les professionnels.">
  <link rel="canonical" href="{BASE_URL}/{BLOG_DIR}/">
  <script>(function(w,d,s,l,i){{w[l]=w[l]||[];w[l].push({{'gtm.start':new Date().getTime(),event:'gtm.js'}});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);}})(window,document,'script','dataLayer','GTM-5WSR4DK5');</script>
  <script async src="https://www.googletagmanager.com/gtag/js?id={GA4_MEASUREMENT_ID}"></script>
  <script>window.dataLayer=window.dataLayer||[];function gtag(){{dataLayer.push(arguments)}}gtag('js',new Date());gtag('config','{GA4_MEASUREMENT_ID}');</script>
  <style>*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}html{{scroll-behavior:smooth}}body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;font-size:16px;line-height:1.65;color:#1a1a1a;background:#fff;-webkit-font-smoothing:antialiased}}a{{color:inherit;text-decoration:none}}.container{{max-width:900px;margin:0 auto;padding:0 5%}}.nav{{border-bottom:1px solid #e5e5e5;height:58px;display:flex;align-items:center;justify-content:space-between;padding:0 5%;position:sticky;top:0;background:rgba(255,255,255,.97);backdrop-filter:blur(8px);z-index:200}}.nav-logo{{display:flex;align-items:center;gap:.5rem;font-weight:700;font-size:.95rem;color:#1a1a1a}}.nav-logo-dot{{width:8px;height:8px;background:#185FA5;border-radius:50%;display:inline-block}}.nav-menu{{display:flex;list-style:none;gap:0}}.nav-menu a{{font-size:.84rem;font-weight:500;color:#444;padding:.4rem .8rem;border-radius:6px;transition:color .12s,background .12s;display:block}}.nav-menu a:hover,.nav-menu a.active{{color:#1a1a1a;background:#f8f8f8}}.nav-actions{{display:flex;align-items:center;gap:.5rem}}.btn{{display:inline-flex;align-items:center;gap:.45rem;padding:.65rem 1.35rem;border-radius:7px;font-size:.9rem;font-weight:600;cursor:pointer;border:none;transition:opacity .12s;line-height:1;white-space:nowrap}}.btn-primary{{background:#1a1a1a;color:#fff}}.btn-sm{{padding:.45rem .9rem;font-size:.82rem}}.skip{{position:absolute;left:-999px;top:0;padding:.5rem 1rem;background:#1a1a1a;color:#fff;font-size:.85rem;z-index:999}}.skip:focus{{left:0}}.footer{{border-top:1px solid #e5e5e5;padding:3rem 0 2rem}}.footer-grid{{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:2.5rem;padding-bottom:2.5rem;border-bottom:1px solid #e5e5e5;margin-bottom:1.5rem}}.footer-brand p{{font-size:.82rem;color:#888;line-height:1.65;max-width:220px;margin-top:.5rem}}.footer-col h4{{font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#888;margin-bottom:.75rem}}.footer-col ul{{list-style:none;padding:0;display:flex;flex-direction:column;gap:.3rem}}.footer-col a{{font-size:.82rem;color:#888}}.footer-col a:hover{{color:#1a1a1a}}.footer-bottom{{display:flex;justify-content:space-between;flex-wrap:wrap;gap:.5rem}}.footer-bottom p,.footer-legal a{{font-size:.77rem;color:#bbb}}.footer-legal{{display:flex;gap:1.5rem}}@media(max-width:700px){{.nav-menu{{display:none}}.footer-grid{{grid-template-columns:1fr 1fr}}}}</style>
</head>
<body>
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-5WSR4DK5" height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
<a href="#main" class="skip">Aller au contenu</a>
<nav class="nav">
  <a href="../index.html" class="nav-logo"><span class="nav-logo-dot"></span>SAHAR Conseil</a>
  <ul class="nav-menu">
    <li><a href="../immobilier.html">Immobilier</a></li>
    <li><a href="../energie-renovation.html">Énergie</a></li>
    <li><a href="../retail-franchise.html">Retail</a></li>
    <li><a href="../rh-recrutement.html">RH</a></li>
    <li><a href="../crm.html">CRM</a></li>
    <li><a href="index.html" class="active">Blog</a></li>
  </ul>
  <div class="nav-actions"><a href="../index.html#contact" class="btn btn-primary btn-sm">Démo gratuite</a></div>
</nav>
<main id="main">
<section style="padding:4.5rem 0 3rem;border-bottom:1px solid #e5e5e5">
  <div class="container">
    <span style="font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#888;display:block;margin-bottom:.75rem">Blog SAHAR</span>
    <h1 style="font-size:clamp(1.75rem,3.5vw,2.5rem);font-weight:700;letter-spacing:-.03em;line-height:1.1;color:#1a1a1a;margin-bottom:.75rem">Prospection, données publiques, pipeline commercial.</h1>
    <p style="font-size:1.05rem;color:#444;line-height:1.75;max-width:560px">Méthodes terrain, analyses de marché et cas d'usage pour les professionnels qui utilisent <strong>DVF, DPE, INSEE et SIRENE</strong> dans leur prospection.</p>
  </div>
</section>
<section style="padding:3.5rem 0">
  <div class="container">
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1rem">
      {cards}
    </div>
  </div>
</section>
</main>
<footer class="footer">
  <div class="container">
    <div class="footer-grid">
      <div class="footer-brand"><strong style="font-size:.95rem;font-weight:700">SAHAR Conseil</strong><p>Open data au service des professionnels.</p></div>
      <div class="footer-col"><h4>Secteurs</h4><ul><li><a href="../immobilier.html">Immobilier DVF</a></li><li><a href="../energie-renovation.html">Énergie &amp; DPE</a></li><li><a href="../retail-franchise.html">Retail</a></li></ul></div>
      <div class="footer-col"><h4>Blog</h4><ul><li><a href="index.html">Tous les articles</a></li></ul></div>
      <div class="footer-col"><h4>Contact</h4><ul><li><a href="../index.html#contact">Démo gratuite</a></li></ul></div>
    </div>
    <div class="footer-bottom"><p>© 2024-2025 SAHAR Conseil</p><div class="footer-legal"><a href="../sitemap.xml">Sitemap</a></div></div>
  </div>
</footer>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# GA4 Measurement Protocol
# ─────────────────────────────────────────────────────────────────────────────

def ping_ga4_publication(slug: str, title: str) -> bool:
    """Envoie un event 'article_published' à GA4 via Measurement Protocol."""
    if not GA4_API_SECRET:
        return False  # silencieux si pas configuré

    url = f"https://www.google-analytics.com/mp/collect?measurement_id={GA4_MEASUREMENT_ID}&api_secret={GA4_API_SECRET}"
    body = {
        "client_id": f"content_factory_{hashlib.md5(slug.encode()).hexdigest()[:8]}",
        "events": [{
            "name": "article_published",
            "params": {
                "article_slug":  slug,
                "article_title": title[:100],
                "source":        "content_factory",
            }
        }]
    }
    try:
        r = requests.post(url, json=body, timeout=10)
        return r.status_code == 204
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────

def publish_article(slug: str) -> bool:
    """Pipeline complet de publication pour un article."""

    art_path = OUT_ARTICLES / f"{slug}.json"
    if not art_path.exists():
        print(f"❌ Article introuvable : {art_path}")
        return False

    data  = json.loads(art_path.read_text())
    meta  = data.get("meta", {})
    title = meta.get("title", data.get("keyword", slug))

    # Vérifier que l'article a passé le check IA
    ai_check = data.get("ai_check", {})
    if not ai_check:
        print(f"⚠️  Article {slug} n'a pas encore passé le check IA (module 05)")

    print(f"\n📤 Publication : {title}")

    # 1. Générer le HTML page complète
    page_html = build_article_page(data)

    # 2. Push article
    repo_path = f"{DOCS_PREFIX}/{BLOG_DIR}/{slug}.html"
    ok = github_push_file(
        repo_path,
        page_html,
        f"blog: article '{title[:50]}'"
    )
    if not ok:
        return False

    # 3. Marquer comme publié dans le JSON
    data["published"] = True
    data["published_at"] = datetime.now().isoformat()
    data["published_url"] = f"{BASE_URL}/{BLOG_DIR}/{slug}.html"
    art_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))

    # 4. Reconstruire l'index blog
    all_articles = []
    for f in OUT_ARTICLES.glob("*.json"):
        d = json.loads(f.read_text())
        if d.get("published"):
            all_articles.append(d)

    if all_articles:
        index_html = build_blog_index(all_articles)
        github_push_file(
            f"{DOCS_PREFIX}/{BLOG_DIR}/index.html",
            index_html,
            f"blog: mise à jour index ({len(all_articles)} articles)"
        )

    # 5. Ping GA4
    if ping_ga4_publication(slug, title):
        print(f"   📊 GA4 : event article_published envoyé")

    print(f"   🌐 URL : {BASE_URL}/{BLOG_DIR}/{slug}.html")
    return True


def publish_all(max_articles: int = 3) -> list:
    """Publie tous les articles checkés non encore publiés."""
    files = sorted(OUT_ARTICLES.glob("*.json"))
    done  = []
    for f in files:
        if len(done) >= max_articles:
            break
        data = json.loads(f.read_text())
        if data.get("ai_check") and not data.get("published"):
            if publish_article(f.stem):
                done.append(f.stem)
    print(f"\n✅ {len(done)} articles publiés")
    return done


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug", type=str)
    parser.add_argument("--all",  action="store_true")
    parser.add_argument("--max",  type=int, default=3)
    args = parser.parse_args()

    if args.slug:
        publish_article(args.slug)
    elif args.all:
        publish_all(args.max)
    else:
        print("Usage: python 07_publisher.py --slug <slug> | --all [--max 3]")
