"""
SAHAR Conseil — Back-office CMS
Éditeur de contenu + Tags tendance + Prompt NotebookLM/Podcast
"""
import streamlit as st
import json
import os
import sys
from datetime import datetime, date
from pathlib import Path

# ── Paths ──
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from shared.auth import check_password

# ── Config ──
st.set_page_config(page_title="SAHAR Back-office", page_icon="⚙️", layout="wide")

CONTENT_DIR = ROOT / "content_factory" / "output"
BLOG_DIR = ROOT / "docs"
KEYWORDS_FILE = ROOT / "content_factory" / "keywords.json"
PROMPTS_DIR = ROOT / "content_factory" / "prompts"
PROMPTS_DIR.mkdir(parents=True, exist_ok=True)
CONTENT_DIR.mkdir(parents=True, exist_ok=True)

# ── Auth ──
if not check_password():
    st.stop()

# ── Sidebar Navigation ──
st.sidebar.image("https://via.placeholder.com/200x60?text=SAHAR+CMS", width=200)
page = st.sidebar.radio("Navigation", [
    "📝 Éditeur d'articles",
    "🏷️ Tags & Tendances",
    "🎙️ Prompt NotebookLM",
    "📊 Dashboard contenu",
    "⚙️ Configuration",
])


# ═══════════════════════════════════════════════════════════════════════════════
# 📝 ÉDITEUR D'ARTICLES
# ═══════════════════════════════════════════════════════════════════════════════
def page_editor():
    st.title("📝 Éditeur d'articles")

    col_list, col_edit = st.columns([1, 3])

    # ── Liste des articles existants ──
    with col_list:
        st.subheader("Articles")

        articles = []
        # Scan content_factory output
        for f in sorted(CONTENT_DIR.glob("*.json"), reverse=True):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                articles.append({"file": f, "data": data})
            except Exception:
                pass

        # Scan blog HTML
        for f in sorted(BLOG_DIR.glob("*.html"), reverse=True):
            if f.name in ("index.html", "immobilier.html", "energie-renovation.html"):
                continue
            articles.append({
                "file": f,
                "data": {"title": f.stem.replace("-", " ").title(), "format": "html", "path": str(f)}
            })

        if st.button("➕ Nouvel article", use_container_width=True):
            st.session_state["editing"] = "new"

        for i, art in enumerate(articles[:30]):
            title = art["data"].get("title", art["file"].stem)
            fmt = art["data"].get("format", "json")
            badge = "📄" if fmt == "html" else "📋"
            if st.button(f"{badge} {title[:40]}", key=f"art_{i}", use_container_width=True):
                st.session_state["editing"] = art

    # ── Zone d'édition ──
    with col_edit:
        editing = st.session_state.get("editing")

        if editing == "new":
            _editor_new_article()
        elif editing and isinstance(editing, dict):
            _editor_existing(editing)
        else:
            st.info("Sélectionnez un article ou créez-en un nouveau.")


def _editor_new_article():
    st.subheader("Nouvel article")

    # ── Métadonnées ──
    col1, col2, col3 = st.columns(3)
    with col1:
        title = st.text_input("Titre", placeholder="Prix immobilier 2026 : les villes qui montent")
    with col2:
        sector = st.selectbox("Secteur", ["immobilier", "energie", "retail", "auto", "sante", "rh", "general"])
    with col3:
        pub_date = st.date_input("Date de publication", value=date.today())

    # ── Tags ──
    tags_raw = st.text_input("Tags (séparés par des virgules)", placeholder="prix immobilier, tendances 2026, investissement")
    tags = [t.strip() for t in tags_raw.split(",") if t.strip()]

    # ── Suggestions de tags tendance ──
    _show_trend_suggestions(sector)

    # ── Éditeur de contenu ──
    st.markdown("### Contenu")
    content = st.text_area(
        "Corps de l'article (Markdown ou HTML)",
        height=400,
        placeholder="## Introduction\n\nVotre contenu ici...\n\n## Analyse\n\n...",
    )

    # ── Meta SEO ──
    with st.expander("🔍 SEO & Meta"):
        meta_desc = st.text_input("Meta description", placeholder="Résumé pour Google (max 160 car.)")
        slug = st.text_input("Slug URL", value=_slugify(title) if title else "")
        canonical = st.text_input("URL canonique", value=f"https://sahar-conseil.fr/{slug}" if slug else "")

    # ── Aperçu ──
    with st.expander("👁️ Aperçu"):
        st.markdown(f"# {title}")
        st.markdown(f"*{pub_date} — {sector}*")
        if tags:
            st.markdown(" ".join([f"`{t}`" for t in tags]))
        st.markdown("---")
        st.markdown(content)

    # ── Actions ──
    col_save, col_html, col_prompt = st.columns(3)
    with col_save:
        if st.button("💾 Sauvegarder brouillon", type="primary", use_container_width=True):
            _save_article(title, sector, tags, content, meta_desc, slug, pub_date, status="draft")
            st.success("Brouillon sauvegardé !")

    with col_html:
        if st.button("🌐 Publier en HTML", use_container_width=True):
            _publish_html(title, sector, tags, content, meta_desc, slug, pub_date)
            st.success(f"Publié : docs/{slug}.html")

    with col_prompt:
        if st.button("🎙️ Générer prompt podcast", use_container_width=True):
            prompt = _generate_podcast_prompt(title, sector, tags, content)
            st.session_state["last_podcast_prompt"] = prompt
            st.success("Prompt généré ! Voir l'onglet NotebookLM")


def _editor_existing(article):
    data = article["data"]
    f = article["file"]

    st.subheader(f"Édition : {data.get('title', f.stem)}")

    if f.suffix == ".html":
        content = f.read_text(encoding="utf-8")
        edited = st.text_area("Contenu HTML", content, height=500)
        if st.button("💾 Sauvegarder", type="primary"):
            f.write_text(edited, encoding="utf-8")
            st.success("Fichier HTML mis à jour !")
    else:
        # JSON article
        title = st.text_input("Titre", value=data.get("title", ""))
        sector = st.selectbox("Secteur", ["immobilier", "energie", "retail", "auto", "sante", "rh", "general"],
                              index=["immobilier", "energie", "retail", "auto", "sante", "rh", "general"].index(
                                  data.get("sector", "general")))
        tags = st.text_input("Tags", value=", ".join(data.get("tags", [])))
        body = st.text_area("Contenu", value=data.get("content", ""), height=400)
        meta = st.text_input("Meta description", value=data.get("meta_description", ""))

        if st.button("💾 Sauvegarder", type="primary"):
            data["title"] = title
            data["sector"] = sector
            data["tags"] = [t.strip() for t in tags.split(",")]
            data["content"] = body
            data["meta_description"] = meta
            data["updated_at"] = datetime.now().isoformat()
            f.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            st.success("Article mis à jour !")


# ═══════════════════════════════════════════════════════════════════════════════
# 🏷️ TAGS & TENDANCES
# ═══════════════════════════════════════════════════════════════════════════════
def page_tags():
    st.title("🏷️ Tags & Tendances")

    tab_manage, tab_trends = st.tabs(["Gestion des tags", "Tendances Google"])

    with tab_manage:
        _manage_keywords()

    with tab_trends:
        _google_trends_explorer()


def _manage_keywords():
    st.subheader("Mots-clés par secteur")

    keywords = _load_keywords()

    sectors = ["immobilier", "energie", "retail", "auto", "sante", "rh"]
    selected = st.selectbox("Secteur", sectors)

    current = keywords.get(selected, [])
    st.write(f"**{len(current)} mots-clés** pour {selected}")

    # Affichage en chips
    if current:
        cols = st.columns(6)
        for i, kw in enumerate(current):
            with cols[i % 6]:
                st.markdown(f"`{kw}`")

    # Ajout
    new_kw = st.text_input("Ajouter des mots-clés (virgules)", key="add_kw")
    if st.button("Ajouter") and new_kw:
        added = [k.strip() for k in new_kw.split(",") if k.strip()]
        current.extend(added)
        keywords[selected] = list(set(current))
        _save_keywords(keywords)
        st.success(f"+{len(added)} mots-clés ajoutés")
        st.rerun()

    # Suppression
    if current:
        to_remove = st.multiselect("Supprimer", current)
        if st.button("Supprimer sélection") and to_remove:
            keywords[selected] = [k for k in current if k not in to_remove]
            _save_keywords(keywords)
            st.success(f"-{len(to_remove)} mots-clés supprimés")
            st.rerun()


def _google_trends_explorer():
    st.subheader("Explorer les tendances Google")

    col1, col2 = st.columns(2)
    with col1:
        keywords_input = st.text_input("Mots-clés à comparer (max 5, virgules)", "rénovation énergétique, DPE, MaPrimeRénov")
    with col2:
        timeframe = st.selectbox("Période", ["today 1-m", "today 3-m", "today 12-m", "today 5-y"], index=1)

    if st.button("🔍 Analyser tendances", type="primary"):
        keywords_list = [k.strip() for k in keywords_input.split(",") if k.strip()][:5]

        try:
            from pytrends.request import TrendReq
            pytrends = TrendReq(hl="fr-FR", tz=60)
            pytrends.build_payload(keywords_list, cat=0, timeframe=timeframe, geo="FR")

            # Intérêt dans le temps
            iot = pytrends.interest_over_time()
            if not iot.empty:
                st.line_chart(iot[keywords_list])

            # Requêtes associées
            related = pytrends.related_queries()
            for kw in keywords_list:
                if kw in related and related[kw].get("rising") is not None:
                    with st.expander(f"🔥 Requêtes montantes : {kw}"):
                        st.dataframe(related[kw]["rising"].head(10), use_container_width=True)

            # Tendances du jour
            with st.expander("📈 Tendances du jour en France"):
                trending = pytrends.trending_searches(pn="france")
                st.dataframe(trending.head(20), use_container_width=True)

        except ImportError:
            st.warning("pytrends n'est pas installé. `pip install pytrends`")
        except Exception as e:
            st.error(f"Erreur Google Trends : {e}")

    # ── Suggestions auto ──
    st.markdown("---")
    st.subheader("💡 Suggestions automatiques")
    st.info("Les suggestions sont basées sur les tendances montantes croisées avec les secteurs SAHAR. "
            "Cliquez sur un tag pour l'ajouter à votre base de mots-clés.")

    # Suggestions prédéfinies par secteur (enrichies par trends)
    suggestions = {
        "immobilier": ["prix immobilier 2026", "taux crédit immobilier", "investissement locatif", "loi Pinel fin", "passoire thermique obligation"],
        "energie": ["MaPrimeRénov 2026", "pompe à chaleur prix", "audit énergétique obligatoire", "panneau solaire rentabilité", "DPE F G interdiction"],
        "retail": ["franchise rentable 2026", "commerce centre-ville", "dark store fermeture", "click and collect", "zone de chalandise IA"],
        "auto": ["voiture électrique occasion", "bonus écologique 2026", "borne recharge copropriété", "leasing social", "ZFE villes"],
        "sante": ["désert médical carte", "télémédecine remboursement", "pharmacie rurale", "maison de santé ouverture", "infirmier libéral installation"],
    }

    for sector, suggs in suggestions.items():
        with st.expander(f"{'🏠🔋🏪🚗🏥'[['immobilier','energie','retail','auto','sante'].index(sector)]} {sector.capitalize()}"):
            cols = st.columns(len(suggs))
            for i, s in enumerate(suggs):
                with cols[i]:
                    if st.button(f"+ {s}", key=f"sugg_{sector}_{i}"):
                        kws = _load_keywords()
                        kws.setdefault(sector, [])
                        if s not in kws[sector]:
                            kws[sector].append(s)
                            _save_keywords(kws)
                            st.success(f"'{s}' ajouté à {sector}")


# ═══════════════════════════════════════════════════════════════════════════════
# 🎙️ PROMPT NOTEBOOKLM / PODCAST
# ═══════════════════════════════════════════════════════════════════════════════
def page_podcast():
    st.title("🎙️ Prompt NotebookLM & Podcast")

    tab_gen, tab_lib, tab_settings = st.tabs(["Générer un prompt", "Bibliothèque de prompts", "Paramètres voix"])

    with tab_gen:
        _podcast_generator()

    with tab_lib:
        _podcast_library()

    with tab_settings:
        _podcast_settings()


def _podcast_generator():
    st.subheader("Générer un script podcast")

    # ── Source ──
    source = st.radio("Source du contenu", ["Saisie libre", "Depuis un article existant"], horizontal=True)

    if source == "Depuis un article existant":
        articles = list(CONTENT_DIR.glob("*.json"))
        if articles:
            selected_file = st.selectbox("Article", articles, format_func=lambda f: f.stem)
            data = json.loads(selected_file.read_text(encoding="utf-8"))
            title = data.get("title", "")
            sector = data.get("sector", "general")
            content = data.get("content", "")
        else:
            st.warning("Aucun article trouvé dans content_factory/output/")
            return
    else:
        title = st.text_input("Titre du sujet", placeholder="Les passoires thermiques en 2026")
        sector = st.selectbox("Secteur", ["immobilier", "energie", "retail", "auto", "sante", "rh"])
        content = st.text_area("Notes / contenu brut", height=200,
                               placeholder="Points clés à aborder dans le podcast...")

    # ── Paramètres du podcast ──
    st.markdown("### Paramètres")
    col1, col2, col3 = st.columns(3)
    with col1:
        duration = st.slider("Durée cible (minutes)", 5, 30, 12)
    with col2:
        tone = st.selectbox("Ton", ["Conversationnel & accessible", "Expert & analytique", "Dynamique & engageant", "Pédagogique & posé"])
    with col3:
        format_type = st.selectbox("Format", ["Monologue expert", "Dialogue 2 voix", "Interview fictive", "Débriefing data"])

    # ── Éléments à inclure ──
    with st.expander("🎯 Éléments à inclure"):
        include_data = st.checkbox("Chiffres & données open data", value=True)
        include_example = st.checkbox("Exemple concret / cas d'usage", value=True)
        include_cta = st.checkbox("Call-to-action SAHAR Conseil", value=True)
        include_trends = st.checkbox("Tendances Google Trends", value=False)
        custom_instructions = st.text_area("Instructions personnalisées", placeholder="Mentionner la ville de Bordeaux...")

    # ── Génération ──
    if st.button("🎙️ Générer le prompt", type="primary", use_container_width=True):
        prompt = _build_notebooklm_prompt(
            title=title, sector=sector, content=content,
            duration=duration, tone=tone, format_type=format_type,
            include_data=include_data, include_example=include_example,
            include_cta=include_cta, include_trends=include_trends,
            custom=custom_instructions
        )

        st.session_state["current_prompt"] = prompt

    # ── Affichage / édition du prompt ──
    prompt = st.session_state.get("current_prompt") or st.session_state.get("last_podcast_prompt", "")
    if prompt:
        st.markdown("### Prompt généré")
        edited_prompt = st.text_area("Modifier le prompt si besoin", prompt, height=400)

        col_copy, col_save = st.columns(2)
        with col_copy:
            st.code(edited_prompt, language=None)
            st.caption("⬆️ Copiez ce prompt et collez-le dans NotebookLM")

        with col_save:
            name = st.text_input("Nom du prompt", value=f"podcast_{_slugify(title)}_{date.today()}")
            if st.button("💾 Sauvegarder dans la bibliothèque"):
                save_path = PROMPTS_DIR / f"{name}.json"
                save_path.write_text(json.dumps({
                    "name": name,
                    "title": title,
                    "sector": sector,
                    "prompt": edited_prompt,
                    "duration": duration,
                    "tone": tone,
                    "format": format_type,
                    "created_at": datetime.now().isoformat(),
                }, ensure_ascii=False, indent=2), encoding="utf-8")
                st.success(f"Prompt sauvegardé : {save_path.name}")


def _podcast_library():
    st.subheader("Bibliothèque de prompts")

    prompts = list(PROMPTS_DIR.glob("*.json"))
    if not prompts:
        st.info("Aucun prompt sauvegardé. Générez-en un dans l'onglet précédent.")
        return

    for f in sorted(prompts, reverse=True):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            with st.expander(f"🎙️ {data.get('title', f.stem)} — {data.get('created_at', '')[:10]}"):
                st.markdown(f"**Secteur:** {data.get('sector')} | **Durée:** {data.get('duration')}min | **Ton:** {data.get('tone')}")
                st.code(data.get("prompt", ""), language=None)

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("📋 Utiliser ce prompt", key=f"use_{f.stem}"):
                        st.session_state["current_prompt"] = data["prompt"]
                        st.rerun()
                with col2:
                    if st.button("🗑️ Supprimer", key=f"del_{f.stem}"):
                        f.unlink()
                        st.success("Prompt supprimé")
                        st.rerun()
        except Exception:
            pass


def _podcast_settings():
    st.subheader("Paramètres voix & style")
    st.info("Ces paramètres sont intégrés automatiquement dans les prompts générés.")

    settings = _load_podcast_settings()

    settings["podcast_name"] = st.text_input("Nom du podcast", value=settings.get("podcast_name", "Data & Terrain"))
    settings["host_name"] = st.text_input("Nom de l'animateur", value=settings.get("host_name", "Tristan"))
    settings["intro_style"] = st.text_area("Style d'introduction",
                                           value=settings.get("intro_style",
                                                               "Bonjour et bienvenue dans Data & Terrain, le podcast qui transforme les données publiques en opportunités business."),
                                           height=80)
    settings["outro_style"] = st.text_area("Style de conclusion",
                                           value=settings.get("outro_style",
                                                               "Retrouvez tous nos outils sur sahar-conseil.fr. À la semaine prochaine !"),
                                           height=80)
    settings["brand_keywords"] = st.text_input("Mots-clés marque à mentionner",
                                               value=settings.get("brand_keywords", "SAHAR Conseil, open data, aide à la décision"))

    if st.button("💾 Sauvegarder les paramètres", type="primary"):
        _save_podcast_settings(settings)
        st.success("Paramètres sauvegardés !")


# ═══════════════════════════════════════════════════════════════════════════════
# 📊 DASHBOARD CONTENU
# ═══════════════════════════════════════════════════════════════════════════════
def page_dashboard():
    st.title("📊 Dashboard contenu")

    # Stats
    nb_articles = len(list(CONTENT_DIR.glob("*.json")))
    nb_html = len([f for f in BLOG_DIR.glob("*.html") if f.name not in ("index.html", "immobilier.html", "energie-renovation.html")])
    nb_prompts = len(list(PROMPTS_DIR.glob("*.json")))
    keywords = _load_keywords()
    nb_keywords = sum(len(v) for v in keywords.values())

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Articles JSON", nb_articles)
    col2.metric("Pages blog HTML", nb_html)
    col3.metric("Prompts podcast", nb_prompts)
    col4.metric("Mots-clés", nb_keywords)

    # Répartition par secteur
    st.subheader("Répartition des articles par secteur")
    sector_count = {}
    for f in BLOG_DIR.glob("*.html"):
        name = f.stem
        if "immobilier" in name or "prix" in name:
            sector_count["Immobilier"] = sector_count.get("Immobilier", 0) + 1
        elif "energie" in name or "thermique" in name or "passoire" in name:
            sector_count["Énergie"] = sector_count.get("Énergie", 0) + 1
        elif "commercial" in name or "attractivite" in name:
            sector_count["Retail"] = sector_count.get("Retail", 0) + 1
        elif "recrutement" in name or "tension" in name:
            sector_count["RH"] = sector_count.get("RH", 0) + 1
        elif "ve" in name or "potentiel-ve" in name:
            sector_count["Auto"] = sector_count.get("Auto", 0) + 1
        else:
            sector_count["Autre"] = sector_count.get("Autre", 0) + 1

    if sector_count:
        import pandas as pd
        df = pd.DataFrame(list(sector_count.items()), columns=["Secteur", "Articles"])
        st.bar_chart(df.set_index("Secteur"))

    # Mots-clés par secteur
    st.subheader("Mots-clés par secteur")
    if keywords:
        import pandas as pd
        kw_data = [{"Secteur": k, "Nombre": len(v)} for k, v in keywords.items()]
        st.dataframe(pd.DataFrame(kw_data), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# ⚙️ CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
def page_config():
    st.title("⚙️ Configuration")

    st.subheader("Chemins")
    st.code(f"""
Racine projet : {ROOT}
Content Factory : {CONTENT_DIR}
Blog (docs/)   : {BLOG_DIR}
Mots-clés      : {KEYWORDS_FILE}
Prompts podcast : {PROMPTS_DIR}
""")

    st.subheader("Template HTML pour publication")
    st.info("Le template ci-dessous est utilisé pour générer les pages blog HTML.")
    template = st.text_area("Template HTML", value=_get_html_template(), height=300)
    if st.button("💾 Sauvegarder le template"):
        (PROMPTS_DIR / "html_template.html").write_text(template, encoding="utf-8")
        st.success("Template sauvegardé !")


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _slugify(text: str) -> str:
    import re
    text = text.lower().strip()
    text = re.sub(r"[àáâã]", "a", text)
    text = re.sub(r"[èéêë]", "e", text)
    text = re.sub(r"[ìíîï]", "i", text)
    text = re.sub(r"[òóôõ]", "o", text)
    text = re.sub(r"[ùúûü]", "u", text)
    text = re.sub(r"[ç]", "c", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")[:80]


def _load_keywords() -> dict:
    if KEYWORDS_FILE.exists():
        try:
            return json.loads(KEYWORDS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_keywords(data: dict):
    KEYWORDS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_podcast_settings() -> dict:
    path = PROMPTS_DIR / "podcast_settings.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_podcast_settings(data: dict):
    path = PROMPTS_DIR / "podcast_settings.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _show_trend_suggestions(sector: str):
    """Affiche des suggestions de tags basées sur les tendances."""
    suggestions = {
        "immobilier": ["prix au m²", "passoire thermique", "investissement locatif", "taux emprunt", "DPE obligatoire"],
        "energie": ["MaPrimeRénov", "pompe à chaleur", "audit énergétique", "panneau solaire", "rénovation globale"],
        "retail": ["zone de chalandise", "franchise", "commerce proximité", "click and collect", "drive piéton"],
        "auto": ["voiture électrique", "bonus écologique", "ZFE", "borne recharge", "leasing social"],
        "sante": ["désert médical", "maison de santé", "télémédecine", "infirmier libéral", "pharmacie rurale"],
    }
    tags = suggestions.get(sector, [])
    if tags:
        st.caption(f"💡 Tendances {sector} : " + " · ".join([f"`{t}`" for t in tags]))


def _save_article(title, sector, tags, content, meta_desc, slug, pub_date, status="draft"):
    data = {
        "title": title,
        "sector": sector,
        "tags": tags,
        "content": content,
        "meta_description": meta_desc,
        "slug": slug,
        "published_date": str(pub_date),
        "status": status,
        "created_at": datetime.now().isoformat(),
    }
    fname = f"{pub_date}_{slug}.json"
    (CONTENT_DIR / fname).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _publish_html(title, sector, tags, content, meta_desc, slug, pub_date):
    """Génère un fichier HTML de blog et le place dans docs/."""
    import markdown
    html_content = markdown.markdown(content) if not content.strip().startswith("<") else content

    template = _get_html_template()
    html = template.replace("{{TITLE}}", title)
    html = html.replace("{{META_DESC}}", meta_desc or title)
    html = html.replace("{{SECTOR}}", sector)
    html = html.replace("{{DATE}}", str(pub_date))
    html = html.replace("{{TAGS}}", ", ".join(tags))
    html = html.replace("{{CONTENT}}", html_content)
    html = html.replace("{{SLUG}}", slug)
    html = html.replace("{{CANONICAL}}", f"https://sahar-conseil.fr/{slug}")

    (BLOG_DIR / f"{slug}.html").write_text(html, encoding="utf-8")
    _save_article(title, sector, tags, content, meta_desc, slug, pub_date, status="published")


def _get_html_template() -> str:
    tpl_path = PROMPTS_DIR / "html_template.html"
    if tpl_path.exists():
        return tpl_path.read_text(encoding="utf-8")

    return """<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{{TITLE}} — SAHAR Conseil</title>
  <meta name="description" content="{{META_DESC}}" />
  <link rel="canonical" href="{{CANONICAL}}" />
  <meta property="og:title" content="{{TITLE}}" />
  <meta property="og:description" content="{{META_DESC}}" />
  <meta property="og:type" content="article" />
  <meta property="og:url" content="{{CANONICAL}}" />
  <style>
    :root { --sage: #4A7C59; --dark: #2D3436; --bg: #FAFAF8; }
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: 'Inter', system-ui, sans-serif; background: var(--bg); color: var(--dark); line-height: 1.7; }
    .container { max-width: 740px; margin: 0 auto; padding: 2rem; }
    header { padding: 1rem 2rem; border-bottom: 1px solid #eee; }
    header a { color: var(--sage); text-decoration: none; font-weight: 700; font-size: 1.1rem; }
    h1 { font-family: 'DM Serif Display', Georgia, serif; font-size: 2.2rem; margin: 2rem 0 0.5rem; color: var(--dark); }
    .meta { color: #888; font-size: 0.9rem; margin-bottom: 2rem; }
    .tags span { background: var(--sage); color: white; padding: 2px 10px; border-radius: 12px; font-size: 0.8rem; margin-right: 6px; }
    article h2 { color: var(--sage); margin: 2rem 0 0.5rem; }
    article p { margin-bottom: 1rem; }
    footer { margin-top: 4rem; padding: 2rem; text-align: center; font-size: 0.8rem; color: #aaa; border-top: 1px solid #eee; }
  </style>
</head>
<body>
  <header><a href="/">SAHAR Conseil</a></header>
  <div class="container">
    <h1>{{TITLE}}</h1>
    <div class="meta">{{DATE}} · {{SECTOR}} <div class="tags">{{TAGS}}</div></div>
    <article>{{CONTENT}}</article>
  </div>
  <footer>© 2025-2026 SAHAR Conseil — <a href="/">Retour au site</a></footer>
</body>
</html>"""


def _build_notebooklm_prompt(title, sector, content, duration, tone, format_type,
                              include_data, include_example, include_cta, include_trends, custom):
    """Construit le prompt optimisé pour NotebookLM Audio Overview."""

    settings = _load_podcast_settings()
    podcast_name = settings.get("podcast_name", "Data & Terrain")
    host = settings.get("host_name", "Tristan")
    intro = settings.get("intro_style", "")
    outro = settings.get("outro_style", "")

    prompt = f"""# Prompt pour NotebookLM — Audio Overview
# Podcast : {podcast_name}
# Épisode : {title}
# Secteur : {sector}
# Durée cible : {duration} minutes
# Format : {format_type}
# Ton : {tone}

---

## INSTRUCTIONS POUR L'AUDIO OVERVIEW

Tu vas créer un épisode de podcast de {duration} minutes pour "{podcast_name}".

### Contexte
{podcast_name} est un podcast produit par SAHAR Conseil qui transforme les données publiques françaises (DVF, DPE, SIRENE, INSEE) en analyses actionables pour les professionnels.

L'animateur s'appelle {host}.

### Format : {format_type}
"""

    if format_type == "Dialogue 2 voix":
        prompt += """- Deux voix distinctes : un expert data et un professionnel du secteur
- Échanges naturels, questions-réponses, rebonds
- Pas de lecture robotique, simuler une vraie conversation\n"""
    elif format_type == "Interview fictive":
        prompt += f"""- {host} interviewe un expert du secteur {sector}
- Questions progressives du général au spécifique
- L'expert apporte des données chiffrées concrètes\n"""
    elif format_type == "Monologue expert":
        prompt += f"""- {host} présente seul le sujet de manière structurée
- Ton d'autorité bienveillante, comme un brief pro
- Transition fluide entre les parties\n"""
    else:
        prompt += """- Format débriefing : présentation de données puis analyse
- Mise en perspective avec des chiffres concrets
- Recommandations pratiques à la fin\n"""

    prompt += f"""
### Ton : {tone}

### Sujet principal
{title}

### Contenu source
{content[:2000] if content else "(Pas de contenu fourni — le podcast doit être basé sur le titre et le secteur)"}

### Structure attendue

1. **Introduction** ({max(1, duration//6)} min)
{f'   Style : {intro}' if intro else '   Accroche percutante avec un chiffre marquant'}

2. **Développement** ({max(2, duration*2//3)} min)
"""

    if include_data:
        prompt += """   - Intégrer des chiffres concrets issus de l'open data français
   - Citer les sources : DVF pour les prix, DPE pour l'énergie, SIRENE pour les entreprises\n"""
    if include_example:
        prompt += """   - Donner un exemple concret : une ville, un quartier, un cas réel
   - Montrer comment les données changent la donne pour un professionnel\n"""
    if include_trends:
        prompt += """   - Mentionner les tendances de recherche Google sur ce sujet
   - Expliquer pourquoi ce sujet intéresse de plus en plus\n"""

    prompt += f"""
3. **Conclusion & CTA** ({max(1, duration//6)} min)
"""
    if include_cta:
        prompt += """   - Mentionner SAHAR Conseil et ses outils d'aide à la décision
   - Inviter à tester les outils gratuits sur sahar-conseil.fr\n"""
    if outro:
        prompt += f"   Style : {outro}\n"

    if custom:
        prompt += f"""
### Instructions supplémentaires
{custom}
"""

    prompt += f"""
---
## RAPPELS IMPORTANTS
- Durée : {duration} minutes, ni plus ni moins
- Langue : français courant, professionnel mais accessible
- Éviter le jargon technique excessif
- Chaque affirmation importante doit être appuyée par un chiffre ou une source
- Le podcast doit donner envie d'agir, pas juste informer
"""
    return prompt


def _generate_podcast_prompt(title, sector, tags, content):
    """Raccourci pour générer un prompt depuis l'éditeur d'articles."""
    return _build_notebooklm_prompt(
        title=title, sector=sector, content=content,
        duration=12, tone="Conversationnel & accessible",
        format_type="Dialogue 2 voix",
        include_data=True, include_example=True,
        include_cta=True, include_trends=False,
        custom=""
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING
# ═══════════════════════════════════════════════════════════════════════════════

if page == "📝 Éditeur d'articles":
    page_editor()
elif page == "🏷️ Tags & Tendances":
    page_tags()
elif page == "🎙️ Prompt NotebookLM":
    page_podcast()
elif page == "📊 Dashboard contenu":
    page_dashboard()
elif page == "⚙️ Configuration":
    page_config()
