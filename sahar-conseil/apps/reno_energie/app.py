"""
SAHAR Conseil — RénoÉnergie Pro v1
Outil de prospection rénovation énergétique pour artisans, courtiers MaPrimeRénov',
diagnostiqueurs DPE et bureaux d'études thermiques.

Détecte les zones à fort potentiel de chantiers, estime les volumes de travaux,
et génère des leads qualifiés.

Data : 876k+ DPE (ADEME) × DVF (transactions) × 101 départements.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import hashlib
import hmac
import time
import re
from collections import defaultdict

# ─── SECURITY ────────────────────────────────────────────────────────────────

# Rate limiting per session
if "rate_limiter" not in st.session_state:
    st.session_state["rate_limiter"] = {"requests": [], "blocked_until": 0}

def rate_limit(max_requests: int = 30, window_seconds: int = 60) -> bool:
    """Returns True if rate limit exceeded."""
    now = time.time()
    rl = st.session_state["rate_limiter"]
    if now < rl["blocked_until"]:
        return True
    rl["requests"] = [t for t in rl["requests"] if now - t < window_seconds]
    if len(rl["requests"]) >= max_requests:
        rl["blocked_until"] = now + 120
        return True
    rl["requests"].append(now)
    return False

def sanitize_input(text: str, max_length: int = 200) -> str:
    """Sanitize user text input."""
    if not isinstance(text, str):
        return ""
    text = text.strip()[:max_length]
    text = re.sub(r'[<>"\';(){}]', '', text)
    text = re.sub(r'(javascript|on\w+)\s*[:=]', '', text, flags=re.IGNORECASE)
    return text

def secure_password_check(input_pwd: str, expected_pwd: str) -> bool:
    """Timing-safe password comparison."""
    return hmac.compare_digest(
        hashlib.sha256(input_pwd.encode()).digest(),
        hashlib.sha256(expected_pwd.encode()).digest()
    )

# ─── CONFIG ──────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="RénoÉnergie Pro — SAHAR Conseil",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .main .block-container{padding-top:1rem;padding-bottom:1rem}
    .stMetric label{font-size:.78rem;color:#73726c}
    .stMetric [data-testid="stMetricValue"]{font-size:1.4rem}
    div[data-testid="stSidebarContent"]{padding-top:.75rem}
    .stTabs [data-baseweb="tab"]{font-size:.85rem}
    [data-testid="stExpander"]{border:1px solid #e5e5e5;border-radius:8px}
</style>
""", unsafe_allow_html=True)


# ─── AUTH ────────────────────────────────────────────────────────────────────

def check_auth():
    if st.session_state.get("auth_ok"):
        return True
    try:
        pwd_attendu = st.secrets["APP_PWD"]
    except Exception:
        return True
    with st.sidebar:
        st.markdown("### 🔐 Accès RénoÉnergie")
        pwd = st.text_input("Mot de passe", type="password", label_visibility="collapsed")
        if pwd:
            if secure_password_check(pwd, pwd_attendu):
                st.session_state["auth_ok"] = True
                st.rerun()
            else:
                st.error("Mot de passe incorrect")
                st.stop()
        else:
            st.info("Entrez votre mot de passe")
            st.stop()

check_auth()


# ─── CONSTANTES RÉNOVATION ──────────────────────────────────────────────────

# Coûts moyens de rénovation par étiquette DPE (€/m²)
COUT_RENO_M2 = {
    "G": 450,  # Rénovation lourde (isolation complète + chauffage + ventilation)
    "F": 300,  # Rénovation significative (isolation + chauffage)
    "E": 150,  # Rénovation légère (pompe à chaleur ou isolation partielle)
}

# MaPrimeRénov' moyennes par type de travaux (€)
MAPRIMERENO_MOY = {
    "G": 15000,  # Rénovation globale performance
    "F": 10000,  # Rénovation ampleur
    "E": 5000,   # Geste simple
}

# Surface moyenne logement France (m²)
SURFACE_MOY = 75

DEPTS = [
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "2A", "2B",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95",
    "971", "972", "973", "974", "976",
]

DEPT_NOMS = {
    "01": "Ain", "02": "Aisne", "03": "Allier", "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes", "06": "Alpes-Maritimes", "07": "Ardèche", "08": "Ardennes",
    "09": "Ariège", "10": "Aube", "11": "Aude", "12": "Aveyron",
    "13": "Bouches-du-Rhône", "14": "Calvados", "15": "Cantal", "16": "Charente",
    "17": "Charente-Maritime", "18": "Cher", "19": "Corrèze",
    "21": "Côte-d'Or", "22": "Côtes-d'Armor", "23": "Creuse",
    "24": "Dordogne", "25": "Doubs", "26": "Drôme", "27": "Eure",
    "28": "Eure-et-Loir", "29": "Finistère", "2A": "Corse-du-Sud", "2B": "Haute-Corse",
    "30": "Gard", "31": "Haute-Garonne", "32": "Gers", "33": "Gironde",
    "34": "Hérault", "35": "Ille-et-Vilaine", "36": "Indre", "37": "Indre-et-Loire",
    "38": "Isère", "39": "Jura", "40": "Landes", "41": "Loir-et-Cher",
    "42": "Loire", "43": "Haute-Loire", "44": "Loire-Atlantique", "45": "Loiret",
    "46": "Lot", "47": "Lot-et-Garonne", "48": "Lozère", "49": "Maine-et-Loire",
    "50": "Manche", "51": "Marne", "52": "Haute-Marne", "53": "Mayenne",
    "54": "Meurthe-et-Moselle", "55": "Meuse", "56": "Morbihan", "57": "Moselle",
    "58": "Nièvre", "59": "Nord", "60": "Oise", "61": "Orne",
    "62": "Pas-de-Calais", "63": "Puy-de-Dôme", "64": "Pyrénées-Atlantiques",
    "65": "Hautes-Pyrénées", "66": "Pyrénées-Orientales", "67": "Bas-Rhin",
    "68": "Haut-Rhin", "69": "Rhône", "70": "Haute-Saône", "71": "Saône-et-Loire",
    "72": "Sarthe", "73": "Savoie", "74": "Haute-Savoie", "75": "Paris",
    "76": "Seine-Maritime", "77": "Seine-et-Marne", "78": "Yvelines",
    "79": "Deux-Sèvres", "80": "Somme", "81": "Tarn", "82": "Tarn-et-Garonne",
    "83": "Var", "84": "Vaucluse", "85": "Vendée", "86": "Vienne",
    "87": "Haute-Vienne", "88": "Vosges", "89": "Yonne", "90": "Territoire de Belfort",
    "91": "Essonne", "92": "Hauts-de-Seine", "93": "Seine-Saint-Denis",
    "94": "Val-de-Marne", "95": "Val-d'Oise",
    "971": "Guadeloupe", "972": "Martinique", "973": "Guyane",
    "974": "La Réunion", "976": "Mayotte",
}


# ─── CHARGEMENT DONNÉES ─────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def load_dpe_communes(departement: str = None) -> pd.DataFrame:
    """Charge l'agrégation DPE par commune depuis le parquet local ou Supabase."""
    # Parquet local
    bases = [
        Path(__file__).resolve().parents[2],
        Path("/mount/src/sahar-conseil/sahar-conseil"),
        Path("/mount/src/sahar-conseil"),
    ]
    for base in bases:
        p = base / "data" / "processed" / "dpe_communes_agg.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            if departement:
                df = df[df["departement"] == departement]
            return df

    # Fallback Supabase
    try:
        from shared.supabase_dpe import get_dpe_communes
        return get_dpe_communes(departement)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_dpe_logements(departement: str, etiquettes=None, commune=None, limit=5000):
    """Charge les logements DPE individuels depuis Supabase."""
    try:
        from shared.supabase_dpe import get_dpe_logements
        return get_dpe_logements(departement, etiquettes=etiquettes, commune=commune, limit=limit)
    except Exception:
        return pd.DataFrame()


def compute_reno_score(row):
    """Score de potentiel rénovation 0-100 pour une commune."""
    # Poids : concentration passoires (40%) + volume absolu (30%) + conso moyenne (30%)
    return 0  # calculé en batch ci-dessous


def enrich_communes(df: pd.DataFrame) -> pd.DataFrame:
    """Enrichit les données communes avec les estimations rénovation."""
    if df.empty:
        return df

    df = df.copy()

    # Nombre de passoires F+G
    df["nb_fg"] = df["nb_f"].fillna(0) + df["nb_g"].fillna(0)

    # Estimation volume travaux par commune
    df["volume_travaux_f"] = df["nb_f"].fillna(0) * SURFACE_MOY * COUT_RENO_M2["F"]
    df["volume_travaux_g"] = df["nb_g"].fillna(0) * SURFACE_MOY * COUT_RENO_M2["G"]
    df["volume_travaux_total"] = df["volume_travaux_f"] + df["volume_travaux_g"]

    # Estimation MaPrimeRénov' mobilisable
    df["maprimerenov_f"] = df["nb_f"].fillna(0) * MAPRIMERENO_MOY["F"]
    df["maprimerenov_g"] = df["nb_g"].fillna(0) * MAPRIMERENO_MOY["G"]
    df["maprimerenov_total"] = df["maprimerenov_f"] + df["maprimerenov_g"]

    # Chiffre d'affaires potentiel artisan (30% du volume travaux = marge artisan)
    df["ca_potentiel"] = (df["volume_travaux_total"] * 0.30).round(0)

    # Score rénovation 0-100
    def _norm(s):
        mn, mx = s.min(), s.max()
        return pd.Series(50, index=s.index) if mx == mn else ((s - mn) / (mx - mn) * 100)

    s_pct = _norm(df["pct_fg"].fillna(0))        # Concentration passoires
    s_vol = _norm(df["nb_fg"].fillna(0))           # Volume absolu
    s_conso = _norm(df["conso_moy"].fillna(0))     # Consommation moyenne

    df["score_reno"] = (s_pct * 0.40 + s_vol * 0.30 + s_conso * 0.30).round(0).clip(0, 100).astype(int)

    # Signal
    df["signal"] = df["score_reno"].apply(
        lambda s: "🟢 Fort potentiel" if s >= 70
        else "🟡 Potentiel modéré" if s >= 40
        else "🔴 Faible potentiel"
    )

    return df.sort_values("score_reno", ascending=False)


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        '<div style="text-align:center;padding:.5rem 0">'
        '<span style="font-size:1.4rem;font-weight:700">SAHAR</span>'
        '<span style="font-size:.8rem;color:#666;margin-left:.3rem">RénoÉnergie</span>'
        '</div>', unsafe_allow_html=True
    )
    st.markdown("---")

    with st.expander("📍 Zone de prospection", expanded=True):
        dept = st.selectbox(
            "Département", options=DEPTS,
            index=DEPTS.index("33"),
            format_func=lambda d: f"{d} — {DEPT_NOMS.get(d, d)}",
        )
        ville_filter = st.text_input("Filtrer par ville", "", key="reno_ville")

    with st.expander("🎯 Cibles", expanded=True):
        cible_etiquettes = st.multiselect(
            "Étiquettes DPE ciblées",
            ["E", "F", "G"],
            default=["F", "G"],
            help="F+G = passoires thermiques (obligation de rénovation)"
        )
        score_min = st.slider("Score potentiel minimum", 0, 100, 30, 5)

    with st.expander("💼 Profil métier", expanded=False):
        profil = st.radio(
            "Je suis...",
            ["Artisan / Entreprise RGE", "Courtier MaPrimeRénov'",
             "Diagnostiqueur DPE", "Bureau d'études thermiques", "Autre"],
            key="profil_metier"
        )
        rayon_km = st.slider("Rayon d'intervention (km)", 5, 100, 30, 5)

    st.markdown("---")
    if st.button("🔄 Rafraîchir", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ─── CHARGEMENT ──────────────────────────────────────────────────────────────

with st.spinner("Chargement des données DPE..."):
    df_communes = load_dpe_communes(dept)

if df_communes.empty:
    st.warning(f"Pas de données DPE pour le département {dept}.")
    st.stop()

# Enrichissement
df_communes = enrich_communes(df_communes)

# Filtre ville
if ville_filter:
    df_communes = df_communes[df_communes["commune"].str.contains(ville_filter, case=False, na=False)]

# Filtre score
df_communes = df_communes[df_communes["score_reno"] >= score_min]

if df_communes.empty:
    st.warning("Aucune commune avec ces critères.")
    st.stop()


# ─── HEADER ──────────────────────────────────────────────────────────────────

_total_fg = int(df_communes["nb_fg"].sum())
_total_volume = df_communes["volume_travaux_total"].sum()
_total_mpr = df_communes["maprimerenov_total"].sum()
_total_ca = df_communes["ca_potentiel"].sum()
_nb_communes = len(df_communes)

st.markdown(
    f'<div style="display:flex;align-items:baseline;gap:1rem;margin-bottom:.5rem">'
    f'<span style="font-size:1.5rem;font-weight:700">🔧 RénoÉnergie Pro</span>'
    f'<span style="color:#888;font-size:.85rem">Dept {dept} — {DEPT_NOMS.get(dept, dept)} — '
    f'{_nb_communes} communes analysées</span>'
    f'</div>', unsafe_allow_html=True
)

# KPIs principaux
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Passoires F+G", f"{_total_fg:,}")
k2.metric("Volume travaux estimé", f"{_total_volume/1_000_000:,.1f} M€")
k3.metric("MaPrimeRénov' mobilisable", f"{_total_mpr/1_000_000:,.1f} M€")
k4.metric("CA potentiel artisan", f"{_total_ca/1_000_000:,.1f} M€")
k5.metric("Score moyen", f"{df_communes['score_reno'].mean():.0f}/100")


# ─── ONGLETS ─────────────────────────────────────────────────────────────────

tab_zones, tab_carte, tab_leads, tab_commune, tab_export = st.tabs([
    "🏆 Top zones", "🗺️ Carte", "📋 Leads qualifiés", "🔍 Détail commune", "📥 Export"
])


# ── TAB 1 : TOP ZONES ───────────────────────────────────────────────────────

with tab_zones:
    st.markdown("### 🏆 Zones à plus fort potentiel de rénovation")
    st.caption("Classement des communes par score de potentiel — concentration passoires × volume × consommation")

    nb_affiche = st.slider("Communes affichées", 10, 100, 30, 5, key="zones_nb")

    col_chart, col_table = st.columns([1, 1])

    with col_chart:
        try:
            import plotly.express as px
            top = df_communes.head(min(20, nb_affiche)).copy()
            fig = px.bar(
                top, x="score_reno", y="commune",
                orientation="h",
                color="volume_travaux_total",
                color_continuous_scale=["#2196F3", "#FF9800", "#F44336"],
                labels={"score_reno": "Score rénovation", "commune": "",
                        "volume_travaux_total": "Volume travaux €"},
                title=f"Top {len(top)} communes — Potentiel rénovation",
                text="score_reno",
            )
            fig.update_layout(
                height=550, yaxis=dict(autorange="reversed"),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0, r=0, t=35, b=0)
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            st.info("Installez plotly pour les graphiques")

    with col_table:
        display_cols = ["commune", "score_reno", "nb_fg", "pct_fg",
                        "volume_travaux_total", "maprimerenov_total", "ca_potentiel",
                        "conso_moy", "signal"]
        df_display = df_communes.head(nb_affiche)[
            [c for c in display_cols if c in df_communes.columns]
        ].copy()

        # Format monétaire
        for col in ["volume_travaux_total", "maprimerenov_total", "ca_potentiel"]:
            if col in df_display.columns:
                df_display[col] = df_display[col].apply(lambda x: f"{x:,.0f} €" if pd.notna(x) else "—")

        df_display = df_display.rename(columns={
            "commune": "Commune", "score_reno": "Score", "nb_fg": "F+G",
            "pct_fg": "% F+G", "volume_travaux_total": "Volume travaux",
            "maprimerenov_total": "MaPrimeRénov'", "ca_potentiel": "CA artisan",
            "conso_moy": "Conso moy", "signal": "Signal",
        })

        st.dataframe(df_display, use_container_width=True, height=550, hide_index=True)

    # Graphique scatter volume × score
    st.markdown("---")
    st.markdown("##### Volume travaux vs Score — Taille = nb passoires F+G")
    try:
        import plotly.express as px
        fig2 = px.scatter(
            df_communes.head(100), x="volume_travaux_total", y="score_reno",
            size="nb_fg", color="pct_fg",
            color_continuous_scale="YlOrRd",
            hover_name="commune",
            hover_data={"ca_potentiel": ":,.0f", "maprimerenov_total": ":,.0f"},
            labels={"volume_travaux_total": "Volume travaux €", "score_reno": "Score",
                    "nb_fg": "Passoires F+G", "pct_fg": "% F+G"},
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)
    except ImportError:
        pass


# ── TAB 2 : CARTE ───────────────────────────────────────────────────────────

with tab_carte:
    st.markdown("### 🗺️ Carte du potentiel rénovation")

    try:
        import folium
        from folium.plugins import HeatMap
        from streamlit_folium import st_folium

        mode_carte = st.radio("Mode", ["Bulles (score)", "Heatmap (volume)"], horizontal=True)

        # On a besoin de coordonnées. Utiliser les logements DPE pour centrer la carte
        # et la commune comme agrégation
        # Pour le centre carte, on prend les coordonnées moyennes des logements
        dpe_sample = load_dpe_logements(dept, etiquettes=["F", "G"], limit=2000)

        if not dpe_sample.empty and "latitude" in dpe_sample.columns:
            lat_c = dpe_sample["latitude"].dropna().median()
            lon_c = dpe_sample["longitude"].dropna().median()
        else:
            # Fallback centres départements approximatifs
            lat_c, lon_c = 46.5, 2.5

        m = folium.Map(location=[lat_c, lon_c], zoom_start=10, tiles="CartoDB positron")

        if mode_carte == "Heatmap (volume)" and not dpe_sample.empty:
            coords = dpe_sample.dropna(subset=["latitude", "longitude"])
            heat_data = [[r["latitude"], r["longitude"]] for _, r in coords.iterrows()]
            HeatMap(heat_data, radius=12, blur=10).add_to(m)
            st.caption("Heatmap = densité de passoires thermiques F+G")

        elif not dpe_sample.empty:
            # Agréger par commune pour les bulles
            coords_agg = dpe_sample.dropna(subset=["latitude", "longitude"]).groupby("commune").agg(
                lat=("latitude", "median"),
                lon=("longitude", "median"),
                nb=("numero_dpe", "count"),
            ).reset_index()

            # Joindre avec scores
            coords_agg = coords_agg.merge(
                df_communes[["commune", "score_reno", "nb_fg", "volume_travaux_total", "ca_potentiel"]],
                on="commune", how="inner"
            )

            for _, r in coords_agg.iterrows():
                score = r.get("score_reno", 0)
                color = "#1D9E75" if score >= 70 else "#FF9800" if score >= 40 else "#E24B4A"
                radius = max(5, min(20, r.get("nb_fg", 0) / 10))

                popup = f"""
                <div style='font-family:sans-serif;font-size:11px;min-width:200px'>
                  <b>{r['commune']}</b><br>
                  <b>Score réno :</b> <span style='color:{color}'>{score}/100</span><br>
                  <b>Passoires F+G :</b> {r.get('nb_fg', 0):,.0f}<br>
                  <b>Volume travaux :</b> {r.get('volume_travaux_total', 0):,.0f} €<br>
                  <b>CA potentiel :</b> {r.get('ca_potentiel', 0):,.0f} €<br>
                  <a href='https://www.google.com/maps/search/?api=1&query={r["lat"]},{r["lon"]}' target='_blank'>📍 Google Maps</a>
                </div>
                """
                folium.CircleMarker(
                    [r["lat"], r["lon"]], radius=radius,
                    color=color, fill=True, fill_color=color, fill_opacity=0.7,
                    tooltip=f"{r['commune']} — Score {score} — {r.get('nb_fg', 0)} F+G",
                    popup=folium.Popup(popup, max_width=250),
                ).add_to(m)

            # Légende
            col_l = st.columns(3)
            with col_l[0]: st.markdown("🟢 Score ≥ 70 (fort potentiel)")
            with col_l[1]: st.markdown("🟡 Score 40–70 (modéré)")
            with col_l[2]: st.markdown("🔴 Score < 40 (faible)")

        st_folium(m, height=550, use_container_width=True)

    except ImportError:
        st.warning("Installer folium et streamlit-folium pour la carte.")


# ── TAB 3 : LEADS QUALIFIÉS ─────────────────────────────────────────────────

with tab_leads:
    st.markdown("### 📋 Leads qualifiés — Logements à rénover")
    st.caption("Liste des logements F+G individuels avec adresse, score d'urgence et estimation travaux")

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        commune_lead = st.selectbox(
            "Commune",
            ["Toutes"] + df_communes["commune"].tolist(),
            key="leads_commune"
        )
    with col_f2:
        etiq_lead = st.multiselect("Étiquettes", ["F", "G"], default=["F", "G"], key="leads_etiq")
    with col_f3:
        nb_leads = st.slider("Nb leads max", 50, 500, 100, 50, key="leads_nb")

    with st.spinner("Chargement des logements..."):
        _commune_arg = commune_lead if commune_lead != "Toutes" else None
        df_leads = load_dpe_logements(dept, etiquettes=etiq_lead, commune=_commune_arg, limit=nb_leads)

    if df_leads.empty:
        st.info("Aucun logement trouvé avec ces critères.")
    else:
        # Enrichir avec estimations travaux
        df_leads = df_leads.copy()
        df_leads["cout_reno_estime"] = df_leads.apply(
            lambda r: (r.get("surface_immeuble", SURFACE_MOY) or SURFACE_MOY) * COUT_RENO_M2.get(r.get("etiquette_dpe", "F"), 300),
            axis=1
        ).round(0)
        df_leads["mpr_estime"] = df_leads["etiquette_dpe"].map(MAPRIMERENO_MOY).fillna(0)
        df_leads["reste_a_charge"] = (df_leads["cout_reno_estime"] - df_leads["mpr_estime"]).clip(0)

        # KPIs leads
        lk1, lk2, lk3, lk4 = st.columns(4)
        lk1.metric("Leads qualifiés", len(df_leads))
        lk2.metric("Volume travaux total", f"{df_leads['cout_reno_estime'].sum():,.0f} €")
        lk3.metric("MaPrimeRénov' total", f"{df_leads['mpr_estime'].sum():,.0f} €")
        lk4.metric("Score urgence moyen", f"{df_leads['score_urgence'].mean():.0f}/100" if "score_urgence" in df_leads.columns else "—")

        st.markdown("---")

        # Tableau leads
        show_cols = []
        for c in ["adresse", "commune", "code_postal", "etiquette_dpe", "score_urgence",
                   "conso_par_m2", "emission_ges", "type_batiment", "periode_construction",
                   "energie_principale", "cout_reno_estime", "mpr_estime", "reste_a_charge"]:
            if c in df_leads.columns:
                show_cols.append(c)

        df_show = df_leads[show_cols].sort_values(
            "score_urgence" if "score_urgence" in df_leads.columns else "cout_reno_estime",
            ascending=False
        )

        st.dataframe(
            df_show, use_container_width=True, height=500, hide_index=True,
            column_config={
                "score_urgence": st.column_config.ProgressColumn("Urgence", min_value=0, max_value=100, format="%d"),
                "cout_reno_estime": st.column_config.NumberColumn("Coût réno €", format="%d €"),
                "mpr_estime": st.column_config.NumberColumn("MaPrimeRénov' €", format="%d €"),
                "reste_a_charge": st.column_config.NumberColumn("Reste à charge €", format="%d €"),
                "conso_par_m2": st.column_config.NumberColumn("kWh/m²/an", format="%.0f"),
                "emission_ges": st.column_config.NumberColumn("kgCO₂/m²/an", format="%.0f"),
            }
        )

        # Sélection lead pour détail
        st.markdown("---")
        st.markdown("##### Fiche lead détaillée")
        if len(df_leads) > 0:
            idx = st.selectbox(
                "Sélectionner un logement",
                range(min(50, len(df_leads))),
                format_func=lambda i: (
                    f"{df_leads.iloc[i].get('adresse', '—')} — "
                    f"{df_leads.iloc[i].get('commune', '—')} — "
                    f"DPE {df_leads.iloc[i].get('etiquette_dpe', '?')}"
                ) if i < len(df_leads) else "",
                key="lead_detail"
            )
            lead = df_leads.iloc[idx]

            dpe_color = {"E": "#FFC107", "F": "#FF9800", "G": "#F44336"}.get(lead.get("etiquette_dpe"), "#999")

            _sv_url = ""
            if pd.notna(lead.get("latitude")) and pd.notna(lead.get("longitude")):
                _sv_url = f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lead['latitude']},{lead['longitude']}"
                _gm_url = f"https://www.google.com/maps/search/?api=1&query={lead['latitude']},{lead['longitude']}"

            st.markdown(f"""
            <div style="border:2px solid {dpe_color};border-radius:12px;padding:16px 20px;background:#fafafa;margin:8px 0">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
                <div>
                  <div style="font-size:1.1rem;font-weight:700">{lead.get('adresse', '—')}</div>
                  <div style="font-size:.85rem;color:#666">{lead.get('commune', '—')} {lead.get('code_postal', '')}</div>
                </div>
                <div style="background:{dpe_color};color:white;padding:8px 20px;border-radius:20px;font-weight:700;font-size:1.2rem">
                  DPE {lead.get('etiquette_dpe', '?')}
                </div>
              </div>
              <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:12px 0">
                <div style="text-align:center"><div style="font-size:1.2rem;font-weight:700">{lead.get('cout_reno_estime', 0):,.0f} €</div><div style="font-size:.7rem;color:#888">Coût rénovation</div></div>
                <div style="text-align:center"><div style="font-size:1.2rem;font-weight:700;color:#4CAF50">{lead.get('mpr_estime', 0):,.0f} €</div><div style="font-size:.7rem;color:#888">MaPrimeRénov'</div></div>
                <div style="text-align:center"><div style="font-size:1.2rem;font-weight:700">{lead.get('reste_a_charge', 0):,.0f} €</div><div style="font-size:.7rem;color:#888">Reste à charge</div></div>
                <div style="text-align:center"><div style="font-size:1.2rem;font-weight:700;color:{dpe_color}">{lead.get('score_urgence', '—')}/100</div><div style="font-size:.7rem;color:#888">Urgence</div></div>
              </div>
              <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:8px 0;font-size:.82rem">
                <div><b>Type :</b> {lead.get('type_batiment', '—')}</div>
                <div><b>Période :</b> {lead.get('periode_construction', '—')}</div>
                <div><b>Énergie :</b> {lead.get('energie_principale', '—')}</div>
                <div><b>Conso :</b> {lead.get('conso_par_m2', 0):.0f} kWh/m²/an</div>
                <div><b>GES :</b> {lead.get('emission_ges', 0):.0f} kgCO₂/m²/an</div>
                <div><b>Surface :</b> {lead.get('surface_immeuble', '—')} m²</div>
              </div>
              {'<div style="margin-top:8px;font-size:.8rem"><a href="' + _sv_url + '" target="_blank" style="color:#1a73e8">📍 Street View</a> · <a href="' + _gm_url + '" target="_blank" style="color:#1a73e8">🗺️ Google Maps</a></div>' if _sv_url else ''}
            </div>
            """, unsafe_allow_html=True)


# ── TAB 4 : DÉTAIL COMMUNE ──────────────────────────────────────────────────

with tab_commune:
    st.markdown("### 🔍 Analyse détaillée par commune")

    commune_sel = st.selectbox("Commune", df_communes["commune"].tolist(), key="detail_commune")

    if commune_sel:
        row = df_communes[df_communes["commune"] == commune_sel].iloc[0]

        # KPIs commune
        cc1, cc2, cc3, cc4, cc5, cc6 = st.columns(6)
        cc1.metric("Score réno", f"{row['score_reno']}/100")
        cc2.metric("Passoires F+G", f"{row['nb_fg']:,.0f}")
        cc3.metric("% F+G", f"{row['pct_fg']:.0f}%")
        cc4.metric("Volume travaux", f"{row['volume_travaux_total']:,.0f} €")
        cc5.metric("MaPrimeRénov'", f"{row['maprimerenov_total']:,.0f} €")
        cc6.metric("Conso moy", f"{row['conso_moy']:.0f} kWh/m²")

        st.markdown("---")

        # Répartition F vs G
        col_rep, col_reco = st.columns([1, 1])

        with col_rep:
            st.markdown("##### Répartition des DPE")
            try:
                import plotly.express as px
                pie_data = pd.DataFrame({
                    "Étiquette": ["E", "F", "G"],
                    "Nombre": [int(row.get("nb_e", 0)), int(row.get("nb_f", 0)), int(row.get("nb_g", 0))],
                })
                pie_data = pie_data[pie_data["Nombre"] > 0]
                fig_pie = px.pie(
                    pie_data, names="Étiquette", values="Nombre",
                    color="Étiquette",
                    color_discrete_map={"E": "#FFC107", "F": "#FF9800", "G": "#F44336"},
                    hole=0.4,
                )
                fig_pie.update_layout(height=300, margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig_pie, use_container_width=True)
            except ImportError:
                st.write(f"E: {row.get('nb_e', 0)} | F: {row.get('nb_f', 0)} | G: {row.get('nb_g', 0)}")

        with col_reco:
            st.markdown("##### Recommandation métier")

            if profil == "Artisan / Entreprise RGE":
                reco = (
                    f"**{commune_sel}** représente un potentiel de **{row['ca_potentiel']:,.0f} €** de CA.\n\n"
                    f"Avec **{int(row['nb_fg'])} passoires thermiques**, cette commune est "
                    f"{'un excellent terrain' if row['score_reno'] >= 70 else 'un bon terrain' if row['score_reno'] >= 40 else 'un terrain modeste'} "
                    f"de prospection.\n\n"
                    f"**Actions recommandées :**\n"
                    f"- Campagne de boîtage ciblée sur les {int(row['nb_g'])} logements G\n"
                    f"- Partenariat avec diagnostiqueurs DPE locaux\n"
                    f"- Présence sur les salons habitat de la zone"
                )
            elif profil == "Courtier MaPrimeRénov'":
                reco = (
                    f"**{row['maprimerenov_total']:,.0f} €** de MaPrimeRénov' mobilisable.\n\n"
                    f"**{int(row['nb_fg'])} dossiers potentiels** à constituer.\n\n"
                    f"**Actions recommandées :**\n"
                    f"- Cibler en priorité les {int(row['nb_g'])} logements G (aides maximales)\n"
                    f"- Partenariat avec artisans RGE locaux\n"
                    f"- Permanence en mairie pour accompagner les propriétaires"
                )
            elif profil == "Diagnostiqueur DPE":
                reco = (
                    f"**{int(row['nb_dpe_efg'])} DPE déjà réalisés** dans cette commune.\n\n"
                    f"Potentiel de re-diagnostics et nouveaux DPE lié à la rénovation.\n\n"
                    f"**Actions recommandées :**\n"
                    f"- Proposer des audits énergétiques aux propriétaires de logements F/G\n"
                    f"- Contact syndics de copropriétés (obligation DPE collectif)\n"
                    f"- Partenariat avec courtiers MaPrimeRénov'"
                )
            else:
                reco = (
                    f"**{commune_sel}** : score **{row['score_reno']}/100**\n\n"
                    f"Volume travaux estimé : **{row['volume_travaux_total']:,.0f} €**\n\n"
                    f"MaPrimeRénov' mobilisable : **{row['maprimerenov_total']:,.0f} €**"
                )

            st.markdown(reco)

        # Logements individuels de la commune
        st.markdown("---")
        st.markdown(f"##### Logements F+G à {commune_sel}")

        with st.spinner("Chargement..."):
            df_commune_logements = load_dpe_logements(dept, etiquettes=["F", "G"], commune=commune_sel, limit=200)

        if not df_commune_logements.empty:
            st.caption(f"{len(df_commune_logements)} logements trouvés")

            # Enrichir
            df_commune_logements = df_commune_logements.copy()
            df_commune_logements["cout_reno"] = df_commune_logements.apply(
                lambda r: (r.get("surface_immeuble", SURFACE_MOY) or SURFACE_MOY) * COUT_RENO_M2.get(r.get("etiquette_dpe", "F"), 300),
                axis=1
            ).round(0)

            show_cols_c = [c for c in ["adresse", "etiquette_dpe", "score_urgence", "conso_par_m2",
                                        "type_batiment", "periode_construction", "cout_reno"] if c in df_commune_logements.columns]
            st.dataframe(
                df_commune_logements[show_cols_c].sort_values(
                    "score_urgence" if "score_urgence" in df_commune_logements.columns else "cout_reno",
                    ascending=False
                ),
                use_container_width=True, height=400, hide_index=True,
                column_config={
                    "score_urgence": st.column_config.ProgressColumn("Urgence", min_value=0, max_value=100, format="%d"),
                    "cout_reno": st.column_config.NumberColumn("Coût réno €", format="%d €"),
                }
            )
        else:
            st.info("Aucun logement F/G trouvé pour cette commune.")


# ── TAB 5 : EXPORT ──────────────────────────────────────────────────────────

with tab_export:
    st.markdown("### 📥 Exports")

    col_e1, col_e2 = st.columns(2)

    with col_e1:
        st.markdown("##### Export communes (scoring)")
        csv_communes = df_communes.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Télécharger le scoring communes (CSV)",
            csv_communes,
            f"reno_scoring_{dept}_{datetime.now().strftime('%Y%m%d')}.csv",
            "text/csv",
            key="dl_communes"
        )
        st.caption(f"{len(df_communes)} communes • {_total_fg:,} passoires F+G")

    with col_e2:
        st.markdown("##### Export leads (logements)")
        if st.button("Charger les leads pour export", key="load_leads_export"):
            with st.spinner("Chargement..."):
                df_export = load_dpe_logements(dept, etiquettes=cible_etiquettes, limit=5000)
            if not df_export.empty:
                df_export = df_export.copy()
                df_export["cout_reno_estime"] = df_export.apply(
                    lambda r: (r.get("surface_immeuble", SURFACE_MOY) or SURFACE_MOY) * COUT_RENO_M2.get(r.get("etiquette_dpe", "F"), 300),
                    axis=1
                ).round(0)
                csv_leads = df_export.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "📥 Télécharger les leads (CSV)",
                    csv_leads,
                    f"reno_leads_{dept}_{datetime.now().strftime('%Y%m%d')}.csv",
                    "text/csv",
                    key="dl_leads"
                )
                st.caption(f"{len(df_export)} logements exportés")
            else:
                st.info("Aucun lead trouvé.")

    st.markdown("---")
    st.markdown("##### Résumé département")
    st.markdown(f"""
    | Indicateur | Valeur |
    |:---|---:|
    | Communes analysées | {_nb_communes} |
    | Passoires F+G | {_total_fg:,} |
    | Volume travaux estimé | {_total_volume:,.0f} € |
    | MaPrimeRénov' mobilisable | {_total_mpr:,.0f} € |
    | CA potentiel artisan (30%) | {_total_ca:,.0f} € |
    | Score réno moyen | {df_communes['score_reno'].mean():.0f}/100 |
    """)


# ─── FOOTER ──────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption(
    "RénoÉnergie Pro — SAHAR Conseil • "
    "Données : ADEME (DPE) + DVF (transactions) • "
    f"876k+ logements analysés • 101 départements • "
    f"Dernière MAJ : {datetime.now().strftime('%d/%m/%Y')}"
)
