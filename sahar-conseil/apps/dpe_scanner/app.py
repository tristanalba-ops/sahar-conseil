"""
SAHAR Conseil — DPE Scanner
Détection de prospects énergie : logements F/G à rénover par commune.

Déploiement : Streamlit Community Cloud
Repo : apps/dpe_scanner/app.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st
import pandas as pd

from shared.auth import verifier_acces
from shared.data_loader import load_dpe
from shared.scoring import score_priorite_renovation
from shared.export import export_excel, export_pdf_rapport

st.set_page_config(
    page_title="DPE Scanner — SAHAR Conseil",
    page_icon="⚡",
    layout="wide",
)

st.markdown("""
<style>
    .main .block-container { padding-top: 1.5rem; }
    .stMetric label { font-size: 0.8rem; color: #73726c; }
</style>
""", unsafe_allow_html=True)

verifier_acces()

# ── SIDEBAR ──────────────────────────────────
with st.sidebar:
    st.markdown("### ⚡ DPE Scanner")
    st.caption("Trouvez vos prospects rénovation")
    st.markdown("---")

    code_postal = st.text_input(
        "Code postal",
        value="69001",
        max_chars=5,
        help="Code postal sur 5 chiffres",
    )

    etiquettes_cibles = st.multiselect(
        "Étiquettes DPE à cibler",
        options=["G", "F", "E", "D", "C", "B", "A"],
        default=["G", "F"],
        help="G et F = passoires thermiques, priorité réglementaire",
    )

    score_min = st.slider("Score urgence minimum", 0, 100, 50, 5)

    nb_resultats = st.slider("Nombre de logements à analyser", 100, 2000, 500, 100)

    st.markdown("---")
    st.markdown("### 📋 À propos")
    st.caption(
        "Source : ADEME — Base des DPE (Diagnostics de Performance Énergétique). "
        "Mise à jour mensuelle."
    )

# ── CHARGEMENT ───────────────────────────────
st.title("⚡ DPE Scanner")
st.caption(f"Prospects rénovation énergétique — {code_postal}")

if len(code_postal) != 5 or not code_postal.isdigit():
    st.warning("Entrer un code postal valide sur 5 chiffres.")
    st.stop()

with st.spinner("Interrogation de l'API ADEME..."):
    df = load_dpe(code_postal, nb_resultats=nb_resultats)

if df.empty:
    st.warning(
        f"Aucun DPE trouvé pour le code postal {code_postal}. "
        "Vérifier le code postal ou essayer une commune voisine."
    )
    st.stop()

# Filtrer par étiquettes cibles
if etiquettes_cibles and "etiquette_dpe" in df.columns:
    df = df[df["etiquette_dpe"].isin(etiquettes_cibles)]

if df.empty:
    st.info(f"Aucun logement avec étiquettes {etiquettes_cibles} dans ce code postal.")
    st.stop()

# Scoring
df["score_urgence"] = score_priorite_renovation(df)
df = df[df["score_urgence"] >= score_min]

# ── KPIs ─────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Logements analysés", f"{len(df):,}".replace(",", " "))
with col2:
    nb_g = (df["etiquette_dpe"] == "G").sum() if "etiquette_dpe" in df.columns else 0
    st.metric("Passoires G", nb_g)
with col3:
    nb_f = (df["etiquette_dpe"] == "F").sum() if "etiquette_dpe" in df.columns else 0
    st.metric("Étiquette F", nb_f)
with col4:
    score_moy = df["score_urgence"].mean()
    st.metric("Score urgence moyen", f"{score_moy:.0f}/100")

st.markdown("---")

# ── TABS ─────────────────────────────────────
tab_liste, tab_stats, tab_export = st.tabs(["📋 Liste prospects", "📊 Statistiques", "📥 Export"])

with tab_liste:
    cols_afficher = [c for c in [
        "score_urgence", "etiquette_dpe", "etiquette_ges",
        "adresse_ban", "nom_commune_ban",
        "type_batiment", "annee_construction",
        "surface_habitable_logement", "conso_5_usages_e_finale",
        "date_etablissement_dpe",
    ] if c in df.columns]

    df_affichage = df[cols_afficher].sort_values("score_urgence", ascending=False).copy()

    rename = {
        "score_urgence": "Score urgence",
        "etiquette_dpe": "DPE",
        "etiquette_ges": "GES",
        "adresse_ban": "Adresse",
        "nom_commune_ban": "Commune",
        "type_batiment": "Type",
        "annee_construction": "Année",
        "surface_habitable_logement": "Surface (m²)",
        "conso_5_usages_e_finale": "Conso (kWh/m²/an)",
        "date_etablissement_dpe": "Date DPE",
    }
    df_affichage = df_affichage.rename(columns={k: v for k, v in rename.items() if k in df_affichage.columns})

    st.caption(f"{len(df_affichage):,} prospects affichés")
    st.dataframe(
        df_affichage,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Score urgence": st.column_config.ProgressColumn(
                "Score urgence", min_value=0, max_value=100, format="%d"
            ),
            "DPE": st.column_config.TextColumn("DPE", width="small"),
        }
    )

with tab_stats:
    import plotly.express as px

    col_s1, col_s2 = st.columns(2)

    with col_s1:
        if "etiquette_dpe" in df.columns:
            dist = df["etiquette_dpe"].value_counts().reset_index()
            dist.columns = ["Étiquette", "Nombre"]
            couleurs = {"G": "#E24B4A", "F": "#BA7517", "E": "#EF9F27",
                        "D": "#1D9E75", "C": "#0F6E56", "B": "#185FA5", "A": "#0C447C"}
            fig = px.bar(
                dist, x="Étiquette", y="Nombre",
                title="Répartition par étiquette DPE",
                color="Étiquette",
                color_discrete_map=couleurs,
            )
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with col_s2:
        if "annee_construction" in df.columns:
            df["annee_construction"] = pd.to_numeric(df["annee_construction"], errors="coerce")
            df_annee = df.dropna(subset=["annee_construction"])
            fig2 = px.histogram(
                df_annee, x="annee_construction", nbins=20,
                title="Année de construction",
                labels={"annee_construction": "Année"},
                color_discrete_sequence=["#185FA5"],
            )
            fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig2, use_container_width=True)

with tab_export:
    st.subheader("📥 Export liste de prospects")
    st.caption("Fichier Excel prêt à importer dans votre CRM ou outil de prospection")

    if st.button("Préparer l'export Excel", type="primary"):
        try:
            xlsx = export_excel(
                df[cols_afficher].rename(columns={k: v for k, v in rename.items() if k in cols_afficher}),
                nom_feuille="Prospects DPE"
            )
            st.download_button(
                "📥 Télécharger Excel",
                data=xlsx,
                file_name=f"sahar_prospects_dpe_{code_postal}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except ImportError:
            st.error("Installer openpyxl : pip install openpyxl")

st.markdown("---")
st.caption("SAHAR Conseil — Source : ADEME DPE. Les données proviennent des diagnostics déposés sur la plateforme ADEME.")
