"""
SAHAR Conseil — DVF Analyse Pro
Analyse du marché immobilier via les Demandes de Valeurs Foncières.

Déploiement : Streamlit Community Cloud
Repo : apps/dvf_analyse/app.py
"""

import sys
import os
from pathlib import Path

# Ajouter le dossier shared au path Python
# Nécessaire pour les imports shared.* depuis Streamlit Cloud
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st
import pandas as pd

from shared.auth import verifier_acces
from shared.data_loader import load_dvf, liste_departements, filtrer_par_periode
from shared.scoring import score_opportunite_immo, label_score
from shared.viz import (
    carte_transactions,
    graphique_prix_evolution,
    graphique_distribution_scores,
    graphique_prix_par_type,
    afficher_kpis_dvf,
    afficher_top_opportunites,
)
from shared.export import export_excel, export_pdf_rapport


# ─────────────────────────────────────────────
# CONFIGURATION PAGE
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="DVF Analyse Pro — SAHAR Conseil",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# CSS minimal pour harmoniser l'interface
st.markdown("""
<style>
    .main .block-container { padding-top: 1.5rem; }
    .stMetric label { font-size: 0.8rem; color: #73726c; }
    div[data-testid="stSidebarContent"] { padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# AUTHENTIFICATION
# ─────────────────────────────────────────────

verifier_acces()


# ─────────────────────────────────────────────
# SIDEBAR — FILTRES
# ─────────────────────────────────────────────

with st.sidebar:
    st.image("https://via.placeholder.com/150x40?text=SAHAR+Conseil", use_column_width=True)
    st.markdown("---")
    st.markdown("### 🔍 Filtres")

    # Département
    departements = liste_departements()
    dept_choisi = st.selectbox(
        "Département",
        options=departements,
        index=departements.index("69"),  # Lyon par défaut
        help="Choisir le département à analyser",
    )

    # Type de bien
    types_bien = st.multiselect(
        "Type de bien",
        options=["Appartement", "Maison"],
        default=["Appartement", "Maison"],
    )

    # Période
    mois_periode = st.slider(
        "Période (derniers mois)",
        min_value=6,
        max_value=60,
        value=24,
        step=6,
        help="Transactions des N derniers mois",
    )

    # Filtre surface
    surface_min, surface_max = st.slider(
        "Surface (m²)",
        min_value=10,
        max_value=500,
        value=(20, 200),
        step=5,
    )

    # Filtre prix/m²
    prix_min, prix_max = st.slider(
        "Prix/m² (€)",
        min_value=500,
        max_value=30000,
        value=(1000, 15000),
        step=500,
    )

    # Seuil score minimum
    score_min = st.slider(
        "Score minimum (opportunités)",
        min_value=0,
        max_value=100,
        value=0,
        step=10,
        help="Filtrer pour n'afficher que les biens avec score ≥ cette valeur",
    )

    st.markdown("---")
    st.markdown("### ⚙️ Paramètres scoring")
    st.caption("Ajuster les poids du score d'opportunité")

    poids_prix = st.slider("Poids sous-valorisation", 0.0, 1.0, 0.40, 0.05)
    poids_volume = st.slider("Poids volume marché", 0.0, 1.0, 0.30, 0.05)
    poids_dynamisme = st.slider("Poids dynamisme récent", 0.0, 1.0, 0.30, 0.05)

    # Recalibrer si la somme ≠ 1
    total = poids_prix + poids_volume + poids_dynamisme
    if abs(total - 1.0) > 0.01:
        st.warning(f"⚠️ Somme des poids = {total:.2f} (doit être 1.0)")

    st.markdown("---")
    if st.button("🔓 Se déconnecter"):
        from shared.auth import deconnecter
        deconnecter()


# ─────────────────────────────────────────────
# CHARGEMENT ET TRAITEMENT DES DONNÉES
# ─────────────────────────────────────────────

st.title("🏠 DVF Analyse Pro")
st.caption(f"Marché immobilier — Département {dept_choisi}")

with st.spinner("Chargement des données DVF..."):
    try:
        df_raw = load_dvf(dept_choisi)
    except Exception as e:
        st.error(f"Erreur lors du chargement : {e}")
        st.info(
            "💡 **Pour les tests sans connexion internet** : "
            "placer un fichier `dvf_{dept}.csv` dans `data/raw/` "
            "et télécharger depuis https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/"
        )
        st.stop()

# Appliquer les filtres
df = df_raw.copy()
df = filtrer_par_periode(df, "date_mutation", mois=mois_periode)
df = df[df["type_local"].isin(types_bien)]
df = df[df["surface_reelle_bati"].between(surface_min, surface_max)]
df = df[df["prix_m2"].between(prix_min, prix_max)]

if df.empty:
    st.warning("Aucune transaction ne correspond aux filtres sélectionnés. Élargir les critères.")
    st.stop()

# Calcul du scoring
try:
    poids_normalises = {
        "poids_prix": poids_prix / total,
        "poids_volume": poids_volume / total,
        "poids_dynamisme": poids_dynamisme / total,
    }
    df["score_opportunite"] = score_opportunite_immo(df, **poids_normalises)
except Exception as e:
    st.error(f"Erreur scoring : {e}")
    df["score_opportunite"] = 50

# Filtre par score minimum
df = df[df["score_opportunite"] >= score_min]


# ─────────────────────────────────────────────
# INTERFACE — KPIs
# ─────────────────────────────────────────────

afficher_kpis_dvf(df)
st.markdown("---")


# ─────────────────────────────────────────────
# INTERFACE — TABS PRINCIPALE
# ─────────────────────────────────────────────

tab_carte, tab_tableau, tab_stats, tab_export = st.tabs([
    "🗺️ Carte", "📋 Tableau", "📊 Statistiques", "📥 Export"
])

with tab_carte:
    col_carte, col_top = st.columns([2, 1])

    with col_carte:
        if "lat" in df.columns and "lon" in df.columns:
            carte_transactions(
                df,
                col_valeur="prix_m2",
                col_score="score_opportunite",
                titre=f"Transactions immobilières — Dép. {dept_choisi}",
                hauteur=500,
            )
        else:
            st.info(
                "📍 Les coordonnées GPS ne sont pas disponibles dans le fichier DVF standard. "
                "Un géocodage BAN sera ajouté en v2 pour activer la carte."
            )
            # Afficher quand même un tableau résumé
            st.dataframe(
                df[["nom_commune", "type_local", "surface_reelle_bati",
                    "prix_m2", "score_opportunite"]].head(20),
                use_container_width=True,
                hide_index=True,
            )

    with col_top:
        st.subheader("🏆 Top 10 opportunités")
        if "score_opportunite" in df.columns:
            top10_cols = [c for c in [
                "score_opportunite", "nom_commune", "type_local",
                "surface_reelle_bati", "prix_m2"
            ] if c in df.columns]
            top10 = df.nlargest(10, "score_opportunite")[top10_cols].copy()
            top10 = top10.rename(columns={
                "score_opportunite": "Score",
                "nom_commune": "Commune",
                "type_local": "Type",
                "surface_reelle_bati": "Surface",
                "prix_m2": "€/m²",
            })
            # Coloration des scores
            st.dataframe(
                top10,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Score": st.column_config.ProgressColumn(
                        "Score", min_value=0, max_value=100, format="%d"
                    )
                }
            )

with tab_tableau:
    cols_tableau = [c for c in [
        "score_opportunite", "nom_commune", "code_commune",
        "type_local", "surface_reelle_bati", "nombre_pieces_principales",
        "prix_m2", "valeur_fonciere", "date_mutation"
    ] if c in df.columns]

    df_affichage = df[cols_tableau].sort_values("score_opportunite", ascending=False)

    if "date_mutation" in df_affichage.columns:
        df_affichage["date_mutation"] = df_affichage["date_mutation"].dt.strftime("%d/%m/%Y")

    st.caption(f"{len(df_affichage):,} transactions affichées")
    st.dataframe(
        df_affichage,
        use_container_width=True,
        hide_index=True,
        column_config={
            "score_opportunite": st.column_config.ProgressColumn(
                "Score", min_value=0, max_value=100, format="%d"
            ),
            "prix_m2": st.column_config.NumberColumn("€/m²", format="%.0f €"),
            "valeur_fonciere": st.column_config.NumberColumn("Prix total", format="%.0f €"),
        }
    )

with tab_stats:
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        if "date_mutation" in df.columns:
            graphique_prix_evolution(df)

    with col_g2:
        graphique_distribution_scores(df)

    if "type_local" in df.columns and df["type_local"].nunique() > 1:
        graphique_prix_par_type(df)

with tab_export:
    st.subheader("📥 Exporter les données")
    col_ex1, col_ex2 = st.columns(2)

    with col_ex1:
        st.markdown("**Export Excel**")
        st.caption("Fichier Excel formaté avec toutes les transactions filtrées")

        if st.button("Préparer l'export Excel", type="primary"):
            cols_export = [c for c in [
                "score_opportunite", "nom_commune", "type_local",
                "surface_reelle_bati", "prix_m2", "valeur_fonciere",
                "date_mutation", "adresse_numero", "adresse_nom_voie",
                "code_postal",
            ] if c in df.columns]

            df_ex = df[cols_export].copy()
            if "date_mutation" in df_ex.columns:
                df_ex["date_mutation"] = df_ex["date_mutation"].dt.strftime("%d/%m/%Y")

            try:
                xlsx_bytes = export_excel(df_ex, nom_feuille="DVF Analyse")
                st.download_button(
                    "📥 Télécharger Excel",
                    data=xlsx_bytes,
                    file_name=f"sahar_dvf_{dept_choisi}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            except ImportError:
                st.error("Installer openpyxl : pip install openpyxl")

    with col_ex2:
        st.markdown("**Export PDF**")
        st.caption("Rapport PDF professionnel prêt à présenter à un client")

        if st.button("Générer le rapport PDF"):
            kpis = {
                "Transactions analysées": f"{len(df):,}",
                "Prix médian (€/m²)": f"{df['prix_m2'].median():,.0f} €",
                "Opportunités (score ≥ 70)": (df["score_opportunite"] >= 70).sum(),
                "Département": dept_choisi,
                "Période": f"Derniers {mois_periode} mois",
            }

            top15 = df.nlargest(15, "score_opportunite")[[
                c for c in ["score_opportunite", "nom_commune",
                             "type_local", "surface_reelle_bati", "prix_m2"]
                if c in df.columns
            ]].rename(columns={
                "score_opportunite": "Score",
                "nom_commune": "Commune",
                "type_local": "Type",
                "surface_reelle_bati": "Surface (m²)",
                "prix_m2": "€/m²",
            })

            try:
                pdf_bytes = export_pdf_rapport(
                    titre=f"Analyse marché immobilier — Département {dept_choisi}",
                    secteur="Immobilier",
                    commune=f"Département {dept_choisi}",
                    kpis=kpis,
                    df_top=top15,
                    nb_transactions=len(df),
                )
                st.download_button(
                    "📄 Télécharger PDF",
                    data=pdf_bytes,
                    file_name=f"sahar_rapport_dvf_{dept_choisi}.pdf",
                    mime="application/pdf",
                )
            except ImportError:
                st.error("Installer reportlab : pip install reportlab")


# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────

st.markdown("---")
st.caption(
    "SAHAR Conseil — Sources : DVF (data.gouv.fr), INSEE. "
    "Données mises à jour quotidiennement. "
    "Les scores sont fournis à titre indicatif et ne constituent pas un conseil en investissement."
)
