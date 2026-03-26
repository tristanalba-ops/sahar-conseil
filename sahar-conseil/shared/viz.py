"""
SAHAR Conseil — viz.py
Visualisations partagées : cartes folium, graphiques plotly, métriques Streamlit.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

try:
    import folium
    from folium.plugins import HeatMap
    from streamlit_folium import st_folium
    FOLIUM_DISPONIBLE = True
except ImportError:
    FOLIUM_DISPONIBLE = False


# ─────────────────────────────────────────────
# CARTES
# ─────────────────────────────────────────────

def carte_transactions(
    df: pd.DataFrame,
    col_valeur: str = "prix_m2",
    col_score: str = "score_opportunite",
    titre: str = "Transactions",
    hauteur: int = 500,
) -> None:
    """
    Affiche une carte folium des transactions avec coloration par score.
    Chaque point = une transaction. Couleur = score opportunité.

    Args:
        df: DataFrame avec colonnes lat, lon, et col_valeur
        col_valeur: Colonne à afficher dans le tooltip
        col_score: Colonne score pour la couleur (optionnel)
        titre: Titre affiché au-dessus de la carte
        hauteur: Hauteur en pixels de la carte
    """
    if not FOLIUM_DISPONIBLE:
        st.warning("Installez folium et streamlit-folium : pip install folium streamlit-folium")
        return

    df_carte = df.dropna(subset=["lat", "lon"]).copy()
    if df_carte.empty:
        st.warning("Aucune coordonnée disponible pour afficher la carte.")
        return

    # Centre de la carte
    lat_centre = df_carte["lat"].median()
    lon_centre = df_carte["lon"].median()

    m = folium.Map(
        location=[lat_centre, lon_centre],
        zoom_start=12,
        tiles="CartoDB positron"
    )

    # Couleur selon le score
    def couleur_score(score):
        if score >= 70:
            return "#1D9E75"   # vert
        elif score >= 40:
            return "#BA7517"   # orange
        else:
            return "#E24B4A"   # rouge

    for _, row in df_carte.iterrows():
        score = int(row.get(col_score, 50)) if col_score in df_carte.columns else 50
        valeur = row.get(col_valeur, "—")
        valeur_fmt = f"{valeur:,.0f}" if isinstance(valeur, (int, float)) else str(valeur)

        popup_html = f"""
        <div style='font-family:sans-serif;font-size:12px;min-width:150px'>
            <b>{col_valeur.replace('_', ' ').title()}</b> : {valeur_fmt}<br>
            <b>Score</b> : {score}/100<br>
            <b>Commune</b> : {row.get('nom_commune', '—')}<br>
            <b>Type</b> : {row.get('type_local', '—')}<br>
            <b>Surface</b> : {row.get('surface_reelle_bati', '—')} m²
        </div>
        """

        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=5,
            color=couleur_score(score),
            fill=True,
            fill_color=couleur_score(score),
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=250),
            tooltip=f"Score: {score}/100 — {valeur_fmt} {col_valeur.replace('prix_m2', '€/m²')}",
        ).add_to(m)

    st.subheader(titre)
    st_folium(m, height=hauteur, use_container_width=True)


def heatmap_prix(df: pd.DataFrame, col_valeur: str = "prix_m2", hauteur: int = 500) -> None:
    """
    Affiche une heatmap de densité des valeurs sur la carte.

    Args:
        df: DataFrame avec lat, lon, et col_valeur
        col_valeur: Colonne numérique pour l'intensité de la heatmap
        hauteur: Hauteur en pixels
    """
    if not FOLIUM_DISPONIBLE:
        return

    df_h = df.dropna(subset=["lat", "lon", col_valeur]).copy()
    if df_h.empty:
        return

    lat_centre = df_h["lat"].median()
    lon_centre = df_h["lon"].median()

    m = folium.Map(location=[lat_centre, lon_centre], zoom_start=12, tiles="CartoDB dark_matter")

    # Normaliser les valeurs pour l'intensité
    val_norm = (df_h[col_valeur] - df_h[col_valeur].min()) / (
        df_h[col_valeur].max() - df_h[col_valeur].min() + 1
    )

    heat_data = [[row["lat"], row["lon"], val_norm.iloc[i]]
                 for i, (_, row) in enumerate(df_h.iterrows())]

    HeatMap(heat_data, radius=12, blur=8).add_to(m)

    st_folium(m, height=hauteur, use_container_width=True)


# ─────────────────────────────────────────────
# GRAPHIQUES PLOTLY
# ─────────────────────────────────────────────

def graphique_prix_evolution(df: pd.DataFrame, col_date: str = "date_mutation") -> None:
    """
    Affiche l'évolution mensuelle du prix médian/m².

    Args:
        df: DataFrame avec col_date et prix_m2
    """
    df_agg = (
        df.groupby(df[col_date].dt.to_period("M"))["prix_m2"]
        .median()
        .reset_index()
    )
    df_agg[col_date] = df_agg[col_date].astype(str)

    fig = px.line(
        df_agg,
        x=col_date,
        y="prix_m2",
        title="Évolution du prix médian (€/m²)",
        labels={col_date: "Mois", "prix_m2": "Prix médian €/m²"},
        color_discrete_sequence=["#1D9E75"],
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font_family="sans-serif",
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


def graphique_distribution_scores(df: pd.DataFrame, col_score: str = "score_opportunite") -> None:
    """
    Histogramme de distribution des scores.

    Args:
        df: DataFrame avec col_score
    """
    fig = px.histogram(
        df,
        x=col_score,
        nbins=20,
        title="Distribution des scores d'opportunité",
        labels={col_score: "Score (0–100)"},
        color_discrete_sequence=["#185FA5"],
    )
    fig.add_vline(x=70, line_dash="dash", line_color="#1D9E75",
                  annotation_text="Seuil opportunité")
    fig.add_vline(x=40, line_dash="dash", line_color="#BA7517",
                  annotation_text="Seuil alerte")
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)


def graphique_prix_par_type(df: pd.DataFrame) -> None:
    """
    Boîte à moustaches prix/m² par type de bien.

    Args:
        df: DataFrame avec type_local et prix_m2
    """
    fig = px.box(
        df,
        x="type_local",
        y="prix_m2",
        title="Distribution prix/m² par type de bien",
        labels={"type_local": "Type", "prix_m2": "Prix €/m²"},
        color="type_local",
        color_discrete_map={"Appartement": "#185FA5", "Maison": "#1D9E75"},
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────
# MÉTRIQUES STREAMLIT
# ─────────────────────────────────────────────

def afficher_kpis_dvf(df: pd.DataFrame) -> None:
    """
    Affiche les 4 KPIs principaux DVF en ligne.

    Args:
        df: DataFrame DVF nettoyé avec prix_m2 et score_opportunite
    """
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Transactions analysées",
            f"{len(df):,}".replace(",", " "),
        )
    with col2:
        prix_med = df["prix_m2"].median()
        st.metric("Prix médian", f"{prix_med:,.0f} €/m²")
    with col3:
        if "score_opportunite" in df.columns:
            nb_opps = (df["score_opportunite"] >= 70).sum()
            st.metric("Opportunités détectées", f"{nb_opps:,}".replace(",", " "))
    with col4:
        if "date_mutation" in df.columns:
            date_max = df["date_mutation"].max()
            st.metric("Dernière transaction", date_max.strftime("%b %Y") if pd.notna(date_max) else "—")


def afficher_top_opportunites(df: pd.DataFrame, n: int = 10, col_score: str = "score_opportunite") -> None:
    """
    Affiche un tableau des N meilleures opportunités.

    Args:
        df: DataFrame avec col_score
        n: Nombre de lignes à afficher
        col_score: Colonne de score
    """
    if col_score not in df.columns:
        return

    cols_afficher = [c for c in [
        col_score, "nom_commune", "type_local",
        "surface_reelle_bati", "prix_m2", "valeur_fonciere",
        "date_mutation"
    ] if c in df.columns]

    top = df.nlargest(n, col_score)[cols_afficher].copy()

    # Renommage lisible
    renommage = {
        col_score: "Score",
        "nom_commune": "Commune",
        "type_local": "Type",
        "surface_reelle_bati": "Surface (m²)",
        "prix_m2": "Prix €/m²",
        "valeur_fonciere": "Prix total (€)",
        "date_mutation": "Date vente",
    }
    top = top.rename(columns={k: v for k, v in renommage.items() if k in top.columns})

    if "Date vente" in top.columns:
        top["Date vente"] = top["Date vente"].dt.strftime("%d/%m/%Y")

    st.subheader(f"Top {n} opportunités")
    st.dataframe(top, use_container_width=True, hide_index=True)
