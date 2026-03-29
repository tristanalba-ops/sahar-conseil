"""
SAHAR Conseil — Estimateur de prix par rue
Données DVF DGFiP — 2.35M transactions, 711k rues
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from supabase import create_client
import os

st.set_page_config(
    page_title="Estimateur par rue · SAHAR Conseil",
    page_icon="🏠",
    layout="wide"
)

# ─── Config ───────────────────────────────────────────────────────────────────
SUPABASE_URL = st.secrets.get("SUPABASE_URL", os.getenv("SUPABASE_URL", ""))
SUPABASE_KEY = st.secrets.get("SUPABASE_ANON_KEY", os.getenv("SUPABASE_ANON_KEY", ""))

@st.cache_resource
def get_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = get_client()

# ─── Requêtes Supabase ────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def search_rues(query: str, commune: str = "") -> list[dict]:
    """Autocomplete rue depuis dvf_mutations."""
    q = supabase.table("dvf_mutations").select(
        "adresse_nom_voie,commune,code_postal,code_departement"
    ).ilike("adresse_nom_voie", f"%{query.upper()}%")

    if commune:
        q = q.ilike("commune", f"%{commune.upper()}%")

    result = q.limit(200).execute()

    # Dédoublonner
    seen = set()
    rues = []
    for r in result.data:
        key = (r["adresse_nom_voie"], r["commune"], r["code_postal"])
        if key not in seen and r["adresse_nom_voie"]:
            seen.add(key)
            rues.append({
                "voie": r["adresse_nom_voie"],
                "commune": r["commune"],
                "cp": r["code_postal"],
                "dept": r["code_departement"],
                "label": f"{r['adresse_nom_voie']} — {r['commune']} ({r['code_postal']})"
            })

    return sorted(rues, key=lambda x: x["label"])[:50]


@st.cache_data(ttl=3600)
def get_stats_rue(voie: str, commune: str) -> dict:
    """Stats complètes pour une rue donnée."""
    result = supabase.table("dvf_mutations").select("*").eq(
        "adresse_nom_voie", voie
    ).eq("commune", commune).eq("nature_mutation", "Vente").execute()

    df = pd.DataFrame(result.data)
    if df.empty:
        return {}

    df = df[df["surface_reelle_bati"].notna() & (df["surface_reelle_bati"] > 0)]
    df = df[df["valeur_fonciere"].notna() & (df["valeur_fonciere"] > 0)]
    df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
    df["date_mutation"] = pd.to_datetime(df["date_mutation"])
    df["annee"] = df["date_mutation"].dt.year

    # Filtrer outliers (IQR)
    q1, q3 = df["prix_m2"].quantile(0.1), df["prix_m2"].quantile(0.9)
    df_clean = df[(df["prix_m2"] >= q1) & (df["prix_m2"] <= q3)]

    # Stats globales
    prix_median = df_clean["prix_m2"].median()
    prix_q1 = df_clean["prix_m2"].quantile(0.25)
    prix_q3 = df_clean["prix_m2"].quantile(0.75)
    prix_moy = df_clean["prix_m2"].mean()

    # Evolution annuelle
    by_year = df_clean.groupby("annee")["prix_m2"].median().reset_index()
    evolution_1y = None
    if len(by_year) >= 2:
        last = by_year.iloc[-1]["prix_m2"]
        prev = by_year.iloc[-2]["prix_m2"]
        evolution_1y = round((last - prev) / prev * 100, 1)

    # Par type
    by_type = df_clean.groupby("type_local").agg(
        nb=("prix_m2", "count"),
        prix_median=("prix_m2", "median"),
        surface_med=("surface_reelle_bati", "median")
    ).reset_index().to_dict("records")

    return {
        "df": df_clean,
        "by_year": by_year,
        "by_type": by_type,
        "prix_median": round(prix_median, 0),
        "prix_q1": round(prix_q1, 0),
        "prix_q3": round(prix_q3, 0),
        "prix_moy": round(prix_moy, 0),
        "nb_ventes": len(df_clean),
        "evolution_1y": evolution_1y,
        "derniere_vente": df_clean["date_mutation"].max().strftime("%b %Y"),
        "surface_med": round(df_clean["surface_reelle_bati"].median(), 0),
        "valeur_med": round(df_clean["valeur_fonciere"].median(), 0),
    }


@st.cache_data(ttl=3600)
def get_stats_commune(commune: str, code_postal: str) -> dict:
    """Prix médian commune pour comparaison."""
    result = supabase.table("dvf_mutations").select(
        "valeur_fonciere,surface_reelle_bati"
    ).eq("commune", commune).eq("code_postal", code_postal).eq(
        "nature_mutation", "Vente"
    ).execute()

    df = pd.DataFrame(result.data)
    if df.empty:
        return {}

    df = df[df["surface_reelle_bati"].notna() & (df["surface_reelle_bati"] > 0)]
    df = df[df["valeur_fonciere"].notna() & (df["valeur_fonciere"] > 0)]
    df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
    q1, q3 = df["prix_m2"].quantile(0.1), df["prix_m2"].quantile(0.9)
    df = df[(df["prix_m2"] >= q1) & (df["prix_m2"] <= q3)]

    return {
        "prix_median_commune": round(df["prix_m2"].median(), 0),
        "nb_ventes_commune": len(df),
    }


# ─── UI ───────────────────────────────────────────────────────────────────────

# CSS minimal
st.markdown("""
<style>
  [data-testid="stAppViewContainer"] { background: #0D0D0D; color: #fff; }
  .metric-card {
    background: #1A1A2E; border: 1px solid #2A2A3E; border-radius: 12px;
    padding: 1.2rem 1.5rem; text-align: center;
  }
  .metric-value { font-size: 2rem; font-weight: 700; color: #00DC82; }
  .metric-label { font-size: 0.85rem; color: #A0A0A0; margin-top: 0.3rem; }
  .metric-delta { font-size: 0.9rem; margin-top: 0.4rem; }
  .badge-green { background: rgba(0,220,130,0.1); color: #00DC82;
    border: 1px solid rgba(0,220,130,0.3); border-radius: 100px;
    padding: 0.2rem 0.8rem; font-size: 0.75rem; display: inline-block; }
  .badge-red { background: rgba(255,107,107,0.1); color: #FF6B6B;
    border: 1px solid rgba(255,107,107,0.3); border-radius: 100px;
    padding: 0.2rem 0.8rem; font-size: 0.75rem; display: inline-block; }
  .badge-gray { background: rgba(160,160,160,0.1); color: #A0A0A0;
    border: 1px solid rgba(160,160,160,0.3); border-radius: 100px;
    padding: 0.2rem 0.8rem; font-size: 0.75rem; display: inline-block; }
  .stTextInput input, .stSelectbox select {
    background: #1A1A2E !important; color: #fff !important;
    border: 1px solid #2A2A3E !important;
  }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div style="padding: 1.5rem 0 0.5rem;">
  <span style="color:#00DC82;font-weight:700;font-size:0.85rem;letter-spacing:2px;text-transform:uppercase;">SAHAR Conseil · DVF DGFiP</span>
  <h1 style="font-size:2rem;font-weight:700;margin:0.3rem 0 0.5rem;">Estimateur de prix par rue</h1>
  <p style="color:#A0A0A0;margin:0;">Prix réels issus des actes notariés · 2.35M transactions · 711 000 rues</p>
</div>
""", unsafe_allow_html=True)

st.divider()

# ─── Recherche ────────────────────────────────────────────────────────────────
col_search1, col_search2 = st.columns([3, 2])
with col_search1:
    query_voie = st.text_input("🔍 Nom de voie", placeholder="Ex : RUE DE RIVOLI, BD VOLTAIRE, AV DU GENERAL DE GAULLE...")
with col_search2:
    query_commune = st.text_input("Commune (optionnel)", placeholder="Ex : Paris, Lyon, Bordeaux...")

rues = []
voie_selectionnee = None
commune_selectionnee = None
cp_selectionne = None

if query_voie and len(query_voie) >= 3:
    with st.spinner("Recherche..."):
        rues = search_rues(query_voie, query_commune)

    if not rues:
        st.info("Aucune voie trouvée. Essayez un nom abrégé (RUE, BD, AV, IMP...).")
    else:
        labels = [r["label"] for r in rues]
        choix = st.selectbox(f"{len(rues)} voie(s) trouvée(s)", options=labels)

        if choix:
            rue_data = next(r for r in rues if r["label"] == choix)
            voie_selectionnee = rue_data["voie"]
            commune_selectionnee = rue_data["commune"]
            cp_selectionne = rue_data["cp"]

# ─── Résultats ────────────────────────────────────────────────────────────────
if voie_selectionnee and commune_selectionnee:
    with st.spinner(f"Chargement des données pour {voie_selectionnee}, {commune_selectionnee}..."):
        stats = get_stats_rue(voie_selectionnee, commune_selectionnee)
        stats_commune = get_stats_commune(commune_selectionnee, cp_selectionne or "")

    if not stats:
        st.warning("Pas assez de données pour cette voie.")
    else:
        st.markdown(f"""
        <div style="margin:1.5rem 0 1rem;">
          <h2 style="font-size:1.4rem;margin-bottom:0.3rem;">{voie_selectionnee.title()}</h2>
          <span style="color:#A0A0A0;">{commune_selectionnee} · {cp_selectionne}</span>
          &nbsp;·&nbsp;
          <span class="badge-gray">{stats['nb_ventes']} vente(s) sur 3 ans</span>
          &nbsp;
          <span class="badge-gray">Dernière : {stats['derniere_vente']}</span>
        </div>
        """, unsafe_allow_html=True)

        # ── KPI cards ──
        c1, c2, c3, c4 = st.columns(4)

        delta_html = ""
        if stats["evolution_1y"] is not None:
            sign = "+" if stats["evolution_1y"] >= 0 else ""
            color = "#00DC82" if stats["evolution_1y"] >= 0 else "#FF6B6B"
            delta_html = f'<div class="metric-delta" style="color:{color};">{sign}{stats["evolution_1y"]}% vs an dernier</div>'

        with c1:
            st.markdown(f"""
            <div class="metric-card">
              <div class="metric-value">{int(stats['prix_median']):,} €</div>
              <div class="metric-label">Prix médian / m²</div>
              {delta_html}
            </div>""", unsafe_allow_html=True)

        with c2:
            ecart = ""
            if stats_commune.get("prix_median_commune"):
                diff = stats["prix_median"] - stats_commune["prix_median_commune"]
                pct = round(diff / stats_commune["prix_median_commune"] * 100, 1)
                sign = "+" if pct >= 0 else ""
                color = "#00DC82" if pct >= 0 else "#FF6B6B"
                ecart = f'<div class="metric-delta" style="color:{color};">{sign}{pct}% vs commune</div>'
            st.markdown(f"""
            <div class="metric-card">
              <div class="metric-value">{int(stats_commune.get('prix_median_commune', 0)):,} €</div>
              <div class="metric-label">Médiane commune</div>
              {ecart}
            </div>""", unsafe_allow_html=True)

        with c3:
            fourchette = f"{int(stats['prix_q1']):,} – {int(stats['prix_q3']):,} €"
            st.markdown(f"""
            <div class="metric-card">
              <div class="metric-value" style="font-size:1.4rem;">{fourchette}</div>
              <div class="metric-label">Fourchette habituelle / m²</div>
              <div class="metric-delta" style="color:#A0A0A0;">25e – 75e percentile</div>
            </div>""", unsafe_allow_html=True)

        with c4:
            valeur_med = f"{int(stats['valeur_med']):,} €"
            st.markdown(f"""
            <div class="metric-card">
              <div class="metric-value" style="font-size:1.4rem;">{valeur_med}</div>
              <div class="metric-label">Valeur médiane bien</div>
              <div class="metric-delta" style="color:#A0A0A0;">Surface : {int(stats['surface_med'])} m² en médiane</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)

        # ── Graphiques ──
        tab1, tab2, tab3 = st.tabs(["📈 Évolution annuelle", "🗺️ Carte des ventes", "📋 Comparables"])

        df = stats["df"]

        with tab1:
            col_g1, col_g2 = st.columns(2)

            with col_g1:
                by_year = stats["by_year"]
                if len(by_year) >= 2:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=by_year["annee"].astype(str),
                        y=by_year["prix_m2"].round(0),
                        marker_color="#00DC82",
                        text=by_year["prix_m2"].round(0).apply(lambda x: f"{int(x):,} €"),
                        textposition="outside",
                    ))
                    fig.update_layout(
                        title="Prix médian /m² par année",
                        paper_bgcolor="#1A1A2E", plot_bgcolor="#1A1A2E",
                        font_color="#fff", showlegend=False,
                        yaxis=dict(showgrid=False, tickformat=","),
                        xaxis=dict(showgrid=False),
                        height=300, margin=dict(t=40, b=20, l=20, r=20)
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Pas assez d'années pour montrer une évolution.")

            with col_g2:
                # Distribution des prix
                fig2 = px.histogram(
                    df, x="prix_m2", nbins=30,
                    title="Distribution des prix /m²",
                    color_discrete_sequence=["#7B61FF"],
                    labels={"prix_m2": "Prix /m²"}
                )
                fig2.add_vline(
                    x=stats["prix_median"], line_color="#00DC82",
                    annotation_text=f"Médiane {int(stats['prix_median']):,}€",
                    annotation_font_color="#00DC82"
                )
                if stats_commune.get("prix_median_commune"):
                    fig2.add_vline(
                        x=stats_commune["prix_median_commune"],
                        line_color="#FFD166", line_dash="dash",
                        annotation_text=f"Commune {int(stats_commune['prix_median_commune']):,}€",
                        annotation_font_color="#FFD166"
                    )
                fig2.update_layout(
                    paper_bgcolor="#1A1A2E", plot_bgcolor="#1A1A2E",
                    font_color="#fff", showlegend=False,
                    height=300, margin=dict(t=40, b=20, l=20, r=20)
                )
                st.plotly_chart(fig2, use_container_width=True)

            # Par type de bien
            if stats["by_type"]:
                st.markdown("**Par type de bien**")
                type_df = pd.DataFrame(stats["by_type"])
                type_df.columns = ["Type", "Nb ventes", "Prix médian /m²", "Surface médiane (m²)"]
                type_df["Prix médian /m²"] = type_df["Prix médian /m²"].apply(lambda x: f"{int(x):,} €")
                type_df["Surface médiane (m²)"] = type_df["Surface médiane (m²)"].apply(lambda x: f"{int(x)} m²")
                st.dataframe(type_df, hide_index=True, use_container_width=True)

        with tab2:
            df_map = df[df["latitude"].notna() & df["longitude"].notna()].copy()
            if df_map.empty:
                st.info("Coordonnées GPS non disponibles pour cette rue.")
            else:
                df_map["info"] = df_map.apply(
                    lambda r: f"{r['adresse_numero'] or ''} {r['adresse_nom_voie']} — {int(r['prix_m2']):,} €/m² ({r['type_local']}, {int(r['surface_reelle_bati'])} m²)",
                    axis=1
                )
                df_map["prix_m2_rounded"] = df_map["prix_m2"].round(0)

                fig_map = px.scatter_mapbox(
                    df_map,
                    lat="latitude", lon="longitude",
                    color="prix_m2",
                    size="surface_reelle_bati",
                    size_max=15,
                    color_continuous_scale=["#7B61FF", "#00DC82", "#FFD166"],
                    hover_name="info",
                    hover_data={"latitude": False, "longitude": False,
                                "prix_m2_rounded": True, "surface_reelle_bati": True,
                                "type_local": True, "date_mutation": True},
                    zoom=14,
                    mapbox_style="carto-darkmatter",
                    labels={"prix_m2": "€/m²", "surface_reelle_bati": "Surface (m²)"},
                    title=f"Transactions — {voie_selectionnee.title()}"
                )
                fig_map.update_layout(
                    paper_bgcolor="#1A1A2E", font_color="#fff",
                    height=450, margin=dict(t=40, b=0, l=0, r=0),
                    coloraxis_colorbar=dict(
                        title="€/m²", tickformat=",",
                        bgcolor="#1A1A2E", tickfont_color="#fff"
                    )
                )
                st.plotly_chart(fig_map, use_container_width=True)

        with tab3:
            comparables = df.sort_values("date_mutation", ascending=False).head(30)[[
                "adresse_numero", "adresse_nom_voie", "date_mutation",
                "type_local", "surface_reelle_bati", "nombre_pieces_principales",
                "valeur_fonciere", "prix_m2"
            ]].copy()

            comparables["adresse_numero"] = comparables["adresse_numero"].fillna("").astype(str)
            comparables["Adresse"] = comparables["adresse_numero"] + " " + comparables["adresse_nom_voie"].str.title()
            comparables["Date"] = pd.to_datetime(comparables["date_mutation"]).dt.strftime("%m/%Y")
            comparables["Type"] = comparables["type_local"]
            comparables["Surface"] = comparables["surface_reelle_bati"].apply(lambda x: f"{int(x)} m²")
            comparables["Pièces"] = comparables["nombre_pieces_principales"].fillna("–").apply(
                lambda x: str(int(x)) if x != "–" else "–"
            )
            comparables["Prix total"] = comparables["valeur_fonciere"].apply(lambda x: f"{int(x):,} €")
            comparables["Prix /m²"] = comparables["prix_m2"].apply(lambda x: f"{int(x):,} €")

            st.dataframe(
                comparables[["Adresse", "Date", "Type", "Surface", "Pièces", "Prix total", "Prix /m²"]],
                hide_index=True,
                use_container_width=True
            )

            # Export CSV
            csv = comparables[["Adresse", "Date", "Type", "Surface", "Pièces", "Prix total", "Prix /m²"]].to_csv(index=False)
            st.download_button(
                "⬇️ Exporter les comparables (CSV)",
                data=csv,
                file_name=f"comparables_{voie_selectionnee.replace(' ', '_')}_{commune_selectionnee}.csv",
                mime="text/csv"
            )

        # ── Note de fiabilité ──
        fiabilite = "🟢 Élevée" if stats["nb_ventes"] >= 20 else "🟡 Moyenne" if stats["nb_ventes"] >= 5 else "🔴 Faible"
        st.markdown(f"""
        <div style="background:#1A1A2E;border:1px solid #2A2A3E;border-radius:8px;padding:1rem 1.2rem;margin-top:1rem;font-size:0.85rem;color:#A0A0A0;">
          <strong style="color:#fff;">Fiabilité de l'estimation :</strong> {fiabilite} ({stats['nb_ventes']} transaction(s) sur 3 ans) &nbsp;·&nbsp;
          <strong style="color:#fff;">Source :</strong> DVF DGFiP (actes notariés) &nbsp;·&nbsp;
          <strong style="color:#fff;">Période :</strong> 2022–2024 &nbsp;·&nbsp;
          Outliers filtrés (P10–P90)
        </div>
        """, unsafe_allow_html=True)

else:
    # État vide — instructions
    st.markdown("""
    <div style="text-align:center;padding:3rem 2rem;color:#A0A0A0;">
      <div style="font-size:3rem;margin-bottom:1rem;">🏘️</div>
      <h3 style="color:#fff;margin-bottom:0.5rem;">Recherchez une rue pour démarrer</h3>
      <p>Tapez au moins 3 caractères du nom de voie (RUE, BD, AV, IMP, ALLEE…)</p>
      <p style="margin-top:1rem;font-size:0.8rem;">
        Données : <strong style="color:#00DC82;">2 354 322 ventes</strong> ·
        <strong style="color:#00DC82;">711 000 rues</strong> ·
        <strong style="color:#00DC82;">16 départements prioritaires</strong>
      </p>
    </div>
    """, unsafe_allow_html=True)
