"""
SAHAR Conseil — Carte Scoring DVF + DPE
========================================
Heatmap + points cliquables par bien.
Score Opportunité Acheteur & Score Probabilité de Vente.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="SAHAR — Carte Scoring Immobilier",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
  .main .block-container { padding-top: .75rem; padding-bottom: 1rem; }
  .stMetric label { font-size: .75rem; color: #888; font-weight: 500; }
  .stMetric [data-testid="stMetricValue"] { font-size: 1.35rem; font-weight: 700; color: #111; }
  div[data-testid="stSidebarContent"] { padding-top: .75rem; }
  .score-badge {
    display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: .78rem; font-weight: 600;
  }
  .badge-green  { background: #e8f5f0; color: #1D9E75; }
  .badge-orange { background: #fff3e0; color: #e65100; }
  .badge-grey   { background: #f0f0f0; color: #666; }
  .badge-red    { background: #fce8e8; color: #c62828; }
  .info-card {
    background: #f8f9fa; border: 1px solid #e5e5e5; border-radius: 10px;
    padding: 14px 18px; margin-bottom: 10px;
  }
  .info-card h4 { margin: 0 0 6px 0; font-size: .9rem; color: #185FA5; }
  .info-card p  { margin: 0; font-size: .83rem; color: #555; }
</style>
""", unsafe_allow_html=True)

# ── Auth ──────────────────────────────────────────────────────────────────────

def check_auth():
    if st.session_state.get("auth_ok"):
        return
    try:
        pwd = st.secrets.get("APP_PWD", "")
    except Exception:
        return  # dev local sans secrets

    if not pwd:
        return

    with st.sidebar:
        st.markdown("### 🔐 Accès SAHAR")
        entered = st.text_input("Mot de passe", type="password", label_visibility="collapsed")
        if entered:
            if entered == pwd:
                st.session_state["auth_ok"] = True
                st.rerun()
            else:
                st.error("Mot de passe incorrect")
                st.stop()
        else:
            st.info("Entrez votre mot de passe pour accéder à la carte.")
            st.stop()

check_auth()

# ── Chargement données ────────────────────────────────────────────────────────

def find_enrichi(dept: str) -> Path | None:
    bases = [
        Path(__file__).resolve().parents[2],
        Path(__file__).resolve().parents[3],
        Path("/mount/src/sahar-conseil/sahar-conseil"),
        Path("/mount/src/sahar-conseil"),
    ]
    for base in bases:
        p = base / "data" / "processed" / f"enrichi_{dept}.parquet"
        if p.exists() and p.stat().st_size > 1000:
            return p
    return None


@st.cache_data(ttl=3600, show_spinner=False)
def load_data(dept: str) -> pd.DataFrame:
    path = find_enrichi(dept)
    if path is None:
        # Fallback : charger DVF de base et calculer scores sans DPE
        from shared.scoring_commune import compute_score_commune
        bases = [
            Path(__file__).resolve().parents[2],
            Path("/mount/src/sahar-conseil/sahar-conseil"),
        ]
        for base in bases:
            p = base / "data" / "processed" / f"dvf_{dept}.parquet"
            if p.exists():
                df = pd.read_parquet(p)
                df = df.dropna(subset=["latitude", "longitude"])
                # Scores simplifiés sans DPE
                med = df.groupby("code_commune")["prix_m2"].transform("median")
                def norm(s):
                    mn, mx = s.min(), s.max()
                    return pd.Series(50.0, index=s.index) if mx == mn else (s - mn) / (mx - mn) * 100
                ratio = ((med - df["prix_m2"]) / med.replace(0, np.nan)).clip(0, 1).fillna(0)
                df["score_acheteur"] = norm(ratio).round(0).clip(0, 100).astype(int)
                duree = (pd.Timestamp.now() - df["date_mutation"]).dt.days.fillna(0) / 365.25
                df["score_vente"] = (
                    (1 - ((duree - 10).abs() / 10).clip(0, 1)) * 100
                ).round(0).clip(0, 100).astype(int)
                df["score_global"] = ((df["score_acheteur"] + df["score_vente"]) / 2).round(0).astype(int)
                df["classe_energie"] = None
                df["passoire"] = False
                df["label_acheteur"] = df["score_acheteur"].apply(
                    lambda x: "🟢 Opportunité forte" if x >= 70 else ("🟡 Intéressant" if x >= 45 else "⚪ Standard"))
                df["label_vente"] = df["score_vente"].apply(
                    lambda x: "🔴 Probable" if x >= 70 else ("🟠 Possible" if x >= 45 else "⚪ Faible"))
                return df.reset_index(drop=True)
        st.error("Aucune donnée disponible. Lancez d'abord pipeline_enrichissement.py")
        st.stop()

    return pd.read_parquet(path)


# ── Sidebar filtres ───────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 🗺️ Carte Scoring")
    st.caption("SAHAR Conseil")
    st.divider()

    dept = st.selectbox(
        "Département",
        options=[str(i).zfill(2) for i in range(1, 96) if i != 20] + ["2A", "2B"],
        index=31  # 33 Gironde par défaut
    )

    with st.spinner("Chargement..."):
        df_all = load_data(dept)

    st.divider()
    st.markdown("**Filtres**")

    types_bien = st.multiselect(
        "Type de bien",
        ["Appartement", "Maison"],
        default=["Appartement", "Maison"]
    )

    communes_dispo = sorted(df_all["nom_commune"].dropna().unique().tolist())
    communes_sel = st.multiselect(
        "Communes (laisser vide = toutes)",
        communes_dispo,
        default=[]
    )

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        score_ach_min = st.slider("Score acheteur min", 0, 100, 0, 5)
    with col_s2:
        score_vente_min = st.slider("Score vente min", 0, 100, 0, 5)

    passoires_only = st.checkbox("🔴 Passoires F/G uniquement", value=False)

    st.divider()
    st.markdown("**Affichage carte**")
    mode_carte = st.radio(
        "Mode",
        ["🔥 Heatmap", "📍 Points", "Les deux"],
        index=2
    )

    score_couleur = st.radio(
        "Colorer par",
        ["Score acheteur", "Score vente", "Score global"],
        index=2
    )

    n_max_points = st.slider("Nb max points affichés", 500, 5000, 2000, 500)

# ── Application filtres ───────────────────────────────────────────────────────

df = df_all.copy()

if types_bien:
    df = df[df["type_local"].isin(types_bien)]
if communes_sel:
    df = df[df["nom_commune"].isin(communes_sel)]
if passoires_only:
    df = df[df["passoire"] == True]

df = df[df["score_acheteur"] >= score_ach_min]
df = df[df["score_vente"] >= score_vente_min]
df = df.dropna(subset=["latitude", "longitude"])

# ── Header ────────────────────────────────────────────────────────────────────

col_title, col_info = st.columns([3, 1])
with col_title:
    st.markdown(f"### 🗺️ Carte scoring — Département {dept}")
    st.caption(f"{len(df):,} biens affichés sur {len(df_all):,} transactions")
with col_info:
    has_dpe = df["classe_energie"].notna().any() if "classe_energie" in df.columns else False
    if has_dpe:
        st.success("✓ DPE intégré", icon="⚡")
    else:
        st.warning("DVF seul (sans DPE)", icon="⚠️")

# ── KPIs ──────────────────────────────────────────────────────────────────────

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    st.metric("Biens filtrés", f"{len(df):,}")
with k2:
    st.metric("Score acheteur médian", f"{df['score_acheteur'].median():.0f}/100")
with k3:
    st.metric("Score vente médian", f"{df['score_vente'].median():.0f}/100")
with k4:
    n_opp = (df["score_acheteur"] >= 70).sum()
    st.metric("Opportunités acheteur (≥70)", f"{n_opp:,}")
with k5:
    n_vente = (df["score_vente"] >= 70).sum()
    st.metric("Probables à vendre (≥70)", f"{n_vente:,}")

st.divider()

# ── CARTE ─────────────────────────────────────────────────────────────────────

score_col_map = {
    "Score acheteur": "score_acheteur",
    "Score vente":    "score_vente",
    "Score global":   "score_global",
}
col_score = score_col_map[score_couleur]

# Échantillon pour performance
df_map = df.sample(min(n_max_points, len(df)), random_state=42) if len(df) > n_max_points else df.copy()

# Centre de la carte
lat_center = df_map["latitude"].median()
lon_center = df_map["longitude"].median()

fig = go.Figure()

# ── Heatmap ──
if mode_carte in ["🔥 Heatmap", "Les deux"]:
    fig.add_trace(go.Densitymapbox(
        lat=df_map["latitude"],
        lon=df_map["longitude"],
        z=df_map[col_score],
        radius=12,
        colorscale=[
            [0,   "#1a237e"],
            [0.3, "#1565c0"],
            [0.5, "#f9a825"],
            [0.75,"#e65100"],
            [1,   "#b71c1c"],
        ],
        opacity=0.65,
        showscale=True,
        colorbar=dict(
            title=dict(text=score_couleur, side="right"),
            thickness=12,
            len=0.6,
        ),
        name="Heatmap",
        hoverinfo="skip",
    ))

# ── Points cliquables ──
if mode_carte in ["📍 Points", "Les deux"]:
    # Couleur par score
    colors = df_map[col_score].values
    hover_text = []
    for _, r in df_map.iterrows():
        energie = r.get("classe_energie", None)
        energie_str = f"DPE {energie}" if pd.notna(energie) else "DPE non dispo"
        txt = (
            f"<b>{r.get('adresse', '—')}</b><br>"
            f"{r.get('nom_commune', '—')} | {r.get('type_local', '—')}<br>"
            f"Surface : {r.get('surface_utile', 0):.0f} m²  |  "
            f"Prix : {r.get('valeur_fonciere', 0):,.0f} €<br>"
            f"Prix/m² : {r.get('prix_m2', 0):.0f} €/m²<br>"
            f"{energie_str}<br>"
            f"─────────────────<br>"
            f"🏠 Score acheteur : <b>{r.get('score_acheteur', 0)}/100</b>  {r.get('label_acheteur', '')}<br>"
            f"📢 Score vente    : <b>{r.get('score_vente', 0)}/100</b>  {r.get('label_vente', '')}"
        )
        hover_text.append(txt)

    fig.add_trace(go.Scattermapbox(
        lat=df_map["latitude"],
        lon=df_map["longitude"],
        mode="markers",
        marker=dict(
            size=8,
            color=colors,
            colorscale=[
                [0,    "#9e9e9e"],
                [0.45, "#ffc107"],
                [0.7,  "#ff5722"],
                [1,    "#b71c1c"],
            ],
            cmin=0,
            cmax=100,
            opacity=0.85,
            showscale=False,
        ),
        text=hover_text,
        hoverinfo="text",
        name="Biens",
    ))

fig.update_layout(
    mapbox=dict(
        style="carto-positron",
        center=dict(lat=lat_center, lon=lon_center),
        zoom=9,
    ),
    margin=dict(l=0, r=0, t=0, b=0),
    height=550,
    showlegend=False,
)

st.plotly_chart(fig, use_container_width=True)

# ── Tableau top opportunités ──────────────────────────────────────────────────

st.divider()
tab1, tab2, tab3 = st.tabs([
    "🏠 Top Opportunités Acheteur",
    "📢 Top Probables à Vendre",
    "🔴 Passoires Thermiques",
])

COLS_AFFICHAGE = {
    "score_acheteur":  "Score Acheteur",
    "score_vente":     "Score Vente",
    "nom_commune":     "Commune",
    "adresse":         "Adresse",
    "type_local":      "Type",
    "surface_utile":   "m²",
    "prix_m2":         "€/m²",
    "valeur_fonciere": "Prix €",
    "classe_energie":  "DPE",
    "label_acheteur":  "Signal Acheteur",
    "label_vente":     "Signal Vente",
}

def show_table(df_top: pd.DataFrame, sort_col: str, label: str):
    cols_ok = [c for c in COLS_AFFICHAGE.keys() if c in df_top.columns]
    df_show = df_top.nlargest(50, sort_col)[cols_ok].rename(columns=COLS_AFFICHAGE).copy()
    st.caption(f"Top 50 — triés par {label}")
    st.dataframe(
        df_show,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Score Acheteur": st.column_config.ProgressColumn(
                "Score Acheteur", min_value=0, max_value=100, format="%d"),
            "Score Vente": st.column_config.ProgressColumn(
                "Score Vente", min_value=0, max_value=100, format="%d"),
            "€/m²":  st.column_config.NumberColumn("€/m²", format="%d €"),
            "Prix €": st.column_config.NumberColumn("Prix €", format="%,.0f €"),
            "m²":    st.column_config.NumberColumn("m²", format="%.0f"),
        }
    )

with tab1:
    show_table(df, "score_acheteur", "Score Acheteur")

with tab2:
    show_table(df, "score_vente", "Score Vente")

with tab3:
    df_pass = df[df["passoire"] == True] if "passoire" in df.columns else pd.DataFrame()
    if df_pass.empty:
        st.info("Aucune passoire thermique dans la sélection (DPE requis).")
        st.markdown("""
        <div class="info-card">
          <h4>Comment activer les données DPE ?</h4>
          <p>Lancez le pipeline d'enrichissement :<br>
          <code>python apps/carte_scoring/pipeline_enrichissement.py --dept 33</code><br>
          Cela enrichira les données DVF avec les DPE ADEME.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.metric("Passoires F/G détectées", f"{len(df_pass):,}")
        show_table(df_pass, "score_vente", "Score Vente")

# ── Analyse par commune ───────────────────────────────────────────────────────

st.divider()
st.markdown("### 📊 Synthèse par commune")

df_comm = df.groupby("nom_commune").agg(
    nb_biens=("prix_m2", "count"),
    prix_median=("prix_m2", "median"),
    score_ach_med=("score_acheteur", "mean"),
    score_vente_med=("score_vente", "mean"),
    nb_passoires=("passoire", "sum") if "passoire" in df.columns else ("prix_m2", "count"),
).reset_index()

df_comm["score_ach_med"]   = df_comm["score_ach_med"].round(0).astype(int)
df_comm["score_vente_med"] = df_comm["score_vente_med"].round(0).astype(int)
df_comm = df_comm[df_comm["nb_biens"] >= 5].sort_values("score_ach_med", ascending=False)

col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    top15 = df_comm.head(15)
    fig_comm = px.bar(
        top15, x="score_ach_med", y="nom_commune",
        orientation="h",
        color="score_ach_med",
        color_continuous_scale=["#9e9e9e", "#ffc107", "#e65100", "#b71c1c"],
        text="score_ach_med",
        title="Top 15 communes — Score Acheteur moyen",
        labels={"score_ach_med": "Score", "nom_commune": ""},
    )
    fig_comm.update_layout(
        height=400, margin=dict(l=0, r=0, t=35, b=0),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False, font=dict(size=11),
        yaxis={"categoryorder": "total ascending"},
    )
    fig_comm.update_traces(textposition="outside")
    st.plotly_chart(fig_comm, use_container_width=True)

with col_chart2:
    fig_scatter = px.scatter(
        df_comm,
        x="score_ach_med",
        y="score_vente_med",
        size="nb_biens",
        color="prix_median",
        hover_name="nom_commune",
        color_continuous_scale="RdYlGn_r",
        title="Communes : Opportunité acheteur vs Probabilité de vente",
        labels={
            "score_ach_med": "Score Acheteur moyen",
            "score_vente_med": "Score Vente moyen",
            "prix_median": "Prix médian €/m²",
            "nb_biens": "Nb biens",
        },
    )
    # Quadrants
    mid = 50
    fig_scatter.add_hline(y=mid, line_dash="dot", line_color="#ccc")
    fig_scatter.add_vline(x=mid, line_dash="dot", line_color="#ccc")
    fig_scatter.add_annotation(x=75, y=75, text="🎯 Zone prioritaire", showarrow=False,
                                font=dict(size=9, color="#1D9E75"))
    fig_scatter.update_layout(
        height=400, margin=dict(l=0, r=0, t=35, b=0),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(size=11),
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

# ── Export ────────────────────────────────────────────────────────────────────

st.divider()
col_exp1, col_exp2 = st.columns(2)

with col_exp1:
    if st.button("⬇️ Exporter les biens filtrés (CSV)", use_container_width=True):
        cols_exp = [c for c in COLS_AFFICHAGE.keys() if c in df.columns]
        csv = df[cols_exp].rename(columns=COLS_AFFICHAGE).to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Télécharger CSV",
            data=csv,
            file_name=f"sahar_scoring_{dept}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

with col_exp2:
    if st.button("⬇️ Exporter synthèse communes (CSV)", use_container_width=True):
        csv_comm = df_comm.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Télécharger CSV communes",
            data=csv_comm,
            file_name=f"sahar_communes_{dept}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

# ── Footer ────────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    f"SAHAR Conseil — Sources : DVF data.gouv.fr, ADEME DPE, BAN. "
    f"Scores à titre indicatif. {datetime.now().strftime('%d/%m/%Y %H:%M')}"
)
