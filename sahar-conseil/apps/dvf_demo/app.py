"""
SAHAR Conseil — Page démo publique
Accès libre sans mot de passe — capture email via Formspree
"""

import streamlit as st
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ── CONFIG ────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="SAHAR Conseil — Démo gratuite",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .main .block-container{padding-top:1.5rem;max-width:900px;margin:0 auto}
    .stMetric label{font-size:.78rem;color:#73726c}
    .stMetric [data-testid="stMetricValue"]{font-size:1.4rem}
    .hero{background:linear-gradient(135deg,#185FA5 0%,#1D9E75 100%);
          padding:40px 32px;border-radius:12px;color:white;margin-bottom:24px}
    .hero h1{font-size:2rem;font-weight:800;margin:0 0 8px 0}
    .hero p{font-size:1.05rem;opacity:.9;margin:0}
    .form-box{background:#f8f9fa;border:1px solid #e5e5e5;border-radius:10px;
              padding:24px;margin-top:16px}
    .badge{display:inline-block;background:#e8f5f0;color:#1D9E75;
           font-size:.78rem;font-weight:600;padding:3px 10px;border-radius:20px;
           margin-right:6px;margin-bottom:4px}
</style>
""", unsafe_allow_html=True)

FORMSPREE_ID = "xbdplokr"

# ── HERO ──────────────────────────────────────────────────────────────────────

st.markdown("""
<div class="hero">
  <h1>🏠 Analysez le marché immobilier de votre territoire</h1>
  <p>Accédez gratuitement aux données DVF (transactions réelles notariées) — 
  prix médians, évolutions, scoring de tension marché par commune.</p>
</div>
""", unsafe_allow_html=True)

col_b1, col_b2, col_b3 = st.columns(3)
with col_b1:
    st.markdown('<span class="badge">✓ Données officielles DVF</span>', unsafe_allow_html=True)
with col_b2:
    st.markdown('<span class="badge">✓ Mises à jour régulières</span>', unsafe_allow_html=True)
with col_b3:
    st.markdown('<span class="badge">✓ Export CSV & PDF</span>', unsafe_allow_html=True)

st.markdown("---")

# ── CHARGEMENT DONNÉES ────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def load_demo_data(dept: str = "33") -> pd.DataFrame:
    """Charge les données DVF pour la démo — Gironde par défaut."""
    bases = [
        Path(__file__).resolve().parents[2],
        Path(__file__).resolve().parents[3],
        Path("/mount/src/sahar-conseil/sahar-conseil"),
        Path("/mount/src/sahar-conseil"),
    ]
    for base in bases:
        p = base / "data" / "processed" / f"dvf_{dept}.parquet"
        if p.exists() and p.stat().st_size > 1000:
            return pd.read_parquet(p)
    return pd.DataFrame()


def score_tension(df: pd.DataFrame) -> pd.DataFrame:
    """Score simplifié pour la démo."""
    now = pd.Timestamp.now()
    recent = df[df["date_mutation"] >= now - pd.DateOffset(months=12)]
    ancien  = df[
        (df["date_mutation"] >= now - pd.DateOffset(months=24)) &
        (df["date_mutation"] < now - pd.DateOffset(months=12))
    ]

    agg = recent.groupby("nom_commune").agg(
        prix_med=("prix_m2", "median"),
        volume=("prix_m2", "count"),
    ).reset_index()

    agg_n1 = ancien.groupby("nom_commune").agg(
        prix_n1=("prix_m2", "median"),
        vol_n1=("prix_m2", "count"),
    ).reset_index()

    r = agg.merge(agg_n1, on="nom_commune", how="left")
    r = r[r["volume"] >= 5]

    r["evol"] = ((r["prix_med"] - r["prix_n1"]) / r["prix_n1"].replace(0, np.nan) * 100).round(1)
    r["tension"] = (r["volume"] / r["vol_n1"].replace(0, np.nan)).round(2)

    def norm(s):
        mn, mx = s.min(), s.max()
        return pd.Series(50, index=s.index) if mx == mn else (s - mn) / (mx - mn) * 100

    r["score"] = (
        norm(r["volume"]) * 0.40 +
        norm(r["evol"].fillna(0)) * 0.30 +
        norm(r["tension"].fillna(1)) * 0.30
    ).round(0).clip(0, 100).astype(int)

    r["signal"] = r["score"].apply(
        lambda x: "🔴 Marché vendeur" if x >= 65
        else ("🟡 Équilibré" if x >= 40 else "🟢 Opportunité acheteur")
    )

    return r.sort_values("score", ascending=False).reset_index(drop=True)


# ── INTERFACE DÉMO ────────────────────────────────────────────────────────────

dept = "33"  # Gironde — données disponibles

with st.spinner("Chargement des données Gironde..."):
    df_raw = load_demo_data(dept)

if df_raw.empty:
    st.warning("Données en cours de chargement. Revenez dans quelques instants.")
    st.stop()

# Filtre période 24 mois
df = df_raw[df_raw["date_mutation"] >= pd.Timestamp.now() - pd.DateOffset(months=24)].copy()
df = df[df["type_local"].isin(["Appartement", "Maison"])]

# ── KPIs GIRONDE ─────────────────────────────────────────────────────────────

st.markdown("### 📊 Gironde — Chiffres clés du marché")

k1, k2, k3, k4 = st.columns(4)
with k1: st.metric("Transactions (24 mois)", f"{len(df):,}")
with k2: st.metric("Prix médian €/m²", f"{df['prix_m2'].median():,.0f} €")
with k3: st.metric("Communes analysées", df["nom_commune"].nunique())
with k4:
    vol_recent = df[df["date_mutation"] >= pd.Timestamp.now() - pd.DateOffset(months=12)]
    vol_ancien = df[
        (df["date_mutation"] >= pd.Timestamp.now() - pd.DateOffset(months=24)) &
        (df["date_mutation"] < pd.Timestamp.now() - pd.DateOffset(months=12))
    ]
    evol_vol = (len(vol_recent) - len(vol_ancien)) / len(vol_ancien) * 100 if len(vol_ancien) > 0 else 0
    st.metric("Volume 12m vs N-1", f"{evol_vol:+.1f}%")

st.markdown("---")

# ── SCORING COMMUNES ──────────────────────────────────────────────────────────

st.markdown("### 🏙️ Score de tension par commune — Top 20")

with st.spinner("Calcul du scoring..."):
    df_score = score_tension(df)

if not df_score.empty:
    try:
        import plotly.express as px
        top20 = df_score.head(20).copy()
        fig = px.bar(
            top20,
            x="score",
            y="nom_commune",
            orientation="h",
            color="score",
            color_continuous_scale=["#E24B4A", "#BA7517", "#1D9E75"],
            text="score",
            labels={"score": "Score", "nom_commune": ""}
        )
        fig.update_layout(
            height=480,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            coloraxis_showscale=False,
            yaxis={"categoryorder": "total ascending"},
            margin=dict(l=0, r=0, t=10, b=0),
            font=dict(size=11)
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
    except ImportError:
        pass

    # Tableau top 10
    st.markdown("**Top 10 communes en détail**")
    df_table = df_score.head(10)[["nom_commune", "score", "signal", "prix_med", "volume", "evol"]].copy()
    df_table.columns = ["Commune", "Score", "Signal", "Prix médian €/m²", "Transactions 12m", "Évolution (%)"]

    st.dataframe(
        df_table,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%d"),
            "Prix médian €/m²": st.column_config.NumberColumn("Prix médian €/m²", format="%d €"),
            "Évolution (%)": st.column_config.NumberColumn("Évolution (%)", format="%.1f%%"),
        }
    )

st.markdown("---")

# ── TEASER FONCTIONNALITÉS ─────────────────────────────────────────────────────

st.markdown("### 🔒 Fonctionnalités complètes — Accès Pro")

col_feat1, col_feat2, col_feat3 = st.columns(3)
with col_feat1:
    st.markdown("""
    **📋 Toutes les transactions**
    - Filtrage par commune, surface, prix
    - Score d'opportunité par bien
    - Recherche par adresse
    """)
with col_feat2:
    st.markdown("""
    **📄 Rapports PDF exportables**
    - Rapport par commune en 1 clic
    - À envoyer à vos clients
    - Données actualisées
    """)
with col_feat3:
    st.markdown("""
    **💼 CRM intégré**
    - Pipeline commercial
    - Contacts & opportunités
    - Séquences email automatiques
    """)

st.markdown("---")

# ── FORMULAIRE CAPTURE EMAIL ──────────────────────────────────────────────────

st.markdown("""
<div class="form-box">
  <h3 style="margin:0 0 6px 0;color:#185FA5">🎯 Accéder à l'outil complet — 7 jours gratuits</h3>
  <p style="color:#666;margin:0 0 16px 0;font-size:.9rem">
    Laissez votre email et je vous envoie un accès dans les 24h. Aucun engagement.
  </p>
</div>
""", unsafe_allow_html=True)

with st.form("form_demo"):
    col_n, col_e = st.columns(2)
    with col_n:
        nom = st.text_input("Votre nom *", placeholder="Pierre Martin")
    with col_e:
        email = st.text_input("Votre email pro *", placeholder="pierre@agence.fr")

    col_t, col_s = st.columns(2)
    with col_t:
        tel = st.text_input("Téléphone (optionnel)", placeholder="06 00 00 00 00")
    with col_s:
        secteur = st.selectbox(
            "Votre activité",
            ["Agent immobilier", "Mandataire", "Chasseur immobilier",
             "Promoteur", "Investisseur", "Notaire", "Autre"]
        )

    territoire = st.text_input(
        "Territoire d'activité",
        placeholder="Ex : Bordeaux, Gironde, Médoc..."
    )

    submitted = st.form_submit_button("→ Je veux tester gratuitement", type="primary", use_container_width=True)

if submitted:
    if not nom or not email:
        st.error("Merci de renseigner votre nom et votre email.")
    else:
        # Envoi Formspree
        try:
            resp = requests.post(
                f"https://formspree.io/f/{FORMSPREE_ID}",
                json={
                    "Nom": nom,
                    "Email": email,
                    "Téléphone": tel,
                    "Secteur": secteur,
                    "Territoire": territoire,
                    "Source": "Demo DVF",
                    "Date": datetime.now().strftime("%d/%m/%Y %H:%M"),
                },
                headers={"Accept": "application/json"},
                timeout=8
            )
            if resp.status_code == 200:
                st.success(
                    f"✅ Merci {nom} ! Vous recevrez un accès à {email} dans les 24 heures. "
                    "Je vous contacte personnellement pour vous accompagner."
                )
                st.balloons()
            else:
                st.warning("Envoi en cours — si vous ne recevez pas d'email, écrivez à contact@sahar-conseil.fr")
        except Exception:
            st.warning("Envoi en cours — si vous ne recevez pas d'email, écrivez à contact@sahar-conseil.fr")

# ── FOOTER ────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption(
    "SAHAR Conseil — contact@sahar-conseil.fr  |  "
    "Données DVF publiées par la DGFiP sur data.gouv.fr  |  "
    "À titre indicatif — ne constitue pas un conseil en investissement"
)
