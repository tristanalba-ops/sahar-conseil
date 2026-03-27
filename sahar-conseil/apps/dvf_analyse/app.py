"""
SAHAR Conseil — DVF Analyse Pro v2
Analyse marché immobilier + CRM Pipeline intégré
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st
import pandas as pd
import numpy as np
import json
from datetime import datetime, date

# ─── CONFIG ──────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="SAHAR Conseil — DVF Analyse Pro",
    page_icon="🏠",
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

# ─── AUTH ─────────────────────────────────────────────────────────────────────

def check_auth():
    if st.session_state.get("auth_ok"):
        return True
    try:
        pwd_attendu = st.secrets["APP_PWD"]
    except Exception:
        return True  # dev sans secrets

    with st.sidebar:
        st.markdown("### 🔐 Accès SAHAR")
        pwd = st.text_input("Mot de passe", type="password", label_visibility="collapsed")
        if pwd:
            if pwd == pwd_attendu:
                st.session_state["auth_ok"] = True
                st.rerun()
            else:
                st.error("Mot de passe incorrect")
                st.stop()
        else:
            st.info("Entrez votre mot de passe")
            st.stop()

check_auth()

# ─── CHARGEMENT DONNÉES ───────────────────────────────────────────────────────

def find_dvf_file(dept: str) -> Path | None:
    """Cherche le fichier DVF dans tous les emplacements possibles."""
    bases = [
        Path(__file__).resolve().parents[2],
        Path(__file__).resolve().parents[3],
        Path("/mount/src/sahar-conseil/sahar-conseil"),
        Path("/mount/src/sahar-conseil"),
    ]
    for base in bases:
        # Parquet d'abord (plus rapide)
        p = base / "data" / "processed" / f"dvf_{dept}.parquet"
        if p.exists() and p.stat().st_size > 1000:
            return p
        # CSV ensuite
        c = base / "data" / "raw" / f"dvf_{dept}.csv"
        if c.exists() and c.stat().st_size > 1000:
            return c
    return None


@st.cache_data(ttl=3600, show_spinner=False)
def load_dvf(dept: str) -> pd.DataFrame:
    """Charge et nettoie les données DVF — parquet prioritaire."""
    path = find_dvf_file(dept)

    if path is None:
        st.error(f"Fichier DVF introuvable pour le département {dept}.")
        st.stop()

    if path.suffix == ".parquet":
        return pd.read_parquet(path)

    # CSV → traitement complet
    cols = ['id_mutation','date_mutation','nature_mutation','valeur_fonciere',
            'adresse_numero','adresse_nom_voie','code_postal','code_commune',
            'nom_commune','type_local','surface_reelle_bati','nombre_pieces_principales',
            'surface_terrain','lot1_surface_carrez','longitude','latitude','id_parcelle']
    dtypes = {'code_commune':'str','code_postal':'str',
              'nature_mutation':'category','type_local':'category','nom_commune':'category'}

    df = pd.read_csv(path, usecols=[c for c in cols if c in pd.read_csv(path, nrows=0).columns],
                     dtype=dtypes, low_memory=False)

    df['date_mutation'] = pd.to_datetime(df['date_mutation'], errors='coerce')
    for col in ['valeur_fonciere','surface_reelle_bati','surface_terrain',
                'lot1_surface_carrez','longitude','latitude']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df[df['type_local'].isin(['Appartement','Maison'])]
    df = df[df['nature_mutation'] == 'Vente']
    df = df.dropna(subset=['valeur_fonciere','surface_reelle_bati'])
    df = df[(df['surface_reelle_bati'] > 5) & (df['valeur_fonciere'] > 1000)]

    df['surface_utile'] = df['surface_reelle_bati']
    if 'lot1_surface_carrez' in df.columns:
        m = df['lot1_surface_carrez'].notna() & (df['lot1_surface_carrez'] > 0)
        df.loc[m, 'surface_utile'] = df.loc[m, 'lot1_surface_carrez']

    df['prix_m2'] = (df['valeur_fonciere'] / df['surface_utile']).round(0)
    df = df[df['prix_m2'].between(500, 25000)]
    df['adresse'] = (df.get('adresse_numero', pd.Series('')).fillna('').astype(str).str.strip() + ' ' +
                     df.get('adresse_nom_voie', pd.Series('')).fillna('').astype(str)).str.strip()
    df['annee'] = df['date_mutation'].dt.year
    df['mois'] = df['date_mutation'].dt.to_period('M').astype(str)

    return df.reset_index(drop=True)


def score_opportunite(df: pd.DataFrame, w_prix=0.4, w_vol=0.3, w_dyn=0.3) -> pd.Series:
    def norm(s):
        mn, mx = s.min(), s.max()
        return pd.Series(50, index=s.index) if mx == mn else ((s - mn) / (mx - mn) * 100)

    med = df.groupby('code_commune')['prix_m2'].transform('median')
    s_prix = norm(((med - df['prix_m2']) / med.replace(0, np.nan)).clip(0).fillna(0))
    s_vol  = norm(df.groupby('code_commune')['prix_m2'].transform('count').astype(float))
    cutoff = pd.Timestamp.now() - pd.DateOffset(months=12)
    s_dyn  = norm((df['date_mutation'] >= cutoff).astype(float))

    return (s_prix * w_prix + s_vol * w_vol + s_dyn * w_dyn).round(0).clip(0, 100).astype(int)


# ─── CRM (session state) ──────────────────────────────────────────────────────

def init_crm():
    if "crm_contacts" not in st.session_state:
        st.session_state.crm_contacts = []
    if "crm_opportunites" not in st.session_state:
        st.session_state.crm_opportunites = []
    if "crm_activites" not in st.session_state:
        st.session_state.crm_activites = []

init_crm()

STAGES = ["Détecté", "Contacté", "Qualifié", "Proposition", "Closing"]
STAGE_COLORS = {"Détecté":"#e5e5e5","Contacté":"#fff3cd","Qualifié":"#cfe2ff",
                "Proposition":"#d1ecf1","Closing":"#d4edda"}

def add_contact(nom, email, tel, type_contact, notes=""):
    st.session_state.crm_contacts.append({
        "id": f"C{len(st.session_state.crm_contacts)+1:04d}",
        "nom": nom, "email": email, "tel": tel,
        "type": type_contact, "notes": notes,
        "date_creation": datetime.now().strftime("%d/%m/%Y"),
    })

def add_opportunite(contact_id, titre, adresse, type_bien, surface, prix,
                    prix_m2, score, source="DVF"):
    st.session_state.crm_opportunites.append({
        "id": f"O{len(st.session_state.crm_opportunites)+1:04d}",
        "contact_id": contact_id,
        "titre": titre, "adresse": adresse,
        "type_bien": type_bien, "surface": surface,
        "prix": prix, "prix_m2": prix_m2, "score": score,
        "source": source, "stage": "Détecté",
        "date_creation": datetime.now().strftime("%d/%m/%Y"),
        "date_update": datetime.now().strftime("%d/%m/%Y"),
    })

def add_activite(opp_id, type_activite, notes, date_str=None, statut="À faire"):
    st.session_state.crm_activites.append({
        "id": f"A{len(st.session_state.crm_activites)+1:04d}",
        "opp_id": opp_id, "type": type_activite,
        "notes": notes, "statut": statut,
        "date": date_str or datetime.now().strftime("%d/%m/%Y"),
        "date_creation": datetime.now().strftime("%d/%m/%Y"),
    })


# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 🏠 DVF Analyse Pro")
    st.caption("SAHAR Conseil")
    st.markdown("---")

    dept = st.selectbox("Département", options=[str(i).zfill(2) for i in range(1,96) if i!=20]+["2A","2B"],
                        index=31)  # 33 par défaut

    types_bien = st.multiselect("Type de bien",
                                 ["Appartement","Maison"],
                                 default=["Appartement","Maison"])

    mois_periode = st.slider("Période (mois)", 6, 60, 36, 6)

    col_s, col_e = st.columns(2)
    with col_s: surf_min = st.number_input("Surface min m²", 10, 500, 20, 10)
    with col_e: surf_max = st.number_input("Surface max m²", 10, 500, 200, 10)

    col_p1, col_p2 = st.columns(2)
    with col_p1: prix_min = st.number_input("Prix min €/m²", 500, 20000, 1000, 500)
    with col_p2: prix_max = st.number_input("Prix max €/m²", 500, 20000, 10000, 500)

    score_min = st.slider("Score minimum", 0, 100, 0, 5)

    st.markdown("---")
    st.markdown("**Pondération scoring**")
    w_prix = st.slider("Sous-valorisation", 0.0, 1.0, 0.40, 0.05)
    w_vol  = st.slider("Volume marché",     0.0, 1.0, 0.30, 0.05)
    w_dyn  = st.slider("Dynamisme",         0.0, 1.0, 0.30, 0.05)
    total_w = w_prix + w_vol + w_dyn
    if abs(total_w - 1.0) > 0.01:
        st.warning(f"Somme poids = {total_w:.2f} ≠ 1")

    st.markdown("---")
    if st.button("🔄 Vider le cache"):
        st.cache_data.clear()
        st.rerun()
    if st.button("🔓 Déconnexion"):
        st.session_state.pop("auth_ok", None)
        st.rerun()


# ─── CHARGEMENT + FILTRAGE ───────────────────────────────────────────────────

with st.spinner("Chargement des données DVF..."):
    df_raw = load_dvf(dept)

cutoff = pd.Timestamp.now() - pd.DateOffset(months=mois_periode)
df = df_raw[
    (df_raw['date_mutation'] >= cutoff) &
    (df_raw['type_local'].isin(types_bien)) &
    (df_raw['surface_utile'].between(surf_min, surf_max)) &
    (df_raw['prix_m2'].between(prix_min, prix_max))
].copy()

if df.empty:
    st.warning("Aucun résultat avec ces filtres. Élargissez les critères.")
    st.stop()

# Scoring
tw = w_prix + w_vol + w_dyn
df['score'] = score_opportunite(df,
    w_prix/tw if tw > 0 else 0.4,
    w_vol/tw  if tw > 0 else 0.3,
    w_dyn/tw  if tw > 0 else 0.3)

df = df[df['score'] >= score_min]
if df.empty:
    st.warning("Aucun bien avec ce score minimum.")
    st.stop()


# ─── HEADER ───────────────────────────────────────────────────────────────────

st.title("🏠 DVF Analyse Pro")
st.caption(f"Département {dept} — {len(df):,} transactions filtrées")

c1, c2, c3, c4, c5 = st.columns(5)
with c1: st.metric("Transactions", f"{len(df):,}")
with c2: st.metric("Prix médian €/m²", f"{df['prix_m2'].median():,.0f} €")
with c3: st.metric("Surface médiane", f"{df['surface_utile'].median():.0f} m²")
with c4: st.metric("Prix médian total", f"{df['valeur_fonciere'].median()/1000:.0f} k€")
with c5:
    nb_opps = (df['score'] >= 70).sum()
    st.metric("Opportunités score ≥70", nb_opps)

st.markdown("---")


# ─── ONGLETS PRINCIPAUX ───────────────────────────────────────────────────────

tab_data, tab_carte, tab_stats, tab_crm, tab_export = st.tabs([
    "📋 Transactions",
    "🗺️ Carte",
    "📊 Statistiques",
    "💼 CRM Pipeline",
    "📥 Export",
])


# ── TAB 1 : TRANSACTIONS ──────────────────────────────────────────────────────

with tab_data:
    col_left, col_right = st.columns([3, 1])

    with col_right:
        st.markdown("**Filtres rapides**")
        communes = ["Toutes"] + sorted(df['nom_commune'].dropna().unique().tolist())
        commune_sel = st.selectbox("Commune", communes)
        tri_col = st.selectbox("Trier par", ["score","prix_m2","valeur_fonciere","date_mutation","surface_utile"])
        tri_asc = st.toggle("Croissant", False)
        nb_lignes = st.slider("Nb lignes", 10, 500, 50, 10)

    with col_left:
        df_affich = df.copy()
        if commune_sel != "Toutes":
            df_affich = df_affich[df_affich['nom_commune'] == commune_sel]

        df_affich = df_affich.sort_values(tri_col, ascending=tri_asc)

        # Colonnes à afficher avec noms lisibles
        df_table = df_affich.head(nb_lignes)[[
            'score','date_mutation','nom_commune','adresse','code_postal',
            'type_local','surface_utile','nombre_pieces_principales',
            'surface_terrain','valeur_fonciere','prix_m2','id_parcelle'
        ]].copy()

        df_table['date_mutation'] = df_table['date_mutation'].dt.strftime('%d/%m/%Y')
        df_table['surface_terrain'] = df_table['surface_terrain'].fillna(0).astype(int)
        df_table['nombre_pieces_principales'] = df_table['nombre_pieces_principales'].fillna(0).astype(int)

        df_table = df_table.rename(columns={
            'score':'Score',
            'date_mutation':'Date vente',
            'nom_commune':'Commune',
            'adresse':'Adresse',
            'code_postal':'CP',
            'type_local':'Type',
            'surface_utile':'Surface m²',
            'nombre_pieces_principales':'Pièces',
            'surface_terrain':'Terrain m²',
            'valeur_fonciere':'Prix €',
            'prix_m2':'€/m²',
            'id_parcelle':'Parcelle',
        })

        st.dataframe(
            df_table,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%d"),
                "Prix €": st.column_config.NumberColumn("Prix €", format="%d €"),
                "€/m²": st.column_config.NumberColumn("€/m²", format="%d €"),
                "Surface m²": st.column_config.NumberColumn("Surface m²", format="%.0f"),
                "Terrain m²": st.column_config.NumberColumn("Terrain m²", format="%d"),
            }
        )
        st.caption(f"{len(df_affich):,} transactions • Affichage des {nb_lignes} premières")

        # Ajouter au CRM depuis le tableau
        with st.expander("➕ Ajouter une opportunité au CRM"):
            idx_sel = st.selectbox("Sélectionner une ligne",
                                   range(min(nb_lignes, len(df_affich))),
                                   format_func=lambda i: f"{df_affich.iloc[i]['adresse']} — {df_affich.iloc[i]['nom_commune']} ({df_affich.iloc[i]['prix_m2']:.0f} €/m²)" if i < len(df_affich) else "")
            if len(df_affich) > 0:
                row = df_affich.iloc[idx_sel]
                st.markdown(f"**{row['adresse']}, {row['nom_commune']} {row['code_postal']}**")
                st.markdown(f"{row['type_local']} — {row['surface_utile']:.0f} m² — {row['valeur_fonciere']:,.0f} € — {row['prix_m2']:.0f} €/m² — Score {row['score']}/100")

                contact_options = ["Créer nouveau contact"] + [f"{c['id']} — {c['nom']}" for c in st.session_state.crm_contacts]
                contact_choix = st.selectbox("Contact associé", contact_options)

                if contact_choix == "Créer nouveau contact":
                    nom_c = st.text_input("Nom")
                    email_c = st.text_input("Email")
                    tel_c = st.text_input("Téléphone")
                    type_c = st.selectbox("Type", ["Vendeur","Acheteur","Investisseur","Promoteur","Agent"])
                else:
                    contact_id = contact_choix.split(" — ")[0]

                if st.button("Ajouter au CRM", type="primary"):
                    if contact_choix == "Créer nouveau contact" and nom_c:
                        add_contact(nom_c, email_c, tel_c, type_c)
                        contact_id = st.session_state.crm_contacts[-1]['id']

                    if contact_choix != "Créer nouveau contact" or (contact_choix == "Créer nouveau contact" and nom_c):
                        add_opportunite(
                            contact_id,
                            titre=f"{row['type_local']} {row['adresse']}, {row['nom_commune']}",
                            adresse=f"{row['adresse']}, {row['nom_commune']} {row['code_postal']}",
                            type_bien=row['type_local'],
                            surface=float(row['surface_utile']),
                            prix=float(row['valeur_fonciere']),
                            prix_m2=float(row['prix_m2']),
                            score=int(row['score'])
                        )
                        st.success("Opportunité ajoutée au CRM !")
                        st.rerun()


# ── TAB 2 : CARTE ─────────────────────────────────────────────────────────────

with tab_carte:
    df_carte = df.dropna(subset=['latitude','longitude']).copy()

    if df_carte.empty:
        st.info("Les coordonnées GPS ne sont pas disponibles dans ce fichier DVF. Fonctionnalité disponible avec les fichiers geo-dvf complets.")
    else:
        try:
            import folium
            from streamlit_folium import st_folium

            max_points = st.slider("Nombre de points sur la carte", 100, 5000, 1000, 100)
            df_map = df_carte.nlargest(max_points, 'score')

            lat_c = df_map['latitude'].median()
            lon_c = df_map['longitude'].median()
            m = folium.Map(location=[lat_c, lon_c], zoom_start=11, tiles="CartoDB positron")

            def couleur(score):
                if score >= 70: return "#1D9E75"
                if score >= 40: return "#BA7517"
                return "#E24B4A"

            for _, row in df_map.iterrows():
                terrain = f" | Terrain {row['surface_terrain']:.0f} m²" if row.get('surface_terrain', 0) > 0 else ""
                pieces = f" | {int(row['nombre_pieces_principales'])} pièces" if row.get('nombre_pieces_principales', 0) > 0 else ""
                popup = f"""
                <div style='font-family:sans-serif;font-size:12px;min-width:200px'>
                  <b>{row['type_local']} — {row['adresse']}</b><br>
                  {row['nom_commune']} {row['code_postal']}<br>
                  <b>Prix :</b> {row['valeur_fonciere']:,.0f} €<br>
                  <b>Surface :</b> {row['surface_utile']:.0f} m²{terrain}{pieces}<br>
                  <b>Prix/m² :</b> {row['prix_m2']:.0f} €<br>
                  <b>Date :</b> {row['date_mutation'].strftime('%d/%m/%Y') if pd.notna(row['date_mutation']) else '—'}<br>
                  <b>Parcelle :</b> {row.get('id_parcelle','—')}<br>
                  <b style='color:{couleur(row["score"])}'>Score : {row["score"]}/100</b>
                </div>"""
                folium.CircleMarker(
                    location=[row['latitude'], row['longitude']],
                    radius=5,
                    color=couleur(row['score']),
                    fill=True,
                    fill_color=couleur(row['score']),
                    fill_opacity=0.75,
                    popup=folium.Popup(popup, max_width=260),
                    tooltip=f"{row['type_local']} — {row['prix_m2']:.0f} €/m² — Score {row['score']}"
                ).add_to(m)

            col_leg = st.columns(3)
            with col_leg[0]: st.markdown("🟢 Score ≥ 70 (opportunité forte)")
            with col_leg[1]: st.markdown("🟡 Score 40–70 (à surveiller)")
            with col_leg[2]: st.markdown("🔴 Score < 40 (marché tendu)")

            st_folium(m, height=520, use_container_width=True)
            st.caption(f"{len(df_map):,} points affichés")

        except ImportError:
            st.warning("Installer folium et streamlit-folium pour activer la carte.")
            st.code("pip install folium streamlit-folium")


# ── TAB 3 : STATISTIQUES ──────────────────────────────────────────────────────

with tab_stats:
    try:
        import plotly.express as px

        col_g1, col_g2 = st.columns(2)

        with col_g1:
            df_mois = df.groupby('mois')['prix_m2'].median().reset_index()
            fig1 = px.line(df_mois, x='mois', y='prix_m2',
                           title='Évolution prix médian €/m²',
                           labels={'mois':'Mois','prix_m2':'€/m²'},
                           color_discrete_sequence=['#185FA5'])
            fig1.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig1, use_container_width=True)

        with col_g2:
            fig2 = px.histogram(df, x='score', nbins=20,
                                title='Distribution des scores',
                                color_discrete_sequence=['#1D9E75'])
            fig2.add_vline(x=70, line_dash='dash', line_color='#1D9E75')
            fig2.add_vline(x=40, line_dash='dash', line_color='#BA7517')
            fig2.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig2, use_container_width=True)

        col_g3, col_g4 = st.columns(2)

        with col_g3:
            fig3 = px.box(df, x='type_local', y='prix_m2',
                          title='Prix €/m² par type',
                          color='type_local',
                          color_discrete_map={'Appartement':'#185FA5','Maison':'#1D9E75'})
            fig3.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', showlegend=False)
            st.plotly_chart(fig3, use_container_width=True)

        with col_g4:
            top_communes = df.groupby('nom_commune').agg(
                nb=('prix_m2','count'), mediane=('prix_m2','median')
            ).nlargest(15,'nb').reset_index()
            fig4 = px.bar(top_communes, x='nb', y='nom_commune', orientation='h',
                          title='Top 15 communes par volume',
                          color='mediane', color_continuous_scale='Blues',
                          labels={'nb':'Transactions','nom_commune':'','mediane':'Médiane €/m²'})
            fig4.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig4, use_container_width=True)

    except ImportError:
        st.info("Installer plotly pour les graphiques : pip install plotly")


# ── TAB 4 : CRM PIPELINE ──────────────────────────────────────────────────────

with tab_crm:
    crm_tabs = st.tabs(["📌 Pipeline", "👥 Contacts", "🎯 Opportunités", "📅 Activités"])

    # ── CRM : PIPELINE KANBAN ──────────────────────────────────────────────

    with crm_tabs[0]:
        st.subheader("Pipeline commercial")

        opps = st.session_state.crm_opportunites
        if not opps:
            st.info("Aucune opportunité dans le pipeline. Ajoutez-en depuis l'onglet Transactions.")
        else:
            cols_kanban = st.columns(len(STAGES))
            for i, stage in enumerate(STAGES):
                stage_opps = [o for o in opps if o['stage'] == stage]
                with cols_kanban[i]:
                    valeur = sum(o['prix'] for o in stage_opps)
                    st.markdown(f"""
                    <div style='background:{STAGE_COLORS[stage]};padding:.6rem .8rem;
                    border-radius:6px;margin-bottom:.75rem'>
                    <b style='font-size:.82rem'>{stage}</b><br>
                    <span style='font-size:.75rem;color:#555'>{len(stage_opps)} opp. • {valeur/1000:.0f}k€</span>
                    </div>""", unsafe_allow_html=True)

                    for opp in stage_opps:
                        score_color = "#1D9E75" if opp['score']>=70 else "#BA7517" if opp['score']>=40 else "#E24B4A"
                        with st.expander(f"📍 {opp['titre'][:35]}...", expanded=False):
                            st.markdown(f"**Adresse :** {opp['adresse']}")
                            st.markdown(f"**Type :** {opp['type_bien']} — {opp['surface']:.0f} m²")
                            st.markdown(f"**Prix :** {opp['prix']:,.0f} € ({opp['prix_m2']:.0f} €/m²)")
                            st.markdown(f"**Score :** <span style='color:{score_color};font-weight:600'>{opp['score']}/100</span>", unsafe_allow_html=True)
                            st.markdown(f"**Source :** {opp['source']} | **Créé :** {opp['date_creation']}")

                            # Avancer dans le pipeline
                            current_idx = STAGES.index(opp['stage'])
                            new_stage = st.selectbox("Étape",
                                                      STAGES,
                                                      index=current_idx,
                                                      key=f"stage_{opp['id']}")
                            if new_stage != opp['stage']:
                                opp['stage'] = new_stage
                                opp['date_update'] = datetime.now().strftime("%d/%m/%Y")
                                st.rerun()

                            # Ajouter activité rapide
                            act_type = st.selectbox("+ Activité", ["Appel","Email","SMS","WhatsApp","Visite","RDV","Note"],
                                                     key=f"act_{opp['id']}")
                            act_note = st.text_input("Note", key=f"note_{opp['id']}")
                            if st.button("Ajouter", key=f"btn_{opp['id']}"):
                                add_activite(opp['id'], act_type, act_note)
                                st.success("Activité ajoutée !")
                                st.rerun()

        # KPIs pipeline
        if opps:
            st.markdown("---")
            st.subheader("KPIs pipeline")
            k1, k2, k3, k4 = st.columns(4)
            with k1: st.metric("Opportunités total", len(opps))
            with k2: st.metric("Valeur pipeline", f"{sum(o['prix'] for o in opps)/1000:.0f}k€")
            with k3:
                closees = [o for o in opps if o['stage'] == 'Closing']
                st.metric("En closing", len(closees))
            with k4:
                score_moyen = sum(o['score'] for o in opps) / len(opps) if opps else 0
                st.metric("Score moyen", f"{score_moyen:.0f}/100")

    # ── CRM : CONTACTS ────────────────────────────────────────────────────

    with crm_tabs[1]:
        col_c1, col_c2 = st.columns([2, 1])

        with col_c2:
            with st.form("form_contact"):
                st.markdown("**Nouveau contact**")
                nom = st.text_input("Nom *")
                email = st.text_input("Email")
                tel = st.text_input("Téléphone")
                type_c = st.selectbox("Type", ["Vendeur","Acheteur","Investisseur","Promoteur","Agent","Autre"])
                notes = st.text_area("Notes", height=80)
                if st.form_submit_button("Créer le contact", type="primary"):
                    if nom:
                        add_contact(nom, email, tel, type_c, notes)
                        st.success(f"Contact {nom} créé !")
                        st.rerun()

        with col_c1:
            contacts = st.session_state.crm_contacts
            if not contacts:
                st.info("Aucun contact. Créez-en un depuis le formulaire.")
            else:
                df_contacts = pd.DataFrame(contacts)
                st.dataframe(df_contacts[['id','nom','email','tel','type','date_creation','notes']].rename(columns={
                    'id':'ID','nom':'Nom','email':'Email','tel':'Téléphone',
                    'type':'Type','date_creation':'Créé le','notes':'Notes'
                }), use_container_width=True, hide_index=True)

    # ── CRM : OPPORTUNITÉS ────────────────────────────────────────────────

    with crm_tabs[2]:
        opps = st.session_state.crm_opportunites
        if not opps:
            st.info("Aucune opportunité. Ajoutez-en depuis l'onglet Transactions.")
        else:
            df_opps = pd.DataFrame(opps)
            cols_show = ['id','titre','adresse','type_bien','surface','prix','prix_m2','score','stage','date_creation']
            df_show = df_opps[cols_show].rename(columns={
                'id':'ID','titre':'Titre','adresse':'Adresse','type_bien':'Type',
                'surface':'Surface m²','prix':'Prix €','prix_m2':'€/m²',
                'score':'Score','stage':'Étape','date_creation':'Créé le'
            })
            st.dataframe(df_show, use_container_width=True, hide_index=True,
                         column_config={
                             "Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%d"),
                             "Prix €": st.column_config.NumberColumn("Prix €", format="%d"),
                             "€/m²": st.column_config.NumberColumn("€/m²", format="%d"),
                         })

    # ── CRM : ACTIVITÉS ───────────────────────────────────────────────────

    with crm_tabs[3]:
        acts = st.session_state.crm_activites

        # Formulaire activité manuelle
        with st.expander("➕ Nouvelle activité"):
            opps_opts = [f"{o['id']} — {o['titre'][:40]}" for o in st.session_state.crm_opportunites]
            if opps_opts:
                with st.form("form_activite"):
                    opp_sel = st.selectbox("Opportunité", opps_opts)
                    c1, c2 = st.columns(2)
                    with c1:
                        type_a = st.selectbox("Type", ["Appel","Email","SMS","WhatsApp","Visite","RDV","Note","Relance"])
                        statut_a = st.selectbox("Statut", ["À faire","Fait","Annulé"])
                    with c2:
                        date_a = st.date_input("Date", value=date.today())
                    notes_a = st.text_area("Notes / compte-rendu", height=80)
                    if st.form_submit_button("Créer l'activité", type="primary"):
                        opp_id = opp_sel.split(" — ")[0]
                        add_activite(opp_id, type_a, notes_a, date_a.strftime("%d/%m/%Y"), statut_a)
                        st.success("Activité créée !")
                        st.rerun()
            else:
                st.info("Créez d'abord une opportunité.")

        # Liste des activités
        if acts:
            df_acts = pd.DataFrame(acts)
            st.dataframe(
                df_acts[['id','opp_id','type','statut','date','notes']].rename(columns={
                    'id':'ID','opp_id':'Opport.','type':'Type','statut':'Statut',
                    'date':'Date','notes':'Notes'
                }),
                use_container_width=True, hide_index=True
            )
        else:
            st.info("Aucune activité enregistrée.")


# ── TAB 5 : EXPORT ────────────────────────────────────────────────────────────

with tab_export:
    st.subheader("Export des données")

    col_ex1, col_ex2 = st.columns(2)

    with col_ex1:
        st.markdown("**Export transactions filtrées**")
        st.caption(f"{len(df):,} transactions avec les filtres actuels")

        df_exp = df[[
            'score','date_mutation','nom_commune','adresse','code_postal',
            'type_local','surface_utile','nombre_pieces_principales',
            'surface_terrain','valeur_fonciere','prix_m2','id_parcelle',
            'longitude','latitude'
        ]].copy()
        df_exp['date_mutation'] = df_exp['date_mutation'].dt.strftime('%d/%m/%Y')
        df_exp = df_exp.rename(columns={
            'score':'Score','date_mutation':'Date vente','nom_commune':'Commune',
            'adresse':'Adresse','code_postal':'Code postal','type_local':'Type',
            'surface_utile':'Surface m²','nombre_pieces_principales':'Pièces',
            'surface_terrain':'Terrain m²','valeur_fonciere':'Prix €',
            'prix_m2':'Prix €/m²','id_parcelle':'Réf. parcelle',
            'longitude':'Longitude','latitude':'Latitude'
        })

        csv_data = df_exp.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button(
            "📥 Télécharger CSV",
            data=csv_data,
            file_name=f"sahar_dvf_{dept}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

        try:
            import io, openpyxl
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                df_exp.to_excel(writer, sheet_name='DVF Transactions', index=False)
                ws = writer.sheets['DVF Transactions']
                from openpyxl.styles import Font, PatternFill
                for cell in ws[1]:
                    cell.font = Font(bold=True, color="FFFFFF")
                    cell.fill = PatternFill(start_color="185FA5", end_color="185FA5", fill_type="solid")
                for col in ws.columns:
                    ws.column_dimensions[col[0].column_letter].width = min(
                        max(len(str(col[0].value or '')), max(len(str(c.value or '')) for c in col)), 40
                    )
            buf.seek(0)
            st.download_button(
                "📥 Télécharger Excel",
                data=buf.getvalue(),
                file_name=f"sahar_dvf_{dept}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except ImportError:
            st.caption("Excel disponible avec : pip install openpyxl")

    with col_ex2:
        st.markdown("**Export CRM**")
        if st.session_state.crm_opportunites:
            df_crm = pd.DataFrame(st.session_state.crm_opportunites)
            csv_crm = df_crm.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                "📥 Export opportunités CRM (CSV)",
                data=csv_crm,
                file_name=f"sahar_crm_opportunites_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
        if st.session_state.crm_contacts:
            df_c = pd.DataFrame(st.session_state.crm_contacts)
            csv_c = df_c.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                "📥 Export contacts CRM (CSV)",
                data=csv_c,
                file_name=f"sahar_crm_contacts_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
        if not st.session_state.crm_opportunites and not st.session_state.crm_contacts:
            st.info("Aucune donnée CRM à exporter.")

        st.markdown("---")
        st.markdown("**Statistiques d'export**")
        st.metric("Transactions disponibles", f"{len(df_raw):,}")
        st.metric("Transactions filtrées", f"{len(df):,}")
        st.metric("Opportunités CRM", len(st.session_state.crm_opportunites))
        st.metric("Contacts CRM", len(st.session_state.crm_contacts))


# ── FOOTER ────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption(
    f"SAHAR Conseil — Sources : DVF data.gouv.fr, INSEE. "
    f"Données à titre indicatif. Dernière mise à jour filtres : {datetime.now().strftime('%d/%m/%Y %H:%M')}"
)
