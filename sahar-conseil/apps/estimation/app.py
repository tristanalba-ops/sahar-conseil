"""
SAHAR Conseil — Estimateur Immobilier
Route : /estimation

Outil public de simulation de valeur immobilière.
Lead magnet : l'utilisateur entre son adresse + caractéristiques
et reçoit une estimation + capture email.

Sources : DVF + DPE + BAN + POI via kpi_engine + econometrics
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st
import pandas as pd
from datetime import datetime

from shared.econometrics import (
    HedoniqueModel,
    estimer_bien,
    valeur_verte,
    prime_localisation,
    calculer_rendement_locatif,
    score_investissement,
    DPE_IMPACT_PCT,
)
from shared.kpi_engine import kpi, label_score

# ─── CONFIG ──────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="SAHAR — Estimer mon bien",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .main .block-container { padding-top: 2rem; padding-bottom: 3rem; max-width: 1100px; }
    h1, h2, h3 { font-family: 'DM Serif Display', serif; }
    .sahar-tag {
        background: #00DC82; color: #0D0D0D; font-size: 11px;
        font-weight: 700; letter-spacing: 1px; text-transform: uppercase;
        padding: 3px 10px; border-radius: 20px; display: inline-block;
    }
    .price-card {
        background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
        border: 2px solid #00DC82; border-radius: 12px;
        padding: 2rem; text-align: center;
    }
    .price-main { font-size: 3rem; font-weight: 800; color: #15803d; line-height: 1; }
    .price-sub  { font-size: 1rem; color: #16a34a; margin-top: .5rem; }
    .axis-bar {
        height: 8px; border-radius: 4px;
        background: #e5e7eb; margin-top: 4px;
        overflow: hidden;
    }
    .axis-fill { height: 100%; border-radius: 4px; background: #00DC82; }
    .info-box {
        background: #f8fafc; border: 1px solid #e2e8f0;
        border-radius: 8px; padding: 1rem 1.25rem; margin-bottom: .75rem;
    }
    .disclaimer {
        font-size: 11px; color: #94a3b8; margin-top: 1rem;
        border-top: 1px solid #e2e8f0; padding-top: .75rem;
    }
    footer { visibility: hidden; }
    #MainMenu { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def geocoder_adresse(adresse: str):
    """Géocode via BAN. Retourne (lat, lon, cp, ville) ou None."""
    try:
        import requests
        r = requests.get(
            "https://api-adresse.data.gouv.fr/search/",
            params={"q": adresse, "limit": 1},
            timeout=5,
        )
        features = r.json().get("features", [])
        if features:
            f = features[0]
            coords = f["geometry"]["coordinates"]
            props = f["properties"]
            return {
                "lat": coords[1],
                "lon": coords[0],
                "cp": props.get("postcode", ""),
                "ville": props.get("city", ""),
                "dept": props.get("postcode", "")[:2] if props.get("postcode") else "",
                "label": props.get("label", adresse),
                "score_ban": props.get("score", 0),
            }
    except Exception:
        pass
    return None


def _condition_bonus(condition: str) -> float:
    """Convertit l'état du bien en bonus/malus €/m²."""
    return {
        "mauvais":   -300,
        "moyen":     -150,
        "bon":          0,
        "tres_bon":   150,
        "excellent":  300,
    }.get(condition, 0)


def _options_bonus(has_parking, has_balcony, has_garden, has_view) -> float:
    """Bonus équipements en €/m² (répartis sur surface moyenne 65m²)."""
    total = 0
    if has_parking: total += 8_000
    if has_balcony: total += 4_000
    if has_garden:  total += 6_000
    if has_view:    total += 5_000
    return total / 65  # converti en €/m²


def sauvegarder_lead(nom, email, telephone, adresse, estimation, confiance):
    """Sauvegarde le lead dans Supabase."""
    try:
        from supabase import create_client
        url = st.secrets.get("SUPABASE_URL", "")
        key = st.secrets.get("SUPABASE_KEY", "")
        if not url or not key:
            return False
        client = create_client(url, key)
        client.table("property_valuations").insert({
            "name": nom,
            "email": email,
            "phone": telephone,
            "address": adresse,
            "estimated_value": estimation,
            "confidence": confiance,
            "created_at": datetime.now().isoformat(),
        }).execute()
        return True
    except Exception:
        return False


# ─── HEADER ──────────────────────────────────────────────────────────────────

col_logo, col_titre = st.columns([1, 5])
with col_logo:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 🏠")
with col_titre:
    st.markdown('<span class="sahar-tag">SAHAR Conseil</span>', unsafe_allow_html=True)
    st.markdown("## Estimez votre bien immobilier")
    st.caption("Modèle économétrique · Données DVF + DPE + POI · Résultat instantané")

st.divider()

# ─── FORMULAIRE ──────────────────────────────────────────────────────────────

with st.form("estimation_form"):
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown("**📍 Localisation**")
        adresse_input = st.text_input(
            "Adresse complète",
            placeholder="Ex : 14 rue des Fleurs, Bordeaux",
            help="L'adresse est géocodée via la Base Adresse Nationale (BAN)",
        )

        col_cp, col_ville = st.columns(2)
        with col_cp:
            code_postal = st.text_input("Code postal", placeholder="33000")
        with col_ville:
            ville = st.text_input("Ville", placeholder="Bordeaux")

    with col2:
        st.markdown("**🏗️ Type de bien**")
        type_bien = st.selectbox(
            "Nature",
            ["Appartement", "Maison", "Terrain", "Autre"],
        )
        surface = st.number_input("Surface habitable (m²)", min_value=10, max_value=1000, value=75)
        nb_pieces = st.number_input("Nombre de pièces", min_value=1, max_value=20, value=3)

    st.markdown("---")
    col3, col4, col5 = st.columns(3)

    with col3:
        st.markdown("**📅 Caractéristiques**")
        annee_construction = st.number_input(
            "Année de construction",
            min_value=1800,
            max_value=datetime.now().year,
            value=1990,
        )
        etage = st.number_input("Étage", min_value=0, max_value=50, value=2)

    with col4:
        st.markdown("**⚡ Énergie & État**")
        dpe = st.selectbox(
            "Classe DPE",
            ["A", "B", "C", "D", "E", "F", "G"],
            index=3,
            help="Diagnostic de Performance Énergétique",
        )
        etat = st.selectbox(
            "État du bien",
            ["mauvais", "moyen", "bon", "tres_bon", "excellent"],
            index=2,
            format_func=lambda x: {
                "mauvais": "😟 Mauvais",
                "moyen": "😐 Moyen",
                "bon": "🙂 Bon",
                "tres_bon": "😊 Très bon",
                "excellent": "🤩 Excellent",
            }[x],
        )

    with col5:
        st.markdown("**✨ Options**")
        has_parking = st.checkbox("🚗 Parking")
        has_balcony = st.checkbox("🌿 Balcon / Terrasse")
        has_garden  = st.checkbox("🌳 Jardin")
        has_view    = st.checkbox("🌅 Vue dégagée")

    submitted = st.form_submit_button(
        "📊 Calculer mon estimation",
        type="primary",
        use_container_width=True,
    )


# ─── CALCUL ──────────────────────────────────────────────────────────────────

if submitted:
    if not adresse_input and not (code_postal and ville):
        st.error("Merci de renseigner l'adresse ou le code postal + ville.")
        st.stop()

    # Construire l'adresse complète
    adresse_complete = adresse_input or f"{code_postal} {ville}"

    with st.spinner("Géocodage et analyse du marché local…"):
        # 1. Géocodage BAN
        geo = geocoder_adresse(adresse_complete)
        if not geo and code_postal:
            geo = geocoder_adresse(f"{code_postal} {ville}")

        lat = geo["lat"] if geo else None
        lon = geo["lon"] if geo else None
        dept = (geo["dept"] if geo else code_postal[:2] if code_postal else None)
        ville_geo = geo["ville"] if geo else ville

        # 2. KPIs localisation POI
        score_loc = 50  # défaut
        kpi_loc = {}
        if lat and lon:
            try:
                kpi_loc = kpi.localisation(lat=lat, lon=lon, rayon_m=500)
                score_loc = kpi_loc.get("score_global", 50)
            except Exception:
                pass

        # 3. KPIs marché immobilier
        evolution_12m = 0.0
        prix_median_commune = None
        kpi_immo = {}
        if dept:
            try:
                kpi_immo = kpi.immobilier(departement=dept)
                evolution_12m = kpi_immo.get("evolution_12m") or 0.0
                prix_median_commune = kpi_immo.get("prix_median")
            except Exception:
                pass

        # 4. Modèle économétrique
        result = estimer_bien(
            surface=surface,
            dpe_label=dpe,
            score_localisation=score_loc,
            evolution_12m=evolution_12m,
            type_bien=type_bien,
            prix_median_commune=prix_median_commune,
        )

        # Ajustements manuels (état + options)
        bonus_etat    = _condition_bonus(etat)
        bonus_options = _options_bonus(has_parking, has_balcony, has_garden, has_view)
        prix_m2_final = result["prix_m2_estime"] + bonus_etat + bonus_options
        prix_m2_final = max(500.0, prix_m2_final)
        valeur_totale = prix_m2_final * surface
        ic_bas  = result["ic_bas"]  + (bonus_etat + bonus_options) * surface / surface
        ic_haut = result["ic_haut"] + (bonus_etat + bonus_options) * surface / surface

        # 5. Valeur verte (si DPE mauvais → simuler passage C)
        vv = None
        if dpe in ["E", "F", "G"]:
            vv = valeur_verte(dpe, "C", surface, prix_m2_final)

    # ─── AFFICHAGE RÉSULTATS ─────────────────────────────────────────────

    st.markdown("---")
    st.markdown("## Résultats")

    # Prix principal
    col_res1, col_res2, col_res3 = st.columns([2, 1, 1])

    with col_res1:
        st.markdown(f"""
        <div class="price-card">
            <div style="font-size:.85rem;text-transform:uppercase;letter-spacing:1px;color:#16a34a;margin-bottom:.5rem;">
                Valeur estimée
            </div>
            <div class="price-main">
                {valeur_totale:,.0f} €
            </div>
            <div class="price-sub">
                {prix_m2_final:,.0f} €/m² · {surface} m²
            </div>
            <div style="margin-top:1rem;padding:.75rem;background:white;border-radius:8px;">
                <span style="font-size:.85rem;color:#64748b;">Fourchette · </span>
                <span style="font-weight:700;color:#0D0D0D;">
                    {ic_bas*surface:,.0f} € — {ic_haut*surface:,.0f} €
                </span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_res2:
        st.markdown("**Fiabilité du modèle**")
        fiab = result["fiabilite"]
        st.markdown(f"""
        <div class="info-box">
            <div style="font-size:2rem;font-weight:800;color:{'#16a34a' if fiab>=65 else '#ca8a04' if fiab>=45 else '#dc2626'}">
                {fiab}%
            </div>
            <div style="font-size:.8rem;color:#64748b;">{result['label_fiabilite']}</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("**Marché local**")
        evol_color = "#16a34a" if evolution_12m >= 0 else "#dc2626"
        evol_sign  = "+" if evolution_12m >= 0 else ""
        st.markdown(f"""
        <div class="info-box">
            <div style="font-size:1.6rem;font-weight:800;color:{evol_color}">
                {evol_sign}{evolution_12m:.1f}%
            </div>
            <div style="font-size:.8rem;color:#64748b;">Évolution prix 12 mois</div>
        </div>
        """, unsafe_allow_html=True)

    with col_res3:
        st.markdown("**Localisation**")
        st.markdown(f"""
        <div class="info-box">
            <div style="font-size:1.6rem;font-weight:800;color:#1d4ed8">{score_loc}/100</div>
            <div style="font-size:.8rem;color:#64748b;">{kpi_loc.get('label_localisation', label_score(score_loc))}</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("**DPE actuel**")
        dpe_colors = {"A":"#00bc4b","B":"#4dbd00","C":"#9dbd00","D":"#f0b429","E":"#f07229","F":"#e84c1e","G":"#cc1a0d"}
        dpe_col = dpe_colors.get(dpe, "#666")
        st.markdown(f"""
        <div class="info-box">
            <div style="font-size:2rem;font-weight:800;color:{dpe_col}">{dpe}</div>
            <div style="font-size:.8rem;color:#64748b;">{DPE_IMPACT_PCT.get(dpe,0)*100:+.0f}% vs marché moyen</div>
        </div>
        """, unsafe_allow_html=True)

    # ─── DÉCOMPOSITION ───────────────────────────────────────────────────

    with st.expander("🔍 Décomposition du prix estimé", expanded=True):
        decomp = result["decomposition"]
        items = [
            ("Base marché communal",  decomp.get("base", 0) + decomp.get("ancrage_commune", 0)),
            ("Effet surface",         decomp.get("effet_surface", 0)),
            ("Effet DPE",             decomp.get("effet_dpe", 0)),
            ("Effet localisation",    decomp.get("effet_localisation", 0)),
            ("Évolution marché",      decomp.get("effet_marche", 0)),
            ("Type de bien",          decomp.get("effet_type", 0)),
            ("État du bien",          bonus_etat),
            ("Options (parking…)",    bonus_options),
        ]

        for label, val in items:
            pct = (val / prix_m2_final * 100) if prix_m2_final else 0
            color = "#16a34a" if val >= 0 else "#dc2626"
            sign  = "+" if val >= 0 else ""
            width = min(100, abs(pct) * 3)
            st.markdown(f"""
            <div style="display:flex;justify-content:space-between;align-items:center;
                        padding:.4rem 0;border-bottom:1px solid #f1f5f9;">
                <span style="font-size:.85rem;color:#475569;">{label}</span>
                <span style="font-size:.9rem;font-weight:700;color:{color};">{sign}{val:,.0f} €/m²</span>
            </div>
            """, unsafe_allow_html=True)

    # ─── VALEUR VERTE ────────────────────────────────────────────────────

    if vv:
        st.markdown("---")
        col_vv1, col_vv2 = st.columns(2)
        with col_vv1:
            st.markdown("### 🌿 Valeur verte — Potentiel rénovation")
            st.markdown(f"""
            En rénovant ce bien de **{dpe}** → **C**, le gain de valeur estimé est :

            **+{vv['gain_total']:,.0f} €** (+{vv['gain_pct']:.1f}%)

            Coût travaux estimé : ~{vv['cout_reno_estime']:,.0f} €
            ROI : **{vv['roi_renovation']:.2f}x** — {vv['commentaire'].split('—')[1].strip() if '—' in vv['commentaire'] else ''}
            """)
        with col_vv2:
            st.info(f"""
            **Aides disponibles :**
            - MaPrimeRénov' (jusqu'à 70% du coût)
            - Éco-PTZ (taux zéro)
            - CEE (Certificats d'Économies d'Énergie)

            👉 [Simuler les aides ADEME](https://www.faire.gouv.fr)
            """)

    # ─── SCORES DÉTAILLÉS POI ────────────────────────────────────────────

    if kpi_loc:
        with st.expander("📍 Détail des équipements à 500m"):
            cats = [
                ("🚌 Transports", "score_transport"),
                ("🛒 Commerces",  "score_commerces"),
                ("🏫 Écoles",     "score_ecoles"),
                ("🏥 Santé",      "score_sante"),
                ("🎭 Loisirs",    "score_loisirs"),
            ]
            cols_poi = st.columns(5)
            for (label, key), col in zip(cats, cols_poi):
                score = kpi_loc.get(key, 50)
                with col:
                    st.metric(label, f"{score}/100")

    # ─── LEAD CAPTURE ────────────────────────────────────────────────────

    st.markdown("---")
    st.markdown("### 📥 Recevez votre rapport complet")
    st.caption("Rapport PDF avec comparables détaillés, carte interactive et recommandations personnalisées.")

    with st.form("lead_form"):
        col_l1, col_l2, col_l3 = st.columns(3)
        with col_l1:
            lead_nom    = st.text_input("Votre nom")
        with col_l2:
            lead_email  = st.text_input("Votre email *")
        with col_l3:
            lead_tel    = st.text_input("Votre téléphone")

        lead_submit = st.form_submit_button(
            "📄 Recevoir mon rapport gratuit",
            type="primary",
            use_container_width=True,
        )

    if lead_submit:
        if not lead_email:
            st.error("Email requis.")
        else:
            ok = sauvegarder_lead(
                nom=lead_nom,
                email=lead_email,
                telephone=lead_tel,
                adresse=adresse_complete,
                estimation=int(valeur_totale),
                confiance=result["fiabilite"],
            )
            st.success(
                "✅ Merci ! Votre rapport sera envoyé sous 24h. "
                "Pour toute question : contact@sahar-conseil.fr"
            )

    # Disclaimer
    st.markdown("""
    <div class="disclaimer">
    ⚠️ Cette estimation est fournie à titre indicatif. Elle repose sur un modèle statistique
    (données DVF, DPE, POI) et ne constitue pas une expertise immobilière.
    La marge d'erreur typique est de ±15%. Pour une évaluation certifiée, contactez un expert immobilier agréé.
    </div>
    """, unsafe_allow_html=True)

# ─── État initial (avant soumission) ─────────────────────────────────────────

else:
    st.info(
        "📝 Renseignez les informations de votre bien ci-dessus et cliquez sur **Calculer mon estimation**. "
        "Le calcul est instantané et basé sur les données publiques françaises."
    )

    with st.expander("ℹ️ Comment fonctionne l'estimation ?"):
        st.markdown("""
        **Modèle économétrique hédonique** — le prix d'un bien est décomposé en contributions de chaque attribut :

        1. **Base marché** — Prix médian DVF des transactions récentes sur votre zone
        2. **Effet DPE** — Impact de la classe énergétique sur la valeur (±8% entre A et G)
        3. **Localisation** — Score des équipements dans un rayon de 500m (transports, commerces, écoles…)
        4. **Dynamisme marché** — Évolution des prix sur 12 mois
        5. **Options** — Parking, balcon, jardin, vue

        Sources : **DVF** (Cerema), **ADEME DPE**, **BAN** (géocodage), **OpenStreetMap** (POI)
        """)
