"""
SAHAR CRM Interne — app.py
CRM dédié à la gestion commerciale de SAHAR Conseil.
Leads, comptes, opportunités, deals, pipeline, séquences.
Distinct de l'app DVF — c'est l'outil du fondateur.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import json

st.set_page_config(
    page_title="SAHAR CRM",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  *{box-sizing:border-box}
  .main .block-container{padding:1rem 1.5rem}
  .stMetric label{font-size:.75rem;color:#888}
  .stMetric [data-testid="stMetricValue"]{font-size:1.35rem;font-weight:700}
  .stTabs [data-baseweb="tab"]{font-size:.85rem;font-weight:500}
  div[data-testid="stExpander"]{border:1px solid #e5e5e5;border-radius:8px;margin-bottom:.5rem}
  .stage-badge{display:inline-block;padding:.2rem .6rem;border-radius:4px;font-size:.72rem;font-weight:600}
  .source-badge{display:inline-block;padding:.15rem .5rem;border-radius:4px;font-size:.7rem;background:#f0f7ff;color:#185FA5;font-weight:600}
</style>
""", unsafe_allow_html=True)

# ─── AUTH ─────────────────────────────────────────────────────────────────────
def check_auth():
    if st.session_state.get("crm_auth"):
        return
    try:
        pwd = st.secrets.get("APP_PWD", "sahar2024")
    except:
        pwd = "sahar2024"
    col = st.columns([1,2,1])[1]
    with col:
        st.markdown("### 🎯 SAHAR CRM")
        p = st.text_input("Mot de passe", type="password")
        if p:
            if p == pwd:
                st.session_state.crm_auth = True
                st.rerun()
            else:
                st.error("Incorrect")
        st.stop()

check_auth()

# ─── SUPABASE ─────────────────────────────────────────────────────────────────
@st.cache_resource
def get_sb():
    try:
        from supabase import create_client
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets.get("SUPABASE_SERVICE_KEY") or st.secrets["SUPABASE_KEY"]
        return create_client(url, key)
    except:
        return None

SB = get_sb()

def sb_get(table, filters=None, order=None, limit=500):
    if not SB:
        return st.session_state.get(f"crm_{table}", [])
    try:
        q = SB.table(table).select("*")
        if order:
            q = q.order(order, desc=True)
        if limit:
            q = q.limit(limit)
        return q.execute().data or []
    except:
        return st.session_state.get(f"crm_{table}", [])

def sb_insert(table, record):
    if SB:
        try:
            SB.table(table).insert(record).execute()
        except Exception as e:
            st.warning(f"DB: {e}")
    key = f"crm_{table}"
    if key not in st.session_state:
        st.session_state[key] = []
    st.session_state[key].append(record)

def sb_update(table, id_val, data):
    if SB:
        try:
            SB.table(table).update(data).eq("id", id_val).execute()
        except Exception as e:
            st.warning(f"DB update: {e}")
    for item in st.session_state.get(f"crm_{table}", []):
        if item.get("id") == id_val:
            item.update(data)

def sb_delete(table, id_val):
    if SB:
        try:
            SB.table(table).delete().eq("id", id_val).execute()
        except:
            pass
    key = f"crm_{table}"
    st.session_state[key] = [i for i in st.session_state.get(key, []) if i.get("id") != id_val]

def new_id(prefix):
    return f"{prefix}{datetime.now().strftime('%y%m%d%H%M%S')}"

def now_str():
    return datetime.now().strftime("%d/%m/%Y %H:%M")

def today_str():
    return date.today().strftime("%d/%m/%Y")

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
STAGES = ["Nouveau", "Contacté", "Qualifié", "Démo", "Proposition", "Closing", "Client", "Perdu"]
STAGE_COLORS = {
    "Nouveau": "#f5f5f5", "Contacté": "#e3f2fd", "Qualifié": "#e8f5e9",
    "Démo": "#fff8e1", "Proposition": "#fff3e0", "Closing": "#fce4ec",
    "Client": "#e8f5e9", "Perdu": "#f5f5f5"
}
STAGE_TEXT = {
    "Nouveau": "#555", "Contacté": "#185FA5", "Qualifié": "#2e7d32",
    "Démo": "#f57f17", "Proposition": "#e65100", "Closing": "#c62828",
    "Client": "#1b5e20", "Perdu": "#999"
}
SOURCES = ["Site web", "Formulaire", "LinkedIn", "Recommandation", "Appel sortant",
           "Email sortant", "DVF détecté", "DPE détecté", "Autre"]
SECTEURS = ["Immobilier", "Énergie / Rénovation", "Retail / Franchise",
            "RH / Recrutement", "Automobile", "Autre"]
OFFRES = ["Starter 49€/mois", "Pro 99€/mois", "Expert 149€/mois", "Étude sur mesure", "Autre"]

# ─── HEADER ───────────────────────────────────────────────────────────────────
st.markdown("### 🎯 SAHAR CRM")

# ─── LOAD DATA ────────────────────────────────────────────────────────────────
leads     = sb_get("crm_leads",       order="created_at")
contacts  = sb_get("crm_contacts",    order="created_at")
opps      = sb_get("crm_opportunites",order="created_at")
activites = sb_get("crm_activites",   order="created_at")

# ─── KPIs HEADER ──────────────────────────────────────────────────────────────
k1,k2,k3,k4,k5,k6 = st.columns(6)
leads_new   = [l for l in leads if l.get("statut") == "nouveau"]
opps_active = [o for o in opps if o.get("stage") not in ["Client","Perdu"]]
opps_demo   = [o for o in opps if o.get("stage") == "Démo"]
opps_close  = [o for o in opps if o.get("stage") == "Closing"]
clients     = [o for o in opps if o.get("stage") == "Client"]
val_pipe    = sum(float(o.get("valeur_deal",0) or 0) for o in opps_active)

with k1: st.metric("Leads nouveaux", len(leads_new))
with k2: st.metric("Pipeline actif", len(opps_active))
with k3: st.metric("Démos planifiées", len(opps_demo))
with k4: st.metric("En closing", len(opps_close))
with k5: st.metric("Clients", len(clients))
with k6: st.metric("Valeur pipeline", f"{val_pipe:.0f}€")

st.markdown("---")

# ─── ONGLETS ──────────────────────────────────────────────────────────────────
tabs = st.tabs(["📥 Leads", "🎯 Pipeline", "👥 Contacts", "📅 Activités", "📊 Dashboard", "⚙️ Séquences"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — LEADS
# ══════════════════════════════════════════════════════════════════════════════
with tabs[0]:
    col_l, col_r = st.columns([2,1])

    with col_r:
        with st.form("new_lead"):
            st.markdown("**➕ Nouveau lead**")
            nom_l    = st.text_input("Nom *")
            email_l  = st.text_input("Email *")
            tel_l    = st.text_input("Téléphone")
            c1,c2    = st.columns(2)
            with c1: secteur_l = st.selectbox("Secteur", SECTEURS, key="sl")
            with c2: source_l  = st.selectbox("Source", SOURCES, key="sol")
            msg_l    = st.text_area("Message / notes", height=70)
            utm_l    = st.text_input("UTM / paramètres URL", placeholder="utm_source=site&utm_medium=organic")
            send_seq = st.toggle("Envoyer J+0", value=True)

            if st.form_submit_button("Créer le lead →", type="primary"):
                if nom_l and email_l:
                    record = {
                        "id": new_id("L"),
                        "nom": nom_l, "email": email_l, "tel": tel_l,
                        "secteur": secteur_l, "source": source_l,
                        "message": msg_l, "utm_params": utm_l,
                        "statut": "nouveau",
                        "created_at": datetime.now().isoformat(),
                    }
                    sb_insert("crm_leads", record)
                    if send_seq and email_l:
                        try:
                            from shared.emails_site import envoyer_j0
                            ok = envoyer_j0(nom_l, email_l, secteur_l, msg_l)
                            if ok: st.toast(f"✉️ J+0 envoyé à {email_l}")
                        except Exception as e:
                            st.warning(f"Email: {e}")
                    st.success(f"✓ Lead {nom_l} créé")
                    st.rerun()

    with col_l:
        # Filtres
        fc1,fc2,fc3 = st.columns(3)
        with fc1: filt_statut = st.selectbox("Statut", ["Tous","nouveau","contacté","qualifié","perdu"])
        with fc2: filt_source = st.selectbox("Source", ["Toutes"] + SOURCES)
        with fc3: filt_sect   = st.selectbox("Secteur", ["Tous"] + SECTEURS)

        leads_f = leads
        if filt_statut != "Tous": leads_f = [l for l in leads_f if l.get("statut") == filt_statut]
        if filt_source != "Toutes": leads_f = [l for l in leads_f if l.get("source") == filt_source]
        if filt_sect != "Tous": leads_f = [l for l in leads_f if l.get("secteur") == filt_sect]

        st.caption(f"{len(leads_f)} leads")

        for lead in leads_f:
            statut_icon = {"nouveau":"🆕","contacté":"📞","qualifié":"✅","perdu":"❌"}.get(lead.get("statut","nouveau"),"•")
            src = lead.get("source","—")
            with st.expander(f"{statut_icon} **{lead['nom']}** — {lead.get('email','')} — {lead.get('secteur','')}"):
                c1,c2,c3 = st.columns(3)
                with c1:
                    st.markdown(f"📞 {lead.get('tel','—')}")
                    st.markdown(f"🏷️ <span class='source-badge'>{src}</span>", unsafe_allow_html=True)
                    if lead.get("utm_params"):
                        st.caption(f"UTM: {lead['utm_params']}")
                with c2:
                    st.markdown(f"📅 {lead.get('created_at','')[:10]}")
                    if lead.get("message"):
                        st.caption(lead["message"][:80])
                with c3:
                    new_st = st.selectbox("Statut", ["nouveau","contacté","qualifié","perdu"],
                                          index=["nouveau","contacté","qualifié","perdu"].index(lead.get("statut","nouveau")),
                                          key=f"lst_{lead['id']}")
                    if new_st != lead.get("statut"):
                        sb_update("crm_leads", lead["id"], {"statut": new_st})
                        st.rerun()

                # Actions
                ba,bb,bc,bd = st.columns(4)
                if lead.get("tel"):
                    with ba: st.markdown(f'<a href="tel:{lead["tel"]}" style="background:#1D9E75;color:#fff;padding:.4rem .8rem;border-radius:5px;font-size:.8rem;text-decoration:none">📞 Appel</a>', unsafe_allow_html=True)
                    with bb: st.markdown(f'<a href="sms:{lead["tel"]}" style="background:#185FA5;color:#fff;padding:.4rem .8rem;border-radius:5px;font-size:.8rem;text-decoration:none">💬 SMS</a>', unsafe_allow_html=True)
                    wa = lead["tel"].replace("+","").replace(" ","")
                    with bc: st.markdown(f'<a href="https://wa.me/{wa}" style="background:#25D366;color:#fff;padding:.4rem .8rem;border-radius:5px;font-size:.8rem;text-decoration:none">🟢 WA</a>', unsafe_allow_html=True)
                if lead.get("email"):
                    with bd: st.markdown(f'<a href="mailto:{lead["email"]}" style="background:#444;color:#fff;padding:.4rem .8rem;border-radius:5px;font-size:.8rem;text-decoration:none">✉️ Mail</a>', unsafe_allow_html=True)

                # Convertir en opportunité
                if st.button("→ Convertir en opportunité", key=f"conv_{lead['id']}"):
                    st.session_state[f"convert_{lead['id']}"] = True

                if st.session_state.get(f"convert_{lead['id']}"):
                    with st.form(f"f_conv_{lead['id']}"):
                        offre = st.selectbox("Offre", OFFRES)
                        valeur = st.number_input("Valeur €/mois", 0, 10000, 49)
                        if st.form_submit_button("Créer l'opportunité"):
                            opp_rec = {
                                "id": new_id("O"),
                                "contact_id": lead["id"],
                                "titre": f"{lead['nom']} — {offre}",
                                "adresse": lead.get("email",""),
                                "type_bien": lead.get("secteur",""),
                                "surface": 0, "prix": valeur*12,
                                "prix_m2": 0, "score": 50,
                                "source": lead.get("source","Site web"),
                                "stage": "Qualifié",
                                "valeur_deal": valeur,
                                "offre": offre,
                                "utm_params": lead.get("utm_params",""),
                                "date_creation": today_str(),
                                "date_update": today_str(),
                            }
                            sb_insert("crm_opportunites", opp_rec)
                            sb_update("crm_leads", lead["id"], {"statut": "qualifié"})
                            st.session_state.pop(f"convert_{lead['id']}", None)
                            st.success("✅ Opportunité créée")
                            st.rerun()

                # Séquences email
                with st.expander("✉️ Séquence email"):
                    cs1,cs2,cs3,cs4 = st.columns(4)
                    if lead.get("email"):
                        with cs1:
                            if st.button("J+0", key=f"j0l_{lead['id']}"):
                                from shared.emails_site import envoyer_j0
                                ok = envoyer_j0(lead["nom"], lead["email"], lead.get("secteur",""))
                                st.success("✅") if ok else st.error("KO")
                        with cs2:
                            if st.button("J+3", key=f"j3l_{lead['id']}"):
                                from shared.emails_site import envoyer_j3
                                ok = envoyer_j3(lead["nom"], lead["email"], lead.get("secteur",""))
                                st.success("✅") if ok else st.error("KO")
                        with cs3:
                            if st.button("J+7", key=f"j7l_{lead['id']}"):
                                from shared.emails_site import envoyer_j7
                                ok = envoyer_j7(lead["nom"], lead["email"], lead.get("secteur",""))
                                st.success("✅") if ok else st.error("KO")
                        with cs4:
                            if st.button("Démo", key=f"deml_{lead['id']}"):
                                st.session_state[f"demo_form_{lead['id']}"] = True

                    if st.session_state.get(f"demo_form_{lead['id']}"):
                        with st.form(f"fdem_{lead['id']}"):
                            date_d = st.text_input("Date", placeholder="Jeudi 3 avril à 14h")
                            lien_d = st.text_input("Lien visio")
                            if st.form_submit_button("Envoyer confirmation"):
                                from shared.emails_site import confirmer_demo
                                ok = confirmer_demo(lead["nom"], lead["email"],
                                                    date_d, lien_d, lead.get("secteur",""))
                                if ok:
                                    sb_update("crm_leads", lead["id"], {"statut":"contacté"})
                                    st.session_state.pop(f"demo_form_{lead['id']}", None)
                                    st.success("✅ Confirmation envoyée")
                                    st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
with tabs[1]:
    col_add, col_main = st.columns([1,3])

    with col_add:
        with st.form("new_opp"):
            st.markdown("**➕ Nouvelle opportunité**")
            nom_o   = st.text_input("Nom / Société *")
            email_o = st.text_input("Email")
            tel_o   = st.text_input("Téléphone")
            offre_o = st.selectbox("Offre", OFFRES)
            sect_o  = st.selectbox("Secteur", SECTEURS)
            src_o   = st.selectbox("Source", SOURCES)
            val_o   = st.number_input("Valeur €/mois", 0, 10000, 49)
            stage_o = st.selectbox("Étape", STAGES[:6])
            utm_o   = st.text_input("UTM params")
            notes_o = st.text_area("Notes", height=60)
            if st.form_submit_button("Créer →", type="primary"):
                if nom_o:
                    sb_insert("crm_opportunites", {
                        "id": new_id("O"),
                        "contact_id": "", "titre": f"{nom_o} — {offre_o}",
                        "adresse": email_o, "type_bien": sect_o,
                        "surface": 0, "prix": val_o*12,
                        "prix_m2": 0, "score": 50,
                        "source": src_o, "stage": stage_o,
                        "valeur_deal": val_o,
                        "offre": offre_o, "notes": notes_o,
                        "utm_params": utm_o,
                        "date_creation": today_str(), "date_update": today_str(),
                    })
                    st.success("✓ Créée")
                    st.rerun()

    with col_main:
        # Kanban par étape
        for stage in STAGES:
            stage_opps = [o for o in opps if o.get("stage") == stage]
            if not stage_opps and stage in ["Perdu"]:
                continue
            val_s = sum(float(o.get("valeur_deal",0) or 0) for o in stage_opps)
            st.markdown(
                f'<div style="background:{STAGE_COLORS[stage]};border-radius:6px;'
                f'padding:.5rem .9rem;margin:.3rem 0;display:flex;justify-content:space-between;align-items:center">'
                f'<span style="font-weight:700;font-size:.85rem;color:{STAGE_TEXT[stage]}">{stage}</span>'
                f'<span style="font-size:.78rem;color:#888">{len(stage_opps)} opp · {val_s:.0f}€/mois</span>'
                f'</div>', unsafe_allow_html=True
            )
            for opp in stage_opps:
                with st.expander(f"**{opp.get('titre','—')}** — {opp.get('type_bien','')}"):
                    c1,c2,c3 = st.columns(3)
                    with c1:
                        st.markdown(f"**Offre :** {opp.get('offre','—')}")
                        st.markdown(f"**Valeur :** {opp.get('valeur_deal',0)}€/mois")
                        if opp.get("utm_params"):
                            st.markdown(f'<span class="source-badge">🔗 {opp["utm_params"][:40]}</span>', unsafe_allow_html=True)
                    with c2:
                        st.markdown(f"**Source :** {opp.get('source','—')}")
                        st.markdown(f"**Créé :** {opp.get('date_creation','—')}")
                        if opp.get("notes"):
                            st.caption(opp["notes"][:80])
                    with c3:
                        new_stage = st.selectbox("Étape", STAGES,
                            index=STAGES.index(opp.get("stage","Nouveau")) if opp.get("stage") in STAGES else 0,
                            key=f"opp_stage_{opp['id']}")
                        if new_stage != opp.get("stage"):
                            sb_update("crm_opportunites", opp["id"],
                                      {"stage": new_stage, "date_update": today_str()})
                            st.rerun()

                    # Actions contact
                    email_opp = opp.get("adresse","")
                    if email_opp and "@" in email_opp:
                        ba,bb = st.columns(2)
                        with ba:
                            if st.button("✉️ Email libre", key=f"eml_{opp['id']}"):
                                st.session_state[f"show_email_{opp['id']}"] = True
                        with bb:
                            if st.button("🗑 Supprimer", key=f"del_{opp['id']}"):
                                sb_delete("crm_opportunites", opp["id"])
                                st.rerun()

                        if st.session_state.get(f"show_email_{opp['id']}"):
                            with st.form(f"fem_{opp['id']}"):
                                nom_dest = opp["titre"].split("—")[0].strip()
                                sujet_e = st.text_input("Objet")
                                msg_e   = st.text_area("Message", height=100)
                                if st.form_submit_button("Envoyer →"):
                                    from shared.automation import email_prospect_generique
                                    ok = email_prospect_generique(email_opp, nom_dest, sujet_e, msg_e)
                                    if ok:
                                        sb_insert("crm_activites", {
                                            "id": new_id("A"),
                                            "opp_id": opp["id"], "type": "Email",
                                            "notes": sujet_e, "statut": "Fait",
                                            "date": today_str(),
                                            "date_creation": now_str(),
                                        })
                                        st.session_state.pop(f"show_email_{opp['id']}", None)
                                        st.success("✅ Envoyé")
                                        st.rerun()

                    # Ajouter activité rapide
                    ca1,ca2,ca3 = st.columns([1,2,1])
                    with ca1:
                        act_type = st.selectbox("Activité",
                            ["Appel","Email","SMS","WhatsApp","Démo","RDV","Note"],
                            key=f"atype_{opp['id']}")
                    with ca2:
                        act_note = st.text_input("Note", key=f"anote_{opp['id']}")
                    with ca3:
                        if st.button("✓", key=f"abtn_{opp['id']}", type="primary"):
                            sb_insert("crm_activites", {
                                "id": new_id("A"),
                                "opp_id": opp["id"], "type": act_type,
                                "notes": act_note, "statut": "Fait",
                                "date": today_str(), "date_creation": now_str(),
                            })
                            st.success("Activité ajoutée")
                            st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — CONTACTS
# ══════════════════════════════════════════════════════════════════════════════
with tabs[2]:
    col_fc, col_lc = st.columns([1,2])

    with col_fc:
        with st.form("new_contact"):
            st.markdown("**➕ Nouveau contact**")
            nom_c   = st.text_input("Nom *")
            email_c = st.text_input("Email")
            tel_c   = st.text_input("Téléphone")
            soc_c   = st.text_input("Société")
            type_c  = st.selectbox("Type", ["Prospect","Client","Partenaire","Prescripteur","Autre"])
            sect_c  = st.selectbox("Secteur", SECTEURS)
            src_c   = st.selectbox("Source", SOURCES)
            notes_c = st.text_area("Notes", height=60)
            if st.form_submit_button("Créer →", type="primary"):
                if nom_c:
                    sb_insert("crm_contacts", {
                        "id": new_id("C"),
                        "nom": nom_c, "email": email_c, "tel": tel_c,
                        "societe": soc_c, "type": type_c,
                        "secteur": sect_c, "source": src_c,
                        "notes": notes_c,
                        "date_creation": today_str(),
                    })
                    st.success("✓ Créé")
                    st.rerun()

    with col_lc:
        search_c = st.text_input("🔍 Rechercher", placeholder="Nom, email, société...")
        contacts_f = contacts
        if search_c:
            s = search_c.lower()
            contacts_f = [c for c in contacts if s in c.get("nom","").lower()
                          or s in c.get("email","").lower()
                          or s in c.get("societe","").lower()]

        st.caption(f"{len(contacts_f)} contacts")

        for c in contacts_f:
            with st.expander(f"**{c['nom']}** {('— ' + c.get('societe','')) if c.get('societe') else ''} — {c.get('type','')}"):
                col1,col2 = st.columns(2)
                with col1:
                    st.markdown(f"✉️ {c.get('email','—')}")
                    st.markdown(f"📞 {c.get('tel','—')}")
                    st.markdown(f"🏢 {c.get('societe','—')}")
                with col2:
                    st.markdown(f"🏷️ {c.get('secteur','—')}")
                    st.markdown(f"🔗 {c.get('source','—')}")
                    if c.get("notes"): st.caption(c["notes"])

                # Actions
                if c.get("tel"):
                    email_btn = ""
                    if c.get("email"):
                        email_btn = f'<a href="mailto:{c["email"]}" style="background:#444;color:#fff;padding:.35rem .75rem;border-radius:5px;font-size:.78rem;text-decoration:none">✉️</a>'
                    wa_num = c["tel"].replace("+","").replace(" ","")
                    st.markdown(
                        f'<div style="display:flex;gap:.5rem;margin-top:.5rem">'
                        f'<a href="tel:{c["tel"]}" style="background:#1D9E75;color:#fff;padding:.35rem .75rem;border-radius:5px;font-size:.78rem;text-decoration:none">📞</a>'
                        f'<a href="sms:{c["tel"]}" style="background:#185FA5;color:#fff;padding:.35rem .75rem;border-radius:5px;font-size:.78rem;text-decoration:none">💬</a>'
                        f'<a href="https://wa.me/{wa_num}" style="background:#25D366;color:#fff;padding:.35rem .75rem;border-radius:5px;font-size:.78rem;text-decoration:none">🟢</a>'
                        f'{email_btn}'
                        f'</div>', unsafe_allow_html=True
                    )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — ACTIVITÉS
# ══════════════════════════════════════════════════════════════════════════════
with tabs[3]:
    # Activités à faire aujourd'hui
    today = today_str()
    acts_today = [a for a in activites if a.get("statut") == "À faire"]

    if acts_today:
        st.markdown(f"**⏳ À faire ({len(acts_today)})**")
        for a in acts_today:
            opp_ref = next((o.get("titre","")[:35] for o in opps if o["id"] == a.get("opp_id")), "—")
            col1,col2,col3 = st.columns([2,2,1])
            with col1: st.markdown(f"**{a['type']}** — {opp_ref}")
            with col2: st.caption(a.get("notes",""))
            with col3:
                if st.button("✓ Fait", key=f"done_{a['id']}"):
                    sb_update("crm_activites", a["id"], {"statut":"Fait"})
                    st.rerun()

        st.markdown("---")

    # Toutes les activités
    st.markdown("**Historique**")
    acts_sorted = sorted(activites, key=lambda x: x.get("date_creation",""), reverse=True)
    for a in acts_sorted[:50]:
        opp_ref = next((o.get("titre","")[:30] for o in opps if o["id"] == a.get("opp_id")), "—")
        icon = {"Appel":"📞","Email":"✉️","SMS":"💬","WhatsApp":"🟢","Démo":"🎯","RDV":"📅","Note":"📝"}.get(a.get("type",""),"•")
        statut_icon = "✅" if a.get("statut") == "Fait" else "⏳"
        st.markdown(
            f'<div style="display:flex;justify-content:space-between;padding:.45rem 0;'
            f'border-bottom:1px solid #f5f5f5;font-size:.85rem">'
            f'<span>{icon} <b>{a.get("type","")}</b> · {opp_ref}</span>'
            f'<span style="color:#888">{statut_icon} {a.get("date","")}</span>'
            f'</div>',
            unsafe_allow_html=True
        )
        if a.get("notes"):
            st.caption(f"  {a['notes'][:80]}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
with tabs[4]:
    try:
        import plotly.express as px
        import plotly.graph_objects as go

        col_d1, col_d2 = st.columns(2)

        with col_d1:
            # Funnel pipeline
            st.markdown("**Funnel pipeline**")
            stage_counts = {s: len([o for o in opps if o.get("stage")==s]) for s in STAGES}
            fig_f = go.Figure(go.Funnel(
                y=STAGES,
                x=[stage_counts[s] for s in STAGES],
                textposition="inside",
                marker=dict(color=["#e5e5e5","#e3f2fd","#e8f5e9","#fff8e1","#fff3e0","#fce4ec","#c8e6c9","#f5f5f5"]),
            ))
            fig_f.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0),
                                 plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_f, use_container_width=True)

        with col_d2:
            # Sources des leads
            st.markdown("**Sources des leads**")
            if leads:
                src_counts = pd.DataFrame(leads)["source"].value_counts().reset_index()
                src_counts.columns = ["Source","Nb"]
                fig_s = px.bar(src_counts, x="Nb", y="Source", orientation="h",
                               color="Nb", color_continuous_scale="Blues")
                fig_s.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0),
                                     plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                                     coloraxis_showscale=False)
                st.plotly_chart(fig_s, use_container_width=True)

        col_d3, col_d4 = st.columns(2)

        with col_d3:
            # Valeur par offre
            st.markdown("**Pipeline par offre**")
            if opps:
                df_opps = pd.DataFrame(opps)
                if "offre" in df_opps.columns:
                    offre_val = df_opps.groupby("offre")["valeur_deal"].sum().reset_index()
                    offre_val.columns = ["Offre","Valeur €/mois"]
                    fig_o = px.pie(offre_val, names="Offre", values="Valeur €/mois",
                                   color_discrete_sequence=px.colors.sequential.Blues_r)
                    fig_o.update_layout(height=260, margin=dict(l=0,r=0,t=10,b=0))
                    st.plotly_chart(fig_o, use_container_width=True)

        with col_d4:
            # Activités par type
            st.markdown("**Activités par type**")
            if activites:
                df_acts = pd.DataFrame(activites)
                act_counts = df_acts["type"].value_counts().reset_index()
                act_counts.columns = ["Type","Nb"]
                fig_a = px.bar(act_counts, x="Type", y="Nb",
                               color="Nb", color_continuous_scale="Greens")
                fig_a.update_layout(height=260, margin=dict(l=0,r=0,t=10,b=0),
                                     plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                                     coloraxis_showscale=False)
                st.plotly_chart(fig_a, use_container_width=True)

        # Tableau récap
        st.markdown("---")
        st.markdown("**Top opportunités actives**")
        if opps:
            df_top = pd.DataFrame([o for o in opps if o.get("stage") not in ["Client","Perdu"]])
            if not df_top.empty and "titre" in df_top.columns:
                cols_show = [c for c in ["titre","stage","offre","valeur_deal","source","utm_params","date_creation"] if c in df_top.columns]
                st.dataframe(df_top[cols_show].rename(columns={
                    "titre":"Titre","stage":"Étape","offre":"Offre",
                    "valeur_deal":"€/mois","source":"Source",
                    "utm_params":"UTM","date_creation":"Créé le"
                }), use_container_width=True, hide_index=True)

    except ImportError:
        st.info("Installer plotly pour les graphiques")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — SÉQUENCES
# ══════════════════════════════════════════════════════════════════════════════
with tabs[5]:
    st.markdown("**Envoi manuel de séquence à plusieurs leads**")

    leads_qualif = [l for l in leads if l.get("statut") in ["nouveau","contacté"]]
    if not leads_qualif:
        st.info("Aucun lead actif.")
    else:
        seq_type = st.selectbox("Séquence à envoyer", ["J+0 Bienvenue","J+3 Relance","J+7 Contenu valeur"])
        leads_sel = st.multiselect(
            "Leads destinataires",
            [f"{l['nom']} — {l.get('email','')}" for l in leads_qualif],
            help="Sélectionner les leads à inclure"
        )
        if st.button(f"Envoyer {seq_type} à {len(leads_sel)} lead(s)", type="primary"):
            sent = 0
            for sel in leads_sel:
                lead = next((l for l in leads_qualif if f"{l['nom']} — {l.get('email','')}" == sel), None)
                if lead and lead.get("email"):
                    try:
                        from shared.emails_site import envoyer_j0, envoyer_j3, envoyer_j7
                        fn = {"J+0 Bienvenue": envoyer_j0,
                              "J+3 Relance": envoyer_j3,
                              "J+7 Contenu valeur": envoyer_j7}[seq_type]
                        ok = fn(lead["nom"], lead["email"], lead.get("secteur",""))
                        if ok: sent += 1
                    except Exception as e:
                        st.warning(f"{lead['nom']}: {e}")
            st.success(f"✅ {sent} email(s) envoyé(s)")

    st.markdown("---")
    st.markdown("**UTM tracking dans les emails**")
    st.info("""
Les liens dans les emails incluent automatiquement les paramètres UTM :
- `utm_source=email`
- `utm_medium=sequence`
- `utm_campaign=j0|j3|j7|demo`
- `utm_content={secteur}`

Ces paramètres sont capturés par GTM → DataLayer → GA4 pour retracer la source exacte de chaque visite et conversion.
    """)
