"""
SAHAR CRM Interne
-----------------
Outil privé du fondateur. Gère les prospects, clients,
opportunités, deals et MRR de SAHAR Conseil.
Distinct de l'app DVF/CRM vendue aux clients.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st
import pandas as pd
from datetime import datetime, date

st.set_page_config(
    page_title="SAHAR CRM",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  *{box-sizing:border-box}
  .main .block-container{padding:1.25rem 2rem}
  .stMetric label{font-size:.75rem;color:#888;font-weight:500}
  .stMetric [data-testid="stMetricValue"]{font-size:1.4rem;font-weight:700;color:#1a1a1a}
  div[data-testid="stSidebarContent"]{padding-top:.75rem}
  .stTabs [data-baseweb="tab"]{font-size:.85rem;font-weight:500}
  [data-testid="stExpander"]{border:1px solid #e8e8e8;border-radius:8px;margin-bottom:.4rem}
  h1{font-size:1.5rem;font-weight:700;letter-spacing:-.02em}
  .action-btn a{display:inline-flex;align-items:center;gap:.3rem;padding:.4rem .85rem;
    border-radius:6px;font-size:.8rem;font-weight:600;text-decoration:none;margin:.2rem .3rem 0 0}
</style>
""", unsafe_allow_html=True)

# ── AUTH ──────────────────────────────────────────────────────────────────────
def check_auth():
    if st.session_state.get("sahar_crm_auth"):
        return
    try:
        pwd = st.secrets.get("APP_PWD", "")
    except Exception:
        return
    if not pwd:
        return
    with st.sidebar:
        st.markdown("### 🎯 SAHAR CRM")
        p = st.text_input("Mot de passe", type="password",
                           label_visibility="collapsed", placeholder="Mot de passe…")
        if p == pwd:
            st.session_state.sahar_crm_auth = True
            st.rerun()
        elif p:
            st.error("Incorrect")
            st.stop()
        else:
            st.caption("Accès réservé")
            st.stop()

check_auth()

try:
    from shared import sahar_crm as crm
    crm.load_all()
except Exception as e:
    st.error(f"Erreur chargement CRM : {e}")
    st.stop()

# ── CONSTANTES ────────────────────────────────────────────────────────────────
STAGES = ["Qualification", "Démo", "Proposition", "Négociation", "Closing", "Perdu"]
STAGE_EMOJI = {"Qualification":"⬜","Démo":"🟡","Proposition":"🔵",
               "Négociation":"🟠","Closing":"✅","Perdu":"❌"}
OFFRES = ["Starter 49€/mois","Pro 99€/mois","Expert 149€/mois","Sur mesure"]
SOURCES = ["Site inbound","LinkedIn","Référral","Cold outreach","Événement","Partenaire","Autre"]
TYPES_ACT = ["Appel","Email","Démo","Meeting","Note","SMS","WhatsApp","Relance"]

def action_btns(tel="", email="", nom=""):
    t = str(tel or "").replace(" ","").replace("-","").replace(".","")
    if t.startswith("0"): t = "+33" + t[1:]
    btns = []
    if t:
        btns += [
            f'<a href="tel:{t}" style="background:#1D9E75;color:#fff">📞 Appeler</a>',
            f'<a href="sms:{t}" style="background:#185FA5;color:#fff">💬 SMS</a>',
            f'<a href="https://wa.me/{t.replace("+","")}" target="_blank" style="background:#25D366;color:#fff">🟢 WA</a>',
        ]
    if email:
        btns.append(f'<a href="mailto:{email}" style="background:#444;color:#fff">✉️ Email</a>')
    if btns:
        st.markdown('<div class="action-btn">' + "".join(btns) + '</div>', unsafe_allow_html=True)

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🎯 SAHAR CRM")
    st.caption("Outil interne — confidentiel")
    st.markdown("---")
    page = st.radio("", ["🏠 Dashboard","🏢 Comptes","🎯 Pipeline",
                          "📅 Activités","💰 Deals & MRR","🧾 Factures","📊 Reporting"],
                    label_visibility="collapsed")
    st.markdown("---")
    kpis = crm.get_kpis()
    st.metric("MRR", f"{kpis['mrr']:,.0f}€")
    st.metric("Pipeline", f"{kpis['pipeline_pond']:,.0f}€")
    st.metric("Clients", kpis["clients"])
    st.markdown("---")
    if st.button("🔄 Rafraîchir", use_container_width=True):
        for k in ["sahar_comptes","sahar_contacts","sahar_opportunites",
                  "sahar_activites","sahar_deals","sahar_factures","sahar_crm_loaded"]:
            st.session_state.pop(k, None)
        st.rerun()

# ── DASHBOARD ─────────────────────────────────────────────────────────────────
if page == "🏠 Dashboard":
    st.title("🏠 Dashboard")
    k = crm.get_kpis()
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    with c1: st.metric("Prospects", k["prospects"])
    with c2: st.metric("Clients", k["clients"])
    with c3: st.metric("Opps actives", k["opps_actives"])
    with c4: st.metric("Pipeline pondéré", f"{k['pipeline_pond']:,.0f}€")
    with c5: st.metric("MRR", f"{k['mrr']:,.0f}€", delta=f"ARR {k['mrr']*12:,.0f}€")
    with c6: st.metric("CA encaissé", f"{k['ca_total']:,.0f}€")
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Funnel pipeline**")
        opps = crm.get_opportunites()
        total = len(opps) or 1
        for stage in STAGES:
            s_opps = [o for o in opps if o.get("stage") == stage]
            val = sum(o.get("valeur",0) for o in s_opps)
            pct = len(s_opps)/total*100
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:.6rem;margin:.3rem 0;font-size:.83rem">' +
                f'<span style="min-width:110px">{STAGE_EMOJI[stage]} {stage}</span>' +
                f'<div style="flex:1;height:6px;background:#f0f0f0;border-radius:4px">' +
                f'<div style="width:{pct:.0f}%;height:100%;background:#185FA5;border-radius:4px"></div></div>' +
                f'<span style="color:#888;font-size:.77rem;min-width:90px;text-align:right">{len(s_opps)} · {val:,.0f}€</span>' +
                f'</div>', unsafe_allow_html=True)
    with col2:
        st.markdown("**Dernières activités**")
        acts = st.session_state.get("sahar_activites",[])[:10]
        cdict = {c["id"]:c["nom"] for c in st.session_state.get("sahar_comptes",[])}
        for a in acts:
            icon = {"Appel":"📞","Email":"✉️","Démo":"🖥️","Meeting":"🤝","Note":"📝","SMS":"💬","WhatsApp":"🟢","Relance":"🔁"}.get(a.get("type",""),"•")
            st.markdown(
                f'<div style="display:flex;gap:.5rem;padding:.35rem 0;border-bottom:1px solid #f5f5f5;font-size:.82rem">' +
                f'<span>{icon}</span>' +
                f'<span style="color:#aaa;min-width:75px">{a.get("date_activite","")}</span>' +
                f'<span style="font-weight:600;min-width:110px">{cdict.get(a.get("compte_id",""),"—")[:14]}</span>' +
                f'<span style="color:#666">{str(a.get("notes",""))[:45]}</span>' +
                f'</div>', unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("**Top opportunités actives**")
    opps_act = [o for o in crm.get_opportunites() if o.get("stage") not in ["Closing","Perdu"]]
    if opps_act:
        df = pd.DataFrame(opps_act)
        cdict2 = {c["id"]:c["nom"] for c in st.session_state.get("sahar_comptes",[])}
        df["compte"] = df["compte_id"].map(cdict2).fillna("—")
        df["pondéré"] = df["valeur"]*df["probabilite"]/100
        st.dataframe(df[["compte","titre","offre","valeur","probabilite","pondéré","stage","utm_source"]].head(10).rename(columns={
            "compte":"Compte","titre":"Titre","offre":"Offre","valeur":"€/mois",
            "probabilite":"Prob%","pondéré":"Pondéré","stage":"Étape","utm_source":"UTM src"
        }), use_container_width=True, hide_index=True,
        column_config={
            "€/mois": st.column_config.NumberColumn(format="%d €"),
            "Pondéré": st.column_config.NumberColumn(format="%d €"),
            "Prob%": st.column_config.ProgressColumn(min_value=0,max_value=100,format="%d%%"),
        })

# ── COMPTES ───────────────────────────────────────────────────────────────────
elif page == "🏢 Comptes":
    st.title("🏢 Comptes")
    tab_list, tab_new = st.tabs(["Liste","Nouveau compte"])
    with tab_new:
        with st.form("f_cpte"):
            c1,c2 = st.columns(2)
            with c1:
                nom=st.text_input("Nom *"); email=st.text_input("Email")
                tel=st.text_input("Téléphone"); ville=st.text_input("Ville")
            with c2:
                type_=st.selectbox("Type",["prospect","client","partenaire","perdu"])
                secteur=st.selectbox("Secteur",["immobilier","energie","retail","rh","auto","autre"])
                source=st.selectbox("Source",SOURCES)
            st.markdown("**UTM Attribution**")
            u1,u2,u3=st.columns(3)
            with u1: utm_s=st.text_input("utm_source",placeholder="linkedin")
            with u2: utm_m=st.text_input("utm_medium",placeholder="social")
            with u3: utm_c=st.text_input("utm_campaign",placeholder="cold_immo")
            landing=st.text_input("Landing page",placeholder="/immobilier.html")
            notes=st.text_area("Notes",height=70)
            if st.form_submit_button("Créer",type="primary") and nom:
                crm.create_compte(nom=nom,type_=type_,secteur=secteur,email=email,
                                   tel=tel,ville=ville,source=source,utm_source=utm_s,
                                   utm_medium=utm_m,utm_campaign=utm_c,landing_page=landing,notes=notes)
                st.success(f"✓ {nom} créé"); st.rerun()
    with tab_list:
        cf,cs=st.columns([1,2])
        with cf: tf=st.selectbox("Type",["Tous","prospect","client","partenaire","perdu"],label_visibility="collapsed")
        with cs: search=st.text_input("Rechercher",placeholder="Nom, email…",label_visibility="collapsed")
        comptes=crm.get_comptes(tf if tf!="Tous" else None)
        if search: comptes=[c for c in comptes if search.lower() in str(c).lower()]
        st.caption(f"{len(comptes)} comptes")
        for c in comptes:
            ti={"prospect":"🎯","client":"✅","partenaire":"🤝","perdu":"❌"}.get(c.get("type",""),"•")
            nb_o=len([o for o in st.session_state.get("sahar_opportunites",[]) if o.get("compte_id")==c["id"]])
            nb_a=len([a for a in st.session_state.get("sahar_activites",[]) if a.get("compte_id")==c["id"]])
            with st.expander(f"{ti} **{c['nom']}** — {c.get('secteur','')} {c.get('ville','')}"):
                co1,co2=st.columns(2)
                with co1:
                    if c.get("email"): st.markdown(f"✉️ {c['email']}")
                    if c.get("tel"): st.markdown(f"📞 {c['tel']}")
                    action_btns(c.get("tel",""),c.get("email",""))
                    if c.get("utm_source"): st.markdown(f"🔗 `{c['utm_source']}` / `{c.get('utm_campaign','—')}`")
                    if c.get("landing_page"): st.markdown(f"📄 `{c['landing_page']}`")
                with co2:
                    st.metric("Opps",nb_o); st.metric("Activités",nb_a)
                    nt=st.selectbox("Type",["prospect","client","partenaire","perdu"],
                                     index=["prospect","client","partenaire","perdu"].index(c.get("type","prospect")),key=f"ct_{c['id']}")
                    if nt!=c.get("type"): crm.update_compte(c["id"],type_=nt); st.rerun()
                if c.get("notes"): st.caption(c["notes"])
                with st.form(f"opp_{c['id']}"):
                    st.caption("Nouvelle opportunité")
                    ot,oo,ov=st.columns(3)
                    with ot: tit=st.text_input("Titre",key=f"ot_{c['id']}")
                    with oo: off=st.selectbox("Offre",OFFRES,key=f"of_{c['id']}")
                    with ov: val=st.number_input("€/mois",0,9999,49,key=f"ov_{c['id']}")
                    if st.form_submit_button("Créer →") and tit:
                        crm.create_opportunite(c["id"],tit,off,val,source=c.get("source",""),utm_source=c.get("utm_source",""),utm_campaign=c.get("utm_campaign",""))
                        st.success("✓"); st.rerun()
                with st.form(f"act_{c['id']}"):
                    ta,na=st.columns(2)
                    with ta: type_a=st.selectbox("Type",TYPES_ACT,key=f"ta_{c['id']}")
                    with na: note_a=st.text_input("Note",key=f"na_{c['id']}")
                    if st.form_submit_button("Logger →"):
                        crm.log_activite(c["id"],type_a,note_a); st.success("✓"); st.rerun()

# ── PIPELINE ──────────────────────────────────────────────────────────────────
elif page == "🎯 Pipeline":
    st.title("🎯 Pipeline")
    opps=crm.get_opportunites(); cdict={c["id"]:c for c in st.session_state.get("sahar_comptes",[])}
    if not opps: st.info("Aucune opportunité. Créez-en depuis un compte.")
    else:
        actives=[o for o in opps if o.get("stage") not in ["Closing","Perdu"]]
        c1,c2,c3,c4=st.columns(4)
        with c1: st.metric("Opps actives",len(actives))
        with c2: st.metric("Pipeline brut",f"{sum(o.get('valeur',0) for o in actives):,.0f}€")
        with c3: st.metric("Pipeline pondéré",f"{sum(o.get('valeur',0)*o.get('probabilite',10)/100 for o in actives):,.0f}€")
        with c4: st.metric("Closés",len([o for o in opps if o.get("stage")=="Closing"]))
        st.markdown("---")
        cols=st.columns(len(STAGES))
        for i,stage in enumerate(STAGES):
            s_opps=[o for o in opps if o.get("stage")==stage]
            val=sum(o.get("valeur",0) for o in s_opps)
            with cols[i]:
                st.markdown(f'<div style="background:#f8f8f8;border:1px solid #e5e5e5;border-radius:8px;padding:.6rem .8rem;margin-bottom:.6rem;font-size:.82rem"><b>{STAGE_EMOJI[stage]} {stage}</b><br><span style="color:#888;font-size:.75rem">{len(s_opps)} · {val:,.0f}€</span></div>',unsafe_allow_html=True)
                for o in s_opps:
                    cpt=cdict.get(o.get("compte_id",""),{})
                    with st.expander(f"{cpt.get('nom','—')[:16]} · {o.get('offre','')[:12]}"):
                        st.markdown(f"**{o['titre']}**")
                        st.markdown(f"💶 {o.get('valeur',0):,.0f}€/mois · {o.get('probabilite',0)}%")
                        if o.get("utm_source"): st.markdown(f"🔗 `{o['utm_source']}` / `{o.get('utm_campaign','—')}`")
                        action_btns(cpt.get("tel",""),cpt.get("email",""))
                        ns=st.selectbox("Étape",STAGES,index=STAGES.index(o.get("stage","Qualification")),key=f"stg_{o['id']}")
                        if ns!=o.get("stage"): crm.update_stage(o["id"],ns); st.rerun()
                        with st.form(f"pa_{o['id']}"):
                            t1,t2=st.columns(2)
                            with t1: ta=st.selectbox("",TYPES_ACT,key=f"pta_{o['id']}",label_visibility="collapsed")
                            with t2: na=st.text_input("",key=f"pna_{o['id']}",placeholder="Note…",label_visibility="collapsed")
                            if st.form_submit_button("Logger"): crm.log_activite(o.get("compte_id",""),ta,na,opp_id=o["id"]); st.rerun()
                        if o.get("stage")=="Négociation":
                            if st.button("🎉 Closer",key=f"cls_{o['id']}",type="primary",use_container_width=True):
                                crm.create_deal(o["compte_id"],o.get("offre",""),o.get("valeur",0),opp_id=o["id"]); st.success("🎉 Deal signé!"); st.rerun()

# ── ACTIVITÉS ─────────────────────────────────────────────────────────────────
elif page == "📅 Activités":
    st.title("📅 Activités")
    clist=st.session_state.get("sahar_comptes",[])
    cdict={c["id"]:c["nom"] for c in clist}
    with st.expander("➕ Nouvelle activité"):
        with st.form("f_act"):
            opts=[f"{c['id']} — {c['nom']}" for c in clist]
            if opts:
                cs=st.selectbox("Compte",opts)
                c1,c2,c3=st.columns(3)
                with c1: ta=st.selectbox("Type",TYPES_ACT)
                with c2: st_a=st.selectbox("Statut",["fait","planifié","annulé"])
                with c3: da=st.date_input("Date",value=date.today()); dur=st.number_input("Durée min",0,480,0)
                na=st.text_area("Notes",height=70)
                if st.form_submit_button("Créer",type="primary"):
                    crm.log_activite(cs.split(" — ")[0],ta,na,statut=st_a,duree_min=dur,date_activite=da.strftime("%d/%m/%Y")); st.success("✓"); st.rerun()
    acts=st.session_state.get("sahar_activites",[])
    tf=st.selectbox("Filtrer",["Tous"]+TYPES_ACT,label_visibility="collapsed")
    if tf!="Tous": acts=[a for a in acts if a.get("type")==tf]
    for a in acts[:100]:
        icon={"Appel":"📞","Email":"✉️","Démo":"🖥️","Meeting":"🤝","Note":"📝","SMS":"💬","WhatsApp":"🟢","Relance":"🔁"}.get(a.get("type",""),"•")
        si={"fait":"✅","planifié":"⏳","annulé":"❌"}.get(a.get("statut",""),"")
        cpt=cdict.get(a.get("compte_id",""),"—")
        st.markdown(f'<div style="display:flex;gap:.6rem;padding:.45rem 0;border-bottom:1px solid #f5f5f5;font-size:.82rem"><span>{icon}</span><span style="color:#aaa;min-width:80px">{a.get("date_activite","—")}</span><span style="font-weight:600;min-width:120px">{cpt[:16]}</span><span style="color:#555">{a.get("type","")} {si}</span><span style="color:#777;flex:1">{str(a.get("notes",""))[:55]}</span></div>',unsafe_allow_html=True)

# ── DEALS & MRR ───────────────────────────────────────────────────────────────
elif page == "💰 Deals & MRR":
    st.title("💰 Deals & MRR")
    mrr=crm.get_mrr(); ca=crm.get_ca_total()
    c1,c2,c3,c4,c5=st.columns(5)
    with c1: st.metric("MRR",f"{mrr['mrr']:,.0f}€")
    with c2: st.metric("ARR",f"{mrr['mrr']*12:,.0f}€")
    with c3: st.metric("Clients actifs",mrr["clients"])
    with c4: st.metric("CA encaissé",f"{ca['ca_total']:,.0f}€")
    with c5: st.metric("En attente",f"{ca['en_attente']:,.0f}€")
    st.markdown("---")
    deals=st.session_state.get("sahar_deals",[])
    cdict={c["id"]:c["nom"] for c in st.session_state.get("sahar_comptes",[])}
    if deals:
        for d in deals:
            cpt=cdict.get(d.get("compte_id",""),"—")
            with st.expander(f"✅ **{cpt}** — {d.get('offre','')} — {d.get('montant',0):,.0f}€/{d.get('recurrence','mois')}"):
                c1d,c2d=st.columns(2)
                with c1d:
                    st.markdown(f"**Offre :** {d.get('offre','—')}"); st.markdown(f"**Montant :** {d.get('montant',0):,.0f}€/{d.get('recurrence','mois')}"); st.markdown(f"**Début :** {d.get('date_debut','—')}")
                with c2d:
                    ns=st.selectbox("Statut",["actif","pause","résilié"],index=["actif","pause","résilié"].index(d.get("statut","actif")),key=f"dst_{d['id']}")
                    if ns!=d.get("statut"): crm._update("sahar_deals",d["id"],{"statut":ns}); d["statut"]=ns; st.rerun()
                if st.button(f"🧾 Émettre facture",key=f"fac_{d['id']}"):
                    crm.create_facture(d["compte_id"],d.get("montant",0),deal_id=d["id"]); st.success("Facture créée!"); st.rerun()
    else:
        st.info("Aucun deal signé. Closez une opportunité depuis le Pipeline.")

# ── FACTURES ──────────────────────────────────────────────────────────────────
elif page == "🧾 Factures":
    st.title("🧾 Factures")
    ca=crm.get_ca_total()
    c1,c2,c3=st.columns(3)
    with c1: st.metric("CA encaissé",f"{ca['ca_total']:,.0f}€")
    with c2: st.metric("En attente",f"{ca['en_attente']:,.0f}€")
    with c3: st.metric("Nb factures",ca["nb_factures"])
    st.markdown("---")
    clients=[c for c in st.session_state.get("sahar_comptes",[]) if c.get("type")=="client"]
    with st.expander("➕ Nouvelle facture"):
        with st.form("f_fac"):
            if clients:
                opts=[f"{c['id']} — {c['nom']}" for c in clients]
                cs=st.selectbox("Client",opts)
                c1f,c2f=st.columns(2)
                with c1f: mnt=st.number_input("Montant HT €",0.0,99999.0,49.0,step=1.0)
                with c2f: ech=st.date_input("Échéance",value=date.today())
                nf=st.text_input("Référence")
                if st.form_submit_button("Créer",type="primary"):
                    f=crm.create_facture(cs.split(" — ")[0],mnt,date_echeance=ech.strftime("%d/%m/%Y"),notes=nf)
                    st.success(f"✓ {f['numero']} — {mnt*1.2:.2f}€ TTC"); st.rerun()
            else: st.info("Aucun client actif.")
    factures=st.session_state.get("sahar_factures",[])
    cdict={c["id"]:c["nom"] for c in st.session_state.get("sahar_comptes",[])}
    for f in factures:
        cpt=cdict.get(f.get("compte_id",""),"—")
        si={"payée":"🟢","en_attente":"🟡","en_retard":"🔴","annulée":"⚫"}.get(f.get("statut",""),"⚪")
        with st.expander(f"{si} **{f.get('numero','—')}** — {cpt} — {f.get('montant_ht',0)*1.2:.0f}€ TTC"):
            c1f,c2f=st.columns(2)
            with c1f:
                st.markdown(f"HT : {f.get('montant_ht',0):.2f}€"); st.markdown(f"TTC : {f.get('montant_ht',0)*1.2:.2f}€")
                st.markdown(f"Émission : {f.get('date_emission','—')}"); st.markdown(f"Échéance : {f.get('date_echeance','—')}")
            with c2f:
                ns=st.selectbox("Statut",["en_attente","payée","en_retard","annulée"],index=["en_attente","payée","en_retard","annulée"].index(f.get("statut","en_attente")),key=f"fst_{f['id']}")
                if ns!=f.get("statut"): crm._update("sahar_factures",f["id"],{"statut":ns}); f["statut"]=ns; st.rerun()

# ── REPORTING ─────────────────────────────────────────────────────────────────
elif page == "📊 Reporting":
    st.title("📊 Reporting")
    try:
        import plotly.express as px
        import plotly.graph_objects as go
        opps=crm.get_opportunites(); comptes=crm.get_comptes()
        deals=st.session_state.get("sahar_deals",[]); acts=st.session_state.get("sahar_activites",[])
        c1,c2=st.columns(2)
        with c1:
            st.markdown("**Funnel pipeline**")
            if opps:
                data={s:len([o for o in opps if o.get("stage")==s]) for s in STAGES}
                fig=go.Figure(go.Funnel(y=list(data.keys()),x=list(data.values()),texttemplate="%{value}",marker=dict(color=["#f5f5f5","#fef3c7","#dbeafe","#d1fae5","#bbf7d0","#fee2e2"])))
                fig.update_layout(height=260,margin=dict(l=0,r=0,t=5,b=0),paper_bgcolor="rgba(0,0,0,0)",font=dict(size=12)); st.plotly_chart(fig,use_container_width=True)
        with c2:
            st.markdown("**Sources d'acquisition**")
            if comptes:
                sources={}
                for c in comptes:
                    s=c.get("utm_source") or c.get("source") or "direct"; sources[s]=sources.get(s,0)+1
                fig2=px.pie(values=list(sources.values()),names=list(sources.keys()),color_discrete_sequence=px.colors.qualitative.Pastel,hole=0.4)
                fig2.update_layout(height=250,margin=dict(l=0,r=0,t=5,b=0),paper_bgcolor="rgba(0,0,0,0)"); st.plotly_chart(fig2,use_container_width=True)
        c3,c4=st.columns(2)
        with c3:
            st.markdown("**MRR par offre**")
            actifs=[d for d in deals if d.get("statut")=="actif"]
            if actifs:
                offres={}
                for d in actifs: offres[d.get("offre","?")]=offres.get(d.get("offre","?"),0)+d.get("montant",0)
                fig3=px.bar(x=list(offres.keys()),y=list(offres.values()),color_discrete_sequence=["#185FA5"])
                fig3.update_layout(height=210,margin=dict(l=0,r=0,t=5,b=0),paper_bgcolor="rgba(0,0,0,0)",showlegend=False); st.plotly_chart(fig3,use_container_width=True)
        with c4:
            st.markdown("**Activités par type**")
            if acts:
                types={}
                for a in acts: types[a.get("type","?")]=types.get(a.get("type","?"),0)+1
                fig4=px.bar(x=list(types.values()),y=list(types.keys()),orientation="h",color_discrete_sequence=["#1D9E75"])
                fig4.update_layout(height=210,margin=dict(l=0,r=0,t=5,b=0),paper_bgcolor="rgba(0,0,0,0)"); st.plotly_chart(fig4,use_container_width=True)
        st.markdown("---")
        k1,k2,k3,k4=st.columns(4)
        closing=len([o for o in opps if o.get("stage")=="Closing"]); tot=len(opps) or 1
        with k1: st.metric("Win rate",f"{closing/tot*100:.0f}%")
        with k2: st.metric("Pipeline pondéré",f"{sum(o.get('valeur',0)*o.get('probabilite',10)/100 for o in opps):,.0f}€")
        with k3: st.metric("ARR projeté",f"{crm.get_mrr()['mrr']*12:,.0f}€")
        with k4:
            secteurs={}
            for c in comptes: secteurs[c.get("secteur","?")]=secteurs.get(c.get("secteur","?"),0)+1
            st.metric("Top secteur",max(secteurs,key=secteurs.get) if secteurs else "—")
    except ImportError: st.info("Installer plotly : pip install plotly")

st.markdown("---")
st.caption(f"SAHAR CRM Interne · {datetime.now().strftime('%d/%m/%Y %H:%M')} · Confidentiel")
