"""
SAHAR Conseil — sahar_crm.py
CRM interne pour gérer les prospects, clients et deals SAHAR.
Tables : sahar_comptes, sahar_contacts, sahar_opportunites,
         sahar_activites, sahar_deals, sahar_factures
"""

import streamlit as st
from datetime import datetime
from typing import Optional


STAGES = ["Qualification", "Démo", "Proposition", "Négociation", "Closing", "Perdu"]
OFFRES = ["Starter 49€", "Pro 99€", "Expert 149€", "Sur mesure"]
SOURCES = ["Site inbound", "LinkedIn", "Référral", "Cold outreach", "Événement", "Partenaire", "Autre"]
TYPES_ACTIVITE = ["Appel", "Email", "Démo", "Meeting", "Note", "SMS", "WhatsApp", "Relance"]


def _sb():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets.get("SUPABASE_SERVICE_KEY") or st.secrets["SUPABASE_KEY"]
        from supabase import create_client
        return create_client(url, key)
    except Exception:
        return None


def _id(prefix: str, table: str) -> str:
    import random, string
    suffix = ''.join(random.choices(string.digits, k=6))
    return f"{prefix}{suffix}"


def _now() -> str:
    return datetime.now().strftime("%d/%m/%Y")


def _init_state():
    for key in ["sahar_comptes","sahar_contacts","sahar_opportunites",
                "sahar_activites","sahar_deals","sahar_factures"]:
        if key not in st.session_state:
            st.session_state[key] = []


def _write(table: str, record: dict) -> bool:
    sb = _sb()
    if sb:
        try:
            sb.table(table).insert(record).execute()
            return True
        except Exception as e:
            st.warning(f"Supabase: {e}")
    return False


def _update(table: str, id_val: str, data: dict):
    sb = _sb()
    if sb:
        try:
            sb.table(table).update(data).eq("id", id_val).execute()
        except Exception:
            pass
    # Mettre à jour session_state
    key = table
    if key in st.session_state:
        for item in st.session_state[key]:
            if item.get("id") == id_val:
                item.update(data)


def _delete(table: str, id_val: str):
    sb = _sb()
    if sb:
        try:
            sb.table(table).delete().eq("id", id_val).execute()
        except Exception:
            pass
    key = table
    if key in st.session_state:
        st.session_state[key] = [i for i in st.session_state[key] if i.get("id") != id_val]


def load_all():
    """Charge tout depuis Supabase au démarrage."""
    if st.session_state.get("sahar_crm_loaded"):
        return
    _init_state()
    sb = _sb()
    if sb:
        try:
            tables = ["sahar_comptes","sahar_contacts","sahar_opportunites",
                      "sahar_activites","sahar_deals","sahar_factures"]
            for t in tables:
                data = sb.table(t).select("*").order("created_at", desc=True).execute().data
                st.session_state[t] = data or []
        except Exception as e:
            st.warning(f"Chargement Supabase: {e}")
    st.session_state.sahar_crm_loaded = True


# ─── COMPTES ──────────────────────────────────────────────────────────────────

def create_compte(nom: str, type_: str = "prospect", secteur: str = "",
                  email: str = "", tel: str = "", ville: str = "",
                  source: str = "", utm_source: str = "", utm_medium: str = "",
                  utm_campaign: str = "", landing_page: str = "",
                  notes: str = "") -> dict:
    record = {
        "id": _id("CPT", "sahar_comptes"),
        "nom": nom, "type": type_, "secteur": secteur,
        "email": email, "tel": tel, "ville": ville,
        "source": source, "utm_source": utm_source,
        "utm_medium": utm_medium, "utm_campaign": utm_campaign,
        "landing_page": landing_page, "notes": notes,
        "score_lead": 0, "score_fit": 0,
        "date_creation": _now(), "date_update": _now(),
    }
    _write("sahar_comptes", record)
    _init_state()
    st.session_state.sahar_comptes.insert(0, record)
    return record


def get_comptes(type_: str = None) -> list:
    _init_state()
    items = st.session_state.get("sahar_comptes", [])
    return [i for i in items if i.get("type") == type_] if type_ else items


def update_compte(id_: str, **kwargs):
    kwargs["date_update"] = _now()
    _update("sahar_comptes", id_, kwargs)


# ─── CONTACTS ─────────────────────────────────────────────────────────────────

def create_contact(compte_id: str, nom: str, prenom: str = "",
                   email: str = "", tel: str = "", poste: str = "") -> dict:
    record = {
        "id": _id("CNT", "sahar_contacts"),
        "compte_id": compte_id, "nom": nom, "prenom": prenom,
        "email": email, "tel": tel, "poste": poste,
        "date_creation": _now(),
    }
    _write("sahar_contacts", record)
    _init_state()
    st.session_state.sahar_contacts.insert(0, record)
    return record


def get_contacts(compte_id: str = None) -> list:
    _init_state()
    items = st.session_state.get("sahar_contacts", [])
    return [i for i in items if i.get("compte_id") == compte_id] if compte_id else items


# ─── OPPORTUNITÉS ─────────────────────────────────────────────────────────────

def create_opportunite(compte_id: str, titre: str, offre: str = "",
                        valeur: float = 0, recurrence: str = "mensuel",
                        source: str = "", utm_source: str = "",
                        utm_campaign: str = "", contact_id: str = "",
                        date_closing_prevu: str = "") -> dict:
    record = {
        "id": _id("OPP", "sahar_opportunites"),
        "compte_id": compte_id, "contact_id": contact_id,
        "titre": titre, "offre": offre,
        "valeur": valeur, "recurrence": recurrence,
        "stage": "Qualification", "probabilite": 10,
        "source": source, "utm_source": utm_source,
        "utm_campaign": utm_campaign,
        "date_closing_prevu": date_closing_prevu,
        "date_creation": _now(), "date_update": _now(),
    }
    _write("sahar_opportunites", record)
    _init_state()
    st.session_state.sahar_opportunites.insert(0, record)
    return record


def update_stage(opp_id: str, stage: str):
    prob_map = {
        "Qualification": 10, "Démo": 25, "Proposition": 50,
        "Négociation": 75, "Closing": 100, "Perdu": 0
    }
    _update("sahar_opportunites", opp_id, {
        "stage": stage,
        "probabilite": prob_map.get(stage, 10),
        "date_update": _now(),
    })


def get_opportunites(stage: str = None) -> list:
    _init_state()
    items = st.session_state.get("sahar_opportunites", [])
    return [i for i in items if i.get("stage") == stage] if stage else items


# ─── ACTIVITÉS ────────────────────────────────────────────────────────────────

def log_activite(compte_id: str, type_: str, notes: str = "",
                  opp_id: str = "", contact_id: str = "",
                  statut: str = "fait", duree_min: int = 0,
                  date_activite: str = "") -> dict:
    record = {
        "id": _id("ACT", "sahar_activites"),
        "compte_id": compte_id, "opp_id": opp_id,
        "contact_id": contact_id, "type": type_,
        "notes": notes, "statut": statut,
        "duree_min": duree_min,
        "date_activite": date_activite or _now(),
        "date_creation": _now(),
    }
    _write("sahar_activites", record)
    _init_state()
    st.session_state.sahar_activites.insert(0, record)
    return record


def get_activites(compte_id: str = None, opp_id: str = None) -> list:
    _init_state()
    items = st.session_state.get("sahar_activites", [])
    if compte_id:
        items = [i for i in items if i.get("compte_id") == compte_id]
    if opp_id:
        items = [i for i in items if i.get("opp_id") == opp_id]
    return items


# ─── DEALS ────────────────────────────────────────────────────────────────────

def create_deal(compte_id: str, offre: str, montant: float,
                recurrence: str = "mensuel", opp_id: str = "",
                date_debut: str = "") -> dict:
    record = {
        "id": _id("DEA", "sahar_deals"),
        "compte_id": compte_id, "opp_id": opp_id,
        "offre": offre, "montant": montant,
        "recurrence": recurrence, "statut": "actif",
        "date_debut": date_debut or _now(),
        "date_creation": _now(),
    }
    _write("sahar_deals", record)
    _init_state()
    st.session_state.sahar_deals.insert(0, record)
    # Mettre le compte en "client"
    update_compte(compte_id, type_="client")
    # Fermer l'opportunité
    if opp_id:
        _update("sahar_opportunites", opp_id, {"stage": "Closing", "probabilite": 100})
    return record


def get_mrr() -> dict:
    _init_state()
    deals = [d for d in st.session_state.get("sahar_deals", []) if d.get("statut") == "actif"]
    mrr = sum(d["montant"] for d in deals if d.get("recurrence") == "mensuel")
    arr = sum(d["montant"] / 12 for d in deals if d.get("recurrence") == "annuel")
    return {"mrr": mrr + arr, "clients": len(deals)}


# ─── FACTURES ─────────────────────────────────────────────────────────────────

def create_facture(compte_id: str, montant_ht: float, deal_id: str = "",
                   date_echeance: str = "", notes: str = "") -> dict:
    factures = st.session_state.get("sahar_factures", [])
    numero = f"SAHAR-{datetime.now().year}-{len(factures)+1:04d}"
    record = {
        "id": _id("FAC", "sahar_factures"),
        "compte_id": compte_id, "deal_id": deal_id,
        "numero": numero, "montant_ht": montant_ht,
        "tva": 0.20,
        "statut": "en_attente",
        "date_emission": _now(),
        "date_echeance": date_echeance,
        "notes": notes,
    }
    _write("sahar_factures", record)
    _init_state()
    st.session_state.sahar_factures.insert(0, record)
    return record


def get_ca_total() -> dict:
    _init_state()
    factures = st.session_state.get("sahar_factures", [])
    payees = [f for f in factures if f.get("statut") == "payée"]
    attente = [f for f in factures if f.get("statut") == "en_attente"]
    return {
        "ca_total": sum(f["montant_ht"] * 1.2 for f in payees),
        "en_attente": sum(f["montant_ht"] * 1.2 for f in attente),
        "nb_factures": len(factures),
    }


# ─── KPIs GLOBAUX ─────────────────────────────────────────────────────────────

def get_kpis() -> dict:
    _init_state()
    comptes = st.session_state.get("sahar_comptes", [])
    opps = st.session_state.get("sahar_opportunites", [])
    acts = st.session_state.get("sahar_activites", [])
    mrr_data = get_mrr()
    ca_data = get_ca_total()

    opps_actives = [o for o in opps if o.get("stage") not in ["Closing","Perdu"]]
    pipeline_brut = sum(o.get("valeur", 0) for o in opps_actives)
    pipeline_pond = sum(o.get("valeur", 0) * o.get("probabilite", 10) / 100 for o in opps_actives)

    return {
        "prospects": len([c for c in comptes if c.get("type") == "prospect"]),
        "clients":   len([c for c in comptes if c.get("type") == "client"]),
        "opps_actives": len(opps_actives),
        "pipeline_brut": pipeline_brut,
        "pipeline_pond": pipeline_pond,
        "mrr": mrr_data["mrr"],
        "ca_total": ca_data["ca_total"],
        "activites": len(acts),
    }
