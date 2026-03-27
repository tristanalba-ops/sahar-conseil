"""
SAHAR Conseil — crm_db.py
Couche de persistance CRM via Supabase.
Fallback automatique sur session_state si Supabase non configuré.
"""

import streamlit as st
from datetime import datetime
from typing import Optional


def _get_client():
    """Retourne le client Supabase ou None si non configuré."""
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        from supabase import create_client
        return create_client(url, key)
    except Exception:
        return None


def _use_supabase() -> bool:
    try:
        return bool(st.secrets.get("SUPABASE_URL") and st.secrets.get("SUPABASE_KEY"))
    except Exception:
        return False


# ─── INIT SESSION STATE FALLBACK ─────────────────────────────────────────────

def init_crm():
    """Initialise le CRM — charge depuis Supabase si dispo, sinon session_state."""
    if "crm_loaded" in st.session_state:
        return

    if _use_supabase():
        try:
            sb = _get_client()
            st.session_state.crm_contacts     = sb.table("crm_contacts").select("*").execute().data
            st.session_state.crm_opportunites = sb.table("crm_opportunites").select("*").execute().data
            st.session_state.crm_activites    = sb.table("crm_activites").select("*").execute().data
        except Exception as e:
            st.warning(f"Supabase non disponible ({e}). Mode local activé.")
            _init_local()
    else:
        _init_local()

    st.session_state.crm_loaded = True


def _init_local():
    if "crm_contacts"     not in st.session_state: st.session_state.crm_contacts     = []
    if "crm_opportunites" not in st.session_state: st.session_state.crm_opportunites = []
    if "crm_activites"    not in st.session_state: st.session_state.crm_activites    = []


def _new_id(prefix: str, liste: list) -> str:
    return f"{prefix}{len(liste)+1:04d}"


# ─── CONTACTS ─────────────────────────────────────────────────────────────────

def add_contact(nom: str, email: str = "", tel: str = "",
                type_contact: str = "Autre", notes: str = "") -> dict:
    record = {
        "id": _new_id("C", st.session_state.crm_contacts),
        "nom": nom, "email": email, "tel": tel,
        "type": type_contact, "notes": notes,
        "date_creation": datetime.now().strftime("%d/%m/%Y"),
    }
    if _use_supabase():
        try:
            _get_client().table("crm_contacts").insert(record).execute()
        except Exception as e:
            st.warning(f"Supabase write error: {e}")
    st.session_state.crm_contacts.append(record)
    return record


def get_contacts() -> list:
    return st.session_state.get("crm_contacts", [])


def update_contact(contact_id: str, **kwargs):
    for c in st.session_state.crm_contacts:
        if c["id"] == contact_id:
            c.update(kwargs)
    if _use_supabase():
        try:
            _get_client().table("crm_contacts").update(kwargs).eq("id", contact_id).execute()
        except Exception:
            pass


def delete_contact(contact_id: str):
    st.session_state.crm_contacts = [c for c in st.session_state.crm_contacts if c["id"] != contact_id]
    if _use_supabase():
        try:
            _get_client().table("crm_contacts").delete().eq("id", contact_id).execute()
        except Exception:
            pass


# ─── OPPORTUNITÉS ─────────────────────────────────────────────────────────────

def add_opportunite(contact_id: str, titre: str, adresse: str,
                    type_bien: str, surface: float, prix: float,
                    prix_m2: float, score: int, source: str = "DVF") -> dict:
    record = {
        "id": _new_id("O", st.session_state.crm_opportunites),
        "contact_id": contact_id, "titre": titre, "adresse": adresse,
        "type_bien": type_bien, "surface": surface, "prix": prix,
        "prix_m2": prix_m2, "score": score, "source": source,
        "stage": "Détecté",
        "date_creation": datetime.now().strftime("%d/%m/%Y"),
        "date_update": datetime.now().strftime("%d/%m/%Y"),
    }
    if _use_supabase():
        try:
            _get_client().table("crm_opportunites").insert(record).execute()
        except Exception as e:
            st.warning(f"Supabase write error: {e}")
    st.session_state.crm_opportunites.append(record)
    return record


def get_opportunites(stage: Optional[str] = None) -> list:
    opps = st.session_state.get("crm_opportunites", [])
    return [o for o in opps if o["stage"] == stage] if stage else opps


def update_stage(opp_id: str, stage: str):
    now = datetime.now().strftime("%d/%m/%Y")
    for o in st.session_state.crm_opportunites:
        if o["id"] == opp_id:
            o["stage"] = stage
            o["date_update"] = now
    if _use_supabase():
        try:
            _get_client().table("crm_opportunites").update(
                {"stage": stage, "date_update": now}
            ).eq("id", opp_id).execute()
        except Exception:
            pass


def delete_opportunite(opp_id: str):
    st.session_state.crm_opportunites = [o for o in st.session_state.crm_opportunites if o["id"] != opp_id]
    if _use_supabase():
        try:
            _get_client().table("crm_opportunites").delete().eq("id", opp_id).execute()
        except Exception:
            pass


# ─── ACTIVITÉS ────────────────────────────────────────────────────────────────

def add_activite(opp_id: str, type_activite: str, notes: str = "",
                 date_str: Optional[str] = None, statut: str = "À faire") -> dict:
    record = {
        "id": _new_id("A", st.session_state.crm_activites),
        "opp_id": opp_id, "type": type_activite, "notes": notes,
        "statut": statut,
        "date": date_str or datetime.now().strftime("%d/%m/%Y"),
        "date_creation": datetime.now().strftime("%d/%m/%Y"),
    }
    if _use_supabase():
        try:
            _get_client().table("crm_activites").insert(record).execute()
        except Exception as e:
            st.warning(f"Supabase write error: {e}")
    st.session_state.crm_activites.append(record)
    return record


def get_activites(opp_id: Optional[str] = None) -> list:
    acts = st.session_state.get("crm_activites", [])
    return [a for a in acts if a["opp_id"] == opp_id] if opp_id else acts


def update_activite(act_id: str, **kwargs):
    for a in st.session_state.crm_activites:
        if a["id"] == act_id:
            a.update(kwargs)
    if _use_supabase():
        try:
            _get_client().table("crm_activites").update(kwargs).eq("id", act_id).execute()
        except Exception:
            pass
