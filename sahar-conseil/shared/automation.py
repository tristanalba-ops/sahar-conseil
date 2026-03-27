"""
SAHAR Conseil — automation.py
Séquences email/SMS automatiques via Brevo (ex Sendinblue).
Gratuit jusqu'à 300 emails/jour.

Configuration dans secrets.toml :
  BREVO_API_KEY = "xkeysib-..."
  BREVO_SENDER_EMAIL = "contact@sahar-conseil.fr"
  BREVO_SENDER_NAME = "SAHAR Conseil"
  APP_URL = "https://sahar-conseil.fr"
"""

import requests
import streamlit as st
from datetime import datetime
from typing import Optional


def _get_brevo_key() -> Optional[str]:
    try:
        return st.secrets.get("BREVO_API_KEY")
    except Exception:
        return None


def _get_sender() -> dict:
    try:
        return {
            "email": st.secrets.get("BREVO_SENDER_EMAIL", "contact@sahar-conseil.fr"),
            "name":  st.secrets.get("BREVO_SENDER_NAME",  "SAHAR Conseil"),
        }
    except Exception:
        return {"email": "contact@sahar-conseil.fr", "name": "SAHAR Conseil"}


def _app_url() -> str:
    try:
        return st.secrets.get("APP_URL", "https://sahar-conseil.fr")
    except Exception:
        return "https://sahar-conseil.fr"


def _render_template(template: str, variables: dict) -> str:
    """Remplace les variables {nom}, {lien_app}, etc. dans un template."""
    variables.setdefault("lien_app", _app_url())
    variables.setdefault("date", datetime.now().strftime("%d/%m/%Y"))
    for key, val in variables.items():
        template = template.replace("{" + key + "}", str(val))
    return template


# ─── EMAIL VIA BREVO ──────────────────────────────────────────────────────────

def envoyer_email(
    destinataire_email: str,
    destinataire_nom: str,
    sujet: str,
    contenu: str,
    variables: dict = None,
) -> bool:
    """
    Envoie un email via l'API Brevo (Sendinblue).
    Retourne True si succès, False sinon.

    Args:
        destinataire_email: Email du destinataire
        destinataire_nom: Nom du destinataire
        sujet: Objet de l'email
        contenu: Corps de l'email (texte brut avec variables {nom}, {lien_app})
        variables: Dict de variables à injecter dans le template

    Exemple:
        envoyer_email(
            "jean@exemple.fr", "Jean",
            "Bienvenue sur SAHAR",
            "Bonjour {nom}, votre accès est prêt : {lien_app}",
            variables={"nom": "Jean"}
        )
    """
    api_key = _get_brevo_key()
    if not api_key:
        st.warning("Brevo non configuré — email non envoyé (ajouter BREVO_API_KEY dans secrets)")
        return False

    vars_merged = {"nom": destinataire_nom, **(variables or {})}
    contenu_rendu = _render_template(contenu, vars_merged)
    sujet_rendu   = _render_template(sujet,   vars_merged)

    # Convertir le texte en HTML simple
    html = "<br>".join(contenu_rendu.split("\n"))

    payload = {
        "sender": _get_sender(),
        "to": [{"email": destinataire_email, "name": destinataire_nom}],
        "subject": sujet_rendu,
        "htmlContent": f"<div style='font-family:sans-serif;font-size:15px;line-height:1.6;max-width:600px'>{html}</div>",
        "textContent": contenu_rendu,
    }

    try:
        r = requests.post(
            "https://api.brevo.com/v3/smtp/email",
            json=payload,
            headers={
                "api-key": api_key,
                "Content-Type": "application/json",
            },
            timeout=15,
        )
        r.raise_for_status()
        return True
    except requests.RequestException as e:
        st.error(f"Erreur envoi email : {e}")
        return False


# ─── SMS VIA BREVO ────────────────────────────────────────────────────────────

def envoyer_sms(
    tel: str,
    message: str,
    nom_destinataire: str = "",
    variables: dict = None,
) -> bool:
    """
    Envoie un SMS via Brevo.
    Nécessite un compte Brevo avec crédits SMS.

    Args:
        tel: Numéro au format international (+33612345678)
        message: Contenu du SMS (max 160 caractères)
        variables: Variables à injecter dans le message
    """
    api_key = _get_brevo_key()
    if not api_key:
        return False

    vars_merged = {"nom": nom_destinataire, **(variables or {})}
    message_rendu = _render_template(message, vars_merged)[:160]

    # Normaliser le numéro
    tel_clean = tel.replace(" ", "").replace("-", "").replace(".", "")
    if tel_clean.startswith("0"):
        tel_clean = "+33" + tel_clean[1:]

    payload = {
        "sender": "SAHAR",
        "recipient": tel_clean,
        "content": message_rendu,
        "type": "transactional",
    }

    try:
        r = requests.post(
            "https://api.brevo.com/v3/transactionalSMS/sms",
            json=payload,
            headers={"api-key": api_key, "Content-Type": "application/json"},
            timeout=15,
        )
        r.raise_for_status()
        return True
    except requests.RequestException as e:
        st.error(f"Erreur envoi SMS : {e}")
        return False


# ─── SÉQUENCES ────────────────────────────────────────────────────────────────

def declencher_sequence(
    contact_email: str,
    contact_nom: str,
    contact_tel: str = "",
    secteur: str = "immobilier",
    variables: dict = None,
    crm_db=None,
) -> int:
    """
    Déclenche la séquence d'emails/SMS pour un nouveau contact.
    Envoie l'email J+0 immédiatement.
    Les étapes suivantes sont à planifier via un scheduler (cron).

    Args:
        contact_email: Email du contact
        contact_nom: Nom du contact
        contact_tel: Téléphone (pour SMS)
        secteur: immobilier | energie | retail
        variables: Variables supplémentaires pour les templates
        crm_db: Module crm_db pour logger les envois

    Returns:
        Nombre d'emails/SMS envoyés (J+0 uniquement)
    """
    # Mapping secteur → séquence
    sequence_map = {
        "immobilier": "SEQ001",
        "energie":    "SEQ002",
        "retail":     "SEQ003",
    }
    seq_id = sequence_map.get(secteur, "SEQ001")

    # Étapes J+0 à envoyer immédiatement
    steps_j0 = {
        "SEQ001": {
            "email": {
                "sujet": "Bienvenue sur SAHAR — vos données DVF sont prêtes",
                "contenu": (
                    "Bonjour {nom},\n\n"
                    "Votre accès à DVF Analyse Pro est activé.\n\n"
                    "Voici ce que vous pouvez faire maintenant :\n"
                    "- Analyser les transactions de votre secteur\n"
                    "- Détecter les biens sous-valorisés\n"
                    "- Exporter vos prospects en Excel\n\n"
                    "Connectez-vous : {lien_app}\n\n"
                    "L'équipe SAHAR"
                )
            }
        },
        "SEQ002": {
            "email": {
                "sujet": "Vos prospects DPE F/G sont prêts",
                "contenu": (
                    "Bonjour {nom},\n\n"
                    "Votre accès au DPE Scanner est activé.\n\n"
                    "Dans votre secteur, nous avons identifié des logements classés F et G "
                    "— passoires thermiques concernées par les interdictions de location 2025.\n\n"
                    "Connectez-vous pour voir la liste : {lien_app}\n\n"
                    "L'équipe SAHAR"
                )
            }
        },
        "SEQ003": {
            "email": {
                "sujet": "Votre score d'attractivité de zone est prêt",
                "contenu": (
                    "Bonjour {nom},\n\n"
                    "Votre analyse Zone Score est disponible.\n\n"
                    "Nous avons calculé le potentiel commercial de votre zone cible.\n\n"
                    "Accédez au rapport : {lien_app}\n\n"
                    "L'équipe SAHAR"
                )
            }
        },
    }

    vars_merged = {"nom": contact_nom, **(variables or {})}
    envoyes = 0

    step = steps_j0.get(seq_id, steps_j0["SEQ001"])

    # Email J+0
    if "email" in step:
        ok = envoyer_email(
            contact_email, contact_nom,
            step["email"]["sujet"],
            step["email"]["contenu"],
            variables=vars_merged,
        )
        if ok:
            envoyes += 1

    return envoyes


# ─── EMAIL DE BIENVENUE LEAD (depuis formulaire site) ─────────────────────────

def email_bienvenue_lead(nom: str, email: str, secteur: str = "") -> bool:
    """
    Envoie l'email de bienvenue automatique quand un lead remplit le formulaire.
    À appeler depuis le webhook Formspree ou manuellement depuis le CRM.
    """
    sujets = {
        "Immobilier":        "Votre démo DVF Analyse Pro — réponse sous 24h",
        "Énergie / Rénovation": "Votre démo DPE Scanner — réponse sous 24h",
        "Retail / Franchise": "Votre démo Zone Score — réponse sous 24h",
    }
    sujet = sujets.get(secteur, "Votre demande SAHAR Conseil — réponse sous 24h")

    contenu = (
        "Bonjour {nom},\n\n"
        "Nous avons bien reçu votre demande.\n\n"
        "Nous vous recontactons sous 24h ouvrées avec une démonstration "
        "sur vos données réelles — pas une présentation générique.\n\n"
        "En attendant, vous pouvez explorer nos ressources :\n"
        "- Comment trouver des prospects qualifiés\n"
        "- Prospecter avec les données publiques\n"
        "- KPIs commerciaux pour votre pipeline\n\n"
        "{lien_app}\n\n"
        "L'équipe SAHAR Conseil\n"
        "contact@sahar-conseil.fr"
    )

    return envoyer_email(email, nom, sujet, contenu, variables={"nom": nom})


# ─── NOTIFICATION INTERNE ─────────────────────────────────────────────────────

def notifier_nouveau_lead(nom: str, email: str, secteur: str, message: str) -> bool:
    """
    Envoie une notification interne quand un nouveau lead arrive.
    """
    api_key = _get_brevo_key()
    if not api_key:
        return False

    try:
        notif_email = st.secrets.get("NOTIF_EMAIL", "contact@sahar-conseil.fr")
    except Exception:
        notif_email = "contact@sahar-conseil.fr"

    contenu = (
        f"Nouveau lead SAHAR\n\n"
        f"Nom : {nom}\n"
        f"Email : {email}\n"
        f"Secteur : {secteur}\n"
        f"Message : {message}\n\n"
        f"Date : {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    )

    return envoyer_email(
        notif_email, "SAHAR Admin",
        f"🔔 Nouveau lead — {nom} ({secteur})",
        contenu,
    )
