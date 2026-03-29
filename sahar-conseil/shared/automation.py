"""
SAHAR Conseil — automation.py
Emails et SMS commerciaux sortants vers les prospects.
Expéditeur : SAHAR Conseil via Brevo
Cibles : prospects détectés via DVF, DPE, SIRENE
"""

import requests
import streamlit as st
from datetime import datetime
from typing import Optional


def _key() -> Optional[str]:
    try:
        return st.secrets.get("BREVO_API_KEY")
    except Exception:
        return None

def _sender() -> dict:
    try:
        return {
            "email": st.secrets.get("BREVO_SENDER_EMAIL", "contact@sahar-conseil.fr"),
            "name":  st.secrets.get("BREVO_SENDER_NAME", "SAHAR Conseil"),
        }
    except Exception:
        return {"email": "contact@sahar-conseil.fr", "name": "SAHAR Conseil"}

def _url() -> str:
    try:
        return st.secrets.get("APP_URL", "https://sahar-conseil.fr")
    except Exception:
        return "https://sahar-conseil.fr"


# ─── HTML TEMPLATE ────────────────────────────────────────────────────────────

def _html(accroche: str, corps: str, cta_txt: str = "", cta_url: str = "") -> str:
    cta = ""
    if cta_txt:
        cta = f'<tr><td style="padding:20px 0 4px"><a href="{cta_url}" style="display:inline-block;background:#1a1a1a;color:#fff;text-decoration:none;padding:11px 22px;border-radius:6px;font-size:14px;font-weight:600">{cta_txt}</a></td></tr>'
    return f"""<!DOCTYPE html><html lang="fr"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:32px 16px;background:#f5f5f5">
<tr><td align="center"><table width="100%" style="max-width:540px;background:#fff;border-radius:8px;border:1px solid #e5e5e5">
  <tr><td style="padding:20px 28px;border-bottom:1px solid #e5e5e5">
    <span style="font-size:14px;font-weight:700;color:#1a1a1a">SAHAR <span style="color:#185FA5">Conseil</span></span>
  </td></tr>
  <tr><td style="padding:28px 28px 8px">
    <p style="margin:0 0 16px;font-size:20px;font-weight:700;color:#1a1a1a;line-height:1.25;letter-spacing:-.02em">{accroche}</p>
    <div style="font-size:15px;color:#333;line-height:1.75">{corps}</div>
  </td></tr>
  {cta}
  <tr><td style="padding:20px 28px;border-top:1px solid #f0f0f0">
    <p style="margin:0;font-size:12px;color:#aaa;line-height:1.6">
      Vous recevez ce message car votre bien ou votre activité a été identifié dans les données publiques françaises (DVF, ADEME DPE).<br>
      Pour ne plus recevoir nos messages, répondez "STOP" à cet email.
    </p>
  </td></tr>
</table></td></tr></table>
</body></html>"""


# ─── ENVOI BREVO ─────────────────────────────────────────────────────────────

def envoyer_email(to_email: str, to_nom: str, sujet: str,
                  html: str, texte: str = "") -> bool:
    api_key = _key()
    if not api_key:
        st.warning("Brevo non configuré (BREVO_API_KEY manquant)")
        return False

    # Nettoyer le nom — pas de majuscules entières
    to_nom_propre = to_nom.strip().title() if to_nom == to_nom.upper() else to_nom.strip()

    payload = {
        "sender": _sender(),
        "to": [{"email": to_email, "name": to_nom_propre}],
        "subject": sujet,
        "htmlContent": html,
        "textContent": texte or sujet,
    }
    try:
        r = requests.post(
            "https://api.brevo.com/v3/smtp/email",
            json=payload,
            headers={"api-key": api_key, "Content-Type": "application/json"},
            timeout=15,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        st.error(f"Erreur envoi email : {e}")
        return False


def envoyer_sms(tel: str, message: str) -> bool:
    api_key = _key()
    if not api_key:
        return False
    tel_clean = tel.replace(" ", "").replace("-", "").replace(".", "")
    if tel_clean.startswith("0"):
        tel_clean = "+33" + tel_clean[1:]
    try:
        r = requests.post(
            "https://api.brevo.com/v3/transactionalSMS/sms",
            json={"sender": "SAHAR", "recipient": tel_clean,
                  "content": message[:160], "type": "transactional"},
            headers={"api-key": api_key, "Content-Type": "application/json"},
            timeout=15,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        st.error(f"Erreur SMS : {e}")
        return False


# ─── TEMPLATES COMMERCIAUX ────────────────────────────────────────────────────

def email_prospect_dvf(to_email: str, to_nom: str,
                       adresse: str, commune: str,
                       prix: float, surface: float,
                       prix_m2: float, mediane_m2: float,
                       score: int) -> bool:
    """
    Email vers un vendeur/propriétaire détecté via DVF.
    Contexte : bien vendu sous la médiane ou marché actif.
    """
    ecart = round((mediane_m2 - prix_m2) / mediane_m2 * 100) if mediane_m2 > 0 else 0
    prenom = to_nom.strip().title() if to_nom == to_nom.upper() else to_nom.split()[0]

    accroche = f"Votre bien à {commune} — analyse de marché gratuite"

    corps = f"""
Bonjour {prenom},<br><br>
Nous avons analysé les transactions récentes à <strong>{commune}</strong> et votre bien 
au <strong>{adresse}</strong> a retenu notre attention.<br><br>
<table style="border-collapse:collapse;width:100%;font-size:14px">
  <tr style="background:#f5f5f5">
    <td style="padding:8px 12px;font-weight:600">Prix de vente</td>
    <td style="padding:8px 12px">{prix:,.0f} €</td>
  </tr>
  <tr>
    <td style="padding:8px 12px;font-weight:600">Surface</td>
    <td style="padding:8px 12px">{surface:.0f} m²</td>
  </tr>
  <tr style="background:#f5f5f5">
    <td style="padding:8px 12px;font-weight:600">Prix au m²</td>
    <td style="padding:8px 12px">{prix_m2:.0f} €/m²</td>
  </tr>
  <tr>
    <td style="padding:8px 12px;font-weight:600">Médiane du secteur</td>
    <td style="padding:8px 12px">{mediane_m2:.0f} €/m²</td>
  </tr>
  {"<tr style='background:#fff3cd'><td style='padding:8px 12px;font-weight:600'>Écart</td><td style='padding:8px 12px;color:#e65100;font-weight:700'>−" + str(ecart) + "% sous la médiane</td></tr>" if ecart > 5 else ""}
</table><br>
Nous pouvons vous fournir une analyse complète du marché dans votre secteur — 
prix signés, délais de vente, opportunités actuelles — en 24h.<br><br>
Êtes-vous disponible pour un échange rapide cette semaine ?
"""
    html = _html(accroche, corps, "Voir l'analyse complète", _url())
    texte = f"Bonjour {prenom}, analyse marché {commune} disponible. Votre bien : {prix_m2:.0f}€/m² vs médiane {mediane_m2:.0f}€/m². Réponse : {_url()}"
    return envoyer_email(to_email, to_nom, accroche, html, texte)


def email_prospect_dpe(to_email: str, to_nom: str,
                       adresse: str, commune: str,
                       etiquette: str, surface: float,
                       annee_construction: int = 0) -> bool:
    """
    Email vers un propriétaire avec logement classé F ou G.
    Contexte réglementaire : interdiction de location.
    """
    prenom = to_nom.strip().title() if to_nom == to_nom.upper() else to_nom.split()[0]
    annee_str = f", construit en {annee_construction}" if annee_construction else ""
    urgence = "depuis janvier 2023" if etiquette == "G" else "depuis janvier 2025"

    accroche = f"Logement classé {etiquette} — interdiction de location en vigueur"

    corps = f"""
Bonjour {prenom},<br><br>
Selon la base de diagnostics énergétiques de l'ADEME, votre logement au 
<strong>{adresse}, {commune}</strong> ({surface:.0f} m²{annee_str}) 
est classé <strong style="color:{'#c62828' if etiquette=='G' else '#e64a19'}">DPE {etiquette}</strong>.<br><br>
<div style="background:#fff3cd;border-left:3px solid #e65100;padding:12px 16px;border-radius:0 6px 6px 0;margin:16px 0;font-size:14px">
  <strong>Ce que ça implique :</strong> ce logement ne peut plus être mis en location {urgence}. 
  En cas de nouveau bail ou renouvellement, vous êtes en infraction.
</div>
<strong>Les travaux éligibles aux aides :</strong>
<ul style="margin:8px 0;padding-left:20px">
  <li>Isolation des murs et combles</li>
  <li>Remplacement du système de chauffage</li>
  <li>Ventilation et menuiseries</li>
</ul>
Selon votre situation, les aides MaPrimeRénov' peuvent couvrir jusqu'à 70% des travaux.<br><br>
Nous pouvons vous mettre en contact avec des artisans RGE qualifiés dans votre secteur 
et vous accompagner dans le montage du dossier d'aide.<br><br>
Souhaitez-vous un devis gratuit cette semaine ?
"""
    html = _html(accroche, corps, "Obtenir un devis gratuit", _url())
    texte = f"Bonjour {prenom}, votre logement {adresse} est classé DPE {etiquette} et ne peut plus être loué {urgence}. Devis travaux gratuit : {_url()}"
    return envoyer_email(to_email, to_nom, accroche, html, texte)


def email_prospect_generique(to_email: str, to_nom: str,
                              sujet: str, message_perso: str,
                              cta_txt: str = "Répondre",
                              cta_url: str = "") -> bool:
    """
    Email libre vers n'importe quel prospect depuis le CRM.
    Pour les relances, qualifications, propositions commerciales.
    """
    prenom = to_nom.strip().title() if to_nom == to_nom.upper() else to_nom.split()[0]
    corps = message_perso.replace("\n", "<br>")
    html = _html(f"Message pour {prenom}", corps,
                 cta_txt, cta_url or _url())
    return envoyer_email(to_email, to_nom, sujet, html, message_perso)


def sms_prospect(tel: str, prenom: str, message: str) -> bool:
    """SMS court vers un prospect — 160 caractères max."""
    prenom_propre = prenom.strip().title() if prenom == prenom.upper() else prenom.split()[0]
    msg = f"{prenom_propre}, {message}"[:160]
    return envoyer_sms(tel, msg)


# ─── NOTIFICATION INTERNE ─────────────────────────────────────────────────────

def notifier_nouveau_lead(nom: str, email: str,
                          secteur: str, message: str) -> bool:
    """Alerte interne quand un lead arrive depuis le site."""
    try:
        notif = st.secrets.get("NOTIF_EMAIL", "contact@sahar-conseil.fr")
    except Exception:
        notif = "contact@sahar-conseil.fr"

    corps = f"""
<strong>Nouveau lead SAHAR</strong><br><br>
<table style="font-size:14px;border-collapse:collapse">
  <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Nom</td><td>{nom}</td></tr>
  <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Email</td><td>{email}</td></tr>
  <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Secteur</td><td>{secteur}</td></tr>
  <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Message</td><td>{message}</td></tr>
  <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Date</td><td>{datetime.now().strftime('%d/%m/%Y %H:%M')}</td></tr>
</table>
"""
    html = _html(f"🔔 Nouveau lead — {nom}", corps)
    return envoyer_email(notif, "SAHAR Admin",
                         f"Nouveau lead : {nom} ({secteur})", html)


# ─── COMPAT (ancienne API) ────────────────────────────────────────────────────

def email_bienvenue_lead(nom: str, email: str, secteur: str = "") -> bool:
    """Redirige vers notifier_nouveau_lead pour compat."""
    return notifier_nouveau_lead(nom, email, secteur, "—")
