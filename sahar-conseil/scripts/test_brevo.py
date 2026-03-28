#!/usr/bin/env python3
"""
SAHAR Conseil — Test Brevo Email
Envoie un email de test pour valider la configuration Brevo.

Usage:
  python scripts/test_brevo.py YOUR_BREVO_API_KEY

Ou avec la clé dans .streamlit/secrets.toml :
  python scripts/test_brevo.py
"""

import sys
import os
import requests
import json

def test_brevo(api_key: str, to_email: str = "tristan.alba@yahoo.fr"):
    """Envoie un email de test via l'API Brevo."""

    print(f"Testing Brevo API...")
    print(f"  To: {to_email}")

    # 1. Vérifier le compte
    r = requests.get(
        "https://api.brevo.com/v3/account",
        headers={"api-key": api_key},
        timeout=10
    )
    if r.status_code == 200:
        account = r.json()
        print(f"  Account: {account.get('email', '?')}")
        print(f"  Plan: {account.get('plan', [{}])[0].get('type', '?') if account.get('plan') else '?'}")
        credits = account.get('plan', [{}])[0].get('credits', 0) if account.get('plan') else 0
        print(f"  Credits: {credits}")
    else:
        print(f"  Account check failed: {r.status_code} {r.text[:200]}")
        return False

    # 2. Envoyer l'email de test
    html = """<!DOCTYPE html><html lang="fr"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:32px 16px;background:#f5f5f5">
<tr><td align="center"><table width="100%" style="max-width:540px;background:#fff;border-radius:8px;border:1px solid #e5e5e5">
  <tr><td style="padding:20px 28px;border-bottom:1px solid #e5e5e5">
    <span style="font-size:14px;font-weight:700;color:#1a1a1a">SAHAR <span style="color:#4A7C59">Conseil</span></span>
  </td></tr>
  <tr><td style="padding:28px">
    <h2 style="margin:0 0 16px;font-size:22px;color:#1a1a1a">Test Brevo OK</h2>
    <p style="font-size:15px;color:#333;line-height:1.75">
      Cet email confirme que la configuration Brevo de SAHAR Conseil fonctionne correctement.<br><br>
      <strong>Ce qui est actif :</strong>
    </p>
    <ul style="font-size:14px;color:#555;line-height:2">
      <li>Emails transactionnels (confirmations, notifications)</li>
      <li>Séquence J0/J3/J7 pour les leads du site</li>
      <li>Prospection DVF et DPE</li>
      <li>Notifications internes nouveau lead</li>
    </ul>
    <div style="background:#f0f7f2;border-radius:6px;padding:16px;margin-top:16px">
      <p style="margin:0;font-size:14px;color:#4A7C59;font-weight:600">
        Configuration validée le """ + __import__('datetime').datetime.now().strftime('%d/%m/%Y à %H:%M') + """
      </p>
    </div>
  </td></tr>
  <tr><td style="padding:16px 28px;border-top:1px solid #f0f0f0">
    <p style="margin:0;font-size:12px;color:#aaa">SAHAR Conseil — sahar-conseil.fr</p>
  </td></tr>
</table></td></tr></table></body></html>"""

    payload = {
        "sender": {"email": "contact@sahar-conseil.fr", "name": "SAHAR Conseil"},
        "to": [{"email": to_email, "name": "Tristan Alba"}],
        "subject": "SAHAR Conseil — Configuration Brevo validée",
        "htmlContent": html,
        "textContent": "Test Brevo OK. La configuration email de SAHAR Conseil fonctionne."
    }

    r = requests.post(
        "https://api.brevo.com/v3/smtp/email",
        json=payload,
        headers={"api-key": api_key, "Content-Type": "application/json"},
        timeout=15
    )

    if r.status_code in (200, 201):
        print(f"\n  EMAIL SENT! Message ID: {r.json().get('messageId', '?')}")
        print(f"  Check inbox: {to_email}")
        return True
    else:
        print(f"\n  SEND FAILED: {r.status_code}")
        print(f"  Response: {r.text[:300]}")
        return False


if __name__ == "__main__":
    # Try CLI arg first, then secrets.toml
    api_key = None

    if len(sys.argv) > 1:
        api_key = sys.argv[1]
    else:
        secrets_path = os.path.join(os.path.dirname(__file__), '..', '.streamlit', 'secrets.toml')
        if os.path.exists(secrets_path):
            with open(secrets_path) as f:
                for line in f:
                    if line.strip().startswith('BREVO_API_KEY'):
                        val = line.split('=', 1)[1].strip().strip('"').strip("'")
                        if val:
                            api_key = val
                        break

    if not api_key:
        print("Usage: python test_brevo.py <BREVO_API_KEY>")
        print("\nPour obtenir une clé Brevo gratuite :")
        print("  1. Créer un compte sur https://app.brevo.com")
        print("  2. Settings > API Keys > Generate")
        print("  3. Coller la clé dans .streamlit/secrets.toml")
        sys.exit(1)

    ok = test_brevo(api_key)
    sys.exit(0 if ok else 1)
