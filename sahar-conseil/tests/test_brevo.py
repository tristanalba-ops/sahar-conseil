"""
SAHAR Conseil — Test envoi email Brevo
Usage :
  streamlit run tests/test_brevo.py
  (ou directement python tests/test_brevo.py avec BREVO_API_KEY en env)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

def test_sans_streamlit():
    """Test direct sans Streamlit — utilise variable d'env."""
    import requests
    api_key = os.environ.get("BREVO_API_KEY")
    if not api_key:
        print("❌ BREVO_API_KEY non définie. Définissez-la en variable d'env.")
        print("   export BREVO_API_KEY=xkeysib-...")
        return False

    payload = {
        "sender": {"email": "contact@sahar-conseil.fr", "name": "SAHAR Conseil"},
        "to": [{"email": "tristan.alba@yahoo.fr", "name": "Tristan Alba"}],
        "subject": "🧪 Test SAHAR — Brevo fonctionne",
        "htmlContent": """
            <html><body style="font-family:sans-serif;padding:20px">
            <h2>Test réussi ✅</h2>
            <p>Cet email confirme que l'intégration Brevo de SAHAR Conseil fonctionne.</p>
            <p style="color:#888;font-size:12px">Envoyé depuis test_brevo.py</p>
            </body></html>
        """,
        "textContent": "Test SAHAR — Brevo fonctionne. Email envoyé depuis test_brevo.py."
    }

    try:
        r = requests.post(
            "https://api.brevo.com/v3/smtp/email",
            json=payload,
            headers={"api-key": api_key, "Content-Type": "application/json"},
            timeout=15,
        )
        if r.status_code in (200, 201):
            data = r.json()
            print(f"✅ Email envoyé ! messageId={data.get('messageId')}")
            return True
        else:
            print(f"❌ Erreur {r.status_code}: {r.text}")
            return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


def test_sequence_emails():
    """Test la séquence J0/J3/J7 + templates DVF/DPE."""
    api_key = os.environ.get("BREVO_API_KEY")
    if not api_key:
        print("❌ BREVO_API_KEY non définie")
        return

    # On monkey-patch st.secrets pour le mode CLI
    import types
    class FakeSecrets:
        def get(self, key, default=None):
            env = os.environ.get(key, default)
            return env

    import streamlit as st
    st.secrets = FakeSecrets()

    print("\n--- Test templates ---")

    from shared.automation import email_prospect_dvf, email_prospect_dpe, email_prospect_generique
    from shared.emails_site import envoyer_j0, envoyer_j3, envoyer_j7

    tests = [
        ("email_prospect_dvf", lambda: email_prospect_dvf(
            "tristan.alba@yahoo.fr", "Tristan Alba",
            "12 rue de la Paix", "Paris 2e", 450000, 65, 6923, 7500, 82)),
        ("email_prospect_dpe", lambda: email_prospect_dpe(
            "tristan.alba@yahoo.fr", "Tristan Alba",
            "8 avenue Foch", "Paris 16e", "G", 85, 1955)),
        ("email_prospect_generique", lambda: email_prospect_generique(
            "tristan.alba@yahoo.fr", "Tristan Alba",
            "Test relance SAHAR", "Bonjour Tristan,\n\nCeci est un test de relance.")),
        ("envoyer_j0", lambda: envoyer_j0("Tristan Alba", "tristan.alba@yahoo.fr", "Immobilier")),
        ("envoyer_j3", lambda: envoyer_j3("Tristan Alba", "tristan.alba@yahoo.fr", "Immobilier")),
        ("envoyer_j7", lambda: envoyer_j7("Tristan Alba", "tristan.alba@yahoo.fr", "Immobilier")),
    ]

    for name, fn in tests:
        try:
            ok = fn()
            print(f"  {'✅' if ok else '❌'} {name}")
        except Exception as e:
            print(f"  ❌ {name} — {e}")


if __name__ == "__main__":
    if "--all" in sys.argv:
        test_sans_streamlit()
        test_sequence_emails()
    else:
        ok = test_sans_streamlit()
        if ok:
            print("\nPour tester tous les templates : python tests/test_brevo.py --all")
