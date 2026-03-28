"""
SAHAR Conseil — auth.py
Authentification simple par mot de passe via st.secrets.
À remplacer par Supabase en v2.
"""

import streamlit as st


def verifier_acces(
    cle_secret: str = "APP_PWD",
    message: str = "🔐 Mot de passe requis",
    placeholder: str = "Entrez votre mot de passe",
) -> bool:
    """
    Vérifie l'accès à l'app avec un mot de passe stocké dans st.secrets.
    Stoppe l'exécution de la page si le mot de passe est incorrect.

    IMPORTANT : Ajouter dans .streamlit/secrets.toml :
        APP_PWD = "votre_mot_de_passe_ici"

    Sur Streamlit Cloud, ajouter dans Settings > Secrets.

    Args:
        cle_secret: Clé dans secrets.toml (défaut "APP_PWD")
        message: Titre affiché
        placeholder: Placeholder du champ mot de passe

    Returns:
        True si accès autorisé.

    Exemple :
        if not verifier_acces():
            st.stop()
        # Suite de l'app accessible uniquement si connecté
    """
    # Si déjà authentifié dans la session, ne pas re-demander
    if st.session_state.get("_sahar_auth_ok"):
        return True

    # Vérifier si le secret est configuré
    try:
        mot_de_passe_attendu = st.secrets[cle_secret]
    except (KeyError, FileNotFoundError):
        # En développement local sans secrets.toml, accès libre
        st.sidebar.warning(
            "⚠️ Mode développement — aucun mot de passe configuré.\n"
            "Créer `.streamlit/secrets.toml` avec `APP_PWD = 'monmotdepasse'`"
        )
        return True

    with st.sidebar:
        st.markdown(f"### {message}")
        pwd_saisie = st.text_input(
            "Mot de passe",
            type="password",
            placeholder=placeholder,
            label_visibility="collapsed",
        )

        if pwd_saisie:
            if pwd_saisie == mot_de_passe_attendu:
                st.session_state["_sahar_auth_ok"] = True
                st.success("✅ Accès autorisé")
                return True
            else:
                st.error("❌ Mot de passe incorrect")
                st.stop()
        else:
            st.info("Entrez le mot de passe pour accéder à l'outil.")
            st.stop()

    return False


def deconnecter():
    """Réinitialise l'état d'authentification."""
    st.session_state.pop("_sahar_auth_ok", None)
    st.rerun()


# Alias pour compatibilité avec app.py
def check_password() -> bool:
    """Alias de verifier_acces() — authentification par mot de passe (clé 'password' dans secrets)."""
    return verifier_acces(cle_secret="password", message="🔐 Back-office SAHAR")
