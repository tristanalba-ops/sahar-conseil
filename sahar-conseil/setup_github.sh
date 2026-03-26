#!/bin/bash
# ============================================================
# SAHAR Conseil — Script de setup initial GitHub
# À exécuter UNE SEULE FOIS depuis votre machine locale.
# ============================================================
# PRÉREQUIS :
#   - Git installé (https://git-scm.com/)
#   - Python 3.9+ installé
#   - Compte GitHub créé
#   - Repo GitHub créé et vide (ex: github.com/VOTRE_USERNAME/sahar-conseil)
#
# UTILISATION :
#   1. Modifier la variable GITHUB_USERNAME ci-dessous
#   2. Ouvrir un terminal dans le dossier sahar-conseil/
#   3. chmod +x setup_github.sh
#   4. ./setup_github.sh
# ============================================================

# ── CONFIGURATION ── Modifier ces valeurs
GITHUB_USERNAME="VOTRE_USERNAME"
REPO_NAME="sahar-conseil"
BRANCHE="main"
# ────────────────────────────────────────

set -e  # Arrêter en cas d'erreur

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   SAHAR Conseil — Setup GitHub           ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# Vérifier que git est installé
if ! command -v git &> /dev/null; then
    echo "❌ Git n'est pas installé. Installer depuis https://git-scm.com/"
    exit 1
fi

# Vérifier que python est installé
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 n'est pas installé. Installer depuis https://python.org/"
    exit 1
fi

echo "✅ Git et Python détectés"
echo ""

# Initialiser le repo Git
echo "📁 Initialisation du repo Git..."
git init
git branch -M $BRANCHE

# Configurer le remote GitHub
REMOTE_URL="https://github.com/${GITHUB_USERNAME}/${REPO_NAME}.git"
git remote add origin $REMOTE_URL
echo "🔗 Remote configuré : $REMOTE_URL"

# Créer le fichier secrets local (ne sera pas commité)
if [ ! -f "apps/dvf_analyse/.streamlit/secrets.toml" ]; then
    cp apps/dvf_analyse/.streamlit/secrets.toml.example apps/dvf_analyse/.streamlit/secrets.toml
    echo ""
    echo "⚠️  IMPORTANT : Éditer apps/dvf_analyse/.streamlit/secrets.toml"
    echo "   Remplacer 'votre_mot_de_passe_ici' par un vrai mot de passe"
    echo ""
fi

# Premier commit
echo "📦 Préparation du premier commit..."
git add .
git status

echo ""
echo "─────────────────────────────────────────"
echo "Résumé des fichiers qui seront committés :"
git diff --cached --name-only
echo "─────────────────────────────────────────"
echo ""

git commit -m "🚀 Initial commit — SAHAR Conseil skeleton

- Structure complète du repo
- Apps : dvf_analyse (complet), dpe_scanner (v0)
- Modules partagés : data_loader, scoring, viz, export, geo_utils, auth
- Site vitrine GitHub Pages
- .gitignore configuré (secrets, data CSV exclus)"

echo ""
echo "📤 Push vers GitHub..."
git push -u origin $BRANCHE

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   ✅ REPO GITHUB CRÉÉ AVEC SUCCÈS !                          ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║  Repo : https://github.com/${GITHUB_USERNAME}/${REPO_NAME}  ║"
echo "║                                                              ║"
echo "║  PROCHAINES ÉTAPES :                                         ║"
echo "║                                                              ║"
echo "║  1. SITE VITRINE (GitHub Pages)                              ║"
echo "║     → Repo Settings > Pages > Source: Deploy from branch     ║"
echo "║     → Branch: main, Folder: /site                           ║"
echo "║     → URL: https://${GITHUB_USERNAME}.github.io/${REPO_NAME}  ║"
echo "║                                                              ║"
echo "║  2. DÉPLOYER L'APP DVF                                       ║"
echo "║     → Aller sur https://share.streamlit.io                   ║"
echo "║     → New app > GitHub > ${REPO_NAME}                        ║"
echo "║     → Main file: apps/dvf_analyse/app.py                    ║"
echo "║     → Secrets: coller le contenu de secrets.toml            ║"
echo "║                                                              ║"
echo "║  3. TÉLÉCHARGER LES DONNÉES DVF                              ║"
echo "║     → https://www.data.gouv.fr/fr/datasets/                  ║"
echo "║       demandes-de-valeurs-foncieres/                         ║"
echo "║     → Placer dans data/raw/dvf_{dept}.csv                   ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
