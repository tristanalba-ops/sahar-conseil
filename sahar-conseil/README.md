# SAHAR Conseil — Suite d'outils SaaS Open Data

> Transformer les données publiques en outils métiers exploitables par des professionnels.

## Apps disponibles

| App | Secteur | Statut | Lien |
|-----|---------|--------|------|
| DVF Analyse Pro | Immobilier | 🟡 En développement | — |
| DPE Scanner | Énergie / Rénovation | 🔴 À venir | — |
| Zone Score Retail | Retail / Franchise | 🔴 À venir | — |
| EV Potential Map | Automobile | 🔴 À venir | — |
| RH Tension Map | RH / Recrutement | 🔴 À venir | — |

## Stack technique

- **Apps** : Python + Streamlit
- **Déploiement** : Streamlit Community Cloud (gratuit)
- **Data processing** : pandas, geopandas, requests
- **Visualisation** : plotly, folium
- **Site vitrine** : GitHub Pages
- **Base de données** : Supabase (optionnel, v2)

## Structure du repo

```
sahar-conseil/
├── apps/                    # Une app Streamlit par outil
│   ├── dvf_analyse/         # Analyse marché immobilier (DVF)
│   ├── dpe_scanner/         # Détection prospects énergie (DPE)
│   ├── zone_score/          # Score attractivité zones retail
│   ├── ev_map/              # Potentiel VE par territoire
│   └── rh_tension/          # Carte tensions recrutement
├── shared/                  # Modules Python partagés par toutes les apps
│   ├── data_loader.py       # Chargement et cache des données
│   ├── scoring.py           # Moteur de scoring 0–100
│   ├── viz.py               # Visualisations (cartes, graphiques)
│   ├── export.py            # Export Excel et PDF
│   ├── geo_utils.py         # Utilitaires géographiques
│   └── auth.py              # Authentification simple
├── data/
│   ├── raw/                 # Données téléchargées (CSV, GeoJSON)
│   └── processed/           # Données nettoyées (parquet, csv)
├── site/                    # Site vitrine GitHub Pages
└── docs/                    # Documentation
```

## Démarrage rapide (développeur)

```bash
# 1. Cloner le repo
git clone https://github.com/VOTRE_USERNAME/sahar-conseil.git
cd sahar-conseil

# 2. Installer les dépendances
pip install -r apps/dvf_analyse/requirements.txt

# 3. Configurer les secrets
cp apps/dvf_analyse/.streamlit/secrets.toml.example apps/dvf_analyse/.streamlit/secrets.toml
# Éditer secrets.toml avec votre mot de passe

# 4. Lancer l'app
cd apps/dvf_analyse
streamlit run app.py
```

## Déploiement sur Streamlit Cloud

1. Aller sur [share.streamlit.io](https://share.streamlit.io)
2. Connecter votre compte GitHub
3. Sélectionner ce repo, branch `main`, fichier `apps/dvf_analyse/app.py`
4. Ajouter les secrets dans l'interface Streamlit Cloud
5. Déployer

## Sources de données

- **DVF** : https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/
- **DPE ADEME** : https://data.ademe.fr/datasets/dpe-v2-logements-existants
- **INSEE IRIS** : https://www.insee.fr/fr/statistiques/contours
- **BAN** : https://adresse.data.gouv.fr/api-doc/adresse
- **SIRENE** : https://api.insee.fr/catalogue/
- **Cadastre** : https://cadastre.data.gouv.fr/

## Contact

Site : https://VOTRE_USERNAME.github.io/sahar-conseil  
Email : contact@sahar-conseil.fr
