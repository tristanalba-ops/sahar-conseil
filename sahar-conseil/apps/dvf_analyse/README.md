# DVF Analyse Pro — SAHAR Conseil

Outil d'analyse du marché immobilier basé sur les Demandes de Valeurs Foncières (DVF).

## Ce que fait cet outil

- Charge les transactions immobilières par département (DVF open data)
- Calcule un score d'opportunité 0–100 pour chaque bien
- Affiche une carte interactive des transactions
- Filtre par type de bien, surface, prix, période
- Exporte en Excel et PDF

## Installation locale

```bash
# Depuis la racine du repo sahar-conseil/
pip install -r apps/dvf_analyse/requirements.txt

# Configurer le mot de passe
cp apps/dvf_analyse/.streamlit/secrets.toml.example apps/dvf_analyse/.streamlit/secrets.toml
# Éditer secrets.toml : APP_PWD = "mon_mot_de_passe"

# Lancer l'app
cd apps/dvf_analyse
streamlit run app.py
```

L'app sera accessible sur http://localhost:8501

## Déploiement Streamlit Cloud (gratuit)

1. Aller sur https://share.streamlit.io
2. "New app" → connecter GitHub
3. Sélectionner :
   - Repository : `sahar-conseil`
   - Branch : `main`
   - Main file path : `apps/dvf_analyse/app.py`
4. Advanced settings → Secrets → coller le contenu de secrets.toml
5. "Deploy"

## Données DVF

Le fichier DVF est téléchargé automatiquement depuis data.gouv.fr au premier lancement.

Pour télécharger manuellement et éviter le délai au démarrage :
- Aller sur : https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/
- Télécharger le fichier CSV du département voulu
- Le placer dans `data/raw/dvf_{dept}.csv` (ex: `dvf_69.csv` pour le Rhône)

## Structure des fichiers

```
apps/dvf_analyse/
├── app.py                       # Application principale Streamlit
├── requirements.txt             # Dépendances Python
├── README.md                    # Ce fichier
└── .streamlit/
    ├── config.toml              # Thème et configuration Streamlit
    ├── secrets.toml             # Secrets locaux (gitignored)
    └── secrets.toml.example     # Template à copier
```

## Score d'opportunité

Le score 0–100 est calculé sur 3 critères pondérables :

| Critère | Poids défaut | Description |
|---------|-------------|-------------|
| Sous-valorisation | 40% | Bien moins cher que la médiane communale |
| Volume de transactions | 30% | Marché liquide = moins risqué |
| Dynamisme 12 mois | 30% | Zone avec transactions récentes |

> Score > 70 = Opportunité forte  
> Score 40–70 = À surveiller  
> Score < 40 = Marché tendu/mature

Les poids sont ajustables dans la sidebar.
