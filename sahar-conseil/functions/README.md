# ImmoAnalyse Cloud Functions

GCP Cloud Functions pour le scoring probabiliste et analyse immobilière.

## Functions

### 1. `/score` (HTTP)
Retourne les scores de probabilité pour une localisation donnée.

**Endpoint**: `GET /score?lat=48.8566&lon=2.3522`

**Response**:
```json
{
  "success": true,
  "location": {
    "latitude": 48.8566,
    "longitude": 2.3522,
    "commune_name": "Paris",
    "commune_id": "75056",
    "population": 2161000
  },
  "individual_scores": {
    "market": 75,
    "economic": 82,
    "demographic": 88,
    "risk": 45,
    "accessibility": 92,
    "energy": 56
  },
  "composite_scores": {
    "investment_opportunity": 78,
    "rental_yield_forecast": 6.2,
    "risk_assessment": 52,
    "gentrification_index": 15,
    "bubble_index": 65
  },
  "probabilities": {
    "price_increase_1y": 0.68,
    "price_increase_5y": 0.82,
    "recession": 0.15,
    "flood": 0.02,
    "good_rental_roi": 0.71
  }
}
```

### 2. `/stats` (HTTP)
Retourne les statistiques de marché pour une commune.

**Endpoint**: `GET /stats?commune_id=75056&period=1y`

**Periods**: `6m`, `1y`, `3y`, `5y`

### 3. `/forecast` (HTTP)
Génère un forecast de prix avec intervalles de confiance.

**Endpoint**: `GET /forecast?commune_id=75056&months=12&confidence=0.95`

## Déploiement

### Prérequis
- Google Cloud SDK (`gcloud` CLI)
- Authentification GCP configurée
- Variables d'environnement définies

### Déployer une function

```bash
gcloud functions deploy score \
  --gen2 \
  --runtime nodejs18 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point score \
  --source ./src \
  --set-env-vars SUPABASE_URL=...,SUPABASE_SERVICE_ROLE_KEY=...
```

### Variables d'environnement requises

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
GOOGLE_CLOUD_PROJECT=your-gcp-project
```

## Développement local

```bash
# Installer les dépendances
npm install

# Tester avec Functions Framework
npm install -g @google-cloud/functions-framework
functions-framework --target=score --debug --source=./src
```

Accédez à http://localhost:8080

## Architecture

Chaque function:
1. Valide les paramètres d'entrée
2. Requête Supabase si nécessaire (localisation, métadonnées)
3. Requête BigQuery pour les données calculées
4. Retourne JSON structuré avec CORS headers

## Monitoring

Via Cloud Logging:
```bash
gcloud functions logs read score --limit 50
```

## Documentation complète

Voir `/docs/gcp-implementation.md`
