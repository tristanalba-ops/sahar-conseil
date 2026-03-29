# ImmoAnalyse — Analyse immobilière fullstack

Analyse probabiliste du marché immobilier avec données publiques (DVF, ADEME, INSEE, Banque de France).

## Stack

- **Frontend** : Next.js 16 (App Router) + React 19 + TypeScript
- **Backend** : Next.js API Routes + Supabase PostgreSQL
- **Auth** : NextAuth.js 4.24 + Supabase
- **IA** : Claude Anthropic API
- **Cloud** : GCP (Cloud Functions, BigQuery, Cloud Run)
- **Styling** : Tailwind CSS 3
- **APIs** : DVF, ADEME, INSEE, Banque de France, BAN

## Démarrage rapide

### Prerequisites
- Node.js 18+
- npm ou yarn

### Setup

```bash
# 1. Installer les dépendances
npm install

# 2. Créer le fichier .env.local
cp .env.example .env.local

# 3. Remplir les variables d'environnement
# NEXT_PUBLIC_SUPABASE_URL
# NEXT_PUBLIC_SUPABASE_ANON_KEY
# SUPABASE_SERVICE_ROLE_KEY
# NEXTAUTH_URL (http://localhost:3000)
# NEXTAUTH_SECRET (openssl rand -base64 32)
# NEXT_PUBLIC_ANTHROPIC_API_KEY

# 4. Lancer en développement
npm run dev
```

Accédez à http://localhost:3000

## Architecture

```
app/
├── components/          # Composants React réutilisables
├── api/                # Routes API Next.js
├── (auth)/             # Routes d'authentification
├── (app)/              # Routes protégées
│   ├── generate/       # Générateur de rapport
│   └── dashboard/      # Dashboard utilisateur
├── layout.tsx          # Root layout
├── page.tsx            # Page d'accueil
└── globals.css         # Styles globaux

lib/
├── auth.ts            # Config NextAuth
├── supabase.ts        # Client Supabase
├── dvf.ts             # Intégrateur DVF API
├── ademe.ts           # Intégrateur ADEME API
├── claude.ts          # Client Claude Anthropic
└── gcp.ts             # Clients GCP (Cloud Functions)

types/
└── index.ts           # Types TypeScript partagés
```

## Flux principal

1. **Authentification** → NextAuth + Supabase Auth
2. **Localisation** → BAN API autocomplete
3. **Requête données** → DVF + ADEME + INSEE + Banque de France
4. **Scoring** → Calcul multi-critères (marché, économie, démographie, risques)
5. **Prévisions** → Regression linéaire avec intervalles de confiance
6. **Rapport** → Génération narrative IA + visualisations
7. **Sauvegarde** → Stockage rapport dans Supabase + BigQuery

## Variables d'environnement

```env
# Supabase (required)
NEXT_PUBLIC_SUPABASE_URL=
NEXT_PUBLIC_SUPABASE_ANON_KEY=
SUPABASE_SERVICE_ROLE_KEY=

# NextAuth (required)
NEXTAUTH_URL=http://localhost:3000
NEXTAUTH_SECRET=

# Claude Anthropic (required)
NEXT_PUBLIC_ANTHROPIC_API_KEY=

# GCP Cloud Functions (optional, for production)
NEXT_PUBLIC_GCP_SCORE_FUNCTION_URL=
NEXT_PUBLIC_GCP_STATS_FUNCTION_URL=
NEXT_PUBLIC_GCP_FORECAST_FUNCTION_URL=

# BAN API (public, no key needed)
NEXT_PUBLIC_BAN_API_URL=https://api-adresse.data.gouv.fr
```

## Développement

```bash
# Lint
npm run lint

# Type check
npm run type-check

# Build
npm run build

# Production
npm start
```

## Documentation complète

Voir `/docs/immo-analyse-implementation.md` pour :
- Architecture détaillée
- Schéma Supabase complet
- Configuration NextAuth
- Intégration GCP Cloud Functions
- Déploiement sur Cloud Run

## License

MIT — SAHAR Conseil
