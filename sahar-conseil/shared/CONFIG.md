# SAHAR Conseil — Guide de configuration

## 1. Supabase (CRM persistant)

### Créer le schema
1. Aller sur https://supabase.com → votre projet
2. SQL Editor → coller le contenu de `supabase_setup.sql` → Run

### Récupérer les credentials
- Settings → API
- Copier : **Project URL** et **anon public key**

### Configurer Streamlit Cloud
Settings → Secrets → ajouter :
```toml
SUPABASE_URL = "https://xxxx.supabase.co"
SUPABASE_KEY = "eyJxxx..."
```

---

## 2. Brevo (emails + SMS automatiques)

### Créer un compte
https://www.brevo.com — gratuit jusqu'à 300 emails/jour

### Récupérer la clé API
Account → SMTP & API → API Keys → Générer

### Configurer Streamlit Cloud
```toml
BREVO_API_KEY = "xkeysib-..."
BREVO_SENDER_EMAIL = "contact@sahar-conseil.fr"
BREVO_SENDER_NAME = "SAHAR Conseil"
NOTIF_EMAIL = "contact@sahar-conseil.fr"
APP_URL = "https://sahar-conseil.fr"
```

---

## 3. Secrets complets (tout en un)

Copier ce bloc dans Streamlit Cloud → Settings → Secrets :

```toml
# Accès app
APP_PWD = "votre_mot_de_passe"

# Supabase
SUPABASE_URL = "https://xxxx.supabase.co"
SUPABASE_KEY = "eyJxxx..."

# Brevo
BREVO_API_KEY = "xkeysib-..."
BREVO_SENDER_EMAIL = "contact@sahar-conseil.fr"
BREVO_SENDER_NAME = "SAHAR Conseil"
NOTIF_EMAIL = "contact@sahar-conseil.fr"
APP_URL = "https://sahar-conseil.fr"
```

---

## 4. Domaine (sahar-conseil.fr)

### DNS Hostinger (déjà fait ✓)
- 4 enregistrements A → IPs GitHub Pages
- CNAME www → tristanalba-ops.github.io
- TXT _github-pages-challenge-tristanalba-ops → token

### GitHub Pages
Repo → Settings → Pages → Custom domain → sahar-conseil.fr

### Après validation
Me donner le signal → je mets à jour toutes les URLs du site.

---

## 5. Séquences email — fonctionnement

| Événement | Séquence déclenchée |
|-----------|---------------------|
| Lead formulaire site | Email bienvenue J+0 |
| Secteur Immobilier | SEQ001 : J+0, J+3, J+7 |
| Secteur Énergie | SEQ002 : J+0 email + J+4 SMS |
| Secteur Retail | SEQ003 : J+0 email |

Les étapes J+3, J+7 nécessitent un scheduler (cron).
Pour l'instant : J+0 automatique, relances manuelles depuis le CRM.

---

## 6. Vérification que tout fonctionne

Dans l'app → onglet CRM → créer un contact test → 
l'email de bienvenue doit arriver dans votre boîte.
