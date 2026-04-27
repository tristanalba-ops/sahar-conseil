# Frontend de conformité RGPD — Guide de déploiement

## Contenu de ce livrable

Trois fichiers prêts à intégrer sur `intentanalytics.fr` :

1. **`mes-donnees.html`** — page de gestion des droits RGPD pour les utilisateurs
2. **`opposition.html`** — page de retrait de logement pour les non-utilisateurs
3. **`consent-banner.html`** — bannière de consentement à intégrer dans toutes les pages

---

## Fichier 1 : mes-donnees.html

**À déployer sur :** `intentanalytics.fr/mes-donnees`

Selon ton architecture (GitHub Pages, Cloudflare Pages, ou autre), trois options :

- **Si servi en HTML statique :** déposer le fichier à la racine en tant que `mes-donnees.html` ou dans un sous-dossier `mes-donnees/index.html` selon ton schéma d'URLs.
- **Si CMS ou framework :** copier le contenu et l'adapter au layout existant si tu veux qu'il hérite du header/footer commun.

**Test après déploiement :** ouvrir `https://intentanalytics.fr/mes-donnees`, sélectionner un type de demande, saisir une adresse email valide, vérifier qu'un message de succès s'affiche. La demande sera tracée dans `gdpr.subject_request` que tu peux consulter via Supabase.

---

## Fichier 2 : opposition.html

**À déployer sur :** `intentanalytics.fr/opposition`

Mêmes options de déploiement que `mes-donnees.html`.

**Test après déploiement :** ouvrir `https://intentanalytics.fr/opposition`, saisir une adresse (par exemple ta propre adresse), vérifier que l'autocomplétion BAN propose des suggestions, sélectionner une adresse, ajouter un email et un motif optionnels, soumettre. La demande sera tracée dans `gdpr.opt_out_registry` et `gdpr.subject_request`.

---

## Fichier 3 : consent-banner.html (à intégrer dans CHAQUE page)

Ce fichier ne se déploie pas seul — son contenu doit être intégré dans toutes les pages publiques du site. La méthode dépend de ton architecture.

### Option A — Intégration manuelle dans chaque page

Pour chaque page HTML de ton site (carte.html, index.html, mes-donnees.html, opposition.html, et les trois pages /legal/*), copier le contenu de `consent-banner.html` (depuis `<style id="ia-consent-style">` jusqu'à `</script>`) et le coller juste avant la balise de fermeture `</body>`.

### Option B — Fichier externe partagé (recommandé)

Si ton site permet un fichier JavaScript partagé, créer un fichier `/consent-banner.js` qui injecte tout le HTML/CSS/JS au chargement. Ajouter ensuite dans toutes les pages :

```html
<script src="/consent-banner.js" defer></script>
```

### Lien "Modifier mes préférences cookies" dans le footer

Pour que les utilisateurs puissent revenir sur leur choix, ajouter dans le footer de chaque page :

```html
<a href="#" onclick="event.preventDefault(); iaConsentShow();">Modifier mes préférences cookies</a>
```

### Comportement de la bannière

- S'affiche au premier chargement si aucun choix n'a été enregistré
- Le choix est stocké dans `localStorage` (clé `ia_consent`)
- Le choix est tracé côté serveur dans `gdpr.consent_log` via RPC
- Si l'utilisateur a refusé, la fonction `window.iaTrackEvent()` devient un no-op pour le tracking analytics
- Le lien dans le footer permet de rouvrir la bannière à tout moment

---

## Autre — Activer 2 paramètres Supabase Auth (5 minutes)

### Leaked password protection

Lien direct : https://supabase.com/dashboard/project/wwvdpixzfaviaapixarb/auth/policies

Authentication → Policies → Password Strength → activer "Prevent use of leaked passwords (HaveIBeenPwned check)".

### Connection pooling percentage based

Lien direct : https://supabase.com/dashboard/project/wwvdpixzfaviaapixarb/database/pooling

Database → Connection Pooling → Pooler Settings → Pool Mode Strategy → "Percentage based".

---

## Politique de confidentialité existante : manques à signaler au DPO

L'audit automatique de ta politique de confidentialité actuelle révèle plusieurs absences à corriger lors de la revue avec le DPO :

- Aucune mention du DPO ou de son rôle
- Aucune mention des durées de conservation/rétention
- Sources de données détaillées manquantes : DVF, DPE, BDNB ne sont pas mentionnées explicitement
- Liste des sous-traitants absente (notamment Supabase comme hébergeur de données)
- Aucune référence à la CNIL ni aux modalités de plainte
- La notion de `ban_id` (clé d'agrégation principale) n'est pas évoquée alors qu'elle est centrale dans le traitement

Mentions présentes et conformes : consentement, intérêt légitime, droits utilisateurs (opposition, rectification, effacement, portabilité), cookies, BAN et ADEME mentionnés au moins une fois.

Ces manques justifient à eux seuls la revue par un DPO ou avocat RGPD spécialisé, qui pourra rédiger une version complète à partir de la DPIA et du LIA déjà fournis.

---

## Récapitulatif final côté backend (déjà en place, rien à faire)

Les fonctions RPC suivantes sont opérationnelles et accessibles via la clé anon publique :

- `POST /rest/v1/rpc/log_consent` — appelé automatiquement par la bannière
- `POST /rest/v1/rpc/request_data_access` — appelé par /mes-donnees
- `POST /rest/v1/rpc/request_opt_out` — appelé par /opposition

Toutes les demandes sont tracées dans :
- `gdpr.subject_request` — registre formel des demandes RGPD avec délai de réponse 30 jours automatique
- `gdpr.consent_log` — journal des consentements
- `gdpr.opt_out_registry` — registre d'opposition (effet immédiat)
- `gdpr.audit_trail` — audit de toutes les opérations sensibles

Tu peux suivre les demandes en cours via :

```sql
SELECT request_uuid, request_type, status, requested_at, due_at 
FROM gdpr.subject_request 
WHERE status NOT IN ('completed', 'expired', 'rejected')
ORDER BY requested_at DESC;
```
