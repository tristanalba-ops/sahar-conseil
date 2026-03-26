# 🚀 Guide de démarrage SAHAR Conseil

Tout ce qu'il faut faire pour passer de ce repo à une app en ligne,
même sans expérience technique.

---

## Étape 1 — Créer un compte GitHub (5 min)

1. Aller sur https://github.com
2. Cliquer "Sign up" et créer un compte gratuit
3. Vérifier votre email

---

## Étape 2 — Créer le repo GitHub (2 min)

1. Sur GitHub, cliquer le bouton vert **"New"** (ou aller sur github.com/new)
2. Remplir :
   - Repository name : `sahar-conseil`
   - Visibility : **Public** (nécessaire pour Streamlit Cloud gratuit)
   - Ne rien cocher d'autre
3. Cliquer **"Create repository"**

---

## Étape 3 — Uploader les fichiers (méthode sans terminal)

Si vous n'êtes pas à l'aise avec le terminal, GitHub permet d'uploader directement.

1. Sur votre repo GitHub vide, cliquer **"uploading an existing file"**
2. Glisser-déposer le dossier `sahar-conseil/` entier
3. En bas, écrire le message : `Initial commit — SAHAR Conseil`
4. Cliquer **"Commit changes"**

**Alternative avec terminal (recommandé) :**
```bash
# Dans un terminal, depuis le dossier sahar-conseil/
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/VOTRE_USERNAME/sahar-conseil.git
git branch -M main
git push -u origin main
```

---

## Étape 4 — Activer le site vitrine (3 min)

1. Sur votre repo GitHub, aller dans **Settings** (engrenage)
2. Dans le menu gauche : **Pages**
3. Source : **Deploy from a branch**
4. Branch : `main`, Folder : `/site`
5. Cliquer **Save**

→ Votre site sera en ligne sur : `https://VOTRE_USERNAME.github.io/sahar-conseil`

(Attendre 2–3 minutes la première fois)

---

## Étape 5 — Déployer l'app DVF sur Streamlit Cloud (5 min)

1. Aller sur https://share.streamlit.io
2. Cliquer **"Sign in with GitHub"**
3. Cliquer **"New app"**
4. Remplir :
   - Repository : `VOTRE_USERNAME/sahar-conseil`
   - Branch : `main`
   - Main file path : `apps/dvf_analyse/app.py`
5. Cliquer **"Advanced settings"**
6. Dans la zone **Secrets**, coller exactement :
   ```toml
   APP_PWD = "mon_mot_de_passe_choisi"
   ```
   (Remplacer par le mot de passe que vous voulez utiliser)
7. Cliquer **"Deploy!"**

→ L'app sera en ligne en 2–3 minutes sur une URL du type :
`https://VOTRE_USERNAME-sahar-conseil-apps-dvf-analyse-app-XXXX.streamlit.app`

---

## Étape 6 — Télécharger les données DVF (10 min)

L'app télécharge les données automatiquement, mais pour accélérer le démarrage :

1. Aller sur https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/
2. Télécharger le fichier du département qui vous intéresse (ex: `69.csv` pour le Rhône)
3. Le renommer `dvf_69.csv` (remplacer 69 par votre département)
4. L'uploader dans GitHub : `data/raw/dvf_69.csv`
5. Sur Streamlit Cloud, l'app redémarrera automatiquement

---

## Étape 7 — Tester et partager

1. Ouvrir l'URL de l'app Streamlit
2. Entrer le mot de passe configuré à l'étape 5
3. Sélectionner un département et explorer
4. Partager le lien avec 5 professionnels de votre réseau pour avoir des retours

---

## Checklist de lancement

- [ ] Compte GitHub créé
- [ ] Repo `sahar-conseil` créé (public)
- [ ] Fichiers uploadés sur GitHub
- [ ] Site vitrine GitHub Pages activé
- [ ] App DVF déployée sur Streamlit Cloud
- [ ] Données DVF chargées (au moins 1 département)
- [ ] Mot de passe configuré dans les secrets
- [ ] App testée et fonctionnelle
- [ ] Lien partagé avec 5 testeurs

---

## En cas de problème

**L'app ne démarre pas ?**
→ Aller sur Streamlit Cloud > votre app > "Logs" pour voir l'erreur

**Les données ne chargent pas ?**
→ Vérifier que le fichier CSV est bien dans `data/raw/dvf_{dept}.csv`
→ Vérifier la connexion internet de Streamlit Cloud (redémarrer l'app)

**Le site vitrine n'apparaît pas ?**
→ Attendre 5 minutes, vider le cache du navigateur
→ Vérifier Settings > Pages > la branche et le dossier

**Besoin d'aide ?**
→ Copier le message d'erreur et demander à Claude de le corriger
→ Claude peut lire les logs Streamlit et proposer un fix immédiat

---

## Prochaines étapes après le lancement

Une fois l'app DVF en ligne :

1. **Semaine 2** : Déployer DPE Scanner (`apps/dpe_scanner/app.py`)
2. **Semaine 3** : Créer la page Formspree pour le formulaire de contact du site vitrine
3. **Semaine 4** : Partager le lien à 20 professionnels LinkedIn dans votre secteur
4. **Mois 2** : Itérer sur les retours utilisateurs, activer les exports
