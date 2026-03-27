# SAHAR Content Factory

Pipeline automatisé de génération de contenu SEO.

## Architecture

```
content_factory/
├── config/
│   └── keywords.json          ← Seeds + maillage + CTAs + paramètres
├── modules/
│   ├── 01_research.py         ← SERP + PAA (ValueSERP ou DuckDuckGo)
│   ├── 02_trends.py           ← Google Trends (PyTrends)
│   ├── 03_scorer.py           ← Score intérêt 0-100
│   ├── 04_writer.py           ← Génération article (Claude API)
│   ├── 05_checker.py          ← Naturalité anti-IA + reformulation
│   ├── 06_audio.py            ← Script podcast + TTS ElevenLabs
│   └── 07_publisher.py        ← Publication GitHub Pages + GA4
├── output/
│   ├── research/              ← {slug}.json (SERP + PAA brutes)
│   ├── scored/                ← {slug}.json (+ score + décision)
│   ├── articles/              ← {slug}.json (article HTML + statut)
│   └── audio/                 ← {slug}.mp3 + {slug}_script.txt
└── run_pipeline.py            ← Orchestrateur principal
```

## Pipeline en 7 étapes

| # | Module | Entrée | Sortie | API |
|---|--------|--------|--------|-----|
| 1 | Research | keywords.json | output/research/*.json | ValueSERP (optionnel) |
| 2 | Trends | output/research/ | enrichit research/ | Google Trends (gratuit) |
| 3 | Scorer | output/research/ | output/scored/*.json | Aucune |
| 4 | Writer | output/scored/ | output/articles/*.json | Claude API |
| 5 | Checker | output/articles/ | met à jour articles/ | Claude API |
| 6 | Audio | output/articles/ | output/audio/*.mp3 | ElevenLabs |
| 7 | Publisher | output/articles/ | docs/blog/*.html | GitHub API |

## Configuration

### 1. Variables d'environnement

```bash
# Obligatoires pour la génération
export ANTHROPIC_API_KEY="sk-ant-..."

# Optionnel (fallback DuckDuckGo si absent)
export VALUESERP_KEY="..."

# Optionnel pour l'audio
export ELEVENLABS_API_KEY="..."

# Pour la publication auto
export GITHUB_TOKEN="ghp_..."
export GA4_API_SECRET="..."
```

### 2. Secrets GitHub Actions

Aller dans : **Settings → Secrets and variables → Actions → New secret**

| Secret | Obligatoire | Description |
|--------|-------------|-------------|
| `ANTHROPIC_API_KEY` | ✅ Oui | Clé API Claude |
| `ELEVENLABS_API_KEY` | ✅ Oui | Clé API audio |
| `VALUESERP_KEY` | ⚠️ Optionnel | SERP scraping |
| `GA4_API_SECRET` | ⚠️ Optionnel | Tracking GA4 |
| `GA4_MEASUREMENT_ID` | ⚠️ Optionnel | ID GA4 |

### 3. Ajouter des mots-clés

Modifier `config/keywords.json` :

```json
{
  "keywords": [
    {
      "seed": "votre mot-clé cible",
      "secteur": "immobilier",
      "cible": "agents immobiliers",
      "url_cible": "immobilier.html",
      "priorite": 1
    }
  ]
}
```

## Utilisation

### Lancement manuel complet

```bash
cd sahar-conseil/content_factory
python run_pipeline.py
```

### Un seul mot-clé

```bash
python run_pipeline.py --keyword "passoires thermiques 2025"
```

### Reprendre depuis une étape

```bash
python run_pipeline.py --from-step 4   # reprendre depuis la rédaction
python run_pipeline.py --from-step 7   # publier seulement
```

### Mode dry-run (sans API)

```bash
python run_pipeline.py --dry-run
```

### Script podcast sans audio

```bash
python run_pipeline.py --script-only   # génère scripts pour NotebookLM
```

### Lancer un module seul

```bash
cd modules
python 01_research.py --keyword "DVF immobilier"
python 03_scorer.py --report
python 04_writer.py --slug "dvf-immobilier" --dry
python 07_publisher.py --all
```

## Scheduler automatique

Le workflow GitHub Actions `content_factory.yml` se déclenche :
- **Automatiquement** : chaque lundi à 6h00 UTC
- **Manuellement** : onglet Actions → Content Factory → Run workflow

## Coûts estimés (20 articles/mois)

| Service | Usage | Coût |
|---------|-------|------|
| Claude API (Sonnet) | 20 articles × 2 passes | ~1€ |
| ElevenLabs | 20 épisodes × 1100 mots | ~2€ |
| ValueSERP | 20 recherches | ~0,03€ |
| GitHub Actions | 20 runs × 10 min | Gratuit |
| **Total** | | **~3€/mois** |

## Statuts des articles

| Statut | Description |
|--------|-------------|
| `draft` | Généré par le writer, pas encore vérifié |
| `reviewed` | Vérifié par le checker, prêt à publier |
| `published` | Publié sur GitHub Pages |
