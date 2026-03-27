# SAHAR Content Factory

Pipeline automatique de génération d'articles SEO + podcasts.

## Architecture

```
run_pipeline.py          ← Orchestrateur principal
modules/
  01_research.py         ← SERP + PAA (ValueSERP / DuckDuckGo fallback)
  02_trends.py           ← Google Trends (PyTrends)
  03_scorer.py           ← Score intérêt 0-100
  04_writer.py           ← Génération article (Claude API)
  05_checker.py          ← Détection + correction ton IA
  06_audio.py            ← Script podcast + TTS ElevenLabs
  07_publisher.py        ← Publication GitHub Pages + GA4
config/
  keywords.json          ← Seeds mots-clés + maillage interne + CTAs
output/
  research/              ← JSON enrichis par module 01+02+03
  articles/              ← JSON articles générés
  audio/                 ← MP3 + scripts podcast
```

## Utilisation rapide

```bash
# Pipeline complet (tous les seeds)
python run_pipeline.py --all

# Un seul mot-clé
python run_pipeline.py --keyword "passoires thermiques 2025"

# Test sans publication (dry run jusqu'au score)
python run_pipeline.py --all --dry-run

# Sans génération audio
python run_pipeline.py --all --skip-audio

# Module par module
python modules/01_research.py --keyword "dvf immobilier"
python modules/02_trends.py   --all
python modules/03_scorer.py   --all
python modules/04_writer.py   --all --max 3
python modules/05_checker.py  --all
python modules/06_audio.py    --all --max 3
python modules/07_publisher.py --all --max 3
```

## Variables d'environnement

| Variable | Module | Obligatoire |
|----------|--------|-------------|
| `ANTHROPIC_API_KEY` | 04, 05, 06 | ✅ Oui |
| `GITHUB_TOKEN` (PAT) | 07 | ✅ Oui |
| `VALUESERP_KEY` | 01 | ⚠️ Optionnel (fallback DDG) |
| `ELEVENLABS_API_KEY` | 06 | ⚠️ Optionnel (script seul si absent) |
| `GA4_API_SECRET` | 07 | ⚠️ Optionnel |

## Secrets GitHub à configurer

Dans Settings → Secrets → Actions :
- `ANTHROPIC_API_KEY`
- `PAT_TOKEN` (Personal Access Token avec scope repo)
- `VALUESERP_KEY`
- `ELEVENLABS_API_KEY`
- `GA4_API_SECRET`

## Déclenchement automatique

Le workflow tourne chaque **lundi à 9h** (Paris).
Déclenchement manuel possible depuis GitHub Actions → Run workflow.

## Coût estimé par article

| Service | Coût |
|---------|------|
| Claude Sonnet (04+05) | ~0,05€ |
| ElevenLabs TTS (06) | ~0,10€ |
| ValueSERP (01) | ~0,01€ |
| **Total** | **~0,16€/article** |
