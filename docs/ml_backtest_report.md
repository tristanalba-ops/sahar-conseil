# Back-Test OOT XGBoost Prix/m² — Intent Analytics v2

**Généré le :** 2026-04-03T12:46:46Z
**Version :** v2 — jointures réelles sur tables Supabase (iris_demographics, dpe_commune_agg, poi_scores_commune, data_demographie_commune)
**Modèle :** `XGBRegressor` — entraîné le 2026-04-03 sur 238,405 transactions (2022–2024)
**Période back-test :** 2025-01-01 → 2025-06-30 *(OOT : données jamais vues lors de l'entraînement)*
**Transactions évaluées :** 212,241

> **Note sur la période :** Le DVF France a un délai de publication de 12–18 mois.
> Les données >= 2025-10-01 ne sont pas encore publiées dans la base.
> Jan–Jun 2025 est la période la plus récente disponible et constitue un vrai
> back-test out-of-time (le modèle n'a jamais vu ces données).

---

## Objectifs

| Objectif | Valeur cible | Résultat | Statut |
|---|---|---|---|
| R² OOT | > 0.50 | **0.663** | ✅ |
| Erreur médiane | < 35% | **21.35%** | ✅ |

---

## Métriques globales

| Indicateur | Back-test OOT v2 | Baseline train |
|---|---|---|
| RMSE | **1,297.9 €/m²** | — |
| MAE | **849.7 €/m²** | 781.8 €/m² |
| R² | **0.663** | 0.696 |
| Erreur médiane | **21.35%** | ~24.4% (estimé) |
| Erreur P75 | 42.37% | — |
| Erreur P90 | 84.33% | — |

### Fourchettes de précision

| Fourchette | % transactions |
|---|---|
| ±10% | **25.8%** |
| ±20% | 47.5% |
| ±30% | 63.1% |

---

## Par type de bien

| Type | N | Erreur médiane | MAE | ±10% | ±20% | ±30% |
|---|---|---|---|---|---|---|
| Appartement | 78,661 | 19.2% | 997 €/m² | 28.1% | 51.5% | 67.6% |
| Maison | 133,580 | 22.8% | 763 €/m² | 24.4% | 45.1% | 60.4% |

---

## Par décile de prix réel

| Décile | Prix médian (€/m²) | Erreur médiane (%) | N |
|---|---|---|---|
| D1 | 806 | 100.8% | 21,247 |
| D10 | 7,890 | 18.7% | 21,220 |
| D2 | 1,297 | 39.3% | 21,218 |
| D3 | 1,667 | 25.2% | 21,244 |
| D4 | 2,000 | 20.1% | 21,215 |
| D5 | 2,356 | 17.6% | 21,242 |
| D6 | 2,742 | 16.5% | 21,186 |
| D7 | 3,217 | 15.9% | 21,228 |
| D8 | 3,853 | 16.1% | 21,215 |
| D9 | 4,902 | 15.7% | 21,226 |

---

## Couverture des features (v2 — jointures réelles)

| Source | Features | Couverture |
|---|---|---|
| DVF mutations (9/32) | surface, nb_pieces, type_local_encoded, log_surface, ratio_terrain_bati, surface_par_piece, annee_vente, mois_vente | 100% |
| market_stats (3/32) | prix_zone_median, evolution_zone_12m, volume_zone | 100.0% |
| iris_demographics (5/32) | revenu_median, taux_chomage, part_proprietaires, part_cadres, taux_pauvrete | **94.8%** ✨ |
| dpe_commune_agg (3/32) | pct_passoire_zone, conso_energie_zone, score_energie_zone | **99.9%** ✨ |
| poi_scores_commune (5/32) | score_transport, score_education, score_sante, score_commerce, score_poi_global | **87.8%** ✨ |
| data_demographie_commune (4/32) | population_commune, superficie_km2, densite_pop_km2, log_population | **100.0%** ✨ |
| Dérivées (2/32) | prix_x_attractivite, ratio_revenu_prix | 100% |
| Target encoding (1/32) | code_commune_te | **100%** ✨ |

> **v1 → v2** : 17/31 features précédemment imputées à la constante nationale sont maintenant
> enrichies par jointure réelle sur les tables Supabase. Impact attendu : R² OOT significativement amélioré.

---

## Analyse et interprétation

### Stabilité temporelle
Le modèle généralisé à des données 2025 montre un R² de **0.663** (vs 0.696 sur le test in-distribution 2022–2024).
Excellente stabilité temporelle.

### Améliorations v2 vs v1
- **Socio-éco commune** (iris_demographics) : revenu médian, taux chômage, part propriétaires — couverture 94.8% vs 0% (v1)
- **DPE zone** (dpe_commune_agg) : pct passoires, conso énergie, score énergie — couverture 99.9% vs 0% (v1)
- **POI scores** (poi_scores_commune) : transport, éducation, santé, commerce — couverture 87.8% vs 0% (v1)
- **Démographie** (data_demographie_commune) : population, densité — couverture 100.0% vs 0% (v1)

### Limitations structurelles
- Pas de latitude/longitude précise → micro-localisation perdue
- Pas de données état du bien / étage / rénovation
- code_commune_te : target-encoding opérationnel ✓

### Prochaines étapes
1. 
2. Ajouter coordonnées GPS (latitude/longitude) depuis BAN pour géolocalisation précise
3. Intégrer données Cadastre (état du bien, étage) — absent du DVF standard

---

*Rapport généré automatiquement par `datamoat/ml/backtest.py` v2*
