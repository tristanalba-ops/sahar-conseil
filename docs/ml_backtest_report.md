# Back-Test OOT XGBoost Prix/m² — SAHAR

**Généré le :** 2026-04-03T12:17:44Z
**Modèle :** `XGBRegressor` — entraîné le 2026-04-02 sur 2,132,403 transactions (2022–2024)
**Période back-test :** 2025-01-01 → 2025-06-30 *(OOT : données jamais vues lors de l'entraînement)*
**Transactions évaluées :** 211,776

> **Note sur la période :** Le DVF France a un délai de publication de 12–18 mois.
> Les données >= 2025-10-01 ne sont pas encore publiées dans la base.
> Jan–Jun 2025 est la période la plus récente disponible et constitue un vrai
> back-test out-of-time (le modèle n'a jamais vu ces données).

---

## Métriques globales

| Indicateur | Back-test OOT | Baseline train |
|---|---|---|
| RMSE | **2,183.7 €/m²** | — |
| MAE | **1,824.9 €/m²** | 816.8 €/m² |
| R² | **0.076** | 0.731 |
| Erreur médiane | **63.34%** | ~26.8% (estimé) |
| Erreur P75 | 111.57% | — |
| Erreur P90 | 195.65% | — |

### Fourchettes de précision

| Fourchette | % transactions |
|---|---|
| ±10% | **7.5%** |
| ±20% | 15.0% |
| ±30% | 22.9% |

---

## Par type de bien

| Type | N | Erreur médiane | MAE | ±10% | ±20% | ±30% |
|---|---|---|---|---|---|---|
| Appartement | 78,276 | 49.8% | 1831 €/m² | 10.8% | 21.2% | 31.3% |
| Maison | 133,500 | 72.5% | 1821 €/m² | 5.5% | 11.4% | 18.0% |

---

## Par décile de prix réel

| Décile | Prix médian (€/m²) | Erreur médiane (%) | N |
|---|---|---|---|
| D1 | 795 | 233.5% | 21,184 |
| D10 | 7,966 | 15.9% | 21,177 |
| D2 | 1,290 | 125.8% | 21,172 |
| D3 | 1,667 | 97.1% | 21,178 |
| D4 | 2,000 | 81.6% | 21,177 |
| D5 | 2,353 | 70.1% | 21,177 |
| D6 | 2,738 | 60.8% | 21,220 |
| D7 | 3,214 | 53.0% | 21,189 |
| D8 | 3,853 | 45.2% | 21,124 |
| D9 | 4,911 | 32.9% | 21,178 |

---

## Couverture des features

| Catégorie | Features |
|---|---|
| Calculées depuis DVF (9/31) | surface, nb_pieces, type_local_encoded, surface_terrain, log_surface, ratio_terrain_bati, surface_par_piece, annee_vente, mois_vente |
| Issues de market_stats (3/31) | prix_zone_median, evolution_zone_12m, volume_zone |
| Dérivées (2/31) | prix_x_attractivite, ratio_revenu_prix |
| Imputées via scaler (17/31) | DPE zone, scores POI, socio-démographie commune |

---

## Analyse et interprétation

### Diagnostic : effondrement de performance en déploiement réel

Le R² chute de **0.731** (test in-distribution 2022-2024) à **0.076** en OOT 2025.
L'erreur médiane passe de ~27% estimé à **63%**. Ce n'est pas de la dérive temporelle —
c'est une défaillance de la **pipeline de features**.

**Cause racine : 17/31 features imputées à la moyenne nationale.**

En production réelle, les features DPE zone, POI et socio-démographie ne sont pas
disponibles directement depuis le DVF brut. Elles ont été imputées à la moyenne
d'entraînement (via `scaler.mean_`), ce qui détruit la capacité discriminante du modèle
pour les marchés en dehors de la moyenne nationale.

**Preuve par les déciles :**
- D10 (7 966 €/m² — Paris/Côte d'Azur) : erreur médiane **15.9%** → le modèle fonctionne
  car `prix_zone_median` (issue de market_stats) suffit à identifier ces marchés.
- D1 (795 €/m² — rural profond) : erreur médiane **233.5%** → le modèle suréstime
  massivement car les features socio-éco imputées à la moyenne nationale ne représentent
  pas ces zones.

**Conclusion : le modèle XGBoost ne doit PAS être utilisé en production sans pipeline
de features complète** (enrichissement socio-éco par commune, DPE zone, POI).

### Ce que le back-test valide
- Le modèle fonctionne sur les marchés premium (D8-D10, erreur 15-45%) avec features partielles.
- `prix_zone_median` issu de market_stats reste un signal fort et disponible.
- L'architecture XGBoost est saine — le problème est la disponibilité des features, pas le modèle.

### Plan de remédiation prioritaire

| Priorité | Action | Impact attendu |
|---|---|---|
| 🔴 P0 | Brancher `iris_demographics` → revenu, chômage, pauvreté par commune | −20 pts erreur médiane |
| 🔴 P0 | Brancher `dpe_commune_agg` → pct_passoire, conso_energie par zone | −5 pts |
| 🟠 P1 | `code_commune` target-encoded en cross-val fold | −10 pts |
| 🟠 P1 | `poi_scores_commune` → scores transport/education/sante | −5 pts |
| 🟡 P2 | Données Cadastre (état, étage) | −5 pts |

**Objectif réaliste post-remédiation P0+P1 : erreur médiane < 25%, R² > 0.65 en OOT.**

---

*Rapport généré automatiquement par `datamoat/ml/backtest.py`*
