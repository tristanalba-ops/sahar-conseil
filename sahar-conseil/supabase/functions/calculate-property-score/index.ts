/**
 * SAHAR Conseil — calculate-property-score
 * Edge Function Supabase — Estimation immobilière hédonique
 *
 * Reçoit les caractéristiques d'un bien, retourne :
 * - estimated_price (€)
 * - price_per_sqm (€/m²)
 * - price_min / price_max (intervalle confiance 90%)
 * - confidence_score (0-100)
 * - margin_of_error (%)
 * - num_comparables (int)
 * - scoring_breakdown { address_score, environmental_score, condition_score, market_score }
 * - valeur_verte (si DPE mauvais)
 *
 * Compatible avec PropertyValueSimulator.jsx
 */

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

// ─── CORS ─────────────────────────────────────────────────────────────────

const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

// ─── Modèle hédonique (coefficients pré-calibrés ADEME/DVF 2020-2024) ──────

const COEFS = {
  intercept:           3200.0,
  surface:               -5.5,   // effet taille (décroissant)
  dpe_score:             12.0,   // €/m² par point DPE
  score_localisation:    18.0,   // €/m² par point POI
  evolution_12m:         80.0,   // €/m² par % de croissance
  type_maison:          200.0,   // prime maison vs appartement
};

const RMSE = 650.0;  // marge d'erreur résiduelle pré-calibrée

const DPE_SCORE: Record<string, number> = {
  A: 100, B: 85, C: 65, D: 45, E: 25, F: 10, G: 0,
};

const DPE_IMPACT_PCT: Record<string, number> = {
  A:  0.08, B:  0.05, C:  0.02, D:  0.00,
  E: -0.04, F: -0.07, G: -0.14,
};

const CONDITION_BONUS: Record<string, number> = {
  mauvais:  -300,
  moyen:    -150,
  bon:          0,
  tres_bon:   150,
  excellent:  300,
};

// ─── Géocodage BAN ────────────────────────────────────────────────────────

async function geocoderBAN(adresse: string): Promise<{
  lat: number; lon: number; cp: string; dept: string; score: number
} | null> {
  try {
    const url = `https://api-adresse.data.gouv.fr/search/?q=${encodeURIComponent(adresse)}&limit=1`;
    const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
    const data = await res.json();
    const feature = data?.features?.[0];
    if (!feature) return null;
    const [lon, lat] = feature.geometry.coordinates;
    const cp = feature.properties.postcode ?? "";
    return { lat, lon, cp, dept: cp.substring(0, 2), score: feature.properties.score ?? 0 };
  } catch {
    return null;
  }
}

// ─── Score POI (Overpass OSM) ─────────────────────────────────────────────

async function scorePOI(lat: number, lon: number, rayon = 500): Promise<number> {
  try {
    const cats = [
      `node["public_transport"](around:${rayon},${lat},${lon});`,
      `node["highway"="bus_stop"](around:${rayon},${lat},${lon});`,
      `node["amenity"~"supermarket|bakery|convenience"](around:${rayon},${lat},${lon});`,
      `node["amenity"~"school|college|kindergarten"](around:${rayon},${lat},${lon});`,
      `node["amenity"~"pharmacy|hospital|clinic"](around:${rayon},${lat},${lon});`,
    ].join("\n");

    const query = `[out:json][timeout:6]; (${cats}); out count;`;
    const res = await fetch("https://overpass-api.de/api/interpreter", {
      method:  "POST",
      body:    new URLSearchParams({ data: query }),
      signal:  AbortSignal.timeout(8000),
    });
    const data = await res.json();
    const total = parseInt(data?.elements?.[0]?.tags?.total ?? "10", 10);
    // Normaliser 0–30 POI → 0–100
    return Math.min(100, Math.round((total / 30) * 100));
  } catch {
    return 50; // fallback neutre
  }
}

// ─── KPIs DVF (API Cerema) ────────────────────────────────────────────────

async function getKPIsDVF(dept: string): Promise<{
  prix_median: number; evolution_12m: number; volume: number;
}> {
  try {
    const url = `https://apidf-preprod.cerema.fr/dvf_opendata/mutations?code_departement=${dept}&anneemut_min=${new Date().getFullYear() - 2}&page_size=500`;
    const res = await fetch(url, { signal: AbortSignal.timeout(10000) });
    const data = await res.json();
    const transactions = data?.results ?? [];

    if (transactions.length < 5) {
      return { prix_median: 3500, evolution_12m: 2.0, volume: 0 };
    }

    const prices: number[] = transactions
      .map((t: Record<string, unknown>) => {
        const val = Number(t.valeurfonc ?? t.valeur_fonciere ?? 0);
        const srf = Number(t.sbati ?? t.surface_reelle_bati ?? 0);
        return srf > 10 ? val / srf : 0;
      })
      .filter((p: number) => p > 200 && p < 25000);

    prices.sort((a, b) => a - b);
    const median = prices[Math.floor(prices.length / 2)] ?? 3500;

    // Évolution approximative : comparer première moitié / deuxième moitié
    const half = Math.floor(prices.length / 2);
    const medianOld = prices[Math.floor(half / 2)] ?? median;
    const evolution = medianOld > 0
      ? ((median - medianOld) / medianOld) * 100
      : 2.0;

    return {
      prix_median:   Math.round(median),
      evolution_12m: Math.round(evolution * 10) / 10,
      volume:        transactions.length,
    };
  } catch {
    return { prix_median: 3500, evolution_12m: 2.0, volume: 0 };
  }
}

// ─── Modèle hédonique ──────────────────────────────────────────────────────

function estimer(
  surface: number,
  dpeLabel: string,
  scoreLoc: number,
  evolution: number,
  typeBien: string,
  prixMedianCommune: number | null,
  etat: string,
  hasParking: boolean,
  hasBalcony: boolean,
  hasGarden:  boolean,
  hasView:    boolean,
): {
  prix_m2: number;
  valeur:  number;
  ic_bas:  number;
  ic_haut: number;
  fiabilite: number;
  decomposition: Record<string, number>;
} {
  const dpeScore   = DPE_SCORE[dpeLabel.toUpperCase()] ?? 45;
  const isMaison   = typeBien.toLowerCase() === "maison" ? 1 : 0;
  const condBonus  = CONDITION_BONUS[etat] ?? 0;
  const optBonus   = (hasParking ? 8000 : 0) + (hasBalcony ? 4000 : 0)
                   + (hasGarden  ? 6000 : 0) + (hasView    ? 5000 : 0);
  const optBonusM2 = optBonus / 65;

  let prix_m2 =
    COEFS.intercept
    + COEFS.surface           * surface
    + COEFS.dpe_score         * dpeScore
    + COEFS.score_localisation * scoreLoc
    + COEFS.evolution_12m     * evolution
    + COEFS.type_maison       * isMaison
    + condBonus
    + optBonusM2;

  // Ancrage sur prix médian communal
  if (prixMedianCommune) {
    const baseRef =
      COEFS.intercept
      + COEFS.surface           * 65
      + COEFS.dpe_score         * 45
      + COEFS.score_localisation * 50
      + COEFS.evolution_12m     * 0
      + COEFS.type_maison       * 0;
    const delta = prix_m2 - baseRef - condBonus - optBonusM2;
    prix_m2 = prixMedianCommune + delta + condBonus + optBonusM2;
  }

  prix_m2 = Math.max(500, Math.min(25000, prix_m2));
  const valeur  = Math.round(prix_m2 * surface);
  const marge   = RMSE * 1.65;
  const ic_bas  = Math.max(200, prix_m2 - marge);
  const ic_haut = prix_m2 + marge;

  const fiabilite = prixMedianCommune ? 70 : 55;

  const decomposition = {
    base:              Math.round(COEFS.intercept + (prixMedianCommune ? prixMedianCommune - COEFS.intercept : 0)),
    effet_surface:     Math.round(COEFS.surface * surface),
    effet_dpe:         Math.round(COEFS.dpe_score * dpeScore),
    effet_localisation: Math.round(COEFS.score_localisation * scoreLoc),
    effet_marche:      Math.round(COEFS.evolution_12m * evolution),
    effet_type:        Math.round(COEFS.type_maison * isMaison),
    effet_etat:        Math.round(condBonus),
    effet_options:     Math.round(optBonusM2),
  };

  return { prix_m2: Math.round(prix_m2), valeur, ic_bas: Math.round(ic_bas), ic_haut: Math.round(ic_haut), fiabilite, decomposition };
}

// ─── Valeur verte ──────────────────────────────────────────────────────────

function calcValeurVerte(dpeActuel: string, dpeCible: string, surface: number, prixM2: number) {
  const iA = DPE_IMPACT_PCT[dpeActuel.toUpperCase()] ?? 0;
  const iC = DPE_IMPACT_PCT[dpeCible.toUpperCase()] ?? 0;
  const gainPct    = (iC - iA) * 100;
  const prixBaseD  = prixM2 / (1 + iA);
  const gainM2     = prixBaseD * (iC - iA);
  const gainTotal  = Math.round(gainM2 * surface);
  const coutReno   = surface * Math.max(1, ["G","F","E","D","C","B","A"].indexOf(dpeCible.toUpperCase()) - ["G","F","E","D","C","B","A"].indexOf(dpeActuel.toUpperCase())) * 350;
  return {
    dpe_actuel:        dpeActuel,
    dpe_cible:         dpeCible,
    gain_pct:          Math.round(gainPct * 10) / 10,
    gain_m2:           Math.round(gainM2),
    gain_total:        gainTotal,
    valeur_avant:      Math.round(prixM2 * surface),
    valeur_apres:      Math.round((prixM2 + gainM2) * surface),
    cout_reno_estime:  Math.round(coutReno),
    roi_renovation:    Math.round(gainTotal / Math.max(1, coutReno) * 100) / 100,
  };
}

// ─── Handler principal ────────────────────────────────────────────────────

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: CORS });
  }

  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "POST requis" }), {
      status: 405, headers: { ...CORS, "Content-Type": "application/json" },
    });
  }

  let body: Record<string, unknown>;
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "JSON invalide" }), {
      status: 400, headers: { ...CORS, "Content-Type": "application/json" },
    });
  }

  // Extraction des champs
  const surface    = Number(body.surface ?? 75);
  const dpe        = String(body.dpe_rating ?? body.dpe ?? "D").toUpperCase();
  const typeBien   = String(body.property_type ?? "Appartement");
  const etat       = String(body.property_condition ?? "bon");
  const hasParking = Boolean(body.has_parking);
  const hasBalcony = Boolean(body.has_balcony);
  const hasGarden  = Boolean(body.has_garden);
  const hasView    = Boolean(body.has_view);
  const adresse    = String(body.address ?? "");
  const cp         = String(body.postal_code ?? "");

  // — Géocodage
  const geoQuery = adresse || cp;
  let geo = null;
  if (geoQuery) {
    geo = await geocoderBAN(geoQuery);
  }

  // — KPIs
  const [scoreLoc, kpisDVF] = await Promise.all([
    geo ? scorePOI(geo.lat, geo.lon) : Promise.resolve(50),
    geo?.dept ? getKPIsDVF(geo.dept) : Promise.resolve({ prix_median: 3500, evolution_12m: 2.0, volume: 0 }),
  ]);

  // — Estimation
  const est = estimer(
    surface, dpe, scoreLoc, kpisDVF.evolution_12m,
    typeBien, kpisDVF.prix_median, etat,
    hasParking, hasBalcony, hasGarden, hasView,
  );

  // — Valeur verte
  let valeurVerte = null;
  if (["E", "F", "G"].includes(dpe)) {
    valeurVerte = calcValeurVerte(dpe, "C", surface, est.prix_m2);
  }

  // — Score de comparables (proxy : volume DVF)
  const numComparables = Math.min(500, Math.max(5, kpisDVF.volume));

  // — Scoring breakdown (0 → 1) pour PropertyValueSimulator.jsx
  const scoringBreakdown = {
    address_score:       (scoreLoc / 100) * (geo?.score ?? 0.8),
    environmental_score: (DPE_SCORE[dpe] ?? 45) / 100,
    condition_score:     (CONDITION_BONUS[etat] + 300) / 600,
    market_score:        Math.min(1, Math.max(0, (kpisDVF.evolution_12m + 5) / 15)),
  };

  const marginOfError = Math.round((RMSE / est.prix_m2) * 100 * 10) / 10;

  const response = {
    // Champs attendus par PropertyValueSimulator.jsx
    estimated_price:   est.valeur,
    price_per_sqm:     est.prix_m2,
    price_min:         Math.round(est.ic_bas * surface),
    price_max:         Math.round(est.ic_haut * surface),
    confidence_score:  est.fiabilite,
    margin_of_error:   marginOfError,
    num_comparables:   numComparables,
    scoring_breakdown: scoringBreakdown,
    // Données enrichies SAHAR
    evolution_12m:     kpisDVF.evolution_12m,
    score_localisation: scoreLoc,
    decomposition:     est.decomposition,
    valeur_verte:      valeurVerte,
    geo:               geo,
    date_calcul:       new Date().toISOString(),
  };

  return new Response(JSON.stringify(response), {
    status: 200,
    headers: { ...CORS, "Content-Type": "application/json" },
  });
});
