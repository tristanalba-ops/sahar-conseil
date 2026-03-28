// Cloud Function: Score Index
// Endpoint: GET /score?lat=48.8566&lon=2.3522
// Returns: Comprehensive probability scores for a location

const functions = require('@google-cloud/functions-framework');
const { createClient } = require('@supabase/supabase-js');
const { BigQuery } = require('@google-cloud/bigquery');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const bigquery = new BigQuery();

functions.http('score', async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.status(204).send('');
    return;
  }

  try {
    const { lat, lon } = req.query;

    if (!lat || !lon) {
      return res.status(400).json({
        error: 'Missing lat or lon parameter',
        example: '/score?lat=48.8566&lon=2.3522'
      });
    }

    const latitude = parseFloat(lat);
    const longitude = parseFloat(lon);

    // Find nearest commune
    const { data: commune, error: communeError } = await supabase.rpc(
      'find_nearest_commune',
      {
        input_lat: latitude,
        input_lon: longitude
      }
    );

    if (communeError || !commune) {
      return res.status(404).json({
        error: 'No commune found for location',
        lat, lon
      });
    }

    // Query BigQuery for scores
    const query = `
      SELECT *
      FROM \`immo-analyse-gcp.immo_data.probability_scores\`
      WHERE commune_id = @commune_id
      ORDER BY calculation_date DESC
      LIMIT 1
    `;

    const [rows] = await bigquery.query({
      query,
      params: { commune_id: commune.code_commune },
      location: 'EU'
    });

    if (rows.length === 0) {
      return res.status(404).json({
        error: 'No score data available',
        commune_id: commune.code_commune
      });
    }

    const scores = rows[0];

    return res.json({
      success: true,
      location: {
        latitude,
        longitude,
        commune_name: commune.nom_commune,
        commune_id: commune.code_commune,
        population: commune.population
      },
      individual_scores: {
        market: scores.score_market,
        economic: scores.score_economic,
        demographic: scores.score_demographic,
        risk: scores.score_risk,
        accessibility: scores.score_accessibility,
        energy: scores.score_energy
      },
      composite_scores: {
        investment_opportunity: scores.composite_investment_opportunity,
        rental_yield_forecast: scores.composite_rental_yield_forecast,
        risk_assessment: scores.composite_risk_assessment,
        gentrification_index: scores.composite_gentrification_index,
        bubble_index: scores.composite_bubble_index
      },
      probabilities: {
        price_increase_1y: scores.prob_price_increase_1y,
        price_increase_5y: scores.prob_price_increase_5y,
        recession: scores.prob_recession,
        flood: scores.prob_flood,
        good_rental_roi: scores.prob_good_rental_roi
      },
      metadata: {
        calculation_date: scores.calculation_date,
        data_sources: scores.metadata_sources,
        confidence: scores.metadata_confidence,
        last_updated: new Date().toISOString()
      }
    });

  } catch (error) {
    console.error('Score function error:', error);
    return res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});
