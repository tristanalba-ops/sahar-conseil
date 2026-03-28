// functions/stats/index.js
// Cloud Function for market statistics
// GET /stats?commune_id=75056&period=6m|1y|5y

const functions = require('@google-cloud/functions-framework');
const { BigQuery } = require('@google-cloud/bigquery');

const bigquery = new BigQuery();

/**
 * Helper: Format number with proper decimal places
 */
function formatNumber(num, decimals = 2) {
  if (num === null || num === undefined) return null;
  return parseFloat(num.toFixed(decimals));
}

/**
 * Helper: Calculate compound annual growth rate
 */
function calculateCAGR(startValue, endValue, years) {
  if (startValue <= 0 || endValue <= 0 || years <= 0) return null;
  return formatNumber(Math.pow(endValue / startValue, 1 / years) - 1);
}

/**
 * Helper: Get market trend description
 */
function getTrendDescription(change) {
  if (change === null) return 'Données insuffisantes';
  if (change > 0.10) return 'Hausse forte';
  if (change > 0.05) return 'Hausse modérée';
  if (change > 0.02) return 'Légère hausse';
  if (change > -0.02) return 'Stable';
  if (change > -0.05) return 'Légère baisse';
  if (change > -0.10) return 'Baisse modérée';
  return 'Baisse forte';
}

/**
 * Helper: Get liquidity assessment
 */
function getLiquidityAssessment(transactionDensity, avgPrice) {
  if (avgPrice < 150000) {
    if (transactionDensity > 50) return { level: 'Très élevée', score: 95 };
    if (transactionDensity > 30) return { level: 'Élevée', score: 80 };
    if (transactionDensity > 15) return { level: 'Bonne', score: 65 };
    if (transactionDensity > 5) return { level: 'Modérée', score: 45 };
    return { level: 'Faible', score: 25 };
  }

  if (avgPrice < 400000) {
    if (transactionDensity > 30) return { level: 'Élevée', score: 85 };
    if (transactionDensity > 15) return { level: 'Bonne', score: 70 };
    if (transactionDensity > 8) return { level: 'Modérée', score: 50 };
    if (transactionDensity > 3) return { level: 'Faible', score: 30 };
    return { level: 'Très faible', score: 15 };
  }

  // Luxury segment
  if (transactionDensity > 10) return { level: 'Élevée', score: 80 };
  if (transactionDensity > 5) return { level: 'Modérée', score: 55 };
  if (transactionDensity > 2) return { level: 'Faible', score: 35 };
  return { level: 'Très faible', score: 15 };
}

/**
 * Helper: Get market concentration assessment
 */
function getConcentrationAssessment(q1, q3) {
  const ratio = (q3 - q1) / ((q3 + q1) / 2);
  if (ratio < 0.3) return { level: 'Très homogène', score: 90 };
  if (ratio < 0.5) return { level: 'Homogène', score: 75 };
  if (ratio < 0.8) return { level: 'Assez diversifié', score: 60 };
  if (ratio < 1.2) return { level: 'Diversifié', score: 45 };
  return { level: 'Très hétérogène', score: 25 };
}

/**
 * Main Cloud Function: Market Statistics
 */
functions.http('stats', async (req, res) => {
  // CORS headers
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.status(204).send('');
    return;
  }

  try {
    const { commune_id, period = '1y' } = req.query;

    // Validation
    if (!commune_id) {
      return res.status(400).json({
        error: 'Missing commune_id parameter',
        example: '/stats?commune_id=75056&period=1y'
      });
    }

    // Validate period
    const validPeriods = ['6m', '1y', '3y', '5y'];
    if (!validPeriods.includes(period)) {
      return res.status(400).json({
        error: `Invalid period. Must be one of: ${validPeriods.join(', ')}`,
        provided: period
      });
    }

    // Calculate date range based on period
    const monthsBack = {
      '6m': 6,
      '1y': 12,
      '3y': 36,
      '5y': 60
    }[period];

    const query = `
      WITH price_history AS (
        SELECT
          DATE_TRUNC(transaction_date, MONTH) as month,
          COUNT(*) as transaction_count,
          AVG(price) as avg_price,
          MIN(price) as min_price,
          MAX(price) as max_price,
          APPROX_QUANTILES(price, 4)[OFFSET(1)] as q1,
          APPROX_QUANTILES(price, 4)[OFFSET(2)] as median,
          APPROX_QUANTILES(price, 4)[OFFSET(3)] as q3,
          AVG(price_per_sqm) as avg_price_per_sqm,
          STDDEV(price) as stddev_price,
          AVG(surface_sqm) as avg_surface,
          COUNT(DISTINCT property_type) as property_types
        FROM \`immo-analyse-gcp.immo_data.dvf_transactions\`
        WHERE commune_id = @commune_id
          AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @months_back MONTH)
        GROUP BY month
        ORDER BY month DESC
      ),
      stats AS (
        SELECT
          COUNT(*) as data_points,
          AVG(avg_price) as avg_price_overall,
          STDDEV(avg_price) as stddev_price_trend,
          FIRST_VALUE(avg_price) OVER (ORDER BY month DESC) as current_avg_price,
          LAST_VALUE(avg_price) OVER (ORDER BY month DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as start_avg_price,
          MIN(min_price) as historical_min,
          MAX(max_price) as historical_max,
          AVG(transaction_count) as avg_transactions_per_month,
          AVG(avg_surface) as avg_surface_overall,
          FIRST_VALUE(median) OVER (ORDER BY month DESC) as current_median,
          LAST_VALUE(median) OVER (ORDER BY month DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as start_median
        FROM price_history
      )
      SELECT
        ph.month,
        ph.transaction_count,
        ph.avg_price,
        ph.min_price,
        ph.max_price,
        ph.q1,
        ph.median,
        ph.q3,
        ph.avg_price_per_sqm,
        ph.stddev_price,
        ph.avg_surface,
        ph.property_types,
        s.data_points,
        s.avg_price_overall,
        s.stddev_price_trend,
        s.current_avg_price,
        s.start_avg_price,
        s.historical_min,
        s.historical_max,
        s.avg_transactions_per_month,
        s.avg_surface_overall,
        s.current_median,
        s.start_median
      FROM price_history ph, stats s
      ORDER BY month DESC
    `;

    const [rows] = await bigquery.query({
      query: query,
      params: {
        commune_id: commune_id,
        months_back: monthsBack
      },
      location: 'EU'
    });

    if (rows.length === 0) {
      return res.status(404).json({
        error: 'No transaction data found for this commune',
        commune_id: commune_id,
        period: period
      });
    }

    // Extract statistics
    const latestData = rows[0];
    const startData = rows[rows.length - 1];

    const stats = {
      data_points: latestData.data_points,
      current: {
        avg_price: formatNumber(latestData.current_avg_price),
        median_price: formatNumber(latestData.current_median),
        avg_price_per_sqm: formatNumber(latestData.avg_price_per_sqm),
        transactions_per_month: latestData.avg_transactions_per_month,
        avg_surface_sqm: formatNumber(latestData.avg_surface_overall)
      },
      range: {
        min_price: formatNumber(latestData.historical_min),
        max_price: formatNumber(latestData.historical_max),
        price_range: formatNumber(latestData.historical_max - latestData.historical_min),
        q1: formatNumber(latestData.q1),
        q3: formatNumber(latestData.q3),
        iqr: formatNumber(latestData.q3 - latestData.q1)
      },
      trends: {
        price_change_pct: formatNumber(
          (latestData.current_avg_price - latestData.start_avg_price) / latestData.start_avg_price * 100
        ),
        cagr_pct: calculateCAGR(
          latestData.start_avg_price,
          latestData.current_avg_price,
          monthsBack / 12
        ),
        trend_description: getTrendDescription(
          (latestData.current_avg_price - latestData.start_avg_price) / latestData.start_avg_price
        ),
        volatility_pct: formatNumber((latestData.stddev_price_trend / latestData.avg_price_overall) * 100),
        volatility_level: latestData.stddev_price_trend / latestData.avg_price_overall > 0.15 ? 'Élevée' :
                         latestData.stddev_price_trend / latestData.avg_price_overall > 0.08 ? 'Modérée' : 'Faible'
      },
      liquidity: {
        transaction_density: formatNumber(latestData.avg_transactions_per_month / (monthsBack / 12)),
        ...getLiquidityAssessment(
          latestData.avg_transactions_per_month / (monthsBack / 12),
          latestData.current_avg_price
        )
      },
      market_concentration: {
        ...getConcentrationAssessment(latestData.q1, latestData.q3),
        iqr_ratio: formatNumber((latestData.q3 - latestData.q1) / ((latestData.q3 + latestData.q1) / 2))
      },
      property_diversity: {
        distinct_types: latestData.property_types,
        avg_surface_sqm: formatNumber(latestData.avg_surface_overall)
      },
      metadata: {
        period: period,
        months_analyzed: monthsBack,
        data_points: rows.length,
        commune_id: commune_id,
        last_updated: new Date().toISOString(),
        data_source: 'DVF (Demandes de Valeurs Foncières)',
        notes: 'Prix en EUR. Surface en m². Basé sur transactions immobilières officielles.'
      }
    };

    // Add historical trend if requested
    const historicalTrend = rows.map(row => ({
      date: row.month,
      price: formatNumber(row.avg_price),
      median: formatNumber(row.median),
      price_per_sqm: formatNumber(row.avg_price_per_sqm),
      transactions: row.transaction_count
    }));

    return res.json({
      success: true,
      commune_id: commune_id,
      period: period,
      stats: stats,
      historical_trend: historicalTrend
    });

  } catch (error) {
    console.error('Stats function error:', {
      message: error.message,
      code: error.code,
      stack: error.stack
    });

    return res.status(500).json({
      success: false,
      error: 'Failed to retrieve statistics',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});
