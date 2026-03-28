// functions/forecast/index.js
// Cloud Function for price forecasting
// GET /forecast?commune_id=75056&months=12&confidence=0.80

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
 * Helper: Simple linear regression
 */
function linearRegression(points) {
  if (points.length < 2) return null;

  const n = points.length;
  const sumX = points.reduce((sum, p) => sum + p.x, 0);
  const sumY = points.reduce((sum, p) => sum + p.y, 0);
  const sumXY = points.reduce((sum, p) => sum + p.x * p.y, 0);
  const sumX2 = points.reduce((sum, p) => sum + p.x * p.x, 0);

  const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
  const intercept = (sumY - slope * sumX) / n;

  // Calculate R²
  const yMean = sumY / n;
  const ssTotal = points.reduce((sum, p) => sum + Math.pow(p.y - yMean, 2), 0);
  const ssRes = points.reduce((sum, p) => {
    const predicted = slope * p.x + intercept;
    return sum + Math.pow(p.y - predicted, 2);
  }, 0);
  const rSquared = 1 - (ssRes / ssTotal);

  return { slope, intercept, rSquared };
}

/**
 * Helper: Calculate confidence intervals
 */
function calculateConfidenceInterval(value, stddev, confidence = 0.95) {
  // Z-scores for common confidence levels
  const zScores = {
    0.80: 1.28,
    0.85: 1.44,
    0.90: 1.645,
    0.95: 1.96,
    0.99: 2.576
  };

  const z = zScores[confidence] || zScores[0.95];
  const margin = z * stddev;

  return {
    lower: value - margin,
    upper: value + margin,
    margin: margin
  };
}

/**
 * Helper: Get forecast confidence assessment
 */
function getConfidenceAssessment(rSquared, dataPoints, timeSpan) {
  let quality = 'Faible';
  let score = 20;

  if (rSquared > 0.8 && dataPoints > 24 && timeSpan > 24) {
    quality = 'Très élevée';
    score = 95;
  } else if (rSquared > 0.65 && dataPoints > 18 && timeSpan > 18) {
    quality = 'Élevée';
    score = 80;
  } else if (rSquared > 0.5 && dataPoints > 12) {
    quality = 'Modérée';
    score = 60;
  } else if (rSquared > 0.3 && dataPoints > 8) {
    quality = 'Acceptable';
    score = 40;
  }

  return { quality, score, modelQuality: rSquared };
}

/**
 * Helper: Generate forecast points using linear regression
 */
function generateForecast(historicalPoints, forecastMonths) {
  if (historicalPoints.length < 3) {
    return null;
  }

  const regression = linearRegression(historicalPoints);
  if (!regression) return null;

  const { slope, intercept, rSquared } = regression;
  const lastX = historicalPoints[historicalPoints.length - 1].x;

  const forecast = [];
  for (let i = 1; i <= forecastMonths; i++) {
    const x = lastX + i;
    const value = slope * x + intercept;
    forecast.push({
      month: i,
      value: Math.max(0, value), // Prevent negative prices
      x: x
    });
  }

  return { forecast, regression };
}

/**
 * Helper: Detect bubble using P/E ratio comparison
 */
function detectBubble(currentPrice, historicalAvg, economicGrowth = 0.02) {
  const fairPrice = historicalAvg * (1 + economicGrowth);
  const premiumRatio = currentPrice / fairPrice;

  let assessment = 'Normal';
  let score = 50;

  if (premiumRatio > 1.35) {
    assessment = 'Probable bubble';
    score = 90;
  } else if (premiumRatio > 1.20) {
    assessment = 'Possible surévaluation';
    score = 75;
  } else if (premiumRatio > 1.10) {
    assessment = 'Légèrement surévalué';
    score = 60;
  } else if (premiumRatio > 0.95) {
    assessment = 'Équilibré';
    score = 45;
  } else if (premiumRatio > 0.85) {
    assessment = 'Légèrement sous-évalué';
    score = 35;
  } else {
    assessment = 'Bien sous-évalué';
    score = 20;
  }

  return {
    assessment,
    score,
    premium_ratio: formatNumber(premiumRatio, 3),
    current_price: formatNumber(currentPrice),
    fair_value: formatNumber(fairPrice)
  };
}

/**
 * Main Cloud Function: Price Forecast
 */
functions.http('forecast', async (req, res) => {
  // CORS headers
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.status(204).send('');
    return;
  }

  try {
    const { commune_id, months = 12, confidence = 0.95 } = req.query;

    // Validation
    if (!commune_id) {
      return res.status(400).json({
        error: 'Missing commune_id parameter',
        example: '/forecast?commune_id=75056&months=12&confidence=0.95'
      });
    }

    const forecastMonths = Math.min(parseInt(months) || 12, 60); // Max 5 years
    const confidenceLevel = Math.min(Math.max(parseFloat(confidence) || 0.95, 0.80), 0.99);

    // Query historical price data (last 5 years)
    const query = `
      WITH price_history AS (
        SELECT
          DATE_TRUNC(transaction_date, MONTH) as month,
          DATE_DIFF(
            DATE_TRUNC(transaction_date, MONTH),
            DATE_TRUNC(MIN(transaction_date) OVER (), MONTH),
            MONTH
          ) as months_from_start,
          AVG(price) as avg_price,
          STDDEV(price) as stddev_price,
          COUNT(*) as transaction_count
        FROM \`immo-analyse-gcp.immo_data.dvf_transactions\`
        WHERE commune_id = @commune_id
          AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 MONTH)
        GROUP BY month
        ORDER BY month ASC
      ),
      stats AS (
        SELECT
          COUNT(*) as data_points,
          AVG(avg_price) as historical_avg,
          STDDEV(avg_price) as price_volatility,
          FIRST_VALUE(avg_price) OVER (ORDER BY month ASC) as start_price,
          LAST_VALUE(avg_price) OVER (ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as current_price,
          DATE_DIFF(MAX(month), MIN(month), MONTH) as time_span_months
        FROM price_history
      )
      SELECT
        ph.month,
        ph.months_from_start,
        ph.avg_price,
        ph.stddev_price,
        ph.transaction_count,
        s.data_points,
        s.historical_avg,
        s.price_volatility,
        s.start_price,
        s.current_price,
        s.time_span_months
      FROM price_history ph, stats s
      ORDER BY ph.month ASC
    `;

    const [rows] = await bigquery.query({
      query: query,
      params: {
        commune_id: commune_id
      },
      location: 'EU'
    });

    if (rows.length < 3) {
      return res.status(404).json({
        error: 'Insufficient historical data for forecast',
        commune_id: commune_id,
        min_required_months: 3,
        data_points: rows.length
      });
    }

    // Extract statistics
    const stats = rows[0];
    const historicalPoints = rows.map(row => ({
      x: row.months_from_start,
      y: row.avg_price
    }));

    // Generate forecast
    const forecastData = generateForecast(historicalPoints, forecastMonths);
    if (!forecastData) {
      return res.status(500).json({
        error: 'Failed to generate forecast'
      });
    }

    const { forecast, regression } = forecastData;

    // Calculate confidence intervals for forecast
    const forecastWithCI = forecast.map((point, idx) => {
      const ci = calculateConfidenceInterval(
        point.value,
        stats.price_volatility * (1 + idx * 0.05), // Increase uncertainty with distance
        confidenceLevel
      );
      return {
        month: point.month,
        date: new Date(new Date().setMonth(new Date().getMonth() + point.month)).toISOString().split('T')[0],
        forecast: formatNumber(point.value),
        lower_bound: formatNumber(Math.max(0, ci.lower)),
        upper_bound: formatNumber(ci.upper),
        confidence: confidenceLevel
      };
    });

    // Get confidence assessment
    const confidenceAssessment = getConfidenceAssessment(
      regression.rSquared,
      stats.data_points,
      stats.time_span_months
    );

    // Detect bubble
    const bubbleAssessment = detectBubble(
      stats.current_price,
      stats.historical_avg,
      regression.slope / stats.historical_avg // Economic growth rate from trend
    );

    // Calculate expected returns
    const expectedPrice = forecast[Math.min(11, forecast.length - 1)].value; // 12 months out
    const expectedReturn = expectedPrice > 0
      ? (expectedPrice - stats.current_price) / stats.current_price
      : 0;

    return res.json({
      success: true,
      commune_id: commune_id,
      forecast_summary: {
        current_price: formatNumber(stats.current_price),
        historical_avg: formatNumber(stats.historical_avg),
        forecast_horizon_months: forecastMonths,
        forecast_1y: forecast.length > 0 ? formatNumber(forecast[Math.min(11, forecast.length - 1)].value) : null,
        forecast_5y: forecast.length > 0 ? formatNumber(forecast[Math.min(59, forecast.length - 1)].value) : null,
        expected_return_1y_pct: formatNumber(expectedReturn * 100, 2),
        annual_growth_rate_pct: formatNumber(regression.slope / stats.historical_avg * 100, 2)
      },
      forecast_details: forecastWithCI,
      market_analysis: {
        historical_data_points: stats.data_points,
        time_span_months: stats.time_span_months,
        price_volatility_pct: formatNumber((stats.price_volatility / stats.historical_avg) * 100, 2),
        trend: regression.slope > 0 ? 'Hausse' : regression.slope < 0 ? 'Baisse' : 'Stable'
      },
      confidence: {
        ...confidenceAssessment,
        confidence_level: confidenceLevel,
        model_fit_r2: formatNumber(regression.rSquared, 3)
      },
      bubble_analysis: bubbleAssessment,
      metadata: {
        commune_id: commune_id,
        forecast_months: forecastMonths,
        confidence_level: confidenceLevel,
        generated_at: new Date().toISOString(),
        data_source: 'DVF (Demandes de Valeurs Foncières)',
        methodology: 'Linear regression with confidence intervals',
        disclaimer: 'Forecast is based on historical data and may not account for market shocks, policy changes, or economic disruptions.',
        notes: 'Prix en EUR. Résultats à titre indicatif. Consulter un professionnel avant décision d\'investissement.'
      }
    });

  } catch (error) {
    console.error('Forecast function error:', {
      message: error.message,
      code: error.code,
      stack: error.stack
    });

    return res.status(500).json({
      success: false,
      error: 'Failed to generate forecast',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});
