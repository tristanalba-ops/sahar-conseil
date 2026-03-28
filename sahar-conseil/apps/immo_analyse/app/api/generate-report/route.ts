import { NextRequest, NextResponse } from 'next/server';
import { getServerSession } from 'next-auth/next';
import { authOptions } from '@/lib/auth';
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(process.env.NEXT_PUBLIC_SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!);

interface BAN_Context { lat?: number; lon?: number; commune_id?: string; }
interface CloudFunctionResponse { success: boolean; data?: any; error?: string; }
interface ScoresResponse {
  individual_scores?: { market: number; economic: number; demographic: number; risk: number; accessibility: number; energy: number; };
  composite_scores?: { investment_opportunity: number; rental_yield_forecast: number; risk_assessment: number; bubble_index: number; };
  probabilities?: { price_increase_1y: number; price_increase_5y: number; recession: number; };
}

export async function POST(request: NextRequest) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user?.id) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });

    const body = await request.json();
    const { address, city, postcode, context } = body;
    if (!address || !city || !postcode) return NextResponse.json({ error: 'Missing required address fields' }, { status: 400 });

    let banContext: BAN_Context = {};
    if (context) { try { const match = context.match(/\((\d+)\)/); if (match) banContext.commune_id = match[1]; } catch (err) { console.error('Failed to parse BAN context:', err); } }
    banContext.lat = 48.8566; banContext.lon = 2.3522; banContext.commune_id = banContext.commune_id || '75056';

    const [scoresRes, statsRes, forecastRes] = await Promise.allSettled([
      callCloudFunction('score', { lat: banContext.lat, lon: banContext.lon }),
      callCloudFunction('stats', { commune_id: banContext.commune_id, period: '1y' }),
      callCloudFunction('forecast', { commune_id: banContext.commune_id, months: 12 }),
    ]);

    let scores: ScoresResponse = {};
    let stats = {};
    let forecast = {};
    if (scoresRes.status === 'fulfilled' && scoresRes.value.success) scores = scoresRes.value.data || {};
    if (statsRes.status === 'fulfilled' && statsRes.value.success) stats = statsRes.value.data || {};
    if (forecastRes.status === 'fulfilled' && forecastRes.value.success) forecast = forecastRes.value.data || {};

    let narrative = 'Analyse narrative indisponible pour le moment.';

    const reportData = { user_id: session.user.id, address, city, postcode, commune_id: banContext.commune_id, latitude: banContext.lat, longitude: banContext.lon, scores, stats, forecast, narrative, created_at: new Date().toISOString() };
    const { data: savedReport, error: saveError } = await supabase.from('reports').insert([reportData]).select('id').single();
    if (saveError) console.error('Failed to save report:', saveError);

    return NextResponse.json({ success: true, location: { address, commune_id: banContext.commune_id, lat: banContext.lat, lon: banContext.lon }, scores, stats, forecast, narrative, reportId: savedReport?.id });
  } catch (error) {
    console.error('Generate report error:', error);
    return NextResponse.json({ error: 'Failed to generate report.' }, { status: 500 });
  }
}

async function callCloudFunction(functionName: string, params: Record<string, any>): Promise<CloudFunctionResponse> {
  try {
    const baseUrl = process.env.NEXT_PUBLIC_GCP_CLOUD_FUNCTIONS_URL;
    if (!baseUrl) throw new Error('GCP_CLOUD_FUNCTIONS_URL not configured');
    const queryString = new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)])).toString();
    const url = `${baseUrl}/${functionName}?${queryString}`;
    const response = await fetch(url, { method: 'GET', headers: { 'Authorization': `Bearer ${process.env.GCP_CLOUD_FUNCTIONS_TOKEN}`, 'Content-Type': 'application/json' } });
    if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    const data = await response.json();
    return { success: true, data };
  } catch (error) {
    console.error(`Cloud function ${functionName} error:`, error);
    return { success: false, error: String(error) };
  }
}
