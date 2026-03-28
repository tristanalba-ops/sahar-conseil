'use client';

import { RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

interface ReportDisplayProps {
  report: {
    location: { address: string; commune_id: string; lat: number; lon: number; };
    scores: {
      individual_scores?: { market: number; economic: number; demographic: number; risk: number; accessibility: number; energy: number; };
      composite_scores?: { investment_opportunity: number; rental_yield_forecast: number; risk_assessment: number; bubble_index: number; };
      probabilities?: { price_increase_1y: number; price_increase_5y: number; recession: number; };
    };
    stats?: { avg_price_per_sqm?: number; median_rent?: number; vacancy_rate?: number; };
    forecast?: { months?: Array<{ month: number; price: number; upper_bound: number; lower_bound: number; }>; };
    narrative: string;
    reportId?: string;
  };
}

export default function ReportDisplay({ report }: ReportDisplayProps) {
  const { location, scores, stats, forecast, narrative } = report;
  const investmentScore = Math.round(scores.composite_scores?.investment_opportunity || 0);
  const rentalYield = (scores.composite_scores?.rental_yield_forecast || 0).toFixed(1);
  const riskScore = Math.round(scores.composite_scores?.risk_assessment || 0);
  const bubbleIndex = Math.round(scores.composite_scores?.bubble_index || 0);

  const radarData = [
    { name: 'Marche', value: Math.round(scores.individual_scores?.market || 0), fullMark: 100 },
    { name: 'Economie', value: Math.round(scores.individual_scores?.economic || 0), fullMark: 100 },
    { name: 'Demographie', value: Math.round(scores.individual_scores?.demographic || 0), fullMark: 100 },
    { name: 'Risque', value: Math.round(scores.individual_scores?.risk || 0), fullMark: 100 },
    { name: 'Accessibilite', value: Math.round(scores.individual_scores?.accessibility || 0), fullMark: 100 },
    { name: 'Energie', value: Math.round(scores.individual_scores?.energy || 0), fullMark: 100 },
  ];

  const forecastData = (forecast?.months || []).map((month: any) => ({
    month: `M${month.month}`, price: Math.round(month.price), upper: Math.round(month.upper_bound), lower: Math.round(month.lower_bound),
  }));

  const getScoreColor = (score: number): string => {
    if (score >= 75) return 'text-green-600 bg-green-50 border-green-200';
    if (score >= 50) return 'text-blue-600 bg-blue-50 border-blue-200';
    if (score >= 25) return 'text-yellow-600 bg-yellow-50 border-yellow-200';
    return 'text-red-600 bg-red-50 border-red-200';
  };

  const getBadgeColor = (score: number): string => {
    if (score >= 75) return 'bg-green-100 text-green-800';
    if (score >= 50) return 'bg-blue-100 text-blue-800';
    if (score >= 25) return 'bg-yellow-100 text-yellow-800';
    return 'bg-red-100 text-red-800';
  };

  return (
    <div className="space-y-8">
      <div className="bg-white rounded-lg shadow-lg p-8">
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-slate-900 mb-2">{location.address}</h1>
          <p className="text-slate-600">{location.commune_id}</p>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className={`p-4 rounded-lg border ${getScoreColor(investmentScore)}`}><p className="text-sm font-medium opacity-75">Opportunite investissement</p><p className="text-3xl font-bold">{investmentScore}</p></div>
          <div className={`p-4 rounded-lg border ${getScoreColor(parseFloat(rentalYield) * 20)}`}><p className="text-sm font-medium opacity-75">Rendement locatif</p><p className="text-3xl font-bold">{rentalYield}%</p></div>
          <div className={`p-4 rounded-lg border ${getScoreColor(100 - riskScore)}`}><p className="text-sm font-medium opacity-75">Evaluation du risque</p><p className="text-3xl font-bold">{riskScore}</p></div>
          <div className={`p-4 rounded-lg border ${getScoreColor(100 - bubbleIndex)}`}><p className="text-sm font-medium opacity-75">Indice de bulle</p><p className="text-3xl font-bold">{bubbleIndex}</p></div>
        </div>
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <div className="bg-white rounded-lg shadow-lg p-8">
          <h2 className="text-xl font-bold text-slate-900 mb-6">Profil d analyse</h2>
          <ResponsiveContainer width="100%" height={300}>
            <RadarChart data={radarData}><PolarGrid stroke="#e2e8f0" /><PolarAngleAxis dataKey="name" tick={{ fontSize: 12 }} /><PolarRadiusAxis angle={90} domain={[0, 100]} tick={{ fontSize: 12 }} /><Radar name="Score" dataKey="value" stroke="#3b82f6" fill="#3b82f6" fillOpacity={0.6} /></RadarChart>
          </ResponsiveContainer>
        </div>
        {forecastData.length > 0 && (
          <div className="bg-white rounded-lg shadow-lg p-8">
            <h2 className="text-xl font-bold text-slate-900 mb-6">Prevision sur 12 mois</h2>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={forecastData}><CartesianGrid stroke="#e2e8f0" /><XAxis dataKey="month" tick={{ fontSize: 12 }} /><YAxis tick={{ fontSize: 12 }} /><Tooltip formatter={(value: number) => `${value.toLocaleString()} EUR`} /><Legend /><Line type="monotone" dataKey="upper" stroke="#10b981" strokeDasharray="5 5" name="Limite haute" /><Line type="monotone" dataKey="price" stroke="#3b82f6" name="Prix prevu" strokeWidth={2} /><Line type="monotone" dataKey="lower" stroke="#ef4444" strokeDasharray="5 5" name="Limite basse" /></LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
      <div className="bg-white rounded-lg shadow-lg p-8">
        <h2 className="text-xl font-bold text-slate-900 mb-6">Analyse des risques</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="p-6 border border-slate-200 rounded-lg"><h3 className="font-semibold text-slate-900 mb-3">Score de risque</h3><div className={`inline-block px-4 py-2 rounded-full text-sm font-bold ${getBadgeColor(riskScore)}`}>{riskScore}/100</div><p className="text-sm text-slate-600 mt-3">Evaluation globale du risque</p></div>
          <div className="p-6 border border-slate-200 rounded-lg"><h3 className="font-semibold text-slate-900 mb-3">Indice de bulle</h3><div className={`inline-block px-4 py-2 rounded-full text-sm font-bold ${getBadgeColor(bubbleIndex)}`}>{bubbleIndex}/100</div><p className="text-sm text-slate-600 mt-3">Survaluation potentielle</p></div>
          <div className="p-6 border border-slate-200 rounded-lg"><h3 className="font-semibold text-slate-900 mb-3">Probabilite recession</h3><div className={`inline-block px-4 py-2 rounded-full text-sm font-bold ${getBadgeColor(Math.round((scores.probabilities?.recession || 0) * 100))}`}>{Math.round((scores.probabilities?.recession || 0) * 100)}%</div><p className="text-sm text-slate-600 mt-3">Recession economique</p></div>
        </div>
      </div>
      <div className="bg-white rounded-lg shadow-lg p-8">
        <h2 className="text-xl font-bold text-slate-900 mb-6">Analyse</h2>
        <div className="prose prose-sm max-w-none"><p className="text-slate-700 leading-relaxed whitespace-pre-wrap">{narrative}</p></div>
      </div>
      <div className="bg-white rounded-lg shadow-lg p-8">
        <div className="flex flex-col sm:flex-row gap-4">
          <button className="flex-1 bg-primary text-white font-semibold py-3 px-6 rounded-lg hover:shadow-lg transition">Telecharger en PDF</button>
          <button className="flex-1 bg-accent text-white font-semibold py-3 px-6 rounded-lg hover:shadow-lg transition">Partager le rapport</button>
        </div>
      </div>
    </div>
  );
}
