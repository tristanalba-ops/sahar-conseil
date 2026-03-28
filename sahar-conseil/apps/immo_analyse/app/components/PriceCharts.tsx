'use client';

import React, { useState } from 'react';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

interface PriceChartsProps {
  priceByType: any[];
  priceByDpe: any[];
  dpeDistribution: any[];
}

type ViewType = 'evolution' | 'dpe' | 'distribution';

const DPE_COLORS: Record<string, string> = {
  A: '#22c55e', B: '#86efac', C: '#facc15', D: '#fb923c',
  E: '#fca5a5', F: '#f87171', G: '#dc2626'
};

const fmtPrice = (v: number) => Math.round(v).toLocaleString('fr-FR') + ' \u20ac';

export const PriceCharts: React.FC<PriceChartsProps> = ({ priceByType, priceByDpe, dpeDistribution }) => {
  const [view, setView] = useState<ViewType>('evolution');

  const tabs: { id: ViewType; label: string }[] = [
    { id: 'evolution', label: '\u00c9volution par type' },
    { id: 'dpe', label: 'Prix par DPE' },
    { id: 'distribution', label: 'Distribution DPE' },
  ];

  // Transform priceByType: group by trimestre, pivot type_local
  const pivotedByType = React.useMemo(() => {
    if (!priceByType?.length) return [];
    const map: Record<string, any> = {};
    for (const r of priceByType) {
      if (!map[r.trimestre]) map[r.trimestre] = { trimestre: r.trimestre };
      const key = r.type === 'Appartement' ? 'appartement' : 'maison';
      map[r.trimestre][key] = r.prix_m2_median;
    }
    return Object.values(map).sort((a: any, b: any) => a.trimestre.localeCompare(b.trimestre));
  }, [priceByType]);

  // Aggregate priceByDpe: latest data per DPE
  const aggByDpe = React.useMemo(() => {
    if (!priceByDpe?.length) return [];
    const map: Record<string, { total: number; count: number; decote: number }> = {};
    for (const r of priceByDpe) {
      if (!map[r.dpe]) map[r.dpe] = { total: 0, count: 0, decote: 0 };
      map[r.dpe].total += r.prix_m2_median * r.nb_ventes;
      map[r.dpe].count += r.nb_ventes;
      map[r.dpe].decote = r.decote_pct;
    }
    return ['A','B','C','D','E','F','G']
      .filter(d => map[d])
      .map(d => ({ dpe: d, prix_m2: Math.round(map[d].total / map[d].count), decote: map[d].decote, color: DPE_COLORS[d] }));
  }, [priceByDpe]);

  return (
    <div className="space-y-4">
      <div className="flex gap-2 border-b border-gray-200">
        {tabs.map(t => (
          <button key={t.id} onClick={() => setView(t.id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 -mb-0.5 transition-colors ${view === t.id ? 'border-blue-500 text-blue-600' : 'border-transparent text-gray-600 hover:text-gray-900'}`}>
            {t.label}
          </button>
        ))}
      </div>

      <div className="bg-white rounded-lg border border-gray-200 p-6">
        {view === 'evolution' && pivotedByType.length > 0 && (
          <div>
            <h3 className="text-lg font-semibold text-gray-900 mb-4">\u00c9volution du prix/m\u00b2 par type</h3>
            <ResponsiveContainer width="100%" height={400}>
              <LineChart data={pivotedByType} margin={{ top: 5, right: 30, left: 0, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="trimestre" tick={{ fill: '#6b7280', fontSize: 12 }} />
                <YAxis tickFormatter={fmtPrice} tick={{ fill: '#6b7280', fontSize: 12 }} />
                <Tooltip formatter={(v: any) => fmtPrice(v)} />
                <Legend />
                <Line type="monotone" dataKey="appartement" stroke="#3b82f6" strokeWidth={2} dot={{ r: 4 }} name="Appartement" />
                <Line type="monotone" dataKey="maison" stroke="#ef4444" strokeWidth={2} dot={{ r: 4 }} name="Maison" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {view === 'dpe' && aggByDpe.length > 0 && (
          <div>
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Prix moyen/m\u00b2 par DPE</h3>
            <ResponsiveContainer width="100%" height={400}>
              <BarChart data={aggByDpe} margin={{ top: 20, right: 30, left: 0, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="dpe" tick={{ fill: '#6b7280', fontSize: 14, fontWeight: 'bold' }} />
                <YAxis tickFormatter={fmtPrice} tick={{ fill: '#6b7280', fontSize: 12 }} />
                <Tooltip formatter={(v: any) => fmtPrice(v)} labelFormatter={(l) => 'DPE ' + l} />
                <Bar dataKey="prix_m2" radius={[8, 8, 0, 0]} label={{ position: 'top', fill: '#374151', fontSize: 11, formatter: (v: any, entry: any) => (entry?.decote ? entry.decote + '%' : '') }}>
                  {aggByDpe.map((e, i) => (
                    <rect key={i} fill={e.color} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {view === 'distribution' && dpeDistribution?.length > 0 && (
          <div>
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Distribution DPE de la commune</h3>
            <div className="space-y-3">
              {dpeDistribution.map((d: any) => (
                <div key={d.etiquette} className="flex items-center gap-3">
                  <span className="w-8 text-center font-bold text-sm" style={{ color: DPE_COLORS[d.etiquette] || '#666' }}>{d.etiquette}</span>
                  <div className="flex-1 bg-gray-100 rounded-full h-6 overflow-hidden">
                    <div className="h-full rounded-full flex items-center pl-2 text-xs font-semibold text-white"
                      style={{ width: Math.max(d.pourcentage, 3) + '%', backgroundColor: DPE_COLORS[d.etiquette] || '#ccc' }}>
                      {d.pourcentage > 8 ? d.pourcentage + '%' : ''}
                    </div>
                  </div>
                  <span className="w-20 text-right text-sm text-gray-700">{d.nombre.toLocaleString('fr-FR')}</span>
                  <span className="w-16 text-right text-xs text-gray-500">{d.pourcentage}%</span>
                </div>
              ))}
            </div>
            {dpeDistribution[0]?.conso_moyenne && (
              <div className="mt-6 grid grid-cols-2 sm:grid-cols-4 gap-3">
                {dpeDistribution.map((d: any) => (
                  <div key={d.etiquette + '-conso'} className="p-3 bg-gray-50 rounded-lg text-center">
                    <div className="text-xs text-gray-500">DPE {d.etiquette}</div>
                    <div className="text-lg font-bold" style={{ color: DPE_COLORS[d.etiquette] }}>{Math.round(d.conso_moyenne)}</div>
                    <div className="text-xs text-gray-400">kWh/m\u00b2/an</div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {!pivotedByType.length && !aggByDpe.length && !dpeDistribution?.length && (
          <div className="flex items-center justify-center p-12">
            <p className="text-gray-600">Aucune donn\u00e9e disponible</p>
          </div>
        )}
      </div>
    </div>
  );
};
