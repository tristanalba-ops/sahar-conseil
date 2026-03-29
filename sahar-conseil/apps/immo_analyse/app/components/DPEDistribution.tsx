'use client';

import React from 'react';

interface DPEDistributionProps {
  distribution: Array<{ etiquette: string; nombre: number; pourcentage: number; conso_moyenne?: number }>;
  summary?: { total_logements: number; distribution: Record<string, number>; conso_moyenne_par_dpe: Record<string, number> };
}

const DPE_COLORS: Record<string, string> = {
  A: '#22c55e', B: '#86efac', C: '#facc15', D: '#fb923c', E: '#fca5a5', F: '#f87171', G: '#dc2626'
};

const DPE_LABELS: Record<string, string> = {
  A: 'Excellent', B: 'Tr\u00e8s bon', C: 'Bon', D: 'Moyen', E: 'Insuffisant', F: 'Mauvais', G: 'Tr\u00e8s mauvais'
};

export const DPEDistribution: React.FC<DPEDistributionProps> = ({ distribution, summary }) => {
  if (!distribution || distribution.length === 0) {
    return (
      <div className="flex items-center justify-center p-8 rounded-lg border border-gray-200 bg-gray-50">
        <p className="text-gray-600 font-medium">Aucune donn\u00e9e DPE disponible</p>
      </div>
    );
  }

  const total = distribution.reduce((acc, d) => acc + d.nombre, 0);
  const performant = distribution.filter(d => ['A', 'B', 'C'].includes(d.etiquette)).reduce((acc, d) => acc + d.nombre, 0);
  const passoire = distribution.filter(d => ['F', 'G'].includes(d.etiquette)).reduce((acc, d) => acc + d.nombre, 0);

  return (
    <div className="space-y-6">
      {/* Stats r\u00e9sum\u00e9 */}
      <div className="grid grid-cols-3 gap-4">
        <div className="p-4 bg-blue-50 rounded-lg text-center">
          <div className="text-2xl font-bold text-blue-700">{total.toLocaleString('fr-FR')}</div>
          <div className="text-xs text-blue-600 mt-1">Logements analys\u00e9s</div>
        </div>
        <div className="p-4 bg-green-50 rounded-lg text-center">
          <div className="text-2xl font-bold text-green-700">{total > 0 ? Math.round(performant / total * 100) : 0}%</div>
          <div className="text-xs text-green-600 mt-1">Performants (A-C)</div>
        </div>
        <div className="p-4 bg-red-50 rounded-lg text-center">
          <div className="text-2xl font-bold text-red-700">{total > 0 ? Math.round(passoire / total * 100) : 0}%</div>
          <div className="text-xs text-red-600 mt-1">Passoires (F-G)</div>
        </div>
      </div>

      {/* Barres horizontales */}
      <div className="space-y-3">
        {distribution.map((d) => (
          <div key={d.etiquette} className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg flex items-center justify-center font-bold text-white text-lg"
              style={{ backgroundColor: DPE_COLORS[d.etiquette] || '#999' }}>
              {d.etiquette}
            </div>
            <div className="flex-1">
              <div className="flex items-center justify-between mb-1">
                <span className="text-sm text-gray-700">{DPE_LABELS[d.etiquette] || ''}</span>
                <span className="text-sm font-semibold text-gray-900">{d.nombre.toLocaleString('fr-FR')} ({d.pourcentage}%)</span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-4 overflow-hidden">
                <div className="h-full rounded-full transition-all duration-500"
                  style={{ width: Math.max(d.pourcentage, 1) + '%', backgroundColor: DPE_COLORS[d.etiquette] || '#ccc' }} />
              </div>
            </div>
            {d.conso_moyenne && (
              <div className="w-20 text-right">
                <div className="text-sm font-semibold text-gray-900">{Math.round(d.conso_moyenne)}</div>
                <div className="text-xs text-gray-500">kWh/m\u00b2</div>
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Consommation par DPE depuis summary */}
      {summary?.conso_moyenne_par_dpe && (
        <div className="p-4 bg-gray-50 rounded-lg">
          <h4 className="text-sm font-semibold text-gray-700 mb-3">Consommation \u00e9nerg\u00e9tique moyenne par classe</h4>
          <div className="flex flex-wrap gap-4">
            {Object.entries(summary.conso_moyenne_par_dpe).sort().map(([dpe, conso]) => (
              <div key={dpe} className="flex items-center gap-2">
                <span className="inline-block w-6 h-6 rounded text-center text-xs font-bold text-white leading-6"
                  style={{ backgroundColor: DPE_COLORS[dpe] || '#999' }}>{dpe}</span>
                <span className="text-sm text-gray-700">{conso} kWh/m\u00b2/an</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};
