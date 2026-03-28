'use client';

import React, { useState, useMemo } from 'react';

interface DVFTransaction {
  id: string;
  date: string;
  adresse: string;
  type: string;
  surface: number;
  pieces: number;
  prix: number;
  prix_m2: number;
  dpe?: string;
}

interface ComparablesTableProps {
  comparables: DVFTransaction[];
}

type SortField = 'date' | 'price_m2' | 'price';
type SortOrder = 'asc' | 'desc';

const getDPEColor = (dpe: string | undefined): string => {
  if (!dpe) return 'bg-gray-200';
  switch (dpe.toUpperCase()) {
    case 'A': return 'bg-green-500 text-white';
    case 'B': return 'bg-green-300 text-gray-900';
    case 'C': return 'bg-yellow-300 text-gray-900';
    case 'D': return 'bg-orange-400 text-white';
    case 'E': return 'bg-red-200 text-gray-900';
    case 'F': return 'bg-red-400 text-white';
    case 'G': return 'bg-red-600 text-white';
    default: return 'bg-gray-200';
  }
};

export const ComparablesTable: React.FC<ComparablesTableProps> = ({ comparables }) => {
  const [sortField, setSortField] = useState<SortField>('date');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');

  const sorted = useMemo(() => {
    if (!comparables || comparables.length === 0) return [];
    return [...comparables].sort((a, b) => {
      let aVal: number | string, bVal: number | string;
      switch (sortField) {
        case 'price_m2': aVal = a.prix_m2 || 0; bVal = b.prix_m2 || 0; break;
        case 'price': aVal = a.prix || 0; bVal = b.prix || 0; break;
        default: aVal = a.date || ''; bVal = b.date || '';
      }
      if (typeof aVal === 'string') return sortOrder === 'asc' ? aVal.localeCompare(bVal as string) : (bVal as string).localeCompare(aVal);
      return sortOrder === 'asc' ? (aVal as number) - (bVal as number) : (bVal as number) - (aVal as number);
    });
  }, [comparables, sortField, sortOrder]);

  const handleSort = (field: SortField) => {
    if (sortField === field) setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    else { setSortField(field); setSortOrder('desc'); }
  };

  if (!comparables || comparables.length === 0) {
    return (<div className="flex items-center justify-center p-8 rounded-lg border border-gray-200 bg-gray-50"><p className="text-gray-600 font-medium">Aucun comparable trouv\u00e9</p></div>);
  }

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return <span className="ml-1 text-gray-400">\u2195</span>;
    return <span className="ml-1">{sortOrder === 'asc' ? '\u2191' : '\u2193'}</span>;
  };

  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200">
      <table className="w-full text-sm">
        <thead className="bg-gray-50 border-b border-gray-200">
          <tr>
            <th className="px-4 py-3 text-left font-semibold text-gray-700"><button onClick={() => handleSort('date')} className="flex items-center hover:text-gray-900">Date <SortIcon field="date" /></button></th>
            <th className="px-4 py-3 text-left font-semibold text-gray-700">Adresse</th>
            <th className="px-4 py-3 text-left font-semibold text-gray-700">Type</th>
            <th className="px-4 py-3 text-right font-semibold text-gray-700">Surface</th>
            <th className="px-4 py-3 text-right font-semibold text-gray-700">Pi\u00e8ces</th>
            <th className="px-4 py-3 text-right font-semibold text-gray-700"><button onClick={() => handleSort('price')} className="flex items-center justify-end hover:text-gray-900">Prix <SortIcon field="price" /></button></th>
            <th className="px-4 py-3 text-right font-semibold text-gray-700"><button onClick={() => handleSort('price_m2')} className="flex items-center justify-end hover:text-gray-900">\u20ac/m\u00b2 <SortIcon field="price_m2" /></button></th>
            <th className="px-4 py-3 text-center font-semibold text-gray-700">DPE</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-200">
          {sorted.map((item, i) => (
            <tr key={item.id || i} className="hover:bg-gray-50">
              <td className="px-4 py-3 text-gray-900 whitespace-nowrap">{item.date ? new Date(item.date).toLocaleDateString('fr-FR') : '-'}</td>
              <td className="px-4 py-3 text-gray-900 max-w-xs truncate">{item.adresse || '-'}</td>
              <td className="px-4 py-3 text-gray-700">{item.type || '-'}</td>
              <td className="px-4 py-3 text-right">{item.surface ? item.surface.toLocaleString('fr-FR') + ' m\u00b2' : '-'}</td>
              <td className="px-4 py-3 text-right">{item.pieces || '-'}</td>
              <td className="px-4 py-3 text-right">{item.prix ? item.prix.toLocaleString('fr-FR') + ' \u20ac' : '-'}</td>
              <td className="px-4 py-3 text-right font-semibold text-blue-600">{item.prix_m2 ? Math.round(item.prix_m2).toLocaleString('fr-FR') + ' \u20ac' : '-'}</td>
              <td className="px-4 py-3 text-center"><span className={`inline-block px-2.5 py-1 rounded font-bold text-xs ${getDPEColor(item.dpe)}`}>{item.dpe || '-'}</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
