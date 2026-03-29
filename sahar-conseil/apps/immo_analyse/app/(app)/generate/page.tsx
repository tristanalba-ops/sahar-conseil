'use client';

import { useState, useEffect } from 'react';
import { createClient } from '@supabase/supabase-js';
import ProgressBar from '../../components/ProgressBar';
import ComparablesTable from '../../components/ComparablesTable';
import PriceCharts from '../../components/PriceCharts';
import DPEDistribution from '../../components/DPEDistribution';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
);

interface BanSuggestion {
  label: string;
  citycode: string;
  postcode: string;
  name: string;
  context: string;
}

interface ReportData {
  code_insee: string;
  comparables: any[];
  price_by_type: any[];
  price_by_dpe: any[];
  dpe_distribution: any;
  dpe_summary: any;
}

export default function GeneratePage() {
  const [address, setAddress] = useState('');
  const [suggestions, setSuggestions] = useState<BanSuggestion[]>([]);
  const [selectedAddress, setSelectedAddress] = useState<BanSuggestion | null>(null);
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<ReportData | null>(null);

  useEffect(() => {
    const timer = setTimeout(() => {
      if (address.length > 2) {
        searchAddress(address);
      } else {
        setSuggestions([]);
      }
    }, 300);
    return () => clearTimeout(timer);
  }, [address]);

  async function searchAddress(query: string) {
    try {
      const response = await fetch(
        `https://ylrrcbklufshebcizgus.supabase.co/functions/v1/address-search`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY}`,
          },
          body: JSON.stringify({ search: query }),
        }
      );
      if (!response.ok) throw new Error('Search failed');
      const data = await response.json();
      setSuggestions(data.features?.map((f: any) => ({
        label: f.properties.label,
        citycode: f.properties.citycode,
        postcode: f.properties.postcode,
        name: f.properties.name,
        context: f.properties.context,
      })) || []);
    } catch (err) {
      console.error('Search error:', err);
      setSuggestions([]);
    }
  }

  async function generateReport() {
    if (!selectedAddress) return;
    setLoading(true);
    setProgress(0);
    setError(null);
    setReport(null);

    try {
      setProgress(30);
      const marketResponse = await fetch(
        `https://ylrrcbklufshebcizgus.supabase.co/functions/v1/report-data`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY}`,
          },
          body: JSON.stringify({ code_insee: selectedAddress.citycode }),
        }
      );
      if (!marketResponse.ok) throw new Error('Market data fetch failed');
      const marketData = await marketResponse.json();
      setProgress(100);
      setReport(marketData);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Une erreur est survenue');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-b from-blue-50 to-white p-8">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-4xl font-bold text-gray-900 mb-2">ImmoAnalyse</h1>
        <p className="text-gray-600 mb-8">Analyse de marché immobilier avec données DVF</p>

        <div className="bg-white rounded-lg shadow-md p-8 mb-8">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Adresse du bien
          </label>
          <div className="relative">
            <input
              type="text"
              value={address}
              onChange={(e) => {
                setAddress(e.target.value);
                setSelectedAddress(null);
              }}
              placeholder="Tapez une adresse (ex: 12 rue de Rivoli, Paris)"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
            {suggestions.length > 0 && (
              <ul className="absolute top-full left-0 right-0 bg-white border border-gray-300 rounded-lg mt-1 max-h-60 overflow-auto z-10">
                {suggestions.map((s, i) => (
                  <li
                    key={i}
                    onClick={() => {
                      setSelectedAddress(s);
                      setAddress(s.label);
                      setSuggestions([]);
                    }}
                    className="px-4 py-2 hover:bg-blue-50 cursor-pointer text-sm"
                  >
                    <div className="font-medium">{s.name}</div>
                    <div className="text-gray-500 text-xs">{s.context}</div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>

        {selectedAddress && (
          <button
            onClick={generateReport}
            disabled={loading}
            className="w-full bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 text-white font-bold py-3 px-4 rounded-lg mb-8 transition"
          >
            {loading ? 'Analyse en cours...' : 'Générer le rapport'}
          </button>
        )}

        {loading && (
          <div className="max-w-2xl">
            <ProgressBar progress={progress} />
            <p className="mt-4 text-center text-gray-600">Analyse en cours...</p>
          </div>
        )}

        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg mb-8">
            Erreur : {error}
          </div>
        )}

        {report && (
          <div className="space-y-8">
            {report.comparables && report.comparables.length > 0 && (
              <>
                <h2 className="text-2xl font-bold text-gray-900">Biens comparables</h2>
                <ComparablesTable comparables={report.comparables} />
              </>
            )}

            {(report.price_by_type || report.price_by_dpe) && (
              <>
                <h2 className="text-2xl font-bold text-gray-900">Analyse du marché</h2>
                <PriceCharts priceByType={report.price_by_type} priceByDpe={report.price_by_dpe} />
              </>
            )}

            {report.dpe_distribution && (
              <>
                <h2 className="text-2xl font-bold text-gray-900">Distribution énergétique</h2>
                <DPEDistribution distribution={report.dpe_distribution} summary={report.dpe_summary} />
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
