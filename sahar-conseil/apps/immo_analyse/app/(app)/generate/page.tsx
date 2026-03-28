'use client';

import { useState, useEffect, useRef, useCallback } from 'react';
import { useSession } from 'next-auth/react';
import { useRouter } from 'next/navigation';
import { ProgressBar } from '@/app/components/ProgressBar';
import { ReportDisplay } from '@/app/components/ReportDisplay';
import { ComparablesTable } from '@/app/components/ComparablesTable';
import { PriceCharts } from '@/app/components/PriceCharts';
import { DPEDistribution } from '@/app/components/DPEDistribution';

const BAN_API = 'https://api-adresse.data.gouv.fr/search';
const SUPABASE_URL = 'https://ylrrcbklufshebcizgus.supabase.co/functions/v1';

interface BanSuggestion {
  label: string;
  postcode: string;
  city: string;
  citycode: string;
  context: string;
  latitude: number;
  longitude: number;
}

export default function GeneratePage() {
  const { data: session, status } = useSession();
  const router = useRouter();

  const [address, setAddress] = useState('');
  const [suggestions, setSuggestions] = useState<BanSuggestion[]>([]);
  const [selectedAddress, setSelectedAddress] = useState<BanSuggestion | null>(null);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState('');
  const [report, setReport] = useState<any>(null);
  const [marketData, setMarketData] = useState<any>(null);

  const suggestionsRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<NodeJS.Timeout>();

  // Redirect if not authenticated
  useEffect(() => {
    if (status === 'unauthenticated') router.push('/login');
  }, [status, router]);

  // Close suggestions on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (suggestionsRef.current && !suggestionsRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  // BAN API search with 300ms debounce
  const searchAddress = useCallback(async (query: string) => {
    if (query.length < 3) { setSuggestions([]); return; }
    try {
      const res = await fetch(`${BAN_API}?q=${encodeURIComponent(query)}&limit=7&autocomplete=1`);
      const data = await res.json();
      const results = (data.features || []).map((f: any) => ({
        label: f.properties.label,
        postcode: f.properties.postcode,
        city: f.properties.city,
        citycode: f.properties.citycode,
        context: f.properties.context,
        latitude: f.geometry.coordinates[1],
        longitude: f.geometry.coordinates[0],
      }));
      setSuggestions(results);
      setShowSuggestions(results.length > 0);
    } catch { setSuggestions([]); }
  }, []);

  const handleAddressChange = (value: string) => {
    setAddress(value);
    setSelectedAddress(null);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => searchAddress(value), 300);
  };

  const handleSelectAddress = (suggestion: BanSuggestion) => {
    setAddress(suggestion.label);
    setSelectedAddress(suggestion);
    setShowSuggestions(false);
    setSuggestions([]);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedAddress) { setError('Veuillez s\u00e9lectionner une adresse.'); return; }

    setLoading(true); setError(''); setProgress(10);
    setReport(null); setMarketData(null);

    try {
      setProgress(20);

      // Appels parall\u00e8les : GCP Cloud Functions + Supabase Edge Function
      const [gcpResult, marketResult] = await Promise.allSettled([
        // 1. Scores d'investissement (GCP)
        fetch('/api/generate-report', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            address: selectedAddress.label,
            postcode: selectedAddress.postcode,
            city: selectedAddress.city,
            context: selectedAddress.context,
            latitude: selectedAddress.latitude,
            longitude: selectedAddress.longitude,
          }),
        }).then(r => r.json()),

        // 2. Donn\u00e9es march\u00e9 : comparables DVF + DPE + prix (Supabase)
        fetch(`${SUPABASE_URL}/report-data`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${(session as any)?.supabaseAccessToken || ''}`,
            'apikey': process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || '',
          },
          body: JSON.stringify({
            code_insee: selectedAddress.citycode,
            type_local: null,
          }),
        }).then(r => r.json()),
      ]);

      setProgress(70);

      // Traitement des r\u00e9sultats
      if (gcpResult.status === 'fulfilled') {
        setReport(gcpResult.value);
      }

      if (marketResult.status === 'fulfilled') {
        setMarketData(marketResult.value);
      }

      setProgress(100);

      if (gcpResult.status === 'rejected' && marketResult.status === 'rejected') {
        setError('Erreur lors de la g\u00e9n\u00e9ration du rapport. Veuillez r\u00e9essayer.');
      }

    } catch (err: any) {
      setError(err.message || 'Erreur inattendue');
    } finally {
      setLoading(false);
    }
  };

  if (status === 'loading') {
    return <div className="flex items-center justify-center min-h-screen"><div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600" /></div>;
  }

  return (
    <div className="max-w-6xl mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">G\u00e9n\u00e9rer un rapport d'analyse</h1>
        <p className="mt-2 text-gray-600">Recherchez une adresse pour obtenir une analyse compl\u00e8te du march\u00e9 immobilier local.</p>
      </div>

      {/* Formulaire de recherche */}
      {!loading && !report && !marketData && (
        <form onSubmit={handleSubmit} className="max-w-2xl">
          <div className="relative" ref={suggestionsRef}>
            <label htmlFor="address" className="block text-sm font-medium text-gray-700 mb-2">
              Adresse du bien
            </label>
            <input
              id="address"
              type="text"
              value={address}
              onChange={(e) => handleAddressChange(e.target.value)}
              placeholder="Ex: 12 rue de la Paix, Paris"
              className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-gray-900"
              autoComplete="off"
            />

            {showSuggestions && suggestions.length > 0 && (
              <div className="absolute z-10 w-full mt-1 bg-white rounded-lg border border-gray-200 shadow-lg max-h-60 overflow-y-auto">
                {suggestions.map((s, i) => (
                  <button key={i} type="button" onClick={() => handleSelectAddress(s)}
                    className="w-full text-left px-4 py-3 hover:bg-blue-50 transition-colors border-b border-gray-100 last:border-0">
                    <div className="font-medium text-gray-900">{s.label}</div>
                    <div className="text-sm text-gray-500">{s.context}</div>
                  </button>
                ))}
              </div>
            )}
          </div>

          {selectedAddress && (
            <div className="mt-4 p-4 bg-blue-50 rounded-lg border border-blue-200">
              <div className="flex items-center gap-2">
                <span className="text-blue-600 font-semibold">\u2713</span>
                <span className="text-blue-900 font-medium">{selectedAddress.label}</span>
              </div>
              <div className="text-sm text-blue-700 mt-1">
                Code INSEE: {selectedAddress.citycode} | {selectedAddress.context}
              </div>
            </div>
          )}

          {error && <div className="mt-4 p-4 bg-red-50 rounded-lg border border-red-200 text-red-700">{error}</div>}

          <button type="submit" disabled={!selectedAddress}
            className="mt-6 w-full py-3 px-6 bg-blue-600 text-white font-semibold rounded-lg hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors">
            Analyser ce bien
          </button>
        </form>
      )}

      {/* Progression */}
      {loading && (
        <div className="max-w-2xl">
          <ProgressBar progress={progress} />
          <p className="mt-4 text-center text-gray-600">Analyse en cours... R\u00e9cup\u00e9ration des donn\u00e9es DVF, DPE et march\u00e9</p>
        </div>
      )}

      {/* R\u00e9sultats */}
      {!loading && (report || marketData) && (
        <div className="space-y-8">
          <button onClick={() => { setReport(null); setMarketData(null); setProgress(0); setSelectedAddress(null); setAddress(''); }}
            className="text-blue-600 hover:text-blue-800 font-medium flex items-center gap-2">
            \u2190 Nouvelle recherche
          </button>

          {/* Scores d'investissement */}
          {report && <ReportDisplay report={report} />}

          {/* Biens comparables */}
          {marketData?.comparables && marketData.comparables.length > 0 && (
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
              <h2 className="text-xl font-bold text-gray-900 mb-4">Biens comparables (ventes r\u00e9centes)</h2>
              <ComparablesTable comparables={marketData.comparables} />
            </div>
          )}

          {/* \u00c9volution des prix */}
          {(marketData?.price_by_type?.length > 0 || marketData?.price_by_dpe?.length > 0 || marketData?.dpe_distribution?.length > 0) && (
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
              <h2 className="text-xl font-bold text-gray-900 mb-4">\u00c9volution des prix et DPE</h2>
              <PriceCharts
                priceByType={marketData.price_by_type || []}
                priceByDpe={marketData.price_by_dpe || []}
                dpeDistribution={marketData.dpe_distribution || []}
              />
            </div>
          )}

          {/* Distribution DPE d\u00e9taill\u00e9e */}
          {marketData?.dpe_distribution && marketData.dpe_distribution.length > 0 && (
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
              <h2 className="text-xl font-bold text-gray-900 mb-4">Diagnostic \u00e9nerg\u00e9tique de la commune</h2>
              <DPEDistribution distribution={marketData.dpe_distribution} summary={marketData.dpe_summary} />
            </div>
          )}
        </div>
      )}
    </div>
  );
      }
