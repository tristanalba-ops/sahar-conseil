'use client';

import { useState, useRef, useEffect, useCallback } from 'react';
import { useSession } from 'next-auth/react';
import { useRouter } from 'next/navigation';
import ProgressBar from '@/app/components/ProgressBar';
import ReportDisplay from '@/app/components/ReportDisplay';

interface BAN_Suggestion {
  properties: { label: string; context: string; latitude: number; longitude: number; };
}

interface Report {
  location: { address: string; commune_id: string; lat: number; lon: number; };
  scores: any; stats: any; forecast: any; narrative: string; reportId?: string;
}

const BAN_API_URL = process.env.NEXT_PUBLIC_BAN_API_URL || 'https://api-adresse.data.gouv.fr';
const DEBOUNCE_MS = 300;

export default function GeneratePage() {
  const { data: session, status } = useSession();
  const router = useRouter();
  const [address, setAddress] = useState('');
  const [suggestions, setSuggestions] = useState<BAN_Suggestion[]>([]);
  const [selectedAddress, setSelectedAddress] = useState<BAN_Suggestion | null>(null);
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState('');
  const [report, setReport] = useState<Report | null>(null);
  const debounceTimer = useRef<NodeJS.Timeout>();
  const suggestionsRef = useRef<HTMLDivElement>(null);

  useEffect(() => { if (status === 'unauthenticated') router.push('/login'); }, [status, router]);

  const fetchSuggestions = useCallback(async (query: string) => {
    if (query.length < 3) { setSuggestions([]); return; }
    try {
      const response = await fetch(`${BAN_API_URL}/search?q=${encodeURIComponent(query)}&type=housenumber&limit=10`);
      const data = await response.json();
      setSuggestions(data.features || []);
    } catch (err) { console.error('BAN API error:', err); setSuggestions([]); }
  }, []);

  const handleAddressChange = (value: string) => {
    setAddress(value); setSelectedAddress(null);
    if (debounceTimer.current) clearTimeout(debounceTimer.current);
    debounceTimer.current = setTimeout(() => fetchSuggestions(value), DEBOUNCE_MS);
  };

  const handleSuggestionSelect = (suggestion: BAN_Suggestion) => {
    setSelectedAddress(suggestion); setAddress(suggestion.properties.label); setSuggestions([]);
  };

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (suggestionsRef.current && !suggestionsRef.current.contains(event.target as Node)) setSuggestions([]);
    };
    if (suggestions.length > 0) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => document.removeEventListener('mousedown', handleClickOutside);
    }
  }, [suggestions]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault(); setError(''); setProgress(0);
    if (!selectedAddress) { setError('Veuillez selectionner une adresse'); return; }
    setLoading(true); setProgress(10);
    try {
      const label = selectedAddress.properties.label;
      const parts = label.split(',');
      const fullAddress = parts[0]?.trim() || label;
      const cityPart = parts[parts.length - 1]?.trim() || '';
      setProgress(20);
      const response = await fetch('/api/generate-report', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ address: fullAddress, city: cityPart, postcode: extractPostcode(label), context: selectedAddress.properties.context, lat: selectedAddress.properties.latitude, lon: selectedAddress.properties.longitude }),
      });
      setProgress(60);
      if (!response.ok) { const errorData = await response.json(); throw new Error(errorData.error || 'Erreur'); }
      const reportData = await response.json();
      setProgress(100);
      setTimeout(() => { setReport(reportData); setLoading(false); }, 500);
    } catch (err) { setError(err instanceof Error ? err.message : 'Une erreur est survenue'); setLoading(false); }
  };

  const handleNewReport = () => { setReport(null); setSelectedAddress(null); setAddress(''); setProgress(0); setError(''); };

  if (status === 'loading') {
    return (<div className="min-h-screen flex items-center justify-center"><div className="text-center"><div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div><p className="mt-4 text-gray-600">Chargement...</p></div></div>);
  }
  if (status === 'unauthenticated') return null;

  return (
    <div className="min-h-screen bg-gradient-to-b from-slate-50 to-white">
      <div className="container max-w-4xl mx-auto px-4 py-12">
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-slate-900 mb-4">Generateur de Rapport</h1>
          <p className="text-lg text-slate-600">Analysez le potentiel d investissement immobilier</p>
        </div>
        {!loading && !report ? (
          <div className="bg-white rounded-lg shadow-lg p-8">
            <form onSubmit={handleSubmit} className="space-y-6">
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-2">Adresse</label>
                <div className="relative" ref={suggestionsRef}>
                  <input type="text" value={address} onChange={(e) => handleAddressChange(e.target.value)} placeholder="Entrez une adresse (ex: 42 rue de la paix, 75001)" className="w-full px-4 py-3 border border-slate-300 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent outline-none transition" autoComplete="off" />
                  {suggestions.length > 0 && (
                    <div className="absolute top-full left-0 right-0 mt-2 bg-white border border-slate-300 rounded-lg shadow-lg z-10 max-h-64 overflow-y-auto">
                      {suggestions.map((s, i) => (
                        <button key={i} type="button" onClick={() => handleSuggestionSelect(s)} className="w-full text-left px-4 py-3 hover:bg-slate-100 transition border-b border-slate-200 last:border-b-0">
                          <div className="font-medium text-slate-900">{s.properties.label}</div>
                          <div className="text-sm text-slate-500">{s.properties.context}</div>
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                {selectedAddress && (<div className="mt-3 p-3 bg-blue-50 border border-blue-200 rounded-lg"><p className="text-sm text-blue-900">OK: {selectedAddress.properties.label}</p></div>)}
              </div>
              {error && (<div className="p-4 bg-red-50 border border-red-200 rounded-lg"><p className="text-sm text-red-700">{error}</p></div>)}
              <button type="submit" disabled={!selectedAddress || loading} className="w-full bg-gradient-to-r from-primary to-accent text-white font-semibold py-3 px-6 rounded-lg hover:shadow-lg transition disabled:opacity-50 disabled:cursor-not-allowed">Generer le Rapport</button>
            </form>
          </div>
        ) : loading ? (
          <div className="bg-white rounded-lg shadow-lg p-8">
            <h2 className="text-2xl font-bold text-slate-900 mb-6">Generation en cours...</h2>
            <ProgressBar progress={progress} />
            <p className="text-center text-slate-600 mt-6">Analyse du marche, rendement locatif, risques et tendances...</p>
          </div>
        ) : report ? (
          <div>
            <div className="mb-6"><button onClick={handleNewReport} className="text-primary hover:text-accent font-semibold transition">Nouveau rapport</button></div>
            <ReportDisplay report={report} />
          </div>
        ) : null}
      </div>
    </div>
  );
}

function extractPostcode(label: string): string { const match = label.match(/\b(\d{5})\b/); return match ? match[1] : ''; }
