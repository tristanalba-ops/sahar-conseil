import Link from 'next/link';
import { ChevronRight, BarChart3, TrendingUp, Shield, MapPin, Zap } from 'lucide-react';
import Navbar from './components/Navbar';
import Footer from './components/Footer';

export default function Home() {
  return (
    <>
      <Navbar />
      <main className="flex-1">
        {/* Hero Section */}
        <section className="relative overflow-hidden bg-gradient-to-b from-primary-50 via-white to-white py-20 sm:py-32">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="text-center">
              <h1 className="text-5xl sm:text-6xl font-bold text-slate-900 mb-6">
                Analysez le marché immobilier
                <span className="block text-primary-600">avec des données officielles</span>
              </h1>
              <p className="text-xl text-slate-600 mb-8 max-w-2xl mx-auto">
                Scoring probabiliste basé sur DVF, ADEME, INSEE et Banque de France.
                Prenez de meilleures décisions immobilières grâce à l'intelligence des données.
              </p>
              <div className="flex gap-4 justify-center flex-wrap">
                <Link href="/auth/login" className="btn-primary">
                  Se connecter <ChevronRight className="w-4 h-4 ml-2" />
                </Link>
                <Link href="/auth/signup" className="btn-secondary">
                  S'inscrire
                </Link>
              </div>
            </div>
          </div>
        </section>

        {/* Features Section */}
        <section className="py-20 bg-white">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <h2 className="text-3xl font-bold text-center mb-16">Fonctionnalités clés</h2>
            <div className="grid md:grid-cols-3 gap-8">
              {[
                {
                  icon: BarChart3,
                  title: 'Scoring intelligent',
                  description: 'Évaluation multi-critères : marché, économie, démographie, risques',
                },
                {
                  icon: TrendingUp,
                  title: 'Prévisions de prix',
                  description: 'Forecast 1/5 ans avec intervalles de confiance et analyse de bulle',
                },
                {
                  icon: MapPin,
                  title: 'Localisation précise',
                  description: 'Recherche par adresse avec données géolocalisées en temps réel',
                },
                {
                  icon: Shield,
                  title: 'Données publiques',
                  description: 'Source unique de vérité : DVF (transactions officielles)',
                },
                {
                  icon: Zap,
                  title: 'Rapports générés',
                  description: 'Narratifs IA pour contextualize les données et probabilités',
                },
                {
                  icon: TrendingUp,
                  title: 'Historique complet',
                  description: 'Dashboard des rapports générés et suivi des analyses',
                },
              ].map((feature, idx) => (
                <div key={idx} className="card-hover">
                  <feature.icon className="w-12 h-12 text-primary-600 mb-4" />
                  <h3 className="text-xl font-semibold mb-2">{feature.title}</h3>
                  <p className="text-slate-600">{feature.description}</p>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* How It Works */}
        <section className="py-20 bg-slate-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <h2 className="text-3xl font-bold text-center mb-16">Comment ça marche</h2>
            <div className="grid md:grid-cols-4 gap-8">
              {[
                { step: '1', title: 'Entrez une adresse', desc: 'Utilisez l\'autocomplétion BAN pour localiser' },
                { step: '2', title: 'Requêtez les APIs', desc: 'Collecte DVF, ADEME, INSEE, Banque de France' },
                { step: '3', title: 'Calculez les scores', desc: 'Évaluation multi-critères et probabilités' },
                { step: '4', title: 'Générez rapport', desc: 'Synthèse IA avec visualisations et KPIs' },
              ].map((item, idx) => (
                <div key={idx} className="text-center">
                  <div className="w-12 h-12 rounded-full bg-primary-600 text-white flex items-center justify-center font-bold text-lg mx-auto mb-4">
                    {item.step}
                  </div>
                  <h3 className="font-semibold mb-2">{item.title}</h3>
                  <p className="text-sm text-slate-600">{item.desc}</p>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* CTA Section */}
        <section className="py-20 bg-gradient-to-r from-primary-600 to-primary-800 text-white">
          <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
            <h2 className="text-4xl font-bold mb-6">Prêt à analyser le marché immobilier ?</h2>
            <p className="text-xl text-primary-100 mb-8">
              Accédez à des données publiques consolidées et transformez-les en décisions immobilières.
            </p>
            <Link href="/auth/signup" className="inline-flex items-center bg-white text-primary-600 font-semibold px-8 py-3 rounded-lg hover:bg-slate-50 transition-colors">
              Créer un compte gratuit <ChevronRight className="w-4 h-4 ml-2" />
            </Link>
          </div>
        </section>
      </main>
      <Footer />
    </>
  );
}
