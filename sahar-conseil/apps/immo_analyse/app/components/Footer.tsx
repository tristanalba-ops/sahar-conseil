import Link from 'next/link';
import { Heart } from 'lucide-react';

export default function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer className="bg-slate-900 text-slate-400 py-12">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid md:grid-cols-4 gap-8 mb-8">
          {/* Brand */}
          <div>
            <div className="flex items-center gap-2 mb-4">
              <div className="w-8 h-8 bg-primary-600 rounded-lg flex items-center justify-center text-white font-bold text-sm">
                IA
              </div>
              <span className="font-bold text-white">ImmoAnalyse</span>
            </div>
            <p className="text-sm">
              Analyse immobilière powered by données publiques françaises.
            </p>
          </div>

          {/* Product */}
          <div>
            <h4 className="font-semibold text-white mb-4">Produit</h4>
            <ul className="space-y-2 text-sm">
              <li><Link href="#" className="hover:text-white transition-colors">Fonctionnalités</Link></li>
              <li><Link href="#" className="hover:text-white transition-colors">Pricing</Link></li>
              <li><Link href="#" className="hover:text-white transition-colors">Documentation</Link></li>
            </ul>
          </div>

          {/* Entreprise */}
          <div>
            <h4 className="font-semibold text-white mb-4">Entreprise</h4>
            <ul className="space-y-2 text-sm">
              <li><Link href="#" className="hover:text-white transition-colors">À propos</Link></li>
              <li><Link href="#" className="hover:text-white transition-colors">Blog</Link></li>
              <li><Link href="#" className="hover:text-white transition-colors">Contact</Link></li>
            </ul>
          </div>

          {/* Legal */}
          <div>
            <h4 className="font-semibold text-white mb-4">Légal</h4>
            <ul className="space-y-2 text-sm">
              <li><Link href="#" className="hover:text-white transition-colors">Mentions légales</Link></li>
              <li><Link href="#" className="hover:text-white transition-colors">Confidentialité</Link></li>
              <li><Link href="#" className="hover:text-white transition-colors">Conditions</Link></li>
            </ul>
          </div>
        </div>

        {/* Divider */}
        <div className="border-t border-slate-800 pt-8 flex flex-col sm:flex-row justify-between items-center">
          <p className="text-sm">
            © {currentYear} ImmoAnalyse. Fait avec <Heart className="w-4 h-4 inline text-red-500 mx-1" /> par SAHAR Conseil.
          </p>
          <div className="flex gap-6 mt-4 sm:mt-0 text-sm">
            <Link href="#" className="hover:text-white transition-colors">Twitter</Link>
            <Link href="#" className="hover:text-white transition-colors">GitHub</Link>
            <Link href="#" className="hover:text-white transition-colors">LinkedIn</Link>
          </div>
        </div>
      </div>
    </footer>
  );
}
