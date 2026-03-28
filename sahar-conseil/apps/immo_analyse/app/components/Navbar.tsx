'use client';

import { useSession, signOut } from 'next-auth/react';
import Link from 'next/link';
import { Menu, X, LogOut } from 'lucide-react';
import { useState } from 'react';

export default function Navbar() {
  const { data: session } = useSession();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  return (
    <nav className="sticky top-0 z-50 bg-white border-b border-slate-200">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          {/* Logo */}
          <Link href="/" className="flex items-center gap-2 font-bold text-xl text-primary-600">
            <div className="w-8 h-8 bg-primary-600 rounded-lg flex items-center justify-center text-white font-bold">
              IA
            </div>
            ImmoAnalyse
          </Link>

          {/* Desktop Menu */}
          <div className="hidden md:flex items-center gap-8">
            <Link href="#" className="text-slate-600 hover:text-slate-900 transition-colors">
              Docs
            </Link>
            <Link href="#" className="text-slate-600 hover:text-slate-900 transition-colors">
              Pricing
            </Link>
            {!session ? (
              <>
                <Link href="/auth/login" className="btn-ghost">
                  Se connecter
                </Link>
                <Link href="/auth/signup" className="btn-primary">
                  S'inscrire
                </Link>
              </>
            ) : (
              <div className="flex items-center gap-4">
                <Link href="/dashboard" className="text-slate-600 hover:text-slate-900">
                  Dashboard
                </Link>
                <button
                  onClick={() => signOut()}
                  className="flex items-center gap-2 text-slate-600 hover:text-slate-900"
                >
                  <LogOut className="w-4 h-4" />
                  Déconnexion
                </button>
              </div>
            )}
          </div>

          {/* Mobile Menu Button */}
          <button
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            className="md:hidden text-slate-600 hover:text-slate-900"
          >
            {mobileMenuOpen ? <X className="w-6 h-6" /> : <Menu className="w-6 h-6" />}
          </button>
        </div>

        {/* Mobile Menu */}
        {mobileMenuOpen && (
          <div className="md:hidden border-t border-slate-200 py-4 space-y-3">
            <Link
              href="#"
              className="block text-slate-600 hover:text-slate-900 transition-colors"
            >
              Docs
            </Link>
            <Link
              href="#"
              className="block text-slate-600 hover:text-slate-900 transition-colors"
            >
              Pricing
            </Link>
            {!session ? (
              <>
                <Link href="/auth/login" className="block btn-ghost text-left">
                  Se connecter
                </Link>
                <Link href="/auth/signup" className="block btn-primary text-center">
                  S'inscrire
                </Link>
              </>
            ) : (
              <button
                onClick={() => signOut()}
                className="w-full text-left px-4 py-2 text-slate-600 hover:bg-slate-100 rounded-lg"
              >
                Déconnexion
              </button>
            )}
          </div>
        )}
      </div>
    </nav>
  );
}
