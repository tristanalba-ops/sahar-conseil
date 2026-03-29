import type { Metadata } from 'next';
import './globals.css';
import { Providers } from './providers';

export const metadata: Metadata = {
  title: 'ImmoAnalyse - Analyse de marché immobilier',
  description: 'Analyse probabiliste du marché immobilier avec données DVF, ADEME, INSEE',
  keywords: ['immobilier', 'DVF', 'analyse', 'scoring', 'marché'],
  authors: [{ name: 'SAHAR Conseil', url: 'https://sahar-conseil.fr' }],
  creator: 'SAHAR Conseil',
  openGraph: {
    type: 'website',
    locale: 'fr_FR',
    url: 'https://immoanalyse.sahar-conseil.fr',
    siteName: 'ImmoAnalyse',
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="fr">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </head>
      <body>
        <Providers>
          <div className="flex flex-col min-h-screen bg-slate-50">
            {children}
          </div>
        </Providers>
      </body>
    </html>
  );
}
