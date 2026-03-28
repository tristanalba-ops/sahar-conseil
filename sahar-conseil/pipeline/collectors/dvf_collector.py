"""
DVF (Demandes de Valeurs Foncières) Data Collector

Collects real estate transaction data from the official French registry.
Source: https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/
"""

import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class DVFCollector:
    """
    Collector for DVF (Real Estate Transaction) data

    API provides transaction data with:
    - Property type (house, apartment, land, etc.)
    - Price and price/m²
    - Surface area
    - Location (commune, coordinates)
    - Transaction date
    """

    BASE_URL = "https://www.data.gouv.fr/api/v2/datasets"
    DVF_DATASET_ID = "5f85ffb8634f4168d3c4d8d8"  # Demandes de Valeurs Foncières
    CHUNKSIZE = 10000

    def __init__(self, cache_dir: str = "./data/cache"):
        self.cache_dir = cache_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ImmoAnalyse/1.0 (+https://github.com/tristanalba-ops/sahar-conseil)'
        })

    def fetch_commune_transactions(
        self,
        commune_code: str,
        radius_m: int = 500,
        months_back: int = 36
    ) -> pd.DataFrame:
        """
        Fetch transactions for a commune within a radius

        Args:
            commune_code: INSEE commune code (e.g., "75056" for Paris)
            radius_m: Search radius in meters
            months_back: How many months of history to fetch

        Returns:
            DataFrame with transaction data
        """
        try:
            # Query DVF API with filters
            filters = f"""
            commune_code = '{commune_code}'
            AND transaction_date >= '{self._get_date_offset(months_back)}'
            ORDER BY transaction_date DESC
            """

            url = f"{self.BASE_URL}/{self.DVF_DATASET_ID}/table/"
            params = {
                'refine': filters,
                'limit': self.CHUNKSIZE
            }

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            df = pd.DataFrame(data.get('data', []))

            # Data validation
            df = self._clean_dvf_data(df)

            logger.info(f"Fetched {len(df)} transactions for commune {commune_code}")
            return df

        except Exception as e:
            logger.error(f"Error fetching DVF data: {e}")
            raise

    def _clean_dvf_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize DVF data"""
        if df.empty:
            return df

        # Column mapping
        df.columns = df.columns.str.lower()

        # Remove invalid records
        df = df[df['price'].notna() & (df['price'] > 0)]
        df = df[df['surface_sqm'].notna() & (df['surface_sqm'] > 0)]

        # Add computed columns
        df['price_per_sqm'] = df['price'] / df['surface_sqm']
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])

        # Data quality filters
        df = df[df['price_per_sqm'] > 100]  # Sanity check
        df = df[df['price_per_sqm'] < 50000]  # Sanity check

        return df.sort_values('transaction_date', ascending=False)

    def _get_date_offset(self, months: int) -> str:
        """Get date string N months ago"""
        date = datetime.now() - timedelta(days=30 * months)
        return date.strftime('%Y-%m-%d')

    def get_summary_stats(
        self,
        df: pd.DataFrame
    ) -> Dict:
        """Get summary statistics for transactions"""
        if df.empty:
            return {}

        return {
            'total_transactions': len(df),
            'avg_price': df['price'].mean(),
            'median_price': df['price'].median(),
            'avg_price_per_sqm': df['price_per_sqm'].mean(),
            'avg_surface_sqm': df['surface_sqm'].mean(),
            'price_min': df['price'].min(),
            'price_max': df['price'].max(),
            'date_range': f"{df['transaction_date'].min().date()} to {df['transaction_date'].max().date()}"
        }
