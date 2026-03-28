"""
ADEME (Agence de la Transition Écologique) Data Collector

Collects energy performance data (DPE - Diagnostic de Performance Énergétique)
Source: https://data.ademe.fr/datasets/dpe-v2-logements-existants
"""

import logging
import requests
import pandas as pd
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class ADEMECollector:
    """
    Collector for ADEME Energy Performance Data (DPE)

    Provides:
    - Energy performance class (A-G)
    - Primary energy consumption (kWh/m²/year)
    - GHG emissions (kg CO2/m²/year)
    - Building type, construction year
    - Performance distribution by commune
    """

    BASE_URL = "https://data.ademe.fr/api/v1"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ImmoAnalyse/1.0'
        })

    def fetch_commune_dpe_distribution(
        self,
        commune_code: str
    ) -> Dict:
        """
        Fetch DPE class distribution for a commune

        Args:
            commune_code: INSEE commune code (e.g., "75056")

        Returns:
            Dictionary with distribution by energy class
        """
        try:
            url = f"{self.BASE_URL}/dpe/communes/{commune_code}"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()

            distribution = {
                'A': data.get('class_a', 0),
                'B': data.get('class_b', 0),
                'C': data.get('class_c', 0),
                'D': data.get('class_d', 0),
                'E': data.get('class_e', 0),
                'F': data.get('class_f', 0),
                'G': data.get('class_g', 0),
            }

            # Calculate metrics
            total = sum(distribution.values())
            if total > 0:
                distribution['good_performance_ratio'] = (
                    distribution['A'] + distribution['B']
                ) / total
                distribution['poor_performance_ratio'] = (
                    distribution['F'] + distribution['G']
                ) / total
            else:
                distribution['good_performance_ratio'] = 0
                distribution['poor_performance_ratio'] = 0

            distribution['total'] = total

            logger.info(f"Fetched DPE data for commune {commune_code}")
            return distribution

        except Exception as e:
            logger.error(f"Error fetching ADEME DPE data: {e}")
            return {}

    def get_energy_score(self, distribution: Dict) -> float:
        """
        Calculate energy performance score (0-100)

        Based on distribution of DPE classes:
        - A, B: High score
        - C, D: Medium score
        - E, F, G: Low score
        """
        if not distribution or distribution.get('total', 0) == 0:
            return 50  # Neutral if no data

        total = distribution['total']
        score = (
            (distribution.get('A', 0) * 100 +
             distribution.get('B', 0) * 85 +
             distribution.get('C', 0) * 70 +
             distribution.get('D', 0) * 55 +
             distribution.get('E', 0) * 40 +
             distribution.get('F', 0) * 25 +
             distribution.get('G', 0) * 10) / total
        )

        return round(score, 1)
