"""
ImmoAnalyse Data Collectors

Unified module for collecting data from public French APIs:
- DVF (Demandes de Valeurs Foncières)
- ADEME (Audit de Dépense Énergétique)
- INSEE (Institut National de la Statistique)
- Banque de France (Interest rates, economic indicators)
- Vigicrues (Flood risk)
- SNCF (Train stations)
- Météo-France (Climate data)
- BRGM (Geological risk)
"""

__version__ = "1.0.0"
__author__ = "SAHAR Conseil"

from .dvf_collector import DVFCollector
from .ademe_collector import ADEMECollector
from .insee_collector import INSEECollector
from .banque_de_france_collector import BanqueDefranceCollector

__all__ = [
    'DVFCollector',
    'ADEMECollector',
    'INSEECollector',
    'BanqueDefranceCollector',
]
