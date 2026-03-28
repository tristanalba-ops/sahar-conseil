# ImmoAnalyse Data Pipeline

Data collection, processing, and aggregation for probability scoring engine.

## Architecture

```
collectors/           # API integrators
├── dvf_collector.py         # Real estate transactions
├── ademe_collector.py        # Energy performance
├── insee_collector.py        # Demographics & employment
└── banque_de_france_collector.py  # Economic indicators

processors/          # Data transformation (TODO)
├── normalization.py
├── aggregation.py
└── feature_engineering.py

orchestrator.py      # Main pipeline controller
```

## Data Collectors

### DVF (Real Estate Transactions)
- **API**: data.gouv.fr
- **Data**: Transaction prices, surface, type, location, date
- **Update Frequency**: Monthly
- **Coverage**: All French communes

### ADEME (Energy Performance)
- **API**: data.ademe.fr
- **Data**: DPE class distribution (A-G), energy consumption
- **Update Frequency**: Quarterly
- **Coverage**: Residential buildings

### INSEE (Demographic & Economic)
- **API**: insee.fr
- **Data**: Population, employment, income, education
- **Update Frequency**: Annual
- **Coverage**: All communes and IRIS zones

### Banque de France (Economic Indicators)
- **Data**: Interest rates, credit growth, GDP, inflation
- **Update Frequency**: Weekly/Monthly
- **Coverage**: National data

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with API keys and credentials
```

## Usage

### Fetch data for a commune

```python
from collectors import DVFCollector, ADEMECollector, INSEECollector

# Initialize collectors
dvf = DVFCollector()
ademe = ADEMECollector()
insee = INSEECollector()

# Fetch data for Paris (code 75056)
commune_code = "75056"
transactions = dvf.fetch_commune_transactions(commune_code)
dpe_dist = ademe.fetch_commune_dpe_distribution(commune_code)
demographics = insee.fetch_commune_demographics(commune_code)

# Get scores
energy_score = ademe.get_energy_score(dpe_dist)
demographic_score = insee.get_demographic_score(demographics)
```

### Run full pipeline (GCP Cloud Scheduler)

```bash
# Deploy scheduler job
gcloud scheduler jobs create pubsub daily-collect \
  --schedule="0 2 * * *" \
  --timezone="UTC" \
  --topic=data-updates \
  --message-body='{"type":"collect"}'
```

Pipeline will:
1. Trigger data collection from all APIs
2. Store raw data in Cloud Storage
3. Process and normalize data
4. Load into BigQuery
5. Calculate probability scores

## Data Quality

Each collector includes:
- Input validation
- Null/NaN handling
- Outlier detection
- Sanity checks
- Error logging

## Performance

Optimizations:
- Caching of API responses (configurable TTL)
- Concurrent requests with ThreadPoolExecutor
- Batch processing for BigQuery loads
- Incremental updates only (delta processing)

## Monitoring

Cloud Logging integration:
```bash
# View collection logs
gcloud logging read "resource.type=cloud_function AND function_name=collect" --limit=50
```

## Future Enhancements

- [ ] Real-time data streaming (Pub/Sub)
- [ ] Machine learning feature engineering
- [ ] Anomaly detection
- [ ] Data quality dashboards
- [ ] API fallback strategies

## Sources

- **DVF**: https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/
- **ADEME**: https://data.ademe.fr/datasets/dpe-v2-logements-existants
- **INSEE**: https://www.insee.fr/
- **BdF**: https://www.banque-france.fr/
