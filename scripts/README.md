# Scripts ETL & maintenance SAHAR

## enrich_dvf_ban_id.py
Enrichit le champ `ban_id` dans `dvf_mutations` via jointure texte sur `ban_adresses`.

**Prérequis DB (créés en prod) :**
- Index `idx_dvf_mutations_geom` (GiST)
- Index `idx_ban_commune_voie_norm` (btree normalisé)
- Fonction `immutable_unaccent(text)`

**Usage :**
```bash
export SUPABASE_DB_URL="postgresql://postgres.[ref]:[pwd]@aws-0-eu-west-3.pooler.supabase.com:6543/postgres"

# Audit sans modifier
python enrich_dvf_ban_id.py --dry-run

# Test sur Gironde d'abord
python enrich_dvf_ban_id.py --dept 33

# Tous les départements
python enrich_dvf_ban_id.py

# Reprendre après interruption
python enrich_dvf_ban_id.py --start-dept 44
```

**Résultat attendu :** passer de ~5% à ~25-35% de couverture ban_id sur DVF
(limité par les DVF pre-2019 sans numéro de voie exploitable).
