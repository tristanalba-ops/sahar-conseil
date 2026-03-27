-- ============================================================
-- SAHAR CRM INTERNE — tables pour gérer tes propres
-- prospects, clients, deals, factures
-- ============================================================

-- COMPTES (entreprises / personnes ciblées)
CREATE TABLE IF NOT EXISTS sahar_comptes (
  id            TEXT PRIMARY KEY,
  nom           TEXT NOT NULL,
  type          TEXT DEFAULT 'prospect',   -- prospect|client|partenaire|perdu
  secteur       TEXT,                       -- immobilier|energie|retail|rh|auto
  email         TEXT,
  tel           TEXT,
  site          TEXT,
  ville         TEXT,
  code_postal   TEXT,
  nb_employes   TEXT,
  notes         TEXT,
  -- Attribution source
  source        TEXT,    -- site|linkedin|referral|cold|event|inbound
  utm_source    TEXT,
  utm_medium    TEXT,
  utm_campaign  TEXT,
  utm_content   TEXT,
  utm_term      TEXT,
  landing_page  TEXT,
  -- Scoring
  score_lead    INTEGER DEFAULT 0,
  score_fit     INTEGER DEFAULT 0,   -- adéquation ICP
  -- Dates
  date_creation TEXT,
  date_update   TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- CONTACTS (personnes rattachées à un compte)
CREATE TABLE IF NOT EXISTS sahar_contacts (
  id            TEXT PRIMARY KEY,
  compte_id     TEXT REFERENCES sahar_comptes(id) ON DELETE SET NULL,
  prenom        TEXT,
  nom           TEXT NOT NULL,
  email         TEXT,
  tel           TEXT,
  poste         TEXT,
  linkedin      TEXT,
  notes         TEXT,
  date_creation TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- OPPORTUNITÉS (deals en cours)
CREATE TABLE IF NOT EXISTS sahar_opportunites (
  id            TEXT PRIMARY KEY,
  compte_id     TEXT REFERENCES sahar_comptes(id) ON DELETE SET NULL,
  contact_id    TEXT REFERENCES sahar_contacts(id) ON DELETE SET NULL,
  titre         TEXT NOT NULL,
  offre         TEXT,   -- starter|pro|expert|sur_mesure
  valeur        NUMERIC DEFAULT 0,
  recurrence    TEXT DEFAULT 'mensuel',  -- mensuel|annuel|unique
  stage         TEXT DEFAULT 'Qualification',
  -- Qualification BANT
  budget        TEXT,
  autorite      TEXT,
  besoin        TEXT,
  timeline      TEXT,
  -- Probabilité
  probabilite   INTEGER DEFAULT 10,
  valeur_ponderee NUMERIC GENERATED ALWAYS AS (valeur * probabilite / 100) STORED,
  -- Source
  source        TEXT,
  utm_source    TEXT,
  utm_campaign  TEXT,
  -- Dates
  date_creation TEXT,
  date_closing_prevu TEXT,
  date_closing_reel  TEXT,
  date_update   TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ACTIVITÉS (interactions tracées)
CREATE TABLE IF NOT EXISTS sahar_activites (
  id            TEXT PRIMARY KEY,
  compte_id     TEXT REFERENCES sahar_comptes(id) ON DELETE CASCADE,
  contact_id    TEXT REFERENCES sahar_contacts(id) ON DELETE SET NULL,
  opp_id        TEXT REFERENCES sahar_opportunites(id) ON DELETE SET NULL,
  type          TEXT NOT NULL,  -- appel|email|demo|meeting|note|sms|whatsapp
  direction     TEXT DEFAULT 'sortant',  -- sortant|entrant
  sujet         TEXT,
  notes         TEXT,
  statut        TEXT DEFAULT 'fait',     -- fait|planifié|annulé
  duree_min     INTEGER,
  date_activite TEXT,
  date_creation TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- DEALS SIGNÉS (clients actifs)
CREATE TABLE IF NOT EXISTS sahar_deals (
  id            TEXT PRIMARY KEY,
  compte_id     TEXT REFERENCES sahar_comptes(id) ON DELETE SET NULL,
  opp_id        TEXT REFERENCES sahar_opportunites(id) ON DELETE SET NULL,
  offre         TEXT NOT NULL,
  montant       NUMERIC NOT NULL,
  recurrence    TEXT DEFAULT 'mensuel',
  statut        TEXT DEFAULT 'actif',   -- actif|pause|résilié
  date_debut    TEXT,
  date_fin      TEXT,
  date_creation TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- FACTURES
CREATE TABLE IF NOT EXISTS sahar_factures (
  id            TEXT PRIMARY KEY,
  deal_id       TEXT REFERENCES sahar_deals(id) ON DELETE SET NULL,
  compte_id     TEXT REFERENCES sahar_comptes(id) ON DELETE SET NULL,
  numero        TEXT NOT NULL,
  montant_ht    NUMERIC NOT NULL,
  tva           NUMERIC DEFAULT 0.20,
  montant_ttc   NUMERIC GENERATED ALWAYS AS (montant_ht * (1 + tva)) STORED,
  statut        TEXT DEFAULT 'en_attente',  -- en_attente|payée|en_retard|annulée
  date_emission TEXT,
  date_echeance TEXT,
  date_paiement TEXT,
  notes         TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- INTERACTIONS SITE (tracking UTM entrant)
CREATE TABLE IF NOT EXISTS sahar_tracking (
  id            TEXT PRIMARY KEY,
  session_id    TEXT,
  email         TEXT,
  page          TEXT,
  utm_source    TEXT,
  utm_medium    TEXT,
  utm_campaign  TEXT,
  utm_content   TEXT,
  utm_term      TEXT,
  referrer      TEXT,
  user_agent    TEXT,
  ip_hash       TEXT,   -- hashé RGPD
  event_type    TEXT,   -- page_view|form_submit|cta_click|demo_request
  event_data    JSONB,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ── STAGES PIPELINE ──────────────────────────────────────────────────────────
-- Qualification → Démo → Proposition → Négociation → Closing → Perdu

-- ── DÉSACTIVER RLS ────────────────────────────────────────────────────────────
ALTER TABLE sahar_comptes      DISABLE ROW LEVEL SECURITY;
ALTER TABLE sahar_contacts     DISABLE ROW LEVEL SECURITY;
ALTER TABLE sahar_opportunites DISABLE ROW LEVEL SECURITY;
ALTER TABLE sahar_activites    DISABLE ROW LEVEL SECURITY;
ALTER TABLE sahar_deals        DISABLE ROW LEVEL SECURITY;
ALTER TABLE sahar_factures     DISABLE ROW LEVEL SECURITY;
ALTER TABLE sahar_tracking     DISABLE ROW LEVEL SECURITY;

-- ── TRIGGERS updated_at ───────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_comptes_updated') THEN
    CREATE TRIGGER trg_comptes_updated BEFORE UPDATE ON sahar_comptes FOR EACH ROW EXECUTE FUNCTION update_updated_at();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_opps_updated') THEN
    CREATE TRIGGER trg_opps_updated BEFORE UPDATE ON sahar_opportunites FOR EACH ROW EXECUTE FUNCTION update_updated_at();
  END IF;
END $$;

-- ── VUES ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW sahar_pipeline_view AS
SELECT
  o.id, o.titre, o.offre, o.valeur, o.probabilite,
  o.valeur_ponderee, o.stage, o.date_closing_prevu,
  o.utm_source, o.utm_campaign, o.source,
  c.nom AS compte_nom, c.secteur, c.email AS compte_email,
  ct.prenom || ' ' || ct.nom AS contact_nom, ct.tel AS contact_tel,
  COUNT(a.id) AS nb_activites,
  MAX(a.date_creation) AS derniere_activite
FROM sahar_opportunites o
LEFT JOIN sahar_comptes c ON o.compte_id = c.id
LEFT JOIN sahar_contacts ct ON o.contact_id = ct.id
LEFT JOIN sahar_activites a ON a.opp_id = o.id
GROUP BY o.id, o.titre, o.offre, o.valeur, o.probabilite,
         o.valeur_ponderee, o.stage, o.date_closing_prevu,
         o.utm_source, o.utm_campaign, o.source,
         c.nom, c.secteur, c.email,
         ct.prenom, ct.nom, ct.tel;

CREATE OR REPLACE VIEW sahar_kpis_view AS
SELECT
  COUNT(*) FILTER (WHERE stage NOT IN ('Closing','Perdu')) AS opps_actives,
  COUNT(*) FILTER (WHERE stage = 'Closing')                AS opps_closing,
  COUNT(*) FILTER (WHERE stage = 'Perdu')                  AS opps_perdues,
  SUM(valeur) FILTER (WHERE stage NOT IN ('Closing','Perdu')) AS pipeline_brut,
  SUM(valeur_ponderee) FILTER (WHERE stage NOT IN ('Perdu'))  AS pipeline_pondere,
  SUM(valeur) FILTER (WHERE stage = 'Closing')             AS valeur_gagnee,
  ROUND(AVG(probabilite))                                  AS probabilite_moy,
  COUNT(DISTINCT compte_id)                                AS nb_comptes
FROM sahar_opportunites;

CREATE OR REPLACE VIEW sahar_mrr_view AS
SELECT
  SUM(montant) FILTER (WHERE recurrence = 'mensuel' AND statut = 'actif') AS mrr,
  SUM(montant) FILTER (WHERE recurrence = 'annuel'  AND statut = 'actif') / 12 AS arr_mensuel,
  COUNT(*) FILTER (WHERE statut = 'actif')   AS clients_actifs,
  COUNT(*) FILTER (WHERE statut = 'résilié') AS churned
FROM sahar_deals;

SELECT 'SAHAR CRM interne créé ✓' AS statut;
