-- ============================================================
-- SAHAR Conseil — Supabase Schema complet
-- Copier-coller dans Supabase > SQL Editor > Run
-- ============================================================

-- CONTACTS
CREATE TABLE IF NOT EXISTS crm_contacts (
  id            TEXT PRIMARY KEY,
  nom           TEXT NOT NULL,
  email         TEXT,
  tel           TEXT,
  type          TEXT DEFAULT 'Autre',
  notes         TEXT,
  source        TEXT DEFAULT 'Manuel',
  score_lead    INTEGER DEFAULT 0,
  date_creation TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- OPPORTUNITÉS
CREATE TABLE IF NOT EXISTS crm_opportunites (
  id            TEXT PRIMARY KEY,
  contact_id    TEXT REFERENCES crm_contacts(id) ON DELETE SET NULL,
  titre         TEXT NOT NULL,
  adresse       TEXT,
  type_bien     TEXT,
  surface       NUMERIC,
  prix          NUMERIC,
  prix_m2       NUMERIC,
  score         INTEGER DEFAULT 0,
  source        TEXT DEFAULT 'DVF',
  stage         TEXT DEFAULT 'Détecté',
  valeur_deal   NUMERIC DEFAULT 0,
  probabilite   INTEGER DEFAULT 10,
  date_creation TEXT,
  date_update   TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ACTIVITÉS
CREATE TABLE IF NOT EXISTS crm_activites (
  id            TEXT PRIMARY KEY,
  opp_id        TEXT REFERENCES crm_opportunites(id) ON DELETE CASCADE,
  contact_id    TEXT REFERENCES crm_contacts(id) ON DELETE SET NULL,
  type          TEXT NOT NULL,
  notes         TEXT,
  statut        TEXT DEFAULT 'À faire',
  date          TEXT,
  date_creation TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- SÉQUENCES EMAIL
CREATE TABLE IF NOT EXISTS crm_sequences (
  id            TEXT PRIMARY KEY,
  nom           TEXT NOT NULL,
  secteur       TEXT,
  actif         BOOLEAN DEFAULT TRUE,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ÉTAPES DE SÉQUENCE
CREATE TABLE IF NOT EXISTS crm_sequence_steps (
  id            TEXT PRIMARY KEY,
  sequence_id   TEXT REFERENCES crm_sequences(id) ON DELETE CASCADE,
  ordre         INTEGER NOT NULL,
  delai_jours   INTEGER DEFAULT 0,
  canal         TEXT DEFAULT 'email',
  sujet         TEXT,
  contenu       TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ENVOIS (log des messages)
CREATE TABLE IF NOT EXISTS crm_envois (
  id            TEXT PRIMARY KEY,
  contact_id    TEXT REFERENCES crm_contacts(id) ON DELETE CASCADE,
  sequence_id   TEXT REFERENCES crm_sequences(id) ON DELETE SET NULL,
  step_id       TEXT REFERENCES crm_sequence_steps(id) ON DELETE SET NULL,
  canal         TEXT,
  statut        TEXT DEFAULT 'envoyé',
  date_envoi    TIMESTAMPTZ DEFAULT NOW()
);

-- LEADS ENTRANTS (depuis formulaire site)
CREATE TABLE IF NOT EXISTS crm_leads (
  id            TEXT PRIMARY KEY,
  nom           TEXT,
  email         TEXT NOT NULL,
  tel           TEXT,
  secteur       TEXT,
  message       TEXT,
  source        TEXT DEFAULT 'site',
  statut        TEXT DEFAULT 'nouveau',
  contact_id    TEXT REFERENCES crm_contacts(id) ON DELETE SET NULL,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ── TRIGGERS updated_at ───────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_contacts_updated
  BEFORE UPDATE ON crm_contacts
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_opportunites_updated
  BEFORE UPDATE ON crm_opportunites
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ── RLS (Row Level Security) ──────────────────────────────────────────────────
ALTER TABLE crm_contacts       ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_opportunites   ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_activites      ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_sequences      ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_sequence_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_envois         ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_leads          ENABLE ROW LEVEL SECURITY;

-- Policies : accès complet via service_role (anon key de l'app)
DO $$
DECLARE t TEXT;
BEGIN
  FOR t IN SELECT unnest(ARRAY[
    'crm_contacts','crm_opportunites','crm_activites',
    'crm_sequences','crm_sequence_steps','crm_envois','crm_leads'
  ]) LOOP
    EXECUTE format(
      'CREATE POLICY "allow_all_%s" ON %s FOR ALL USING (true) WITH CHECK (true)',
      t, t
    );
  END LOOP;
END $$;

-- ── DONNÉES INITIALES — séquences exemple ────────────────────────────────────
INSERT INTO crm_sequences (id, nom, secteur, actif) VALUES
  ('SEQ001', 'Bienvenue Immobilier', 'immobilier', true),
  ('SEQ002', 'Bienvenue Énergie RGE', 'energie', true),
  ('SEQ003', 'Nurturing Retail', 'retail', true)
ON CONFLICT (id) DO NOTHING;

INSERT INTO crm_sequence_steps (id, sequence_id, ordre, delai_jours, canal, sujet, contenu) VALUES
  ('S001E01','SEQ001',1,0,'email',
   'Bienvenue sur SAHAR — vos données DVF sont prêtes',
   'Bonjour {nom},\n\nVotre accès à DVF Analyse Pro est activé.\n\nVoici ce que vous pouvez faire dès maintenant :\n- Analyser les transactions de votre secteur\n- Détecter les biens sous-valorisés\n- Exporter vos prospects en Excel\n\nConnectez-vous : {lien_app}\n\nL''équipe SAHAR'),
  ('S001E02','SEQ001',2,3,'email',
   'Avez-vous testé le scoring DVF ?',
   'Bonjour {nom},\n\nIl y a 3 jours vous avez rejoint SAHAR.\n\nUne question rapide : avez-vous eu le temps de tester le scoring sur votre secteur ?\n\nSi vous avez besoin d''aide, répondez directement à cet email.\n\nL''équipe SAHAR'),
  ('S001E03','SEQ001',3,7,'email',
   'Rapport de marché offert — {commune}',
   'Bonjour {nom},\n\nOn vous a préparé un rapport de marché sur votre zone.\n\nLes 3 points clés du marché actuel :\n- Prix médian : en cours de calcul\n- Volume de transactions : actif\n- Opportunités score >70 : disponibles\n\nConsultez votre tableau de bord : {lien_app}\n\nL''équipe SAHAR'),
  ('S002E01','SEQ002',1,0,'email',
   'Vos prospects DPE F/G sont prêts',
   'Bonjour {nom},\n\nVotre accès au DPE Scanner est activé.\n\nDans votre secteur, nous avons identifié des logements classés F et G — passoires thermiques concernées par les interdictions de location 2025.\n\nConnectez-vous pour voir la liste : {lien_app}\n\nL''équipe SAHAR'),
  ('S002E02','SEQ002',2,4,'sms',
   NULL,
   'SAHAR - Bonjour {nom}, 3 nouveaux prospects rénovation détectés dans votre secteur. Consultez votre espace : {lien_app}'),
  ('S003E01','SEQ003',1,0,'email',
   'Votre score d''attractivité de zone est prêt',
   'Bonjour {nom},\n\nVotre analyse Zone Score est disponible.\n\nNous avons calculé le potentiel commercial de votre zone cible.\n\nAccédez au rapport : {lien_app}\n\nL''équipe SAHAR')
ON CONFLICT (id) DO NOTHING;

-- ── VUE PIPELINE ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vue_pipeline AS
SELECT
  o.id,
  o.titre,
  o.stage,
  o.prix,
  o.score,
  o.date_creation,
  c.nom AS contact_nom,
  c.email AS contact_email,
  c.tel AS contact_tel,
  COUNT(a.id) AS nb_activites,
  MAX(a.date_creation) AS derniere_activite
FROM crm_opportunites o
LEFT JOIN crm_contacts c ON o.contact_id = c.id
LEFT JOIN crm_activites a ON a.opp_id = o.id
GROUP BY o.id, o.titre, o.stage, o.prix, o.score, o.date_creation,
         c.nom, c.email, c.tel;

-- ── VUE KPIs ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vue_kpis AS
SELECT
  COUNT(*) FILTER (WHERE stage = 'Détecté')    AS nb_detectes,
  COUNT(*) FILTER (WHERE stage = 'Contacté')   AS nb_contactes,
  COUNT(*) FILTER (WHERE stage = 'Qualifié')   AS nb_qualifies,
  COUNT(*) FILTER (WHERE stage = 'Proposition') AS nb_propositions,
  COUNT(*) FILTER (WHERE stage = 'Closing')    AS nb_closing,
  COUNT(*)                                      AS total_opps,
  ROUND(AVG(score))                             AS score_moyen,
  SUM(prix)                                     AS valeur_pipeline,
  SUM(prix) FILTER (WHERE stage = 'Closing')   AS valeur_closing
FROM crm_opportunites;

SELECT 'Schema SAHAR créé avec succès ✓' AS statut;
