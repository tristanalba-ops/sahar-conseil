-- =============================================================================
-- SAHAR Conseil -- Migration: data_georisques
-- Created: 2026-04-03
-- Source : GeoRisques API v1 (georisques.gouv.fr)
--
-- Endpoints utilises :
--   /gaspar/risques?code_insee={code}  -> liste risques par commune
--   /zonage_sismique?code_insee={code} -> zone sismique 1-5
--   /radon?code_insee={code}           -> classe radon 1-3
--
-- Deja applique via exec_sql RPC le 2026-04-03.
-- Ce fichier est pour le controle de version.
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.data_georisques (
    code_commune       TEXT PRIMARY KEY,
    risque_inondation  BOOLEAN   NOT NULL DEFAULT FALSE,
    risque_argile      BOOLEAN   NOT NULL DEFAULT FALSE,
    risque_seisme      SMALLINT  NOT NULL DEFAULT 1 CHECK (risque_seisme BETWEEN 1 AND 5),
    risque_radon       SMALLINT  NOT NULL DEFAULT 1 CHECK (risque_radon BETWEEN 1 AND 3),
    nb_risques_total   SMALLINT  NOT NULL DEFAULT 0,
    pprn_prescrit      BOOLEAN   NOT NULL DEFAULT FALSE,
    pprt_prescrit      BOOLEAN   NOT NULL DEFAULT FALSE,
    source             TEXT      NOT NULL DEFAULT 'GeoRisques API v1',
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE public.data_georisques IS
    'Risques naturels et technologiques par commune (GeoRisques API v1)';

COMMENT ON COLUMN public.data_georisques.risque_seisme IS
    'Zone sismique 1 (tres faible) a 5 (tres forte)';

COMMENT ON COLUMN public.data_georisques.risque_radon IS
    'Classe radon 1 (faible) a 3 (significatif)';

COMMENT ON COLUMN public.data_georisques.pprn_prescrit IS
    'Plan de Prevention des Risques Naturels prescrit (approx: risque naturel majeur present)';

COMMENT ON COLUMN public.data_georisques.pprt_prescrit IS
    'Plan de Prevention des Risques Technologiques prescrit (approx: risque industriel/nucleaire present)';

-- RLS
ALTER TABLE public.data_georisques ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS data_georisques_anon_select ON public.data_georisques;
CREATE POLICY data_georisques_anon_select
    ON public.data_georisques
    FOR SELECT TO anon
    USING (true);

GRANT SELECT ON public.data_georisques TO anon;
GRANT SELECT ON public.data_georisques TO authenticated;
