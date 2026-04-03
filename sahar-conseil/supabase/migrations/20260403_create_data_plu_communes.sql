-- =============================================================================
-- SAHAR Conseil -- Migration: data_plu_communes
-- Created: 2026-04-03
-- Source : Geoportail de l'Urbanisme (IGN) -- apicarto.ign.fr/api/gpu/zone-urba
--
-- Zone PLU dominante par commune (U, AU, N, A) et pourcentages
-- Notes :
--   - Pourcentages calcules par COMPTAGE de zones (pas par surface)
--   - Par defaut : top 500 communes DVF, --all pour France entiere
--   - L'API IGN necessite le contournement SSL sur Windows
--
-- Deja applique via exec_sql RPC le 2026-04-03.
-- Ce fichier est pour le controle de version.
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.data_plu_communes (
    code_commune      TEXT         PRIMARY KEY,
    zone_dominante    TEXT,
    pct_zone_U        NUMERIC(5,2),
    pct_zone_AU       NUMERIC(5,2),
    pct_zone_N        NUMERIC(5,2),
    pct_zone_A        NUMERIC(5,2),
    nb_zones_total    INTEGER,
    date_approbation  DATE,
    source            TEXT         NOT NULL DEFAULT 'IGN Geoportail Urbanisme',
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE public.data_plu_communes IS
    'Zonage PLU par commune (U/AU/N/A) -- Geoportail de l Urbanisme IGN';

COMMENT ON COLUMN public.data_plu_communes.pct_zone_U IS
    'Pourcentage de zones U (urbaines) par rapport au nombre total de polygones PLU';

COMMENT ON COLUMN public.data_plu_communes.pct_zone_AU IS
    'Pourcentage de zones AU (a urbaniser)';

-- RLS
ALTER TABLE public.data_plu_communes ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS data_plu_communes_anon_select ON public.data_plu_communes;
CREATE POLICY data_plu_communes_anon_select
    ON public.data_plu_communes
    FOR SELECT TO anon
    USING (true);

GRANT SELECT ON public.data_plu_communes TO anon;
GRANT SELECT ON public.data_plu_communes TO authenticated;
