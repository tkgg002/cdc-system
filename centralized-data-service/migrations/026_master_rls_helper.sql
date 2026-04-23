-- Migration 026 — R5 RLS helper for future master tables
-- Plan ref: §11.15 (security: multi-tenant isolation via RLS)
--
-- This migration installs a helper function that R8 DDL generator calls
-- whenever it creates a new public.<master_table>. The helper:
--   1. Enables ROW LEVEL SECURITY on the given table.
--   2. Installs a default permissive policy `USING (true)` — dev mode.
--      Production tighten via app.current_partner setting (future work).
--
-- Idempotent. Safe to run multiple times.

CREATE OR REPLACE FUNCTION cdc_internal.enable_master_rls(p_table_name TEXT)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  policy_exists BOOLEAN;
BEGIN
  -- Guard identifier (defence-in-depth, DDL generator also validates)
  IF p_table_name !~ '^[a-z_][a-z0-9_]{0,62}$' THEN
    RAISE EXCEPTION 'invalid table name: %', p_table_name;
  END IF;

  EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', p_table_name);

  SELECT EXISTS (
    SELECT 1 FROM pg_policies
     WHERE schemaname = 'public' AND tablename = p_table_name
       AND policyname = 'rls_master_default_permissive'
  ) INTO policy_exists;

  IF NOT policy_exists THEN
    EXECUTE format(
      'CREATE POLICY rls_master_default_permissive ON public.%I
        FOR ALL USING (true) WITH CHECK (true)', p_table_name);
  END IF;
END;
$$;

COMMENT ON FUNCTION cdc_internal.enable_master_rls IS
  'Enables RLS + permissive default policy on a master table. Called by
   the R8 DDL generator (master_registry_loader.go) when a new master
   is approved and materialised. Production hardening (tenant-scoped
   USING clause) is a separate migration (R8 follow-up).';
