-- Manual confirmation/rejection for ad lead -> sale attribution.
-- The live analytics endpoint still computes deterministic and candidate matches,
-- while this table stores human decisions that should survive refreshes.

CREATE TABLE IF NOT EXISTS public.ad_sale_attributions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id text NOT NULL,
  trigger_message_id text NOT NULL,
  trigger_message_db_id bigint,
  remote_jid text NOT NULL,
  sale_id uuid NOT NULL REFERENCES public.sales(id) ON DELETE CASCADE,
  status text NOT NULL DEFAULT 'confirmed'
    CHECK (status IN ('confirmed', 'rejected')),
  source text NOT NULL DEFAULT 'manual'
    CHECK (source IN ('manual', 'system')),
  note text,
  created_by uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (session_id, trigger_message_id, sale_id)
);

CREATE INDEX IF NOT EXISTS idx_ad_sale_attributions_event
  ON public.ad_sale_attributions (session_id, trigger_message_id, status);

CREATE INDEX IF NOT EXISTS idx_ad_sale_attributions_sale
  ON public.ad_sale_attributions (sale_id);

ALTER TABLE public.ad_sale_attributions ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "ad_sale_attributions_authenticated_all" ON public.ad_sale_attributions;
CREATE POLICY "ad_sale_attributions_authenticated_all"
  ON public.ad_sale_attributions
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

REVOKE ALL ON public.ad_sale_attributions FROM anon;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.ad_sale_attributions TO authenticated;

CREATE OR REPLACE FUNCTION public.touch_ad_sale_attributions_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_ad_sale_attributions_updated_at ON public.ad_sale_attributions;
CREATE TRIGGER trg_ad_sale_attributions_updated_at
  BEFORE UPDATE ON public.ad_sale_attributions
  FOR EACH ROW
  EXECUTE FUNCTION public.touch_ad_sale_attributions_updated_at();
