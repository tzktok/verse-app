CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY,
  google_sub text NOT NULL UNIQUE,
  email text NOT NULL,
  display_name text NOT NULL,
  credits integer NOT NULL DEFAULT 100 CHECK (credits >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS sessions (
  token_hash text PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sessions_user_id_idx ON sessions(user_id);
CREATE TABLE IF NOT EXISTS songs (
  id uuid PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  replicate_id text UNIQUE,
  title text NOT NULL,
  style text NOT NULL,
  status text NOT NULL CHECK (status IN ('submitting','starting','processing','succeeded','failed','canceled')),
  audio_url text,
  charged boolean NOT NULL DEFAULT true,
  error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS songs_user_created_idx ON songs(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS songs_active_idx ON songs(status) WHERE status IN ('submitting','starting','processing');
CREATE TABLE IF NOT EXISTS credit_events (
  id uuid PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  amount integer NOT NULL CHECK (amount <> 0),
  kind text NOT NULL CHECK (kind IN ('welcome','song','refund','admin_topup')),
  reference text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS credit_events_user_created_idx ON credit_events(user_id, created_at DESC);

ALTER TABLE songs ADD COLUMN IF NOT EXISTS storage_key text;
ALTER TABLE songs ADD COLUMN IF NOT EXISTS source_url text;
ALTER TABLE songs ADD COLUMN IF NOT EXISTS settings jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE songs ADD COLUMN IF NOT EXISTS request_id uuid;
ALTER TABLE songs DROP CONSTRAINT IF EXISTS songs_status_check;
ALTER TABLE songs ADD CONSTRAINT songs_status_check CHECK (status IN ('submitting','starting','processing','saving','succeeded','failed','canceled'));
CREATE UNIQUE INDEX IF NOT EXISTS songs_request_idx ON songs(user_id, request_id) WHERE request_id IS NOT NULL;

-- Run directly in PostgreSQL after confirming a manual payment. A reference
-- can be used only once, so retrying the same grant cannot add credits twice.
CREATE OR REPLACE FUNCTION grant_manual_credits(
  p_user_id uuid, p_credits integer, p_reference text
) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  granted_user_id uuid;
  new_balance integer;
BEGIN
  IF p_credits IS NULL OR p_credits < 1 OR p_credits > 1000000 THEN
    RAISE EXCEPTION 'Credits must be between 1 and 1,000,000';
  END IF;
  IF p_reference IS NULL OR length(trim(p_reference)) < 4 OR length(p_reference) > 100 THEN
    RAISE EXCEPTION 'A unique reference of 4 to 100 characters is required';
  END IF;

  INSERT INTO credit_events(id, user_id, amount, kind, reference)
  SELECT gen_random_uuid(), id, p_credits, 'admin_topup', 'manual:' || p_reference
  FROM users WHERE id = p_user_id
  ON CONFLICT (reference) DO NOTHING
  RETURNING user_id INTO granted_user_id;

  IF granted_user_id IS NULL THEN
    RAISE EXCEPTION 'Account not found or reference already used';
  END IF;

  UPDATE users SET credits = credits + p_credits, updated_at = now()
  WHERE id = granted_user_id
  RETURNING credits INTO new_balance;
  RETURN new_balance;
END;
$$;
