-- ============================================================
-- SumoSim: Rikishi Dossier Schema Extensions
--
-- Adds fields needed for the rikishi profile/dossier feature.
-- Run this AFTER the base schema (supabase_schema.sql).
-- Safe to run multiple times.
-- ============================================================

-- Extend wrestlers table with dossier fields
DO $$
BEGIN
    -- Kanji shikona
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='shikona_jp') THEN
        ALTER TABLE wrestlers ADD COLUMN shikona_jp TEXT;
    END IF;

    -- Prefecture (for Japanese wrestlers)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='prefecture') THEN
        ALTER TABLE wrestlers ADD COLUMN prefecture TEXT;
    END IF;

    -- Sumo API numeric ID (for cross-referencing)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='api_id') THEN
        ALTER TABLE wrestlers ADD COLUMN api_id INTEGER;
    END IF;

    -- Highest rank achieved
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='highest_rank') THEN
        ALTER TABLE wrestlers ADD COLUMN highest_rank TEXT;
    END IF;

    -- Highest rank number
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='highest_rank_number') THEN
        ALTER TABLE wrestlers ADD COLUMN highest_rank_number SMALLINT;
    END IF;

    -- Active/retired status
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='is_active') THEN
        ALTER TABLE wrestlers ADD COLUMN is_active BOOLEAN DEFAULT TRUE;
    END IF;

    -- Retirement date
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='retired_date') THEN
        ALTER TABLE wrestlers ADD COLUMN retired_date DATE;
    END IF;

    -- Debut basho
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='debut_basho') THEN
        ALTER TABLE wrestlers ADD COLUMN debut_basho TEXT;
    END IF;

    -- Career totals (denormalized for quick display)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='career_wins') THEN
        ALTER TABLE wrestlers ADD COLUMN career_wins INTEGER DEFAULT 0;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='career_losses') THEN
        ALTER TABLE wrestlers ADD COLUMN career_losses INTEGER DEFAULT 0;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='career_absences') THEN
        ALTER TABLE wrestlers ADD COLUMN career_absences INTEGER DEFAULT 0;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='wrestlers' AND column_name='total_yusho') THEN
        ALTER TABLE wrestlers ADD COLUMN total_yusho INTEGER DEFAULT 0;
    END IF;
END $$;

-- Index for active/retired filtering
CREATE INDEX IF NOT EXISTS idx_wrestlers_active ON wrestlers(is_active);
CREATE INDEX IF NOT EXISTS idx_wrestlers_api_id ON wrestlers(api_id);

-- ============================================================
-- View: Rikishi dossier — joins career stats for display
-- ============================================================
CREATE OR REPLACE VIEW rikishi_dossier AS
SELECT
    w.*,
    -- Top 3 kimarite from bout history
    (SELECT jsonb_agg(t) FROM (
        SELECT kimarite, COUNT(*) as count
        FROM bout_records
        WHERE winner_id = w.wrestler_id AND kimarite IS NOT NULL
        GROUP BY kimarite
        ORDER BY count DESC
        LIMIT 3
    ) t) AS top_kimarite,
    -- Last 5 basho records
    (SELECT jsonb_agg(t ORDER BY t.basho_id DESC) FROM (
        SELECT basho_id, rank, rank_number, wins, losses, absences,
               is_yusho, special_prizes
        FROM tournament_records
        WHERE wrestler_id = w.wrestler_id
        ORDER BY basho_id DESC
        LIMIT 5
    ) t) AS recent_basho
FROM wrestlers w;
