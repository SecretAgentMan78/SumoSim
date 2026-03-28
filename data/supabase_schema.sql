-- SumoSim Supabase Schema
-- Run this in the Supabase SQL Editor to create all tables.
-- Version: 1.0 — Haru 2026

-- ============================================================
-- 1. WRESTLERS
-- Core wrestler identity. One row per wrestler (not per basho).
-- shikona and physical stats may change over time; we track
-- the current/most recent values here.
-- ============================================================
CREATE TABLE IF NOT EXISTS wrestlers (
    wrestler_id     TEXT PRIMARY KEY,
    shikona         TEXT NOT NULL,
    heya            TEXT NOT NULL,
    birth_date      DATE,
    height_cm       REAL,
    weight_kg       REAL,
    fighting_style  TEXT NOT NULL DEFAULT 'hybrid'
        CHECK (fighting_style IN ('oshi', 'yotsu', 'hybrid')),
    country         TEXT DEFAULT 'Japan',
    current_rank    TEXT,
    current_rank_number SMALLINT,
    current_side    TEXT CHECK (current_side IN ('east', 'west', NULL)),
    current_basho   TEXT,                    -- basho_id when rank was last updated
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- ============================================================
-- 2. BANZUKE
-- A wrestler's rank for a specific basho. This is the per-basho
-- roster — one row per wrestler per tournament they appear in.
-- ============================================================
CREATE TABLE IF NOT EXISTS banzuke (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basho_id        TEXT NOT NULL,           -- e.g. '2026.03'
    wrestler_id     TEXT NOT NULL REFERENCES wrestlers(wrestler_id),
    rank            TEXT NOT NULL
        CHECK (rank IN ('yokozuna', 'ozeki', 'sekiwake', 'komusubi', 'maegashira')),
    rank_number     SMALLINT,                -- e.g. 1-17 for maegashira
    side            TEXT CHECK (side IN ('east', 'west')),
    division        TEXT NOT NULL DEFAULT 'makuuchi'
        CHECK (division IN ('makuuchi', 'juryo')),
    is_kyujo        BOOLEAN DEFAULT FALSE,
    kyujo_reason    TEXT,
    UNIQUE (basho_id, wrestler_id)
);

CREATE INDEX IF NOT EXISTS idx_banzuke_basho ON banzuke(basho_id);
CREATE INDEX IF NOT EXISTS idx_banzuke_wrestler ON banzuke(wrestler_id);

-- ============================================================
-- 3. TOURNAMENT RECORDS
-- A wrestler's W-L record for a completed basho, plus prizes.
-- ============================================================
CREATE TABLE IF NOT EXISTS tournament_records (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basho_id        TEXT NOT NULL,
    wrestler_id     TEXT NOT NULL REFERENCES wrestlers(wrestler_id),
    rank            TEXT NOT NULL,
    rank_number     SMALLINT,
    wins            SMALLINT NOT NULL DEFAULT 0,
    losses          SMALLINT NOT NULL DEFAULT 0,
    absences        SMALLINT NOT NULL DEFAULT 0,
    is_yusho        BOOLEAN DEFAULT FALSE,
    is_jun_yusho    BOOLEAN DEFAULT FALSE,
    special_prizes  TEXT[] DEFAULT '{}',     -- e.g. {'shukun-sho', 'kanto-sho'}
    UNIQUE (basho_id, wrestler_id)
);

CREATE INDEX IF NOT EXISTS idx_tourney_basho ON tournament_records(basho_id);
CREATE INDEX IF NOT EXISTS idx_tourney_wrestler ON tournament_records(wrestler_id);

-- ============================================================
-- 4. BOUT RECORDS
-- Individual bout results with kimarite. This is the big table
-- that grows with every scraped basho.
-- ============================================================
CREATE TABLE IF NOT EXISTS bout_records (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basho_id        TEXT NOT NULL,
    day             SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 16),
    east_id         TEXT NOT NULL REFERENCES wrestlers(wrestler_id),
    west_id         TEXT NOT NULL REFERENCES wrestlers(wrestler_id),
    winner_id       TEXT NOT NULL REFERENCES wrestlers(wrestler_id),
    kimarite        TEXT,
    UNIQUE (basho_id, day, east_id, west_id)
);

CREATE INDEX IF NOT EXISTS idx_bout_basho ON bout_records(basho_id);
CREATE INDEX IF NOT EXISTS idx_bout_east ON bout_records(east_id);
CREATE INDEX IF NOT EXISTS idx_bout_west ON bout_records(west_id);
CREATE INDEX IF NOT EXISTS idx_bout_winner ON bout_records(winner_id);

-- ============================================================
-- 5. INJURY NOTES
-- Per-wrestler injury/health info for a specific basho.
-- Severity: 0.0 = healthy, 1.0 = severely compromised.
-- ============================================================
CREATE TABLE IF NOT EXISTS injury_notes (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basho_id        TEXT NOT NULL,
    wrestler_id     TEXT NOT NULL REFERENCES wrestlers(wrestler_id),
    severity        REAL NOT NULL DEFAULT 0.0 CHECK (severity BETWEEN 0.0 AND 1.0),
    note            TEXT,
    UNIQUE (basho_id, wrestler_id)
);

-- ============================================================
-- 6. SYNC METADATA
-- Tracks when each table was last synced to local SQLite.
-- ============================================================
CREATE TABLE IF NOT EXISTS sync_metadata (
    table_name      TEXT PRIMARY KEY,
    last_synced_at  TIMESTAMPTZ,
    row_count       INTEGER DEFAULT 0
);

-- Seed sync metadata
INSERT INTO sync_metadata (table_name) VALUES
    ('wrestlers'), ('banzuke'), ('tournament_records'),
    ('bout_records'), ('injury_notes')
ON CONFLICT (table_name) DO NOTHING;

-- ============================================================
-- 7. HELPER VIEWS
-- ============================================================

-- Current basho roster: joins wrestlers + latest banzuke
CREATE OR REPLACE VIEW current_roster AS
SELECT
    w.wrestler_id, w.shikona, w.heya, w.birth_date,
    w.height_cm, w.weight_kg, w.fighting_style, w.country,
    b.basho_id, b.rank, b.rank_number, b.side, b.division,
    b.is_kyujo, b.kyujo_reason
FROM wrestlers w
JOIN banzuke b ON w.wrestler_id = b.wrestler_id
WHERE b.basho_id = (SELECT MAX(basho_id) FROM banzuke);

-- H2H summary: win counts between every pair
CREATE OR REPLACE VIEW h2h_summary AS
SELECT
    LEAST(east_id, west_id) AS wrestler_a,
    GREATEST(east_id, west_id) AS wrestler_b,
    COUNT(*) AS total_bouts,
    COUNT(*) FILTER (WHERE winner_id = LEAST(east_id, west_id)) AS a_wins,
    COUNT(*) FILTER (WHERE winner_id = GREATEST(east_id, west_id)) AS b_wins
FROM bout_records
GROUP BY LEAST(east_id, west_id), GREATEST(east_id, west_id);

-- Kimarite stats per wrestler
CREATE OR REPLACE VIEW kimarite_stats AS
SELECT
    winner_id AS wrestler_id,
    kimarite,
    COUNT(*) AS usage_count
FROM bout_records
WHERE kimarite IS NOT NULL
GROUP BY winner_id, kimarite
ORDER BY winner_id, usage_count DESC;

-- ============================================================
-- MIGRATION: Add current rank fields to existing wrestlers table
-- Safe to run multiple times (IF NOT EXISTS / DO NOTHING).
-- ============================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wrestlers' AND column_name='current_rank') THEN
        ALTER TABLE wrestlers ADD COLUMN current_rank TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wrestlers' AND column_name='current_rank_number') THEN
        ALTER TABLE wrestlers ADD COLUMN current_rank_number SMALLINT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wrestlers' AND column_name='current_side') THEN
        ALTER TABLE wrestlers ADD COLUMN current_side TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wrestlers' AND column_name='current_basho') THEN
        ALTER TABLE wrestlers ADD COLUMN current_basho TEXT;
    END IF;
END $$;

-- Backfill current rank from latest banzuke
UPDATE wrestlers w
SET
    current_rank = b.rank,
    current_rank_number = b.rank_number,
    current_side = b.side,
    current_basho = b.basho_id
FROM banzuke b
WHERE w.wrestler_id = b.wrestler_id
  AND b.basho_id = (SELECT MAX(basho_id) FROM banzuke);
