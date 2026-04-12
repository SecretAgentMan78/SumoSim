-- ============================================================
-- SumoSim: Family Relations Table
--
-- Stores family relationships between wrestlers.
-- Manually maintained through the app's dossier panel.
-- ============================================================

CREATE TABLE IF NOT EXISTS family_relations (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    wrestler_id     TEXT NOT NULL REFERENCES wrestlers(wrestler_id),
    related_id      TEXT NOT NULL REFERENCES wrestlers(wrestler_id),
    relationship    TEXT NOT NULL,  -- 'uncle', 'father', 'brother', 'grandfather', 'cousin', 'nephew', 'son'
    UNIQUE (wrestler_id, related_id)
);

CREATE INDEX IF NOT EXISTS idx_family_wrestler ON family_relations(wrestler_id);
CREATE INDEX IF NOT EXISTS idx_family_related ON family_relations(related_id);
