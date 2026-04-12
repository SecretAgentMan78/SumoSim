"""
tools/migrate_add_juryo.py
--------------------------
One-time migration: adds Juryo-related columns to the `wrestlers` table
and extends the scope from Makuuchi-only to all active sekitori.

Safe to run multiple times (checks for column existence first).

Usage:
    python -m tools.migrate_add_juryo
"""

import sqlite3
from pathlib import Path

DB_PATH = Path("data/sumosim_local.db")

# Columns to add if not present
NEW_COLUMNS: list[tuple[str, str]] = [
    ("division",           "TEXT NOT NULL DEFAULT 'Makuuchi'"),
    ("rank_label",         "TEXT"),
    ("rank_value",         "INTEGER"),
    ("current_basho",      "TEXT"),
    ("basho_wins",         "INTEGER DEFAULT 0"),
    ("basho_losses",       "INTEGER DEFAULT 0"),
    ("basho_absences",     "INTEGER DEFAULT 0"),
    ("makuuchi_wins",      "INTEGER DEFAULT 0"),
    ("makuuchi_losses",    "INTEGER DEFAULT 0"),
    ("makuuchi_absences",  "INTEGER DEFAULT 0"),
    ("juryo_wins",         "INTEGER DEFAULT 0"),
    ("juryo_losses",       "INTEGER DEFAULT 0"),
    ("juryo_absences",     "INTEGER DEFAULT 0"),
    ("yusho_makuuchi",     "INTEGER DEFAULT 0"),
    ("yusho_juryo",        "INTEGER DEFAULT 0"),
    ("updated_at",         "TEXT"),
]

# Indexes to create
NEW_INDEXES: list[tuple[str, str, str]] = [
    # (index_name, table, column_expression)
    ("idx_wrestlers_division",  "wrestlers", "division"),
    ("idx_wrestlers_rank_value","wrestlers", "rank_value"),
    ("idx_wrestlers_basho",     "wrestlers", "current_basho"),
]


def run() -> None:
    if not DB_PATH.exists():
        print(f"✗ DB not found at {DB_PATH}. Run `python -m tools.db_manage sync` first.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # ── Add missing columns ────────────────────────────────────────────────
    existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(wrestlers)")}
    added = 0
    for col_name, col_def in NEW_COLUMNS:
        if col_name not in existing_cols:
            cur.execute(f"ALTER TABLE wrestlers ADD COLUMN {col_name} {col_def}")
            print(f"  + Added column: {col_name}")
            added += 1

    if added == 0:
        print("  ✓ All columns already present — nothing to add.")
    else:
        print(f"  ✓ Added {added} column(s).")

    # ── Add indexes ────────────────────────────────────────────────────────
    existing_idxs = {
        row[1]
        for row in cur.execute("SELECT type, name FROM sqlite_master WHERE type='index'")
    }
    for idx_name, table, col_expr in NEW_INDEXES:
        if idx_name not in existing_idxs:
            cur.execute(f"CREATE INDEX {idx_name} ON {table} ({col_expr})")
            print(f"  + Created index: {idx_name}")

    conn.commit()
    conn.close()
    print("\n✅ Migration complete.")
    print("   Next: run `python -m tools.scrape_rikishi` to populate Juryo data.")


if __name__ == "__main__":
    run()
