#!/usr/bin/env python3
"""
Push local SQLite data to Supabase.

Usage:
    python -m tools.push_to_supabase                    # push all tables
    python -m tools.push_to_supabase basho_entries       # push one table
    python -m tools.push_to_supabase basho_entries bout_records  # push specific tables

Prerequisites:
    1. Run supabase_sync_migration.sql in the Supabase SQL Editor first
    2. Set SUPABASE_URL and SUPABASE_KEY in .env
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def load_env():
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        import os
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def main():
    load_env()

    from data.db import SumoDatabase

    db = SumoDatabase()

    if not db.is_online:
        print("ERROR: Cannot push — Supabase is not connected.")
        print("Check SUPABASE_URL and SUPABASE_KEY in .env")
        sys.exit(1)

    # Parse table names from CLI args
    tables = sys.argv[1:] if len(sys.argv) > 1 else None

    if tables:
        print(f"Pushing tables: {', '.join(tables)}")
    else:
        print("Pushing all data tables to Supabase...")

    results = db.push_local_to_supabase(tables)

    print(f"\nPush results:")
    for table, count in results.items():
        status = f"{count} rows" if count >= 0 else "FAILED"
        print(f"  {table}: {status}")

    # Quick verify — count what's in Supabase now
    print(f"\nVerifying Supabase row counts...")
    for table in (tables or ["wrestlers", "basho_entries", "bout_records",
                             "injury_notes", "family_relations", "modifier_overrides"]):
        try:
            resp = db._http.get(
                f"/{table}",
                params={"select": "count", "limit": "0"},
                headers={**db._http.headers, "Prefer": "count=exact"},
            )
            count = resp.headers.get("content-range", "unknown")
            print(f"  {table}: {count}")
        except Exception as e:
            print(f"  {table}: error — {e}")


if __name__ == "__main__":
    main()
