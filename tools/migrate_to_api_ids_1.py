#!/usr/bin/env python3
"""
SumoSim: Migrate wrestler_id from shikona-based to numeric API ID.

This is a one-time migration that:
1. Fetches the banzuke from sumo-api.com to get real API IDs
2. Builds a mapping: old_id (e.g. "hoshoryu") -> new_id (e.g. "19")
3. Updates all tables in both Supabase and local SQLite

Usage:
    python -m tools.migrate_to_api_ids --basho 202603
    python -m tools.migrate_to_api_ids --basho 202603 --dry-run
    python -m tools.migrate_to_api_ids --basho 202603 --apply
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

API_BASE = "https://www.sumo-api.com"


def fetch_banzuke_ids(basho_id: str) -> dict[str, dict]:
    """
    Fetch banzuke and build mapping from shikona -> {api_id, shikona, shikona_jp, ...}.
    """
    import httpx

    url = f"{API_BASE}/api/basho/{basho_id}/banzuke/Makuuchi"
    print(f"Fetching banzuke for {basho_id}...")
    resp = httpx.get(url, timeout=15.0)
    resp.raise_for_status()
    data = resp.json()

    mapping = {}
    for side in ["east", "west"]:
        for entry in data.get(side, []):
            api_id = entry.get("rikishiID", entry.get("rikishiId"))
            shikona_en = entry.get("shikonaEn", entry.get("shikona_en", ""))
            shikona_jp = entry.get("shikonaJp", entry.get("shikona_jp", ""))

            if not api_id or not shikona_en:
                continue

            old_id = shikona_en.lower().replace(" ", "").replace("-", "")
            mapping[old_id] = {
                "api_id": api_id,
                "new_id": str(api_id),
                "shikona": shikona_en,
                "shikona_jp": shikona_jp,
                "old_id": old_id,
            }

    print(f"  Found {len(mapping)} wrestlers")
    return mapping


def fetch_rikishi_detail(api_id: int) -> dict | None:
    """Fetch full rikishi detail for shikona_full / disambiguation."""
    import httpx
    try:
        resp = httpx.get(f"{API_BASE}/api/rikishi/{api_id}", timeout=15.0)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def migrate_local_sqlite(mapping: dict[str, dict], dry_run: bool = False):
    """Migrate local SQLite database to use numeric IDs."""
    import sqlite3
    from data.db import SumoDatabase

    db = SumoDatabase()
    db_path = db._db_path

    if not db_path.exists():
        print("  Local DB does not exist — nothing to migrate")
        return

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Check how many records exist
    for table in ["wrestlers", "banzuke", "tournament_records", "bout_records", "injury_notes"]:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count} rows")
        except Exception:
            print(f"  {table}: not found")

    if dry_run:
        print("\n  DRY RUN — showing what would change:")
        for old_id, info in sorted(mapping.items()):
            print(f"    {old_id} -> {info['new_id']} ({info['shikona']})")
        conn.close()
        return

    print("\n  Migrating local SQLite...")

    # Disable FK constraints during migration
    conn.execute("PRAGMA foreign_keys = OFF")

    for old_id, info in mapping.items():
        new_id = info["new_id"]
        if old_id == new_id:
            continue

        # wrestlers table
        conn.execute("UPDATE wrestlers SET wrestler_id = ? WHERE wrestler_id = ?",
                     (new_id, old_id))

        # banzuke
        conn.execute("UPDATE banzuke SET wrestler_id = ? WHERE wrestler_id = ?",
                     (new_id, old_id))

        # tournament_records
        conn.execute("UPDATE tournament_records SET wrestler_id = ? WHERE wrestler_id = ?",
                     (new_id, old_id))

        # bout_records — 3 ID columns
        conn.execute("UPDATE bout_records SET east_id = ? WHERE east_id = ?",
                     (new_id, old_id))
        conn.execute("UPDATE bout_records SET west_id = ? WHERE west_id = ?",
                     (new_id, old_id))
        conn.execute("UPDATE bout_records SET winner_id = ? WHERE winner_id = ?",
                     (new_id, old_id))

        # injury_notes
        conn.execute("UPDATE injury_notes SET wrestler_id = ? WHERE wrestler_id = ?",
                     (new_id, old_id))

    # Also update shikona_jp and api_id on wrestlers table
    for old_id, info in mapping.items():
        new_id = info["new_id"]
        conn.execute(
            "UPDATE wrestlers SET api_id = ?, shikona_jp = ? WHERE wrestler_id = ?",
            (info["api_id"], info.get("shikona_jp"), new_id)
        )

    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()
    conn.close()
    print("  Local migration complete")


def migrate_supabase(mapping: dict[str, dict], db, dry_run: bool = False):
    """Migrate Supabase tables to use numeric IDs."""
    if not db.is_online:
        print("  Supabase not connected — skipping remote migration")
        return

    if dry_run:
        print("  DRY RUN — Supabase migration would update all tables")
        return

    print("\n  Migrating Supabase...")
    print("  NOTE: Supabase migration requires running SQL directly.")
    print("  Copy and run the following SQL in the Supabase SQL Editor:\n")

    # Generate SQL migration script
    sql_lines = [
        "-- SumoSim: Migrate wrestler_id from shikona to numeric API ID",
        "-- Generated by tools/migrate_to_api_ids.py",
        "-- Run this in the Supabase SQL Editor",
        "",
        "BEGIN;",
        "",
        "-- Temporarily disable FK constraints",
        "ALTER TABLE banzuke DROP CONSTRAINT IF EXISTS banzuke_wrestler_id_fkey;",
        "ALTER TABLE tournament_records DROP CONSTRAINT IF EXISTS tournament_records_wrestler_id_fkey;",
        "ALTER TABLE bout_records DROP CONSTRAINT IF EXISTS bout_records_east_id_fkey;",
        "ALTER TABLE bout_records DROP CONSTRAINT IF EXISTS bout_records_west_id_fkey;",
        "ALTER TABLE bout_records DROP CONSTRAINT IF EXISTS bout_records_winner_id_fkey;",
        "ALTER TABLE injury_notes DROP CONSTRAINT IF EXISTS injury_notes_wrestler_id_fkey;",
        "",
    ]

    for old_id, info in sorted(mapping.items()):
        new_id = info["new_id"]
        shikona_jp = info.get("shikona_jp", "").replace("'", "''")
        api_id = info["api_id"]

        sql_lines.append(f"-- {info['shikona']}: {old_id} -> {new_id}")
        sql_lines.append(f"UPDATE wrestlers SET wrestler_id = '{new_id}', api_id = {api_id}, shikona_jp = '{shikona_jp}' WHERE wrestler_id = '{old_id}';")
        sql_lines.append(f"UPDATE banzuke SET wrestler_id = '{new_id}' WHERE wrestler_id = '{old_id}';")
        sql_lines.append(f"UPDATE tournament_records SET wrestler_id = '{new_id}' WHERE wrestler_id = '{old_id}';")
        sql_lines.append(f"UPDATE bout_records SET east_id = '{new_id}' WHERE east_id = '{old_id}';")
        sql_lines.append(f"UPDATE bout_records SET west_id = '{new_id}' WHERE west_id = '{old_id}';")
        sql_lines.append(f"UPDATE bout_records SET winner_id = '{new_id}' WHERE winner_id = '{old_id}';")
        sql_lines.append(f"UPDATE injury_notes SET wrestler_id = '{new_id}' WHERE wrestler_id = '{old_id}';")
        sql_lines.append("")

    sql_lines.extend([
        "-- Restore FK constraints",
        "ALTER TABLE banzuke ADD CONSTRAINT banzuke_wrestler_id_fkey FOREIGN KEY (wrestler_id) REFERENCES wrestlers(wrestler_id);",
        "ALTER TABLE tournament_records ADD CONSTRAINT tournament_records_wrestler_id_fkey FOREIGN KEY (wrestler_id) REFERENCES wrestlers(wrestler_id);",
        "ALTER TABLE bout_records ADD CONSTRAINT bout_records_east_id_fkey FOREIGN KEY (east_id) REFERENCES wrestlers(wrestler_id);",
        "ALTER TABLE bout_records ADD CONSTRAINT bout_records_west_id_fkey FOREIGN KEY (west_id) REFERENCES wrestlers(wrestler_id);",
        "ALTER TABLE bout_records ADD CONSTRAINT bout_records_winner_id_fkey FOREIGN KEY (winner_id) REFERENCES wrestlers(wrestler_id);",
        "ALTER TABLE injury_notes ADD CONSTRAINT injury_notes_wrestler_id_fkey FOREIGN KEY (wrestler_id) REFERENCES wrestlers(wrestler_id);",
        "",
        "COMMIT;",
    ])

    sql_text = "\n".join(sql_lines)

    # Save to file
    output_path = Path(__file__).parent.parent / "data" / "migrate_ids.sql"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(sql_text)
    print(f"  SQL migration saved to: {output_path}")
    print(f"  Run it in the Supabase SQL Editor to complete the migration.")


def update_haru2026_file(mapping: dict[str, dict], dry_run: bool = False):
    """Update haru_2026.py to use numeric IDs."""
    haru_path = Path(__file__).parent.parent / "data" / "haru_2026.py"
    if not haru_path.exists():
        print("  haru_2026.py not found — skipping")
        return

    content = haru_path.read_text(encoding="utf-8")
    changes = 0

    for old_id, info in mapping.items():
        new_id = info["new_id"]
        # Replace wrestler_id="old_id" with wrestler_id="new_id"
        old_pattern = f'wrestler_id="{old_id}"'
        new_pattern = f'wrestler_id="{new_id}"'
        if old_pattern in content:
            content = content.replace(old_pattern, new_pattern)
            changes += 1

    # Also update TournamentRecord wrestler_ids
    for old_id, info in mapping.items():
        new_id = info["new_id"]
        old_rec = f'wrestler_id="{old_id}"'
        new_rec = f'wrestler_id="{new_id}"'
        # Already handled above

        # Update dict keys: records["old_id"]
        old_key = f'records["{old_id}"]'
        new_key = f'records["{new_id}"]'
        if old_key in content:
            content = content.replace(old_key, new_key)
            changes += 1

        # Update injury notes keys
        old_inj = f'"{old_id}": {{'
        new_inj = f'"{new_id}": {{'
        if old_inj in content:
            content = content.replace(old_inj, new_inj)
            changes += 1

    if dry_run:
        print(f"  DRY RUN — would make {changes} replacements in haru_2026.py")
        return

    haru_path.write_text(content, encoding="utf-8")
    print(f"  Updated haru_2026.py: {changes} replacements")


def save_mapping(mapping: dict[str, dict]):
    """Save the ID mapping for reference."""
    output_path = Path(__file__).parent.parent / "data" / "id_mapping.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)
    print(f"  ID mapping saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate wrestler_id from shikona to numeric API ID"
    )
    parser.add_argument("--basho", default="202603",
                        help="Basho to fetch banzuke from (default: 202603)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing")
    parser.add_argument("--apply", action="store_true",
                        help="Actually perform the migration")

    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("Use --dry-run to preview or --apply to execute the migration.")
        sys.exit(1)

    # Load .env
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                import os
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

    # Step 1: Get the mapping
    mapping = fetch_banzuke_ids(args.basho)
    save_mapping(mapping)

    # Step 2: Preview/apply
    print(f"\nMapping ({len(mapping)} wrestlers):")
    for old_id, info in sorted(mapping.items(), key=lambda x: x[1]["api_id"]):
        jp = info.get('shikona_jp', '')
        try:
            print(f"  {old_id:<20} -> {info['new_id']:<8} ({info['shikona']} / {jp})")
        except UnicodeEncodeError:
            print(f"  {old_id:<20} -> {info['new_id']:<8} ({info['shikona']})")

    print()

    # Step 3: Migrate local SQLite
    migrate_local_sqlite(mapping, dry_run=args.dry_run)

    # Step 4: Generate Supabase SQL
    from data.db import SumoDatabase
    db = SumoDatabase()
    migrate_supabase(mapping, db, dry_run=args.dry_run)

    # Step 5: Update haru_2026.py
    update_haru2026_file(mapping, dry_run=args.dry_run)

    if args.dry_run:
        print("\nDRY RUN complete. Use --apply to execute.")
    else:
        print("\nMigration complete!")
        print("NEXT STEPS:")
        print("  1. Run data/migrate_ids.sql in the Supabase SQL Editor")
        print("  2. Delete data/sumosim_local.db and re-run: python -m tools.db_manage migrate")
        print("  3. Update tools/scrape_h2h.py output to use numeric IDs")
        print("  4. Re-run the H2H scraper if needed")


if __name__ == "__main__":
    main()
