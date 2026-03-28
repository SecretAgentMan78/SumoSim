#!/usr/bin/env python3
"""
SumoSim Database Management CLI

Commands:
    migrate     Load existing Python data files into the database
    sync        Pull latest from Supabase to local SQLite
    status      Show sync status and row counts
    basho       List available basho in the database

Usage:
    python -m tools.db_manage migrate              # One-time migration
    python -m tools.db_manage sync                 # Sync from Supabase
    python -m tools.db_manage status               # Show DB status
    python -m tools.db_manage basho                # List available basho

Environment variables (or .env file):
    SUPABASE_URL    Your Supabase project URL
    SUPABASE_KEY    Your Supabase anon/service key
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def load_env():
    """Load .env file if it exists."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                import os
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def cmd_migrate(args):
    from data.db import SumoDatabase

    print("Migrating existing data into database...")
    db = SumoDatabase()

    if db.is_online:
        print(f"  Connected to Supabase")
    else:
        print(f"  Offline mode — writing to local SQLite only")

    counts = db.migrate_from_haru2026()

    print(f"\nMigration results:")
    for table, count in counts.items():
        print(f"  {table}: {count} rows")
    print(f"\nLocal DB: {db._db_path}")


def cmd_sync(args):
    from data.db import SumoDatabase

    db = SumoDatabase()
    if not db.is_online:
        print("ERROR: Cannot sync — Supabase is not connected.")
        print("Check SUPABASE_URL and SUPABASE_KEY environment variables.")
        sys.exit(1)

    print("Syncing from Supabase to local SQLite...")
    results = db.sync_all()

    print(f"\nSync results:")
    for table, count in results.items():
        print(f"  {table}: {count} rows")


def cmd_status(args):
    from data.db import SumoDatabase

    db = SumoDatabase()
    print(f"Online: {db.is_online}")
    print(f"Local DB: {db._db_path}")
    print(f"DB exists: {db._db_path.exists()}")

    if db._db_path.exists():
        import sqlite3
        conn = sqlite3.connect(str(db._db_path))
        print(f"\nTable row counts:")
        for table in ["wrestlers", "banzuke", "tournament_records", "bout_records", "injury_notes"]:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {table}: {count}")
            except Exception:
                print(f"  {table}: (not created)")
        conn.close()

    status = db.get_sync_status()
    if status:
        print(f"\nSync status:")
        for table, info in status.items():
            synced = info["last_synced_at"] or "never"
            print(f"  {table}: synced {synced} ({info['row_count']} rows)")


def cmd_basho(args):
    from data.db import SumoDatabase

    db = SumoDatabase()
    basho_list = db.get_available_basho()
    if basho_list:
        print(f"Available basho ({len(basho_list)}):")
        for b in basho_list:
            print(f"  {b}")
    else:
        print("No basho data found. Run 'migrate' first.")


def main():
    load_env()

    parser = argparse.ArgumentParser(description="SumoSim Database Management")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("migrate", help="Load Python data files into the database")
    sub.add_parser("sync", help="Sync Supabase → local SQLite")
    sub.add_parser("status", help="Show database status")
    sub.add_parser("basho", help="List available basho")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    cmds = {
        "migrate": cmd_migrate,
        "sync": cmd_sync,
        "status": cmd_status,
        "basho": cmd_basho,
    }
    cmds[args.command](args)


if __name__ == "__main__":
    main()
