#!/usr/bin/env python3
"""
SumoSim: Scrape tournament records from banzuke endpoint.

Populates tournament_records with wins, losses, absences, rank, rank_number,
and side for each wrestler for each basho.

Usage:
    python -m tools.scrape_basho_records --last 6    # Last 6 basho
    python -m tools.scrape_basho_records --basho 202601,202511,202509
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

API_BASE = "https://www.sumo-api.com"
RATE_LIMIT = 0.8


def _get(path: str) -> dict | None:
    import httpx
    try:
        resp = httpx.get(f"{API_BASE}{path}", timeout=15.0)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception as e:
        print(f"  Error: {e}")
        return None


def parse_rank_string(rank_str: str) -> tuple[str, int | None, str | None]:
    """Parse 'Yokozuna 1 East' -> ('yokozuna', 1, 'east')"""
    parts = rank_str.strip().split()
    rank = parts[0].lower() if parts else "maegashira"
    number = None
    side = None
    for p in parts[1:]:
        if p.isdigit():
            number = int(p)
        elif p.lower() in ("east", "west"):
            side = p.lower()
    return rank, number, side


def get_all_basho_ids(n: int = 6) -> list[str]:
    """Generate the last N basho IDs from current date."""
    basho_months = [1, 3, 5, 7, 9, 11]
    now = datetime.now()
    year = now.year
    month = now.month

    ids = []
    # Start from current or most recent basho month
    while len(ids) < n:
        # Find most recent basho month <= current position
        for bm in reversed(basho_months):
            if year < now.year or (year == now.year and bm <= month):
                ids.append(f"{year}{bm:02d}")
                if len(ids) >= n:
                    break
        month = 12
        year -= 1
        if year < 2020:
            break

    return ids


def scrape_basho(basho_id: str, divisions: list[str] | None = None) -> list[dict]:
    """Scrape banzuke for a basho across divisions, return tournament record dicts.

    Uses the /api/basho/:id endpoint to get actual yusho winners.
    Jun-yusho is determined as the second-best record in each division
    (excluding the yusho winner).
    """
    if divisions is None:
        divisions = ["Makuuchi", "Juryo"]

    basho_dotted = f"{basho_id[:4]}.{basho_id[4:]}"

    # Fetch basho metadata for actual yusho winners
    basho_meta = _get(f"/api/basho/{basho_id}")
    yusho_ids: set[int] = set()
    if basho_meta:
        for y in basho_meta.get("yusho", []):
            rid = y.get("rikishiId", y.get("rikishiID"))
            if rid:
                yusho_ids.add(rid)
    time.sleep(RATE_LIMIT)

    records = []

    for division in divisions:
        data = _get(f"/api/basho/{basho_id}/banzuke/{division}")
        if not data:
            continue

        # First pass: collect all entries for this division
        division_entries = []
        for side_key in ["east", "west"]:
            for entry in (data.get(side_key) or []):
                api_id = entry.get("rikishiID", entry.get("rikishiId"))
                if not api_id:
                    continue

                rank_str = entry.get("rank", "")
                rank, number, side = parse_rank_string(rank_str)

                wins = entry.get("wins", 0)
                losses = entry.get("losses", 0)
                absences = entry.get("absences", 0)

                division_entries.append({
                    "basho_id": basho_dotted,
                    "wrestler_id": str(api_id),
                    "api_id": api_id,
                    "shikona": entry.get("shikonaEn", ""),
                    "rank": rank,
                    "rank_number": number,
                    "side": side,
                    "wins": wins,
                    "losses": losses,
                    "absences": absences,
                    "is_yusho": api_id in yusho_ids,
                    "is_jun_yusho": False,
                })

        # Second pass: determine jun-yusho (best record excluding yusho winner)
        yusho_wins = 0
        for e in division_entries:
            if e["is_yusho"]:
                yusho_wins = e["wins"]
                break

        # Find the best non-yusho win count
        non_yusho_wins = [
            e["wins"] for e in division_entries
            if not e["is_yusho"] and e["wins"] > 0
        ]
        if non_yusho_wins:
            jun_yusho_wins = max(non_yusho_wins)
            # Jun-yusho typically requires a strong record (at least 10 wins
            # in Makuuchi, or close to yusho winner)
            for e in division_entries:
                if not e["is_yusho"] and e["wins"] == jun_yusho_wins:
                    e["is_jun_yusho"] = True

        # Remove api_id helper field before adding to records
        for e in division_entries:
            e.pop("api_id", None)

        records.extend(division_entries)
        time.sleep(RATE_LIMIT)

    return records


def save_to_database(all_records: list[dict]):
    """Save tournament records to local SQLite and Supabase."""
    import sqlite3
    from data.db import SumoDatabase

    db = SumoDatabase()
    conn = db._local_conn()

    # Ensure side column exists in tournament_records
    try:
        conn.execute("ALTER TABLE tournament_records ADD COLUMN side TEXT")
    except sqlite3.OperationalError:
        pass

    # Auto-create stub wrestlers for any not yet in the DB
    existing = set(
        r[0] for r in conn.execute("SELECT wrestler_id FROM wrestlers").fetchall()
    )
    stubs_created = 0
    for r in all_records:
        wid = r["wrestler_id"]
        if wid not in existing:
            conn.execute(
                """INSERT OR IGNORE INTO wrestlers
                   (wrestler_id, shikona, heya, fighting_style, api_id, is_active)
                   VALUES (?, ?, '', 'hybrid', ?, 1)""",
                (wid, r.get("shikona", f"Rikishi {wid}"), int(wid) if wid.isdigit() else None),
            )
            existing.add(wid)
            stubs_created += 1

            # Also create in Supabase
            if db.is_online:
                try:
                    db._rest_upsert("wrestlers", {
                        "wrestler_id": wid,
                        "shikona": r.get("shikona", f"Rikishi {wid}"),
                        "heya": "",
                        "fighting_style": "hybrid",
                        "api_id": int(wid) if wid.isdigit() else None,
                        "is_active": True,
                    }, on_conflict="wrestler_id")
                except Exception:
                    pass

    if stubs_created:
        print(f"  Created {stubs_created} stub wrestler records")
    conn.commit()

    saved = 0
    for r in all_records:
        try:
            conn.execute(
                """INSERT OR REPLACE INTO tournament_records
                   (basho_id, wrestler_id, rank, rank_number, side,
                    wins, losses, absences, is_yusho, is_jun_yusho)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (r["basho_id"], r["wrestler_id"], r["rank"],
                 r["rank_number"], r["side"],
                 r["wins"], r["losses"], r["absences"],
                 1 if r.get("is_yusho") else 0,
                 1 if r.get("is_jun_yusho") else 0),
            )
            saved += 1
        except Exception as e:
            print(f"  Error saving {r.get('shikona')}: {e}")

    conn.commit()
    conn.close()

    # Also push to Supabase
    if db.is_online:
        print("  Pushing to Supabase...")
        errors = 0
        for r in all_records:
            try:
                row = {
                    "basho_id": r["basho_id"],
                    "wrestler_id": r["wrestler_id"],
                    "rank": r["rank"],
                    "rank_number": r["rank_number"],
                    "side": r.get("side"),
                    "wins": r["wins"],
                    "losses": r["losses"],
                    "absences": r["absences"],
                    "is_yusho": r.get("is_yusho", False),
                    "is_jun_yusho": r.get("is_jun_yusho", False),
                }
                db._rest_upsert("tournament_records", row,
                                on_conflict="basho_id,wrestler_id")
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"    Error: {e}")
        if errors:
            print(f"  {errors} Supabase errors")

    print(f"  Saved {saved} tournament records to local DB")
    return saved


def main():
    parser = argparse.ArgumentParser(
        description="Scrape tournament records from banzuke data"
    )
    parser.add_argument("--last", type=int, default=6,
                        help="Number of recent basho to scrape (default: 6)")
    parser.add_argument("--basho", default=None,
                        help="Comma-separated basho IDs (e.g. 202601,202511)")
    args = parser.parse_args()

    # Load .env
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        import os
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

    if args.basho:
        basho_ids = [b.strip() for b in args.basho.split(",")]
    else:
        basho_ids = get_all_basho_ids(args.last)

    print(f"Scraping tournament records for: {basho_ids}")

    all_records = []
    for basho_id in basho_ids:
        print(f"  {basho_id}...", end=" ", flush=True)
        records = scrape_basho(basho_id)
        time.sleep(RATE_LIMIT)

        if records:
            print(f"{len(records)} wrestlers")
            all_records.extend(records)
        else:
            print("no data")

    print(f"\nTotal records: {len(all_records)}")

    # Show sample
    if all_records:
        sample = all_records[0]
        print(f"Sample: {sample['shikona']} @ {sample['basho_id']} "
              f"= {sample['rank']} {sample['rank_number']} {sample['side']} "
              f"({sample['wins']}W-{sample['losses']}L)")

    save_to_database(all_records)
    print("Done.")


if __name__ == "__main__":
    main()
