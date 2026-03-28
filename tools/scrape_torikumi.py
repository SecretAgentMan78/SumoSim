#!/usr/bin/env python3
"""
SumoSim Torikumi Scraper

Fetches official match schedules (torikumi) and results from the Sumo API.
Designed to run daily during a basho to pull in:
  - Announced matchups (before bouts happen)
  - Completed results with kimarite (after bouts happen)

The scraper checks all 15 days and pulls whatever is available.
Pre-basho, this might be just Day 1 and 2 (announced the day before).
During the basho, completed days have results and the next day's
schedule is usually posted after ~18:00 JST.

Usage:
    python -m tools.scrape_torikumi                      # Current basho
    python -m tools.scrape_torikumi --basho 202603       # Specific basho
    python -m tools.scrape_torikumi --basho 202603 --db  # Write to database
    python -m tools.scrape_torikumi --days 1,2           # Only fetch specific days

API endpoint:
    GET /api/basho/:bashoId/torikumi/:division/:day
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

API_BASE = "https://www.sumo-api.com"
DIVISION = "Makuuchi"
RATE_LIMIT_SECONDS = 1.0


def fetch_torikumi(basho_id: str, day: int) -> dict | None:
    """
    Fetch torikumi for a specific basho/division/day.

    Returns the parsed JSON response, or None if not available.
    """
    import httpx

    url = f"{API_BASE}/api/basho/{basho_id}/torikumi/{DIVISION}/{day}"
    try:
        resp = httpx.get(url, timeout=15.0)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        # The API returns an empty array or object if no data
        if not data:
            return None
        return data
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        print(f"  HTTP error for day {day}: {e.response.status_code}")
        return None
    except Exception as e:
        print(f"  Error fetching day {day}: {e}")
        return None


def normalize_match(match: dict) -> dict:
    """Normalize field names from the API response (handles camelCase)."""
    def get(key, *alts):
        val = match.get(key)
        if val is not None:
            return val
        for alt in alts:
            val = match.get(alt)
            if val is not None:
                return val
        return None

    return {
        "east_id": get("eastId", "east_id"),
        "east_shikona": get("eastShikona", "east_shikona"),
        "west_id": get("westId", "west_id"),
        "west_shikona": get("westShikona", "west_shikona"),
        "winner_id": get("winnerId", "winner_id"),
        "winner_shikona": get("winnerEn", "winner_en", "winnerShikona"),
        "kimarite": get("kimarite"),
        "is_complete": get("winnerId", "winner_id") is not None,
    }


def build_wrestler_id_map(basho_id: str) -> dict[int, str]:
    """
    Build a mapping from numeric rikishi IDs to our string wrestler_ids.
    Uses the banzuke endpoint to get the roster.
    """
    import httpx

    url = f"{API_BASE}/api/basho/{basho_id}/banzuke/{DIVISION}"
    try:
        resp = httpx.get(url, timeout=15.0)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Error fetching banzuke for ID mapping: {e}")
        return {}

    id_map = {}
    for side in ["east", "west"]:
        for entry in data.get(side, []):
            rikishi_id = entry.get("rikishiID", entry.get("rikishi_id"))
            shikona = entry.get("shikonaEn", entry.get("shikona_en", ""))
            if rikishi_id and shikona:
                # Convert shikona to our wrestler_id format (lowercase)
                wrestler_id = shikona.lower().replace(" ", "")
                id_map[rikishi_id] = wrestler_id
    return id_map


def scrape_all_days(
    basho_id: str,
    days: list[int] | None = None,
    verbose: bool = True,
) -> dict[int, list[dict]]:
    """
    Scrape torikumi for all (or specified) days of a basho.

    Returns: {day: [normalized_match, ...]}
    Only includes days that have data.
    """
    if verbose:
        print(f"Fetching torikumi for basho {basho_id}...")

    # Build ID map first
    if verbose:
        print("  Building wrestler ID map from banzuke...")
    id_map = build_wrestler_id_map(basho_id)
    if verbose:
        print(f"  Mapped {len(id_map)} wrestlers")
    time.sleep(RATE_LIMIT_SECONDS)

    target_days = days or list(range(1, 16))
    results: dict[int, list[dict]] = {}

    for day in target_days:
        if verbose:
            print(f"  Day {day}...", end=" ", flush=True)

        data = fetch_torikumi(basho_id, day)
        time.sleep(RATE_LIMIT_SECONDS)

        if data is None:
            if verbose:
                print("not available")
            continue

        # The response is typically a list of match objects,
        # or an object with a "torikumi" or "matches" key
        matches = data
        if isinstance(data, dict):
            matches = data.get("torikumi", data.get("matches", []))

        if not matches:
            if verbose:
                print("empty")
            continue

        day_matches = []
        for m in matches:
            norm = normalize_match(m)

            # Map numeric IDs to string wrestler_ids if needed
            for field in ["east_id", "west_id", "winner_id"]:
                val = norm.get(field)
                if isinstance(val, int) and val in id_map:
                    norm[field] = id_map[val]
                elif isinstance(val, int):
                    # Try to use shikona as fallback
                    shikona_field = field.replace("_id", "_shikona")
                    if norm.get(shikona_field):
                        norm[field] = norm[shikona_field].lower().replace(" ", "")

            day_matches.append(norm)

        n_complete = sum(1 for m in day_matches if m["is_complete"])
        if verbose:
            if n_complete == len(day_matches):
                print(f"{len(day_matches)} bouts (completed)")
            elif n_complete == 0:
                print(f"{len(day_matches)} bouts (scheduled, no results yet)")
            else:
                print(f"{len(day_matches)} bouts ({n_complete} completed)")

        results[day] = day_matches

    return results


def save_to_json(results: dict, basho_id: str, output_path: str | None = None):
    """Save scraped torikumi to a JSON file."""
    if output_path is None:
        output_path = f"torikumi_{basho_id}.json"

    # Add metadata
    output = {
        "basho_id": basho_id,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "days": {},
    }
    for day, matches in results.items():
        output["days"][str(day)] = matches

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved to {output_path}")


def save_to_database(results: dict, basho_id: str):
    """Save scraped torikumi to the SumoSim database."""
    from data.db import SumoDatabase
    from data.models import BoutRecord

    db = SumoDatabase()
    print(f"\nWriting to database (online: {db.is_online})...")

    # Convert basho_id format: 202603 -> 2026.03
    basho_dotted = f"{basho_id[:4]}.{basho_id[4:]}"

    bout_records = []
    schedule_only = []

    for day, matches in results.items():
        for m in matches:
            if m["is_complete"] and m["winner_id"]:
                # Completed bout — save as BoutRecord
                try:
                    br = BoutRecord(
                        basho_id=basho_dotted,
                        day=day,
                        east_id=m["east_id"],
                        west_id=m["west_id"],
                        winner_id=m["winner_id"],
                        kimarite=m.get("kimarite"),
                    )
                    bout_records.append(br)
                except (ValueError, TypeError) as e:
                    print(f"  Skipping invalid bout day {day}: {e}")
            else:
                # Scheduled but not played — store as schedule info
                schedule_only.append({
                    "day": day,
                    "east_id": m["east_id"],
                    "west_id": m["west_id"],
                    "east_shikona": m.get("east_shikona", ""),
                    "west_shikona": m.get("west_shikona", ""),
                })

    if bout_records:
        n = db.upsert_bout_records(bout_records)
        print(f"  Saved {n} completed bout records")

    if schedule_only:
        print(f"  Found {len(schedule_only)} scheduled (unplayed) bouts")
        # Save schedule to a local JSON for the tournament simulator to use
        sched_path = Path(__file__).parent.parent / "data" / f"schedule_{basho_id}.json"
        with open(sched_path, "w") as f:
            json.dump({
                "basho_id": basho_dotted,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "scheduled_bouts": schedule_only,
            }, f, indent=2)
        print(f"  Schedule saved to {sched_path}")

    return len(bout_records), len(schedule_only)


def load_schedule_for_simulator(basho_id: str) -> dict[int, list]:
    """
    Load scraped schedule data for use by the TournamentSimulator.

    Returns: {day: [MatchupEntry, ...]} for days with known matchups.
    """
    from data.models import MatchupEntry

    # Try JSON schedule file first
    # basho_id can be either "202603" or "2026.03"
    basho_flat = basho_id.replace(".", "")
    sched_path = Path(__file__).parent.parent / "data" / f"schedule_{basho_flat}.json"

    if not sched_path.exists():
        return {}

    with open(sched_path) as f:
        data = json.load(f)

    schedules: dict[int, list] = {}
    for bout in data.get("scheduled_bouts", []):
        day = bout["day"]
        if day not in schedules:
            schedules[day] = []
        schedules[day].append(
            MatchupEntry(east_id=bout["east_id"], west_id=bout["west_id"])
        )

    # Also check for completed bouts in the database
    try:
        from data.db import SumoDatabase
        basho_dotted = f"{basho_flat[:4]}.{basho_flat[4:]}"
        db = SumoDatabase()
        bout_records = db.get_bout_records(basho_dotted)
        for br in bout_records:
            day = br.day
            if day not in schedules:
                schedules[day] = []
            # Check if this matchup is already in the schedule
            existing = {(m.east_id, m.west_id) for m in schedules[day]}
            if (br.east_id, br.west_id) not in existing:
                schedules[day].append(
                    MatchupEntry(east_id=br.east_id, west_id=br.west_id)
                )
    except Exception:
        pass

    return schedules


def main():
    parser = argparse.ArgumentParser(
        description="Scrape official torikumi (match schedules) from Sumo API"
    )
    parser.add_argument(
        "--basho", default="202603",
        help="Basho ID in YYYYMM format (default: 202603 for Haru 2026)"
    )
    parser.add_argument(
        "--days", default=None,
        help="Comma-separated days to fetch (default: all 1-15)"
    )
    parser.add_argument(
        "--db", action="store_true",
        help="Write results to the SumoSim database"
    )
    parser.add_argument(
        "--json", default=None,
        help="Output JSON file path (default: torikumi_BASHO.json)"
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress verbose output"
    )

    args = parser.parse_args()

    # Load .env for database credentials
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                import os
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

    days = None
    if args.days:
        days = [int(d.strip()) for d in args.days.split(",")]

    results = scrape_all_days(args.basho, days=days, verbose=not args.quiet)

    if not results:
        print("\nNo torikumi data available yet.")
        return

    # Summary
    total_bouts = sum(len(m) for m in results.values())
    completed = sum(1 for matches in results.values() for m in matches if m["is_complete"])
    scheduled = total_bouts - completed

    print(f"\nSummary:")
    print(f"  Days with data: {sorted(results.keys())}")
    print(f"  Total bouts: {total_bouts}")
    print(f"  Completed: {completed}")
    print(f"  Scheduled (no results): {scheduled}")

    # Save JSON
    save_to_json(results, args.basho, args.json)

    # Save to database
    if args.db:
        n_bouts, n_sched = save_to_database(results, args.basho)

    # Print the schedule for upcoming days
    for day in sorted(results.keys()):
        matches = results[day]
        if not any(m["is_complete"] for m in matches):
            print(f"\n  Day {day} scheduled matchups:")
            for m in matches:
                east = m.get("east_shikona") or m["east_id"]
                west = m.get("west_shikona") or m["west_id"]
                print(f"    {east} vs {west}")


if __name__ == "__main__":
    main()
