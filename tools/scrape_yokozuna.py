#!/usr/bin/env python3
"""
SumoSim: Scrape all historical yokozuna profiles and match histories.

Fetches full profiles, career stats, and complete match histories
for every yokozuna since 1958 that exists in sumo-api.com.

Usage:
    python -m tools.scrape_yokozuna              # Scrape all yokozuna
    python -m tools.scrape_yokozuna --dry-run    # Preview only
    python -m tools.scrape_yokozuna --id 3846    # Single yokozuna by API ID
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

# All known yokozuna API IDs (discovered by traversing opponent networks)
YOKOZUNA = {
    1407: "Tochinishiki",    # 35th
    1403: "Asashio",         # 46th
    1319: "Kashiwado",       # 47th
    1511: "Taiho",           # 48th
    2779: "Tochinoumi",      # 49th
    2672: "Sadanoyama",      # 50th
    2075: "Kitanofuji",      # 52nd
    2079: "Kotozakura",      # 53rd - not current Kotozakura
    1032: "Wajima",          # 54th
    1036: "Kitanoumi",       # 55th
    1217: "Mienoumi",        # 57th
    1024: "Chiyonofuji",     # 58th
    1010: "Takanosato",      # 59th
    1275: "Futahaguro",      # 60th (Kitao)
    1136: "Hokutoumi",       # 61st
    904:  "Onokuni",         # 62nd
    5649: "Asahifuji",       # 63rd
    4913: "Akebono",         # 64th
    4789: "Takanohana",      # 65th
    4997: "Wakanohana III",  # 66th
    3859: "Musashimaru",     # 67th
    3846: "Asashoryu",       # 68th
    3081: "Hakuho",          # 69th
    3363: "Harumafuji",      # 70th (Ama)
    3181: "Kakuryu",         # 71st
    3285: "Kisenosato",      # 72nd
    45:   "Terunofuji",      # 73rd
    19:   "Hoshoryu",        # 74th
    8850: "Onosato",         # 75th
}


def _get(path: str, params: dict | None = None):
    import httpx
    try:
        resp = httpx.get(f"{API_BASE}{path}", params=params, timeout=30.0)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception as e:
        print(f"  Error: {e}")
        return None


def scrape_yokozuna(api_id: int, name: str, dry_run: bool = False) -> dict:
    """Scrape full profile, stats, and match history for one yokozuna."""
    print(f"\n{'='*50}")
    print(f"  {name} (API ID: {api_id})")
    print(f"{'='*50}")

    # 1. Profile
    print(f"  Fetching profile...", end=" ", flush=True)
    profile = _get(f"/api/rikishi/{api_id}")
    time.sleep(RATE_LIMIT)
    if profile:
        print(f"{profile.get('shikonaEn', '?')} - {profile.get('currentRank', 'retired')}")
    else:
        print("not found")
        return {}

    # 2. Stats
    print(f"  Fetching career stats...", end=" ", flush=True)
    stats = _get(f"/api/rikishi/{api_id}/stats")
    time.sleep(RATE_LIMIT)
    if stats:
        wins = sum((stats.get("winsByDivision") or {}).values())
        losses = sum((stats.get("lossByDivision") or {}).values())
        print(f"{wins}W-{losses}L")
    else:
        print("no stats")

    # 3. Match history
    print(f"  Fetching match history...", end=" ", flush=True)
    matches = _get(f"/api/rikishi/{api_id}/matches", params={"limit": "10000"})
    time.sleep(RATE_LIMIT)
    if matches:
        if isinstance(matches, dict):
            matches = matches.get("records", matches.get("matches", []))
        print(f"{len(matches)} matches")
    else:
        matches = []
        print("none")

    if dry_run:
        return {"api_id": api_id, "name": name, "matches": len(matches)}

    return {
        "api_id": api_id,
        "name": name,
        "profile": profile,
        "stats": stats,
        "matches": matches,
    }


def save_to_database(data: dict):
    """Save a yokozuna's profile, stats, and matches to the database."""
    from tools.scrape_rikishi import normalize_rikishi, save_matches_to_database
    from data.db import SumoDatabase

    db = SumoDatabase()
    api_id = data["api_id"]
    profile = data.get("profile", {})
    stats = data.get("stats")
    matches = data.get("matches", [])

    if not profile:
        return

    # Normalize and save profile
    norm = normalize_rikishi(profile, stats=stats)
    # Force is_active=False for retired yokozuna (unless they have a current rank)
    current_rank = profile.get("currentRank", "")
    if not current_rank or current_rank == "none":
        norm["is_active"] = False
    # Set highest_rank to yokozuna
    norm["highest_rank"] = "yokozuna"

    conn = db._local_conn()

    # Ensure extended columns exist
    import sqlite3
    for col, col_type in [
        ("shikona_jp", "TEXT"), ("prefecture", "TEXT"), ("api_id", "INTEGER"),
        ("highest_rank", "TEXT"), ("highest_rank_number", "INTEGER"),
        ("is_active", "INTEGER DEFAULT 1"), ("retired_date", "TEXT"),
        ("debut_basho", "TEXT"), ("career_wins", "INTEGER DEFAULT 0"),
        ("career_losses", "INTEGER DEFAULT 0"),
        ("career_absences", "INTEGER DEFAULT 0"),
        ("total_yusho", "INTEGER DEFAULT 0"),
        ("shikona_full", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE wrestlers ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass

    # Upsert wrestler
    conn.execute(
        """INSERT OR REPLACE INTO wrestlers
           (wrestler_id, shikona, shikona_jp, heya, birth_date,
            height_cm, weight_kg, fighting_style, country, prefecture,
            api_id, highest_rank, is_active, debut_basho,
            career_wins, career_losses, career_absences, total_yusho,
            updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (norm["wrestler_id"], norm["shikona"], norm.get("shikona_jp"),
         norm["heya"], norm.get("birth_date"),
         norm.get("height_cm"), norm.get("weight_kg"),
         norm.get("fighting_style", "hybrid"),
         norm.get("country", "Japan"), norm.get("prefecture"),
         norm.get("api_id"), "yokozuna",
         1 if norm.get("is_active") else 0,
         norm.get("debut_basho"),
         norm.get("career_wins", 0), norm.get("career_losses", 0),
         norm.get("career_absences", 0), norm.get("total_yusho", 0),
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()

    # Also push to Supabase
    if db.is_online:
        try:
            row = {k: v for k, v in norm.items() if v is not None}
            row["highest_rank"] = "yokozuna"
            row["updated_at"] = datetime.now(timezone.utc).isoformat()
            db._rest_upsert("wrestlers", row, on_conflict="wrestler_id")
        except Exception as e:
            print(f"    Supabase profile error: {e}")

    print(f"  Saved profile: {norm['shikona']}")

    # Save match history
    if matches:
        try:
            n = save_matches_to_database(api_id, norm["wrestler_id"], matches)
            print(f"  Saved {n} bout records")
        except Exception as e:
            print(f"  Error saving matches (will retry locally only): {e}")
            # Fall back to local-only save
            try:
                from data.models import BoutRecord
                conn = db._local_conn()
                saved = 0
                for m in matches:
                    try:
                        basho = m.get("bashoId", m.get("basho_id", ""))
                        if not basho:
                            continue
                        if len(str(basho)) == 6:
                            basho_dotted = f"{str(basho)[:4]}.{str(basho)[4:]}"
                        elif "." in str(basho):
                            basho_dotted = str(basho)
                        else:
                            continue

                        day = m.get("day", 0)
                        if not (1 <= day <= 16):
                            continue

                        east_id = str(m.get("eastId", m.get("east_id", "")))
                        west_id = str(m.get("westId", m.get("west_id", "")))
                        winner_id = str(m.get("winnerId", m.get("winner_id", "")))
                        kimarite = m.get("kimarite")

                        if not east_id or not west_id or not winner_id:
                            continue

                        # Ensure wrestlers exist
                        for wid, shikona in [(east_id, m.get("eastShikona", "")),
                                              (west_id, m.get("westShikona", ""))]:
                            conn.execute(
                                "INSERT OR IGNORE INTO wrestlers (wrestler_id, shikona, heya, fighting_style, api_id) VALUES (?, ?, '', 'hybrid', ?)",
                                (wid, shikona or f"Rikishi {wid}", int(wid) if wid.isdigit() else None),
                            )

                        conn.execute(
                            """INSERT OR REPLACE INTO bout_records
                               (basho_id, day, east_id, west_id, winner_id, kimarite)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            (basho_dotted, day, east_id, west_id, winner_id, kimarite),
                        )
                        saved += 1
                    except Exception:
                        continue

                conn.commit()
                conn.close()
                print(f"  Saved {saved} bout records (local only)")
            except Exception as e2:
                print(f"  Local save also failed: {e2}")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape all historical yokozuna from sumo-api.com"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview only, don't save to database")
    parser.add_argument("--id", type=int, default=None,
                        help="Scrape a single yokozuna by API ID")
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

    if args.id:
        targets = {args.id: YOKOZUNA.get(args.id, f"Yokozuna #{args.id}")}
    else:
        targets = YOKOZUNA

    print(f"Scraping {len(targets)} yokozuna...")
    print(f"Estimated time: ~{len(targets) * 3 * RATE_LIMIT / 60:.0f} minutes")

    results = []
    for api_id, name in sorted(targets.items()):
        data = scrape_yokozuna(api_id, name, dry_run=args.dry_run)
        if data and not args.dry_run:
            save_to_database(data)
        results.append(data)

    # Summary
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    total_matches = 0
    for r in results:
        if r:
            n = len(r.get("matches", [])) if not args.dry_run else r.get("matches", 0)
            total_matches += n if isinstance(n, int) else 0
            print(f"  {r.get('name', '?')}: {n} matches")

    print(f"\nTotal yokozuna: {len(results)}")
    print(f"Total matches: {total_matches}")

    if args.dry_run:
        print("\nDRY RUN — use without --dry-run to save to database")
    else:
        print("\nDone! All yokozuna profiles and match histories saved.")


if __name__ == "__main__":
    main()
