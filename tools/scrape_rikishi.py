#!/usr/bin/env python3
"""
SumoSim Rikishi Scraper

Fetches comprehensive rikishi (wrestler) profiles from sumo-api.com
including kanji names, birth details, career records, and match history.

This populates the extended wrestler fields needed for the Rikishi
Dossier feature.

Endpoints used:
    GET /api/rikishis                           — List all rikishi
    GET /api/rikishi/:id                        — Full profile
    GET /api/rikishi/:id/matches                — All career matches
    GET /api/basho/:bashoId/banzuke/:division   — Roster for a basho

Usage:
    python -m tools.scrape_rikishi --active             # Current active only
    python -m tools.scrape_rikishi --all                # All rikishi (large!)
    python -m tools.scrape_rikishi --basho 202603       # Specific basho roster
    python -m tools.scrape_rikishi --id 19              # Single rikishi by API ID
    python -m tools.scrape_rikishi --active --matches   # Include full match history
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

API_BASE = "https://www.sumo-api.com"
RATE_LIMIT = 0.8  # seconds between requests


def _get(path: str, params: dict | None = None) -> dict | list | None:
    """Fetch from the API with rate limiting."""
    import httpx
    url = f"{API_BASE}{path}"
    try:
        resp = httpx.get(url, params=params, timeout=15.0)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  Error fetching {path}: {e}")
        return None


def fetch_rikishi_list(limit: int = 10000, skip: int = 0) -> list[dict]:
    """Fetch the full list of rikishi IDs and basic info."""
    print(f"Fetching rikishi list (limit={limit})...")
    data = _get("/api/rikishis", params={"limit": str(limit), "skip": str(skip)})
    if not data:
        return []
    # Response is typically {"records": [...], "total": N}
    if isinstance(data, dict):
        records = data.get("records", data.get("rikishis", []))
        total = data.get("total", len(records))
        print(f"  Found {total} rikishi ({len(records)} in this page)")
        return records
    return data if isinstance(data, list) else []


def fetch_rikishi_detail(api_id: int) -> dict | None:
    """Fetch full detail for a single rikishi."""
    return _get(f"/api/rikishi/{api_id}")


def fetch_rikishi_stats(api_id: int) -> dict | None:
    """Fetch career stats for a single rikishi (wins/losses by division, yusho, etc.)."""
    return _get(f"/api/rikishi/{api_id}/stats")


def fetch_rikishi_matches(api_id: int, limit: int = 10000) -> list[dict]:
    """Fetch all matches for a rikishi."""
    data = _get(f"/api/rikishi/{api_id}/matches", params={"limit": str(limit)})
    if not data:
        return []
    if isinstance(data, dict):
        return data.get("records", data.get("matches", []))
    return data if isinstance(data, list) else []


def fetch_basho_roster(basho_id: str, division: str = "Makuuchi") -> list[dict]:
    """Fetch all rikishi from a specific basho banzuke."""
    data = _get(f"/api/basho/{basho_id}/banzuke/{division}")
    if not data:
        return []
    roster = []
    for side in ["east", "west"]:
        for entry in data.get(side, []):
            roster.append(entry)
    return roster


def normalize_rikishi(raw: dict, stats: dict | None = None) -> dict:
    """Normalize rikishi detail fields from API response.

    Args:
        raw: Response from /api/rikishi/:id
        stats: Response from /api/rikishi/:id/stats (optional)
    """
    def g(key, *alts):
        val = raw.get(key)
        if val is not None:
            return val
        for alt in alts:
            val = raw.get(alt)
            if val is not None:
                return val
        return None

    api_id = g("id", "rikishiID", "rikishiId")
    shikona_en = g("shikonaEn", "shikona_en", "currentShikona", "shikona", "")
    wrestler_id = str(api_id) if api_id else shikona_en.lower().replace(" ", "").replace("-", "")

    # Parse birth date
    birth_date = g("birthDate", "birth_date")
    if birth_date and isinstance(birth_date, str):
        birth_date = birth_date[:10] if len(birth_date) >= 10 else birth_date

    # Parse height/weight
    height = g("height", "heightCm")
    weight = g("weight", "weightKg")

    # Determine fighting style from kimarite data if available
    fighting_style = "hybrid"

    # Determine active status
    is_active = True
    retired_date = None
    intai = g("intpiDate", "intaiDate", "retired", "retiredDate")
    if intai:
        is_active = False
        if isinstance(intai, str) and len(intai) >= 10:
            retired_date = intai[:10]

    status = g("status", "rikishiStatus")
    if status and isinstance(status, str) and status.lower() in ("retired", "intai"):
        is_active = False

    # Parse career stats from /stats endpoint
    career_wins = 0
    career_losses = 0
    career_absences = 0
    total_yusho = 0

    if stats:
        # Sum wins/losses/absences across all divisions
        win_by_div = stats.get("winByDivision", stats.get("winsByDivision", {}))
        loss_by_div = stats.get("lossByDivision", stats.get("lossesByDivision", {}))
        abs_by_div = stats.get("absenceByDivision", stats.get("absencesByDivision", {}))

        if isinstance(win_by_div, dict):
            career_wins = sum(win_by_div.values())
        if isinstance(loss_by_div, dict):
            career_losses = sum(loss_by_div.values())
        if isinstance(abs_by_div, dict):
            career_absences = sum(abs_by_div.values())

        # Yusho count — use Makuuchi only from yushoByDivision if available
        yusho_by_div = stats.get("yushoByDivision") or {}
        if isinstance(yusho_by_div, dict) and "Makuuchi" in yusho_by_div:
            total_yusho = yusho_by_div["Makuuchi"]
        else:
            total_yusho = stats.get("yusho", stats.get("yushoCount", 0)) or 0
            if isinstance(total_yusho, dict):
                total_yusho = total_yusho.get("Makuuchi", sum(total_yusho.values()))

    # Parse shusshin (birthplace)
    # API format: "Mongolia, Ulaanbaatar" (country, city) for foreigners
    #             "Saitama-ken, Tokorozawa-shi" (prefecture, city) for Japanese
    #             "Mongolia" (just country, no city)
    shusshin = g("shusshin", "birthPlace", "")
    country = "Japan"
    prefecture = None

    known_countries = {"Mongolia", "Ukraine", "Kazakhstan", "Georgia", "Brazil",
                       "Bulgaria", "Russia", "Egypt", "Tonga", "China", "USA",
                       "South Korea", "Taiwan", "Philippines"}

    if shusshin:
        if "," in shusshin:
            parts = [p.strip() for p in shusshin.split(",")]
            first = parts[0]
            # First part is either a country or a Japanese prefecture
            if first in known_countries:
                country = first
                prefecture = parts[1] if len(parts) > 1 else None
            elif first.endswith("-ken") or first.endswith("-fu") or first.endswith("-to") or first.endswith("-do") or first in ("Tokyo", "Osaka", "Kyoto", "Hokkaido"):
                country = "Japan"
                prefecture = first.replace("-ken", "").replace("-fu", "").replace("-to", "").replace("-do", "")
            else:
                # Unknown format — assume Japanese
                country = "Japan"
                prefecture = first
        else:
            # Single value
            if shusshin in known_countries:
                country = shusshin
            elif shusshin.endswith("-ken") or shusshin.endswith("-fu"):
                country = "Japan"
                prefecture = shusshin.replace("-ken", "").replace("-fu", "")
            else:
                country = "Japan"
                prefecture = shusshin

    return {
        "wrestler_id": wrestler_id,
        "api_id": api_id,
        "shikona": shikona_en,
        "shikona_jp": g("shikonaJp", "shikona_jp", "currentShikonaJp"),
        "heya": g("heya", "heyaName", "stable", ""),
        "birth_date": birth_date,
        "height_cm": float(height) if height else None,
        "weight_kg": float(weight) if weight else None,
        "country": country,
        "prefecture": prefecture,
        "fighting_style": fighting_style,
        "highest_rank": g("highestRank", "highest_rank", "currentRank"),
        "highest_rank_number": g("highestRankNumber", "highest_rank_number"),
        "debut_basho": g("debutBashoId", "debut", "debutBasho"),
        "is_active": is_active,
        "retired_date": retired_date,
        "career_wins": career_wins,
        "career_losses": career_losses,
        "career_absences": career_absences,
        "total_yusho": total_yusho,
    }


def save_to_database(rikishi_list: list[dict], include_matches: bool = False):
    """Save scraped rikishi data to the database."""
    from data.db import SumoDatabase
    db = SumoDatabase()
    print(f"\nWriting to database (online: {db.is_online})...")

    import sqlite3

    conn = db._local_conn()

    # Ensure extended columns exist in local SQLite
    for col, col_type in [
        ("shikona_jp", "TEXT"), ("prefecture", "TEXT"), ("api_id", "INTEGER"),
        ("highest_rank", "TEXT"), ("highest_rank_number", "INTEGER"),
        ("is_active", "INTEGER DEFAULT 1"), ("retired_date", "TEXT"),
        ("debut_basho", "TEXT"), ("career_wins", "INTEGER DEFAULT 0"),
        ("career_losses", "INTEGER DEFAULT 0"),
        ("career_absences", "INTEGER DEFAULT 0"),
        ("total_yusho", "INTEGER DEFAULT 0"),
    ]:
        try:
            conn.execute(f"ALTER TABLE wrestlers ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    saved = 0
    for r in rikishi_list:
        try:
            conn.execute(
                """INSERT OR REPLACE INTO wrestlers
                   (wrestler_id, shikona, shikona_jp, heya, birth_date,
                    height_cm, weight_kg, fighting_style, country, prefecture,
                    api_id, highest_rank, highest_rank_number, is_active,
                    retired_date, debut_basho, career_wins, career_losses,
                    career_absences, total_yusho, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (r["wrestler_id"], r["shikona"], r.get("shikona_jp"),
                 r["heya"], r.get("birth_date"),
                 r.get("height_cm"), r.get("weight_kg"),
                 r.get("fighting_style", "hybrid"),
                 r.get("country", "Japan"), r.get("prefecture"),
                 r.get("api_id"), r.get("highest_rank"),
                 r.get("highest_rank_number"),
                 1 if r.get("is_active", True) else 0,
                 r.get("retired_date"), r.get("debut_basho"),
                 r.get("career_wins", 0), r.get("career_losses", 0),
                 r.get("career_absences", 0), r.get("total_yusho", 0),
                 datetime.now(timezone.utc).isoformat()),
            )
            saved += 1
        except Exception as e:
            print(f"  Error saving {r.get('shikona', '?')}: {e}")

    conn.commit()

    # Also push to Supabase if online
    if db.is_online:
        print("  Pushing to Supabase...")
        errors = 0
        for r in rikishi_list:
            try:
                # Explicitly map to DB column names with correct types
                row = {
                    "wrestler_id": r["wrestler_id"],
                    "shikona": r["shikona"],
                    "heya": r.get("heya", ""),
                }
                # Optional fields — only include if not None
                optionals = {
                    "shikona_jp": r.get("shikona_jp"),
                    "birth_date": r.get("birth_date"),
                    "height_cm": float(r["height_cm"]) if r.get("height_cm") else None,
                    "weight_kg": float(r["weight_kg"]) if r.get("weight_kg") else None,
                    "fighting_style": r.get("fighting_style", "hybrid"),
                    "country": r.get("country", "Japan"),
                    "prefecture": r.get("prefecture"),
                    "api_id": int(r["api_id"]) if r.get("api_id") else None,
                    "highest_rank": r.get("highest_rank"),
                    "highest_rank_number": int(r["highest_rank_number"]) if r.get("highest_rank_number") else None,
                    "is_active": bool(r.get("is_active", True)),
                    "retired_date": r.get("retired_date"),
                    "debut_basho": r.get("debut_basho"),
                    "career_wins": int(r["career_wins"]) if r.get("career_wins") else 0,
                    "career_losses": int(r["career_losses"]) if r.get("career_losses") else 0,
                    "career_absences": int(r["career_absences"]) if r.get("career_absences") else 0,
                    "total_yusho": int(r["total_yusho"]) if r.get("total_yusho") else 0,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                for k, v in optionals.items():
                    if v is not None:
                        row[k] = v

                db._rest_upsert("wrestlers", row, on_conflict="wrestler_id")
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"    Supabase error for {r.get('shikona', '?')}: {e}")
                elif errors == 4:
                    print(f"    (suppressing further errors...)")
        if errors:
            print(f"  {errors} Supabase errors total")

    conn.close()
    print(f"  Saved {saved} rikishi to local DB")
    return saved


def save_matches_to_database(api_id: int, wrestler_id: str, matches: list[dict]):
    """Save match history to bout_records table.

    Uses numeric API IDs for wrestler identification.
    Auto-creates stub wrestler records for any opponent not yet in the DB.
    """
    from data.db import SumoDatabase
    from data.models import BoutRecord

    db = SumoDatabase()
    records = []

    # Collect all wrestler IDs we'll need
    needed_ids: dict[str, str] = {}  # api_id_str -> shikona

    for m in matches:
        basho = m.get("bashoId", m.get("basho_id", ""))
        if not basho:
            continue
        # Convert YYYYMM to YYYY.MM
        if len(str(basho)) == 6:
            basho_dotted = f"{str(basho)[:4]}.{str(basho)[4:]}"
        elif "." in str(basho):
            basho_dotted = str(basho)
        else:
            continue

        day = m.get("day", 0)
        if not (1 <= day <= 16):
            continue

        # Use numeric API IDs
        east_api_id = m.get("eastId", m.get("east_id"))
        west_api_id = m.get("westId", m.get("west_id"))
        winner_api_id = m.get("winnerId", m.get("winner_id"))
        kimarite = m.get("kimarite")

        if not east_api_id or not west_api_id or not winner_api_id:
            continue

        east_id = str(east_api_id)
        west_id = str(west_api_id)
        winner_id = str(winner_api_id)

        # Track shikona for stub creation
        east_shikona = m.get("eastShikona", m.get("east_shikona", f"Rikishi {east_id}"))
        west_shikona = m.get("westShikona", m.get("west_shikona", f"Rikishi {west_id}"))
        needed_ids[east_id] = east_shikona
        needed_ids[west_id] = west_shikona

        try:
            records.append(BoutRecord(
                basho_id=basho_dotted,
                day=day,
                east_id=east_id,
                west_id=west_id,
                winner_id=winner_id,
                kimarite=kimarite,
            ))
        except (ValueError, TypeError):
            continue

    # Auto-create stub records for wrestlers not yet in the DB
    if needed_ids:
        import sqlite3
        conn = db._local_conn()

        # Ensure extended columns exist
        for col, col_type in [
            ("shikona_jp", "TEXT"), ("prefecture", "TEXT"), ("api_id", "INTEGER"),
            ("highest_rank", "TEXT"), ("highest_rank_number", "INTEGER"),
            ("is_active", "INTEGER DEFAULT 1"), ("retired_date", "TEXT"),
            ("debut_basho", "TEXT"), ("career_wins", "INTEGER DEFAULT 0"),
            ("career_losses", "INTEGER DEFAULT 0"),
            ("career_absences", "INTEGER DEFAULT 0"),
            ("total_yusho", "INTEGER DEFAULT 0"),
        ]:
            try:
                conn.execute(f"ALTER TABLE wrestlers ADD COLUMN {col} {col_type}")
            except sqlite3.OperationalError:
                pass

        existing = set()
        for row in conn.execute("SELECT wrestler_id FROM wrestlers").fetchall():
            existing.add(row["wrestler_id"])

        stubs_created = 0
        for wid, shikona in needed_ids.items():
            if wid not in existing:
                conn.execute(
                    """INSERT OR IGNORE INTO wrestlers
                       (wrestler_id, shikona, heya, fighting_style, api_id, updated_at)
                       VALUES (?, ?, '', 'hybrid', ?, ?)""",
                    (wid, shikona, int(wid), datetime.now(timezone.utc).isoformat()),
                )
                existing.add(wid)
                stubs_created += 1

                # Also create in Supabase if online
                if db.is_online:
                    try:
                        db._rest_upsert("wrestlers", {
                            "wrestler_id": wid,
                            "shikona": shikona,
                            "heya": "",
                            "fighting_style": "hybrid",
                            "api_id": int(wid),
                        }, on_conflict="wrestler_id")
                    except Exception:
                        pass

        if stubs_created:
            print(f"    Created {stubs_created} stub wrestler records")
        conn.commit()
        conn.close()

    if records:
        n = db.upsert_bout_records(records)
        return n
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Scrape rikishi profiles from Sumo API"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--active", action="store_true",
                      help="Scrape active Makuuchi wrestlers only")
    mode.add_argument("--all", action="store_true",
                      help="Scrape ALL rikishi (thousands — takes hours)")
    mode.add_argument("--basho", default=None,
                      help="Scrape roster from specific basho (YYYYMM)")
    mode.add_argument("--id", type=int, default=None,
                      help="Scrape a single rikishi by API ID")

    parser.add_argument("--matches", action="store_true",
                        help="Also scrape full match history for each rikishi")
    parser.add_argument("--db", action="store_true", default=True,
                        help="Write to database (default: True)")
    parser.add_argument("--json-output", default=None,
                        help="Also save raw data to JSON file")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit number of rikishi to process")

    args = parser.parse_args()

    # Load .env
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                import os
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

    rikishi_to_scrape: list[int] = []  # API IDs

    if args.id:
        rikishi_to_scrape = [args.id]
    elif args.basho:
        print(f"Fetching roster for basho {args.basho}...")
        roster = fetch_basho_roster(args.basho)
        time.sleep(RATE_LIMIT)
        for entry in roster:
            api_id = entry.get("rikishiID", entry.get("rikishiId"))
            if api_id:
                rikishi_to_scrape.append(api_id)
        print(f"  {len(rikishi_to_scrape)} wrestlers in roster")
    elif args.all:
        print("Fetching complete rikishi list...")
        records = fetch_rikishi_list(limit=10000)
        time.sleep(RATE_LIMIT)
        for r in records:
            api_id = r.get("id", r.get("rikishiId"))
            if api_id:
                rikishi_to_scrape.append(api_id)
        print(f"  {len(rikishi_to_scrape)} total rikishi")
    else:
        # Default: active Makuuchi (current basho)
        from datetime import date
        today = date.today()
        basho_months = [1, 3, 5, 7, 9, 11]
        best = min(basho_months, key=lambda m: abs(today.month - m))
        basho_id = f"{today.year}{best:02d}"
        print(f"Fetching active Makuuchi roster ({basho_id})...")
        roster = fetch_basho_roster(basho_id)
        time.sleep(RATE_LIMIT)
        for entry in roster:
            api_id = entry.get("rikishiID", entry.get("rikishiId"))
            if api_id:
                rikishi_to_scrape.append(api_id)
        print(f"  {len(rikishi_to_scrape)} wrestlers in roster")

    if args.limit:
        rikishi_to_scrape = rikishi_to_scrape[:args.limit]

    # Fetch details for each rikishi
    all_rikishi: list[dict] = []
    total = len(rikishi_to_scrape)

    for i, api_id in enumerate(rikishi_to_scrape, 1):
        print(f"  [{i}/{total}] Fetching rikishi {api_id}...", end=" ", flush=True)
        raw = fetch_rikishi_detail(api_id)
        time.sleep(RATE_LIMIT)

        if not raw:
            print("not found")
            continue

        # Also fetch career stats
        stats = fetch_rikishi_stats(api_id)
        time.sleep(RATE_LIMIT)

        norm = normalize_rikishi(raw, stats=stats)
        try:
            print(f"{norm['shikona']} (W:{norm['career_wins']} L:{norm['career_losses']})")
        except UnicodeEncodeError:
            print(f"{norm['shikona']} (W:{norm['career_wins']} L:{norm['career_losses']})")
        all_rikishi.append(norm)

        # Fetch match history if requested
        if args.matches:
            print(f"    Fetching matches...", end=" ", flush=True)
            matches = fetch_rikishi_matches(api_id)
            time.sleep(RATE_LIMIT)
            if matches:
                print(f"{len(matches)} matches")
                n = save_matches_to_database(api_id, norm["wrestler_id"], matches)
                print(f"    Saved {n} bout records")
            else:
                print("none")

    # Summary
    print(f"\n{'='*50}")
    print(f"Scraped {len(all_rikishi)} rikishi profiles")
    active = sum(1 for r in all_rikishi if r.get("is_active"))
    retired = len(all_rikishi) - active
    print(f"  Active: {active}")
    print(f"  Retired: {retired}")

    # Save to JSON
    if args.json_output:
        with open(args.json_output, "w", encoding="utf-8") as f:
            json.dump(all_rikishi, f, indent=2, ensure_ascii=False)
        print(f"  JSON saved to {args.json_output}")

    # Save to database
    if args.db:
        saved = save_to_database(all_rikishi)

    print("Done.")


if __name__ == "__main__":
    main()
