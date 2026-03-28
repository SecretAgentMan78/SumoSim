#!/usr/bin/env python3
"""
Scrape head-to-head bout records for all Makuuchi wrestlers from the Sumo API.

Usage:
    python -m tools.scrape_h2h                    # default: 202603 banzuke
    python -m tools.scrape_h2h --basho 202601     # specific basho
    python -m tools.scrape_h2h --output data/h2h_haru2026.py
    python -m tools.scrape_h2h --limit-pairs 10   # test run
    python -m tools.scrape_h2h --inspect           # show raw API response

This script:
  1. Fetches the Makuuchi banzuke from sumo-api.com to get all wrestler IDs
  2. For each unique pair, fetches their head-to-head bout history
  3. Outputs a Python file with BoutRecord data and kimarite analysis

IMPORTANT: The script auto-discovers API response field names on first request
and prints them, so if the API format changes you can see exactly what came back.

Requires: requests (pip install requests)
Rate limiting: 1 second between requests to be respectful of the free API.
"""

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path
from itertools import combinations

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library required. Install with: pip install requests")
    sys.exit(1)

BASE_URL = "https://www.sumo-api.com/api"
RATE_LIMIT_DELAY = 1.0  # seconds between API calls

# Map sumo-api shikona to our internal wrestler_id (lowercase, no spaces).
# Updated for Haru 2026 Makuuchi banzuke.
SHIKONA_TO_ID = {}  # No longer needed — we use numeric API IDs directly


def fetch_json(url: str, retries: int = 3) -> dict | list | None:
    """Fetch JSON from the Sumo API with retry logic."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = (attempt + 1) * 5
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif resp.status_code == 404:
                return []  # no data for this pair
            else:
                print(f"  HTTP {resp.status_code} for {url}")
                if attempt < retries - 1:
                    time.sleep(2)
        except requests.RequestException as e:
            print(f"  Request error: {e}")
            if attempt < retries - 1:
                time.sleep(2)
    return None


def fetch_banzuke(basho_id: str) -> list[dict]:
    """Fetch the Makuuchi banzuke and return list of {api_id, shikona, rank}."""
    url = f"{BASE_URL}/basho/{basho_id}/banzuke/Makuuchi"
    print(f"Fetching banzuke for {basho_id}...")
    data = fetch_json(url)
    if not data:
        print("ERROR: Could not fetch banzuke")
        sys.exit(1)

    wrestlers = []
    for side in ["east", "west"]:
        for entry in data.get(side, []):
            wrestlers.append({
                "api_id": entry.get("rikishiID", entry.get("rikishiId", 0)),
                "shikona": entry.get("shikonaEn", entry.get("shikona", "")),
                "rank": entry.get("rank", ""),
            })
    print(f"  Found {len(wrestlers)} wrestlers in Makuuchi")
    return wrestlers


def fetch_h2h(rikishi_id: int, opponent_id: int) -> list[dict]:
    """Fetch head-to-head bout records between two wrestlers.

    Returns a list of match dicts, each containing bout details.
    """
    url = f"{BASE_URL}/rikishi/{rikishi_id}/matches/{opponent_id}"
    data = fetch_json(url)
    if data is None:
        return []
    # Handle various response formats the API might return
    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        # Could be wrapped: {"matches": [...]} or {"records": [...]}
        for key in ("matches", "records", "bouts", "results"):
            if key in data and isinstance(data[key], list):
                return data[key]
        # Could be a single match object
        if any(k in data for k in ("bashoId", "winnerId", "kimarite", "day")):
            return [data]
    return []


def convert_basho_id(api_basho) -> str:
    """Convert API basho ID to our 'YYYY.MM' format.

    Handles: '202601', 202601 (int), '2026.01' (passthrough)
    """
    s = str(api_basho).strip()
    if len(s) == 6 and s.isdigit():
        return f"{s[:4]}.{s[4:]}"
    if len(s) == 7 and s[4] == '.':
        return s  # already formatted
    return ""


def extract_match_fields(match: dict) -> dict:
    """Normalize match dict field names to a standard format.

    The Sumo API may use camelCase or other conventions. This normalizes
    to: basho, day, east_id, west_id, winner_id, kimarite, east_shikona,
    west_shikona, winner_shikona.
    """
    basho = (match.get("bashoId")
             or match.get("basho_id")
             or match.get("basho")
             or "")

    day = (match.get("day")
           or match.get("matchDay")
           or match.get("matchNo")
           or 0)

    east_id = (match.get("eastId")
               or match.get("east_id")
               or match.get("rikishi1Id")
               or 0)

    west_id = (match.get("westId")
               or match.get("west_id")
               or match.get("rikishi2Id")
               or 0)

    winner_id = (match.get("winnerId")
                 or match.get("winner_id")
                 or 0)

    kimarite = (match.get("kimarite")
                or match.get("winningTechnique")
                or match.get("technique")
                or "")

    east_shikona = (match.get("eastShikona")
                    or match.get("east_shikona")
                    or match.get("eastShikonaEn")
                    or match.get("rikishi1Shikona")
                    or "")

    west_shikona = (match.get("westShikona")
                    or match.get("west_shikona")
                    or match.get("westShikonaEn")
                    or match.get("rikishi2Shikona")
                    or "")

    winner_shikona = (match.get("winnerEn")
                      or match.get("winner")
                      or match.get("winnerShikona")
                      or match.get("winnerShikonaEn")
                      or "")

    return {
        "basho": basho,
        "day": day,
        "east_id": east_id,
        "west_id": west_id,
        "winner_id": winner_id,
        "kimarite": kimarite,
        "east_shikona": east_shikona,
        "west_shikona": west_shikona,
        "winner_shikona": winner_shikona,
    }


def generate_python_output(
    all_records: dict[tuple[str, str], list[dict]],
    api_to_internal: dict[int, str],
) -> str:
    """Generate Python source code with all H2H bout records and kimarite data."""

    # First pass: collect all bouts and kimarite stats
    all_bouts: list[dict] = []
    kimarite_by_winner: dict[str, Counter] = {}
    kimarite_by_matchup: dict[tuple[str, str], Counter] = {}

    for (_id_a, _id_b), matches in sorted(all_records.items()):
        for m in matches:
            fields = extract_match_fields(m)

            # Resolve API IDs to internal IDs (now just str(api_id))
            east_internal = api_to_internal.get(fields["east_id"], "")
            west_internal = api_to_internal.get(fields["west_id"], "")
            winner_internal = api_to_internal.get(fields["winner_id"], "")

            # Fallback: use the API ID directly if it's numeric
            if not east_internal and fields["east_id"]:
                east_internal = str(fields["east_id"])
            if not west_internal and fields["west_id"]:
                west_internal = str(fields["west_id"])
            if not winner_internal and fields["winner_id"]:
                winner_internal = str(fields["winner_id"])

            if not east_internal or not west_internal or not winner_internal:
                continue

            basho = convert_basho_id(fields["basho"])
            if not basho:
                continue

            day = fields["day"]
            if not isinstance(day, int) or day < 1 or day > 16:
                day = 1

            if winner_internal not in (east_internal, west_internal):
                continue

            kimarite = fields["kimarite"] or ""
            loser = west_internal if winner_internal == east_internal else east_internal

            all_bouts.append({
                "basho": basho,
                "day": day,
                "east_id": east_internal,
                "west_id": west_internal,
                "winner_id": winner_internal,
                "kimarite": kimarite,
            })

            # Track kimarite stats
            if kimarite:
                if winner_internal not in kimarite_by_winner:
                    kimarite_by_winner[winner_internal] = Counter()
                kimarite_by_winner[winner_internal][kimarite] += 1

                matchup_key = (winner_internal, loser)
                if matchup_key not in kimarite_by_matchup:
                    kimarite_by_matchup[matchup_key] = Counter()
                kimarite_by_matchup[matchup_key][kimarite] += 1

    # Generate Python code
    lines = [
        '"""',
        'Head-to-head bout records for Haru 2026 Makuuchi wrestlers.',
        'Auto-generated by tools/scrape_h2h.py from sumo-api.com',
        f'Generated: {time.strftime("%Y-%m-%d %H:%M")}',
        '',
        f'Total bouts: {len(all_bouts)}',
        f'Wrestlers with kimarite data: {len(kimarite_by_winner)}',
        '"""',
        '',
        'from data.models import BoutRecord',
        '',
        '',
        'def haru_2026_bout_records() -> list[BoutRecord]:',
        '    """Return all historical bout records between Haru 2026 Makuuchi wrestlers.',
        '',
        '    Each BoutRecord includes kimarite (winning technique) when available.',
        '    Use haru_2026_kimarite_stats() for aggregate technique analysis.',
        '    """',
        '    return [',
    ]

    for bout in sorted(all_bouts, key=lambda b: (b["basho"], b["day"])):
        kim_str = f', kimarite="{bout["kimarite"]}"' if bout["kimarite"] else ""
        lines.append(
            f'        BoutRecord('
            f'basho_id="{bout["basho"]}", day={bout["day"]}, '
            f'east_id="{bout["east_id"]}", west_id="{bout["west_id"]}", '
            f'winner_id="{bout["winner_id"]}"{kim_str}'
            f'),'
        )

    lines.append('    ]')
    lines.append('')
    lines.append('')

    # Generate kimarite stats function
    lines.append('def haru_2026_kimarite_stats() -> dict[str, dict]:')
    lines.append('    """Return kimarite (winning technique) frequency stats per wrestler.')
    lines.append('')
    lines.append('    Returns: {wrestler_id: {"total_wins": N, "techniques": {"yorikiri": 5, ...}}}')
    lines.append("    Use this to analyze a wrestler's fighting style and most common")
    lines.append('    winning techniques, which feeds into the matchup/style modifier.')
    lines.append('    """')
    lines.append('    return {')

    for wrestler_id in sorted(kimarite_by_winner.keys()):
        counter = kimarite_by_winner[wrestler_id]
        total = sum(counter.values())
        techs = counter.most_common()
        tech_dict = ", ".join(f'"{k}": {v}' for k, v in techs)
        lines.append(f'        "{wrestler_id}": {{"total_wins": {total}, "techniques": {{{tech_dict}}}}},')

    lines.append('    }')
    lines.append('')
    lines.append('')

    # Generate matchup-specific kimarite function
    lines.append('def haru_2026_matchup_kimarite() -> dict[tuple[str, str], dict]:')
    lines.append('    """Return kimarite frequencies for specific matchups.')
    lines.append('')
    lines.append('    Returns: {(winner_id, loser_id): {"yorikiri": 3, "oshidashi": 1, ...}}')
    lines.append('    This reveals HOW a wrestler typically beats a specific opponent.')
    lines.append('    """')
    lines.append('    return {')

    for (winner, loser) in sorted(kimarite_by_matchup.keys()):
        counter = kimarite_by_matchup[(winner, loser)]
        techs = counter.most_common()
        tech_dict = ", ".join(f'"{k}": {v}' for k, v in techs)
        lines.append(f'        ("{winner}", "{loser}"): {{{tech_dict}}},')

    lines.append('    }')
    lines.append('')

    # Summary stats
    total_pairs = len(set(
        tuple(sorted([b["east_id"], b["west_id"]])) for b in all_bouts
    ))
    kim_count = sum(1 for b in all_bouts if b["kimarite"])
    lines.append(f'# Stats: {len(all_bouts)} bouts across {total_pairs} matchup pairs')
    lines.append(f'# Kimarite recorded for {kim_count} bouts')
    lines.append('')

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape H2H records and kimarite data from Sumo API"
    )
    parser.add_argument(
        "--basho", default="202603",
        help="Basho ID in YYYYMM format (default: 202603 for Haru 2026)"
    )
    parser.add_argument(
        "--output", default=None,
        help="Output Python file path (default: data/h2h_haru2026.py)"
    )
    parser.add_argument(
        "--json-output", default=None,
        help="Also output a JSON summary file with kimarite analysis"
    )
    parser.add_argument(
        "--cache-dir", default=".h2h_cache",
        help="Directory to cache API responses (default: .h2h_cache)"
    )
    parser.add_argument(
        "--no-cache", action="store_true",
        help="Ignore cached responses"
    )
    parser.add_argument(
        "--limit-pairs", type=int, default=0,
        help="Limit number of pairs to fetch (0=all, useful for testing)"
    )
    parser.add_argument(
        "--inspect", action="store_true",
        help="Fetch one pair and print raw JSON to inspect field names, then exit"
    )
    parser.add_argument(
        "--db", action="store_true",
        help="Write bout records directly to the database (Supabase + local SQLite)"
    )
    args = parser.parse_args()

    # Load .env for database credentials
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        import os
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

    # Determine output path
    if args.output is None:
        script_dir = Path(__file__).resolve().parent.parent
        output_path = script_dir / "data" / "h2h_haru2026.py"
    else:
        output_path = Path(args.output)

    # Set up cache directory
    cache_dir = Path(args.cache_dir)
    if not args.no_cache:
        cache_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Fetch banzuke to get all wrestler IDs
    wrestlers = fetch_banzuke(args.basho)

    # Build mapping: api_id -> wrestler_id (now they're the same: str(api_id))
    api_to_internal: dict[int, str] = {}
    our_wrestlers: list[dict] = []

    for w in wrestlers:
        api_id = w["api_id"]
        internal_id = str(api_id)
        api_to_internal[api_id] = internal_id
        our_wrestlers.append({**w, "internal_id": internal_id})

    print(f"\nMapped {len(our_wrestlers)} wrestlers (using numeric API IDs)")

    # Print the ID mapping for reference
    print("\nAPI ID -> wrestler_id mapping:")
    for w in sorted(our_wrestlers, key=lambda x: x["rank"]):
        print(f"  {w['api_id']:>5} = {w['internal_id']:<8} ({w['shikona']} - {w['rank']})")
    print()

    # Step 2: For --inspect mode, fetch one pair and show raw response
    if args.inspect:
        if len(our_wrestlers) >= 2:
            w_a, w_b = our_wrestlers[0], our_wrestlers[1]
            print(f"Inspecting: {w_a['shikona']} (ID {w_a['api_id']}) vs "
                  f"{w_b['shikona']} (ID {w_b['api_id']})")
            url = f"{BASE_URL}/rikishi/{w_a['api_id']}/matches/{w_b['api_id']}"
            print(f"URL: {url}")
            raw = fetch_json(url)
            print(f"\nResponse type: {type(raw).__name__}")
            print(f"Raw response:\n{json.dumps(raw, indent=2, default=str)[:5000]}")

            if isinstance(raw, list) and len(raw) > 0:
                print(f"\nFirst match object keys: {list(raw[0].keys())}")
                print(f"First match:\n{json.dumps(raw[0], indent=2, default=str)}")
                fields = extract_match_fields(raw[0])
                print(f"\nExtracted fields: {json.dumps(fields, indent=2, default=str)}")
                if fields["kimarite"]:
                    print(f"  KIMARITE FOUND: '{fields['kimarite']}'")
                else:
                    print(f"  WARNING: No kimarite in response")
            elif isinstance(raw, dict):
                print(f"\nTop-level keys: {list(raw.keys())}")
                for k, v in raw.items():
                    if isinstance(v, list) and len(v) > 0:
                        print(f"\n'{k}' is a list with {len(v)} items.")
                        print(f"First item keys: {list(v[0].keys())}")
                        print(f"First item:\n{json.dumps(v[0], indent=2, default=str)}")
        else:
            print("Need at least 2 wrestlers to inspect")
        return

    # Step 3: Fetch H2H for all unique pairs
    pairs = list(combinations(our_wrestlers, 2))
    total_pairs = len(pairs)

    if args.limit_pairs > 0:
        pairs = pairs[:args.limit_pairs]
        print(f"Limited to {len(pairs)} pairs (of {total_pairs} total)")

    print(f"Fetching H2H records for {len(pairs)} pairs...")
    print(f"Estimated time: ~{len(pairs) * RATE_LIMIT_DELAY / 60:.0f} minutes")
    print(f"(Cached results will be used where available)\n")

    all_records: dict[tuple[str, str], list[dict]] = {}
    pairs_with_history = 0
    total_bouts = 0
    total_with_kimarite = 0
    first_response_logged = False

    for i, (w_a, w_b) in enumerate(pairs, 1):
        id_a = w_a["internal_id"]
        id_b = w_b["internal_id"]
        api_a = w_a["api_id"]
        api_b = w_b["api_id"]

        # Check cache
        cache_file = cache_dir / f"{api_a}_{api_b}.json"
        matches = None

        if not args.no_cache and cache_file.exists():
            try:
                with open(cache_file) as f:
                    matches = json.load(f)
            except (json.JSONDecodeError, IOError):
                pass

        if matches is None:
            # Fetch from API
            matches = fetch_h2h(api_a, api_b)

            # Log first successful non-empty response for debugging
            if matches and not first_response_logged:
                first_response_logged = True
                print(f"\n  == First API response sample ==")
                print(f"  Pair: {w_a['shikona']} vs {w_b['shikona']}")
                if len(matches) > 0:
                    sample = matches[0]
                    print(f"  Fields: {list(sample.keys())}")
                    print(f"  Sample: {json.dumps(sample, indent=4, default=str)}")
                    fields = extract_match_fields(sample)
                    if fields["kimarite"]:
                        print(f"  KIMARITE: '{fields['kimarite']}'")
                    else:
                        print(f"  WARNING: No kimarite found. Keys: {list(sample.keys())}")
                print(f"  ================================\n")

            # Cache the result
            if not args.no_cache:
                try:
                    with open(cache_file, 'w') as f:
                        json.dump(matches if matches else [], f)
                except IOError:
                    pass

            # Rate limit
            time.sleep(RATE_LIMIT_DELAY)

        key = (id_a, id_b)
        all_records[key] = matches or []

        if matches:
            pairs_with_history += 1
            total_bouts += len(matches)
            for m in matches:
                fields = extract_match_fields(m)
                if fields["kimarite"]:
                    total_with_kimarite += 1

        # Progress
        if i % 25 == 0 or i == len(pairs):
            pct = i / len(pairs) * 100
            print(f"  [{i}/{len(pairs)}] ({pct:.0f}%) -- "
                  f"{pairs_with_history} pairs with history, "
                  f"{total_bouts} bouts ({total_with_kimarite} with kimarite)")

    print(f"\nDone fetching! {pairs_with_history} pairs have bout history")
    print(f"  Total bouts: {total_bouts}")
    print(f"  With kimarite: {total_with_kimarite} "
          f"({total_with_kimarite / max(total_bouts, 1) * 100:.0f}%)")

    # Step 4: Generate output
    print(f"\nWriting Python output to {output_path}...")
    py_code = generate_python_output(all_records, api_to_internal)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(py_code)

    bout_count = py_code.count("BoutRecord(")
    print(f"  Written {len(py_code):,} bytes ({bout_count} BoutRecords)")

    # Optional JSON output
    if args.json_output:
        json_path = Path(args.json_output)
        print(f"\nWriting JSON analysis to {json_path}...")

        analysis = {"matchups": {}, "kimarite_profiles": {}, "matchup_kimarite": {}}

        for (id_a, id_b), matches in sorted(all_records.items()):
            a_wins, b_wins = 0, 0
            kimarite_list = []
            for m in matches:
                fields = extract_match_fields(m)
                w = api_to_internal.get(fields["winner_id"], "")
                if w == id_a:
                    a_wins += 1
                elif w == id_b:
                    b_wins += 1
                if fields["kimarite"]:
                    kimarite_list.append({
                        "basho": convert_basho_id(fields["basho"]),
                        "winner": w,
                        "kimarite": fields["kimarite"],
                    })

            if a_wins + b_wins > 0:
                analysis["matchups"][f"{id_a}:{id_b}"] = {
                    "a": id_a, "b": id_b,
                    "a_wins": a_wins, "b_wins": b_wins,
                    "total": a_wins + b_wins,
                    "kimarite": kimarite_list,
                }

        with open(json_path, 'w') as f:
            json.dump(analysis, f, indent=2)
        print(f"  Written {len(analysis['matchups'])} matchup entries")

    print("\n" + "=" * 60)
    print("DONE!")
    print("=" * 60)
    print(f"\nGenerated file: {output_path}")
    print(f"\nThe file provides three functions:")
    print(f"  haru_2026_bout_records()     -> list[BoutRecord] with kimarite")
    print(f"  haru_2026_kimarite_stats()   -> per-wrestler technique frequencies")
    print(f"  haru_2026_matchup_kimarite() -> per-matchup technique frequencies")
    print(f"\nTo use in SumoSim:")
    print(f"  python main.py")

    # Step 5: Write to database if requested
    if args.db:
        print(f"\nWriting bout records to database...")
        try:
            from data.db import SumoDatabase
            from data.models import BoutRecord

            db = SumoDatabase()
            records = []

            for (_id_a, _id_b), matches in all_records.items():
                for m in matches:
                    fields = extract_match_fields(m)
                    east_id = api_to_internal.get(fields["east_id"], str(fields["east_id"]) if fields["east_id"] else "")
                    west_id = api_to_internal.get(fields["west_id"], str(fields["west_id"]) if fields["west_id"] else "")
                    winner_id = api_to_internal.get(fields["winner_id"], str(fields["winner_id"]) if fields["winner_id"] else "")

                    if not east_id or not west_id or not winner_id:
                        continue
                    if winner_id not in (east_id, west_id):
                        continue

                    basho = convert_basho_id(fields["basho"])
                    if not basho:
                        continue

                    day = fields["day"]
                    if not isinstance(day, int) or day < 1 or day > 16:
                        day = 1

                    # Ensure wrestlers exist (create stubs if not)
                    for wid, shikona in [(east_id, fields.get("east_shikona", "")),
                                          (west_id, fields.get("west_shikona", ""))]:
                        try:
                            db._local_conn().execute(
                                "INSERT OR IGNORE INTO wrestlers (wrestler_id, shikona, heya, fighting_style, api_id) VALUES (?, ?, '', 'hybrid', ?)",
                                (wid, shikona or f"Rikishi {wid}", int(wid) if wid.isdigit() else None)
                            ).connection.commit()
                        except Exception:
                            pass

                    try:
                        records.append(BoutRecord(
                            basho_id=basho,
                            day=day,
                            east_id=east_id,
                            west_id=west_id,
                            winner_id=winner_id,
                            kimarite=fields["kimarite"] or None,
                        ))
                    except (ValueError, TypeError):
                        continue

            if records:
                n = db.upsert_bout_records(records)
                print(f"  Written {n} bout records to database")
                print(f"  Online: {db.is_online}")
            else:
                print("  No valid bout records to write")

        except Exception as e:
            print(f"  Database write failed: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
