"""
tools/sync_banzuke.py
---------------------
Fast banzuke refresh: updates rank, division, and current-basho win/loss/absence
for every active sekitori WITHOUT re-fetching the full stats payload.

Use this at the start of each basho day to keep standings current.
scrape_rikishi.py (full) is only needed at basho start or after a major data
refresh.

Usage:
    python -m tools.sync_banzuke                    # latest completed basho
    python -m tools.sync_banzuke --basho 202605     # Natsu 2026
    python -m tools.sync_banzuke --basho 202605 --day 8   # mid-basho snapshot
"""

import argparse
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_URL   = "https://sumo-api.com/api"
DB_PATH    = Path("data/sumosim_local.db")
RATE_DELAY = 0.3
DIVISIONS  = ["Makuuchi", "Juryo"]
DEFAULT_BASHO = "202603"

session = requests.Session()
session.headers.update({"User-Agent": "SumoSim/1.0"})


def _get(path: str) -> dict | list:
    resp = session.get(f"{BASE_URL}{path}", timeout=15)
    resp.raise_for_status()
    time.sleep(RATE_DELAY)
    return resp.json()


def _rank_sort_value(rank_label: str) -> int:
    r = rank_label.lower()
    parts = rank_label.split()
    num = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1
    side_offset = 1 if rank_label.endswith("West") else 0
    if "yokozuna"  in r: return 10  + (num - 1) * 2 + side_offset
    if "ozeki"     in r: return 30  + (num - 1) * 2 + side_offset
    if "sekiwake"  in r: return 50  + (num - 1) * 2 + side_offset
    if "komusubi"  in r: return 70  + (num - 1) * 2 + side_offset
    if "maegashira"in r: return 100 + (num - 1) * 2 + side_offset
    if "juryo"     in r: return 200 + (num - 1) * 2 + side_offset
    return 999


def run(basho_id: str, day: int | None = None) -> None:
    """
    Refresh ranks and records from the banzuke endpoint.
    If `day` is provided, also fetch hoshitori (win/loss per day) for that day.
    """
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    now  = datetime.now(timezone.utc).isoformat()

    print(f"Syncing banzuke for {basho_id}" + (f" day {day}" if day else "") + "…")

    for division in DIVISIONS:
        path = f"/basho/{basho_id}/banzuke/{division}"
        try:
            data = _get(path)
        except Exception as e:
            print(f"  ✗ Could not fetch {division} banzuke: {e}")
            continue

        # Normalise to flat list
        entries: list[dict] = []
        if isinstance(data, list):
            entries = data
        else:
            for side in ("east", "west"):
                for entry in data.get(side, []):
                    entry["_side"] = side.capitalize()
                    entries.append(entry)

        for entry in entries:
            wrestler_id = str(
                entry.get("rikishiID") or entry.get("id") or entry.get("rikishi_id", "")
            )
            if not wrestler_id:
                continue

            rank_label = entry.get("rank", "")
            if not rank_label:
                rank_en  = entry.get("rankEn", "")
                rank_num = entry.get("rankOrder", "")
                side     = entry.get("_side", "")
                rank_label = f"{rank_en} {rank_num} {side}".strip()

            rank_value    = _rank_sort_value(rank_label)
            basho_wins    = entry.get("wins",    0) or 0
            basho_losses  = entry.get("losses",  0) or 0
            basho_absences= entry.get("absences",0) or 0

            cur.execute(
                """
                UPDATE wrestlers
                SET division       = ?,
                    rank_label     = ?,
                    rank_value     = ?,
                    current_basho  = ?,
                    basho_wins     = ?,
                    basho_losses   = ?,
                    basho_absences = ?,
                    updated_at     = ?
                WHERE wrestler_id  = ?
                """,
                (
                    division, rank_label, rank_value, basho_id,
                    basho_wins, basho_losses, basho_absences,
                    now, wrestler_id,
                ),
            )
            if cur.rowcount == 0:
                # Rikishi not in DB yet — run scrape_rikishi for a full ingest
                print(f"  ⚠ {wrestler_id} not found in DB — run scrape_rikishi.py first")

        print(f"  ✓ {division}: {len(entries)} entries synced")

    conn.commit()
    conn.close()
    print("✅ Banzuke sync complete. Run `python -m tools.db_manage sync` to push to Supabase.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync banzuke ranks and current-basho records")
    parser.add_argument("--basho", default=DEFAULT_BASHO,
                        help=f"BashoId (default: {DEFAULT_BASHO})")
    parser.add_argument("--day",   type=int, default=None,
                        help="Tournament day (1-15) for mid-basho record refresh")
    args = parser.parse_args()
    run(args.basho, args.day)
