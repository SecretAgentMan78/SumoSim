"""
tools/scrape_rikishi.py
-----------------------
Scrapes all active sekitori (Makuuchi + Juryo) from sumo-api.com,
upserts wrestler records into the local DB and syncs to Supabase.

Usage:
    python -m tools.scrape_rikishi              # latest completed basho
    python -m tools.scrape_rikishi --basho 202603
    python -m tools.scrape_rikishi --basho 202605  # Natsu 2026 (once published)

What it does:
  1. Fetches the banzuke for the target basho for BOTH Makuuchi and Juryo.
  2. For every rikishi on the banzuke, fetches /api/rikishi/:id and
     /api/rikishi/:id/stats (separate endpoint).
  3. Upserts into the `wrestlers` table (adds `division` column if missing).
  4. Syncs updated rows to Supabase.

Division column values: "Makuuchi" | "Juryo"
Rank examples stored as-is from API: "Yokozuna 1 East", "Maegashira 5 West",
                                      "Juryo 3 East", etc.
"""

import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL   = "https://sumo-api.com/api"
DB_PATH    = Path("data/sumosim_local.db")
CACHE_DIR  = Path("data/cache/rikishi")
RATE_DELAY = 0.4   # seconds between API calls (be polite)

DIVISIONS = ["Makuuchi", "Juryo"]

# Latest completed basho — update this each cycle or pass --basho on CLI.
# 202603 = Haru 2026 (March).  202605 = Natsu 2026 (May, if published).
DEFAULT_BASHO = "202603"

# ── Helpers ───────────────────────────────────────────────────────────────────

session = requests.Session()
session.headers.update({"User-Agent": "SumoSim/1.0 (personal research tool)"})


def _get(path: str, cache_key: str | None = None) -> dict | list:
    """GET from sumo-api with optional disk cache."""
    if cache_key:
        cache_file = CACHE_DIR / f"{cache_key}.json"
        if cache_file.exists():
            with open(cache_file, encoding="utf-8") as f:
                return json.load(f)

    url = f"{BASE_URL}{path}"
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    time.sleep(RATE_DELAY)

    if cache_key:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(CACHE_DIR / f"{cache_key}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return data


def _ensure_columns(conn: sqlite3.Connection) -> None:
    """Add new columns to wrestlers table if they don't exist yet."""
    cur = conn.cursor()
    existing = {row[1] for row in cur.execute("PRAGMA table_info(wrestlers)")}
    needed = {
        "division":      "TEXT NOT NULL DEFAULT 'Makuuchi'",
        "rank_label":    "TEXT",          # e.g. "Maegashira 5 West"
        "rank_value":    "INTEGER",       # numeric sort order (lower = higher rank)
        "current_basho": "TEXT",          # bashoId of most recent data
        "basho_wins":    "INTEGER",       # wins in current_basho
        "basho_losses":  "INTEGER",       # losses in current_basho
        "basho_absences": "INTEGER",      # absences in current_basho
        "makuuchi_wins":  "INTEGER",
        "makuuchi_losses": "INTEGER",
        "makuuchi_absences": "INTEGER",
        "juryo_wins":    "INTEGER",
        "juryo_losses":  "INTEGER",
        "juryo_absences": "INTEGER",
        "yusho_makuuchi": "INTEGER DEFAULT 0",
        "yusho_juryo":   "INTEGER DEFAULT 0",
        "updated_at":    "TEXT",
    }
    for col, typedef in needed.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE wrestlers ADD COLUMN {col} {typedef}")
    conn.commit()


def _rank_sort_value(rank_label: str) -> int:
    """
    Convert a rank label string to a sortable integer (lower = higher rank).

    Examples:
        "Yokozuna 1 East"   ->   10
        "Ozeki 1 West"      ->   30
        "Sekiwake 1 East"   ->   50
        "Komusubi 1 East"   ->   70
        "Maegashira 1 East" ->  100
        "Maegashira 17 West"->  134
        "Juryo 1 East"      ->  200
        "Juryo 14 West"     ->  227
    """
    r = rank_label.lower()
    # Extract numeric part (e.g. "5" from "Maegashira 5 East")
    parts = rank_label.split()
    num = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1
    side_offset = 1 if rank_label.endswith("West") else 0

    if "yokozuna" in r:
        return 10 + (num - 1) * 2 + side_offset
    if "ozeki" in r:
        return 30 + (num - 1) * 2 + side_offset
    if "sekiwake" in r:
        return 50 + (num - 1) * 2 + side_offset
    if "komusubi" in r:
        return 70 + (num - 1) * 2 + side_offset
    if "maegashira" in r:
        return 100 + (num - 1) * 2 + side_offset
    if "juryo" in r:
        return 200 + (num - 1) * 2 + side_offset
    return 999


def _fetch_banzuke(basho_id: str, division: str) -> list[dict]:
    """Return list of rikishi entries from the banzuke endpoint."""
    data = _get(
        f"/basho/{basho_id}/banzuke/{division}",
        cache_key=f"banzuke_{basho_id}_{division}",
    )
    # API returns {"east": [...], "west": [...]} or similar — normalise to flat list
    if isinstance(data, list):
        return data
    rikishi_list: list[dict] = []
    for side in ("east", "west"):
        for entry in data.get(side, []):
            entry["_side"] = side.capitalize()
            rikishi_list.append(entry)
    return rikishi_list


def _fetch_rikishi_detail(rikishi_id: str) -> dict:
    return _get(f"/rikishi/{rikishi_id}", cache_key=f"rikishi_{rikishi_id}")


def _fetch_rikishi_stats(rikishi_id: str) -> dict:
    return _get(f"/rikishi/{rikishi_id}/stats", cache_key=f"stats_{rikishi_id}")


def _upsert_wrestler(conn: sqlite3.Connection, row: dict) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO wrestlers (
            wrestler_id, shikona, shikona_en, division, rank_label, rank_value,
            heya, birth_date, height, weight, nationality, shusshin,
            current_basho, basho_wins, basho_losses, basho_absences,
            makuuchi_wins, makuuchi_losses, makuuchi_absences,
            juryo_wins, juryo_losses, juryo_absences,
            yusho_makuuchi, yusho_juryo, updated_at
        ) VALUES (
            :wrestler_id, :shikona, :shikona_en, :division, :rank_label, :rank_value,
            :heya, :birth_date, :height, :weight, :nationality, :shusshin,
            :current_basho, :basho_wins, :basho_losses, :basho_absences,
            :makuuchi_wins, :makuuchi_losses, :makuuchi_absences,
            :juryo_wins, :juryo_losses, :juryo_absences,
            :yusho_makuuchi, :yusho_juryo, :updated_at
        )
        ON CONFLICT(wrestler_id) DO UPDATE SET
            shikona          = excluded.shikona,
            shikona_en       = excluded.shikona_en,
            division         = excluded.division,
            rank_label       = excluded.rank_label,
            rank_value       = excluded.rank_value,
            heya             = excluded.heya,
            birth_date       = excluded.birth_date,
            height           = excluded.height,
            weight           = excluded.weight,
            nationality      = excluded.nationality,
            shusshin         = excluded.shusshin,
            current_basho    = excluded.current_basho,
            basho_wins       = excluded.basho_wins,
            basho_losses     = excluded.basho_losses,
            basho_absences   = excluded.basho_absences,
            makuuchi_wins    = excluded.makuuchi_wins,
            makuuchi_losses  = excluded.makuuchi_losses,
            makuuchi_absences= excluded.makuuchi_absences,
            juryo_wins       = excluded.juryo_wins,
            juryo_losses     = excluded.juryo_losses,
            juryo_absences   = excluded.juryo_absences,
            yusho_makuuchi   = excluded.yusho_makuuchi,
            yusho_juryo      = excluded.yusho_juryo,
            updated_at       = excluded.updated_at
        """,
        row,
    )


def _build_rank_label(entry: dict, division: str) -> str:
    """Construct a human-readable rank label from a banzuke entry."""
    rank = entry.get("rank", "")
    # API may already give full label, e.g. "Maegashira 5 East"
    if rank:
        return rank
    # Fallback: build from parts
    rank_en  = entry.get("rankEn", entry.get("rank_en", ""))
    rank_num = entry.get("rankOrder", entry.get("rank_order", ""))
    side     = entry.get("_side", entry.get("side", ""))
    if rank_en:
        return f"{rank_en} {rank_num} {side}".strip()
    return division  # last resort


# ── Main ──────────────────────────────────────────────────────────────────────

def run(basho_id: str) -> None:
    print(f"Scraping banzuke for basho {basho_id} — divisions: {', '.join(DIVISIONS)}")

    conn = sqlite3.connect(DB_PATH)
    _ensure_columns(conn)

    now_iso = datetime.now(timezone.utc).isoformat()
    seen_ids: set[str] = set()

    for division in DIVISIONS:
        print(f"\n── {division} ──")
        entries = _fetch_banzuke(basho_id, division)
        print(f"  Banzuke entries: {len(entries)}")

        for entry in entries:
            # Numeric wrestler ID (the stable key we migrated to)
            wrestler_id = str(entry.get("rikishiID") or entry.get("id") or entry.get("rikishi_id", ""))
            if not wrestler_id or wrestler_id in seen_ids:
                continue
            seen_ids.add(wrestler_id)

            rank_label = _build_rank_label(entry, division)
            rank_value = _rank_sort_value(rank_label)

            # Fetch detail + stats
            try:
                detail = _fetch_rikishi_detail(wrestler_id)
            except Exception as e:
                print(f"  ✗ detail fetch failed for {wrestler_id}: {e}")
                detail = {}

            try:
                stats = _fetch_rikishi_stats(wrestler_id)
            except Exception as e:
                print(f"  ✗ stats fetch failed for {wrestler_id}: {e}")
                stats = {}

            # ── Parse detail ──────────────────────────────────────────────
            shikona    = detail.get("shikonaEn") or entry.get("shikonaEn", "")
            shikona_jp = detail.get("shikona")   or entry.get("shikona", "")
            heya       = detail.get("heya", "")
            birth_date = detail.get("birthDate", "")
            height     = detail.get("height")
            weight     = detail.get("weight")
            # Shusshin: "Country, City" for foreigners; "Prefecture-ken, City-shi" for Japanese
            shusshin   = detail.get("birthPlace", detail.get("shusshin", ""))
            nationality = detail.get("nationality", detail.get("country", ""))

            # ── Parse stats ───────────────────────────────────────────────
            # Stats uses "winsByDivision" (plural) per API learnings
            wins_by_div   = stats.get("winsByDivision", {})
            losses_by_div = stats.get("lossesByDivision", {})
            abs_by_div    = stats.get("absencesByDivision", {})

            maku_wins   = wins_by_div.get("Makuuchi", 0)   or 0
            maku_losses = losses_by_div.get("Makuuchi", 0) or 0
            maku_abs    = abs_by_div.get("Makuuchi", 0)    or 0
            jury_wins   = wins_by_div.get("Juryo", 0)      or 0
            jury_losses = losses_by_div.get("Juryo", 0)    or 0
            jury_abs    = abs_by_div.get("Juryo", 0)       or 0

            # Yusho counts
            yusho_by_div   = stats.get("yushoByDivision", {})
            yusho_makuuchi = yusho_by_div.get("Makuuchi", 0) or 0
            yusho_juryo    = yusho_by_div.get("Juryo", 0)    or 0

            # Current basho record from banzuke entry (wins/losses so far this basho)
            basho_wins    = entry.get("wins",    0) or 0
            basho_losses  = entry.get("losses",  0) or 0
            basho_absences = entry.get("absences", 0) or 0

            row = dict(
                wrestler_id      = wrestler_id,
                shikona          = shikona_jp or shikona,
                shikona_en       = shikona,
                division         = division,
                rank_label       = rank_label,
                rank_value       = rank_value,
                heya             = heya,
                birth_date       = birth_date,
                height           = height,
                weight           = weight,
                nationality      = nationality,
                shusshin         = shusshin,
                current_basho    = basho_id,
                basho_wins       = basho_wins,
                basho_losses     = basho_losses,
                basho_absences   = basho_absences,
                makuuchi_wins    = maku_wins,
                makuuchi_losses  = maku_losses,
                makuuchi_absences= maku_abs,
                juryo_wins       = jury_wins,
                juryo_losses     = jury_losses,
                juryo_absences   = jury_abs,
                yusho_makuuchi   = yusho_makuuchi,
                yusho_juryo      = yusho_juryo,
                updated_at       = now_iso,
            )

            _upsert_wrestler(conn, row)
            print(f"  ✓ {shikona:<20}  {rank_label}")

    conn.commit()
    conn.close()

    total = len(seen_ids)
    print(f"\n✅ Done. {total} sekitori upserted for basho {basho_id}.")
    print("   Run `python -m tools.db_manage sync` to push to Supabase.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Makuuchi + Juryo rikishi data")
    parser.add_argument(
        "--basho",
        default=DEFAULT_BASHO,
        help=f"BashoId to use (default: {DEFAULT_BASHO}). "
             "Format: YYYYMM e.g. 202603, 202605.",
    )
    args = parser.parse_args()
    run(args.basho)
