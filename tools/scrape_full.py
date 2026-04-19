"""
tools/scrape_full.py
--------------------
Comprehensive data scraper for SumoSim.

Builds the full data foundation by pulling from sumo-api.com:

  Phase 1: Banzuke + wrestler profiles + career stats
    - Fetches banzuke for target basho (Makuuchi + Juryo)
    - For each wrestler: profile (/rikishi/:id) + stats (/rikishi/:id/stats)
    - Populates: wrestlers table, basho_entries table
    - Career W/L/A totals come from the API stats endpoint (authoritative)

  Phase 2: Full match history
    - For each wrestler: /rikishi/:id/matches (all career bouts)
    - Populates: bout_records table
    - Rate-limited; ~15-30 min for ~70 wrestlers

  Phase 3: Historical basho entries (optional, --history flag)
    - Walks backward through past basho to build basho_entries for each
      wrestler's career, enabling rank progression charts and yusho tracking

Data quality:
  - Career totals on wrestlers table are ALWAYS authoritative (from /stats)
  - bout_records may be incomplete for lower divisions — that's OK
  - basho_entries grow richer as more history is scraped
  - API field names are probed dynamically (handles lossByDivision vs
    lossesByDivision, etc.)

Usage:
    python -m tools.scrape_full                             # profiles + stats only (~5 min)
    python -m tools.scrape_full --matches                   # + match history (~20 min)
    python -m tools.scrape_full --matches --history          # + historical basho (~45 min)
    python -m tools.scrape_full --basho 202605               # target a specific basho
    python -m tools.scrape_full --wrestler 19                # scrape a single wrestler (debug)
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL      = "https://sumo-api.com/api"
DB_PATH       = Path("data/sumosim_local.db")
CACHE_DIR     = Path("data/cache/scrape_full")
RATE_DELAY    = 0.45   # seconds between API calls (be polite)
MATCH_LIMIT   = 10000  # max matches to fetch per wrestler
DIVISIONS     = ["Makuuchi", "Juryo"]
DEFAULT_BASHO = "202603"

session = requests.Session()
session.headers.update({"User-Agent": "SumoSim/1.0 (personal research tool)"})


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP
# ═══════════════════════════════════════════════════════════════════════════════

def _get(path: str, cache_key: str | None = None,
         params: dict | None = None) -> Any | None:
    """GET from sumo-api with optional disk cache and retry."""
    if cache_key:
        cache_file = CACHE_DIR / f"{cache_key}.json"
        if cache_file.exists():
            with open(cache_file, encoding="utf-8") as f:
                return json.load(f)

    url = f"{BASE_URL}{path}"
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=20, params=params)
            if resp.status_code == 429:
                wait = (attempt + 1) * 5
                print(f"    ⏳ Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            time.sleep(RATE_DELAY)

            if cache_key:
                CACHE_DIR.mkdir(parents=True, exist_ok=True)
                with open(CACHE_DIR / f"{cache_key}.json", "w",
                          encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            return data
        except requests.exceptions.Timeout:
            print(f"    ⏳ Timeout on {path}, retry {attempt+1}/3")
            time.sleep(2)
        except Exception as e:
            print(f"    ✗ Error on {path}: {e}")
            if attempt < 2:
                time.sleep(2)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════════════════════════

def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Create/migrate all tables to the target schema."""
    cur = conn.cursor()

    # ── basho_entries table (replaces old banzuke + tournament_records) ────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS basho_entries (
            basho_id        TEXT NOT NULL,
            wrestler_id     TEXT NOT NULL,
            division        TEXT NOT NULL DEFAULT 'Makuuchi',
            rank            TEXT,
            rank_number     INTEGER,
            side            TEXT,
            wins            INTEGER DEFAULT 0,
            losses          INTEGER DEFAULT 0,
            absences        INTEGER DEFAULT 0,
            is_kyujo        INTEGER DEFAULT 0,
            is_yusho        INTEGER DEFAULT 0,
            is_jun_yusho    INTEGER DEFAULT 0,
            special_prizes  TEXT DEFAULT '[]',
            PRIMARY KEY (basho_id, wrestler_id)
        )
    """)

    # ── Ensure wrestlers table has all needed columns ─────────────────────
    existing = {row[1] for row in cur.execute("PRAGMA table_info(wrestlers)")}
    needed_cols = {
        # Profile
        "shikona_en":          "TEXT",
        "shikona_full":        "TEXT",
        "nationality":         "TEXT",
        "shusshin":            "TEXT",
        # Current rank (from latest banzuke scrape)
        "division":            "TEXT",
        "rank_label":          "TEXT",
        "rank_value":          "INTEGER",
        # Career stats — authoritative totals from /stats endpoint
        "career_wins":         "INTEGER DEFAULT 0",
        "career_losses":       "INTEGER DEFAULT 0",
        "career_absences":     "INTEGER DEFAULT 0",
        # Division-specific breakdowns
        "makuuchi_wins":       "INTEGER DEFAULT 0",
        "makuuchi_losses":     "INTEGER DEFAULT 0",
        "makuuchi_absences":   "INTEGER DEFAULT 0",
        "juryo_wins":          "INTEGER DEFAULT 0",
        "juryo_losses":        "INTEGER DEFAULT 0",
        "juryo_absences":      "INTEGER DEFAULT 0",
        # Yusho
        "total_yusho":         "INTEGER DEFAULT 0",
        "yusho_makuuchi":      "INTEGER DEFAULT 0",
        "yusho_juryo":         "INTEGER DEFAULT 0",
        # Basho snapshot
        "current_basho":       "TEXT",
        "basho_wins":          "INTEGER DEFAULT 0",
        "basho_losses":        "INTEGER DEFAULT 0",
        "basho_absences":      "INTEGER DEFAULT 0",
        # Metadata
        "updated_at":          "TEXT",
    }
    added = []
    for col, typedef in needed_cols.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE wrestlers ADD COLUMN {col} {typedef}")
            added.append(col)
    if added:
        print(f"  [schema] Added {len(added)} column(s) to wrestlers: {', '.join(added[:5])}{'...' if len(added) > 5 else ''}")

    # ── Ensure bout_records has needed columns ─────────────────────────
    bout_cols = {row[1] for row in cur.execute("PRAGMA table_info(bout_records)")}
    for col in ("division", "east_rank", "west_rank"):
        if col not in bout_cols:
            cur.execute(f"ALTER TABLE bout_records ADD COLUMN {col} TEXT")
            print(f"  [schema] Added '{col}' column to bout_records")

    # ── Indexes ───────────────────────────────────────────────────────────
    cur.execute("CREATE INDEX IF NOT EXISTS idx_basho_entries_wrestler ON basho_entries(wrestler_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_basho_entries_basho ON basho_entries(basho_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wrestlers_rank_value ON wrestlers(rank_value)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bout_records_east ON bout_records(east_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bout_records_west ON bout_records(west_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bout_records_winner ON bout_records(winner_id)")

    conn.commit()
    print("  [schema] OK")



# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _rank_sort_value(rank_label: str) -> int:
    """Convert a rank label to a sortable integer (lower = higher rank)."""
    r = rank_label.lower()
    parts = rank_label.split()
    num = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1
    side_offset = 1 if rank_label.endswith("West") else 0

    if "yokozuna" in r:   return 10 + (num - 1) * 2 + side_offset
    if "ozeki" in r:      return 30 + (num - 1) * 2 + side_offset
    if "sekiwake" in r:   return 50 + (num - 1) * 2 + side_offset
    if "komusubi" in r:   return 70 + (num - 1) * 2 + side_offset
    if "maegashira" in r: return 100 + (num - 1) * 2 + side_offset
    if "juryo" in r:      return 200 + (num - 1) * 2 + side_offset
    return 999


def _build_rank_label(entry: dict, division: str) -> str:
    """Construct a rank label from a banzuke entry."""
    rank = entry.get("rank", "")
    if rank:
        return rank
    rank_en  = entry.get("rankEn", entry.get("rank_en", ""))
    rank_num = entry.get("rankOrder", entry.get("rank_order", ""))
    side     = entry.get("_side", entry.get("side", ""))
    if rank_en:
        return f"{rank_en} {rank_num} {side}".strip()
    return division


def _parse_rank_label(rank_label: str) -> tuple[str, int | None, str | None]:
    """Parse 'Maegashira 5 East' → ('maegashira', 5, 'east')."""
    parts = rank_label.split()
    rank_str = parts[0].lower() if parts else "maegashira"
    rank_number = None
    side = None
    if len(parts) >= 2 and parts[1].isdigit():
        rank_number = int(parts[1])
    if len(parts) >= 3 and parts[2].lower() in ("east", "west"):
        side = parts[2].lower()
    return rank_str, rank_number, side


def _convert_basho_id(raw: str | int) -> str:
    """Convert API basho ID to YYYY.MM format."""
    s = str(raw).strip()
    if len(s) == 6 and s.isdigit():
        return f"{s[:4]}.{s[4:]}"
    if len(s) == 7 and s[4] == ".":
        return s
    return s


def _safe_int(val, default=0) -> int:
    """Safely convert to int."""
    try:
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _get_dict(d: dict | None, *keys) -> Any:
    """Try multiple keys on a dict, return first non-None value."""
    if not d:
        return None
    for k in keys:
        val = d.get(k)
        if val is not None:
            return val
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1: Banzuke + Profiles + Stats
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_banzuke(basho_id: str, division: str) -> list[dict]:
    """Fetch banzuke entries for a basho/division."""
    data = _get(f"/basho/{basho_id}/banzuke/{division}",
                cache_key=f"banzuke_{basho_id}_{division}")
    if data is None:
        return []
    if isinstance(data, list):
        return data
    # API returns {"east": [...], "west": [...]}
    entries = []
    for side in ("east", "west"):
        for entry in data.get(side, []):
            entry["_side"] = side.capitalize()
            entries.append(entry)
    return entries


def _fetch_profile(wrestler_id: str) -> dict | None:
    """Fetch wrestler profile from /rikishi/:id."""
    return _get(f"/rikishi/{wrestler_id}",
                cache_key=f"rikishi_{wrestler_id}")


def _fetch_stats(wrestler_id: str) -> dict | None:
    """Fetch career stats from /rikishi/:id/stats."""
    return _get(f"/rikishi/{wrestler_id}/stats",
                cache_key=f"stats_{wrestler_id}")


def _parse_stats(stats: dict) -> dict:
    """Extract career totals from the stats endpoint.

    The API has used different field names across versions:
      - winsByDivision / lossByDivision (current)
      - winsByDivision / lossesByDivision (legacy)
    We probe for both.
    """
    wins_by_div = stats.get("winsByDivision", {}) or {}
    # Probe both spellings for losses
    losses_by_div = (stats.get("lossByDivision")
                     or stats.get("lossesByDivision")
                     or {})
    abs_by_div = (stats.get("absencesByDivision")
                  or stats.get("absenceByDivision")
                  or {})
    yusho_by_div = (stats.get("yushoByDivision")
                    or stats.get("yushosByDivision")
                    or {})

    # Sum across ALL divisions for career totals
    career_wins   = sum(_safe_int(v) for v in wins_by_div.values())
    career_losses = sum(_safe_int(v) for v in losses_by_div.values())
    career_abs    = sum(_safe_int(v) for v in abs_by_div.values())

    return {
        "career_wins":       career_wins,
        "career_losses":     career_losses,
        "career_absences":   career_abs,
        # Division breakdowns
        "makuuchi_wins":     _safe_int(wins_by_div.get("Makuuchi")),
        "makuuchi_losses":   _safe_int(losses_by_div.get("Makuuchi")),
        "makuuchi_absences": _safe_int(abs_by_div.get("Makuuchi")),
        "juryo_wins":        _safe_int(wins_by_div.get("Juryo")),
        "juryo_losses":      _safe_int(losses_by_div.get("Juryo")),
        "juryo_absences":    _safe_int(abs_by_div.get("Juryo")),
        # Yusho — Makuuchi only for headline, but store Juryo too
        "total_yusho":       _safe_int(yusho_by_div.get("Makuuchi")),
        "yusho_makuuchi":    _safe_int(yusho_by_div.get("Makuuchi")),
        "yusho_juryo":       _safe_int(yusho_by_div.get("Juryo")),
    }


def _upsert_wrestler(conn: sqlite3.Connection, row: dict) -> None:
    """Upsert a wrestler row using only columns that exist in the DB."""
    live_cols = {r[1] for r in conn.execute("PRAGMA table_info(wrestlers)")}
    cols = [k for k in row if k in live_cols and row[k] is not None]
    if not cols:
        return

    col_list = ", ".join(cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    update_set = ", ".join(f"{c} = excluded.{c}" for c in cols if c != "wrestler_id")

    conn.execute(f"""
        INSERT INTO wrestlers ({col_list})
        VALUES ({placeholders})
        ON CONFLICT(wrestler_id) DO UPDATE SET {update_set}
    """, {c: row[c] for c in cols})


def _upsert_basho_entry(conn: sqlite3.Connection, row: dict) -> None:
    """Upsert a basho_entries row."""
    conn.execute("""
        INSERT OR REPLACE INTO basho_entries
        (basho_id, wrestler_id, division, rank, rank_number, side,
         wins, losses, absences, is_kyujo, is_yusho, is_jun_yusho, special_prizes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        row["basho_id"], row["wrestler_id"], row.get("division", "Makuuchi"),
        row.get("rank"), row.get("rank_number"), row.get("side"),
        row.get("wins", 0), row.get("losses", 0), row.get("absences", 0),
        1 if row.get("is_kyujo") else 0,
        1 if row.get("is_yusho") else 0,
        1 if row.get("is_jun_yusho") else 0,
        json.dumps(row.get("special_prizes", [])),
    ))


def phase1_profiles(conn: sqlite3.Connection, basho_id: str,
                    single_wrestler: str | None = None) -> list[str]:
    """Scrape banzuke, profiles, and career stats. Returns list of wrestler IDs."""
    basho_dotted = _convert_basho_id(basho_id)
    now_iso = datetime.now(timezone.utc).isoformat()
    wrestler_ids = []

    if single_wrestler:
        # Debug mode: single wrestler
        entries = [{"rikishiID": int(single_wrestler), "_side": "East"}]
        divisions_to_scan = [("Makuuchi", entries)]
    else:
        divisions_to_scan = []
        for div in DIVISIONS:
            print(f"\n── {div} banzuke ──")
            entries = _fetch_banzuke(basho_id, div)
            print(f"  {len(entries)} entries")
            divisions_to_scan.append((div, entries))

    for division, entries in divisions_to_scan:
        for entry in entries:
            wid = str(_get_dict(entry, "rikishiID", "id", "rikishi_id") or "")
            if not wid:
                continue

            rank_label = _build_rank_label(entry, division)
            rank_value = _rank_sort_value(rank_label)
            rank_str, rank_number, side = _parse_rank_label(rank_label)

            # ── Profile ───────────────────────────────────────────────
            detail = _fetch_profile(wid) or {}
            shikona_en = (detail.get("shikonaEn")
                          or entry.get("shikonaEn")
                          or entry.get("shikona", ""))
            shikona_jp = detail.get("shikona") or entry.get("shikona", "")
            heya       = detail.get("heya", "") or ""
            birth_date = detail.get("birthDate", "")
            height     = detail.get("height")
            weight     = detail.get("weight")
            shusshin   = detail.get("birthPlace", detail.get("shusshin", ""))
            nationality = detail.get("nationality", detail.get("country", ""))

            # Derive country from shusshin/nationality
            country = nationality or "Japan"
            prefecture = None
            if shusshin:
                if "," in shusshin:
                    parts = shusshin.split(",")
                    if country == "Japan" or not nationality:
                        prefecture = parts[0].strip()
                    else:
                        prefecture = parts[-1].strip()
                elif country == "Japan":
                    prefecture = shusshin.strip()

            # Highest rank
            highest_rank = detail.get("highestRank", "")
            # Debut
            debut_basho = detail.get("intpiDate", detail.get("debutBasho", ""))

            # ── Stats ─────────────────────────────────────────────────
            stats_raw = _fetch_stats(wid) or {}
            stats = _parse_stats(stats_raw) if stats_raw else {}

            # Log the first wrestler's stats keys for debugging field names
            if len(wrestler_ids) == 0 and stats_raw:
                loss_keys = [k for k in stats_raw if "loss" in k.lower()]
                print(f"  [debug] Stats keys with 'loss': {loss_keys}")

            # ── Fighting style (from existing classify or default) ────
            # Keep existing fighting_style if already set
            existing = conn.execute(
                "SELECT fighting_style FROM wrestlers WHERE wrestler_id = ?",
                (wid,)
            ).fetchone()
            fighting_style = (existing[0] if existing and existing[0]
                              and existing[0] != "hybrid" else "hybrid")

            # ── Upsert wrestler ───────────────────────────────────────
            wrestler_row = {
                "wrestler_id":      wid,
                "shikona":          shikona_jp or shikona_en,
                "shikona_en":       shikona_en,
                "shikona_jp":       shikona_jp,
                "heya":             heya or "Unknown",
                "birth_date":       birth_date[:10] if birth_date else None,
                "height_cm":        float(height) if height else None,
                "weight_kg":        float(weight) if weight else None,
                "country":          country,
                "prefecture":       prefecture,
                "nationality":      nationality,
                "shusshin":         shusshin,
                "api_id":           int(wid),
                "fighting_style":   fighting_style,
                "is_active":        1,
                "highest_rank":     highest_rank.lower().split()[0] if highest_rank else None,
                "debut_basho":      str(debut_basho) if debut_basho else None,
                # Current rank
                "current_rank":     rank_str,
                "current_rank_number": rank_number,
                "current_side":     side,
                "division":         division,
                "rank_label":       rank_label,
                "rank_value":       rank_value,
                "current_basho":    basho_id,
                # Career stats from API (authoritative)
                **stats,
                "updated_at":       now_iso,
            }
            _upsert_wrestler(conn, wrestler_row)

            # ── Basho entry ───────────────────────────────────────────
            basho_wins = _safe_int(entry.get("wins"))
            basho_losses = _safe_int(entry.get("losses"))
            basho_abs = _safe_int(entry.get("absences"))

            _upsert_basho_entry(conn, {
                "basho_id":     basho_dotted,
                "wrestler_id":  wid,
                "division":     division,
                "rank":         rank_str,
                "rank_number":  rank_number,
                "side":         side,
                "wins":         basho_wins,
                "losses":       basho_losses,
                "absences":     basho_abs,
                "is_kyujo":     basho_wins == 0 and basho_losses == 0 and basho_abs > 0,
            })

            # Also write to legacy banzuke table for backward compat
            conn.execute("""
                INSERT OR REPLACE INTO banzuke
                (basho_id, wrestler_id, rank, rank_number, side, division, is_kyujo)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (basho_dotted, wid, rank_str, rank_number, side,
                  division.lower(), 1 if basho_wins == 0 and basho_losses == 0 and basho_abs > 0 else 0))

            wrestler_ids.append(wid)
            print(f"  ✓ {shikona_en:<20} {rank_label:<24} "
                  f"{stats.get('career_wins',0)}W-{stats.get('career_losses',0)}L")

    conn.commit()
    return wrestler_ids


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2: Match History
# ═══════════════════════════════════════════════════════════════════════════════

def phase2_matches(conn: sqlite3.Connection, wrestler_ids: list[str]) -> int:
    """Fetch full match history for each wrestler."""
    total_bouts = 0

    for i, wid in enumerate(wrestler_ids, 1):
        # Get shikona for display
        row = conn.execute("SELECT shikona_en, shikona FROM wrestlers WHERE wrestler_id = ?",
                           (wid,)).fetchone()
        name = (row[0] or row[1] or wid) if row else wid

        print(f"  [{i}/{len(wrestler_ids)}] {name}...", end=" ", flush=True)

        data = _get(f"/rikishi/{wid}/matches",
                     cache_key=f"matches_{wid}",
                     params={"limit": str(MATCH_LIMIT)})

        if data is None:
            print("no data")
            continue

        # Normalize response format
        matches = []
        if isinstance(data, list):
            matches = data
        elif isinstance(data, dict):
            matches = (data.get("records")
                       or data.get("matches")
                       or data.get("results")
                       or [])

        # Purge old bout records for this wrestler to avoid duplicates
        # from prior scrapers that may have swapped east/west ordering.
        # Only purge if we got fresh data to replace them with.
        if matches:
            conn.execute(
                "DELETE FROM bout_records WHERE east_id = ? OR west_id = ?",
                (wid, wid),
            )
            conn.commit()

        inserted = 0
        for m in matches:
            basho_raw = str(m.get("bashoId", ""))
            basho_id = _convert_basho_id(basho_raw)
            if not basho_id or len(basho_id) != 7:
                continue

            day = _safe_int(m.get("day"), 0)
            if day < 1 or day > 16:
                continue

            east_id  = str(_get_dict(m, "eastId", "eastID") or "")
            west_id  = str(_get_dict(m, "westId", "westID") or "")
            winner_id = str(_get_dict(m, "winnerId", "winnerID") or "")
            kimarite = m.get("kimarite")
            division = m.get("division", "")
            east_rank = m.get("eastRank", "")
            west_rank = m.get("westRank", "")

            if not east_id or not west_id or not winner_id:
                continue
            if winner_id not in (east_id, west_id):
                continue

            try:
                conn.execute("""
                    INSERT OR REPLACE INTO bout_records
                    (basho_id, day, east_id, west_id, winner_id, kimarite,
                     division, east_rank, west_rank)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (basho_id, day, east_id, west_id, winner_id,
                      kimarite, division or None,
                      east_rank or None, west_rank or None))
                inserted += 1
            except Exception:
                pass

        conn.commit()
        total_bouts += inserted
        print(f"{len(matches)} matches, {inserted} new bouts")

    return total_bouts


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3: Historical Basho Entries
# ═══════════════════════════════════════════════════════════════════════════════

def phase3_history(conn: sqlite3.Connection, wrestler_ids: list[str]) -> int:
    """Build basho_entries from each wrestler's match history.

    Groups bout_records by basho to reconstruct per-tournament records.
    Then fetches basho metadata to determine yusho winners.
    """
    total_entries = 0

    for i, wid in enumerate(wrestler_ids, 1):
        row = conn.execute("SELECT shikona_en, shikona FROM wrestlers WHERE wrestler_id = ?",
                           (wid,)).fetchone()
        name = (row[0] or row[1] or wid) if row else wid

        # Get all basho this wrestler appears in from bout_records
        basho_rows = conn.execute("""
            SELECT basho_id,
                   COUNT(*) as total_bouts,
                   SUM(CASE WHEN winner_id = ? THEN 1 ELSE 0 END) as wins,
                   GROUP_CONCAT(DISTINCT division) as divisions
            FROM bout_records
            WHERE east_id = ? OR west_id = ?
            GROUP BY basho_id
            ORDER BY basho_id
        """, (wid, wid, wid)).fetchall()

        new_entries = 0
        for br in basho_rows:
            basho_id = br[0]
            total_bouts = br[1]
            wins = br[2]
            losses = total_bouts - wins

            # Skip if we already have this entry from Phase 1
            existing = conn.execute(
                "SELECT 1 FROM basho_entries WHERE basho_id = ? AND wrestler_id = ?",
                (basho_id, wid)
            ).fetchone()
            if existing:
                continue

            # Determine division from bout data
            division = br[3] or "Makuuchi"
            if "," in division:
                # Multiple divisions in same basho — take the highest
                divs = division.split(",")
                for d in ["Makuuchi", "Juryo", "Makushita"]:
                    if d in divs:
                        division = d
                        break

            _upsert_basho_entry(conn, {
                "basho_id":     basho_id,
                "wrestler_id":  wid,
                "division":     division,
                "wins":         wins,
                "losses":       losses,
                "absences":     0,
            })
            new_entries += 1

        if new_entries > 0:
            conn.commit()
            total_entries += new_entries

        if i % 10 == 0 or i == len(wrestler_ids):
            print(f"  [{i}/{len(wrestler_ids)}] {name}: {new_entries} historical entries")

    return total_entries


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 0: Backfill orphan wrestler IDs from bout_records
# ═══════════════════════════════════════════════════════════════════════════════

def phase0_backfill(conn: sqlite3.Connection) -> int:
    """Find wrestler IDs referenced in bout_records but missing from the
    wrestlers table, and fetch their basic profile from the API.

    Also backfills existing stub records that have no heya or profile data.

    Only fetches name, heya, and birth info — no stats or matches.
    These are historical opponents who appear in career record lookups.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    filled = 0
    failed = 0

    # ── Pass 1: IDs in bout_records with no row in wrestlers at all ───
    orphans = conn.execute("""
        SELECT DISTINCT wid FROM (
            SELECT east_id AS wid FROM bout_records
            UNION
            SELECT west_id AS wid FROM bout_records
        )
        WHERE wid NOT IN (SELECT wrestler_id FROM wrestlers)
    """).fetchall()
    orphan_ids = [r[0] for r in orphans]

    # ── Pass 2: Existing stubs with missing profile data ──────────────
    incomplete = conn.execute("""
        SELECT wrestler_id FROM wrestlers
        WHERE (heya IS NULL OR heya = '' OR heya = 'Unknown')
          AND height_cm IS NULL
    """).fetchall()
    incomplete_ids = [r[0] for r in incomplete]

    # Combine and deduplicate
    all_ids = list(dict.fromkeys(orphan_ids + incomplete_ids))

    if not all_ids:
        print("  All wrestlers have profile data — nothing to backfill")
        return 0

    print(f"  Found {len(orphan_ids)} orphan IDs + {len(incomplete_ids)} incomplete stubs = {len(all_ids)} to backfill")
    print(f"  Estimated time: ~{len(all_ids) * RATE_DELAY / 60:.0f} minutes")
    print(f"  Fetching basic profiles from API...")

    for i, wid in enumerate(all_ids, 1):
        detail = _fetch_profile(wid)

        if not detail:
            # Insert a minimal stub so the ID resolves to something
            conn.execute("""
                INSERT OR IGNORE INTO wrestlers
                (wrestler_id, shikona, heya, fighting_style, is_active)
                VALUES (?, ?, ?, ?, ?)
            """, (wid, f"Unknown #{wid}", "Unknown", "hybrid", 0))
            failed += 1
        else:
            shikona_en = detail.get("shikonaEn", "")
            shikona_jp = detail.get("shikona", "")
            heya = detail.get("heya", "") or "Unknown"
            birth_date = detail.get("birthDate", "")
            height = detail.get("height")
            weight = detail.get("weight")
            highest_rank = detail.get("highestRank", "")
            shusshin = detail.get("birthPlace", detail.get("shusshin", ""))
            nationality = detail.get("nationality", detail.get("country", ""))

            country = nationality or "Japan"
            prefecture = None
            if shusshin and "," in shusshin:
                parts = shusshin.split(",")
                if country == "Japan" or not nationality:
                    prefecture = parts[0].strip()

            _upsert_wrestler(conn, {
                "wrestler_id":  wid,
                "shikona":      shikona_jp or shikona_en or f"Unknown #{wid}",
                "shikona_en":   shikona_en,
                "shikona_jp":   shikona_jp,
                "heya":         heya,
                "birth_date":   birth_date[:10] if birth_date else None,
                "height_cm":    float(height) if height else None,
                "weight_kg":    float(weight) if weight else None,
                "country":      country,
                "prefecture":   prefecture,
                "api_id":       int(wid),
                "fighting_style": "hybrid",
                "is_active":    0,  # Historical opponents default to inactive
                "highest_rank": highest_rank.lower().split()[0] if highest_rank else None,
                "updated_at":   now_iso,
            })
            filled += 1

        if i % 50 == 0 or i == len(all_ids):
            conn.commit()
            print(f"    [{i}/{len(all_ids)}] {filled} filled, {failed} not found")

    conn.commit()
    print(f"  Done: {filled} profiles fetched, {failed} not found on API")
    return filled


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def run(args: argparse.Namespace) -> None:
    basho_id = args.basho
    print(f"╔══════════════════════════════════════════════════╗")
    print(f"║  SumoSim Full Data Scraper                      ║")
    print(f"║  Target basho: {basho_id:<35}║")
    print(f"╚══════════════════════════════════════════════════╝")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Schema
    print("\n── Schema ──")
    _ensure_schema(conn)

    # Phase 0: Backfill orphan opponents
    if args.backfill:
        print("\n── Phase 0: Backfill Orphan Opponents ──")
        phase0_backfill(conn)

    # Phase 1: Profiles + Stats
    print("\n── Phase 1: Profiles + Career Stats ──")
    wrestler_ids = phase1_profiles(
        conn, basho_id,
        single_wrestler=args.wrestler
    )
    print(f"\n  Total: {len(wrestler_ids)} wrestlers")

    # Phase 2: Match History
    if args.matches:
        print(f"\n── Phase 2: Match History ──")
        print(f"  Fetching full career bouts for {len(wrestler_ids)} wrestlers...")
        total = phase2_matches(conn, wrestler_ids)
        print(f"\n  Total: {total} new bout records")

    # Phase 3: Historical Basho Entries
    if args.history:
        if not args.matches:
            print("\n  ⚠ --history requires --matches (need bout data to build history)")
        else:
            print(f"\n── Phase 3: Historical Basho Entries ──")
            total = phase3_history(conn, wrestler_ids)
            print(f"\n  Total: {total} historical basho entries")

    conn.close()

    print(f"\n{'═'*52}")
    print(f"  ✅ Done. Database: {DB_PATH}")
    print(f"  Next: python -m tools.db_manage sync  (push to Supabase)")
    print(f"{'═'*52}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SumoSim comprehensive data scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m tools.scrape_full                     # profiles + stats (~5 min)
  python -m tools.scrape_full --matches           # + match history (~20 min)
  python -m tools.scrape_full --matches --history # + historical basho entries
  python -m tools.scrape_full --backfill          # fill in missing opponent names (~15 min)
  python -m tools.scrape_full --wrestler 19       # debug single wrestler
        """
    )
    parser.add_argument("--basho", default=DEFAULT_BASHO,
                        help=f"Target basho ID (default: {DEFAULT_BASHO})")
    parser.add_argument("--backfill", action="store_true",
                        help="Phase 0: fetch profiles for opponent IDs missing from wrestlers table")
    parser.add_argument("--matches", action="store_true",
                        help="Phase 2: fetch full match history for each wrestler")
    parser.add_argument("--history", action="store_true",
                        help="Phase 3: build historical basho entries from bout data")
    parser.add_argument("--wrestler", type=str, default=None,
                        help="Debug: scrape a single wrestler by ID")
    args = parser.parse_args()
    run(args)
