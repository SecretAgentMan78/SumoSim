"""
SumoSim Database Layer

Provides unified data access with Supabase as remote source of truth
and local SQLite as offline fallback. On startup:
  1. Try to connect to Supabase
  2. Sync any new data to local SQLite
  3. If Supabase is unavailable, use local SQLite

Usage:
    from data.db import SumoDatabase

    db = SumoDatabase()
    roster = db.get_roster("2026.03")
    records = db.get_tournament_records("hoshoryu")
    bouts = db.get_bout_records("2026.03")

    # Push local data to empty Supabase tables:
    db.push_local_to_supabase(["basho_entries", "bout_records"])
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from data.models import (
    BoutRecord,
    FightingStyle,
    Rank,
    TournamentRecord,
    WrestlerProfile,
)

logger = logging.getLogger(__name__)

# Default local DB path (alongside the package)
_DEFAULT_DB_PATH = Path(__file__).parent / "sumosim_local.db"

# Supabase config — set via environment variables or .env file
_SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
_SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")


def _row_get(row, key, default=None):
    """Safe accessor for sqlite3.Row — returns default if key missing or value is None."""
    try:
        val = row[key]
        return val if val is not None else default
    except (IndexError, KeyError):
        return default


def _parse_date(val: str | None) -> date | None:
    """Parse a date string safely, handling multiple ISO formats.

    Handles: '1999-05-22', '1999-05-22T00:00:00Z', '1999-05-22T00:00:00+00:00'
    Returns None if parsing fails or val is empty/None.
    """
    if not val:
        return None
    try:
        # Strip time component if present: take only the date part
        return date.fromisoformat(val[:10])
    except (ValueError, TypeError):
        return None


class SumoDatabase:
    """
    Unified data access layer.

    Reads from Supabase when online, falls back to local SQLite.
    Writes always go to Supabase first, then sync to local.
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        supabase_url: str = "",
        supabase_key: str = "",
    ):
        self._db_path = Path(db_path) if db_path else _DEFAULT_DB_PATH
        self._supabase_url = supabase_url or _SUPABASE_URL
        self._supabase_key = supabase_key or _SUPABASE_KEY
        self._http = None
        self._online = False

        # Always init local SQLite
        self._init_local_db()

        # Try Supabase connection
        self._try_connect_supabase()

    # ================================================================
    # Supabase connection (raw httpx — no SDK needed)
    # ================================================================

    def _try_connect_supabase(self) -> None:
        """Attempt to connect to Supabase REST API. Fail silently if unavailable."""
        if not self._supabase_url or not self._supabase_key:
            logger.info("No Supabase credentials — running in offline mode")
            return

        try:
            import httpx
            self._http = httpx.Client(
                base_url=f"{self._supabase_url.rstrip('/')}/rest/v1",
                headers={
                    "apikey": self._supabase_key,
                    "Authorization": f"Bearer {self._supabase_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                timeout=15.0,
            )
            # Quick connectivity check
            resp = self._http.get("/wrestlers", params={"select": "wrestler_id", "limit": "1"})
            resp.raise_for_status()
            self._online = True
            logger.info("Connected to Supabase")
        except ImportError:
            logger.warning("httpx not installed — running in offline mode")
            logger.warning("Install with: pip install httpx")
            self._http = None
            self._online = False
        except Exception as e:
            logger.warning(f"Supabase unavailable, using local DB: {e}")
            self._http = None
            self._online = False

    @property
    def is_online(self) -> bool:
        return self._online

    def _rest_upsert(self, table: str, data: dict | list[dict], on_conflict: str = "") -> None:
        """Upsert rows to Supabase via PostgREST UPSERT (POST with merge-duplicates).

        Args:
            table: Table name
            data: Row dict or list of row dicts
            on_conflict: Comma-separated column names for conflict resolution
                         (e.g. "wrestler_id" or "basho_id,wrestler_id")
        """
        headers = {
            **self._http.headers,
            "Prefer": "resolution=merge-duplicates",
        }
        params = {}
        if on_conflict:
            params["on_conflict"] = on_conflict

        resp = self._http.post(
            f"/{table}",
            json=data if isinstance(data, list) else [data],
            headers=headers,
            params=params,
        )
        if not resp.is_success:
            logger.error(f"Supabase upsert failed for {table}: {resp.status_code} {resp.text}")
        resp.raise_for_status()

    # ================================================================
    # Local SQLite setup
    # ================================================================

    def _init_local_db(self) -> None:
        """Create local SQLite tables if they don't exist."""
        conn = sqlite3.connect(str(self._db_path))
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS wrestlers (
                wrestler_id     TEXT PRIMARY KEY,
                shikona         TEXT NOT NULL,
                heya            TEXT NOT NULL,
                birth_date      TEXT,
                height_cm       REAL,
                weight_kg       REAL,
                fighting_style  TEXT NOT NULL DEFAULT 'hybrid',
                country         TEXT DEFAULT 'Japan',
                current_rank    TEXT,
                current_rank_number INTEGER,
                current_side    TEXT,
                current_basho   TEXT,
                shikona_jp      TEXT,
                shikona_full    TEXT,
                prefecture      TEXT,
                api_id          INTEGER,
                highest_rank    TEXT,
                highest_rank_number INTEGER,
                is_active       INTEGER DEFAULT 1,
                retired_date    TEXT,
                debut_basho     TEXT,
                career_wins     INTEGER DEFAULT 0,
                career_losses   INTEGER DEFAULT 0,
                career_absences INTEGER DEFAULT 0,
                total_yusho     INTEGER DEFAULT 0,
                updated_at      TEXT
            );

            CREATE TABLE IF NOT EXISTS banzuke (
                basho_id        TEXT NOT NULL,
                wrestler_id     TEXT NOT NULL,
                rank            TEXT NOT NULL,
                rank_number     INTEGER,
                side            TEXT,
                division        TEXT NOT NULL DEFAULT 'makuuchi',
                is_kyujo        INTEGER DEFAULT 0,
                kyujo_reason    TEXT,
                PRIMARY KEY (basho_id, wrestler_id)
            );

            CREATE TABLE IF NOT EXISTS tournament_records (
                basho_id        TEXT NOT NULL,
                wrestler_id     TEXT NOT NULL,
                rank            TEXT NOT NULL,
                rank_number     INTEGER,
                side            TEXT,
                wins            INTEGER NOT NULL DEFAULT 0,
                losses          INTEGER NOT NULL DEFAULT 0,
                absences        INTEGER NOT NULL DEFAULT 0,
                is_yusho        INTEGER DEFAULT 0,
                is_jun_yusho    INTEGER DEFAULT 0,
                special_prizes  TEXT DEFAULT '[]',
                PRIMARY KEY (basho_id, wrestler_id)
            );

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
            );

            CREATE TABLE IF NOT EXISTS bout_records (
                basho_id        TEXT NOT NULL,
                day             INTEGER NOT NULL,
                east_id         TEXT NOT NULL,
                west_id         TEXT NOT NULL,
                winner_id       TEXT NOT NULL,
                kimarite        TEXT,
                east_rank       TEXT,
                west_rank       TEXT,
                division        TEXT,
                PRIMARY KEY (basho_id, day, east_id, west_id)
            );

            CREATE TABLE IF NOT EXISTS injury_notes (
                basho_id        TEXT NOT NULL,
                wrestler_id     TEXT NOT NULL,
                severity        REAL NOT NULL DEFAULT 0.0,
                note            TEXT,
                PRIMARY KEY (basho_id, wrestler_id)
            );

            CREATE TABLE IF NOT EXISTS sync_metadata (
                table_name      TEXT PRIMARY KEY,
                last_synced_at  TEXT,
                row_count       INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS family_relations (
                wrestler_id     TEXT NOT NULL,
                related_id      TEXT NOT NULL,
                relationship    TEXT NOT NULL,
                PRIMARY KEY (wrestler_id, related_id)
            );

            CREATE TABLE IF NOT EXISTS modifier_overrides (
                wrestler_id     TEXT NOT NULL PRIMARY KEY,
                momentum        TEXT,
                injury_severity REAL DEFAULT 0.0
            );
        """)
        conn.close()

        # Upgrade existing DBs that may be missing new columns/tables
        self._ensure_local_schema()

    def _ensure_local_schema(self) -> None:
        """Add columns or tables that may be missing from an older local DB.

        Safe to call repeatedly — uses IF NOT EXISTS and checks PRAGMA.
        """
        conn = sqlite3.connect(str(self._db_path))

        # Ensure bout_records has division column
        bout_cols = {row[1] for row in conn.execute("PRAGMA table_info(bout_records)")}
        if "division" not in bout_cols:
            conn.execute("ALTER TABLE bout_records ADD COLUMN division TEXT")

        conn.commit()
        conn.close()

    def _local_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        return conn

    # ================================================================
    # SYNC: Supabase → Local SQLite
    # ================================================================

    def sync_all(self) -> dict[str, int]:
        """
        Pull all data from Supabase and upsert into local SQLite.
        Returns dict of {table_name: rows_synced}.
        """
        if not self._online:
            logger.warning("Cannot sync — Supabase is not connected")
            return {}

        results = {}
        results["wrestlers"] = self._sync_table_wrestlers()
        results["basho_entries"] = self._sync_table_basho_entries()
        results["bout_records"] = self._sync_table_bout_records()
        results["injury_notes"] = self._sync_table_injury_notes()
        results["family_relations"] = self._sync_table_family_relations()
        results["modifier_overrides"] = self._sync_table_modifier_overrides()

        logger.info(f"Sync complete: {results}")
        return results

    def _sync_table_wrestlers(self) -> int:
        # Paginate — PostgREST default limit is 1000
        all_rows = []
        page_size = 1000
        offset = 0
        while True:
            resp = self._http.get("/wrestlers", params={
                "select": "*",
                "offset": str(offset),
                "limit": str(page_size),
            })
            resp.raise_for_status()
            batch = resp.json()
            all_rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        conn = self._local_conn()
        for r in all_rows:
            conn.execute(
                """INSERT OR REPLACE INTO wrestlers
                   (wrestler_id, shikona, heya, birth_date, height_cm,
                    weight_kg, fighting_style, country, current_rank,
                    current_rank_number, current_side, current_basho,
                    shikona_jp, shikona_full, prefecture, api_id,
                    highest_rank, highest_rank_number, is_active,
                    retired_date, debut_basho, career_wins, career_losses,
                    career_absences, total_yusho, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (r["wrestler_id"], r["shikona"], r["heya"], r.get("birth_date"),
                 r.get("height_cm"), r.get("weight_kg"),
                 r.get("fighting_style", "hybrid"), r.get("country", "Japan"),
                 r.get("current_rank"), r.get("current_rank_number"),
                 r.get("current_side"), r.get("current_basho"),
                 r.get("shikona_jp"), r.get("shikona_full"),
                 r.get("prefecture"), r.get("api_id"),
                 r.get("highest_rank"), r.get("highest_rank_number"),
                 1 if r.get("is_active", True) else 0,
                 r.get("retired_date"), r.get("debut_basho"),
                 r.get("career_wins", 0), r.get("career_losses", 0),
                 r.get("career_absences", 0), r.get("total_yusho", 0),
                 r.get("updated_at")),
            )
        conn.execute(
            "INSERT OR REPLACE INTO sync_metadata VALUES (?, ?, ?)",
            ("wrestlers", datetime.now(timezone.utc).isoformat(), len(all_rows)),
        )
        conn.commit()
        conn.close()
        return len(all_rows)

    def _sync_table_basho_entries(self) -> int:
        """Sync basho_entries from Supabase to local SQLite (paginated)."""
        all_rows = []
        page_size = 1000
        offset = 0
        while True:
            resp = self._http.get("/basho_entries", params={
                "select": "*",
                "offset": str(offset),
                "limit": str(page_size),
            })
            resp.raise_for_status()
            batch = resp.json()
            all_rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        conn = self._local_conn()
        for r in all_rows:
            conn.execute(
                """INSERT OR REPLACE INTO basho_entries
                   (basho_id, wrestler_id, division, rank, rank_number, side,
                    wins, losses, absences, is_kyujo,
                    is_yusho, is_jun_yusho, special_prizes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (r["basho_id"], r["wrestler_id"],
                 r.get("division", "Makuuchi"),
                 r.get("rank"), r.get("rank_number"), r.get("side"),
                 r.get("wins", 0), r.get("losses", 0),
                 r.get("absences", 0),
                 1 if r.get("is_kyujo") else 0,
                 1 if r.get("is_yusho") else 0,
                 1 if r.get("is_jun_yusho") else 0,
                 json.dumps(r.get("special_prizes", []))),
            )

        # Also populate legacy banzuke table for backward compat
        for r in all_rows:
            conn.execute(
                """INSERT OR REPLACE INTO banzuke
                   (basho_id, wrestler_id, rank, rank_number, side, division, is_kyujo)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (r["basho_id"], r["wrestler_id"],
                 r.get("rank"), r.get("rank_number"), r.get("side"),
                 (r.get("division") or "Makuuchi").lower(),
                 1 if r.get("is_kyujo") else 0),
            )

        conn.execute(
            "INSERT OR REPLACE INTO sync_metadata VALUES (?, ?, ?)",
            ("basho_entries", datetime.now(timezone.utc).isoformat(), len(all_rows)),
        )
        conn.commit()
        conn.close()
        return len(all_rows)

    def _sync_table_bout_records(self) -> int:
        # Bout records can be large — fetch in pages
        all_rows = []
        page_size = 1000
        offset = 0
        while True:
            resp = self._http.get("/bout_records", params={
                "select": "*",
                "offset": str(offset),
                "limit": str(page_size),
            })
            resp.raise_for_status()
            batch = resp.json()
            all_rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        conn = self._local_conn()
        for r in all_rows:
            conn.execute(
                """INSERT OR REPLACE INTO bout_records
                   (basho_id, day, east_id, west_id, winner_id, kimarite,
                    east_rank, west_rank, division)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (r["basho_id"], r["day"], r["east_id"],
                 r["west_id"], r["winner_id"], r.get("kimarite"),
                 r.get("east_rank"), r.get("west_rank"),
                 r.get("division")),
            )
        conn.execute(
            "INSERT OR REPLACE INTO sync_metadata VALUES (?, ?, ?)",
            ("bout_records", datetime.now(timezone.utc).isoformat(), len(all_rows)),
        )
        conn.commit()
        conn.close()
        return len(all_rows)

    def _sync_table_injury_notes(self) -> int:
        resp = self._http.get("/injury_notes", params={"select": "*"})
        resp.raise_for_status()
        rows = resp.json()
        conn = self._local_conn()
        for r in rows:
            conn.execute(
                """INSERT OR REPLACE INTO injury_notes
                   (basho_id, wrestler_id, severity, note)
                   VALUES (?, ?, ?, ?)""",
                (r["basho_id"], r["wrestler_id"], r["severity"], r.get("note")),
            )
        conn.execute(
            "INSERT OR REPLACE INTO sync_metadata VALUES (?, ?, ?)",
            ("injury_notes", datetime.now(timezone.utc).isoformat(), len(rows)),
        )
        conn.commit()
        conn.close()
        return len(rows)

    def _sync_table_family_relations(self) -> int:
        try:
            resp = self._http.get("/family_relations", params={"select": "*"})
            resp.raise_for_status()
            rows = resp.json()
        except Exception:
            return 0
        conn = self._local_conn()
        for r in rows:
            conn.execute(
                """INSERT OR REPLACE INTO family_relations
                   (wrestler_id, related_id, relationship)
                   VALUES (?, ?, ?)""",
                (r["wrestler_id"], r["related_id"], r["relationship"]),
            )
        conn.execute(
            "INSERT OR REPLACE INTO sync_metadata VALUES (?, ?, ?)",
            ("family_relations", datetime.now(timezone.utc).isoformat(), len(rows)),
        )
        conn.commit()
        conn.close()
        return len(rows)

    def _sync_table_modifier_overrides(self) -> int:
        try:
            resp = self._http.get("/modifier_overrides", params={"select": "*"})
            resp.raise_for_status()
            rows = resp.json()
        except Exception:
            return 0
        conn = self._local_conn()
        for r in rows:
            conn.execute(
                """INSERT OR REPLACE INTO modifier_overrides
                   (wrestler_id, momentum, injury_severity)
                   VALUES (?, ?, ?)""",
                (r["wrestler_id"], r.get("momentum"), r.get("injury_severity", 0.0)),
            )
        conn.execute(
            "INSERT OR REPLACE INTO sync_metadata VALUES (?, ?, ?)",
            ("modifier_overrides", datetime.now(timezone.utc).isoformat(), len(rows)),
        )
        conn.commit()
        conn.close()
        return len(rows)

    # ================================================================
    # PUSH: Local SQLite → Supabase (for initial population)
    # ================================================================

    def push_local_to_supabase(self, tables: list[str] | None = None) -> dict[str, int]:
        """Push local SQLite data up to Supabase.

        Use this to populate empty Supabase tables from local data.
        Pushes in batches of 500 with upsert (merge-duplicates).

        Args:
            tables: List of table names to push. If None, pushes all
                    data tables (wrestlers, basho_entries, bout_records,
                    injury_notes, family_relations, modifier_overrides).

        Returns: {table_name: rows_pushed}
        """
        if not self._online:
            logger.warning("Cannot push — Supabase is not connected")
            return {}

        all_tables = tables or [
            "wrestlers", "basho_entries", "bout_records",
            "injury_notes", "family_relations", "modifier_overrides",
        ]

        results = {}
        for table in all_tables:
            try:
                n = self._push_table(table)
                results[table] = n
            except Exception as e:
                logger.error(f"Failed to push {table}: {e}")
                results[table] = -1

        logger.info(f"Push complete: {results}")
        return results

    def _push_table(self, table: str) -> int:
        """Push a single table from local SQLite to Supabase."""
        # Table-specific config: (conflict_columns, column_transforms, query)
        table_config = {
            "wrestlers": {
                "on_conflict": "wrestler_id",
                "transforms": {
                    "is_active": lambda v: bool(v),
                },
                # Only push wrestlers that have at least a shikona
                # (skips empty stubs that would violate NOT NULL)
                "query": "SELECT * FROM wrestlers WHERE shikona IS NOT NULL AND shikona != ''",
            },
            "basho_entries": {
                "on_conflict": "basho_id,wrestler_id",
                "transforms": {
                    "is_kyujo": lambda v: bool(v),
                    "is_yusho": lambda v: bool(v),
                    "is_jun_yusho": lambda v: bool(v),
                    "special_prizes": lambda v: json.loads(v) if isinstance(v, str) else (v or []),
                },
            },
            "bout_records": {
                "on_conflict": "basho_id,day,east_id,west_id",
                "transforms": {},
            },
            "injury_notes": {
                "on_conflict": "basho_id,wrestler_id",
                "transforms": {},
            },
            "family_relations": {
                "on_conflict": "wrestler_id,related_id",
                "transforms": {},
            },
            "modifier_overrides": {
                "on_conflict": "wrestler_id",
                "transforms": {},
            },
        }

        config = table_config.get(table)
        if not config:
            logger.warning(f"Unknown table for push: {table}")
            return 0

        conn = self._local_conn()
        query = config.get("query", f"SELECT * FROM {table}")
        rows = conn.execute(query).fetchall()
        conn.close()

        if not rows:
            return 0

        # Discover which columns Supabase actually has for this table
        # by fetching a single row with select=* and checking the keys.
        # Falls back to sending all columns if discovery fails.
        supabase_cols = None
        try:
            probe = self._http.get(f"/{table}", params={"select": "*", "limit": "1"})
            probe.raise_for_status()
            probe_rows = probe.json()
            if probe_rows:
                supabase_cols = set(probe_rows[0].keys())
            else:
                # Empty table — try OPTIONS or just send and let PostgREST tell us
                # We'll rely on the column definitions endpoint
                defn = self._http.get(f"/{table}", params={"select": "*", "limit": "0"})
                # PostgREST returns column info in the response even for 0 rows
                # if we inspect the schema via a HEAD + content-type parse.
                # Safest fallback: just use all columns and let errors tell us.
                supabase_cols = None
        except Exception:
            supabase_cols = None

        # Convert sqlite3.Row objects to dicts, applying transforms
        # and filtering to only columns Supabase knows about
        transforms = config["transforms"]
        dicts = []
        for r in rows:
            d = dict(r)
            for col, fn in transforms.items():
                if col in d:
                    d[col] = fn(d[col])
            # Strip columns Supabase doesn't have
            if supabase_cols is not None:
                d = {k: v for k, v in d.items() if k in supabase_cols}
            dicts.append(d)

        # Push in batches
        batch_size = 500
        on_conflict = config["on_conflict"]
        pushed = 0
        for i in range(0, len(dicts), batch_size):
            batch = dicts[i:i + batch_size]
            try:
                self._rest_upsert(table, batch, on_conflict=on_conflict)
                pushed += len(batch)
            except Exception as e:
                logger.error(f"Batch {i//batch_size + 1} failed for {table}: {e}")
                # Continue with remaining batches instead of aborting
                continue

        return pushed

    # ================================================================
    # WRITE: Push to Supabase (and sync to local)
    # ================================================================

    def upsert_wrestler(self, w: WrestlerProfile, basho_id: str = "") -> None:
        """Insert or update a wrestler in both Supabase and local."""
        row = {
            "wrestler_id": w.wrestler_id,
            "shikona": w.shikona,
            "heya": w.heya,
            "birth_date": w.birth_date.isoformat() if w.birth_date else None,
            "height_cm": w.height_cm,
            "weight_kg": w.weight_kg,
            "fighting_style": w.fighting_style.value,
            "country": w.country,
            "current_rank": w.rank.value,
            "current_rank_number": w.rank_number,
            "current_side": w.side,
            "current_basho": basho_id or None,
        }
        if self._online:
            self._rest_upsert("wrestlers", row, on_conflict="wrestler_id")

        conn = self._local_conn()
        conn.execute(
            """INSERT OR REPLACE INTO wrestlers
               (wrestler_id, shikona, heya, birth_date, height_cm,
                weight_kg, fighting_style, country, current_rank,
                current_rank_number, current_side, current_basho, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (row["wrestler_id"], row["shikona"], row["heya"],
             row["birth_date"], row["height_cm"], row["weight_kg"],
             row["fighting_style"], row["country"], row["current_rank"],
             row["current_rank_number"], row["current_side"],
             row["current_basho"], datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()

    def upsert_bout_records(self, records: list[BoutRecord]) -> int:
        """Bulk insert bout records. Returns count inserted."""
        if not records:
            return 0

        rows = [
            {
                "basho_id": br.basho_id,
                "day": br.day,
                "east_id": br.east_id,
                "west_id": br.west_id,
                "winner_id": br.winner_id,
                "kimarite": br.kimarite,
            }
            for br in records
        ]

        if self._online:
            # Supabase upsert in batches of 500
            for i in range(0, len(rows), 500):
                batch = rows[i:i + 500]
                self._rest_upsert("bout_records", batch, on_conflict="basho_id,day,east_id,west_id")

        conn = self._local_conn()
        for r in rows:
            conn.execute(
                """INSERT OR REPLACE INTO bout_records
                   (basho_id, day, east_id, west_id, winner_id, kimarite)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (r["basho_id"], r["day"], r["east_id"],
                 r["west_id"], r["winner_id"], r["kimarite"]),
            )
        conn.commit()
        conn.close()
        return len(rows)

    def upsert_tournament_record(self, tr: TournamentRecord) -> None:
        """Insert or update a tournament record into basho_entries."""
        row = {
            "basho_id": tr.basho_id,
            "wrestler_id": tr.wrestler_id,
            "rank": tr.rank.value,
            "rank_number": tr.rank_number,
            "side": tr.side,
            "wins": tr.wins,
            "losses": tr.losses,
            "absences": tr.absences,
            "is_yusho": tr.is_yusho,
            "is_jun_yusho": tr.is_jun_yusho,
            "special_prizes": list(tr.special_prizes),
        }
        if self._online:
            self._rest_upsert("basho_entries", row, on_conflict="basho_id,wrestler_id")

        conn = self._local_conn()
        conn.execute(
            """INSERT OR REPLACE INTO basho_entries
               (basho_id, wrestler_id, rank, rank_number, side, wins, losses,
                absences, is_yusho, is_jun_yusho, special_prizes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (row["basho_id"], row["wrestler_id"], row["rank"],
             row["rank_number"], row.get("side"),
             row["wins"], row["losses"],
             row["absences"], 1 if row["is_yusho"] else 0,
             1 if row["is_jun_yusho"] else 0,
             json.dumps(row["special_prizes"])),
        )
        conn.commit()
        conn.close()

    def upsert_injury_note(
        self, basho_id: str, wrestler_id: str, severity: float, note: str = ""
    ) -> None:
        """Insert or update an injury note."""
        row = {
            "basho_id": basho_id,
            "wrestler_id": wrestler_id,
            "severity": severity,
            "note": note,
        }
        if self._online:
            self._rest_upsert("injury_notes", row, on_conflict="basho_id,wrestler_id")

        conn = self._local_conn()
        conn.execute(
            """INSERT OR REPLACE INTO injury_notes
               (basho_id, wrestler_id, severity, note)
               VALUES (?, ?, ?, ?)""",
            (row["basho_id"], row["wrestler_id"], row["severity"], row["note"]),
        )
        conn.commit()
        conn.close()

    # ================================================================
    # READ: Query from local SQLite (always available)
    # ================================================================

    def get_roster(self, basho_id: str) -> list[WrestlerProfile]:
        """Get the full roster for a basho as WrestlerProfile objects.

        Rank comes from basho_entries (or legacy banzuke) for the specified basho,
        so historical queries return the rank at that time, not the current rank.
        """
        conn = self._local_conn()

        # Try basho_entries first, fall back to banzuke
        be_count = conn.execute(
            "SELECT COUNT(*) FROM basho_entries WHERE basho_id = ?", (basho_id,)
        ).fetchone()[0]

        if be_count > 0:
            source_table = "basho_entries"
            kyujo_filter = "AND COALESCE(b.is_kyujo, 0) = 0"
        else:
            source_table = "banzuke"
            kyujo_filter = "AND b.is_kyujo = 0"

        rows = conn.execute(
            f"""SELECT w.wrestler_id, w.shikona, w.heya, w.birth_date,
                      w.height_cm, w.weight_kg, w.fighting_style, w.country,
                      w.shikona_jp, w.prefecture, w.api_id,
                      w.highest_rank, w.highest_rank_number,
                      w.is_active, w.debut_basho,
                      w.career_wins, w.career_losses, w.career_absences,
                      w.total_yusho,
                      b.rank, b.rank_number, b.side, b.division
               FROM wrestlers w
               JOIN {source_table} b ON w.wrestler_id = b.wrestler_id
               WHERE b.basho_id = ? {kyujo_filter}
               ORDER BY
                   CASE b.rank
                       WHEN 'yokozuna' THEN 1
                       WHEN 'ozeki' THEN 2
                       WHEN 'sekiwake' THEN 3
                       WHEN 'komusubi' THEN 4
                       WHEN 'maegashira' THEN 5
                       WHEN 'juryo' THEN 6
                   END,
                   b.rank_number""",
            (basho_id,),
        ).fetchall()
        conn.close()

        return [
            WrestlerProfile(
                wrestler_id=r["wrestler_id"],
                shikona=r["shikona"],
                heya=r["heya"],
                birth_date=_parse_date(r["birth_date"]),
                height_cm=r["height_cm"],
                weight_kg=r["weight_kg"],
                fighting_style=FightingStyle(r["fighting_style"]),
                rank=Rank(r["rank"]),
                rank_number=r["rank_number"],
                side=r["side"],
                country=r["country"] or "Japan",
                shikona_jp=_row_get(r, "shikona_jp"),
                prefecture=_row_get(r, "prefecture"),
                api_id=_row_get(r, "api_id"),
                highest_rank=_row_get(r, "highest_rank"),
                highest_rank_number=_row_get(r, "highest_rank_number"),
                is_active=bool(_row_get(r, "is_active", 1)),
                debut_basho=_row_get(r, "debut_basho"),
                career_wins=_row_get(r, "career_wins", 0) or 0,
                career_losses=_row_get(r, "career_losses", 0) or 0,
                career_absences=_row_get(r, "career_absences", 0) or 0,
                total_yusho=_row_get(r, "total_yusho", 0) or 0,
            )
            for r in rows
        ]

    def _basho_entry_to_tournament_record(self, r) -> TournamentRecord | None:
        """Convert a basho_entries row to a TournamentRecord model."""
        try:
            rank_str = (r["rank"] or "maegashira").lower().split()[0]
            valid_ranks = {e.value: e for e in Rank}
            rank = valid_ranks.get(rank_str, Rank.MAEGASHIRA)

            return TournamentRecord(
                basho_id=r["basho_id"],
                wrestler_id=r["wrestler_id"],
                rank=rank,
                rank_number=_row_get(r, "rank_number"),
                side=(_row_get(r, "side") or "").lower() or None,
                wins=r["wins"],
                losses=r["losses"],
                absences=_row_get(r, "absences", 0),
                is_yusho=bool(_row_get(r, "is_yusho", 0)),
                is_jun_yusho=bool(_row_get(r, "is_jun_yusho", 0)),
                special_prizes=tuple(json.loads(_row_get(r, "special_prizes", "[]") or "[]")),
            )
        except Exception as e:
            logger.debug(f"Skipped basho entry: {e}")
            return None

    def get_tournament_records(
        self, wrestler_id: str, limit: int = 6
    ) -> list[TournamentRecord]:
        """Get recent tournament records for a wrestler from basho_entries."""
        conn = self._local_conn()

        # Primary: basho_entries
        rows = conn.execute(
            """SELECT * FROM basho_entries
               WHERE wrestler_id = ?
               ORDER BY basho_id DESC LIMIT ?""",
            (wrestler_id, limit),
        ).fetchall()

        # Fallback: tournament_records (legacy data not yet migrated)
        if not rows:
            rows = conn.execute(
                """SELECT * FROM tournament_records
                   WHERE wrestler_id = ?
                   ORDER BY basho_id DESC LIMIT ?""",
                (wrestler_id, limit),
            ).fetchall()

        conn.close()

        results = []
        for r in rows:
            tr = self._basho_entry_to_tournament_record(r)
            if tr:
                results.append(tr)
        return results

    def get_all_tournament_records(self, basho_id: str = "") -> dict[str, list[TournamentRecord]]:
        """Get tournament records grouped by wrestler_id from basho_entries.

        If basho_id is given, only return records for the most recent 3 basho
        up to and including that basho.
        """
        conn = self._local_conn()

        # Determine source table — prefer basho_entries if it has data
        be_count = conn.execute("SELECT COUNT(*) FROM basho_entries").fetchone()[0]
        table = "basho_entries" if be_count > 0 else "tournament_records"

        if basho_id:
            basho_rows = conn.execute(
                f"SELECT DISTINCT basho_id FROM {table} WHERE basho_id <= ? ORDER BY basho_id DESC LIMIT 3",
                (basho_id,),
            ).fetchall()
            basho_ids = [b["basho_id"] for b in basho_rows]
            if not basho_ids:
                conn.close()
                return {}
            placeholders = ",".join("?" * len(basho_ids))
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE basho_id IN ({placeholders}) ORDER BY basho_id",
                basho_ids,
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT * FROM {table} ORDER BY basho_id"
            ).fetchall()
        conn.close()

        records: dict[str, list[TournamentRecord]] = {}
        for r in rows:
            tr = self._basho_entry_to_tournament_record(r)
            if tr:
                records.setdefault(r["wrestler_id"], []).append(tr)
        return records

    def get_bout_records(self, basho_id: str = "") -> list[BoutRecord]:
        """
        Get bout records. If basho_id is empty, returns all records.
        """
        conn = self._local_conn()
        if basho_id:
            rows = conn.execute(
                "SELECT * FROM bout_records WHERE basho_id = ? ORDER BY day",
                (basho_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM bout_records ORDER BY basho_id, day"
            ).fetchall()
        conn.close()

        return [
            BoutRecord(
                basho_id=r["basho_id"],
                day=r["day"],
                east_id=r["east_id"],
                west_id=r["west_id"],
                winner_id=r["winner_id"],
                kimarite=r["kimarite"],
            )
            for r in rows
        ]

    def get_injury_notes(self, basho_id: str) -> dict[str, dict]:
        """Get injury notes for a basho as {wrestler_id: {severity, note}}."""
        conn = self._local_conn()
        rows = conn.execute(
            "SELECT * FROM injury_notes WHERE basho_id = ?",
            (basho_id,),
        ).fetchall()
        conn.close()

        return {
            r["wrestler_id"]: {
                "severity": r["severity"],
                "note": _row_get(r, "note", ""),
            }
            for r in rows
        }

    def get_available_basho(self) -> list[str]:
        """Return list of basho IDs that have entry data, newest first."""
        conn = self._local_conn()
        # Combine basho IDs from both tables
        rows = conn.execute(
            """SELECT DISTINCT basho_id FROM (
                   SELECT basho_id FROM basho_entries
                   UNION
                   SELECT basho_id FROM banzuke
               ) ORDER BY basho_id DESC"""
        ).fetchall()
        conn.close()
        return [r["basho_id"] for r in rows]

    def get_sync_status(self) -> dict[str, dict]:
        """Return sync metadata for all tables."""
        conn = self._local_conn()
        rows = conn.execute("SELECT * FROM sync_metadata").fetchall()
        conn.close()
        return {
            r["table_name"]: {
                "last_synced_at": r["last_synced_at"],
                "row_count": r["row_count"],
            }
            for r in rows
        }

    # ================================================================
    # DOSSIER: Extended queries for the Rikishi Dossier panel
    # ================================================================

    def get_all_wrestlers(self, active_only: bool = True) -> list[WrestlerProfile]:
        """Get wrestlers for the Rikishi Dossier panel.

        Two modes:
          active_only=True  → Current Makuuchi + Juryo sekitori, ordered by rank
                              (Y1E first → J14W last).
          active_only=False → All wrestlers with meaningful data, ordered by
                              total_yusho descending then career_wins descending.

        Data source strategy:
          - Makuuchi wrestlers come from the banzuke table (authoritative rank).
          - Juryo wrestlers come from the wrestlers table rank_label/rank_value
            columns (populated by scrape_rikishi.py but not in the banzuke table).
          - Stub records (~2000 empty rows from bout history scraping) are
            excluded by requiring rank_value to be set OR a banzuke entry.
        """
        conn = self._local_conn()

        # ── Detect which columns exist on the wrestlers table ─────────
        col_info = conn.execute("PRAGMA table_info(wrestlers)").fetchall()
        live_cols = {row[1] for row in col_info}
        has_rank_value = "rank_value" in live_cols
        has_rank_label = "rank_label" in live_cols

        # ── Find the most recent basho in the banzuke table ──────────
        latest_row = conn.execute(
            "SELECT MAX(basho_id) AS latest FROM banzuke"
        ).fetchone()
        latest_basho = latest_row["latest"] if latest_row else None

        if active_only:
            seen_ids = set()
            rows = []

            # ── Part 1: Makuuchi from banzuke (authoritative) ─────────
            if latest_basho:
                banzuke_rows = conn.execute(
                    """SELECT w.*,
                              b.rank      AS banzuke_rank,
                              b.rank_number AS banzuke_rank_number,
                              b.side      AS banzuke_side,
                              b.division  AS banzuke_division
                       FROM wrestlers w
                       JOIN banzuke b ON w.wrestler_id = b.wrestler_id
                       WHERE b.basho_id = ?
                       ORDER BY
                           CASE lower(b.rank)
                               WHEN 'yokozuna'  THEN 1
                               WHEN 'ozeki'     THEN 2
                               WHEN 'sekiwake'  THEN 3
                               WHEN 'komusubi'  THEN 4
                               WHEN 'maegashira' THEN 5
                               WHEN 'juryo'     THEN 6
                               ELSE 7
                           END,
                           b.rank_number,
                           CASE lower(COALESCE(b.side, ''))
                               WHEN 'east' THEN 0
                               ELSE 1
                           END""",
                    (latest_basho,),
                ).fetchall()
                for r in banzuke_rows:
                    seen_ids.add(r["wrestler_id"])
                rows.extend(banzuke_rows)

            # ── Part 2: Juryo (and any other sekitori not in banzuke) ─
            # These were written by scrape_rikishi.py with rank_value set
            if has_rank_value:
                # Juryo rank_value range: 200-227 per scrape_rikishi.py
                # But also catch any Makuuchi wrestlers missed by banzuke
                juryo_rows = conn.execute(
                    """SELECT *, NULL AS banzuke_rank, NULL AS banzuke_rank_number,
                              NULL AS banzuke_side, NULL AS banzuke_division
                       FROM wrestlers
                       WHERE is_active = 1
                         AND rank_value IS NOT NULL
                         AND wrestler_id NOT IN (
                             SELECT wrestler_id FROM banzuke
                             WHERE basho_id = ?
                         )
                       ORDER BY rank_value, shikona""",
                    (latest_basho or "",),
                ).fetchall()
                for r in juryo_rows:
                    if r["wrestler_id"] not in seen_ids:
                        seen_ids.add(r["wrestler_id"])
                        rows.append(r)
        else:
            # ── All mode: every wrestler with meaningful data ─────────
            # Exclude stubs: require either rank_value set, career_wins > 0,
            # or a banzuke entry
            if has_rank_value:
                rows = conn.execute(
                    """SELECT *, NULL AS banzuke_rank, NULL AS banzuke_rank_number,
                              NULL AS banzuke_side, NULL AS banzuke_division
                       FROM wrestlers
                       WHERE rank_value IS NOT NULL
                          OR career_wins > 10
                          OR total_yusho > 0
                          OR highest_rank IN ('yokozuna', 'ozeki')
                       ORDER BY
                           COALESCE(total_yusho, 0) DESC,
                           COALESCE(career_wins, 0) DESC,
                           shikona"""
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT *, NULL AS banzuke_rank, NULL AS banzuke_rank_number,
                              NULL AS banzuke_side, NULL AS banzuke_division
                       FROM wrestlers
                       WHERE career_wins > 10
                          OR total_yusho > 0
                          OR highest_rank IN ('yokozuna', 'ozeki')
                       ORDER BY
                           COALESCE(total_yusho, 0) DESC,
                           COALESCE(career_wins, 0) DESC,
                           shikona"""
                ).fetchall()
        conn.close()

        # ── Pre-compute bout stats for wrestlers missing career totals ─
        # This avoids per-wrestler queries inside the loop.
        _bout_stats_cache: dict[str, tuple[int, int]] = {}  # {wid: (wins, losses)}
        wids_needing_stats = []
        for r in rows:
            cw = _row_get(r, "career_wins", 0) or 0
            cl = _row_get(r, "career_losses", 0) or 0
            # If aggregate career columns are empty, we need bout_records
            if cw == 0 or cl == 0:
                wids_needing_stats.append(r["wrestler_id"])

        if wids_needing_stats:
            conn2 = self._local_conn()
            try:
                # Wins: count where wrestler is winner
                placeholders = ",".join("?" * len(wids_needing_stats))
                win_rows = conn2.execute(
                    f"""SELECT winner_id, COUNT(*) as cnt
                        FROM bout_records
                        WHERE winner_id IN ({placeholders})
                        GROUP BY winner_id""",
                    wids_needing_stats,
                ).fetchall()
                for wr in win_rows:
                    _bout_stats_cache[wr["winner_id"]] = (wr["cnt"], 0)

                # Losses: count total bouts per wrestler, then subtract wins
                # Total bouts as east
                east_rows = conn2.execute(
                    f"""SELECT east_id, COUNT(*) as cnt
                        FROM bout_records
                        WHERE east_id IN ({placeholders})
                        GROUP BY east_id""",
                    wids_needing_stats,
                ).fetchall()
                total_bouts: dict[str, int] = {}
                for er in east_rows:
                    total_bouts[er["east_id"]] = er["cnt"]

                # Total bouts as west
                west_rows = conn2.execute(
                    f"""SELECT west_id, COUNT(*) as cnt
                        FROM bout_records
                        WHERE west_id IN ({placeholders})
                        GROUP BY west_id""",
                    wids_needing_stats,
                ).fetchall()
                for wr2 in west_rows:
                    total_bouts[wr2["west_id"]] = total_bouts.get(wr2["west_id"], 0) + wr2["cnt"]

                # Losses = total bouts - wins
                for wid in wids_needing_stats:
                    wins = _bout_stats_cache.get(wid, (0, 0))[0]
                    total = total_bouts.get(wid, 0)
                    losses = total - wins
                    _bout_stats_cache[wid] = (wins, losses)
            except Exception as e:
                logger.debug(f"Bout stats pre-computation failed: {e}")
            finally:
                conn2.close()

        result = []
        for r in rows:
            try:
                # ── Determine rank ────────────────────────────────────
                # Priority: banzuke JOIN > rank_label column > current_rank
                banzuke_rank = _row_get(r, "banzuke_rank")
                rank_label_col = _row_get(r, "rank_label", "") if has_rank_label else ""

                if banzuke_rank:
                    rank_str = banzuke_rank.lower().split()[0]
                elif rank_label_col:
                    rank_str = rank_label_col.lower().split()[0]
                else:
                    rank_str = (
                        _row_get(r, "current_rank")
                        or _row_get(r, "highest_rank")
                        or "maegashira"
                    )
                    rank_str = rank_str.lower().split()[0]

                valid_ranks = {e.value: e for e in Rank}
                rank = valid_ranks.get(rank_str, Rank.MAEGASHIRA)

                # ── Rank number ───────────────────────────────────────
                rank_number = _row_get(r, "banzuke_rank_number")
                if not rank_number and rank_label_col:
                    parts = rank_label_col.split()
                    if len(parts) >= 2 and parts[1].isdigit():
                        rank_number = int(parts[1])
                if not rank_number:
                    rank_number = (
                        _row_get(r, "current_rank_number")
                        or _row_get(r, "highest_rank_number")
                    )
                # Clamp to valid range for the model (1-18)
                if rank_number is not None:
                    rank_number = max(1, min(18, int(rank_number)))

                # ── Side ──────────────────────────────────────────────
                side = (_row_get(r, "banzuke_side") or "").lower() or None
                if not side and rank_label_col:
                    if rank_label_col.endswith("East"):
                        side = "east"
                    elif rank_label_col.endswith("West"):
                        side = "west"
                if not side:
                    side = (_row_get(r, "current_side") or "").lower() or None
                if side and side not in ("east", "west"):
                    side = None

                result.append(WrestlerProfile(
                    wrestler_id=r["wrestler_id"],
                    shikona=r["shikona"],
                    heya=r["heya"] or "",
                    birth_date=_parse_date(r["birth_date"]),
                    height_cm=_row_get(r, "height_cm"),
                    weight_kg=_row_get(r, "weight_kg"),
                    fighting_style=FightingStyle(r["fighting_style"]) if r["fighting_style"] in [e.value for e in FightingStyle] else FightingStyle.HYBRID,
                    rank=rank,
                    rank_number=rank_number,
                    side=side,
                    country=_row_get(r, "country") or "Japan",
                    shikona_jp=_row_get(r, "shikona_jp"),
                    shikona_full=_row_get(r, "shikona_full"),
                    prefecture=_row_get(r, "prefecture"),
                    api_id=_row_get(r, "api_id"),
                    highest_rank=_row_get(r, "highest_rank"),
                    highest_rank_number=_row_get(r, "highest_rank_number"),
                    is_active=bool(_row_get(r, "is_active", 1)),
                    debut_basho=_row_get(r, "debut_basho"),
                    career_wins=self._career_stat(r, "wins", _bout_stats_cache),
                    career_losses=self._career_stat(r, "losses", _bout_stats_cache),
                    career_absences=self._career_stat(r, "absences", _bout_stats_cache),
                    total_yusho=self._career_yusho(r),
                ))
            except Exception as e:
                logger.debug(f"Skipped wrestler {_row_get(r, 'shikona', '?')}: {e}")
                continue
        return result

    def _career_stat(self, row, stat_type: str, bout_cache: dict = None) -> int:
        """Get career total for a stat, falling back to division columns or bout_records cache.

        Priority:
          1. career_wins/career_losses/career_absences (legacy aggregate columns)
          2. Pre-computed count from bout_records (authoritative, all divisions)
          3. Sum of makuuchi_ + juryo_ columns (scrape_rikishi partial data)
        """
        aggregate = _row_get(row, f"career_{stat_type}", 0) or 0
        if aggregate > 0:
            return aggregate

        # Prefer bout_records cache — covers ALL divisions
        if bout_cache and stat_type in ("wins", "losses"):
            wid = row["wrestler_id"]
            cached = bout_cache.get(wid)
            if cached:
                val = cached[0] if stat_type == "wins" else cached[1]
                if val > 0:
                    return val

        # Fall back to summing division-specific columns
        maku = _row_get(row, f"makuuchi_{stat_type}", 0) or 0
        juryo = _row_get(row, f"juryo_{stat_type}", 0) or 0
        div_total = maku + juryo
        if div_total > 0:
            return div_total

        return 0

    @staticmethod
    def _career_yusho(row) -> int:
        """Get Makuuchi yusho count only."""
        total = _row_get(row, "total_yusho", 0) or 0
        if total > 0:
            return total
        # Only count Makuuchi yusho — not Juryo or lower divisions
        return _row_get(row, "yusho_makuuchi", 0) or 0

    def get_top_kimarite(self, wrestler_id: str, n: int = 3) -> list[tuple[str, int]]:
        """Get top N kimarite for a wrestler from bout records.

        Returns list of (kimarite, count) sorted by frequency.
        """
        conn = self._local_conn()
        rows = conn.execute(
            """SELECT kimarite, COUNT(*) as cnt
               FROM bout_records
               WHERE winner_id = ? AND kimarite IS NOT NULL AND kimarite != ''
               GROUP BY kimarite
               ORDER BY cnt DESC
               LIMIT ?""",
            (wrestler_id, n),
        ).fetchall()
        conn.close()
        return [(r["kimarite"], r["cnt"]) for r in rows]

    def get_recent_basho_records(
        self, wrestler_id: str, n: int = 5
    ) -> list[TournamentRecord]:
        """Get the most recent N tournament records for a wrestler."""
        return self.get_tournament_records(wrestler_id, limit=n)

    def get_career_bouts(
        self, wrestler_id: str
    ) -> list[BoutRecord]:
        """Get all career bouts for a wrestler (for lifetime record drilldown)."""
        conn = self._local_conn()
        rows = conn.execute(
            """SELECT * FROM bout_records
               WHERE east_id = ? OR west_id = ?
               ORDER BY basho_id, day""",
            (wrestler_id, wrestler_id),
        ).fetchall()
        conn.close()

        return [
            BoutRecord(
                basho_id=r["basho_id"],
                day=r["day"],
                east_id=r["east_id"],
                west_id=r["west_id"],
                winner_id=r["winner_id"],
                kimarite=r["kimarite"],
            )
            for r in rows
        ]

    def get_career_bouts_detailed(
        self, wrestler_id: str
    ) -> list[dict]:
        """Get all career bouts with rank data for the career record dialog.

        Returns list of dicts with keys: basho_id, day, east_id, west_id,
        winner_id, kimarite, east_rank, west_rank, division.
        """
        conn = self._local_conn()
        rows = conn.execute(
            """SELECT basho_id, day, east_id, west_id, winner_id,
                      kimarite, division,
                      east_rank, west_rank
               FROM bout_records
               WHERE east_id = ? OR west_id = ?
               ORDER BY basho_id, day""",
            (wrestler_id, wrestler_id),
        ).fetchall()
        conn.close()

        return [
            {
                "basho_id":   r["basho_id"],
                "day":        r["day"],
                "east_id":    r["east_id"],
                "west_id":    r["west_id"],
                "winner_id":  r["winner_id"],
                "kimarite":   r["kimarite"],
                "east_rank":  _row_get(r, "east_rank", ""),
                "west_rank":  _row_get(r, "west_rank", ""),
                "division":   _row_get(r, "division", ""),
            }
            for r in rows
        ]

    def get_wrestler_name(self, wrestler_id: str) -> str:
        """Quick lookup of shikona by wrestler_id."""
        conn = self._local_conn()
        row = conn.execute(
            "SELECT shikona FROM wrestlers WHERE wrestler_id = ?",
            (wrestler_id,),
        ).fetchone()
        conn.close()
        return row["shikona"] if row else f"#{wrestler_id}"

    def get_wrestler_info_bulk(self, wrestler_ids: list[str]) -> dict[str, dict]:
        """Bulk lookup of wrestler info for opponent display.

        Returns: {wrestler_id: {name, heya, rank, style}}
        The rank here is the current/highest rank — use get_historical_ranks
        for basho-specific ranks.
        """
        if not wrestler_ids:
            return {}
        conn = self._local_conn()
        placeholders = ",".join("?" * len(wrestler_ids))
        rows = conn.execute(
            f"""SELECT wrestler_id,
                       COALESCE(shikona_en, shikona) AS name,
                       heya,
                       COALESCE(rank_label, current_rank, highest_rank, '') AS rank,
                       fighting_style
                FROM wrestlers
                WHERE wrestler_id IN ({placeholders})""",
            wrestler_ids,
        ).fetchall()
        conn.close()
        return {
            r["wrestler_id"]: {
                "name": r["name"] or f"#{r['wrestler_id']}",
                "heya": r["heya"] or "",
                "rank": r["rank"] or "",
                "style": (r["fighting_style"] or "").title(),
            }
            for r in rows
        }

    def get_historical_ranks(
        self, basho_wrestler_pairs: list[tuple[str, str]]
    ) -> dict[tuple[str, str], str]:
        """Look up the rank each wrestler held at a specific basho.

        Args:
            basho_wrestler_pairs: list of (basho_id, wrestler_id) tuples

        Returns:
            {(basho_id, wrestler_id): "Maegashira 5 East"} or similar.
            Checks basho_entries first, falls back to legacy banzuke table.
        """
        if not basho_wrestler_pairs:
            return {}

        result: dict[tuple[str, str], str] = {}
        conn = self._local_conn()
        unique_pairs = list(set(basho_wrestler_pairs))

        for basho_id, wid in unique_pairs:
            # Try basho_entries first
            row = conn.execute(
                """SELECT rank, rank_number, side
                   FROM basho_entries
                   WHERE basho_id = ? AND wrestler_id = ?""",
                (basho_id, wid),
            ).fetchone()

            # Fall back to banzuke
            if not row or not row["rank"]:
                row = conn.execute(
                    """SELECT rank, rank_number, side
                       FROM banzuke
                       WHERE basho_id = ? AND wrestler_id = ?""",
                    (basho_id, wid),
                ).fetchone()

            if row and row["rank"]:
                rank = row["rank"].capitalize()
                num = f" {row['rank_number']}" if row["rank_number"] else ""
                side_val = _row_get(row, "side", "")
                side = f" {side_val.capitalize()}" if side_val else ""
                result[(basho_id, wid)] = f"{rank}{num}{side}"

        conn.close()
        return result

    # ── Family relations ───────────────────────────────────────────

    def get_family_relations(self, wrestler_id: str) -> list[dict]:
        """Get all family relations for a wrestler.

        Returns list of {related_id, related_name, relationship}.
        Includes both directions (e.g. if A is uncle of B,
        querying B also returns A as nephew).
        """
        conn = self._local_conn()
        rows = conn.execute(
            """SELECT fr.related_id AS related_id, fr.relationship,
                      w.shikona AS related_name
               FROM family_relations fr
               LEFT JOIN wrestlers w ON w.wrestler_id = fr.related_id
               WHERE fr.wrestler_id = ?
               UNION
               SELECT fr.wrestler_id AS related_id,
                      CASE fr.relationship
                          WHEN 'uncle' THEN 'nephew'
                          WHEN 'nephew' THEN 'uncle'
                          WHEN 'father' THEN 'son'
                          WHEN 'son' THEN 'father'
                          WHEN 'grandfather' THEN 'grandson'
                          WHEN 'grandson' THEN 'grandfather'
                          WHEN 'brother' THEN 'brother'
                          WHEN 'cousin' THEN 'cousin'
                          ELSE fr.relationship
                      END AS relationship,
                      w.shikona AS related_name
               FROM family_relations fr
               LEFT JOIN wrestlers w ON w.wrestler_id = fr.wrestler_id
               WHERE fr.related_id = ?""",
            (wrestler_id, wrestler_id),
        ).fetchall()
        conn.close()
        return [
            {
                "related_id": r["related_id"],
                "related_name": r["related_name"] or f"#{r['related_id']}",
                "relationship": r["relationship"],
            }
            for r in rows
        ]

    def add_family_relation(
        self, wrestler_id: str, related_id: str, relationship: str
    ) -> bool:
        """Add a family relation. Only stores one direction."""
        conn = self._local_conn()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO family_relations
                   (wrestler_id, related_id, relationship)
                   VALUES (?, ?, ?)""",
                (wrestler_id, related_id, relationship),
            )
            conn.commit()

            # Also push to Supabase
            if self.is_online:
                try:
                    self._rest_upsert("family_relations", {
                        "wrestler_id": wrestler_id,
                        "related_id": related_id,
                        "relationship": relationship,
                    }, on_conflict="wrestler_id,related_id")
                except Exception:
                    pass

            return True
        except Exception as e:
            logger.error(f"Failed to add family relation: {e}")
            return False
        finally:
            conn.close()

    def remove_family_relation(self, wrestler_id: str, related_id: str) -> bool:
        """Remove a family relation."""
        conn = self._local_conn()
        try:
            conn.execute(
                "DELETE FROM family_relations WHERE wrestler_id = ? AND related_id = ?",
                (wrestler_id, related_id),
            )
            conn.commit()
            return True
        except Exception:
            return False
        finally:
            conn.close()

    # ── Modifier overrides ─────────────────────────────────────────

    def get_modifier_overrides(self) -> dict[str, dict]:
        """Get all saved modifier overrides.

        Returns: {wrestler_id: {"momentum": str|None, "injury_severity": float}}
        """
        conn = self._local_conn()
        try:
            rows = conn.execute("SELECT * FROM modifier_overrides").fetchall()
        except Exception:
            return {}
        finally:
            conn.close()

        return {
            r["wrestler_id"]: {
                "momentum": r["momentum"],
                "injury_severity": r["injury_severity"] or 0.0,
            }
            for r in rows
        }

    def save_modifier_override(
        self, wrestler_id: str, momentum: str | None, injury_severity: float
    ) -> None:
        """Save a modifier override for a wrestler (local + Supabase)."""
        conn = self._local_conn()
        try:
            if momentum is None and injury_severity == 0.0:
                # No overrides — remove the row
                conn.execute(
                    "DELETE FROM modifier_overrides WHERE wrestler_id = ?",
                    (wrestler_id,),
                )
                # Also remove from Supabase
                if self.is_online:
                    try:
                        self._http.delete(
                            f"/modifier_overrides?wrestler_id=eq.{wrestler_id}"
                        )
                    except Exception:
                        pass
            else:
                conn.execute(
                    """INSERT OR REPLACE INTO modifier_overrides
                       (wrestler_id, momentum, injury_severity)
                       VALUES (?, ?, ?)""",
                    (wrestler_id, momentum, injury_severity),
                )
                # Also push to Supabase
                if self.is_online:
                    try:
                        self._rest_upsert("modifier_overrides", {
                            "wrestler_id": wrestler_id,
                            "momentum": momentum,
                            "injury_severity": injury_severity,
                        }, on_conflict="wrestler_id")
                    except Exception:
                        pass
            conn.commit()
        except Exception as e:
            logger.debug(f"Failed to save modifier override: {e}")
        finally:
            conn.close()

    # ================================================================
    # MIGRATION: Load existing Python data files into the database
    # ================================================================

    def migrate_from_haru2026(self) -> dict[str, int]:
        """
        One-time migration: load data from haru_2026.py and
        h2h_haru2026.py into the database.
        """
        counts = {}

        # 1. Wrestlers + Banzuke
        from data.haru_2026 import haru_2026_roster
        roster = haru_2026_roster()
        for w in roster:
            self.upsert_wrestler(w, basho_id="2026.03")
            # Also insert banzuke entry
            row = {
                "basho_id": "2026.03",
                "wrestler_id": w.wrestler_id,
                "rank": w.rank.value,
                "rank_number": w.rank_number,
                "side": w.side,
                "division": w.division.value,
                "is_kyujo": False,
            }
            if self._online:
                self._rest_upsert("banzuke", row, on_conflict="basho_id,wrestler_id")
            conn = self._local_conn()
            conn.execute(
                """INSERT OR REPLACE INTO banzuke
                   (basho_id, wrestler_id, rank, rank_number, side, division, is_kyujo)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (row["basho_id"], row["wrestler_id"], row["rank"],
                 row["rank_number"], row["side"], row["division"], 0),
            )
            conn.commit()
            conn.close()
        counts["wrestlers"] = len(roster)

        # 2. Tournament records
        from data.haru_2026 import haru_2026_tournament_records
        all_records = haru_2026_tournament_records()
        total_tr = 0
        for wrestler_id, records in all_records.items():
            for tr in records:
                self.upsert_tournament_record(tr)
                total_tr += 1
        counts["tournament_records"] = total_tr

        # 3. Injury notes
        from data.haru_2026 import haru_2026_injury_notes
        notes = haru_2026_injury_notes()
        for wid, info in notes.items():
            self.upsert_injury_note("2026.03", wid, info["severity"], info.get("note", ""))
        counts["injury_notes"] = len(notes)

        # 4. Bout records (from H2H scraper output, if available)
        try:
            from data.h2h_haru2026 import haru_2026_bout_records
            bouts = haru_2026_bout_records()
            n = self.upsert_bout_records(bouts)
            counts["bout_records"] = n
        except ImportError:
            logger.info("h2h_haru2026.py not found — skipping bout records")
            counts["bout_records"] = 0

        logger.info(f"Migration complete: {counts}")
        return counts
