"""
SumoSim Sumo API Client

REST client for sumo-api.com — the fallback data source.
Provides structured JSON data for wrestlers, banzuke, torikumi, and matches.

Endpoints used:
    GET /api/rikishis                              — All wrestlers
    GET /api/rikishi/:id                           — Wrestler details
    GET /api/rikishi/:id/matches                   — Wrestler match history
    GET /api/rikishi/:id/matches/:opponentId       — Head-to-head bouts
    GET /api/basho/:bashoId                        — Tournament info
    GET /api/basho/:bashoId/banzuke/:division      — Banzuke
    GET /api/basho/:bashoId/torikumi/:division/:day — Day results
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import requests

from data.models import (
    BoutRecord,
    FightingStyle,
    MatchupEntry,
    Rank,
    TournamentRecord,
    WrestlerProfile,
)
from utils.config import get_config

logger = logging.getLogger(__name__)

BASE_URL = "https://sumo-api.com/api"

# Map sumo-api rank strings to our Rank enum
_RANK_MAP = {
    "yokozuna": Rank.YOKOZUNA,
    "ozeki": Rank.OZEKI,
    "sekiwake": Rank.SEKIWAKE,
    "komusubi": Rank.KOMUSUBI,
    "maegashira": Rank.MAEGASHIRA,
}


class SumoAPIClient:
    """
    Client for the sumo-api.com REST API.

    Handles rate limiting, error handling, and conversion of JSON
    responses into SumoSim data models.

    Usage:
        client = SumoAPIClient()
        roster = client.fetch_makuuchi_banzuke("202501")
        results = client.fetch_day_results("202501", day=1)
    """

    def __init__(self, delay_ms: int | None = None):
        cfg = get_config()
        self._delay_s = (delay_ms or cfg.scrape_delay_ms) / 1000.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "SumoSim/1.0 (sumo tournament simulator)",
            "Accept": "application/json",
        })
        self._last_request_time = 0.0

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def fetch_all_rikishi(self) -> list[dict]:
        """Fetch all active wrestlers (raw JSON)."""
        data = self._get("/rikishis")
        if data and isinstance(data, dict) and "records" in data:
            return data["records"]
        if isinstance(data, list):
            return data
        return []

    def fetch_rikishi(self, rikishi_id: int) -> Optional[dict]:
        """Fetch a single wrestler's details."""
        return self._get(f"/rikishi/{rikishi_id}")

    def fetch_rikishi_matches(
        self, rikishi_id: int, opponent_id: int | None = None
    ) -> list[dict]:
        """Fetch match history for a wrestler, optionally filtered by opponent."""
        if opponent_id:
            data = self._get(f"/rikishi/{rikishi_id}/matches/{opponent_id}")
        else:
            data = self._get(f"/rikishi/{rikishi_id}/matches")
        if data and isinstance(data, dict) and "records" in data:
            return data["records"]
        if isinstance(data, list):
            return data
        return []

    def fetch_basho_info(self, basho_id: str) -> Optional[dict]:
        """Fetch tournament metadata (date, location, etc.)."""
        api_basho = basho_id.replace(".", "")
        return self._get(f"/basho/{api_basho}")

    def fetch_makuuchi_banzuke(self, basho_id: str) -> list[dict]:
        """Fetch the Makuuchi banzuke for a tournament (raw JSON)."""
        api_basho = basho_id.replace(".", "")
        data = self._get(f"/basho/{api_basho}/banzuke/Makuuchi")
        if data and isinstance(data, dict) and "records" in data:
            return data["records"]
        if isinstance(data, list):
            return data
        return []

    def fetch_day_results(self, basho_id: str, day: int) -> list[dict]:
        """Fetch torikumi (bout results) for a specific day (raw JSON)."""
        api_basho = basho_id.replace(".", "")
        data = self._get(f"/basho/{api_basho}/torikumi/Makuuchi/{day}")
        if data and isinstance(data, dict) and "records" in data:
            return data["records"]
        if isinstance(data, list):
            return data
        return []

    # ------------------------------------------------------------------
    # Model conversion methods
    # ------------------------------------------------------------------

    def fetch_makuuchi_roster(self, basho_id: str) -> list[WrestlerProfile]:
        """Fetch and convert banzuke to WrestlerProfile models."""
        raw = self.fetch_makuuchi_banzuke(basho_id)
        profiles = []
        for entry in raw:
            try:
                profile = self._parse_banzuke_entry(entry)
                if profile:
                    profiles.append(profile)
            except Exception as e:
                logger.warning(f"Failed to parse banzuke entry: {e}")
                continue
        logger.info(f"Fetched {len(profiles)} wrestlers from API for {basho_id}")
        return profiles

    def fetch_bout_results(self, basho_id: str, day: int) -> list[BoutRecord]:
        """Fetch and convert day results to BoutRecord models."""
        raw = self.fetch_day_results(basho_id, day)
        records = []
        for entry in raw:
            try:
                record = self._parse_torikumi_entry(entry, basho_id, day)
                if record:
                    records.append(record)
            except Exception as e:
                logger.warning(f"Failed to parse bout entry: {e}")
                continue
        return records

    def fetch_head_to_head(
        self, rikishi_id: int, opponent_id: int
    ) -> list[BoutRecord]:
        """Fetch and convert head-to-head match history."""
        raw = self.fetch_rikishi_matches(rikishi_id, opponent_id)
        records = []
        for entry in raw:
            try:
                record = self._parse_match_entry(entry)
                if record:
                    records.append(record)
            except Exception as e:
                logger.warning(f"Failed to parse match entry: {e}")
                continue
        return records

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    def _parse_banzuke_entry(self, entry: dict) -> Optional[WrestlerProfile]:
        """Convert a sumo-api banzuke entry to WrestlerProfile."""
        # The API returns nested rikishi data within banzuke entries
        rikishi = entry.get("rikishiID") or entry.get("rikishi", {})
        if isinstance(rikishi, int):
            # Just an ID, need the surrounding entry data
            wrestler_id = str(rikishi)
            shikona = entry.get("shikona", entry.get("shikonaEn", "Unknown"))
        elif isinstance(rikishi, dict):
            wrestler_id = str(rikishi.get("id", entry.get("rikishiID", "")))
            shikona = rikishi.get("shikonaEn", rikishi.get("shikona", "Unknown"))
        else:
            wrestler_id = str(entry.get("rikishiID", ""))
            shikona = entry.get("shikonaEn", entry.get("shikona", "Unknown"))

        if not wrestler_id:
            return None

        # Parse rank
        rank_str = entry.get("rank", entry.get("rankValue", "")).lower()
        rank_number = entry.get("rankNumber")
        side = entry.get("side", "").lower() or None

        rank = self._parse_rank(rank_str)
        if rank is None:
            return None  # Skip non-makuuchi ranks

        # Physical attributes — may be nested or top-level
        height = entry.get("height") or (rikishi.get("height") if isinstance(rikishi, dict) else None)
        weight = entry.get("weight") or (rikishi.get("weight") if isinstance(rikishi, dict) else None)
        heya = entry.get("heya", entry.get("heyaName", "Unknown"))
        if isinstance(rikishi, dict):
            heya = rikishi.get("heya", heya)

        birth_date = None
        bd_str = entry.get("birthDate") or (rikishi.get("birthDate") if isinstance(rikishi, dict) else None)
        if bd_str:
            birth_date = self._parse_date(bd_str)

        return WrestlerProfile(
            wrestler_id=wrestler_id,
            shikona=shikona,
            rank=rank,
            rank_number=rank_number,
            side=side if side in ("east", "west") else None,
            heya=heya if isinstance(heya, str) else "Unknown",
            height_cm=float(height) if height else None,
            weight_kg=float(weight) if weight else None,
            birth_date=birth_date,
            fighting_style=FightingStyle.HYBRID,  # classified later by kimarite analysis
        )

    def _parse_torikumi_entry(
        self, entry: dict, basho_id: str, day: int
    ) -> Optional[BoutRecord]:
        """Convert a sumo-api torikumi entry to BoutRecord."""
        east_id = str(entry.get("eastID", entry.get("eastId", "")))
        west_id = str(entry.get("westID", entry.get("westId", "")))
        winner_id = str(entry.get("winnerID", entry.get("winnerId", "")))
        kimarite = entry.get("kimarite", None)

        if not east_id or not west_id or not winner_id:
            return None
        if winner_id not in (east_id, west_id):
            return None

        return BoutRecord(
            basho_id=basho_id,
            day=day,
            east_id=east_id,
            west_id=west_id,
            winner_id=winner_id,
            kimarite=kimarite,
        )

    def _parse_match_entry(self, entry: dict) -> Optional[BoutRecord]:
        """Convert a sumo-api match history entry to BoutRecord."""
        basho_id_raw = str(entry.get("bashoId", ""))
        if len(basho_id_raw) == 6:
            basho_id = f"{basho_id_raw[:4]}.{basho_id_raw[4:]}"
        else:
            basho_id = basho_id_raw

        day = entry.get("day", 1)
        east_id = str(entry.get("eastID", entry.get("eastId", "")))
        west_id = str(entry.get("westID", entry.get("westId", "")))
        winner_id = str(entry.get("winnerID", entry.get("winnerId", "")))
        kimarite = entry.get("kimarite")

        if not east_id or not west_id or not winner_id:
            return None
        if winner_id not in (east_id, west_id):
            return None

        try:
            return BoutRecord(
                basho_id=basho_id,
                day=int(day),
                east_id=east_id,
                west_id=west_id,
                winner_id=winner_id,
                kimarite=kimarite,
            )
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get(self, endpoint: str) -> Optional[Any]:
        """Make a rate-limited GET request."""
        self._rate_limit()
        url = f"{BASE_URL}{endpoint}"
        try:
            logger.debug(f"API GET: {url}")
            resp = self._session.get(url, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            logger.warning(f"API timeout: {url}")
            return None
        except requests.exceptions.HTTPError as e:
            logger.warning(f"API HTTP error {e.response.status_code}: {url}")
            if e.response.status_code == 429:
                # Rate limited — back off
                time.sleep(self._delay_s * 3)
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"API request failed: {url} — {e}")
            return None

    def _rate_limit(self) -> None:
        """Enforce minimum delay between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._delay_s:
            time.sleep(self._delay_s - elapsed)
        self._last_request_time = time.time()

    @staticmethod
    def _parse_rank(rank_str: str) -> Optional[Rank]:
        """Parse a rank string into Rank enum, or None if not makuuchi."""
        rank_str = rank_str.lower().strip()
        for key, rank in _RANK_MAP.items():
            if rank_str.startswith(key):
                return rank
        return None

    @staticmethod
    def _parse_date(date_str: str) -> Optional["date"]:
        """Parse a date string in various formats."""
        from datetime import date as Date
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return Date.fromisoformat(date_str.split("T")[0])
            except (ValueError, AttributeError):
                continue
        return None
