"""
SumoSim Data Manager

Orchestration layer that coordinates between the scraper, API client,
and cache. Implements the dual-source fallback strategy:

    1. Check cache first
    2. Try Sumo Reference scraper
    3. Fall back to Sumo API
    4. Cache results for offline use

This is the single entry point the rest of the application uses
to access wrestler and bout data.
"""

from __future__ import annotations

import logging
from typing import Optional, Sequence

from data.cache import CacheManager
from data.models import (
    BoutRecord,
    FightingStyle,
    MatchupEntry,
    Rank,
    TournamentRecord,
    WrestlerProfile,
)

logger = logging.getLogger(__name__)


class DataManager:
    """
    High-level data access layer.

    Transparently handles caching and source fallback so callers never
    need to worry about where data comes from.

    Usage:
        dm = DataManager()
        roster = dm.get_roster("2025.01")
        results = dm.get_day_results("2025.01", day=1)
        h2h = dm.get_head_to_head("Y001", "O001")
    """

    def __init__(
        self,
        cache: CacheManager | None = None,
        scraper=None,
        api_client=None,
    ):
        self._cache = cache or CacheManager()
        self._scraper = scraper
        self._api_client = api_client
        self._scraper_available: bool | None = None
        self._api_available: bool | None = None

    # Lazy initialization of network clients
    def _get_scraper(self):
        if self._scraper_available is False:
            return None
        if self._scraper is None:
            try:
                from data.scraper import SumoScraper
                self._scraper = SumoScraper()
                self._scraper_available = True
            except ImportError:
                logger.warning("BeautifulSoup not available — scraper disabled")
                self._scraper_available = False
        return self._scraper

    def _get_api_client(self):
        if self._api_available is False:
            return None
        if self._api_client is None:
            try:
                from data.api_client import SumoAPIClient
                self._api_client = SumoAPIClient()
                self._api_available = True
            except ImportError:
                logger.warning("requests not available — API client disabled")
                self._api_available = False
        return self._api_client

    # ------------------------------------------------------------------
    # Roster / Banzuke
    # ------------------------------------------------------------------

    def get_roster(
        self, basho_id: str, force_refresh: bool = False
    ) -> list[WrestlerProfile]:
        """
        Get the Makuuchi roster for a specific tournament.

        Resolution order: cache → scraper → API → empty list.
        """
        # 1. Check cache
        if not force_refresh:
            cached = self._cache.load_banzuke(basho_id)
            if cached:
                logger.info(f"Loaded roster for {basho_id} from cache ({len(cached)} wrestlers)")
                return self._dicts_to_profiles(cached)

        # 2. Try scraper
        profiles = self._try_scraper_banzuke(basho_id)

        # 3. Fallback to API
        if not profiles:
            profiles = self._try_api_banzuke(basho_id)

        # 4. Cache result
        if profiles:
            self._cache.save_banzuke(basho_id, self._profiles_to_dicts(profiles))

        if not profiles:
            logger.warning(f"No roster data available for {basho_id}")

        return profiles

    # ------------------------------------------------------------------
    # Bout Results
    # ------------------------------------------------------------------

    def get_day_results(
        self,
        basho_id: str,
        day: int,
        is_active_basho: bool = False,
        force_refresh: bool = False,
    ) -> list[BoutRecord]:
        """
        Get bout results for a specific day of a tournament.

        Resolution order: cache → scraper → API → empty list.
        """
        # 1. Check cache
        if not force_refresh:
            cached = self._cache.load_day_results(basho_id, day, is_active_basho)
            if cached:
                logger.info(f"Loaded day {day} results for {basho_id} from cache")
                return self._dicts_to_bout_records(cached, basho_id, day)

        # 2. Try scraper
        records = self._try_scraper_results(basho_id, day)

        # 3. Fallback to API
        if not records:
            records = self._try_api_results(basho_id, day)

        # 4. Cache result
        if records:
            is_historical = not is_active_basho
            self._cache.save_day_results(
                basho_id, day,
                self._bout_records_to_dicts(records),
                is_historical=is_historical,
            )

        return records

    def get_all_results(
        self, basho_id: str, is_active_basho: bool = False
    ) -> dict[int, list[BoutRecord]]:
        """Get results for all 15 days of a tournament."""
        all_results = {}
        for day in range(1, 16):
            results = self.get_day_results(basho_id, day, is_active_basho)
            if results:
                all_results[day] = results
        return all_results

    # ------------------------------------------------------------------
    # Head-to-Head
    # ------------------------------------------------------------------

    def get_head_to_head(
        self, wrestler_a_id: str, wrestler_b_id: str
    ) -> list[BoutRecord]:
        """
        Get all historical bouts between two wrestlers.

        Uses cache first, then attempts to fetch from API.
        """
        # 1. Check cache
        cached = self._cache.load_head_to_head(wrestler_a_id, wrestler_b_id)
        if cached:
            return self._dicts_to_bout_records_generic(cached)

        # 2. Try API (the API has a dedicated h2h endpoint)
        records = self._try_api_head_to_head(wrestler_a_id, wrestler_b_id)

        # 3. Cache
        if records:
            self._cache.save_head_to_head(
                wrestler_a_id, wrestler_b_id,
                self._bout_records_to_dicts(records),
            )

        return records

    # ------------------------------------------------------------------
    # Tournament Records
    # ------------------------------------------------------------------

    def get_tournament_records(
        self, basho_id: str
    ) -> list[TournamentRecord]:
        """
        Get final tournament standings/records for a basho.

        Computed from day results if not directly available.
        """
        # Check cache
        cached = self._cache.load_tournament_records(basho_id)
        if cached:
            return self._dicts_to_tournament_records(cached)

        # Compute from day results
        all_results = self.get_all_results(basho_id)
        if not all_results:
            return []

        roster = self.get_roster(basho_id)
        records = self._compute_tournament_records(basho_id, roster, all_results)

        if records:
            self._cache.save_tournament_records(
                basho_id,
                [self._tournament_record_to_dict(r) for r in records],
            )

        return records

    # ------------------------------------------------------------------
    # Schedule / Torikumi
    # ------------------------------------------------------------------

    def get_torikumi(self, basho_id: str, day: int) -> list[MatchupEntry]:
        """
        Get the bout schedule (torikumi) for a specific day.

        Extracts matchup pairs from bout results (east/west pairings).
        """
        results = self.get_day_results(basho_id, day)
        return [
            MatchupEntry(east_id=r.east_id, west_id=r.west_id)
            for r in results
        ]

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def get_available_basho(self) -> list[str]:
        """List all basho with cached data."""
        return self._cache.list_cached_basho()

    def refresh_all(self, basho_id: str) -> dict:
        """Force-refresh all data for a basho. Returns summary stats."""
        roster = self.get_roster(basho_id, force_refresh=True)
        day_counts = {}
        for day in range(1, 16):
            results = self.get_day_results(basho_id, day, force_refresh=True)
            if results:
                day_counts[day] = len(results)

        return {
            "basho_id": basho_id,
            "wrestlers": len(roster),
            "days_with_results": len(day_counts),
            "total_bouts": sum(day_counts.values()),
        }

    # ------------------------------------------------------------------
    # Source fallback methods
    # ------------------------------------------------------------------

    def _try_scraper_banzuke(self, basho_id: str) -> list[WrestlerProfile]:
        scraper = self._get_scraper()
        if not scraper:
            return []
        try:
            profiles = scraper.fetch_banzuke(basho_id)
            if profiles:
                logger.info(f"Scraper: fetched {len(profiles)} wrestlers for {basho_id}")
            return profiles
        except Exception as e:
            logger.warning(f"Scraper failed for banzuke {basho_id}: {e}")
            self._scraper_available = False
            return []

    def _try_api_banzuke(self, basho_id: str) -> list[WrestlerProfile]:
        client = self._get_api_client()
        if not client:
            return []
        try:
            profiles = client.fetch_makuuchi_roster(basho_id)
            if profiles:
                logger.info(f"API: fetched {len(profiles)} wrestlers for {basho_id}")
            return profiles
        except Exception as e:
            logger.warning(f"API failed for banzuke {basho_id}: {e}")
            self._api_available = False
            return []

    def _try_scraper_results(
        self, basho_id: str, day: int
    ) -> list[BoutRecord]:
        scraper = self._get_scraper()
        if not scraper:
            return []
        try:
            records = scraper.fetch_day_results(basho_id, day)
            if records:
                logger.info(f"Scraper: fetched {len(records)} bouts for {basho_id} day {day}")
            return records
        except Exception as e:
            logger.warning(f"Scraper failed for results {basho_id} day {day}: {e}")
            return []

    def _try_api_results(
        self, basho_id: str, day: int
    ) -> list[BoutRecord]:
        client = self._get_api_client()
        if not client:
            return []
        try:
            records = client.fetch_bout_results(basho_id, day)
            if records:
                logger.info(f"API: fetched {len(records)} bouts for {basho_id} day {day}")
            return records
        except Exception as e:
            logger.warning(f"API failed for results {basho_id} day {day}: {e}")
            return []

    def _try_api_head_to_head(
        self, wrestler_a_id: str, wrestler_b_id: str
    ) -> list[BoutRecord]:
        client = self._get_api_client()
        if not client:
            return []
        try:
            # API needs numeric IDs — try extracting from our string IDs
            a_num = self._extract_api_id(wrestler_a_id)
            b_num = self._extract_api_id(wrestler_b_id)
            if a_num is not None and b_num is not None:
                return client.fetch_head_to_head(a_num, b_num)
        except Exception as e:
            logger.warning(f"API failed for h2h {wrestler_a_id} vs {wrestler_b_id}: {e}")
        return []

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _profiles_to_dicts(profiles: list[WrestlerProfile]) -> list[dict]:
        return [
            {
                "wrestler_id": p.wrestler_id,
                "shikona": p.shikona,
                "rank": p.rank.value,
                "rank_number": p.rank_number,
                "side": p.side,
                "heya": p.heya,
                "height_cm": p.height_cm,
                "weight_kg": p.weight_kg,
                "birth_date": p.birth_date.isoformat() if p.birth_date else None,
                "fighting_style": p.fighting_style.value,
            }
            for p in profiles
        ]

    @staticmethod
    def _dicts_to_profiles(dicts: list[dict]) -> list[WrestlerProfile]:
        profiles = []
        for d in dicts:
            try:
                birth_date = None
                if d.get("birth_date"):
                    from datetime import date
                    birth_date = date.fromisoformat(d["birth_date"])
                profiles.append(WrestlerProfile(
                    wrestler_id=d["wrestler_id"],
                    shikona=d["shikona"],
                    rank=Rank(d["rank"]),
                    rank_number=d.get("rank_number"),
                    side=d.get("side"),
                    heya=d.get("heya", "Unknown"),
                    height_cm=d.get("height_cm"),
                    weight_kg=d.get("weight_kg"),
                    birth_date=birth_date,
                    fighting_style=FightingStyle(d.get("fighting_style", "hybrid")),
                ))
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"Failed to deserialize wrestler: {e}")
        return profiles

    @staticmethod
    def _bout_records_to_dicts(records: list[BoutRecord]) -> list[dict]:
        return [
            {
                "basho_id": r.basho_id,
                "day": r.day,
                "east_id": r.east_id,
                "west_id": r.west_id,
                "winner_id": r.winner_id,
                "kimarite": r.kimarite,
            }
            for r in records
        ]

    @staticmethod
    def _dicts_to_bout_records(
        dicts: list[dict], basho_id: str, day: int
    ) -> list[BoutRecord]:
        records = []
        for d in dicts:
            try:
                records.append(BoutRecord(
                    basho_id=d.get("basho_id", basho_id),
                    day=d.get("day", day),
                    east_id=d["east_id"],
                    west_id=d["west_id"],
                    winner_id=d["winner_id"],
                    kimarite=d.get("kimarite"),
                ))
            except (ValueError, KeyError):
                continue
        return records

    @staticmethod
    def _dicts_to_bout_records_generic(dicts: list[dict]) -> list[BoutRecord]:
        records = []
        for d in dicts:
            try:
                records.append(BoutRecord(
                    basho_id=d["basho_id"],
                    day=d["day"],
                    east_id=d["east_id"],
                    west_id=d["west_id"],
                    winner_id=d["winner_id"],
                    kimarite=d.get("kimarite"),
                ))
            except (ValueError, KeyError):
                continue
        return records

    @staticmethod
    def _tournament_record_to_dict(rec: TournamentRecord) -> dict:
        return {
            "basho_id": rec.basho_id,
            "wrestler_id": rec.wrestler_id,
            "rank": rec.rank.value,
            "rank_number": rec.rank_number,
            "wins": rec.wins,
            "losses": rec.losses,
            "absences": rec.absences,
            "special_prizes": list(rec.special_prizes),
            "is_yusho": rec.is_yusho,
        }

    @staticmethod
    def _dicts_to_tournament_records(dicts: list[dict]) -> list[TournamentRecord]:
        records = []
        for d in dicts:
            try:
                records.append(TournamentRecord(
                    basho_id=d["basho_id"],
                    wrestler_id=d["wrestler_id"],
                    rank=Rank(d["rank"]),
                    rank_number=d.get("rank_number"),
                    wins=d["wins"],
                    losses=d["losses"],
                    absences=d.get("absences", 0),
                    special_prizes=tuple(d.get("special_prizes", [])),
                    is_yusho=d.get("is_yusho", False),
                ))
            except (ValueError, KeyError):
                continue
        return records

    def _compute_tournament_records(
        self,
        basho_id: str,
        roster: list[WrestlerProfile],
        all_results: dict[int, list[BoutRecord]],
    ) -> list[TournamentRecord]:
        """Compute tournament records from day-by-day results."""
        wins: dict[str, int] = {}
        losses: dict[str, int] = {}

        for day_results in all_results.values():
            for bout in day_results:
                wins[bout.winner_id] = wins.get(bout.winner_id, 0) + 1
                losses[bout.loser_id] = losses.get(bout.loser_id, 0) + 1

        records = []
        for w in roster:
            w_wins = wins.get(w.wrestler_id, 0)
            w_losses = losses.get(w.wrestler_id, 0)
            total = w_wins + w_losses
            absences = max(0, 15 - total) if total < 15 else 0

            records.append(TournamentRecord(
                basho_id=basho_id,
                wrestler_id=w.wrestler_id,
                rank=w.rank,
                rank_number=w.rank_number,
                wins=w_wins,
                losses=w_losses,
                absences=absences,
            ))

        # Determine yusho winner
        if records:
            records.sort(key=lambda r: (-r.wins, r.losses))
            top = records[0]
            # Mark yusho
            records[0] = TournamentRecord(
                basho_id=top.basho_id,
                wrestler_id=top.wrestler_id,
                rank=top.rank,
                rank_number=top.rank_number,
                wins=top.wins,
                losses=top.losses,
                absences=top.absences,
                is_yusho=True,
            )

        return records

    @staticmethod
    def _extract_api_id(wrestler_id: str) -> Optional[int]:
        """Extract numeric API ID from wrestler_id if possible."""
        # If it's already numeric
        try:
            return int(wrestler_id)
        except ValueError:
            pass
        # If it's prefixed like "api_123"
        if wrestler_id.startswith("api_") or wrestler_id.startswith("sr_"):
            return None  # Can't convert shikona-based IDs to numeric
        return None
