"""
SumoSim Data Layer Tests

Tests for the cache manager, data manager, scraper parsing,
and API client parsing. Uses sample data for offline testing.
"""

from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data.cache import CacheManager, CacheTTL
from data.data_manager import DataManager
from data.models import (
    BoutRecord, FightingStyle, Rank, TournamentRecord, WrestlerProfile,
)
from data.sample_data import (
    sample_bout_records, sample_roster, sample_tournament_records,
)
from utils.config import SimulationConfig, set_config


def _deterministic_config():
    cfg = SimulationConfig(random_seed=42, bout_iterations=100, tournament_iterations=5)
    set_config(cfg)
    return cfg


# ===========================================================================
# CACHE TESTS
# ===========================================================================

class TestCacheManager(unittest.TestCase):

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.cache = CacheManager(cache_dir=Path(self._tmpdir))

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_save_and_load_wrestlers(self):
        data = [{"wrestler_id": "X", "shikona": "TestWrestler", "rank": "yokozuna"}]
        self.cache.save_wrestlers(data)
        loaded = self.cache.load_wrestlers()
        self.assertIsNotNone(loaded)
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["shikona"], "TestWrestler")

    def test_stale_wrestler_cache_returns_none(self):
        data = [{"wrestler_id": "X"}]
        self.cache.save_wrestlers(data)

        # Manually backdate the metadata
        meta_path = Path(self._tmpdir) / "wrestlers_meta.json"
        old_time = time.time() - (200 * 3600)  # 200 hours ago
        with open(meta_path, "w") as f:
            json.dump({"timestamp": old_time}, f)

        loaded = self.cache.load_wrestlers()
        self.assertIsNone(loaded)

    def test_save_and_load_banzuke(self):
        data = [{"shikona": "Wrestler1"}, {"shikona": "Wrestler2"}]
        self.cache.save_banzuke("2025.01", data)
        loaded = self.cache.load_banzuke("2025.01")
        self.assertIsNotNone(loaded)
        self.assertEqual(len(loaded), 2)

    def test_basho_directory_structure(self):
        self.cache.save_banzuke("2025.01", [{"test": True}])
        basho_dir = Path(self._tmpdir) / "202501"
        self.assertTrue(basho_dir.is_dir())
        self.assertTrue((basho_dir / "banzuke.json").exists())

    def test_save_and_load_day_results(self):
        bouts = [
            {"east_id": "A", "west_id": "B", "winner_id": "A", "kimarite": "yorikiri"}
        ]
        self.cache.save_day_results("2025.01", 1, bouts, is_historical=True)
        loaded = self.cache.load_day_results("2025.01", 1)
        self.assertIsNotNone(loaded)
        self.assertEqual(len(loaded), 1)

    def test_historical_results_never_expire(self):
        bouts = [{"east_id": "A", "west_id": "B", "winner_id": "A"}]
        self.cache.save_day_results("2020.01", 1, bouts, is_historical=True)

        # Backdate metadata
        basho_dir = Path(self._tmpdir) / "202001"
        meta_path = basho_dir / "results_day01_meta.json"
        with open(meta_path, "w") as f:
            json.dump({"timestamp": 0, "is_historical": True}, f)

        loaded = self.cache.load_day_results("2020.01", 1, is_active_basho=False)
        self.assertIsNotNone(loaded)

    def test_save_and_load_head_to_head(self):
        bouts = [
            {"basho_id": "2025.01", "day": 3, "east_id": "A", "west_id": "B",
             "winner_id": "A", "kimarite": "yorikiri"}
        ]
        self.cache.save_head_to_head("A", "B", bouts)
        loaded = self.cache.load_head_to_head("A", "B")
        self.assertIsNotNone(loaded)
        self.assertEqual(len(loaded), 1)

    def test_h2h_cache_symmetric(self):
        bouts = [{"test": True}]
        self.cache.save_head_to_head("A", "B", bouts)
        # Should be loadable with reversed order
        loaded = self.cache.load_head_to_head("B", "A")
        self.assertIsNotNone(loaded)

    def test_list_cached_basho(self):
        self.cache.save_banzuke("2025.01", [])
        self.cache.save_banzuke("2024.11", [])
        self.cache.save_banzuke("2024.09", [])

        basho_list = self.cache.list_cached_basho()
        self.assertEqual(len(basho_list), 3)
        self.assertIn("2025.01", basho_list)
        self.assertIn("2024.11", basho_list)

    def test_clear_basho(self):
        self.cache.save_banzuke("2025.01", [{"test": True}])
        self.cache.clear_basho("2025.01")
        loaded = self.cache.load_banzuke("2025.01")
        self.assertIsNone(loaded)

    def test_clear_all(self):
        self.cache.save_banzuke("2025.01", [])
        self.cache.save_wrestlers([])
        self.cache.clear_all()
        self.assertEqual(self.cache.list_cached_basho(), [])
        self.assertIsNone(self.cache.load_wrestlers())

    def test_save_and_load_tournament_records(self):
        recs = [{"basho_id": "2025.01", "wrestler_id": "X", "rank": "ozeki",
                 "wins": 10, "losses": 5}]
        self.cache.save_tournament_records("2025.01", recs)
        loaded = self.cache.load_tournament_records("2025.01")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded[0]["wins"], 10)

    def test_corrupt_json_returns_none(self):
        path = Path(self._tmpdir) / "corrupt.json"
        with open(path, "w") as f:
            f.write("{invalid json!!!")
        result = self.cache._read_json(path)
        self.assertIsNone(result)

    def test_cache_age(self):
        self.cache.save_banzuke("2025.01", [])
        age = self.cache.get_cache_age_hours("2025.01", "banzuke")
        self.assertIsNotNone(age)
        self.assertLess(age, 0.01)  # Just created, should be near-zero


# ===========================================================================
# SAMPLE DATA TESTS
# ===========================================================================

class TestSampleData(unittest.TestCase):

    def test_sample_roster_size(self):
        roster = sample_roster()
        self.assertEqual(len(roster), 20)

    def test_sample_roster_ranks(self):
        roster = sample_roster()
        yokozuna = [w for w in roster if w.rank == Rank.YOKOZUNA]
        ozeki = [w for w in roster if w.rank == Rank.OZEKI]
        self.assertEqual(len(yokozuna), 1)
        self.assertGreaterEqual(len(ozeki), 2)

    def test_sample_roster_unique_ids(self):
        roster = sample_roster()
        ids = [w.wrestler_id for w in roster]
        self.assertEqual(len(ids), len(set(ids)))

    def test_sample_roster_unique_heya(self):
        roster = sample_roster()
        # At minimum, should have several different heya
        heyas = {w.heya for w in roster}
        self.assertGreater(len(heyas), 5)

    def test_sample_bout_records(self):
        bouts = sample_bout_records()
        self.assertEqual(len(bouts), 30)  # 10 bouts × 3 days

    def test_sample_bout_days(self):
        bouts = sample_bout_records()
        days = {b.day for b in bouts}
        self.assertEqual(days, {1, 2, 3})

    def test_sample_bout_winners_are_participants(self):
        for bout in sample_bout_records():
            self.assertIn(bout.winner_id, (bout.east_id, bout.west_id))

    def test_sample_tournament_records(self):
        records = sample_tournament_records()
        self.assertGreater(len(records), 3)
        for wid, recs in records.items():
            self.assertEqual(len(recs), 3)  # 3 basho each
            for rec in recs:
                self.assertLessEqual(rec.wins + rec.losses, 15)


# ===========================================================================
# DATA MANAGER TESTS (with mock/cache only, no network)
# ===========================================================================

class TestDataManager(unittest.TestCase):

    def setUp(self):
        _deterministic_config()
        self._tmpdir = tempfile.mkdtemp()
        self.cache = CacheManager(cache_dir=Path(self._tmpdir))
        # DataManager with no scraper/API — pure cache mode
        # Set flags to prevent lazy initialization of network clients
        self.dm = DataManager(cache=self.cache, scraper=None, api_client=None)
        self.dm._scraper_available = False
        self.dm._api_available = False

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_get_roster_from_cache(self):
        roster = sample_roster()
        self.cache.save_banzuke(
            "2025.01", DataManager._profiles_to_dicts(roster)
        )
        loaded = self.dm.get_roster("2025.01")
        self.assertEqual(len(loaded), 20)
        self.assertEqual(loaded[0].shikona, "Onosato")

    def test_get_roster_empty_without_sources(self):
        # No cache, no scraper, no API → empty list
        loaded = self.dm.get_roster("2099.01")
        self.assertEqual(loaded, [])

    def test_get_day_results_from_cache(self):
        bouts = sample_bout_records()
        day1 = [b for b in bouts if b.day == 1]
        self.cache.save_day_results(
            "2025.01", 1,
            DataManager._bout_records_to_dicts(day1),
            is_historical=True,
        )
        loaded = self.dm.get_day_results("2025.01", 1)
        self.assertEqual(len(loaded), 10)

    def test_get_all_results(self):
        bouts = sample_bout_records()
        for day in [1, 2, 3]:
            day_bouts = [b for b in bouts if b.day == day]
            self.cache.save_day_results(
                "2025.01", day,
                DataManager._bout_records_to_dicts(day_bouts),
                is_historical=True,
            )
        all_results = self.dm.get_all_results("2025.01")
        self.assertEqual(len(all_results), 3)
        self.assertEqual(len(all_results[1]), 10)

    def test_get_head_to_head_from_cache(self):
        bouts = [b for b in sample_bout_records()
                 if {b.east_id, b.west_id} == {"sr_onosato", "sr_hoshoryu"}]
        self.cache.save_head_to_head(
            "sr_onosato", "sr_hoshoryu",
            DataManager._bout_records_to_dicts(bouts),
        )
        loaded = self.dm.get_head_to_head("sr_onosato", "sr_hoshoryu")
        self.assertGreater(len(loaded), 0)

    def test_get_torikumi(self):
        bouts = sample_bout_records()
        day1 = [b for b in bouts if b.day == 1]
        self.cache.save_day_results(
            "2025.01", 1,
            DataManager._bout_records_to_dicts(day1),
            is_historical=True,
        )
        matchups = self.dm.get_torikumi("2025.01", 1)
        self.assertEqual(len(matchups), 10)
        self.assertEqual(matchups[0].east_id, "sr_onosato")

    def test_compute_tournament_records(self):
        roster = sample_roster()
        self.cache.save_banzuke("2025.01", DataManager._profiles_to_dicts(roster))

        bouts = sample_bout_records()
        for day in [1, 2, 3]:
            day_bouts = [b for b in bouts if b.day == day]
            self.cache.save_day_results(
                "2025.01", day,
                DataManager._bout_records_to_dicts(day_bouts),
                is_historical=True,
            )

        records = self.dm.get_tournament_records("2025.01")
        self.assertGreater(len(records), 0)

        # Check that wins/losses are consistent
        total_wins = sum(r.wins for r in records)
        total_losses = sum(r.losses for r in records)
        self.assertEqual(total_wins, total_losses)

    def test_serialization_roundtrip_profiles(self):
        roster = sample_roster()
        dicts = DataManager._profiles_to_dicts(roster)
        restored = DataManager._dicts_to_profiles(dicts)
        self.assertEqual(len(restored), len(roster))
        for orig, rest in zip(roster, restored):
            self.assertEqual(orig.wrestler_id, rest.wrestler_id)
            self.assertEqual(orig.shikona, rest.shikona)
            self.assertEqual(orig.rank, rest.rank)

    def test_serialization_roundtrip_bouts(self):
        bouts = sample_bout_records()[:5]
        dicts = DataManager._bout_records_to_dicts(bouts)
        restored = DataManager._dicts_to_bout_records(dicts, "2025.01", 1)
        self.assertEqual(len(restored), 5)
        for orig, rest in zip(bouts, restored):
            self.assertEqual(orig.winner_id, rest.winner_id)

    def test_available_basho(self):
        self.cache.save_banzuke("2025.01", [])
        self.cache.save_banzuke("2024.11", [])
        available = self.dm.get_available_basho()
        self.assertEqual(len(available), 2)


# ===========================================================================
# SCRAPER PARSING TESTS (offline — test parsers with fake HTML)
# ===========================================================================

class TestScraperParsing(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_parse_banzuke_line_regex(self):
        """Test regex parsing of banzuke text lines."""
        from data.scraper import _BANZUKE_LINE_RE
        line = "Y1e    Terunofuji     Mongolia  Isegahama   29.11.1991    192   174"
        match = _BANZUKE_LINE_RE.match(line)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "Y")
        self.assertEqual(match.group(2), "1")
        self.assertEqual(match.group(3), "e")
        self.assertEqual(match.group(4), "Terunofuji")
        # Group 5 is birthplace+heya combined
        self.assertIn("Isegahama", match.group(5))
        self.assertEqual(match.group(7), "192")
        self.assertEqual(match.group(8), "174")

    def test_parse_banzuke_line_maegashira(self):
        from data.scraper import _BANZUKE_LINE_RE
        # Normal case with space between birthplace and heya
        line = "M10e   Kinbozan       Kazakhstan Kise        24.06.1997    192   181"
        match = _BANZUKE_LINE_RE.match(line)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "M")
        self.assertEqual(match.group(2), "10")

    def test_parse_banzuke_line_merged_fields(self):
        """Handle real-world case where birthplace and heya merge."""
        from data.scraper import _BANZUKE_LINE_RE
        line = "M10e   Kinbozan       KazakhstanKise        24.06.1997    192   181"
        match = _BANZUKE_LINE_RE.match(line)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(4), "Kinbozan")

    def test_parse_banzuke_page(self):
        """Test full banzuke page parsing with mock HTML."""
        from data.scraper import SumoScraper
        scraper = SumoScraper.__new__(SumoScraper)  # skip __init__

        fake_html = """
        <html><body><pre>
        Hatsu 2025
        Tokyo, Ryogoku Kokugikan

        Makuuchi

        Y1e    Terunofuji     Mongolia  Isegahama   29.11.1991    192   174
        O1e    Takakeisho     Hyogo     Tokiwayama  05.08.1996    175   165
        M1e    Nishikigi      Iwate     Isenoumi    25.08.1990    185   180
        M1w    Tobizaru       Tokyo     Oitekaze    24.04.1992    174   135

        Juryo

        J1e    Kagayaki       Ishikawa  Takadagawa  01.06.1994    193   156
        </pre></body></html>
        """
        profiles = scraper._parse_banzuke_text(fake_html)
        self.assertEqual(len(profiles), 4)  # Only Makuuchi
        self.assertEqual(profiles[0].shikona, "Terunofuji")
        self.assertEqual(profiles[0].rank, Rank.YOKOZUNA)
        self.assertEqual(profiles[0].side, "east")
        self.assertAlmostEqual(profiles[0].height_cm, 192.0)
        self.assertEqual(profiles[1].rank, Rank.OZEKI)
        self.assertEqual(profiles[2].rank, Rank.MAEGASHIRA)
        self.assertEqual(profiles[2].rank_number, 1)
        self.assertEqual(profiles[3].side, "west")

    def test_parse_results_page(self):
        """Test results page parsing with mock HTML."""
        from data.scraper import SumoScraper
        scraper = SumoScraper.__new__(SumoScraper)

        fake_html = """
        <html><body><pre>
        Hatsu 2025, Day 1

        Juryo

        SomeJuryo1 yorikiri SomeJuryo2

        Makuuchi

        Onosato oshidashi Hoshoryu
        Abi uwatenage Kotozakura
        Kirishima yorikiri Daieisho
        </pre></body></html>
        """
        records = scraper._parse_results_text(fake_html, "2025.01", 1)
        self.assertEqual(len(records), 3)  # Only Makuuchi
        self.assertEqual(records[0].winner_id, "sr_onosato")
        self.assertEqual(records[0].kimarite, "oshidashi")
        self.assertEqual(records[0].loser_id, "sr_hoshoryu")
        self.assertEqual(records[1].kimarite, "uwatenage")

    def test_parse_date_dmy(self):
        from data.scraper import SumoScraper
        d = SumoScraper._parse_date_dmy("29.11.1991")
        self.assertIsNotNone(d)
        self.assertEqual(d.year, 1991)
        self.assertEqual(d.month, 11)
        self.assertEqual(d.day, 29)

    def test_parse_date_dmy_invalid(self):
        from data.scraper import SumoScraper
        d = SumoScraper._parse_date_dmy("not-a-date")
        self.assertIsNone(d)


# ===========================================================================
# API CLIENT PARSING TESTS (offline — test parsers with fake JSON)
# ===========================================================================

class TestAPIClientParsing(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_parse_banzuke_entry(self):
        from data.api_client import SumoAPIClient
        client = SumoAPIClient.__new__(SumoAPIClient)

        entry = {
            "rikishiID": 45,
            "shikona": "Terunofuji",
            "rank": "Yokozuna",
            "rankNumber": 1,
            "side": "East",
            "heya": "Isegahama",
            "height": 192,
            "weight": 174,
            "birthDate": "1991-11-29",
        }
        profile = client._parse_banzuke_entry(entry)
        self.assertIsNotNone(profile)
        self.assertEqual(profile.wrestler_id, "45")
        self.assertEqual(profile.rank, Rank.YOKOZUNA)
        self.assertAlmostEqual(profile.height_cm, 192.0)

    def test_parse_torikumi_entry(self):
        from data.api_client import SumoAPIClient
        client = SumoAPIClient.__new__(SumoAPIClient)

        entry = {
            "eastID": 100,
            "westID": 200,
            "winnerID": 100,
            "kimarite": "yorikiri",
        }
        record = client._parse_torikumi_entry(entry, "2025.01", 1)
        self.assertIsNotNone(record)
        self.assertEqual(record.east_id, "100")
        self.assertEqual(record.winner_id, "100")
        self.assertEqual(record.kimarite, "yorikiri")

    def test_parse_torikumi_entry_invalid_winner(self):
        from data.api_client import SumoAPIClient
        client = SumoAPIClient.__new__(SumoAPIClient)

        entry = {
            "eastID": 100,
            "westID": 200,
            "winnerID": 999,  # not a participant
            "kimarite": "yorikiri",
        }
        record = client._parse_torikumi_entry(entry, "2025.01", 1)
        self.assertIsNone(record)

    def test_parse_match_entry(self):
        from data.api_client import SumoAPIClient
        client = SumoAPIClient.__new__(SumoAPIClient)

        entry = {
            "bashoId": "202501",
            "day": 3,
            "eastID": 45,
            "westID": 67,
            "winnerID": 45,
            "kimarite": "uwatenage",
        }
        record = client._parse_match_entry(entry)
        self.assertIsNotNone(record)
        self.assertEqual(record.basho_id, "2025.01")
        self.assertEqual(record.day, 3)

    def test_parse_rank(self):
        from data.api_client import SumoAPIClient
        self.assertEqual(SumoAPIClient._parse_rank("Yokozuna"), Rank.YOKOZUNA)
        self.assertEqual(SumoAPIClient._parse_rank("ozeki"), Rank.OZEKI)
        self.assertEqual(SumoAPIClient._parse_rank("maegashira"), Rank.MAEGASHIRA)
        self.assertIsNone(SumoAPIClient._parse_rank("juryo"))
        self.assertIsNone(SumoAPIClient._parse_rank("makushita"))


# ===========================================================================
# INTEGRATION: DATA LAYER → ENGINE
# ===========================================================================

class TestDataToEngine(unittest.TestCase):
    """Verify sample data flows correctly into the simulation engine."""

    def setUp(self):
        _deterministic_config()

    def test_sample_roster_feeds_tournament(self):
        from engine.tournament_simulator import TournamentSimulator
        roster = sample_roster()
        sim = TournamentSimulator(roster=roster)
        result = sim.simulate_tournament("sample.01")
        self.assertEqual(result.total_days_simulated, 15)
        self.assertIsNotNone(result.yusho_winner_id)

    def test_sample_records_feed_ratings(self):
        from engine.probability import compute_base_rating
        roster = sample_roster()
        records_map = sample_tournament_records()

        onosato = next(w for w in roster if w.wrestler_id == "sr_onosato")
        onosato_recs = records_map.get("sr_onosato", [])
        rating = compute_base_rating(onosato, onosato_recs)
        # Yokozuna with 14-1, 13-2, 13-2 should be well above base
        self.assertGreater(rating, 1850.0)

    def test_sample_bouts_feed_h2h(self):
        from engine.probability import build_head_to_head
        bouts = sample_bout_records()
        h2h = build_head_to_head("sr_onosato", "sr_hoshoryu", bouts)
        # Day 3 has Onosato vs Hoshoryu, Onosato wins
        self.assertEqual(h2h.total, 1)
        self.assertEqual(h2h.a_wins, 1)


if __name__ == "__main__":
    unittest.main()
