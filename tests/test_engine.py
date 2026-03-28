"""
SumoSim Test Suite — unittest-based

Comprehensive tests for data models, modifiers, probability engine,
bout simulator, and tournament simulator.
"""

from __future__ import annotations

import math
import sys
import unittest
from datetime import date
from pathlib import Path

# Ensure sumosim is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data.models import (
    Basho, BoutRecord, BoutResult, FatigueCurve, FightingStyle,
    HeadToHead, MatchupEntry, MomentumState, Rank,
    TournamentRecord, WrestlerProfile, WrestlerRating, WrestlerStanding,
)
from engine.bout_simulator import BoutSimulator
from engine.probability import (
    build_head_to_head, build_wrestler_rating, compute_base_rating,
    compute_head_to_head_adjustment, logistic_win_probability,
)
from engine.tournament_simulator import TournamentSimulator
from modifiers.base import BoutContext, ModifierResult
from modifiers.injury_fatigue import InjuryFatigueModifier, compute_daily_fatigue
from modifiers.matchup import MatchupModifier
from modifiers.momentum import MomentumModifier
from utils.config import SimulationConfig, set_config


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _deterministic_config():
    cfg = SimulationConfig(random_seed=42, bout_iterations=1_000, tournament_iterations=10)
    set_config(cfg)
    return cfg


def _yokozuna():
    return WrestlerProfile(
        wrestler_id="Y001", shikona="Terunofuji", rank=Rank.YOKOZUNA,
        heya="Isegahama", height_cm=192.0, weight_kg=178.0,
        birth_date=date(1991, 11, 29), fighting_style=FightingStyle.YOTSU, side="east",
    )

def _ozeki():
    return WrestlerProfile(
        wrestler_id="O001", shikona="Kotozakura", rank=Rank.OZEKI,
        heya="Sadogatake", height_cm=189.0, weight_kg=175.0,
        birth_date=date(1997, 5, 13), fighting_style=FightingStyle.YOTSU, side="west",
    )

def _maegashira():
    return WrestlerProfile(
        wrestler_id="M010", shikona="Tamawashi", rank=Rank.MAEGASHIRA,
        rank_number=10, heya="Kataonami", height_cm=189.0, weight_kg=172.0,
        fighting_style=FightingStyle.OSHI, side="east",
    )

def _sample_tournament_records():
    return [
        TournamentRecord(basho_id="2025.01", wrestler_id="Y001", rank=Rank.YOKOZUNA, wins=12, losses=3),
        TournamentRecord(basho_id="2024.11", wrestler_id="Y001", rank=Rank.YOKOZUNA, wins=13, losses=2, is_yusho=True),
        TournamentRecord(basho_id="2024.09", wrestler_id="Y001", rank=Rank.YOKOZUNA, wins=11, losses=4),
    ]

def _sample_bout_records():
    return [
        BoutRecord(basho_id="2025.01", day=3, east_id="Y001", west_id="O001", winner_id="Y001", kimarite="yorikiri"),
        BoutRecord(basho_id="2024.11", day=12, east_id="O001", west_id="Y001", winner_id="O001", kimarite="oshidashi"),
        BoutRecord(basho_id="2024.09", day=14, east_id="Y001", west_id="O001", winner_id="Y001", kimarite="uwatenage"),
        BoutRecord(basho_id="2024.07", day=11, east_id="O001", west_id="Y001", winner_id="Y001", kimarite="yorikiri"),
    ]

def _small_roster():
    wrestlers = []
    heyas = ["Isegahama", "Sadogatake", "Kataonami", "Takasago",
             "Dewanoumi", "Tokitsukaze", "Kasugano", "Miyagino"]
    styles = [FightingStyle.YOTSU, FightingStyle.OSHI, FightingStyle.HYBRID]
    for i in range(8):
        if i == 0:
            rank, rn = Rank.YOKOZUNA, None
        elif i == 1:
            rank, rn = Rank.OZEKI, None
        elif i < 4:
            rank, rn = Rank.SEKIWAKE, None
        else:
            rank, rn = Rank.MAEGASHIRA, i - 3
        wrestlers.append(WrestlerProfile(
            wrestler_id=f"W{i:03d}", shikona=f"Wrestler_{i}",
            rank=rank, rank_number=rn, heya=heyas[i % len(heyas)],
            height_cm=180.0 + i, weight_kg=150.0 + i * 5,
            fighting_style=styles[i % len(styles)],
            side="east" if i % 2 == 0 else "west",
        ))
    return wrestlers


# ===========================================================================
# DATA MODEL TESTS
# ===========================================================================

class TestWrestlerProfile(unittest.TestCase):

    def test_creation(self):
        y = _yokozuna()
        self.assertEqual(y.shikona, "Terunofuji")
        self.assertEqual(y.rank, Rank.YOKOZUNA)

    def test_full_rank_display(self):
        self.assertEqual(_yokozuna().full_rank, "Yokozuna East")
        self.assertEqual(_maegashira().full_rank, "Maegashira 10 East")

    def test_bmi(self):
        y = _yokozuna()
        expected = round(178.0 / (1.92 * 1.92), 1)
        self.assertEqual(y.bmi, expected)

    def test_immutability(self):
        y = _yokozuna()
        with self.assertRaises(Exception):
            y.shikona = "NewName"

    def test_rank_tier_ordering(self):
        self.assertLess(Rank.YOKOZUNA.tier, Rank.OZEKI.tier)
        self.assertLess(Rank.OZEKI.tier, Rank.MAEGASHIRA.tier)


class TestBoutRecord(unittest.TestCase):

    def test_valid_bout(self):
        b = BoutRecord(basho_id="2025.01", day=1, east_id="A", west_id="B", winner_id="A", kimarite="yorikiri")
        self.assertEqual(b.loser_id, "B")

    def test_invalid_winner(self):
        with self.assertRaises(ValueError):
            BoutRecord(basho_id="2025.01", day=1, east_id="A", west_id="B", winner_id="C")

    def test_invalid_basho_format(self):
        with self.assertRaises(ValueError):
            BoutRecord(basho_id="2025-01", day=1, east_id="A", west_id="B", winner_id="A")


class TestTournamentRecord(unittest.TestCase):

    def test_kachi_koshi(self):
        rec = TournamentRecord(basho_id="2025.01", wrestler_id="X", rank=Rank.OZEKI, wins=8, losses=7)
        self.assertTrue(rec.is_kachi_koshi)
        self.assertFalse(rec.is_make_koshi)

    def test_make_koshi(self):
        rec = TournamentRecord(basho_id="2025.01", wrestler_id="X", rank=Rank.MAEGASHIRA, wins=5, losses=10)
        self.assertTrue(rec.is_make_koshi)

    def test_total_exceeds_15(self):
        with self.assertRaises(ValueError):
            TournamentRecord(basho_id="2025.01", wrestler_id="X", rank=Rank.MAEGASHIRA, wins=10, losses=10)

    def test_win_rate(self):
        rec = TournamentRecord(basho_id="2025.01", wrestler_id="X", rank=Rank.OZEKI, wins=12, losses=3)
        self.assertAlmostEqual(rec.win_rate, 0.8)


class TestBashoEnum(unittest.TestCase):

    def test_all_six(self):
        self.assertEqual(len(Basho), 6)

    def test_display_names(self):
        self.assertIn("January", Basho.HATSU.display_name)
        self.assertIn("November", Basho.KYUSHU.display_name)

    def test_cities(self):
        self.assertEqual(Basho.HATSU.city, "Tokyo")
        self.assertEqual(Basho.HARU.city, "Osaka")
        self.assertEqual(Basho.KYUSHU.city, "Fukuoka")


# ===========================================================================
# PROBABILITY ENGINE TESTS
# ===========================================================================

class TestLogisticProbability(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_equal_ratings(self):
        self.assertAlmostEqual(logistic_win_probability(1500, 1500), 0.5)

    def test_higher_favored(self):
        self.assertGreater(logistic_win_probability(1700, 1500), 0.5)

    def test_lower_disadvantaged(self):
        self.assertLess(logistic_win_probability(1300, 1500), 0.5)

    def test_symmetry(self):
        p1 = logistic_win_probability(1700, 1500)
        p2 = logistic_win_probability(1500, 1700)
        self.assertAlmostEqual(p1 + p2, 1.0)

    def test_extreme_difference(self):
        self.assertGreater(logistic_win_probability(2500, 1000), 0.95)

    def test_bounded(self):
        for e, w in [(3000, 0), (0, 3000), (1500, 1500)]:
            p = logistic_win_probability(float(e), float(w))
            self.assertTrue(0.0 <= p <= 1.0)


class TestBaseRating(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_yokozuna_higher(self):
        self.assertGreater(
            compute_base_rating(_yokozuna(), []),
            compute_base_rating(_maegashira(), []),
        )

    def test_performance_boosts(self):
        base = compute_base_rating(_yokozuna(), [])
        boosted = compute_base_rating(_yokozuna(), _sample_tournament_records())
        self.assertGreater(boosted, base)

    def test_empty_history(self):
        self.assertEqual(compute_base_rating(_yokozuna(), []), 1800.0)

    def test_maegashira_number(self):
        m1 = WrestlerProfile(wrestler_id="M1", shikona="M1", rank=Rank.MAEGASHIRA,
                             rank_number=1, heya="H", fighting_style=FightingStyle.HYBRID)
        m15 = WrestlerProfile(wrestler_id="M15", shikona="M15", rank=Rank.MAEGASHIRA,
                              rank_number=15, heya="H", fighting_style=FightingStyle.HYBRID)
        self.assertGreater(compute_base_rating(m1, []), compute_base_rating(m15, []))


class TestHeadToHead(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_build(self):
        h2h = build_head_to_head("Y001", "O001", _sample_bout_records())
        self.assertEqual(h2h.total, 4)
        self.assertEqual(h2h.a_wins, 3)
        self.assertEqual(h2h.b_wins, 1)

    def test_win_rate(self):
        h2h = build_head_to_head("Y001", "O001", _sample_bout_records())
        self.assertAlmostEqual(h2h.win_rate_for("Y001"), 0.75)

    def test_adjustment_positive(self):
        adj = compute_head_to_head_adjustment("Y001", "O001", _sample_bout_records())
        self.assertGreater(adj, 0)

    def test_adjustment_negative(self):
        adj = compute_head_to_head_adjustment("O001", "Y001", _sample_bout_records())
        self.assertLess(adj, 0)


# ===========================================================================
# MODIFIER TESTS
# ===========================================================================

class TestMomentumModifier(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_all_wins_positive(self):
        mod = MomentumModifier(weight=1.0, streak_window=5)
        ctx = BoutContext(east=_yokozuna(), west=_ozeki())
        ctx.east_recent_results = [True] * 5
        ctx.west_recent_results = [False] * 5
        r = mod.compute(ctx)
        self.assertGreater(r.east_adjustment, 0)
        self.assertLess(r.west_adjustment, 0)

    def test_no_history_zero(self):
        mod = MomentumModifier(weight=1.0)
        ctx = BoutContext(east=_yokozuna(), west=_ozeki())
        r = mod.compute(ctx)
        self.assertEqual(r.east_adjustment, 0.0)

    def test_override(self):
        mod = MomentumModifier(weight=1.0)
        ctx = BoutContext(east=_yokozuna(), west=_ozeki())
        ctx.east_momentum_override = MomentumState.HOT.value
        ctx.west_momentum_override = MomentumState.COLD.value
        r = mod.compute(ctx)
        self.assertGreater(r.east_adjustment, 0)
        self.assertLess(r.west_adjustment, 0)

    def test_disabled(self):
        mod = MomentumModifier(weight=1.0)
        mod.enabled = False
        ctx = BoutContext(east=_yokozuna(), west=_ozeki())
        ctx.east_recent_results = [True] * 5
        r = mod.compute(ctx)
        self.assertEqual(r.east_adjustment, 0.0)

    def test_weight_zero(self):
        mod = MomentumModifier(weight=0.0)
        ctx = BoutContext(east=_yokozuna(), west=_ozeki())
        ctx.east_recent_results = [True] * 5
        r = mod.compute(ctx)
        self.assertEqual(r.east_adjustment, 0.0)


class TestMatchupModifier(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_oshi_vs_yotsu(self):
        mod = MatchupModifier(weight=1.0)
        ctx = BoutContext(east=_maegashira(), west=_yokozuna())  # oshi vs yotsu
        r = mod.compute(ctx)
        self.assertLess(r.east_adjustment, 0)  # oshi disadvantaged
        self.assertGreater(r.west_adjustment, 0)

    def test_same_style_neutral(self):
        mod = MatchupModifier(weight=1.0)
        ctx = BoutContext(east=_yokozuna(), west=_ozeki())  # both yotsu
        r = mod.compute(ctx)
        self.assertEqual(r.east_adjustment, 0.0)

    def test_style_override(self):
        mod = MatchupModifier(weight=1.0)
        ctx = BoutContext(east=_yokozuna(), west=_ozeki())
        ctx.east_style_override = FightingStyle.OSHI.value
        r = mod.compute(ctx)
        self.assertLess(r.east_adjustment, 0)

    def test_custom_matrix(self):
        mod = MatchupModifier(weight=1.0)
        mod.set_interaction(FightingStyle.OSHI.value, FightingStyle.YOTSU.value, 0.8)
        ctx = BoutContext(east=_maegashira(), west=_yokozuna())
        r = mod.compute(ctx)
        self.assertGreater(r.east_adjustment, 0)

    def test_reset_matrix(self):
        mod = MatchupModifier(weight=1.0)
        mod.set_interaction(FightingStyle.OSHI.value, FightingStyle.YOTSU.value, 0.8)
        mod.reset_matrix()
        ctx = BoutContext(east=_maegashira(), west=_yokozuna())
        r = mod.compute(ctx)
        self.assertLess(r.east_adjustment, 0)


class TestInjuryFatigueModifier(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_healthy_day_one(self):
        mod = InjuryFatigueModifier()
        ctx = BoutContext(east=_yokozuna(), west=_ozeki(), day=1)
        r = mod.compute(ctx)
        self.assertGreaterEqual(r.east_adjustment, -5.0)

    def test_injury_penalty(self):
        mod = InjuryFatigueModifier()
        ctx = BoutContext(east=_yokozuna(), west=_ozeki(), day=5)
        ctx.east_injury_severity = 0.8
        ctx.west_injury_severity = 0.0
        r = mod.compute(ctx)
        self.assertLess(r.east_adjustment, r.west_adjustment)

    def test_fatigue_increases(self):
        mod = InjuryFatigueModifier()
        ctx_early = BoutContext(east=_yokozuna(), west=_ozeki(), day=2)
        ctx_late = BoutContext(east=_yokozuna(), west=_ozeki(), day=14)
        early = mod.compute(ctx_early)
        late = mod.compute(ctx_late)
        self.assertLess(late.east_adjustment, early.east_adjustment)

    def test_disabled(self):
        mod = InjuryFatigueModifier()
        mod.enabled = False
        ctx = BoutContext(east=_yokozuna(), west=_ozeki(), day=10)
        ctx.east_injury_severity = 1.0
        r = mod.compute(ctx)
        self.assertEqual(r.east_adjustment, 0.0)

    def test_fatigue_curves(self):
        for curve in FatigueCurve:
            mod = InjuryFatigueModifier(fatigue_curve=curve)
            self.assertAlmostEqual(mod._apply_curve(0.0), 0.0, places=1)
            self.assertAlmostEqual(mod._apply_curve(1.0), 1.0, places=1)

    def test_exponential_lower_midpoint(self):
        lin = InjuryFatigueModifier(fatigue_curve=FatigueCurve.LINEAR)
        exp = InjuryFatigueModifier(fatigue_curve=FatigueCurve.EXPONENTIAL)
        self.assertLess(exp._apply_curve(0.5), lin._apply_curve(0.5))


class TestDailyFatigue(unittest.TestCase):

    def test_increases(self):
        fatigue = 0.0
        for day in range(1, 16):
            fatigue = compute_daily_fatigue(day, 170.0, fatigue, 0.6)
        self.assertGreater(fatigue, 0.0)

    def test_heavy_more(self):
        fl, fh = 0.0, 0.0
        for day in range(1, 16):
            fl = compute_daily_fatigue(day, 130.0, fl, 0.6)
            fh = compute_daily_fatigue(day, 200.0, fh, 0.6)
        self.assertGreater(fh, fl)

    def test_capped(self):
        f = 0.9
        for _ in range(50):
            f = compute_daily_fatigue(15, 200.0, f, 0.0)
        self.assertLessEqual(f, 1.0)


# ===========================================================================
# BOUT SIMULATOR TESTS
# ===========================================================================

class TestBoutSimulator(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_basic(self):
        sim = BoutSimulator()
        r = sim.simulate(_yokozuna(), _ozeki(), day=1)
        self.assertEqual(r.east_id, "Y001")
        self.assertEqual(r.west_id, "O001")
        self.assertTrue(0.0 <= r.east_win_probability <= 1.0)
        self.assertAlmostEqual(r.east_win_probability + r.west_win_probability, 1.0, places=3)
        self.assertIn(r.winner_id, ("Y001", "O001"))

    def test_higher_rank_favored(self):
        sim = BoutSimulator()
        r = sim.simulate(_yokozuna(), _maegashira(), day=1)
        self.assertGreater(r.east_win_probability, 0.5)

    def test_confidence_interval(self):
        sim = BoutSimulator()
        r = sim.simulate(_yokozuna(), _ozeki(), day=1)
        lo, hi = r.confidence_interval_95
        self.assertLessEqual(lo, r.east_win_probability)
        self.assertGreaterEqual(hi, r.east_win_probability)

    def test_modifiers_affect_result(self):
        sim_plain = BoutSimulator(config=SimulationConfig(random_seed=42, bout_iterations=5000))
        r_plain = sim_plain.simulate(_yokozuna(), _ozeki(), day=1)

        mom = MomentumModifier(weight=1.0)
        sim_mod = BoutSimulator(modifiers=[mom], config=SimulationConfig(random_seed=42, bout_iterations=5000))
        ctx = BoutContext(east=_yokozuna(), west=_ozeki(), day=1)
        ctx.east_recent_results = [False] * 5
        ctx.west_recent_results = [True] * 5
        r_mod = sim_mod.simulate(_yokozuna(), _ozeki(), context=ctx, day=1)

        self.assertGreater(r_mod.west_win_probability, r_plain.west_win_probability)

    def test_deterministic(self):
        sim = BoutSimulator()
        p = sim.simulate_deterministic(1700.0, 1500.0)
        self.assertGreater(p, 0.5)
        self.assertLess(p, 1.0)

    def test_wilson_ci(self):
        lo, hi = BoutSimulator._wilson_confidence_interval(500, 1000)
        self.assertLess(lo, 0.5)
        self.assertGreater(hi, 0.5)

    def test_reproducibility(self):
        cfg = SimulationConfig(random_seed=123, bout_iterations=1000)
        sim1 = BoutSimulator(config=cfg)
        r1 = sim1.simulate(_yokozuna(), _ozeki())

        sim2 = BoutSimulator(config=cfg)
        r2 = sim2.simulate(_yokozuna(), _ozeki())

        self.assertEqual(r1.east_win_probability, r2.east_win_probability)


# ===========================================================================
# TOURNAMENT SIMULATOR TESTS
# ===========================================================================

class TestTournamentSimulator(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_single_tournament(self):
        sim = TournamentSimulator(roster=_small_roster())
        r = sim.simulate_tournament("test.01")
        self.assertEqual(r.basho_id, "test.01")
        self.assertEqual(r.total_days_simulated, 15)
        self.assertEqual(len(r.final_standings), 8)
        self.assertIsNotNone(r.yusho_winner_id)

        total_w = sum(s.wins for s in r.final_standings)
        total_l = sum(s.losses for s in r.final_standings)
        self.assertEqual(total_w, total_l)

    def test_standings_sorted(self):
        sim = TournamentSimulator(roster=_small_roster())
        r = sim.simulate_tournament("test.01")
        wins = [s.wins for s in r.final_standings]
        self.assertEqual(wins, sorted(wins, reverse=True))

    def test_yusho_winner_has_most_wins(self):
        sim = TournamentSimulator(roster=_small_roster())
        r = sim.simulate_tournament("test.01")
        top_wins = r.final_standings[0].wins
        yusho = next(s for s in r.final_standings if s.wrestler_id == r.yusho_winner_id)
        self.assertEqual(yusho.wins, top_wins)

    def test_multiple_simulations(self):
        sim = TournamentSimulator(roster=_small_roster())
        probs = sim.simulate_multiple("test.01", n=5)
        self.assertEqual(probs.num_simulations, 5)
        self.assertGreater(len(probs.yusho_probabilities), 0)
        self.assertAlmostEqual(sum(probs.yusho_probabilities.values()), 1.0, places=1)

    def test_callback(self):
        days_seen = []
        def on_day(day, bouts, standings):
            days_seen.append(day)
        sim = TournamentSimulator(roster=_small_roster())
        sim.simulate_tournament("test.01", callback=on_day)
        self.assertEqual(days_seen, list(range(1, 16)))

    def test_with_modifiers(self):
        mods = [MomentumModifier(), MatchupModifier(), InjuryFatigueModifier()]
        sim = TournamentSimulator(roster=_small_roster(), modifiers=mods)
        r = sim.simulate_tournament("test.01")
        self.assertEqual(r.total_days_simulated, 15)
        self.assertIsNotNone(r.yusho_winner_id)

    def test_no_duplicate_within_day(self):
        sim = TournamentSimulator(roster=_small_roster())
        r = sim.simulate_tournament("test.01")
        for day, bouts in r.day_results.items():
            wrestlers = []
            for b in bouts:
                wrestlers.extend([b.east_id, b.west_id])
            self.assertEqual(len(wrestlers), len(set(wrestlers)),
                             f"Day {day}: duplicate wrestler")


# ===========================================================================
# INTEGRATION TESTS
# ===========================================================================

class TestIntegration(unittest.TestCase):

    def setUp(self):
        _deterministic_config()

    def test_full_pipeline_bout(self):
        mods = [MomentumModifier(weight=0.5), MatchupModifier(weight=0.3), InjuryFatigueModifier()]
        sim = BoutSimulator(modifiers=mods)
        ctx = BoutContext(east=_yokozuna(), west=_ozeki(), day=10)
        ctx.east_recent_results = [True, True, False, True, True]
        ctx.west_recent_results = [True, False, True, False, True]
        ctx.east_injury_severity = 0.1

        r = sim.simulate(
            _yokozuna(), _ozeki(), context=ctx,
            east_tournament_history=_sample_tournament_records(),
            bout_history=_sample_bout_records(), day=10,
        )
        self.assertIn(r.winner_id, ("Y001", "O001"))
        self.assertGreater(r.east_win_probability, 0.0)
        self.assertLess(r.east_win_probability, 1.0)

    def test_rating_composition(self):
        rating = build_wrestler_rating(
            _yokozuna(), _sample_tournament_records(),
            opponent=_ozeki(), bout_history=_sample_bout_records(),
        )
        self.assertGreater(rating.base_rating, 1800.0)

    def test_all_modifiers_describe(self):
        ctx = BoutContext(east=_yokozuna(), west=_ozeki(), day=5)
        ctx.east_recent_results = [True, True]
        for Cls in [MomentumModifier, MatchupModifier, InjuryFatigueModifier]:
            mod = Cls()
            r = mod.compute(ctx)
            self.assertIsInstance(r, ModifierResult)
            self.assertGreater(len(r.description), 0)


if __name__ == "__main__":
    unittest.main()
