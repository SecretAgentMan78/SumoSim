"""
SumoSim Tournament Simulator

Simulates a full 15-day basho with matchup generation, day-by-day
progression, fatigue tracking, and playoff resolution.
"""

from __future__ import annotations

import itertools
from collections import defaultdict
from typing import Sequence

import numpy as np

from data.models import (
    BoutRecord,
    BoutResult,
    FatigueCurve,
    MatchupEntry,
    TournamentRecord,
    TournamentResult,
    TournamentProbabilities,
    WrestlerProfile,
    WrestlerStanding,
)
from engine.bout_simulator import BoutSimulator
from modifiers.base import BaseModifier, BoutContext
from modifiers.injury_fatigue import compute_daily_fatigue
from utils.config import SimulationConfig, TOURNAMENT_DAYS, get_config


class TournamentSimulator:
    """
    Simulates a full 15-day tournament.

    Can use real torikumi (scraped schedules) or generate plausible
    matchups using a Swiss-style pairing heuristic.

    Usage:
        sim = TournamentSimulator(roster, modifiers=[...])
        result = sim.simulate_tournament("2025.01")
        probs = sim.simulate_multiple("2025.01", n=1000)
    """

    def __init__(
        self,
        roster: Sequence[WrestlerProfile],
        modifiers: Sequence[BaseModifier] | None = None,
        config: SimulationConfig | None = None,
        tournament_histories: dict[str, list[TournamentRecord]] | None = None,
        bout_histories: dict[tuple[str, str], list[BoutRecord]] | None = None,
        bout_records: Sequence[BoutRecord] | None = None,
        schedules: dict[int, list[MatchupEntry]] | None = None,
        brother_pairs: Sequence[tuple[str, str]] | None = None,
    ):
        """
        Args:
            roster: All wrestlers competing in this tournament.
            modifiers: Active modifier instances.
            config: Simulation config.
            tournament_histories: wrestler_id -> list of recent TournamentRecords.
            bout_histories: (wrestler_a_id, wrestler_b_id) -> list of BoutRecords.
            bout_records: Flat list of all BoutRecords (for kimarite prediction).
            schedules: day -> list of MatchupEntry (real torikumi if available).
            brother_pairs: List of (wrestler_id, wrestler_id) pairs who are
                           brothers/relatives and cannot face each other (same
                           restriction as heya-mates). For Haru 2026:
                           [("wakamotoharu", "wakatakakage"),
                            ("kotoshoho", "kotoeiho")]
        """
        self._config = config or get_config()
        self._roster = list(roster)
        self._roster_map = {w.wrestler_id: w for w in self._roster}
        self._modifiers = list(modifiers) if modifiers else []
        self._tournament_histories = tournament_histories or {}
        self._bout_histories = bout_histories or {}
        self._schedules = schedules or {}

        # Build exclusion set: pairs who cannot face each other
        # Includes same-heya and brother pairs
        self._exclusions: set[frozenset] = set()
        # Same-heya
        heya_groups: dict[str, list[str]] = defaultdict(list)
        for w in self._roster:
            heya_groups[w.heya].append(w.wrestler_id)
        for members in heya_groups.values():
            if len(members) > 1:
                for a, b in itertools.combinations(members, 2):
                    self._exclusions.add(frozenset([a, b]))
        # Brother pairs
        for a, b in (brother_pairs or []):
            self._exclusions.add(frozenset([a, b]))

        # Kimarite predictor
        self._kimarite_predictor = None
        if bout_records:
            from engine.kimarite_predictor import KimaritePredictor
            self._kimarite_predictor = KimaritePredictor(bout_records, roster)

        self._bout_sim = BoutSimulator(
            modifiers=self._modifiers, config=self._config
        )
        self._rng = np.random.default_rng(self._config.random_seed)

    def simulate_tournament(
        self,
        basho_id: str = "sim",
        callback: callable | None = None,
    ) -> TournamentResult:
        """
        Simulate a single complete tournament.

        Args:
            basho_id: Identifier for this tournament.
            callback: Optional function called after each day with
                      (day, day_results, standings) for progress updates.

        Returns:
            Complete TournamentResult.
        """
        # Initialize standings
        standings: dict[str, WrestlerStanding] = {}
        for w in self._roster:
            standings[w.wrestler_id] = WrestlerStanding(
                wrestler_id=w.wrestler_id,
                shikona=w.shikona,
                rank=w.rank,
                rank_number=w.rank_number,
            )

        # Track fatigue per wrestler
        fatigue: dict[str, float] = {w.wrestler_id: 0.0 for w in self._roster}

        # Track who has fought whom (to avoid repeats)
        matchup_tracker: set[frozenset] = set()

        day_results: dict[int, list[BoutResult]] = {}

        for day in range(1, TOURNAMENT_DAYS + 1):
            # Get or generate matchups for this day
            matchups = self._get_matchups(day, standings, matchup_tracker)

            # Simulate each bout
            day_bouts: list[BoutResult] = []
            for entry in matchups:
                east = self._roster_map.get(entry.east_id)
                west = self._roster_map.get(entry.west_id)
                if not east or not west:
                    continue

                # Build context with fatigue state
                ctx = BoutContext(east=east, west=west, day=day, basho_id=basho_id)
                ctx.east_cumulative_fatigue = fatigue.get(east.wrestler_id, 0.0)
                ctx.west_cumulative_fatigue = fatigue.get(west.wrestler_id, 0.0)

                # Add recent results for momentum
                ctx.east_recent_results = self._get_recent_results(
                    east.wrestler_id, day_results
                )
                ctx.west_recent_results = self._get_recent_results(
                    west.wrestler_id, day_results
                )

                result = self._bout_sim.simulate(
                    east, west,
                    context=ctx,
                    east_tournament_history=self._tournament_histories.get(
                        east.wrestler_id, []
                    ),
                    west_tournament_history=self._tournament_histories.get(
                        west.wrestler_id, []
                    ),
                    bout_history=self._get_bout_history(
                        east.wrestler_id, west.wrestler_id
                    ),
                    day=day,
                    iterations=max(
                        100, self._config.bout_iterations // 10
                    ),  # fewer iterations per bout in tournament mode
                )

                # Predict kimarite for this bout
                if self._kimarite_predictor:
                    kim, _ = self._kimarite_predictor.predict(
                        result.winner_id, result.loser_id
                    )
                    result.predicted_kimarite = kim

                day_bouts.append(result)

                # Update standings
                winner_standing = standings[result.winner_id]
                loser_standing = standings[result.loser_id]
                standings[result.winner_id] = WrestlerStanding(
                    wrestler_id=winner_standing.wrestler_id,
                    shikona=winner_standing.shikona,
                    rank=winner_standing.rank,
                    rank_number=winner_standing.rank_number,
                    wins=winner_standing.wins + 1,
                    losses=winner_standing.losses,
                )
                standings[result.loser_id] = WrestlerStanding(
                    wrestler_id=loser_standing.wrestler_id,
                    shikona=loser_standing.shikona,
                    rank=loser_standing.rank,
                    rank_number=loser_standing.rank_number,
                    wins=loser_standing.wins,
                    losses=loser_standing.losses + 1,
                )

                # Track matchup
                matchup_tracker.add(
                    frozenset([entry.east_id, entry.west_id])
                )

            day_results[day] = day_bouts

            # Update fatigue
            for wid in fatigue:
                fatigue[wid] = compute_daily_fatigue(
                    day=day,
                    weight_kg=(
                        self._roster_map[wid].weight_kg
                        if wid in self._roster_map else None
                    ),
                    previous_fatigue=fatigue[wid],
                    recovery_factor=self._config.default_recovery_factor,
                )

            if callback:
                callback(day, day_bouts, list(standings.values()))

        # Determine yusho winner (and playoff if needed)
        final_standings = sorted(
            standings.values(), key=lambda s: (-s.wins, s.losses)
        )
        playoff_results = []
        yusho_winner_id = None

        if final_standings:
            top_wins = final_standings[0].wins
            leaders = [s for s in final_standings if s.wins == top_wins]

            if len(leaders) == 1:
                yusho_winner_id = leaders[0].wrestler_id
            else:
                # Playoff (kettei-sen)
                playoff_results, yusho_winner_id = self._simulate_playoff(
                    leaders, day_results, basho_id
                )

        return TournamentResult(
            basho_id=basho_id,
            day_results=day_results,
            final_standings=final_standings,
            yusho_winner_id=yusho_winner_id,
            playoff_results=playoff_results,
        )

    def simulate_multiple(
        self,
        basho_id: str = "sim",
        n: int | None = None,
        progress_callback: callable | None = None,
    ) -> TournamentProbabilities:
        """
        Run multiple tournament simulations and aggregate probabilities.

        Args:
            basho_id: Tournament identifier.
            n: Number of simulations (default from config).
            progress_callback: Called with (current, total) after each simulation.

        Returns:
            TournamentProbabilities with yusho, kachi-koshi, and avg wins.
        """
        num_sims = n or self._config.tournament_iterations

        yusho_counts: dict[str, int] = defaultdict(int)
        kachi_koshi_counts: dict[str, int] = defaultdict(int)
        total_wins: dict[str, float] = defaultdict(float)

        for i in range(num_sims):
            result = self.simulate_tournament(basho_id)

            if result.yusho_winner_id:
                yusho_counts[result.yusho_winner_id] += 1

            for standing in result.final_standings:
                wid = standing.wrestler_id
                total_wins[wid] += standing.wins
                if standing.wins >= 8:
                    kachi_koshi_counts[wid] += 1

            if progress_callback:
                progress_callback(i + 1, num_sims)

        # Compute probabilities
        all_ids = {w.wrestler_id for w in self._roster}
        yusho_probs = {
            wid: yusho_counts.get(wid, 0) / num_sims for wid in all_ids
        }
        kk_probs = {
            wid: kachi_koshi_counts.get(wid, 0) / num_sims for wid in all_ids
        }
        avg_wins = {
            wid: total_wins.get(wid, 0.0) / num_sims for wid in all_ids
        }

        return TournamentProbabilities(
            basho_id=basho_id,
            num_simulations=num_sims,
            yusho_probabilities=dict(
                sorted(yusho_probs.items(), key=lambda x: -x[1])
            ),
            kachi_koshi_probabilities=kk_probs,
            average_wins=avg_wins,
        )

    # ------------------------------------------------------------------
    # Matchup generation
    # ------------------------------------------------------------------

    def _get_matchups(
        self,
        day: int,
        standings: dict[str, WrestlerStanding],
        used: set[frozenset],
    ) -> list[MatchupEntry]:
        """
        Get matchups for a day: use real schedule if available, else generate.

        If the official schedule has fewer bouts than needed (partial day),
        the known matchups are used and remaining wrestlers are paired
        by the generator.
        """
        official = self._schedules.get(day, [])
        n_wrestlers = len(standings)
        expected_bouts = n_wrestlers // 2

        if len(official) >= expected_bouts:
            # Full official schedule — use as-is
            return official

        if official:
            # Partial schedule — use official bouts, generate the rest
            paired: set[str] = set()
            for m in official:
                paired.add(m.east_id)
                paired.add(m.west_id)
                used.add(frozenset([m.east_id, m.west_id]))

            # Build reduced standings for unpaired wrestlers
            remaining = {
                wid: st for wid, st in standings.items() if wid not in paired
            }
            generated = self._generate_matchups(remaining, used)
            return list(official) + generated

        # No official schedule — generate all
        return self._generate_matchups(standings, used)

    def _generate_matchups(
        self,
        standings: dict[str, WrestlerStanding],
        used: set[frozenset],
    ) -> list[MatchupEntry]:
        """
        Swiss-style pairing: group wrestlers by record, pair within groups.
        Avoids repeat matchups, same-heya pairings, and brother pairings.
        """
        # Group wrestlers by win count
        by_wins: dict[int, list[str]] = defaultdict(list)
        for wid, standing in standings.items():
            by_wins[standing.wins].append(wid)

        # Shuffle within groups for randomness
        for win_count in by_wins:
            self._rng.shuffle(by_wins[win_count])

        # Flatten into a priority queue: highest wins first
        available = []
        for wins in sorted(by_wins.keys(), reverse=True):
            available.extend(by_wins[wins])

        matchups: list[MatchupEntry] = []
        paired: set[str] = set()

        for wid in available:
            if wid in paired:
                continue

            # Find best opponent: similar record, not yet paired, not repeated,
            # not excluded (same heya or brothers)
            best_opponent = None
            for oid in available:
                if oid == wid or oid in paired:
                    continue
                if frozenset([wid, oid]) in used:
                    continue
                if frozenset([wid, oid]) in self._exclusions:
                    continue

                best_opponent = oid
                break

            if best_opponent is None:
                # Relaxed search: allow repeats but still respect exclusions
                for oid in available:
                    if oid == wid or oid in paired:
                        continue
                    if frozenset([wid, oid]) in self._exclusions:
                        continue

                    best_opponent = oid
                    break

            if best_opponent is None:
                # Last resort: anyone unpaired (exclusions override only
                # if absolutely no other option exists)
                for oid in available:
                    if oid != wid and oid not in paired:
                        best_opponent = oid
                        break

            if best_opponent:
                matchups.append(MatchupEntry(east_id=wid, west_id=best_opponent))
                paired.add(wid)
                paired.add(best_opponent)

        return matchups

    # ------------------------------------------------------------------
    # Playoffs
    # ------------------------------------------------------------------

    def _simulate_playoff(
        self,
        leaders: list[WrestlerStanding],
        day_results: dict[int, list[BoutResult]],
        basho_id: str,
    ) -> tuple[list[BoutResult], str | None]:
        """
        Simulate a kettei-sen (playoff) among tied leaders.

        For 2 wrestlers: single bout.
        For 3+: round-robin, then sudden death if still tied.
        """
        playoff_bouts: list[BoutResult] = []

        if len(leaders) == 2:
            east = self._roster_map[leaders[0].wrestler_id]
            west = self._roster_map[leaders[1].wrestler_id]
            result = self._bout_sim.simulate(
                east, west, day=16,  # "day 16" for playoff
                iterations=self._config.bout_iterations,
            )
            result_with_flag = BoutResult(
                day=16,
                east_id=result.east_id,
                west_id=result.west_id,
                east_win_probability=result.east_win_probability,
                west_win_probability=result.west_win_probability,
                winner_id=result.winner_id,
                confidence_interval_95=result.confidence_interval_95,
                is_playoff=True,
            )
            playoff_bouts.append(result_with_flag)
            return playoff_bouts, result.winner_id

        # 3+ way tie: simplified round-robin
        playoff_wins: dict[str, int] = defaultdict(int)
        pairs = list(itertools.combinations(leaders, 2))

        for a, b in pairs:
            east = self._roster_map[a.wrestler_id]
            west = self._roster_map[b.wrestler_id]
            result = self._bout_sim.simulate(
                east, west, day=16,
                iterations=self._config.bout_iterations,
            )
            result_with_flag = BoutResult(
                day=16,
                east_id=result.east_id,
                west_id=result.west_id,
                east_win_probability=result.east_win_probability,
                west_win_probability=result.west_win_probability,
                winner_id=result.winner_id,
                confidence_interval_95=result.confidence_interval_95,
                is_playoff=True,
            )
            playoff_bouts.append(result_with_flag)
            playoff_wins[result.winner_id] += 1

        # Winner has most playoff wins
        if playoff_wins:
            winner = max(playoff_wins, key=lambda wid: playoff_wins[wid])
            return playoff_bouts, winner

        # Fallback: highest-ranked leader
        return playoff_bouts, leaders[0].wrestler_id

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_recent_results(
        self,
        wrestler_id: str,
        day_results: dict[int, list[BoutResult]],
    ) -> list[bool]:
        """
        Extract recent bout results for a wrestler from this tournament.
        Returns list of bools (True=win), most recent first.
        """
        results: list[tuple[int, bool]] = []
        for day in sorted(day_results.keys(), reverse=True):
            for bout in day_results[day]:
                if wrestler_id in (bout.east_id, bout.west_id):
                    results.append((day, bout.winner_id == wrestler_id))
        return [won for _, won in results]

    def _get_bout_history(
        self, wrestler_a: str, wrestler_b: str
    ) -> list[BoutRecord]:
        """Look up historical bouts between two wrestlers."""
        key = (wrestler_a, wrestler_b)
        if key in self._bout_histories:
            return self._bout_histories[key]
        # Try reverse order
        key_rev = (wrestler_b, wrestler_a)
        return self._bout_histories.get(key_rev, [])
