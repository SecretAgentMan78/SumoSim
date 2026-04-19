"""
SumoSim Kimarite Predictor

Predicts the winning technique for a bout by sampling from a weighted
probability distribution built from three data sources:

  1. H2H history (weight 5.0): How has this winner beaten this loser before?
     Most specific — captures the unique dynamics of a particular matchup.

  2. Winner's technique profile (weight 2.0): What kimarite does the winner
     use most across all opponents? Captures personal tendencies.

  3. Global baseline (weight 0.5): Historical frequency of each kimarite
     across ALL bouts in the database. Ensures every known technique has
     a non-zero chance of being selected, including rare ones.

The result is stochastic — repeated calls produce different techniques
weighted by historical likelihood, so simulations feel alive rather
than deterministic.
"""

from __future__ import annotations

import random
from collections import Counter, defaultdict
from typing import Optional, Sequence

from data.models import BoutRecord, FightingStyle, WrestlerProfile


# Weight constants for blending sources
_H2H_WEIGHT = 5.0       # H2H history — strongest signal
_PROFILE_WEIGHT = 2.0   # Winner's overall profile
_GLOBAL_WEIGHT = 0.5    # Global baseline — ensures long tail of rare kimarite

# Fallback kimarite by fighting style matchup (used only when no data at all)
_STYLE_DEFAULTS = {
    (FightingStyle.OSHI, FightingStyle.OSHI): ["oshidashi", "tsukidashi", "hatakikomi"],
    (FightingStyle.OSHI, FightingStyle.YOTSU): ["oshidashi", "tsukiotoshi", "hikiotoshi"],
    (FightingStyle.OSHI, FightingStyle.HYBRID): ["oshidashi", "hatakikomi", "tsukidashi"],
    (FightingStyle.YOTSU, FightingStyle.OSHI): ["yorikiri", "uwatenage", "kotenage"],
    (FightingStyle.YOTSU, FightingStyle.YOTSU): ["yorikiri", "uwatenage", "shitatenage"],
    (FightingStyle.YOTSU, FightingStyle.HYBRID): ["yorikiri", "shitatenage", "uwatenage"],
    (FightingStyle.HYBRID, FightingStyle.OSHI): ["yorikiri", "hatakikomi", "oshidashi"],
    (FightingStyle.HYBRID, FightingStyle.YOTSU): ["hatakikomi", "yorikiri", "katasukashi"],
    (FightingStyle.HYBRID, FightingStyle.HYBRID): ["yorikiri", "oshidashi", "hatakikomi"],
}


class KimaritePredictor:
    """
    Predicts the winning technique for a bout by sampling from a
    weighted probability distribution.

    Usage:
        predictor = KimaritePredictor(bout_records, roster)
        technique, confidence = predictor.predict("hoshoryu", "onosato")
        technique, confidence = predictor.sample("hoshoryu", "onosato")
        top_5 = predictor.predict_top_n("hoshoryu", "onosato", n=5)
    """

    def __init__(
        self,
        bout_records: Sequence[BoutRecord],
        roster: Sequence[WrestlerProfile] | None = None,
    ):
        self._style_map: dict[str, FightingStyle] = {}
        if roster:
            self._style_map = {w.wrestler_id: w.fighting_style for w in roster}

        # Build per-matchup kimarite counts: (winner, loser) -> Counter
        self._matchup_kimarite: dict[tuple[str, str], Counter] = defaultdict(Counter)

        # Build per-wrestler kimarite profile: winner -> Counter
        self._wrestler_kimarite: dict[str, Counter] = defaultdict(Counter)

        # Build global kimarite frequency across ALL bouts
        self._global_kimarite: Counter = Counter()

        for br in bout_records:
            if br.kimarite:
                self._matchup_kimarite[(br.winner_id, br.loser_id)][br.kimarite] += 1
                self._wrestler_kimarite[br.winner_id][br.kimarite] += 1
                self._global_kimarite[br.kimarite] += 1

    def _build_distribution(
        self, winner_id: str, loser_id: str
    ) -> dict[str, float]:
        """
        Build a weighted probability distribution over all known kimarite.

        Blends H2H history, winner profile, and global baseline.
        Returns {kimarite: score} (unnormalized).
        """
        scores: Counter = Counter()

        # Source 1: H2H history (strongest signal)
        h2h = self._matchup_kimarite.get((winner_id, loser_id))
        if h2h:
            total = sum(h2h.values())
            for technique, count in h2h.items():
                scores[technique] += (count / total) * _H2H_WEIGHT

        # Source 2: Winner's overall technique profile
        profile = self._wrestler_kimarite.get(winner_id)
        if profile:
            total = sum(profile.values())
            for technique, count in profile.items():
                scores[technique] += (count / total) * _PROFILE_WEIGHT

        # Source 3: Global baseline — every known kimarite gets a chance
        if self._global_kimarite:
            total = sum(self._global_kimarite.values())
            for technique, count in self._global_kimarite.items():
                scores[technique] += (count / total) * _GLOBAL_WEIGHT

        # Source 4: Style-based fallback if nothing else
        if not scores:
            w_style = self._style_map.get(winner_id, FightingStyle.HYBRID)
            l_style = self._style_map.get(loser_id, FightingStyle.HYBRID)
            defaults = _STYLE_DEFAULTS.get(
                (w_style, l_style),
                ["yorikiri", "oshidashi", "hatakikomi"]
            )
            for i, tech in enumerate(defaults):
                scores[tech] = 1.0 / (i + 1)

        return dict(scores)

    def predict(
        self, winner_id: str, loser_id: str
    ) -> tuple[str, float]:
        """
        Return the single most likely kimarite (deterministic).

        Returns:
            (kimarite, confidence) where confidence is 0.0-1.0
        """
        top = self.predict_top_n(winner_id, loser_id, n=1)
        if top:
            return top[0]
        return ("yorikiri", 0.1)

    def sample(
        self, winner_id: str, loser_id: str
    ) -> tuple[str, float]:
        """
        Randomly sample a kimarite from the weighted distribution.

        Higher-probability techniques are selected more often, but rare
        techniques can still appear. This is the method to use for
        simulation — it produces varied, realistic outcomes.

        Returns:
            (kimarite, probability) where probability is the technique's
            share of the distribution (not a confidence score).
        """
        scores = self._build_distribution(winner_id, loser_id)
        if not scores:
            return ("yorikiri", 0.1)

        total = sum(scores.values())
        techniques = list(scores.keys())
        weights = [scores[t] / total for t in techniques]

        # Weighted random sample
        chosen = random.choices(techniques, weights=weights, k=1)[0]
        probability = scores[chosen] / total

        return chosen, probability

    def predict_top_n(
        self, winner_id: str, loser_id: str, n: int = 3
    ) -> list[tuple[str, float]]:
        """
        Return the top N most likely kimarite with their probabilities.

        Returns:
            List of (kimarite, probability) sorted by likelihood.
        """
        scores = self._build_distribution(winner_id, loser_id)
        if not scores:
            return [("yorikiri", 0.1)]

        total = sum(scores.values())
        ranked = sorted(scores.items(), key=lambda x: -x[1])

        return [
            (technique, score / total)
            for technique, score in ranked[:n]
        ]

    def predict_for_bout(
        self, east_id: str, west_id: str, east_win_prob: float
    ) -> tuple[str, float]:
        """
        Sample a kimarite for a simulated bout, considering who won.

        Uses the actual winner (determined by east_win_prob and the
        BoutResult.winner_id) to sample from the correct distribution.
        This is called after the bout winner is already determined.
        """
        # Determine the likely winner based on the simulation
        if east_win_prob >= 0.5:
            return self.sample(east_id, west_id)
        else:
            return self.sample(west_id, east_id)

    def sample_for_winner(
        self, winner_id: str, loser_id: str
    ) -> tuple[str, float]:
        """
        Sample a kimarite given a known winner. Use this when the bout
        outcome is already determined (e.g. after Monte Carlo sampling).

        Returns:
            (kimarite, probability)
        """
        return self.sample(winner_id, loser_id)
