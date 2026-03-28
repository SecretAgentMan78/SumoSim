"""
SumoSim Kimarite Predictor

Predicts the most likely winning technique for a bout based on:
  1. H2H history: How has this specific winner beaten this specific loser before?
  2. Winner's overall technique profile: What kimarite does this wrestler use most?
  3. Fighting style interaction: Oshi vs yotsu matchups favor certain techniques.

The H2H history is weighted most heavily since it captures the specific
dynamics of a given matchup.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Optional, Sequence

from data.models import BoutRecord, FightingStyle, WrestlerProfile


# Weight constants for blending sources
_H2H_WEIGHT = 3.0      # H2H history counts 3x
_PROFILE_WEIGHT = 1.0   # Winner's overall profile counts 1x

# Fallback kimarite by fighting style matchup
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
    Predicts the winning technique for a bout.

    Usage:
        predictor = KimaritePredictor(bout_records, roster)
        technique, confidence = predictor.predict("hoshoryu", "onosato")
        top_3 = predictor.predict_top_n("hoshoryu", "onosato", n=3)
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

        for br in bout_records:
            if br.kimarite:
                self._matchup_kimarite[(br.winner_id, br.loser_id)][br.kimarite] += 1
                self._wrestler_kimarite[br.winner_id][br.kimarite] += 1

    def predict(
        self, winner_id: str, loser_id: str
    ) -> tuple[str, float]:
        """
        Predict the most likely kimarite for this winner beating this loser.

        Returns:
            (kimarite, confidence) where confidence is 0.0-1.0
        """
        top = self.predict_top_n(winner_id, loser_id, n=1)
        if top:
            return top[0]
        return ("yorikiri", 0.1)  # ultimate fallback

    def predict_top_n(
        self, winner_id: str, loser_id: str, n: int = 3
    ) -> list[tuple[str, float]]:
        """
        Predict the top N most likely kimarite with confidence scores.

        Returns:
            List of (kimarite, confidence) sorted by likelihood.
        """
        scores: Counter = Counter()

        # Source 1: H2H history (highest weight)
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

        # Source 3: Style-based fallback if we have no data
        if not scores:
            w_style = self._style_map.get(winner_id, FightingStyle.HYBRID)
            l_style = self._style_map.get(loser_id, FightingStyle.HYBRID)
            defaults = _STYLE_DEFAULTS.get(
                (w_style, l_style),
                ["yorikiri", "oshidashi", "hatakikomi"]
            )
            for i, tech in enumerate(defaults):
                scores[tech] = 1.0 / (i + 1)  # decreasing weight

        # Normalize to confidence scores
        total = sum(scores.values())
        if total == 0:
            return [("yorikiri", 0.1)]

        results = [
            (technique, score / total)
            for technique, score in scores.most_common(n)
        ]
        return results

    def predict_for_bout(
        self, east_id: str, west_id: str, east_win_prob: float
    ) -> tuple[str, float]:
        """
        Predict kimarite considering who is more likely to win.

        Blends the predicted kimarite for each possible winner
        weighted by their win probability.
        """
        # Get top technique for each possible winner
        east_kim, east_conf = self.predict(east_id, west_id)
        west_kim, west_conf = self.predict(west_id, east_id)

        # Return the predicted winner's most likely technique
        if east_win_prob >= 0.5:
            return east_kim, east_conf
        else:
            return west_kim, west_conf
