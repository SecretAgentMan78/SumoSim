"""
SumoSim Probability Engine

Core mathematical functions for converting ratings into win probabilities.
Handles base rating calculation from historical data and head-to-head adjustments.
"""

from __future__ import annotations

import math
from typing import Sequence

from data.models import (
    BoutRecord,
    HeadToHead,
    Rank,
    TournamentRecord,
    WrestlerProfile,
    WrestlerRating,
)
from utils.config import (
    DEFAULT_RANK_RATINGS,
    MAEGASHIRA_RATING_STEP,
    SimulationConfig,
    get_config,
)


def logistic_win_probability(
    rating_east: float,
    rating_west: float,
    k: float | None = None,
) -> float:
    """
    Convert a rating differential into east's win probability.

    Uses the logistic (sigmoid) function:
        P(east wins) = 1 / (1 + exp(-k * (R_east - R_west)))

    Args:
        rating_east: East wrestler's effective rating.
        rating_west: West wrestler's effective rating.
        k: Logistic scaling constant. If None, uses config default.

    Returns:
        Probability in [0, 1] that east wins.
    """
    if k is None:
        k = get_config().logistic_k

    diff = rating_east - rating_west
    # Clamp to avoid overflow in exp
    exponent = -k * diff
    exponent = max(-500.0, min(500.0, exponent))
    return 1.0 / (1.0 + math.exp(exponent))


def compute_base_rating(
    wrestler: WrestlerProfile,
    tournament_history: Sequence[TournamentRecord],
    config: SimulationConfig | None = None,
) -> float:
    """
    Compute a wrestler's base strength rating from recent tournament results.

    The rating starts from a rank-based default, then adjusts based on
    actual performance weighted by recency.

    Args:
        wrestler: The wrestler's current profile.
        tournament_history: Recent tournament records, ordered most-recent-first.
        config: Simulation config (uses global default if None).

    Returns:
        Base rating as a float (typically 1300-2000 range).
    """
    cfg = config or get_config()

    # Start with rank-based default
    rank_rating = DEFAULT_RANK_RATINGS.get(wrestler.rank.value, 1500.0)
    if wrestler.rank == Rank.MAEGASHIRA and wrestler.rank_number:
        rank_rating -= (wrestler.rank_number - 1) * MAEGASHIRA_RATING_STEP

    if not tournament_history:
        return rank_rating

    # Use up to recency_basho_count most recent tournaments
    recent = list(tournament_history[: cfg.recency_basho_count])

    # Compute weighted performance score
    total_weight = 0.0
    weighted_performance = 0.0

    for i, record in enumerate(recent):
        # Exponential decay: most recent = weight 1.0, older decays
        weight = cfg.recency_decay ** i

        if record.total_bouts == 0:
            continue

        # Performance: win rate adjusted for rank context
        win_rate = record.wins / record.total_bouts
        rank_expectation = _rank_expected_win_rate(record.rank, record.rank_number)

        # Performance score: how much better/worse than expected
        # Scale: +1.0 means significantly exceeded expectations
        performance = (win_rate - rank_expectation) * 2.0

        weighted_performance += weight * performance
        total_weight += weight

    if total_weight == 0:
        return rank_rating

    avg_performance = weighted_performance / total_weight

    # Convert performance score to rating adjustment
    # A performance of +1.0 (dominant) shifts rating up ~200 points
    adjustment = avg_performance * 200.0

    return rank_rating + adjustment


def compute_head_to_head_adjustment(
    wrestler_id: str,
    opponent_id: str,
    bout_history: Sequence[BoutRecord],
    config: SimulationConfig | None = None,
) -> float:
    """
    Compute a rating adjustment based on head-to-head record.

    A strong winning record against a specific opponent provides a bonus;
    a losing record applies a penalty.

    Args:
        wrestler_id: The wrestler we're computing the adjustment for.
        opponent_id: Their opponent.
        bout_history: All historical bouts between these two wrestlers.
        config: Simulation config.

    Returns:
        Rating adjustment (positive = advantage, negative = disadvantage).
    """
    cfg = config or get_config()

    if not bout_history:
        return 0.0

    h2h = build_head_to_head(wrestler_id, opponent_id, bout_history)
    if h2h.total < 2:
        # Not enough data for meaningful adjustment
        return 0.0

    win_rate = h2h.win_rate_for(wrestler_id)
    if win_rate is None:
        return 0.0

    # Deviation from 50%: +0.5 means 100% wins, -0.5 means 0% wins
    deviation = win_rate - 0.5

    # Scale by confidence (more bouts = more confidence, capped at 20)
    confidence = min(h2h.total / 20.0, 1.0)

    # Max h2h adjustment is ~100 rating points at full weight
    max_h2h_points = 100.0
    adjustment = deviation * confidence * max_h2h_points * 2.0

    return adjustment * cfg.head_to_head_weight


def build_head_to_head(
    wrestler_a_id: str,
    wrestler_b_id: str,
    bout_history: Sequence[BoutRecord],
) -> HeadToHead:
    """Build a HeadToHead record from a sequence of bout records."""
    relevant = [
        b for b in bout_history
        if {b.east_id, b.west_id} == {wrestler_a_id, wrestler_b_id}
    ]

    a_wins = sum(1 for b in relevant if b.winner_id == wrestler_a_id)
    b_wins = sum(1 for b in relevant if b.winner_id == wrestler_b_id)

    return HeadToHead(
        wrestler_a_id=wrestler_a_id,
        wrestler_b_id=wrestler_b_id,
        a_wins=a_wins,
        b_wins=b_wins,
        bouts=relevant,
    )


def build_wrestler_rating(
    wrestler: WrestlerProfile,
    tournament_history: Sequence[TournamentRecord],
    opponent: WrestlerProfile | None = None,
    bout_history: Sequence[BoutRecord] | None = None,
    config: SimulationConfig | None = None,
) -> WrestlerRating:
    """
    Build a complete WrestlerRating with base rating and head-to-head adjustment.

    Modifier adjustments are applied separately by the simulation engine.
    """
    cfg = config or get_config()

    base = compute_base_rating(wrestler, tournament_history, cfg)

    h2h_adj = 0.0
    if opponent and bout_history:
        h2h_adj = compute_head_to_head_adjustment(
            wrestler.wrestler_id, opponent.wrestler_id, bout_history, cfg
        )

    return WrestlerRating(
        wrestler_id=wrestler.wrestler_id,
        base_rating=base + h2h_adj,
    )


def _rank_expected_win_rate(rank: Rank, rank_number: int | None) -> float:
    """
    Expected win rate for a wrestler at a given rank.

    Used to contextualize performance: a Yokozuna going 10-5 is
    underperforming, while a Maegashira 15 going 10-5 is outstanding.
    """
    expectations = {
        Rank.YOKOZUNA: 0.75,
        Rank.OZEKI: 0.65,
        Rank.SEKIWAKE: 0.55,
        Rank.KOMUSUBI: 0.50,
        Rank.MAEGASHIRA: 0.50,
    }
    base = expectations.get(rank, 0.50)

    # Lower-ranked maegashira are expected to win slightly less
    if rank == Rank.MAEGASHIRA and rank_number:
        base -= (rank_number - 1) * 0.01  # M1=0.50, M17=0.34

    return max(0.20, min(0.80, base))
