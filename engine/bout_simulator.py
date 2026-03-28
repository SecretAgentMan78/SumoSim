"""
SumoSim Bout Simulator

Monte Carlo simulation engine for individual bouts.
Combines base ratings with modifier adjustments and random noise.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np

from data.models import (
    BoutRecord,
    BoutResult,
    TournamentRecord,
    WrestlerProfile,
    WrestlerRating,
)
from engine.probability import (
    build_wrestler_rating,
    logistic_win_probability,
)
from modifiers.base import BaseModifier, BoutContext, ModifierResult
from utils.config import SimulationConfig, get_config


class BoutSimulator:
    """
    Simulates individual bouts using Monte Carlo methods.

    Usage:
        sim = BoutSimulator(modifiers=[MomentumModifier(), MatchupModifier()])
        result = sim.simulate(east, west, context, ...)
    """

    def __init__(
        self,
        modifiers: Sequence[BaseModifier] | None = None,
        config: SimulationConfig | None = None,
    ):
        self._modifiers = list(modifiers) if modifiers else []
        self._config = config or get_config()
        self._rng: np.random.Generator | None = None
        self._reset_rng()

    @property
    def modifiers(self) -> list[BaseModifier]:
        return self._modifiers

    @modifiers.setter
    def modifiers(self, value: list[BaseModifier]) -> None:
        self._modifiers = value

    @property
    def config(self) -> SimulationConfig:
        return self._config

    @config.setter
    def config(self, value: SimulationConfig) -> None:
        self._config = value
        self._reset_rng()

    def _reset_rng(self) -> None:
        """Initialize or reinitialize the random number generator."""
        seed = self._config.random_seed
        self._rng = np.random.default_rng(seed)

    def simulate(
        self,
        east: WrestlerProfile,
        west: WrestlerProfile,
        context: BoutContext | None = None,
        east_tournament_history: Sequence[TournamentRecord] | None = None,
        west_tournament_history: Sequence[TournamentRecord] | None = None,
        bout_history: Sequence[BoutRecord] | None = None,
        day: int = 1,
        iterations: int | None = None,
    ) -> BoutResult:
        """
        Run a Monte Carlo simulation for a single bout.

        Args:
            east: East wrestler profile.
            west: West wrestler profile.
            context: Pre-built bout context (if None, one is created).
            east_tournament_history: East's recent tournament records.
            west_tournament_history: West's recent tournament records.
            bout_history: Historical bouts between these two wrestlers.
            day: Tournament day (1-15).
            iterations: Override iteration count.

        Returns:
            BoutResult with win probabilities and a sampled winner.
        """
        n = iterations or self._config.bout_iterations

        # Build context if not provided
        if context is None:
            context = BoutContext(east=east, west=west, day=day)

        # Compute base ratings
        east_rating = build_wrestler_rating(
            east,
            east_tournament_history or [],
            opponent=west,
            bout_history=bout_history,
            config=self._config,
        )
        west_rating = build_wrestler_rating(
            west,
            west_tournament_history or [],
            opponent=east,
            bout_history=bout_history,
            config=self._config,
        )

        # Apply modifiers
        modifier_descriptions = []
        for mod in self._modifiers:
            if not mod.enabled:
                continue
            result = mod.compute(context)
            east_rating.momentum_adjustment += result.east_adjustment
            west_rating.momentum_adjustment += result.west_adjustment
            modifier_descriptions.append(result.description)

        # Run Monte Carlo
        east_effective = east_rating.effective_rating
        west_effective = west_rating.effective_rating

        east_wins = self._run_monte_carlo(east_effective, west_effective, n)

        # Compute probability and CI
        east_prob = east_wins / n
        west_prob = 1.0 - east_prob

        ci_low, ci_high = self._wilson_confidence_interval(east_wins, n)

        # Determine winner (sample from the distribution)
        winner_id = (
            east.wrestler_id if self._rng.random() < east_prob
            else west.wrestler_id
        )

        return BoutResult(
            day=context.day,
            east_id=east.wrestler_id,
            west_id=west.wrestler_id,
            east_win_probability=round(east_prob, 4),
            west_win_probability=round(west_prob, 4),
            winner_id=winner_id,
            confidence_interval_95=(round(ci_low, 4), round(ci_high, 4)),
        )

    def simulate_deterministic(
        self,
        east_effective_rating: float,
        west_effective_rating: float,
    ) -> float:
        """
        Quick probability calculation without Monte Carlo.
        Returns east win probability directly from the logistic function.
        """
        return logistic_win_probability(
            east_effective_rating,
            west_effective_rating,
            k=self._config.logistic_k,
        )

    def _run_monte_carlo(
        self,
        east_rating: float,
        west_rating: float,
        n: int,
    ) -> int:
        """
        Run N iterations of the bout with Gaussian noise.

        Vectorized with NumPy for performance.

        Returns:
            Number of iterations won by east.
        """
        sigma = self._config.noise_sigma

        # Generate noise for both wrestlers across all iterations
        east_noise = self._rng.normal(0.0, sigma, size=n) * 100.0
        west_noise = self._rng.normal(0.0, sigma, size=n) * 100.0

        # Effective ratings with noise
        east_noisy = east_rating + east_noise
        west_noisy = west_rating + west_noise

        # Compute win probability per iteration using vectorized logistic
        k = self._config.logistic_k
        diffs = east_noisy - west_noisy
        exponents = np.clip(-k * diffs, -500.0, 500.0)
        probs = 1.0 / (1.0 + np.exp(exponents))

        # Sample outcomes
        random_draws = self._rng.random(size=n)
        east_wins = int(np.sum(random_draws < probs))

        return east_wins

    @staticmethod
    def _wilson_confidence_interval(
        successes: int, total: int, z: float = 1.96
    ) -> tuple[float, float]:
        """
        Wilson score interval for binomial proportion.

        More accurate than the normal approximation, especially
        for probabilities near 0 or 1.
        """
        if total == 0:
            return (0.0, 1.0)

        p_hat = successes / total
        z2 = z * z
        denom = 1.0 + z2 / total

        center = (p_hat + z2 / (2.0 * total)) / denom
        spread = (z / denom) * (
            (p_hat * (1.0 - p_hat) / total + z2 / (4.0 * total * total)) ** 0.5
        )

        return (max(0.0, center - spread), min(1.0, center + spread))
