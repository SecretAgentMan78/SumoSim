"""
Momentum / Form Modifier

Adjusts wrestler ratings based on recent bout results (hot/cold streaks).
"""

from __future__ import annotations

from modifiers.base import BaseModifier, BoutContext, ModifierResult
from data.models import MomentumState
from utils.config import get_config


# Mapping from manual override states to fixed momentum scores
_OVERRIDE_SCORES: dict[str, float] = {
    MomentumState.HOT.value: 1.0,
    MomentumState.WARM.value: 0.5,
    MomentumState.NEUTRAL.value: 0.0,
    MomentumState.COOL.value: -0.5,
    MomentumState.COLD.value: -1.0,
}


class MomentumModifier(BaseModifier):
    """
    Calculates momentum from recent bout results within a configurable window.

    Momentum score ranges from -1.0 (all recent losses) to +1.0 (all recent
    wins), then scaled by the momentum weight and max adjustment.

    Users can override individual wrestlers to a fixed state (Hot/Neutral/Cold).
    """

    def __init__(self, weight: float | None = None, streak_window: int | None = None):
        cfg = get_config()
        self._weight = weight if weight is not None else cfg.momentum_weight
        self._streak_window = (
            streak_window if streak_window is not None else cfg.momentum_streak_window
        )
        self._enabled = True

    @property
    def name(self) -> str:
        return "Momentum / Form"

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = value

    @property
    def weight(self) -> float:
        return self._weight

    @weight.setter
    def weight(self, value: float) -> None:
        self._weight = max(0.0, min(1.0, value))

    @property
    def streak_window(self) -> int:
        return self._streak_window

    @streak_window.setter
    def streak_window(self, value: int) -> None:
        self._streak_window = max(1, min(15, value))

    def compute(self, context: BoutContext) -> ModifierResult:
        if not self._enabled or self._weight == 0.0:
            return ModifierResult(description="Momentum: disabled")

        cfg = get_config()
        max_adj = cfg.momentum_max_adjustment

        east_score = self._compute_score(
            context.east_recent_results, context.east_momentum_override
        )
        west_score = self._compute_score(
            context.west_recent_results, context.west_momentum_override
        )

        east_adj = east_score * self._weight * max_adj
        west_adj = west_score * self._weight * max_adj

        return ModifierResult(
            east_adjustment=east_adj,
            west_adjustment=west_adj,
            description=(
                f"Momentum: E={east_score:+.2f}({east_adj:+.0f}pts) "
                f"W={west_score:+.2f}({west_adj:+.0f}pts)"
            ),
        )

    def _compute_score(
        self, recent_results: list[bool], override: str | None
    ) -> float:
        """
        Compute momentum score in [-1.0, +1.0].

        Uses weighted recent results (more recent bouts weighted higher)
        or a fixed override value.
        """
        if override and override in _OVERRIDE_SCORES:
            return _OVERRIDE_SCORES[override]

        if not recent_results:
            return 0.0

        window = recent_results[: self._streak_window]
        if not window:
            return 0.0

        # Weighted average: most recent bout has weight=window_size,
        # oldest has weight=1, then normalized to [-1, +1]
        n = len(window)
        total_weight = 0.0
        weighted_sum = 0.0
        for i, won in enumerate(window):
            w = n - i  # most recent = highest weight
            total_weight += w
            weighted_sum += w * (1.0 if won else -1.0)

        return weighted_sum / total_weight
