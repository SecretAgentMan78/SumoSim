"""
Matchup / Style Modifier

Adjusts ratings based on fighting style interactions (oshi vs yotsu vs hybrid).
"""

from __future__ import annotations

from modifiers.base import BaseModifier, BoutContext, ModifierResult
from data.models import FightingStyle
from utils.config import get_config


# Default style interaction matrix.
# Values represent the advantage the ROW style has over the COLUMN style.
# Positive = row has advantage, negative = row has disadvantage, 0 = neutral.
# Scale: -1.0 to +1.0
_DEFAULT_STYLE_MATRIX: dict[str, dict[str, float]] = {
    FightingStyle.OSHI.value: {
        FightingStyle.OSHI.value: 0.0,
        FightingStyle.YOTSU.value: -0.3,   # oshi is disadvantaged vs yotsu
        FightingStyle.HYBRID.value: 0.15,   # oshi has slight edge vs hybrid
    },
    FightingStyle.YOTSU.value: {
        FightingStyle.OSHI.value: 0.3,      # yotsu has advantage vs oshi
        FightingStyle.YOTSU.value: 0.0,
        FightingStyle.HYBRID.value: -0.1,   # yotsu slightly disadvantaged vs hybrid
    },
    FightingStyle.HYBRID.value: {
        FightingStyle.OSHI.value: -0.15,    # hybrid slightly weaker vs oshi
        FightingStyle.YOTSU.value: 0.1,     # hybrid slightly better vs yotsu
        FightingStyle.HYBRID.value: 0.0,
    },
}


class MatchupModifier(BaseModifier):
    """
    Applies a style-based advantage/disadvantage between two wrestlers.

    The interaction is looked up from a style matrix (rock-paper-scissors-ish)
    and scaled by the matchup weight and max adjustment.

    Users can override individual wrestler style classifications and can
    edit the interaction matrix itself for advanced what-if exploration.
    """

    def __init__(self, weight: float | None = None):
        cfg = get_config()
        self._weight = weight if weight is not None else cfg.matchup_weight
        self._enabled = True
        # Deep copy so user edits don't mutate the default
        self._style_matrix: dict[str, dict[str, float]] = {
            k: dict(v) for k, v in _DEFAULT_STYLE_MATRIX.items()
        }

    @property
    def name(self) -> str:
        return "Matchup / Style"

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
    def style_matrix(self) -> dict[str, dict[str, float]]:
        return self._style_matrix

    def set_interaction(self, attacker: str, defender: str, value: float) -> None:
        """Set a custom interaction value between two styles."""
        value = max(-1.0, min(1.0, value))
        if attacker not in self._style_matrix:
            self._style_matrix[attacker] = {}
        self._style_matrix[attacker][defender] = value

    def reset_matrix(self) -> None:
        """Reset to default style interactions."""
        self._style_matrix = {
            k: dict(v) for k, v in _DEFAULT_STYLE_MATRIX.items()
        }

    def compute(self, context: BoutContext) -> ModifierResult:
        if not self._enabled or self._weight == 0.0:
            return ModifierResult(description="Matchup: disabled")

        cfg = get_config()
        max_adj = cfg.matchup_max_adjustment

        east_style = (
            context.east_style_override or context.east.fighting_style.value
        )
        west_style = (
            context.west_style_override or context.west.fighting_style.value
        )

        # Look up advantage for east over west
        east_advantage = self._get_interaction(east_style, west_style)
        # West's advantage is the inverse
        west_advantage = self._get_interaction(west_style, east_style)

        east_adj = east_advantage * self._weight * max_adj
        west_adj = west_advantage * self._weight * max_adj

        return ModifierResult(
            east_adjustment=east_adj,
            west_adjustment=west_adj,
            description=(
                f"Matchup: {east_style} vs {west_style} "
                f"E={east_advantage:+.2f}({east_adj:+.0f}pts) "
                f"W={west_advantage:+.2f}({west_adj:+.0f}pts)"
            ),
        )

    def _get_interaction(self, attacker: str, defender: str) -> float:
        """Look up the interaction value, defaulting to 0 if unknown."""
        return self._style_matrix.get(attacker, {}).get(defender, 0.0)
