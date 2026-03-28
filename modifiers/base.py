"""
SumoSim Modifier System — Base Class

All modifiers extend BaseModifier and return signed rating adjustments.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from data.models import WrestlerProfile


class BoutContext:
    """
    All information available to modifiers for a single bout evaluation.

    This is a mutable container passed through the modifier pipeline,
    carrying both static data and accumulated state.
    """

    __slots__ = (
        "east", "west", "day", "basho_id",
        "east_recent_results", "west_recent_results",
        "east_injury_severity", "west_injury_severity",
        "east_cumulative_fatigue", "west_cumulative_fatigue",
        "east_momentum_override", "west_momentum_override",
        "east_style_override", "west_style_override",
    )

    def __init__(
        self,
        east: WrestlerProfile,
        west: WrestlerProfile,
        day: int = 1,
        basho_id: str = "",
    ):
        self.east = east
        self.west = west
        self.day = day
        self.basho_id = basho_id

        # Recent bout results as list of bools (True=win), most recent first
        self.east_recent_results: list[bool] = []
        self.west_recent_results: list[bool] = []

        # Injury: 0.0 = healthy, 1.0 = severely compromised
        self.east_injury_severity: float = 0.0
        self.west_injury_severity: float = 0.0

        # Fatigue: accumulated over tournament days (0.0 = fresh)
        self.east_cumulative_fatigue: float = 0.0
        self.west_cumulative_fatigue: float = 0.0

        # Manual overrides (None = use data-driven calculation)
        self.east_momentum_override: str | None = None
        self.west_momentum_override: str | None = None
        self.east_style_override: str | None = None
        self.west_style_override: str | None = None


class ModifierResult:
    """The output of a single modifier: adjustments for east and west."""

    __slots__ = ("east_adjustment", "west_adjustment", "description")

    def __init__(
        self,
        east_adjustment: float = 0.0,
        west_adjustment: float = 0.0,
        description: str = "",
    ):
        self.east_adjustment = east_adjustment
        self.west_adjustment = west_adjustment
        self.description = description

    def __repr__(self) -> str:
        return (
            f"ModifierResult(east={self.east_adjustment:+.1f}, "
            f"west={self.west_adjustment:+.1f}, desc='{self.description}')"
        )


class BaseModifier(ABC):
    """
    Abstract base for all bout modifiers.

    Subclasses implement `compute()` which receives the full bout context
    and returns signed rating adjustments for each wrestler.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable modifier name for GUI display."""
        ...

    @abstractmethod
    def compute(self, context: BoutContext) -> ModifierResult:
        """
        Compute rating adjustments based on bout context.

        Returns a ModifierResult with signed floats:
          - Positive = rating boost (wrestler is advantaged)
          - Negative = rating penalty (wrestler is disadvantaged)
        """
        ...

    @property
    def enabled(self) -> bool:
        """Whether this modifier is active. Override to support toggling."""
        return True
