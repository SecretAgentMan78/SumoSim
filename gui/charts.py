"""
SumoSim Charts

Matplotlib widgets embedded in PyQt6 for:
  - Win probability convergence animation
  - Yusho probability bar chart
  - Kachi-koshi probability distribution
  - Day-by-day standings progression
"""

from __future__ import annotations

import logging
from typing import Optional, Sequence

import matplotlib
matplotlib.use("QtAgg")

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

from PyQt6.QtWidgets import QVBoxLayout, QWidget, QSizePolicy

logger = logging.getLogger(__name__)

# ── SumoSim chart color palette ────────────────────────────────────
COLORS = {
    "east": "#8B0000",
    "west": "#00008B",
    "gold": "#B8860B",
    "green": "#006400",
    "red": "#8B0000",
    "gray": "#888888",
    "bg": "#FAFAF5",
    "grid": "#E0D8C8",
}

RANK_PALETTE = [
    "#B8860B",  # yokozuna (gold)
    "#8B0000",  # ozeki (dark red)
    "#00008B",  # sekiwake (dark blue)
    "#006400",  # komusubi (dark green)
    "#3C3C3C",  # maegashira (gray)
]


class MplCanvas(FigureCanvas):
    """Base matplotlib canvas embedded in Qt."""

    def __init__(self, width=6, height=4, dpi=100, parent=None):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.fig.patch.set_facecolor(COLORS["bg"])
        self.axes = self.fig.add_subplot(111)
        self.axes.set_facecolor(COLORS["bg"])
        super().__init__(self.fig)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    def clear(self) -> None:
        self.axes.clear()
        self.axes.set_facecolor(COLORS["bg"])


class YushoProbabilityChart(QWidget):
    """
    Horizontal bar chart showing each wrestler's yusho probability.
    Top N wrestlers, sorted by probability.
    """

    def __init__(self, parent=None, max_wrestlers: int = 15):
        super().__init__(parent)
        self._max = max_wrestlers
        self._canvas = MplCanvas(width=8, height=5, parent=self)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._canvas)

    def update_chart(
        self,
        names: list[str],
        probabilities: list[float],
        title: str = "Yusho Probability",
    ) -> None:
        ax = self._canvas.axes
        ax.clear()
        ax.set_facecolor(COLORS["bg"])

        # Take top N
        pairs = sorted(zip(names, probabilities), key=lambda x: -x[1])
        pairs = pairs[: self._max]
        pairs.reverse()  # matplotlib barh goes bottom-to-top

        if not pairs:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", fontsize=12)
            self._canvas.draw()
            return

        names_sorted = [p[0] for p in pairs]
        probs_sorted = [p[1] * 100 for p in pairs]

        bars = ax.barh(
            range(len(names_sorted)),
            probs_sorted,
            color=COLORS["east"],
            edgecolor="none",
            height=0.7,
        )

        ax.set_yticks(range(len(names_sorted)))
        ax.set_yticklabels(names_sorted, fontsize=10)
        ax.set_xlabel("Probability (%)", fontsize=10)
        ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
        ax.grid(axis="x", color=COLORS["grid"], linewidth=0.5)
        ax.set_axisbelow(True)

        # Value labels
        for bar, val in zip(bars, probs_sorted):
            if val > 1:
                ax.text(
                    bar.get_width() + 0.5,
                    bar.get_y() + bar.get_height() / 2,
                    f"{val:.1f}%",
                    va="center",
                    fontsize=9,
                    color=COLORS["gray"],
                )

        self._canvas.fig.tight_layout()
        self._canvas.draw()


class WinProbabilityConvergenceChart(QWidget):
    """
    Line chart showing how win probability converges as Monte Carlo
    iterations accumulate. Used to visualize simulation stability.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._canvas = MplCanvas(width=6, height=3, parent=self)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._canvas)

    def update_chart(
        self,
        iterations: list[int],
        east_probs: list[float],
        east_name: str = "East",
        west_name: str = "West",
        final_ci: tuple[float, float] = (0.0, 1.0),
    ) -> None:
        ax = self._canvas.axes
        ax.clear()
        ax.set_facecolor(COLORS["bg"])

        west_probs = [1 - p for p in east_probs]

        ax.plot(iterations, [p * 100 for p in east_probs],
                color=COLORS["east"], linewidth=2, label=east_name)
        ax.plot(iterations, [p * 100 for p in west_probs],
                color=COLORS["west"], linewidth=2, label=west_name)

        # CI band
        if final_ci[0] > 0:
            ax.axhspan(
                final_ci[0] * 100, final_ci[1] * 100,
                alpha=0.1, color=COLORS["east"],
            )

        ax.axhline(y=50, color=COLORS["grid"], linewidth=0.5, linestyle="--")

        ax.set_xlabel("Iterations", fontsize=10)
        ax.set_ylabel("Win Probability (%)", fontsize=10)
        ax.set_title("Probability Convergence", fontsize=11, fontweight="bold")
        ax.legend(fontsize=9)
        ax.set_ylim(0, 100)
        ax.grid(color=COLORS["grid"], linewidth=0.5, alpha=0.5)

        self._canvas.fig.tight_layout()
        self._canvas.draw()


class StandingsProgressionChart(QWidget):
    """
    Line chart showing top wrestlers' win counts progressing day by day
    across the tournament.
    """

    def __init__(self, parent=None, top_n: int = 8):
        super().__init__(parent)
        self._top_n = top_n
        self._canvas = MplCanvas(width=8, height=4, parent=self)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._canvas)

    def update_chart(
        self,
        standings_by_day: dict[int, dict[str, int]],
        wrestler_names: dict[str, str],
        title: str = "Tournament Progression",
    ) -> None:
        """
        Args:
            standings_by_day: {day: {wrestler_id: cumulative_wins}}
            wrestler_names: {wrestler_id: shikona}
        """
        ax = self._canvas.axes
        ax.clear()
        ax.set_facecolor(COLORS["bg"])

        if not standings_by_day:
            self._canvas.draw()
            return

        days = sorted(standings_by_day.keys())

        # Find top N by final day wins
        final_day = max(days)
        final_standings = standings_by_day.get(final_day, {})
        top_wrestlers = sorted(
            final_standings.keys(), key=lambda w: -final_standings.get(w, 0)
        )[: self._top_n]

        colors = (
            RANK_PALETTE * ((len(top_wrestlers) // len(RANK_PALETTE)) + 1)
        )

        for i, wid in enumerate(top_wrestlers):
            wins_series = [standings_by_day.get(d, {}).get(wid, 0) for d in days]
            name = wrestler_names.get(wid, wid[:12])
            ax.plot(
                days, wins_series,
                color=colors[i % len(colors)],
                linewidth=2,
                marker="o",
                markersize=3,
                label=name,
            )

        ax.set_xlabel("Day", fontsize=10)
        ax.set_ylabel("Wins", fontsize=10)
        ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
        ax.set_xticks(range(1, 16))
        ax.legend(fontsize=8, loc="upper left", ncol=2)
        ax.grid(color=COLORS["grid"], linewidth=0.5, alpha=0.5)

        self._canvas.fig.tight_layout()
        self._canvas.draw()
