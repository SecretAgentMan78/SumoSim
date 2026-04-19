"""
SumoSim Bout Simulator Panel

Primary interaction surface:
  - East/West wrestler dropdown selection
  - Head-to-head historical record display
  - Animated win probability bar
  - Active modifiers impact summary
  - Confidence interval display
  - "Simulate Bout" button
"""

from __future__ import annotations

import logging
from typing import Optional, Sequence

from PyQt6.QtCore import Qt, QThread, pyqtSignal, QPropertyAnimation, QEasingCurve
from PyQt6.QtGui import QFont, QPainter, QColor, QBrush, QPixmap
from PyQt6.QtWidgets import (
    QComboBox,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from data.models import BoutResult, Rank, WrestlerProfile, WrestlerRating

logger = logging.getLogger(__name__)


# ── Probability bar widget ─────────────────────────────────────────

class WinProbabilityBar(QWidget):
    """
    A horizontal bar showing east vs west win probability.
    East in dark red on the left, West in dark blue on the right.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._east_prob = 0.5
        self._ci_low = 0.0
        self._ci_high = 1.0
        self.setMinimumHeight(64)
        self.setMaximumHeight(72)

    def set_probability(
        self, east_prob: float, ci_low: float = 0.0, ci_high: float = 1.0
    ) -> None:
        self._east_prob = max(0.0, min(1.0, east_prob))
        self._ci_low = ci_low
        self._ci_high = ci_high
        self.update()

    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        w = self.width()
        h = self.height()
        split = int(w * self._east_prob)

        # East side (dark red)
        painter.setBrush(QBrush(QColor("#8B0000")))
        painter.setPen(Qt.PenStyle.NoPen)
        painter.drawRoundedRect(0, 0, split, h, 6, 6)

        # West side (dark blue)
        painter.setBrush(QBrush(QColor("#00008B")))
        painter.drawRoundedRect(split, 0, w - split, h, 6, 6)

        # CI markers
        painter.setPen(QColor("#FFFFFF80"))
        ci_left = int(w * self._ci_low)
        ci_right = int(w * self._ci_high)
        painter.drawLine(ci_left, 4, ci_left, h - 4)
        painter.drawLine(ci_right, 4, ci_right, h - 4)

        # Percentage labels
        painter.setPen(QColor("white"))
        font = QFont("Outfit", 28, QFont.Weight.Bold)
        painter.setFont(font)

        east_pct = f"{self._east_prob * 100:.1f}%"
        west_pct = f"{(1 - self._east_prob) * 100:.1f}%"

        if split > 60:
            painter.drawText(8, 0, split - 16, h,
                           Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft,
                           east_pct)
        if w - split > 60:
            painter.drawText(split + 8, 0, w - split - 16, h,
                           Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight,
                           west_pct)

        painter.end()


# ── Simulation worker thread ───────────────────────────────────────

class SimulationWorker(QThread):
    """Runs Monte Carlo bout simulation off the main thread."""

    finished = pyqtSignal(object)  # BoutResult
    progress = pyqtSignal(int, int)  # current, total

    def __init__(self, bout_simulator, east, west, context,
                 tournament_records, bout_records):
        super().__init__()
        self._sim = bout_simulator
        self._east = east
        self._west = west
        self._context = context
        self._tournament_records = tournament_records
        self._bout_records = bout_records

    def run(self):
        try:
            result = self._sim.simulate(
                self._east, self._west,
                context=self._context,
                east_tournament_history=self._tournament_records.get(self._east.wrestler_id, []),
                west_tournament_history=self._tournament_records.get(self._west.wrestler_id, []),
                bout_history=self._bout_records,
            )
            self.finished.emit(result)
        except Exception as e:
            logger.error(f"Simulation error: {e}")
            self.finished.emit(None)


# ── Main Bout Panel ────────────────────────────────────────────────

class BoutPanel(QWidget):
    """
    Bout Simulator tab.

    Layout:
        ┌───────────────────────────────────────────┐
        │  [East Dropdown ▼]   VS   [West Dropdown ▼] │
        │                                             │
        │  ┌─ Head-to-Head ──────────────────────┐   │
        │  │  Record: 5-3  |  Last: Onosato (D14) │   │
        │  └─────────────────────────────────────┘   │
        │                                             │
        │  ┌─ Modifier Impact ───────────────────┐   │
        │  │  Momentum:  +25 (east)  -10 (west)  │   │
        │  │  Matchup:   +15 (east)   0  (west)  │   │
        │  │  Injury:     0  (east)  -40 (west)  │   │
        │  └─────────────────────────────────────┘   │
        │                                             │
        │           [ 🎲 Simulate Bout ]              │
        │                                             │
        │  ┌─ Bout Simulation Outcome ─────────────┐ │
        │  │  Winner: Hoshoryu (East)               │ │
        │  │  Winning Kimarite: yorikiri (42%)      │ │
        │  │  ██████████ 63.2% ░░░░░░░ 36.8%       │ │
        │  │  95% CI: [58.1% – 68.3%]               │ │
        │  └─────────────────────────────────────┘   │
        └───────────────────────────────────────────┘
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._roster: list[WrestlerProfile] = []
        self._tournament_histories: dict = {}
        self._bout_records: list = []
        self._h2h_index: dict = {}
        self._main_window = parent
        self._last_result: Optional[BoutResult] = None
        self._worker: Optional[SimulationWorker] = None
        self._build_ui()

    def set_roster(self, roster: list[WrestlerProfile], tournament_histories: dict | None = None, bout_records: list | None = None) -> None:
        self._roster = roster
        self._tournament_histories = tournament_histories or {}
        self._bout_records = bout_records or []
        # Build H2H index: (id_a, id_b) -> {a_wins, b_wins, total, last_bouts}
        self._h2h_index = self._build_h2h_index()
        print(f"[BoutPanel] Received {len(self._bout_records)} bout records, "
              f"built {len(self._h2h_index)} H2H index entries")
        self._populate_dropdowns()

    def preselect_wrestler(self, wrestler: WrestlerProfile) -> None:
        """Pre-select a wrestler in the east dropdown (from sidebar click)."""
        idx = self._east_combo.findData(wrestler.wrestler_id)
        if idx >= 0:
            self._east_combo.setCurrentIndex(idx)

    def _build_h2h_index(self) -> dict:
        """Build a lookup index from bout_records: (id_a, id_b) -> summary dict.

        Keys are always (min_id, max_id) sorted alphabetically for consistency.
        Tracks win counts, kimarite frequencies per winner, and recent bouts.
        """
        index: dict[tuple[str, str], dict] = {}
        for br in self._bout_records:
            # Canonical key — sorted alphabetically
            key = tuple(sorted([br.east_id, br.west_id]))
            if key not in index:
                index[key] = {
                    "wins": {},            # wrestler_id -> win count
                    "east_kimarite": {},   # kimarite Counter for key[0]'s wins
                    "west_kimarite": {},   # kimarite Counter for key[1]'s wins
                    "total": 0,
                    "last_bouts": [],      # most recent bouts (up to 5)
                }
            entry = index[key]
            entry["total"] += 1
            entry["wins"][br.winner_id] = entry["wins"].get(br.winner_id, 0) + 1

            # Track kimarite by which side of the key won
            if br.kimarite:
                if br.winner_id == key[0]:
                    kim_dict = entry["east_kimarite"]
                else:
                    kim_dict = entry["west_kimarite"]
                kim_dict[br.kimarite] = kim_dict.get(br.kimarite, 0) + 1

            entry["last_bouts"].append(br)

        # Sort last_bouts by basho descending, keep only 5 most recent
        for entry in index.values():
            entry["last_bouts"].sort(
                key=lambda b: (b.basho_id, b.day), reverse=True
            )
            entry["last_bouts"] = entry["last_bouts"][:5]

        return index

    def _get_h2h_text(self, east_id: str, west_id: str,
                      east_name: str, west_name: str) -> str:
        """Format the H2H record between two wrestlers as display text."""
        key = tuple(sorted([east_id, west_id]))
        entry = self._h2h_index.get(key)

        if not entry or entry["total"] == 0:
            return (
                f"<b>{east_name}</b> vs <b>{west_name}</b>: "
                f"No previous meetings on record"
            )

        e_wins = entry["wins"].get(east_id, 0)
        w_wins = entry["wins"].get(west_id, 0)
        total = entry["total"]

        # Color the leader's count
        if e_wins > w_wins:
            record = f"<span style='color:#8B0000'><b>{e_wins}</b></span>-{w_wins}"
        elif w_wins > e_wins:
            record = f"{e_wins}-<span style='color:#00008B'><b>{w_wins}</b></span>"
        else:
            record = f"{e_wins}-{w_wins}"

        result = f"<b>{east_name}</b>  {record}  <b>{west_name}</b>"
        result += f"  ({total} career meetings)"

        # Show kimarite breakdown per side
        # The index key is alphabetically sorted — map back to east/west
        if east_id == key[0]:
            e_kim = entry.get("east_kimarite", {})
            w_kim = entry.get("west_kimarite", {})
        else:
            e_kim = entry.get("west_kimarite", {})
            w_kim = entry.get("east_kimarite", {})
        kim_parts = []
        if e_kim:
            top_e = sorted(e_kim.items(), key=lambda x: -x[1])[:2]
            e_str = ", ".join(f"{k} ({v})" for k, v in top_e)
            kim_parts.append(f"{east_name}: {e_str}")
        if w_kim:
            top_w = sorted(w_kim.items(), key=lambda x: -x[1])[:2]
            w_str = ", ".join(f"{k} ({v})" for k, v in top_w)
            kim_parts.append(f"{west_name}: {w_str}")
        if kim_parts:
            result += "<br><small>Winning kimarite — " + " │ ".join(kim_parts) + "</small>"

        # Show last few results
        recent = entry["last_bouts"][:3]
        if recent:
            recent_strs = []
            for b in recent:
                w = east_name if b.winner_id == east_id else west_name
                kim = f" ({b.kimarite})" if b.kimarite else ""
                recent_strs.append(f"{b.basho_id} D{b.day}: {w}{kim}")
            result += "<br><small>Recent: " + " │ ".join(recent_strs) + "</small>"

        return result

    # ── UI construction ────────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(16)

        # ── Wrestler selection row ─────────────────────────────────
        selection_row = QHBoxLayout()

        # East
        east_box = QVBoxLayout()
        east_box.setAlignment(Qt.AlignmentFlag.AlignCenter)
        east_label = QLabel("East (東)")
        east_label.setFont(QFont("Outfit", 22, QFont.Weight.Bold))
        east_label.setStyleSheet("color: #8B0000;")
        east_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        east_box.addWidget(east_label)

        self._east_photo = QLabel()
        self._east_photo.setFixedSize(100, 100)
        self._east_photo.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._east_photo.setStyleSheet(
            "border: 2px solid #8B0000; border-radius: 6px; background-color: #F0EBE0;"
        )
        east_box.addWidget(self._east_photo, alignment=Qt.AlignmentFlag.AlignCenter)

        self._east_combo = QComboBox()
        self._east_combo.setMinimumWidth(300)
        self._east_combo.setFont(QFont("Outfit", 20))
        self._east_combo.currentIndexChanged.connect(self._on_selection_changed)
        east_box.addWidget(self._east_combo)

        self._east_info = QLabel("")
        self._east_info.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._east_info.setFont(QFont("Outfit", 20))
        self._east_info.setStyleSheet("color: #666;")
        east_box.addWidget(self._east_info)

        selection_row.addLayout(east_box)

        # VS label
        vs_label = QLabel("VS")
        vs_label.setFont(QFont("Outfit", 40, QFont.Weight.Bold))
        vs_label.setStyleSheet("color: #B8860B;")
        vs_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        vs_label.setMinimumWidth(80)
        selection_row.addWidget(vs_label)

        # West
        west_box = QVBoxLayout()
        west_box.setAlignment(Qt.AlignmentFlag.AlignCenter)
        west_label = QLabel("West (西)")
        west_label.setFont(QFont("Outfit", 22, QFont.Weight.Bold))
        west_label.setStyleSheet("color: #00008B;")
        west_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        west_box.addWidget(west_label)

        self._west_photo = QLabel()
        self._west_photo.setFixedSize(100, 100)
        self._west_photo.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._west_photo.setStyleSheet(
            "border: 2px solid #00008B; border-radius: 6px; background-color: #F0EBE0;"
        )
        west_box.addWidget(self._west_photo, alignment=Qt.AlignmentFlag.AlignCenter)

        self._west_combo = QComboBox()
        self._west_combo.setMinimumWidth(300)
        self._west_combo.setFont(QFont("Outfit", 20))
        self._west_combo.currentIndexChanged.connect(self._on_selection_changed)
        west_box.addWidget(self._west_combo)

        self._west_info = QLabel("")
        self._west_info.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._west_info.setFont(QFont("Outfit", 20))
        self._west_info.setStyleSheet("color: #666;")
        west_box.addWidget(self._west_info)

        selection_row.addLayout(west_box)
        layout.addLayout(selection_row)

        # ── Head-to-head group ─────────────────────────────────────
        h2h_group = QGroupBox("Head-to-Head Record")
        h2h_group.setFont(QFont("Outfit", 20, QFont.Weight.Bold))
        h2h_layout = QVBoxLayout(h2h_group)
        self._h2h_label = QLabel("Select two wrestlers to see their history")
        self._h2h_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._h2h_label.setTextFormat(Qt.TextFormat.RichText)
        self._h2h_label.setWordWrap(True)
        self._h2h_label.setFont(QFont("Outfit", 22))
        h2h_layout.addWidget(self._h2h_label)
        layout.addWidget(h2h_group)

        # ── Modifier impact group ──────────────────────────────────
        mod_group = QGroupBox("Modifier Impact")
        mod_group.setFont(QFont("Outfit", 20, QFont.Weight.Bold))
        mod_layout = QVBoxLayout(mod_group)

        self._modifier_grid = QGridLayout()
        self._modifier_grid.setColumnStretch(0, 2)
        self._modifier_grid.setColumnStretch(1, 1)
        self._modifier_grid.setColumnStretch(2, 1)

        headers = ["Modifier", "East Δ", "West Δ"]
        for col, hdr in enumerate(headers):
            lbl = QLabel(hdr)
            lbl.setFont(QFont("Outfit", 20, QFont.Weight.Bold))
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self._modifier_grid.addWidget(lbl, 0, col)

        self._mod_rows: list[tuple[QLabel, QLabel, QLabel]] = []
        for i, name in enumerate(["Momentum", "Matchup", "Injury/Fatigue"]):
            name_lbl = QLabel(name)
            name_lbl.setFont(QFont("Outfit", 20))
            east_lbl = QLabel("—")
            east_lbl.setFont(QFont("Outfit", 20))
            east_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            west_lbl = QLabel("—")
            west_lbl.setFont(QFont("Outfit", 20))
            west_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self._modifier_grid.addWidget(name_lbl, i + 1, 0)
            self._modifier_grid.addWidget(east_lbl, i + 1, 1)
            self._modifier_grid.addWidget(west_lbl, i + 1, 2)
            self._mod_rows.append((name_lbl, east_lbl, west_lbl))

        mod_layout.addLayout(self._modifier_grid)
        layout.addWidget(mod_group)

        # ── Simulate button ────────────────────────────────────────
        btn_row = QHBoxLayout()
        btn_row.addStretch()

        self._simulate_btn = QPushButton("🎲  Simulate Bout")
        self._simulate_btn.setObjectName("primary")
        self._simulate_btn.setMinimumWidth(280)
        self._simulate_btn.setMinimumHeight(50)
        self._simulate_btn.setFont(QFont("Outfit", 24))
        self._simulate_btn.clicked.connect(self._on_simulate)
        btn_row.addWidget(self._simulate_btn)

        btn_row.addStretch()
        layout.addLayout(btn_row)

        # ── Bout Simulation Outcome group ──────────────────────────
        outcome_group = QGroupBox("Bout Simulation Outcome")
        outcome_group.setFont(QFont("Outfit", 20, QFont.Weight.Bold))
        outcome_layout = QVBoxLayout(outcome_group)
        outcome_layout.setSpacing(10)

        # Winner row
        winner_row = QHBoxLayout()
        winner_label = QLabel("Simulation Winner:")
        winner_label.setFont(QFont("Outfit", 22))
        winner_row.addWidget(winner_label)
        self._outcome_winner = QLabel("—")
        self._outcome_winner.setFont(QFont("Outfit", 26, QFont.Weight.Bold))
        self._outcome_winner.setAlignment(Qt.AlignmentFlag.AlignLeft)
        winner_row.addWidget(self._outcome_winner)
        winner_row.addStretch()
        outcome_layout.addLayout(winner_row)

        # Kimarite row
        kim_row = QHBoxLayout()
        kim_label = QLabel("Winning Kimarite:")
        kim_label.setFont(QFont("Outfit", 22))
        kim_row.addWidget(kim_label)
        self._outcome_kimarite = QLabel("—")
        self._outcome_kimarite.setFont(QFont("Outfit", 26, QFont.Weight.Bold))
        self._outcome_kimarite.setAlignment(Qt.AlignmentFlag.AlignLeft)
        kim_row.addWidget(self._outcome_kimarite)
        kim_row.addStretch()
        outcome_layout.addLayout(kim_row)

        # Confidence section header
        conf_header = QLabel("Simulation Confidence")
        conf_header.setFont(QFont("Outfit", 20, QFont.Weight.Bold))
        conf_header.setStyleSheet("color: #666; margin-top: 6px;")
        outcome_layout.addWidget(conf_header)

        # Probability bar
        self._prob_bar = WinProbabilityBar()
        self._prob_bar.setMinimumHeight(64)
        self._prob_bar.setMaximumHeight(72)
        outcome_layout.addWidget(self._prob_bar)

        # CI label
        self._ci_label = QLabel("95% CI: —")
        self._ci_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._ci_label.setFont(QFont("Outfit", 20))
        self._ci_label.setStyleSheet("color: #666;")
        outcome_layout.addWidget(self._ci_label)

        # Initially hidden until first simulation
        outcome_group.setVisible(False)
        self._outcome_group = outcome_group
        layout.addWidget(outcome_group)

        layout.addStretch()

    # ── Dropdown population ────────────────────────────────────────

    def _populate_dropdowns(self) -> None:
        for combo in (self._east_combo, self._west_combo):
            combo.blockSignals(True)
            combo.clear()
            combo.addItem("— Select Wrestler —", None)
            for w in self._roster:
                label = f"{w.full_rank} — {w.shikona}"
                combo.addItem(label, w.wrestler_id)
            combo.blockSignals(False)

        # Default: first two in roster if available
        if len(self._roster) >= 2:
            self._east_combo.setCurrentIndex(1)
            self._west_combo.setCurrentIndex(2)

    def _get_selected_east(self) -> Optional[WrestlerProfile]:
        wid = self._east_combo.currentData()
        if not wid:
            return None
        return next((w for w in self._roster if w.wrestler_id == wid), None)

    def _get_selected_west(self) -> Optional[WrestlerProfile]:
        wid = self._west_combo.currentData()
        if not wid:
            return None
        return next((w for w in self._roster if w.wrestler_id == wid), None)

    # ── Selection changed ──────────────────────────────────────────

    def _on_selection_changed(self) -> None:
        east = self._get_selected_east()
        west = self._get_selected_west()

        if east:
            info_parts = [east.heya]
            if east.height_cm:
                info_parts.append(f"{east.height_cm:.0f}cm")
            if east.weight_kg:
                info_parts.append(f"{east.weight_kg:.0f}kg")
            self._east_info.setText("  |  ".join(info_parts))
            self._set_wrestler_photo(self._east_photo, east.wrestler_id, east.shikona)
        else:
            self._east_info.setText("")
            self._east_photo.clear()

        if west:
            info_parts = [west.heya]
            if west.height_cm:
                info_parts.append(f"{west.height_cm:.0f}cm")
            if west.weight_kg:
                info_parts.append(f"{west.weight_kg:.0f}kg")
            self._west_info.setText("  |  ".join(info_parts))
            self._set_wrestler_photo(self._west_photo, west.wrestler_id, west.shikona)
        else:
            self._west_info.setText("")
            self._west_photo.clear()

        if east and west and east.wrestler_id != west.wrestler_id:
            self._simulate_btn.setEnabled(True)
            if self._h2h_index:
                self._h2h_label.setText(
                    self._get_h2h_text(
                        east.wrestler_id, west.wrestler_id,
                        east.shikona, west.shikona
                    )
                )
            else:
                self._h2h_label.setText(
                    f"<b>{east.shikona}</b> vs <b>{west.shikona}</b>: "
                    f"H2H data not loaded — run tools/scrape_h2h.py"
                )
        else:
            self._simulate_btn.setEnabled(east is not None and west is not None)

    def _set_wrestler_photo(self, label: QLabel, wrestler_id: str, shikona: str = "") -> None:
        """Load and display a wrestler's portrait in the given QLabel."""
        from gui.main_window import _rikishi_image_path
        img_path = _rikishi_image_path(wrestler_id, shikona)
        if img_path:
            pixmap = QPixmap(img_path)
            # Scale width to 96, keep aspect ratio, then take top 96px
            pixmap = pixmap.scaledToWidth(
                96, Qt.TransformationMode.SmoothTransformation
            )
            if pixmap.height() > 96:
                pixmap = pixmap.copy(0, 0, 96, 96)
            label.setPixmap(pixmap)
        else:
            label.setText("No photo")
            label.setStyleSheet(
                label.styleSheet() + " color: #999; font-size: 11px;"
            )

    # ── Simulation ─────────────────────────────────────────────────

    def _get_modifier_panel(self):
        """Reach the ModifierPanel via the main window."""
        mw = self._main_window
        if mw and hasattr(mw, "_modifier_panel"):
            return mw._modifier_panel
        return None

    def _on_simulate(self) -> None:
        east = self._get_selected_east()
        west = self._get_selected_west()
        if not east or not west:
            return
        if east.wrestler_id == west.wrestler_id:
            return

        self._simulate_btn.setEnabled(False)
        self._simulate_btn.setText("Simulating…")

        from engine.bout_simulator import BoutSimulator
        from modifiers.base import BoutContext
        from modifiers.momentum import MomentumModifier
        from modifiers.matchup import MatchupModifier
        from modifiers.injury_fatigue import InjuryFatigueModifier

        # ── Read modifier settings from the panel ──────────────────
        panel = self._get_modifier_panel()
        modifiers = []
        mod_results = []  # Track individual modifier outputs for display

        if panel:
            # Momentum
            mom_settings = panel.get_momentum_settings()
            mom_mod = MomentumModifier(
                weight=mom_settings["weight"],
                streak_window=mom_settings["streak_window"],
            )
            modifiers.append(mom_mod)

            # Matchup / Style
            match_settings = panel.get_matchup_settings()
            match_mod = MatchupModifier(weight=match_settings["weight"])
            # Apply custom style matrix if set
            try:
                matrix = panel.get_style_matrix()
                if matrix:
                    from data.models import FightingStyle
                    styles = [FightingStyle.OSHI, FightingStyle.YOTSU, FightingStyle.HYBRID]
                    for r, row in enumerate(matrix):
                        for c, val in enumerate(row):
                            if val != 0.0:
                                match_mod.set_interaction(
                                    styles[r].value, styles[c].value, val
                                )
            except Exception:
                pass
            modifiers.append(match_mod)

            # Injury / Fatigue
            inj_settings = panel.get_injury_fatigue_settings()
            inj_mod = InjuryFatigueModifier()
            modifiers.append(inj_mod)

            # Per-wrestler overrides
            overrides = panel.get_wrestler_overrides()
        else:
            overrides = {}

        # ── Build BoutContext with overrides ────────────────────────
        context = BoutContext(east=east, west=west, day=1)

        east_ov = overrides.get(east.wrestler_id, {})
        west_ov = overrides.get(west.wrestler_id, {})

        # Momentum overrides
        context.east_momentum_override = east_ov.get("momentum")
        context.west_momentum_override = west_ov.get("momentum")

        # Injury severity
        context.east_injury_severity = east_ov.get("injury_severity", 0.0)
        context.west_injury_severity = west_ov.get("injury_severity", 0.0)

        # Style overrides
        context.east_style_override = east_ov.get("style")
        context.west_style_override = west_ov.get("style")

        # ── Compute individual modifier results for display ────────
        for mod in modifiers:
            if mod.enabled:
                try:
                    mr = mod.compute(context)
                    mod_results.append((mod.name, mr))
                except Exception as e:
                    logger.debug(f"Modifier {mod.name} compute failed: {e}")

        # ── Run simulation ─────────────────────────────────────────
        sim = BoutSimulator(modifiers=modifiers)

        # Get tournament history for both wrestlers
        east_history = self._tournament_histories.get(east.wrestler_id, [])
        west_history = self._tournament_histories.get(west.wrestler_id, [])

        result = sim.simulate(
            east, west,
            context=context,
            east_tournament_history=east_history,
            west_tournament_history=west_history,
            bout_history=self._bout_records,
            day=1,
        )

        # ── Predict kimarite ───────────────────────────────────────
        kim_confidence = 0.0
        try:
            from engine.kimarite_predictor import KimaritePredictor
            predictor = KimaritePredictor(self._bout_records, self._roster)
            # Sample from weighted distribution using the actual winner
            winner_id = result.winner_id
            loser_id = result.loser_id
            kim, kim_confidence = predictor.sample_for_winner(winner_id, loser_id)
            result.predicted_kimarite = kim
        except Exception as e:
            logger.debug(f"Kimarite prediction failed: {e}")

        self._display_result(result, east, west, mod_results, kim_confidence)

        self._simulate_btn.setEnabled(True)
        self._simulate_btn.setText("🎲  Simulate Bout")

    def _display_result(
        self, result: BoutResult, east: WrestlerProfile, west: WrestlerProfile,
        mod_results: list | None = None,
        kim_confidence: float = 0.0,
    ) -> None:
        self._last_result = result

        # ── Show the outcome group ─────────────────────────────────
        self._outcome_group.setVisible(True)

        # Winner
        is_east = result.winner_id == east.wrestler_id
        winner = east if is_east else west
        side = "East" if is_east else "West"
        color = "#8B0000" if is_east else "#00008B"
        self._outcome_winner.setText(f"{winner.shikona} ({side})")
        self._outcome_winner.setStyleSheet(f"color: {color};")

        # Kimarite
        if result.predicted_kimarite:
            kim_text = result.predicted_kimarite
            if kim_confidence > 0:
                kim_text += f"  ({kim_confidence * 100:.0f}% likely)"
            self._outcome_kimarite.setText(kim_text)
            self._outcome_kimarite.setStyleSheet(f"color: {color};")
        else:
            self._outcome_kimarite.setText("—")
            self._outcome_kimarite.setStyleSheet("color: #666;")

        # Probability bar
        self._prob_bar.set_probability(
            result.east_win_probability,
            ci_low=result.confidence_interval_95[0],
            ci_high=result.confidence_interval_95[1],
        )

        # CI label
        ci_lo = result.confidence_interval_95[0] * 100
        ci_hi = result.confidence_interval_95[1] * 100
        self._ci_label.setText(f"95% CI: [{ci_lo:.1f}% – {ci_hi:.1f}%]")

        # H2H update
        if self._h2h_index:
            h2h_text = self._get_h2h_text(
                east.wrestler_id, west.wrestler_id,
                east.shikona, west.shikona
            )
            self._h2h_label.setText(h2h_text)
        else:
            self._h2h_label.setText(
                f"<b>{east.shikona}</b> vs <b>{west.shikona}</b>"
            )

        # ── Modifier impact rows ───────────────────────────────────
        mod_name_map = {"Momentum": 0, "Matchup": 1, "Injury": 2,
                        "Momentum / Form": 0, "Matchup / Style": 1,
                        "Injury / Fatigue": 2}

        for _, east_lbl, west_lbl in self._mod_rows:
            east_lbl.setText("—")
            east_lbl.setStyleSheet("color: #666;")
            west_lbl.setText("—")
            west_lbl.setStyleSheet("color: #666;")

        if mod_results:
            for mod_name, mr in mod_results:
                row_idx = mod_name_map.get(mod_name)
                if row_idx is None:
                    for key, idx in mod_name_map.items():
                        if key.lower() in mod_name.lower():
                            row_idx = idx
                            break
                if row_idx is not None and row_idx < len(self._mod_rows):
                    _, east_lbl, west_lbl = self._mod_rows[row_idx]

                    e_adj = mr.east_adjustment
                    if e_adj != 0:
                        adj_color = "#006400" if e_adj > 0 else "#8B0000"
                        east_lbl.setText(f"{e_adj:+.0f}")
                        east_lbl.setStyleSheet(f"color: {adj_color}; font-weight: bold;")
                    else:
                        east_lbl.setText("0")
                        east_lbl.setStyleSheet("color: #666;")

                    w_adj = mr.west_adjustment
                    if w_adj != 0:
                        adj_color = "#006400" if w_adj > 0 else "#8B0000"
                        west_lbl.setText(f"{w_adj:+.0f}")
                        west_lbl.setStyleSheet(f"color: {adj_color}; font-weight: bold;")
                    else:
                        west_lbl.setText("0")
                        west_lbl.setStyleSheet("color: #666;")
