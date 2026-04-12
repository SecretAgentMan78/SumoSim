"""
SumoSim Modifier Controls Panel

Organized into three collapsible sections:
  - Momentum / Form: weight slider, streak window, per-wrestler overrides
  - Matchup / Style: style matrix weight, auto-classifications, custom matrix
  - Injury / Fatigue: per-wrestler severity, fatigue curve, recovery factor

Changes apply immediately to subsequent simulations.
"""

from __future__ import annotations

import logging
from typing import Optional

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtWidgets import (
    QComboBox,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QScrollArea,
    QSlider,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from data.models import (
    FatigueCurve,
    FightingStyle,
    MomentumState,
    WrestlerProfile,
)

logger = logging.getLogger(__name__)


class LabeledSlider(QWidget):
    """A horizontal slider with a label showing the current value."""

    valueChanged = pyqtSignal(float)

    def __init__(
        self,
        label: str,
        min_val: float = 0.0,
        max_val: float = 1.0,
        default: float = 0.5,
        step: float = 0.01,
        suffix: str = "",
        parent=None,
    ):
        super().__init__(parent)
        self._min = min_val
        self._max = max_val
        self._step = step
        self._suffix = suffix

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self._label = QLabel(label)
        self._label.setMinimumWidth(140)
        layout.addWidget(self._label)

        self._slider = QSlider(Qt.Orientation.Horizontal)
        steps = int((max_val - min_val) / step)
        self._slider.setRange(0, steps)
        self._slider.setValue(int((default - min_val) / step))
        self._slider.valueChanged.connect(self._on_slider)
        layout.addWidget(self._slider, stretch=1)

        self._value_label = QLabel()
        self._value_label.setMinimumWidth(60)
        self._value_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        layout.addWidget(self._value_label)

        self._update_label()

    def value(self) -> float:
        return self._min + self._slider.value() * self._step

    def set_value(self, val: float) -> None:
        self._slider.setValue(int((val - self._min) / self._step))

    def _on_slider(self) -> None:
        self._update_label()
        self.valueChanged.emit(self.value())

    def _update_label(self) -> None:
        v = self.value()
        if self._step >= 1:
            self._value_label.setText(f"{v:.0f}{self._suffix}")
        else:
            self._value_label.setText(f"{v:.2f}{self._suffix}")


class ModifierPanel(QWidget):
    """
    Modifier Controls tab.

    All controls modify the shared modifier state that the bout and
    tournament simulators read from.
    """

    # Emitted when any modifier setting changes
    modifiers_changed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._main_window = parent
        self._roster: list[WrestlerProfile] = []
        self._build_ui()

    def set_roster(self, roster: list[WrestlerProfile]) -> None:
        self._roster = roster
        self._populate_wrestler_tables()

    # ── State access (read by simulators) ──────────────────────────

    def get_momentum_settings(self) -> dict:
        return {
            "weight": self._momentum_weight.value(),
            "streak_window": int(self._streak_window.value()),
        }

    def get_matchup_settings(self) -> dict:
        return {
            "weight": self._matchup_weight.value(),
        }

    def get_injury_fatigue_settings(self) -> dict:
        return {
            "fatigue_curve": self._fatigue_curve_combo.currentData(),
            "recovery_factor": self._recovery_slider.value(),
        }

    def get_wrestler_overrides(self) -> dict:
        """Return per-wrestler override settings."""
        overrides = {}
        for row in range(self._override_table.rowCount()):
            wid_item = self._override_table.item(row, 0)
            if not wid_item:
                continue
            wid = wid_item.data(Qt.ItemDataRole.UserRole)

            momentum_widget = self._override_table.cellWidget(row, 2)
            injury_widget = self._override_table.cellWidget(row, 3)

            override = {}
            if momentum_widget and momentum_widget.currentIndex() > 0:
                override["momentum"] = momentum_widget.currentData()
            if injury_widget:
                override["injury_severity"] = injury_widget.value()

            if override:
                overrides[wid] = override

        return overrides

    def get_style_matrix(self) -> list[list[float]]:
        """Return the 3x3 style interaction matrix values."""
        matrix = []
        for r in range(3):
            row = []
            for c in range(3):
                item = self._style_matrix.item(r, c)
                try:
                    row.append(float(item.text()) if item else 0.0)
                except ValueError:
                    row.append(0.0)
            matrix.append(row)
        return matrix

    def set_style_matrix(self, matrix: list[list[float]]) -> None:
        """Set the 3x3 style interaction matrix values."""
        for r in range(min(3, len(matrix))):
            for c in range(min(3, len(matrix[r]))):
                val = matrix[r][c]
                item = QTableWidgetItem(f"{val:+.1f}")
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                if val > 0:
                    item.setForeground(QColor("#006400"))
                elif val < 0:
                    item.setForeground(QColor("#8B0000"))
                else:
                    item.setForeground(QColor("#333"))
                self._style_matrix.setItem(r, c, item)

    def get_full_state(self) -> dict:
        """Return complete modifier state as a serializable dict for save/load."""
        state = {
            "momentum": self.get_momentum_settings(),
            "matchup": self.get_matchup_settings(),
            "injury_fatigue": {
                "fatigue_curve": self._fatigue_curve_combo.currentData().value,
                "recovery_factor": self._recovery_slider.value(),
            },
            "style_matrix": self.get_style_matrix(),
            "wrestler_overrides": {},
        }

        # Serialize per-wrestler overrides
        for row in range(self._override_table.rowCount()):
            wid_item = self._override_table.item(row, 0)
            if not wid_item:
                continue
            wid = wid_item.data(Qt.ItemDataRole.UserRole)

            style_combo = self._override_table.cellWidget(row, 1)
            momentum_combo = self._override_table.cellWidget(row, 2)
            injury_spin = self._override_table.cellWidget(row, 3)

            entry = {}
            if style_combo:
                entry["style"] = style_combo.currentData().value
            if momentum_combo and momentum_combo.currentIndex() > 0:
                entry["momentum"] = momentum_combo.currentData()
            if injury_spin and injury_spin.value() > 0:
                entry["injury_severity"] = injury_spin.value()

            if entry:
                state["wrestler_overrides"][wid] = entry

        return state

    def set_full_state(self, state: dict) -> None:
        """Restore complete modifier state from a dict (loaded scenario)."""
        from data.models import FatigueCurve

        # Global sliders
        mom = state.get("momentum", {})
        if "weight" in mom:
            self._momentum_weight.set_value(mom["weight"])
        if "streak_window" in mom:
            self._streak_window.set_value(mom["streak_window"])

        matchup = state.get("matchup", {})
        if "weight" in matchup:
            self._matchup_weight.set_value(matchup["weight"])

        inj = state.get("injury_fatigue", {})
        if "recovery_factor" in inj:
            self._recovery_slider.set_value(inj["recovery_factor"])
        if "fatigue_curve" in inj:
            try:
                fc = FatigueCurve(inj["fatigue_curve"])
                idx = self._fatigue_curve_combo.findData(fc)
                if idx >= 0:
                    self._fatigue_curve_combo.setCurrentIndex(idx)
            except ValueError:
                pass

        # Style matrix
        if "style_matrix" in state:
            self.set_style_matrix(state["style_matrix"])

        # Per-wrestler overrides
        overrides = state.get("wrestler_overrides", {})
        for row in range(self._override_table.rowCount()):
            wid_item = self._override_table.item(row, 0)
            if not wid_item:
                continue
            wid = wid_item.data(Qt.ItemDataRole.UserRole)
            entry = overrides.get(wid, {})

            if "style" in entry:
                style_combo = self._override_table.cellWidget(row, 1)
                if style_combo:
                    from data.models import FightingStyle
                    try:
                        fs = FightingStyle(entry["style"])
                        idx = style_combo.findData(fs)
                        if idx >= 0:
                            style_combo.setCurrentIndex(idx)
                    except ValueError:
                        pass

            if "momentum" in entry:
                mom_combo = self._override_table.cellWidget(row, 2)
                if mom_combo:
                    for i in range(mom_combo.count()):
                        if mom_combo.itemData(i) == entry["momentum"]:
                            mom_combo.setCurrentIndex(i)
                            break

            if "injury_severity" in entry:
                inj_spin = self._override_table.cellWidget(row, 3)
                if inj_spin:
                    inj_spin.setValue(entry["injury_severity"])

        self._on_change()

    def apply_injury_notes(self, notes: dict[str, dict]) -> None:
        """Pre-populate injury severity from data (e.g., haru_2026_injury_notes)."""
        for row in range(self._override_table.rowCount()):
            wid_item = self._override_table.item(row, 0)
            if not wid_item:
                continue
            wid = wid_item.data(Qt.ItemDataRole.UserRole)
            if wid in notes:
                inj_widget = self._override_table.cellWidget(row, 3)
                if inj_widget:
                    inj_widget.setValue(notes[wid].get("severity", 0.0))

    # ── UI construction ────────────────────────────────────────────

    def _build_ui(self) -> None:
        # Scrollable container
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(12)

        # ── Momentum section ───────────────────────────────────────
        mom_group = QGroupBox("⚡ Momentum / Form")
        mom_layout = QVBoxLayout(mom_group)

        self._momentum_weight = LabeledSlider(
            "Momentum Weight:", 0.0, 1.0, 0.5, 0.05
        )
        self._momentum_weight.valueChanged.connect(self._on_change)
        mom_layout.addWidget(self._momentum_weight)

        self._streak_window = LabeledSlider(
            "Streak Window:", 3, 15, 5, 1, " bouts"
        )
        self._streak_window.valueChanged.connect(self._on_change)
        mom_layout.addWidget(self._streak_window)

        mom_help = QLabel(
            "Controls how strongly recent form influences ratings. "
            "Higher weight = larger swings from hot/cold streaks."
        )
        mom_help.setWordWrap(True)
        mom_help.setStyleSheet("color: #888; font-size: 11px; padding: 4px 0;")
        mom_layout.addWidget(mom_help)

        layout.addWidget(mom_group)

        # ── Matchup / Style section ────────────────────────────────
        match_group = QGroupBox("🤼 Matchup / Style")
        match_layout = QVBoxLayout(match_group)

        self._matchup_weight = LabeledSlider(
            "Matchup Weight:", 0.0, 1.0, 0.3, 0.05
        )
        self._matchup_weight.valueChanged.connect(self._on_change)
        match_layout.addWidget(self._matchup_weight)

        # Style interaction display
        matrix_label = QLabel("Style Interaction Matrix:")
        matrix_label.setFont(QFont("Outfit", 10, QFont.Weight.Bold))
        match_layout.addWidget(matrix_label)

        self._style_matrix = QTableWidget(3, 3)
        self._style_matrix.setHorizontalHeaderLabels(["vs Oshi", "vs Yotsu", "vs Hybrid"])
        self._style_matrix.setVerticalHeaderLabels(["Oshi", "Yotsu", "Hybrid"])
        self._style_matrix.setMaximumHeight(120)
        header = self._style_matrix.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        # Default matrix values
        defaults = [
            [0.0, -0.3, -0.1],    # Oshi vs...
            [0.3, 0.0, 0.1],      # Yotsu vs...
            [0.1, -0.1, 0.0],     # Hybrid vs...
        ]
        for r in range(3):
            for c in range(3):
                item = QTableWidgetItem(f"{defaults[r][c]:+.1f}")
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                if defaults[r][c] > 0:
                    item.setForeground(QColor("#006400"))
                elif defaults[r][c] < 0:
                    item.setForeground(QColor("#8B0000"))
                self._style_matrix.setItem(r, c, item)

        match_layout.addWidget(self._style_matrix)

        match_help = QLabel(
            "Positive = attacker advantage. Oshi (pushing) wrestlers struggle "
            "against Yotsu (belt) grapplers. Hybrid wrestlers have slight edges."
        )
        match_help.setWordWrap(True)
        match_help.setStyleSheet("color: #888; font-size: 11px; padding: 4px 0;")
        match_layout.addWidget(match_help)

        layout.addWidget(match_group)

        # ── Injury / Fatigue section ───────────────────────────────
        inj_group = QGroupBox("🩹 Injury / Fatigue")
        inj_layout = QVBoxLayout(inj_group)

        curve_row = QHBoxLayout()
        curve_row.addWidget(QLabel("Fatigue Curve:"))
        self._fatigue_curve_combo = QComboBox()
        for fc in FatigueCurve:
            self._fatigue_curve_combo.addItem(fc.value.replace("_", " ").title(), fc)
        self._fatigue_curve_combo.setCurrentIndex(2)  # S-curve default
        self._fatigue_curve_combo.currentIndexChanged.connect(self._on_change)
        curve_row.addWidget(self._fatigue_curve_combo)
        curve_row.addStretch()
        inj_layout.addLayout(curve_row)

        self._recovery_slider = LabeledSlider(
            "Recovery Factor:", 0.0, 1.0, 0.6, 0.05
        )
        self._recovery_slider.valueChanged.connect(self._on_change)
        inj_layout.addWidget(self._recovery_slider)

        inj_help = QLabel(
            "Fatigue accumulates over the 15-day tournament. Heavier wrestlers "
            "fatigue faster. Recovery factor determines how much a wrestler "
            "recovers between days (1.0 = full daily recovery)."
        )
        inj_help.setWordWrap(True)
        inj_help.setStyleSheet("color: #888; font-size: 11px; padding: 4px 0;")
        inj_layout.addWidget(inj_help)

        layout.addWidget(inj_group)

        # ── Per-wrestler overrides ─────────────────────────────────
        override_group = QGroupBox("👤 Per-Wrestler Overrides")
        override_layout = QVBoxLayout(override_group)

        self._override_table = QTableWidget()
        self._override_table.setColumnCount(4)
        self._override_table.setHorizontalHeaderLabels(
            ["Wrestler", "Style", "Momentum", "Injury"]
        )
        oh = self._override_table.horizontalHeader()
        oh.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self._override_table.setAlternatingRowColors(True)
        self._override_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._override_table.setStyleSheet("alternate-background-color: #F0EDE4;")
        override_layout.addWidget(self._override_table)

        layout.addWidget(override_group)

        # ── Reset button ───────────────────────────────────────────
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        reset_btn = QPushButton("⟲  Reset All to Defaults")
        reset_btn.clicked.connect(self._on_reset_defaults)
        btn_row.addWidget(reset_btn)
        layout.addLayout(btn_row)

        layout.addStretch()

        scroll.setWidget(container)

        # Wrap scroll in the panel's own layout
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(scroll)

    # ── Per-wrestler override table ────────────────────────────────

    def _populate_wrestler_tables(self) -> None:
        self._override_table.setRowCount(len(self._roster))

        for row, w in enumerate(self._roster):
            # Name
            name_item = QTableWidgetItem(f"{w.full_rank} — {w.shikona}")
            name_item.setData(Qt.ItemDataRole.UserRole, w.wrestler_id)
            self._override_table.setItem(row, 0, name_item)

            # Style combo
            style_combo = QComboBox()
            for fs in FightingStyle:
                style_combo.addItem(fs.value.title(), fs)
            idx = style_combo.findData(w.fighting_style)
            if idx >= 0:
                style_combo.setCurrentIndex(idx)
            style_combo.currentIndexChanged.connect(self._on_change)
            self._override_table.setCellWidget(row, 1, style_combo)

            # Momentum override
            mom_combo = QComboBox()
            mom_combo.addItem("Auto", None)
            for ms in MomentumState:
                mom_combo.addItem(ms.value.title(), ms)
            mom_combo.currentIndexChanged.connect(self._on_change)
            self._override_table.setCellWidget(row, 2, mom_combo)

            # Injury severity
            inj_spin = QDoubleSpinBox()
            inj_spin.setRange(0.0, 1.0)
            inj_spin.setSingleStep(0.1)
            inj_spin.setValue(0.0)
            inj_spin.setDecimals(2)
            inj_spin.valueChanged.connect(self._on_change)
            self._override_table.setCellWidget(row, 3, inj_spin)

    # ── Change handling ────────────────────────────────────────────

    def _on_change(self, *args) -> None:
        self.modifiers_changed.emit()
        self._update_summary()

    def _update_summary(self) -> None:
        """Push a summary string to the main window status bar."""
        parts = []
        mw = self._momentum_weight.value()
        if mw > 0:
            parts.append(f"Mom:{mw:.1f}")
        maw = self._matchup_weight.value()
        if maw > 0:
            parts.append(f"Match:{maw:.1f}")

        overrides = self.get_wrestler_overrides()
        if overrides:
            injured = sum(
                1 for v in overrides.values()
                if v.get("injury_severity", 0) > 0
            )
            if injured:
                parts.append(f"Injured:{injured}")

        summary = " | ".join(parts) if parts else "No modifiers active"

        if self._main_window and hasattr(self._main_window, "update_modifier_summary"):
            self._main_window.update_modifier_summary(summary)

    def _on_reset_defaults(self) -> None:
        """Reset all sliders and overrides to defaults."""
        self._momentum_weight.set_value(0.5)
        self._streak_window.set_value(5)
        self._matchup_weight.set_value(0.3)
        self._recovery_slider.set_value(0.6)
        self._fatigue_curve_combo.setCurrentIndex(2)

        # Reset override table
        for row in range(self._override_table.rowCount()):
            mom_combo = self._override_table.cellWidget(row, 2)
            if mom_combo:
                mom_combo.setCurrentIndex(0)
            inj_spin = self._override_table.cellWidget(row, 3)
            if inj_spin:
                inj_spin.setValue(0.0)

        self._on_change()
