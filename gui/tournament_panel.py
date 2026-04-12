"""
SumoSim Tournament Simulator Panel

Features:
  - Tournament / basho selector
  - Day-by-day results table (scrollable grid)
  - Running leaderboard that updates per day
  - Yusho probability bar chart (matplotlib embedded)
  - Simulation speed controls (step / run all)
"""

from __future__ import annotations

import logging
from typing import Optional

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtWidgets import (
    QComboBox,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QSizePolicy,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from data.models import (
    Basho,
    BoutResult,
    TournamentProbabilities,
    TournamentResult,
    WrestlerProfile,
    WrestlerStanding,
)

logger = logging.getLogger(__name__)


# ── Tournament worker thread ───────────────────────────────────────

class TournamentWorker(QThread):
    """Runs tournament simulation in a background thread."""

    single_finished = pyqtSignal(object)       # TournamentResult
    multi_finished = pyqtSignal(object)         # TournamentProbabilities
    day_completed = pyqtSignal(int, list)       # day number, list of BoutResult
    progress = pyqtSignal(int, int)             # current, total

    def __init__(self, tournament_sim, basho_id, mode="single", n_sims=100):
        super().__init__()
        self._sim = tournament_sim
        self._basho_id = basho_id
        self._mode = mode
        self._n_sims = n_sims

    def run(self):
        try:
            if self._mode == "single":
                result = self._sim.simulate_tournament(
                    self._basho_id,
                    day_callback=self._day_cb,
                )
                self.single_finished.emit(result)
            else:
                probs = self._sim.simulate_multiple(
                    self._basho_id,
                    n=self._n_sims,
                    progress_callback=self._progress_cb,
                )
                self.multi_finished.emit(probs)
        except Exception as e:
            logger.error(f"Tournament simulation error: {e}")
            if self._mode == "single":
                self.single_finished.emit(None)
            else:
                self.multi_finished.emit(None)

    def _day_cb(self, day: int, results: list) -> None:
        self.day_completed.emit(day, results)

    def _progress_cb(self, current: int, total: int) -> None:
        self.progress.emit(current, total)


# ── Main Tournament Panel ──────────────────────────────────────────

class TournamentPanel(QWidget):
    """
    Tournament Simulator tab.

    Layout:
        ┌──────────────────────────────────────────────────┐
        │  [Basho: Hatsu 2025 ▼]   [Iterations: 1000]     │
        │  [ ▶ Step Day ]  [ ▶▶ Run All ]  [ 📊 Full Sim ]│
        ├────────────────────────┬─────────────────────────┤
        │  Day-by-Day Results    │  Leaderboard            │
        │  ┌─────────────────┐   │  ┌───────────────────┐  │
        │  │ D1: East v West │   │  │ 1. Onosato  3-0  │  │
        │  │ D1: East v West │   │  │ 2. Hoshoryu 2-1  │  │
        │  │ D2: East v West │   │  │ 3. Abi      2-1  │  │
        │  │     ...          │   │  │    ...            │  │
        │  └─────────────────┘   │  └───────────────────┘  │
        ├────────────────────────┴─────────────────────────┤
        │  Yusho Probability Chart                         │
        │  ┌───────────────────────────────────────────┐   │
        │  │ ████ Onosato 35.2%                        │   │
        │  │ ███  Hoshoryu 18.1%                       │   │
        │  │ ██   Kotozakura 12.4%                     │   │
        │  └───────────────────────────────────────────┘   │
        └──────────────────────────────────────────────────┘
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._roster: list[WrestlerProfile] = []
        self._tournament_histories: dict = {}
        self._bout_records: list = []
        self._main_window = parent
        self._current_result: Optional[TournamentResult] = None
        self._worker: Optional[TournamentWorker] = None
        self._build_ui()

    def set_roster(self, roster: list[WrestlerProfile], tournament_histories: dict | None = None, bout_records: list | None = None) -> None:
        self._roster = roster
        self._tournament_histories = tournament_histories or {}
        self._bout_records = bout_records or []
        self._enable_controls(bool(roster))

    # ── UI construction ────────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(10)

        # ── Control row ────────────────────────────────────────────
        ctrl = QHBoxLayout()

        ctrl.addWidget(QLabel("Basho:"))
        self._basho_combo = QComboBox()
        self._basho_combo.setMinimumWidth(200)
        for b in Basho:
            self._basho_combo.addItem(b.display_name, b.value)
        # Default to most recent
        self._basho_combo.setCurrentIndex(0)
        ctrl.addWidget(self._basho_combo)

        ctrl.addWidget(QLabel("Year:"))
        self._year_spin = QSpinBox()
        self._year_spin.setRange(2000, 2030)
        self._year_spin.setValue(2025)
        self._year_spin.setMinimumWidth(80)
        ctrl.addWidget(self._year_spin)

        ctrl.addStretch()

        ctrl.addWidget(QLabel("Simulations:"))
        self._iter_spin = QSpinBox()
        self._iter_spin.setRange(10, 10000)
        self._iter_spin.setValue(100)
        self._iter_spin.setSingleStep(100)
        self._iter_spin.setMinimumWidth(80)
        ctrl.addWidget(self._iter_spin)

        layout.addLayout(ctrl)

        # ── Button row ─────────────────────────────────────────────
        btn_row = QHBoxLayout()

        self._step_btn = QPushButton("▶  Step Day")
        self._step_btn.clicked.connect(self._on_step_day)
        self._step_btn.setMinimumWidth(120)
        btn_row.addWidget(self._step_btn)

        self._run_btn = QPushButton("▶▶  Run All 15 Days")
        self._run_btn.setObjectName("primary")
        self._run_btn.clicked.connect(self._on_run_all)
        self._run_btn.setMinimumWidth(160)
        btn_row.addWidget(self._run_btn)

        self._multi_btn = QPushButton("📊  Full Probability Sim")
        self._multi_btn.clicked.connect(self._on_multi_sim)
        self._multi_btn.setMinimumWidth(180)
        btn_row.addWidget(self._multi_btn)

        btn_row.addStretch()

        self._export_btn = QPushButton("📥  Export Results")
        self._export_btn.clicked.connect(self._on_export)
        self._export_btn.setEnabled(False)
        btn_row.addWidget(self._export_btn)

        self._reset_btn = QPushButton("⟲  Reset")
        self._reset_btn.clicked.connect(self._on_reset)
        btn_row.addWidget(self._reset_btn)

        layout.addLayout(btn_row)

        # ── Middle: results table + leaderboard ────────────────────
        mid_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Day-by-day results
        results_group = QGroupBox("Day-by-Day Results")
        results_layout = QVBoxLayout(results_group)

        self._results_table = QTableWidget()
        self._results_table.setColumnCount(6)
        self._results_table.setHorizontalHeaderLabels(
            ["Day", "East", "West", "Winner", "Kimarite", "Prob"]
        )
        header = self._results_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        self._results_table.setAlternatingRowColors(True)
        self._results_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._results_table.setStyleSheet(
            "alternate-background-color: #F0EDE4;"
        )
        results_layout.addWidget(self._results_table)
        mid_splitter.addWidget(results_group)

        # Leaderboard
        board_group = QGroupBox("Leaderboard")
        board_layout = QVBoxLayout(board_group)

        self._leaderboard = QTableWidget()
        self._leaderboard.setColumnCount(4)
        self._leaderboard.setHorizontalHeaderLabels(
            ["#", "Wrestler", "Record", "Rank"]
        )
        lb_header = self._leaderboard.horizontalHeader()
        lb_header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        lb_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self._leaderboard.setAlternatingRowColors(True)
        self._leaderboard.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._leaderboard.setStyleSheet(
            "alternate-background-color: #F0EDE4;"
        )
        board_layout.addWidget(self._leaderboard)
        mid_splitter.addWidget(board_group)

        mid_splitter.setSizes([600, 400])
        layout.addWidget(mid_splitter, stretch=3)

        # ── Bottom: Yusho probability display ──────────────────────
        yusho_group = QGroupBox("Yusho Probabilities")
        yusho_layout = QVBoxLayout(yusho_group)

        self._yusho_container = QVBoxLayout()
        self._yusho_placeholder = QLabel(
            "Run a full probability simulation to see yusho chances"
        )
        self._yusho_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._yusho_placeholder.setStyleSheet("color: #999; font-size: 13px; padding: 20px;")
        self._yusho_container.addWidget(self._yusho_placeholder)
        yusho_layout.addLayout(self._yusho_container)

        layout.addWidget(yusho_group, stretch=2)

    def _enable_controls(self, enabled: bool) -> None:
        for w in (self._step_btn, self._run_btn, self._multi_btn):
            w.setEnabled(enabled)

    # ── Basho ID helper ────────────────────────────────────────────

    def _get_basho_id(self) -> str:
        year = self._year_spin.value()
        month = self._basho_combo.currentData()
        return f"{year}.{month}"

    # ── Brother pair resolution ─────────────────────────────────────

    # Brother pairs defined by shikona (stable across ID changes)
    _BROTHER_PAIRS_BY_SHIKONA = [
        ("Wakamotoharu", "Wakatakakage"),
        ("Kotoshoho", "Kotoeiho"),
    ]

    def _resolve_brother_pairs(self) -> list[tuple[str, str]]:
        """Resolve brother pairs from shikona to wrestler_id."""
        name_to_id = {w.shikona: w.wrestler_id for w in self._roster}
        pairs = []
        for a_name, b_name in self._BROTHER_PAIRS_BY_SHIKONA:
            a_id = name_to_id.get(a_name)
            b_id = name_to_id.get(b_name)
            if a_id and b_id:
                pairs.append((a_id, b_id))
        return pairs

    # ── Single tournament step/run ─────────────────────────────────

    def _on_step_day(self) -> None:
        if not self._roster:
            return
        self._on_run_all()

    def _on_run_all(self) -> None:
        if not self._roster:
            return

        self._enable_controls(False)
        self._run_btn.setText("Simulating…")

        basho_id = self._get_basho_id()

        # Load official torikumi schedule if available
        official_schedules = {}
        try:
            from tools.scrape_torikumi import load_schedule_for_simulator
            official_schedules = load_schedule_for_simulator(basho_id)
            if official_schedules:
                logger.info(
                    f"Loaded official schedule for days: {sorted(official_schedules.keys())}"
                )
        except Exception as e:
            logger.debug(f"No official schedule available: {e}")

        from engine.tournament_simulator import TournamentSimulator
        sim = TournamentSimulator(
            roster=self._roster,
            tournament_histories=self._tournament_histories,
            bout_records=self._bout_records,
            schedules=official_schedules,
            brother_pairs=self._resolve_brother_pairs(),
        )

        result = sim.simulate_tournament(basho_id)
        self._display_tournament_result(result)

        self._enable_controls(True)
        self._run_btn.setText("▶▶  Run All 15 Days")

    def _on_multi_sim(self) -> None:
        if not self._roster:
            return

        self._enable_controls(False)
        self._multi_btn.setText("Simulating…")

        basho_id = self._get_basho_id()
        n = self._iter_spin.value()

        # Load official torikumi schedule if available
        official_schedules = {}
        try:
            from tools.scrape_torikumi import load_schedule_for_simulator
            official_schedules = load_schedule_for_simulator(basho_id)
        except Exception:
            pass

        from engine.tournament_simulator import TournamentSimulator
        sim = TournamentSimulator(
            roster=self._roster,
            tournament_histories=self._tournament_histories,
            bout_records=self._bout_records,
            schedules=official_schedules,
            brother_pairs=self._resolve_brother_pairs(),
        )

        probs = sim.simulate_multiple(basho_id, n=n)
        self._display_probabilities(probs)

        self._enable_controls(True)
        self._multi_btn.setText("📊  Full Probability Sim")

    def _on_reset(self) -> None:
        self._results_table.setRowCount(0)
        self._leaderboard.setRowCount(0)
        self._current_result = None
        self._export_btn.setEnabled(False)
        self._clear_yusho_bars()

    def _on_export(self) -> None:
        if not self._current_result:
            return

        from PyQt6.QtWidgets import QFileDialog, QMessageBox

        basho_id = self._get_basho_id().replace(".", "")
        default_name = f"sumosim_{basho_id}_projection"

        path, selected_filter = QFileDialog.getSaveFileName(
            self,
            "Export Tournament Results",
            default_name,
            "Excel Workbook (*.xlsx);;CSV Files (*.csv);;All Files (*)",
        )
        if not path:
            return

        try:
            from engine.export import export_csv, export_xlsx

            if path.endswith(".csv") or "CSV" in selected_filter:
                out = export_csv(self._current_result, self._roster, path)
                QMessageBox.information(
                    self, "Export Complete",
                    f"Results exported to:\n{out}\n"
                    f"(Standings file also created alongside)"
                )
            else:
                if not path.endswith(".xlsx"):
                    path += ".xlsx"
                out = export_xlsx(self._current_result, self._roster, path)
                QMessageBox.information(
                    self, "Export Complete",
                    f"Results exported to:\n{out}"
                )
        except ImportError as e:
            QMessageBox.warning(
                self, "Export Error",
                f"Missing dependency for export:\n{e}\n\n"
                f"For XLSX: pip install openpyxl\n"
                f"CSV export works without extra dependencies."
            )
        except Exception as e:
            QMessageBox.warning(
                self, "Export Error",
                f"Could not export results:\n{e}"
            )

    # ── Display tournament result ──────────────────────────────────

    def _display_tournament_result(self, result: TournamentResult) -> None:
        self._current_result = result
        self._export_btn.setEnabled(True)

        # Populate results table
        all_bouts = []
        for day in sorted(result.day_results.keys()):
            for bout in result.day_results[day]:
                all_bouts.append((day, bout))

        self._results_table.setRowCount(len(all_bouts))
        wrestler_map = {w.wrestler_id: w for w in self._roster}

        for row, (day, bout) in enumerate(all_bouts):
            east_name = wrestler_map.get(bout.east_id, None)
            west_name = wrestler_map.get(bout.west_id, None)
            winner_name = wrestler_map.get(bout.winner_id, None)

            day_item = QTableWidgetItem(str(day))
            day_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self._results_table.setItem(row, 0, day_item)

            east_item = QTableWidgetItem(
                east_name.shikona if east_name else bout.east_id
            )
            self._results_table.setItem(row, 1, east_item)

            west_item = QTableWidgetItem(
                west_name.shikona if west_name else bout.west_id
            )
            self._results_table.setItem(row, 2, west_item)

            winner_item = QTableWidgetItem(
                winner_name.shikona if winner_name else bout.winner_id
            )
            # Color the winner's cell
            if bout.winner_id == bout.east_id:
                winner_item.setForeground(QColor("#8B0000"))
            else:
                winner_item.setForeground(QColor("#00008B"))
            winner_item.setFont(QFont("Outfit", -1, QFont.Weight.Bold))
            self._results_table.setItem(row, 3, winner_item)

            kim_text = bout.predicted_kimarite or "—"
            kim_item = QTableWidgetItem(kim_text)
            kim_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            kim_item.setForeground(QColor("#555"))
            self._results_table.setItem(row, 4, kim_item)

            prob_text = f"{bout.east_win_probability * 100:.0f}%"
            prob_item = QTableWidgetItem(prob_text)
            prob_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            prob_item.setForeground(QColor("#999"))
            self._results_table.setItem(row, 5, prob_item)

        # Populate leaderboard
        standings = sorted(
            result.final_standings,
            key=lambda s: (-s.wins, s.losses),
        )
        self._leaderboard.setRowCount(len(standings))

        for row, st in enumerate(standings):
            pos_item = QTableWidgetItem(str(row + 1))
            pos_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self._leaderboard.setItem(row, 0, pos_item)

            name_item = QTableWidgetItem(st.shikona)
            if result.yusho_winner_id == st.wrestler_id:
                name_item.setFont(QFont("Outfit", -1, QFont.Weight.Bold))
                name_item.setForeground(QColor("#B8860B"))
                name_item.setText(f"🏆 {st.shikona}")
            self._leaderboard.setItem(row, 1, name_item)

            rec_item = QTableWidgetItem(st.record)
            rec_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            if st.wins >= 8:
                rec_item.setForeground(QColor("#006400"))
            elif st.losses >= 8:
                rec_item.setForeground(QColor("#8B0000"))
            self._leaderboard.setItem(row, 2, rec_item)

            rank_item = QTableWidgetItem(
                wrestler_map[st.wrestler_id].full_rank
                if st.wrestler_id in wrestler_map else ""
            )
            self._leaderboard.setItem(row, 3, rank_item)

    # ── Display probabilities (bar chart in Qt widgets) ────────────

    def _display_probabilities(self, probs: TournamentProbabilities) -> None:
        self._clear_yusho_bars()

        wrestler_map = {w.wrestler_id: w for w in self._roster}

        # Sort by yusho probability descending, show top 15
        sorted_probs = sorted(
            probs.yusho_probabilities.items(),
            key=lambda x: -x[1],
        )[:15]

        if not sorted_probs:
            return

        max_prob = sorted_probs[0][1] if sorted_probs else 1.0

        for wid, prob in sorted_probs:
            if prob < 0.001:
                continue

            row = QHBoxLayout()

            name = wrestler_map[wid].shikona if wid in wrestler_map else wid
            name_lbl = QLabel(name)
            name_lbl.setMinimumWidth(120)
            name_lbl.setFont(QFont("Outfit", 11))
            row.addWidget(name_lbl)

            # Bar
            bar_frame = QFrame()
            bar_frame.setMinimumHeight(22)
            bar_frame.setMaximumHeight(22)
            bar_width = max(4, int(400 * (prob / max(max_prob, 0.01))))
            bar_frame.setFixedWidth(bar_width)
            bar_frame.setStyleSheet(
                "background-color: #8B0000; border-radius: 3px;"
            )
            row.addWidget(bar_frame)

            pct_lbl = QLabel(f" {prob * 100:.1f}%")
            pct_lbl.setStyleSheet("font-size: 12px; color: #666;")
            row.addWidget(pct_lbl)

            # Kachi-koshi probability
            kk = probs.kachi_koshi_probabilities.get(wid, 0)
            avg = probs.average_wins.get(wid, 0)
            extra_lbl = QLabel(f"KK: {kk * 100:.0f}%  Avg: {avg:.1f}W")
            extra_lbl.setStyleSheet("font-size: 11px; color: #999;")
            row.addWidget(extra_lbl)

            row.addStretch()

            self._yusho_container.addLayout(row)

        sim_label = QLabel(
            f"Based on {probs.num_simulations} simulations"
        )
        sim_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        sim_label.setStyleSheet("color: #999; font-size: 11px; padding: 4px;")
        self._yusho_container.addWidget(sim_label)

    def _clear_yusho_bars(self) -> None:
        """Remove all dynamically added yusho bar widgets."""
        while self._yusho_container.count():
            child = self._yusho_container.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
            elif child.layout():
                # Recursively clear sub-layouts
                self._clear_layout(child.layout())

        # Re-add placeholder
        self._yusho_placeholder = QLabel(
            "Run a full probability simulation to see yusho chances"
        )
        self._yusho_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._yusho_placeholder.setStyleSheet("color: #999; font-size: 13px; padding: 20px;")
        self._yusho_container.addWidget(self._yusho_placeholder)

    @staticmethod
    def _clear_layout(layout) -> None:
        while layout.count():
            child = layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
            elif child.layout():
                TournamentPanel._clear_layout(child.layout())
