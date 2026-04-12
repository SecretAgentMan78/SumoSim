"""
SumoSim Main Window

Application shell with:
  - Menu bar (File, Data, Settings, Help)
  - Left sidebar: searchable wrestler roster
  - Central tab area: Bout Simulator, Tournament Simulator, Modifier Controls
  - Status bar: data freshness, simulation progress, active modifier summary
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from PyQt6.QtCore import Qt, QSize
from PyQt6.QtGui import QAction, QColor, QFont, QIcon, QPixmap
from PyQt6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMenu,
    QMenuBar,
    QMessageBox,
    QProgressBar,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

from data.models import Rank, WrestlerProfile
from gui.bout_panel import BoutPanel
from gui.modifier_panel import ModifierPanel
from gui.rikishi_panel import RikishiDossierPanel
from gui.tournament_panel import TournamentPanel

logger = logging.getLogger(__name__)

# ── Rank badge colors ──────────────────────────────────────────────
_RANK_COLORS = {
    Rank.YOKOZUNA: "#B8860B",   # dark gold
    Rank.OZEKI: "#8B0000",      # dark red
    Rank.SEKIWAKE: "#00008B",   # dark blue
    Rank.KOMUSUBI: "#006400",   # dark green
    Rank.MAEGASHIRA: "#3C3C3C", # dark gray
}

_RANK_LABELS = {
    Rank.YOKOZUNA: "Y", Rank.OZEKI: "O", Rank.SEKIWAKE: "S",
    Rank.KOMUSUBI: "K", Rank.MAEGASHIRA: "M",
}


def _rikishi_image_path(wrestler_id: str, shikona: str = "") -> str | None:
    """Resolve the path to a rikishi's portrait image, or None if not found.

    Checks for images named by API ID (e.g. 19.jpg) and by shikona
    (e.g. hoshoryu.jpg) in both the package and project root directories.
    """
    import os
    pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    project_dir = os.path.dirname(pkg_dir)

    # Build candidate filenames
    candidates = [f"{wrestler_id}.jpg"]
    if shikona:
        candidates.append(f"{shikona.lower()}.jpg")

    # Check both directories
    for base_dir in [pkg_dir, project_dir]:
        img_dir = os.path.join(base_dir, "data", "images", "rikishi")
        for filename in candidates:
            path = os.path.join(img_dir, filename)
            if os.path.isfile(path):
                return path
    return None


class MainWindow(QMainWindow):
    """
    SumoSim application main window.

    Coordinates data flow between panels and manages the wrestler roster.
    """

    def __init__(self):
        super().__init__()

        self.setWindowTitle("SumoSim — 相撲シミュレーター")
        self.setMinimumSize(1200, 800)
        self.resize(1440, 900)

        # ── State ──────────────────────────────────────────────────
        self._roster: list[WrestlerProfile] = []
        self._current_basho: str = ""

        # ── Build UI ───────────────────────────────────────────────
        self._build_menu_bar()
        self._build_status_bar()
        self._build_central_widget()
        self._apply_stylesheet()

    # ================================================================
    # Public interface for external data loading
    # ================================================================

    def set_roster(
        self,
        roster: list[WrestlerProfile],
        basho_id: str = "",
        tournament_histories: dict | None = None,
        injury_notes: dict | None = None,
        bout_records: list | None = None,
    ) -> None:
        """Load a roster into all panels."""
        self._roster = sorted(
            roster, key=lambda w: (w.rank.tier, w.rank_number or 0)
        )
        self._current_basho = basho_id
        self._tournament_histories = tournament_histories or {}
        self._bout_records = bout_records or []
        self._bout_panel.set_roster(
            self._roster,
            tournament_histories=self._tournament_histories,
            bout_records=self._bout_records,
        )
        self._tournament_panel.set_roster(
            self._roster,
            tournament_histories=self._tournament_histories,
            bout_records=self._bout_records,
        )
        self._modifier_panel.set_roster(self._roster)

        # Initialize rikishi dossier panel with database access
        try:
            from data.db import SumoDatabase
            db = SumoDatabase()
            self._rikishi_panel.set_data(db)
        except Exception as e:
            logger.warning(f"Could not initialize rikishi panel: {e}")

        # Pre-populate injury sliders if notes provided
        if injury_notes:
            self._modifier_panel.apply_injury_notes(injury_notes)

        # Load saved modifier overrides from database
        try:
            from data.db import SumoDatabase
            db = SumoDatabase()
            overrides = db.get_modifier_overrides()
            if overrides:
                self._apply_saved_overrides(overrides)
        except Exception as e:
            logger.debug(f"Could not load modifier overrides: {e}")

        self._update_status(f"Roster loaded: {len(self._roster)} wrestlers ({basho_id or 'custom'})")

    def get_roster(self) -> list[WrestlerProfile]:
        return self._roster

    # ================================================================
    # Menu bar
    # ================================================================

    def _build_menu_bar(self) -> None:
        menubar = self.menuBar()

        # ── File ───────────────────────────────────────────────────
        file_menu = menubar.addMenu("&File")

        load_action = QAction("&Load Scenario…", self)
        load_action.setShortcut("Ctrl+O")
        load_action.triggered.connect(self._on_load_scenario)
        file_menu.addAction(load_action)

        save_action = QAction("&Save Scenario…", self)
        save_action.setShortcut("Ctrl+S")
        save_action.triggered.connect(self._on_save_scenario)
        file_menu.addAction(save_action)

        file_menu.addSeparator()

        export_action = QAction("&Export Results…", self)
        export_action.setShortcut("Ctrl+E")
        export_action.triggered.connect(self._on_export_results)
        file_menu.addAction(export_action)

        file_menu.addSeparator()

        quit_action = QAction("&Quit", self)
        quit_action.setShortcut("Ctrl+Q")
        quit_action.triggered.connect(self.close)
        file_menu.addAction(quit_action)

        # ── Data ───────────────────────────────────────────────────
        data_menu = menubar.addMenu("&Data")

        refresh_action = QAction("&Refresh Scrape", self)
        refresh_action.setShortcut("Ctrl+R")
        refresh_action.triggered.connect(self._on_refresh_data)
        data_menu.addAction(refresh_action)

        cache_action = QAction("&Manage Cache…", self)
        cache_action.triggered.connect(self._on_manage_cache)
        data_menu.addAction(cache_action)

        data_menu.addSeparator()

        sample_action = QAction("Load &Sample Data", self)
        sample_action.triggered.connect(self._on_load_sample_data)
        data_menu.addAction(sample_action)

        # ── Settings ───────────────────────────────────────────────
        settings_menu = menubar.addMenu("&Settings")

        sim_params_action = QAction("&Simulation Parameters…", self)
        sim_params_action.triggered.connect(self._on_sim_params)
        settings_menu.addAction(sim_params_action)

        display_action = QAction("&Display Options…", self)
        display_action.triggered.connect(self._on_display_options)
        settings_menu.addAction(display_action)

        # ── Help ───────────────────────────────────────────────────
        help_menu = menubar.addMenu("&Help")

        about_action = QAction("&About SumoSim", self)
        about_action.triggered.connect(self._on_about)
        help_menu.addAction(about_action)

        glossary_action = QAction("Sumo &Glossary", self)
        glossary_action.triggered.connect(self._on_glossary)
        help_menu.addAction(glossary_action)

        modifier_guide_action = QAction("&Modifier Guide", self)
        modifier_guide_action.triggered.connect(self._on_modifier_guide)
        help_menu.addAction(modifier_guide_action)

    # ================================================================
    # Status bar
    # ================================================================

    def _build_status_bar(self) -> None:
        self._statusbar = QStatusBar()
        self.setStatusBar(self._statusbar)

        self._status_label = QLabel("No data loaded")
        self._statusbar.addWidget(self._status_label, 1)

        self._modifier_summary_label = QLabel("")
        self._statusbar.addPermanentWidget(self._modifier_summary_label)

        self._progress_bar = QProgressBar()
        self._progress_bar.setMaximumWidth(200)
        self._progress_bar.setVisible(False)
        self._statusbar.addPermanentWidget(self._progress_bar)

    def _update_status(self, text: str) -> None:
        self._status_label.setText(text)

    def show_progress(self, current: int, total: int, label: str = "") -> None:
        """Show simulation progress in the status bar."""
        self._progress_bar.setVisible(True)
        self._progress_bar.setMaximum(total)
        self._progress_bar.setValue(current)
        if label:
            self._update_status(label)
        if current >= total:
            self._progress_bar.setVisible(False)

    def update_modifier_summary(self, summary: str) -> None:
        """Update the modifier summary in the status bar."""
        self._modifier_summary_label.setText(summary)

    # ================================================================
    # Central layout: sidebar + tabbed panels
    # ================================================================

    def _build_central_widget(self) -> None:
        # ── Tabbed panels ──────────────────────────────────────────
        self._tab_widget = QTabWidget()

        self._rikishi_panel = RikishiDossierPanel(parent=self)
        self._tab_widget.addTab(self._rikishi_panel, "📖  Rikishi Dossier")

        self._bout_panel = BoutPanel(parent=self)
        self._tab_widget.addTab(self._bout_panel, "⚔  Bout Simulator")

        self._tournament_panel = TournamentPanel(parent=self)
        self._tab_widget.addTab(self._tournament_panel, "🏆  Tournament")

        self._modifier_panel = ModifierPanel(parent=self)
        self._tab_widget.addTab(self._modifier_panel, "🎛  Modifiers")

        self.setCentralWidget(self._tab_widget)

    # ================================================================
    # Wrestler lookup
    # ================================================================

    def _apply_saved_overrides(self, overrides: dict) -> None:
        """Apply saved modifier overrides to the modifier panel."""
        from data.models import MomentumState

        table = self._modifier_panel._override_table
        for row in range(table.rowCount()):
            item = table.item(row, 0)
            if not item:
                continue
            wid = item.data(Qt.ItemDataRole.UserRole)
            if wid not in overrides:
                continue

            ov = overrides[wid]

            # Momentum
            mom_val = ov.get("momentum")
            if mom_val:
                mom_widget = table.cellWidget(row, 2)
                if mom_widget:
                    try:
                        ms = MomentumState(mom_val)
                        idx = mom_widget.findData(ms)
                        if idx >= 0:
                            mom_widget.setCurrentIndex(idx)
                    except ValueError:
                        pass

            # Injury
            inj_val = ov.get("injury_severity", 0.0)
            if inj_val and inj_val > 0:
                inj_widget = table.cellWidget(row, 3)
                if inj_widget:
                    inj_widget.setValue(inj_val)

    def find_wrestler(self, wrestler_id: str) -> Optional[WrestlerProfile]:
        """Look up a wrestler by ID from the loaded roster."""
        return next((w for w in self._roster if w.wrestler_id == wrestler_id), None)

    # ================================================================
    # Menu handlers (stubs — wired up later)
    # ================================================================

    def _on_load_scenario(self) -> None:
        from PyQt6.QtWidgets import QFileDialog
        path, _ = QFileDialog.getOpenFileName(
            self, "Load Scenario", "",
            "SumoSim Scenarios (*.json);;All Files (*)"
        )
        if not path:
            return

        try:
            import json
            with open(path) as f:
                data = json.load(f)

            if "modifiers" in data:
                self._modifier_panel.set_full_state(data["modifiers"])

            name = data.get("name", Path(path).stem)
            self._update_status(f"Loaded scenario: {name}")
        except Exception as e:
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.warning(self, "Load Error", f"Could not load scenario:\n{e}")

    def _on_save_scenario(self) -> None:
        from PyQt6.QtWidgets import QFileDialog
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Scenario", "scenario.json",
            "SumoSim Scenarios (*.json);;All Files (*)"
        )
        if not path:
            return

        try:
            import json
            data = {
                "name": Path(path).stem,
                "basho": self._current_basho,
                "modifiers": self._modifier_panel.get_full_state(),
            }

            with open(path, 'w') as f:
                json.dump(data, f, indent=2)

            self._update_status(f"Saved scenario: {Path(path).name}")
        except Exception as e:
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.warning(self, "Save Error", f"Could not save scenario:\n{e}")

    def _on_export_results(self) -> None:
        self._update_status("Export Results: not yet implemented")

    def _on_refresh_data(self) -> None:
        self._update_status("Refreshing data…")
        # Will be wired to DataManager.refresh_all()

    def _on_manage_cache(self) -> None:
        self._update_status("Cache management: not yet implemented")

    def _on_load_sample_data(self) -> None:
        """Load the built-in sample dataset for immediate use."""
        from data.sample_data import sample_roster
        roster = sample_roster()
        self.set_roster(roster, basho_id="2025.01")
        self._update_status(f"Sample data loaded: {len(roster)} wrestlers (Hatsu 2025)")

    def _on_sim_params(self) -> None:
        self._update_status("Simulation parameters dialog: not yet implemented")

    def _on_display_options(self) -> None:
        self._update_status("Display options: not yet implemented")

    def _on_about(self) -> None:
        QMessageBox.about(
            self,
            "About SumoSim",
            "<h2>SumoSim 1.0</h2>"
            "<p>相撲シミュレーター<br>"
            "Grand Sumo Tournament Simulator</p>"
            "<p>Monte Carlo bout simulation with tunable subjective modifiers.</p>"
            "<p>Data: Sumo Reference &amp; Sumo API<br>"
            "Engine: NumPy/SciPy vectorized simulation<br>"
            "GUI: PyQt6</p>",
        )

    def _on_glossary(self) -> None:
        QMessageBox.information(
            self,
            "Sumo Glossary",
            "<b>Honbasho</b> — Official tournament (6 per year)<br>"
            "<b>Basho</b> — Tournament<br>"
            "<b>Makuuchi</b> — Top division (42 wrestlers)<br>"
            "<b>Banzuke</b> — Official ranking list<br>"
            "<b>Torikumi</b> — Daily bout schedule<br>"
            "<b>Kimarite</b> — Winning technique<br>"
            "<b>Yusho</b> — Tournament championship<br>"
            "<b>Kachi-koshi</b> — Winning record (8+ wins)<br>"
            "<b>Kettei-sen</b> — Playoff bout<br>"
            "<b>Yokozuna</b> — Highest rank<br>"
            "<b>Oshi-zumo</b> — Pushing/thrusting style<br>"
            "<b>Yotsu-zumo</b> — Belt-grappling style<br>",
        )

    def _on_modifier_guide(self) -> None:
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QTextBrowser, QDialogButtonBox

        dlg = QDialog(self)
        dlg.setWindowTitle("Modifier Guide")
        dlg.resize(560, 520)
        layout = QVBoxLayout(dlg)

        browser = QTextBrowser()
        browser.setOpenExternalLinks(False)
        browser.setStyleSheet(
            "QTextBrowser { background-color: #FFFDF5; font-size: 13px; "
            "padding: 12px; border: none; }"
        )
        browser.setHtml(
            "<h2>Modifier Guide</h2>"

            "<h3>Momentum / Form</h3>"
            "<p><b>Momentum Weight</b> controls how strongly recent form influences "
            "the rating. At 0, momentum is ignored. At 1.0, a hot streak can swing "
            "win probability by up to ±15%. Default: 0.5.</p>"
            "<p><b>Streak Window</b> sets how many recent bouts are considered. "
            "A 5-bout window captures short-term form; a 15-bout window captures "
            "sustained runs across a full tournament. Default: 5.</p>"
            "<p>Per-wrestler momentum can be manually overridden to Hot, Neutral, "
            "or Cold in the override table for what-if scenarios.</p>"

            "<h3>Matchup / Style</h3>"
            "<p><b>Style Matrix Weight</b> controls how much the style matchup "
            "influences the outcome. At 0, style is ignored. At 1.0, a favorable "
            "style matchup can adjust win probability by up to ±10%. Default: 0.3.</p>"
            "<p>The <b>style interaction matrix</b> defines advantages between the "
            "three archetypes: oshi-zumo (pushing/thrusting), yotsu-zumo (belt "
            "grappling), and hybrid. Positive values favor the row style against "
            "the column style. The default values are derived from empirical "
            "win rates in the H2H bout data (via <tt>tools/analyze_style_matrix.py"
            "</tt>). You can edit the matrix directly to test alternative "
            "hypotheses about stylistic dominance.</p>"

            "<h3>Injury / Fatigue</h3>"
            "<p><b>Injury Severity</b> is set per wrestler from 0 (fully healthy) to "
            "1.0 (severely compromised). A severity of 0.5 reduces effective rating "
            "by approximately 8%. Injuries can be pre-loaded from data or set "
            "manually in the override table.</p>"

            "<p><b>Recovery Factor</b> simulates how well a wrestler recovers between "
            "days. Younger wrestlers and those with lighter schedules recover more. "
            "Range: 0 (no recovery) to 1.0 (full daily recovery). Default: 0.6.</p>"

            "<h3>Fatigue Curves</h3>"
            "<p>The fatigue curve models how fatigue accumulates across the 15-day "
            "tournament. All three options produce the same total fatigue by Day 15, "
            "but they differ in <i>when</i> the fatigue hits hardest.</p>"

            "<p><b>Linear</b> — Fatigue increases at a constant rate each day. "
            "Day 1 to Day 2 is the same fatigue jump as Day 14 to Day 15. "
            "Predictable and even, but doesn't reflect how bodies actually respond "
            "to sustained exertion.</p>"

            "<p><b>Exponential</b> — Fatigue is light early and accelerates sharply "
            "toward the end. The first week feels relatively easy, but fatigue "
            "compounds rapidly in the final days. By Days 12–15, wrestlers are "
            "hitting a wall. This models the idea that accumulated damage has a "
            "snowball effect — each day of wear makes the next day disproportionately "
            "harder. Makes late-tournament upsets more likely, especially for heavier "
            "wrestlers.</p>"

            "<p><b>S-Curve</b> (default) — The most realistic option. Fatigue is "
            "gentle in the first few days as wrestlers are fresh, steepens through "
            "the middle of the tournament as the grind sets in, then levels off "
            "toward the end as wrestlers either adapt or are already near their "
            "floor. This concentrates the fatigue impact in the critical middle "
            "days (roughly Days 7–12) where the yusho race typically takes shape. "
            "There's a big difference between Day 5 and Day 10, but less difference "
            "between Day 12 and Day 15 because the body has already adjusted to "
            "the sustained load.</p>"

            "<h3>Scenarios</h3>"
            "<p>Use <b>File → Save Scenario</b> to save all current modifier "
            "settings (sliders, style matrix, per-wrestler overrides) to a JSON "
            "file. <b>File → Load Scenario</b> restores them. This lets you quickly "
            "switch between different what-if configurations.</p>"
        )
        layout.addWidget(browser)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(dlg.close)
        layout.addWidget(buttons)

        dlg.exec()

    # ================================================================
    # Stylesheet
    # ================================================================

    def _apply_stylesheet(self) -> None:
        self.setStyleSheet("""
            QMainWindow {
                background-color: #F5F0E8;
            }
            QTabWidget::pane {
                border: 1px solid #C0B090;
                background-color: #FAFAF5;
                border-radius: 4px;
            }
            QTabBar::tab {
                background-color: #E8E0D0;
                border: 1px solid #C0B090;
                padding: 8px 18px;
                margin-right: 2px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                font-size: 13px;
            }
            QTabBar::tab:selected {
                background-color: #FAFAF5;
                border-bottom-color: #FAFAF5;
                font-weight: bold;
            }
            QListWidget {
                background-color: #FFFDF5;
                border: 1px solid #C0B090;
                border-radius: 4px;
                font-size: 13px;
            }
            QListWidget::item {
                padding: 4px 6px;
                min-height: 38px;
                border-bottom: 1px solid #EDE8DC;
            }
            QListWidget::item:selected {
                background-color: #D4C8A8;
                color: #1A1A1A;
            }
            QLineEdit {
                padding: 5px 8px;
                border: 1px solid #C0B090;
                border-radius: 4px;
                background-color: #FFFDF5;
                font-size: 13px;
            }
            QStatusBar {
                background-color: #E8E0D0;
                border-top: 1px solid #C0B090;
                font-size: 12px;
            }
            QMenuBar {
                background-color: #E8E0D0;
                border-bottom: 1px solid #C0B090;
            }
            QMenuBar::item:selected {
                background-color: #D4C8A8;
            }
            QPushButton {
                padding: 6px 16px;
                border: 1px solid #C0B090;
                border-radius: 4px;
                background-color: #E8E0D0;
                font-size: 13px;
            }
            QPushButton:hover {
                background-color: #D4C8A8;
            }
            QPushButton:pressed {
                background-color: #C0B090;
            }
            QPushButton#primary {
                background-color: #8B0000;
                color: white;
                border-color: #6B0000;
                font-weight: bold;
            }
            QPushButton#primary:hover {
                background-color: #A00000;
            }
            QGroupBox {
                font-weight: bold;
                border: 1px solid #C0B090;
                border-radius: 4px;
                margin-top: 12px;
                padding-top: 12px;
                background-color: #FAFAF5;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                padding: 0 6px;
            }
            QSlider::groove:horizontal {
                height: 6px;
                background: #C0B090;
                border-radius: 3px;
            }
            QSlider::handle:horizontal {
                background: #8B0000;
                width: 16px;
                margin: -5px 0;
                border-radius: 8px;
            }
            QProgressBar {
                border: 1px solid #C0B090;
                border-radius: 3px;
                text-align: center;
                background-color: #E8E0D0;
            }
            QProgressBar::chunk {
                background-color: #8B0000;
                border-radius: 2px;
            }
        """)
