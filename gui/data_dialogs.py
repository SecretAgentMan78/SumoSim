"""
SumoSim Data Dialogs

GUI components for the Data menu:
  - ScrapeDialog:  Run scrape_full.py in a background thread with live progress
  - CacheDialog:   Inspect and clear cache directories
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSizePolicy,
    QTextEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
)

logger = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────

def _project_root() -> Path:
    """Find the project root (parent of gui/)."""
    here = Path(__file__).resolve().parent
    candidate = here.parent
    if (candidate / "data").is_dir():
        return candidate
    return here


def _dir_size(path: Path) -> tuple[int, int]:
    """Return (total_bytes, file_count) for a directory."""
    total = 0
    count = 0
    if path.exists():
        for f in path.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
                count += 1
    return total, count


def _fmt_size(nbytes: int) -> str:
    """Human-readable file size."""
    if nbytes < 1024:
        return f"{nbytes} B"
    elif nbytes < 1024 * 1024:
        return f"{nbytes / 1024:.1f} KB"
    else:
        return f"{nbytes / (1024 * 1024):.1f} MB"


# ====================================================================
# Scrape Worker Thread
# ====================================================================

class ScrapeWorker(QThread):
    """Runs tools/scrape_full.py as a subprocess in the background.

    Emits line-by-line output for the progress log, plus a finished
    signal with success/failure status.
    """

    line_output = pyqtSignal(str)       # each line of stdout/stderr
    progress = pyqtSignal(int, int)     # (current, total) wrestlers processed
    finished_ok = pyqtSignal(str)       # summary message on success
    finished_err = pyqtSignal(str)      # error message on failure

    def __init__(self, basho_id: str, include_matches: bool = False,
                 include_history: bool = False, include_backfill: bool = False,
                 push_to_supabase: bool = False):
        super().__init__()
        self._basho_id = basho_id
        self._matches = include_matches
        self._history = include_history
        self._backfill = include_backfill
        self._push = push_to_supabase
        self._cancelled = False
        self._process = None

    def cancel(self):
        self._cancelled = True
        if self._process and self._process.poll() is None:
            self._process.kill()

    def run(self):
        root = _project_root()

        # ── Build command ──────────────────────────────────────────
        cmd = [sys.executable, "-m", "tools.scrape_full",
               "--basho", self._basho_id]
        if self._backfill:
            cmd.append("--backfill")
        if self._matches:
            cmd.append("--matches")
        if self._history:
            cmd.append("--history")

        phase_desc = "profiles + stats"
        if self._matches:
            phase_desc += " + matches"
        if self._history:
            phase_desc += " + history"
        if self._backfill:
            phase_desc = "backfill + " + phase_desc

        self.line_output.emit(f"── Scraping basho {self._basho_id} ({phase_desc}) ──")
        self.line_output.emit(f"Command: {' '.join(cmd)}")
        self.line_output.emit("")

        # ── Run scrape_full.py ─────────────────────────────────────
        try:
            self._process = subprocess.Popen(
                cmd,
                cwd=str(root),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            wrestler_count = 0
            for line in self._process.stdout:
                if self._cancelled:
                    self._process.kill()
                    self.finished_err.emit("Scrape cancelled by user.")
                    return

                line = line.rstrip()
                self.line_output.emit(line)

                # Parse progress from lines like "  ✓ Hoshoryu ..."
                if "✓" in line or "✗" in line:
                    wrestler_count += 1
                    self.progress.emit(wrestler_count, 70)

            self._process.wait()
            if self._process.returncode != 0:
                self.finished_err.emit(
                    f"Scrape failed (exit code {self._process.returncode})"
                )
                return

        except Exception as e:
            self.finished_err.emit(f"Scrape error: {e}")
            return

        # ── Optional: Push to Supabase ─────────────────────────────
        if self._push and not self._cancelled:
            self.line_output.emit("")
            self.line_output.emit("── Pushing to Supabase ──")
            try:
                self._process = subprocess.Popen(
                    [sys.executable, "-m", "tools.push_to_supabase",
                     "wrestlers", "basho_entries", "bout_records"],
                    cwd=str(root),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
                for line in self._process.stdout:
                    if self._cancelled:
                        self._process.kill()
                        break
                    self.line_output.emit(line.rstrip())
                self._process.wait()
            except Exception as e:
                self.line_output.emit(f"Push error (non-fatal): {e}")

        self.finished_ok.emit(
            f"Done: {wrestler_count} wrestlers processed for basho {self._basho_id}."
        )


# ====================================================================
# Scrape Dialog (Data → Refresh Scrape)
# ====================================================================

class ScrapeDialog(QDialog):
    """Modal dialog for running a data scrape with live progress."""

    data_refreshed = pyqtSignal()

    def __init__(self, parent=None, current_basho: str = "202603"):
        super().__init__(parent)
        self.setWindowTitle("Refresh Scrape")
        self.setMinimumSize(600, 480)
        self._worker = None
        self._build_ui(current_basho)

    def _build_ui(self, current_basho: str):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        # ── Options ────────────────────────────────────────────────
        opts_group = QGroupBox("Scrape Options")
        opts_group.setFont(QFont("Outfit", 11))
        opts_layout = QVBoxLayout(opts_group)

        # Row 1: Basho selector
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Basho:"))
        self._basho_combo = QComboBox()
        self._basho_combo.setEditable(True)
        self._basho_combo.setFont(QFont("Outfit", 11))
        for bid in ["202605", "202603", "202601", "202511", "202509"]:
            self._basho_combo.addItem(bid)
        idx = self._basho_combo.findText(current_basho)
        if idx >= 0:
            self._basho_combo.setCurrentIndex(idx)
        else:
            self._basho_combo.setEditText(current_basho)
        row1.addWidget(self._basho_combo)
        row1.addStretch()
        opts_layout.addLayout(row1)

        # Row 2: Phase checkboxes
        row2 = QHBoxLayout()
        self._cb_backfill = QCheckBox("Backfill opponents")
        self._cb_backfill.setToolTip("Phase 0: Fetch profiles for wrestler IDs missing from the DB (~15 min)")
        row2.addWidget(self._cb_backfill)

        self._cb_matches = QCheckBox("Match history")
        self._cb_matches.setToolTip("Phase 2: Fetch full career bouts for each wrestler (~20 min)")
        row2.addWidget(self._cb_matches)

        self._cb_history = QCheckBox("Historical basho")
        self._cb_history.setToolTip("Phase 3: Build historical basho entries from bout data (~10 min)")
        row2.addWidget(self._cb_history)
        opts_layout.addLayout(row2)

        # Row 3: Push checkbox
        row3 = QHBoxLayout()
        self._cb_push = QCheckBox("Push to Supabase after scrape")
        self._cb_push.setChecked(True)
        row3.addWidget(self._cb_push)
        row3.addStretch()
        opts_layout.addLayout(row3)

        layout.addWidget(opts_group)

        # ── Progress bar ───────────────────────────────────────────
        self._progress = QProgressBar()
        self._progress.setRange(0, 70)
        self._progress.setValue(0)
        self._progress.setTextVisible(True)
        self._progress.setFormat("Phase 1: %v / %m wrestlers")
        layout.addWidget(self._progress)

        # ── Log output ─────────────────────────────────────────────
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setFont(QFont("Consolas", 9) if sys.platform == "win32"
                          else QFont("Menlo", 9))
        self._log.setMinimumHeight(200)
        layout.addWidget(self._log)

        # ── Buttons ────────────────────────────────────────────────
        btn_layout = QHBoxLayout()

        self._start_btn = QPushButton("Start Scrape")
        self._start_btn.setFont(QFont("Outfit", 11, QFont.Weight.Bold))
        self._start_btn.setObjectName("primary")
        self._start_btn.clicked.connect(self._on_start)
        btn_layout.addWidget(self._start_btn)

        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.setFont(QFont("Outfit", 11))
        self._cancel_btn.setEnabled(False)
        self._cancel_btn.clicked.connect(self._on_cancel)
        btn_layout.addWidget(self._cancel_btn)

        btn_layout.addStretch()

        self._close_btn = QPushButton("Close")
        self._close_btn.setFont(QFont("Outfit", 11))
        self._close_btn.clicked.connect(self.close)
        btn_layout.addWidget(self._close_btn)

        layout.addLayout(btn_layout)

    def _on_start(self):
        basho_id = self._basho_combo.currentText().strip()
        if not basho_id or len(basho_id) != 6 or not basho_id.isdigit():
            QMessageBox.warning(self, "Invalid Basho",
                                "Enter a basho ID in YYYYMM format (e.g. 202603).")
            return

        self._start_btn.setEnabled(False)
        self._cancel_btn.setEnabled(True)
        self._progress.setValue(0)
        self._log.clear()

        self._worker = ScrapeWorker(
            basho_id=basho_id,
            include_backfill=self._cb_backfill.isChecked(),
            include_matches=self._cb_matches.isChecked(),
            include_history=self._cb_history.isChecked(),
            push_to_supabase=self._cb_push.isChecked(),
        )
        self._worker.line_output.connect(self._on_log_line)
        self._worker.progress.connect(self._on_progress)
        self._worker.finished_ok.connect(self._on_finished_ok)
        self._worker.finished_err.connect(self._on_finished_err)
        self._worker.start()

    def _on_cancel(self):
        if self._worker:
            self._worker.cancel()
        self._cancel_btn.setEnabled(False)

    def _on_log_line(self, line: str):
        self._log.append(line)
        # Auto-scroll to bottom
        sb = self._log.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _on_progress(self, current: int, total: int):
        self._progress.setMaximum(total)
        self._progress.setValue(current)

    def _on_finished_ok(self, msg: str):
        self._log.append(f"\n✅ {msg}")
        self._start_btn.setEnabled(True)
        self._cancel_btn.setEnabled(False)
        self._progress.setValue(self._progress.maximum())
        self.data_refreshed.emit()

    def _on_finished_err(self, msg: str):
        self._log.append(f"\n❌ {msg}")
        self._start_btn.setEnabled(True)
        self._cancel_btn.setEnabled(False)


# ====================================================================
# Cache Dialog (Data → Manage Cache)
# ====================================================================

class CacheDialog(QDialog):
    """Dialog showing cache directories with sizes and clear buttons."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manage Cache")
        self.setMinimumSize(520, 400)
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        # Description
        desc = QLabel(
            "SumoSim caches scraped API data to avoid re-fetching. "
            "Clear individual caches here, or clear all to force a full re-scrape."
        )
        desc.setWordWrap(True)
        desc.setFont(QFont("Outfit", 10))
        layout.addWidget(desc)

        # ── Cache tree ─────────────────────────────────────────────
        self._tree = QTreeWidget()
        self._tree.setHeaderLabels(["Cache", "Files", "Size", ""])
        self._tree.setColumnWidth(0, 220)
        self._tree.setColumnWidth(1, 60)
        self._tree.setColumnWidth(2, 80)
        self._tree.setColumnWidth(3, 80)
        self._tree.setFont(QFont("Outfit", 10))
        self._tree.setAlternatingRowColors(True)
        layout.addWidget(self._tree)

        self._populate_tree()

        # ── Buttons ────────────────────────────────────────────────
        btn_layout = QHBoxLayout()

        refresh_btn = QPushButton("Refresh")
        refresh_btn.setFont(QFont("Outfit", 10))
        refresh_btn.clicked.connect(self._populate_tree)
        btn_layout.addWidget(refresh_btn)

        clear_all_btn = QPushButton("Clear All Caches")
        clear_all_btn.setFont(QFont("Outfit", 10))
        clear_all_btn.setStyleSheet("color: #8B0000; font-weight: bold;")
        clear_all_btn.clicked.connect(self._on_clear_all)
        btn_layout.addWidget(clear_all_btn)

        btn_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.setFont(QFont("Outfit", 10))
        close_btn.clicked.connect(self.close)
        btn_layout.addWidget(close_btn)

        layout.addLayout(btn_layout)

    def _get_cache_dirs(self) -> list[tuple[str, Path, str]]:
        """Return list of (label, path, description) for each cache directory."""
        root = _project_root()
        return [
            ("API Response Cache (scrape_full)",
             root / "data" / "cache" / "scrape_full",
             "Cached profile, stats, banzuke, and match JSON from sumo-api.com"),
            ("API Response Cache (rikishi)",
             root / "data" / "cache" / "rikishi",
             "Cached rikishi detail and stats JSON from scrape_rikishi.py"),
            ("H2H Cache",
             root / ".h2h_cache",
             "Cached head-to-head match history from scrape_h2h.py"),
            ("DataManager Cache",
             root / "data" / "cache",
             "CacheManager JSON files (banzuke, results, records)"),
        ]

    def _populate_tree(self):
        self._tree.clear()
        total_bytes = 0
        total_files = 0

        for label, path, description in self._get_cache_dirs():
            nbytes, nfiles = _dir_size(path)
            total_bytes += nbytes
            total_files += nfiles

            item = QTreeWidgetItem([
                label,
                str(nfiles) if path.exists() else "—",
                _fmt_size(nbytes) if path.exists() else "—",
                "",
            ])
            item.setToolTip(0, description)
            item.setToolTip(1, str(path))
            item.setData(0, Qt.ItemDataRole.UserRole, str(path))

            # Add subdirectories as children
            if path.exists():
                for subdir in sorted(path.iterdir()):
                    if subdir.is_dir():
                        sub_bytes, sub_files = _dir_size(subdir)
                        child = QTreeWidgetItem([
                            f"  {subdir.name}",
                            str(sub_files),
                            _fmt_size(sub_bytes),
                            "",
                        ])
                        child.setData(0, Qt.ItemDataRole.UserRole, str(subdir))
                        item.addChild(child)

            self._tree.addTopLevelItem(item)

            # Add clear button for this cache
            clear_btn = QPushButton("Clear")
            clear_btn.setFont(QFont("Outfit", 9))
            clear_btn.setFixedWidth(70)
            clear_btn.setEnabled(path.exists() and nfiles > 0)
            clear_btn.clicked.connect(lambda checked, p=path, l=label: self._on_clear(p, l))
            self._tree.setItemWidget(item, 3, clear_btn)

        # Summary row
        summary = QTreeWidgetItem([
            f"Total",
            str(total_files),
            _fmt_size(total_bytes),
            "",
        ])
        font = summary.font(0)
        font.setBold(True)
        summary.setFont(0, font)
        summary.setFont(1, font)
        summary.setFont(2, font)
        self._tree.addTopLevelItem(summary)

        self._tree.expandAll()

    def _on_clear(self, path: Path, label: str):
        reply = QMessageBox.question(
            self, "Clear Cache",
            f"Delete all files in:\n{path}\n\nThis will force a re-download on next scrape.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            try:
                if path.exists():
                    shutil.rmtree(path)
                    path.mkdir(parents=True, exist_ok=True)
                self._populate_tree()
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Failed to clear {label}:\n{e}")

    def _on_clear_all(self):
        reply = QMessageBox.question(
            self, "Clear All Caches",
            "Delete ALL cached data?\n\n"
            "This will force a full re-scrape from sumo-api.com.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            for label, path, _ in self._get_cache_dirs():
                try:
                    if path.exists():
                        shutil.rmtree(path)
                        path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    logger.error(f"Failed to clear {label}: {e}")
            self._populate_tree()
