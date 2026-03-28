#!/usr/bin/env python3
"""
SumoSim — Grand Sumo Tournament Simulator
Application entry point.

Usage:
    python main.py                # Launch GUI, load from database
    python main.py --basho 2026.03  # Launch with specific basho
    python main.py --sample       # Launch with built-in sample data
    python main.py --legacy       # Launch with hardcoded haru_2026.py (fallback)
"""

from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


def main() -> None:
    parser = argparse.ArgumentParser(description="SumoSim — Grand Sumo Tournament Simulator")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--sample", action="store_true", help="Load sample data (20 wrestlers)")
    group.add_argument("--legacy", action="store_true", help="Load from hardcoded haru_2026.py")
    group.add_argument("--basho", default=None, help="Load specific basho from database (e.g. 2026.03)")
    args = parser.parse_args()

    # Load .env for database credentials
    from pathlib import Path
    env_path = Path(__file__).resolve().parent / ".env"
    if env_path.exists():
        import os
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

    from PyQt6.QtWidgets import QApplication
    from PyQt6.QtGui import QFont

    from gui.main_window import MainWindow

    app = QApplication(sys.argv)
    app.setApplicationName("SumoSim")
    app.setOrganizationName("SumoSim")

    # Set a clean default font
    font = QFont("Segoe UI", 10)
    font.setStyleHint(QFont.StyleHint.SansSerif)
    app.setFont(font)

    window = MainWindow()
    window.show()

    if args.sample:
        from data.sample_data import sample_roster
        window.set_roster(sample_roster(), basho_id="2025.01")

    elif args.legacy:
        _load_legacy(window)

    elif args.basho:
        _load_from_database(window, args.basho)

    else:
        # Default: try database first, fall back to legacy
        _load_default(window)

    sys.exit(app.exec())


def _load_from_database(window, basho_id: str) -> bool:
    """Load roster and data from the database for a specific basho."""
    try:
        from data.db import SumoDatabase
        db = SumoDatabase()

        roster = db.get_roster(basho_id)
        if not roster:
            print(f"No roster found for {basho_id} in database")
            return False

        tournament_histories = db.get_all_tournament_records(basho_id)
        bout_records = db.get_bout_records()  # All bout records for H2H
        injury_notes = db.get_injury_notes(basho_id)

        print(f"Loaded from database ({basho_id}):")
        print(f"  Roster: {len(roster)} wrestlers")
        print(f"  Tournament records: {len(tournament_histories)} wrestlers")
        print(f"  Bout records: {len(bout_records)} total")
        print(f"  Injury notes: {len(injury_notes)} entries")
        print(f"  Online: {db.is_online}")

        window.set_roster(
            roster,
            basho_id=basho_id,
            tournament_histories=tournament_histories,
            injury_notes=injury_notes,
            bout_records=bout_records,
        )
        return True

    except Exception as e:
        print(f"Database load failed: {e}")
        return False


def _load_legacy(window) -> bool:
    """Load from hardcoded haru_2026.py data file."""
    try:
        from data.haru_2026 import (
            haru_2026_roster,
            haru_2026_tournament_records,
            haru_2026_injury_notes,
        )

        roster = haru_2026_roster()
        tournament_histories = haru_2026_tournament_records()
        injury_notes = haru_2026_injury_notes()

        # Try to load H2H bout records
        bout_records = []
        try:
            from data.h2h_haru2026 import haru_2026_bout_records
            bout_records = haru_2026_bout_records()
            print(f"Loaded {len(bout_records)} H2H bout records")
        except ImportError:
            print("H2H data not found — run: python -m tools.scrape_h2h")
        except Exception as e:
            print(f"Error loading H2H data: {e}")

        window.set_roster(
            roster,
            basho_id="2026.03",
            tournament_histories=tournament_histories,
            injury_notes=injury_notes,
            bout_records=bout_records,
        )
        print(f"Loaded legacy data: {len(roster)} wrestlers")
        return True

    except Exception as e:
        print(f"Legacy load failed: {e}")
        return False


def _load_default(window) -> None:
    """Try database first (most recent basho), fall back to legacy."""
    try:
        from data.db import SumoDatabase
        db = SumoDatabase()
        available = db.get_available_basho()

        if available:
            basho_id = available[0]  # Most recent
            print(f"Loading from database: {basho_id}")
            if _load_from_database(window, basho_id):
                return
    except Exception as e:
        print(f"Database not available: {e}")

    # Fall back to legacy
    print("Falling back to legacy data...")
    if not _load_legacy(window):
        print("No data available. Use --sample for demo data.")


if __name__ == "__main__":
    main()
