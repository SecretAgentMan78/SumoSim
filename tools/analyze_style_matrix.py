#!/usr/bin/env python3
"""
Analyze H2H bout data to compute empirical style-vs-style win rates.

Produces a 3x3 matrix showing how each fighting style (oshi, yotsu, hybrid)
performs against each other style, derived from actual bout outcomes rather
than assumptions.

Usage:
    python -m tools.analyze_style_matrix

Requires:
    - data/haru_2026.py         (roster with fighting_style tags)
    - data/h2h_haru2026.py      (bout records from scraper)

Output:
    Prints the empirical matrix and optionally patches modifier_panel.py
    with the new defaults.
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.models import FightingStyle


def load_data():
    """Load roster and bout records."""
    from data.haru_2026 import haru_2026_roster

    roster = haru_2026_roster()
    style_map = {w.wrestler_id: w.fighting_style for w in roster}

    try:
        from data.h2h_haru2026 import haru_2026_bout_records
        bouts = haru_2026_bout_records()
    except ImportError:
        print("ERROR: data/h2h_haru2026.py not found.")
        print("Run the scraper first:  python -m tools.scrape_h2h")
        sys.exit(1)

    return style_map, bouts


def compute_style_matrix(style_map, bouts):
    """
    Compute win counts for each style-vs-style pairing.

    For each bout we know the winner_id; the loser is the other wrestler.
    We look up each wrestler's fighting_style and tally:
        wins[winner_style][loser_style] += 1

    Returns:
        wins:   dict of {(winner_style, loser_style): count}
        totals: dict of {(style_a, style_b): total_bouts}  (unordered pair)
        rates:  dict of {(row_style, col_style): win_rate}
    """
    styles = [FightingStyle.OSHI, FightingStyle.YOTSU, FightingStyle.HYBRID]

    # wins[(A, B)] = number of times style A beat style B
    wins = defaultdict(int)
    skipped = 0

    for bout in bouts:
        winner = bout.winner_id
        # Determine loser
        loser = bout.west_id if winner == bout.east_id else bout.east_id

        w_style = style_map.get(winner)
        l_style = style_map.get(loser)

        if w_style is None or l_style is None:
            skipped += 1
            continue

        wins[(w_style, l_style)] += 1

    # Compute totals and rates for each ordered pair
    totals = {}
    rates = {}

    for row in styles:
        for col in styles:
            w = wins[(row, col)]
            l = wins[(col, row)]
            total = w + l
            totals[(row, col)] = total
            rates[(row, col)] = w / total if total > 0 else 0.5

    return wins, totals, rates, skipped


def rates_to_adjustments(rates, scale=0.2):
    """
    Convert win rates to signed adjustment values for the style matrix.

    A 50% win rate → 0.0 adjustment (no edge).
    A 60% win rate → +scale * 0.2 = positive advantage.
    A 40% win rate → -scale * 0.2 = disadvantage.

    The formula:  adjustment = (rate - 0.5) * 2 * scale

    With default scale=0.2, a 60/40 matchup becomes ±0.04,
    which when multiplied by the style matrix weight slider (default 0.3)
    gives a modest but meaningful edge.

    Mirror matchups (oshi vs oshi) are forced to 0.0.
    """
    styles = [FightingStyle.OSHI, FightingStyle.YOTSU, FightingStyle.HYBRID]
    matrix = []
    for row in styles:
        row_vals = []
        for col in styles:
            if row == col:
                row_vals.append(0.0)
            else:
                adj = (rates[(row, col)] - 0.5) * 2 * scale
                row_vals.append(round(adj, 2))
        matrix.append(row_vals)
    return matrix


def print_report(wins, totals, rates, matrix, skipped, total_bouts):
    """Print a human-readable analysis report."""
    styles = [FightingStyle.OSHI, FightingStyle.YOTSU, FightingStyle.HYBRID]
    labels = ["Oshi", "Yotsu", "Hybrid"]

    print("=" * 65)
    print("  STYLE-VS-STYLE ANALYSIS — Empirical Win Rates")
    print("=" * 65)
    print(f"\n  Bouts analyzed: {total_bouts}  (skipped {skipped} with unknown wrestlers)\n")

    # Raw win counts
    print("  RAW WIN COUNTS (row beat column):")
    print(f"  {'':>10s}  {'vs Oshi':>10s}  {'vs Yotsu':>10s}  {'vs Hybrid':>10s}")
    print("  " + "-" * 50)
    for i, row in enumerate(styles):
        counts = []
        for j, col in enumerate(styles):
            w = wins[(row, col)]
            counts.append(f"{w:>10d}")
        print(f"  {labels[i]:>10s}  {'  '.join(counts)}")

    # Win rates
    print(f"\n  WIN RATES (row vs column):")
    print(f"  {'':>10s}  {'vs Oshi':>10s}  {'vs Yotsu':>10s}  {'vs Hybrid':>10s}  {'Sample':>8s}")
    print("  " + "-" * 58)
    for i, row in enumerate(styles):
        cells = []
        sample_total = 0
        for j, col in enumerate(styles):
            r = rates[(row, col)]
            n = totals[(row, col)]
            sample_total += n
            if row == col:
                cells.append(f"{'—':>10s}")
            else:
                pct = f"{r:.1%}"
                cells.append(f"{pct:>10s}")
        print(f"  {labels[i]:>10s}  {'  '.join(cells)}  {sample_total // 2:>8d}")

    # Adjustment matrix
    print(f"\n  ADJUSTMENT MATRIX (for modifier_panel defaults):")
    print(f"  {'':>10s}  {'vs Oshi':>10s}  {'vs Yotsu':>10s}  {'vs Hybrid':>10s}")
    print("  " + "-" * 50)
    for i, row_vals in enumerate(matrix):
        cells = []
        for val in row_vals:
            s = f"{val:+.2f}"
            cells.append(f"{s:>10s}")
        print(f"  {labels[i]:>10s}  {'  '.join(cells)}")

    # Interpretation
    print("\n  INTERPRETATION:")
    cross_matchups = []
    for i, row in enumerate(styles):
        for j, col in enumerate(styles):
            if i != j:
                r = rates[(row, col)]
                n = totals[(row, col)]
                cross_matchups.append((labels[i], labels[j], r, n))

    cross_matchups.sort(key=lambda x: x[2], reverse=True)
    for attacker, defender, rate, n in cross_matchups:
        bar_len = int((rate - 0.3) * 50)  # scale for display
        bar = "█" * max(0, bar_len)
        print(f"    {attacker:>8s} vs {defender:<8s}: {rate:.1%} ({n:>4d} bouts) {bar}")

    print()


def generate_code_snippet(matrix):
    """Generate the Python code for modifier_panel.py defaults."""
    labels = ["Oshi", "Yotsu", "Hybrid"]
    print("  CODE FOR modifier_panel.py (replace existing defaults):")
    print()
    print("        # Empirical style-vs-style matrix from H2H bout data")
    print("        defaults = [")
    for i, row_vals in enumerate(matrix):
        vals = ", ".join(f"{v:+.2f}" for v in row_vals)
        pad = " " * 12
        print(f"{pad}[{vals}],    # {labels[i]} vs...")
    print("        ]")
    print()


def patch_modifier_panel(matrix, dry_run=False):
    """Patch modifier_panel.py with the new default matrix values."""
    panel_path = Path(__file__).resolve().parent.parent / "gui" / "modifier_panel.py"

    if not panel_path.exists():
        print(f"  WARNING: {panel_path} not found, cannot patch.")
        return False

    content = panel_path.read_text()

    # Find the existing defaults block
    import re
    pattern = r"(defaults = \[)\s*\n\s*\[.*?\],\s*#.*?\n\s*\[.*?\],\s*#.*?\n\s*\[.*?\],\s*#.*?\n\s*(\])"
    match = re.search(pattern, content, re.DOTALL)

    if not match:
        print("  WARNING: Could not find existing defaults block in modifier_panel.py")
        return False

    labels = ["Oshi", "Yotsu", "Hybrid"]
    new_block = "defaults = [\n"
    for i, row_vals in enumerate(matrix):
        vals = ", ".join(f"{v:+.2f}" for v in row_vals)
        new_block += f"            [{vals}],    # {labels[i]} vs...\n"
    new_block += "        ]"

    if dry_run:
        print("  DRY RUN — would replace:")
        print(f"    OLD: {match.group(0)[:80]}...")
        print(f"    NEW: {new_block[:80]}...")
        return True

    new_content = content[:match.start()] + new_block + content[match.end():]
    panel_path.write_text(new_content)
    print(f"  PATCHED {panel_path}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Analyze H2H bout data to compute empirical style-vs-style matrix"
    )
    parser.add_argument(
        "--scale", type=float, default=0.2,
        help="Scale factor for converting win rates to adjustments (default: 0.2)"
    )
    parser.add_argument(
        "--patch", action="store_true",
        help="Patch modifier_panel.py with the computed defaults"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what --patch would do without writing"
    )
    args = parser.parse_args()

    print("\nLoading data...")
    style_map, bouts = load_data()
    print(f"  {len(style_map)} wrestlers with style tags")
    print(f"  {len(bouts)} bout records")

    print("\nComputing style matrix...")
    wins, totals, rates, skipped = compute_style_matrix(style_map, bouts)
    matrix = rates_to_adjustments(rates, scale=args.scale)

    print_report(wins, totals, rates, matrix, skipped, len(bouts))
    generate_code_snippet(matrix)

    if args.patch or args.dry_run:
        patch_modifier_panel(matrix, dry_run=args.dry_run)

    # Also update the modifier guide help text
    print("  NOTE: If the matrix changes significantly from the old defaults,")
    print("  consider updating the Modifier Guide (Help menu) to mention that")
    print("  the style matrix is now empirically derived.")


if __name__ == "__main__":
    main()
