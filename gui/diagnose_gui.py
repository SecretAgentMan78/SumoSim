#!/usr/bin/env python3
"""
Diagnostic: check SumoSim GUI files for known issues.
Run from the project root:  python tools/diagnose_gui.py
"""

import re
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
# If we're inside the sumosim package (tools/ is under sumosim/), go up one more
if (root / "gui").exists() and not (root / "sumosim").exists():
    # We're inside the sumosim package dir — root IS the package
    pkg = root
    root = root.parent
else:
    pkg = root / "sumosim"
issues = []

# ── 1. main_window.py ────────────────────────────────────────────
mw = pkg / "gui" / "main_window.py"
if not mw.exists():
    issues.append("MISSING: sumosim/gui/main_window.py")
else:
    src = mw.read_text(encoding='utf-8')

    # Issue 3: Font color — should NOT have white foreground for Y/O
    if 'QColor("white")' in src or "QColor('white')" in src:
        issues.append(
            "FONT COLOR BUG: main_window.py still sets white foreground for "
            "Yokozuna/Ozeki. Look for QColor('white') or QColor(\"white\") "
            "near _populate_roster_list."
        )
    elif "item.setForeground(QColor(color))" not in src:
        issues.append(
            "FONT COLOR BUG: main_window.py doesn't set foreground to rank color. "
            "Expected: item.setForeground(QColor(color))"
        )
    else:
        print("  OK  Roster font colors use rank color (not white)")

    # Issue 1 (sidebar): Images — should crop from top, not center
    if "scaledToWidth" in src and "copy(0, 0, 36, 36)" in src:
        print("  OK  Sidebar thumbnails crop from top")
    elif "KeepAspectRatioByExpanding" in src:
        issues.append(
            "IMAGE BUG: main_window.py uses center-crop for sidebar thumbnails. "
            "Should use scaledToWidth + copy(0, 0, 36, 36) for top-crop."
        )
    else:
        issues.append(
            "IMAGE UNKNOWN: Can't find expected image cropping code in main_window.py"
        )

    # Check set_roster signature has bout_records param
    if "bout_records" not in src:
        issues.append(
            "DATA BUG: main_window.py set_roster() missing bout_records parameter. "
            "H2H records won't be passed to bout_panel."
        )
    else:
        print("  OK  set_roster passes bout_records")

# ── 2. bout_panel.py ─────────────────────────────────────────────
bp = pkg / "gui" / "bout_panel.py"
if not bp.exists():
    issues.append("MISSING: sumosim/gui/bout_panel.py")
else:
    src = bp.read_text(encoding='utf-8')

    # Issue 1 (bout panel): Images — should crop from top
    if "scaledToWidth" in src and "copy(0, 0, 96, 96)" in src:
        print("  OK  Bout panel photos crop from top")
    elif "KeepAspectRatioByExpanding" in src:
        issues.append(
            "IMAGE BUG: bout_panel.py uses center-crop for wrestler photos. "
            "Should use scaledToWidth + copy(0, 0, 96, 96) for top-crop."
        )
    else:
        issues.append(
            "IMAGE UNKNOWN: Can't find expected photo code in bout_panel.py"
        )

    # Issue 2: H2H display — should have _build_h2h_index
    if "_build_h2h_index" in src:
        print("  OK  H2H index builder present")
    else:
        issues.append(
            "H2H BUG: bout_panel.py missing _build_h2h_index method. "
            "Historical records won't display."
        )

    # Check set_roster accepts bout_records
    if "bout_records" in src and "_h2h_index" in src:
        print("  OK  bout_panel.set_roster handles bout_records")
    else:
        issues.append(
            "H2H BUG: bout_panel.py set_roster() doesn't accept/process bout_records"
        )

    # Check for the old bug where simulation overwrites H2H
    # Look for _display_result restoring H2H
    if "self._h2h_index" in src and "_get_h2h_text" in src:
        # Check _display_result specifically
        display_result_match = re.search(
            r"def _display_result.*?(?=\n    def |\nclass |\Z)",
            src, re.DOTALL
        )
        if display_result_match:
            dr_src = display_result_match.group(0)
            if "_get_h2h_text" in dr_src or "self._h2h_index" in dr_src:
                print("  OK  _display_result preserves H2H record")
            else:
                issues.append(
                    "H2H BUG: _display_result replaces H2H with 'Simulation winner' "
                    "without restoring the H2H record."
                )
    
    # Check RichText format is set on h2h_label
    if "RichText" in src:
        print("  OK  H2H label uses RichText format")
    else:
        issues.append(
            "DISPLAY BUG: bout_panel.py h2h_label not set to RichText — "
            "HTML tags will show as literal text."
        )

# ── 3. Check image directory ─────────────────────────────────────
img_dir = pkg / "data" / "images" / "rikishi"
if img_dir.exists():
    jpgs = list(img_dir.glob("*.jpg"))
    print(f"  OK  Found {len(jpgs)} rikishi images in {img_dir}")
    if len(jpgs) == 0:
        issues.append("IMAGE BUG: Image directory exists but contains no .jpg files")
else:
    issues.append(
        f"IMAGE BUG: Directory not found: {img_dir}\n"
        "    Rikishi photos won't load. Expected: sumosim/data/images/rikishi/*.jpg"
    )

# ── 4. Check _rikishi_image_path function ─────────────────────────
if mw.exists():
    src = mw.read_text(encoding='utf-8')
    if "_rikishi_image_path" in src:
        # Check the path construction
        if 'os.path.join(pkg_dir, "data", "images", "rikishi"' in src:
            print("  OK  _rikishi_image_path looks correct")
        else:
            issues.append(
                "IMAGE BUG: _rikishi_image_path may use wrong path. "
                "Expected: os.path.join(pkg_dir, 'data', 'images', 'rikishi', ...)"
            )
    else:
        issues.append("IMAGE BUG: _rikishi_image_path function not found in main_window.py")

# ── 5. Check main.py entry point ─────────────────────────────────
main_py = root / "main.py"
if not main_py.exists():
    # Try sumosim/main.py
    main_py = pkg / "main.py"
if main_py.exists():
    src = main_py.read_text(encoding='utf-8')
    if "bout_records" in src and "haru_2026_bout_records" in src:
        print("  OK  main.py loads and passes bout_records")
    else:
        issues.append(
            "DATA BUG: main.py doesn't load/pass bout_records from h2h_haru2026.py. "
            "H2H records won't be available in the bout panel."
        )
else:
    issues.append("WARNING: Can't find main.py entry point")

# ── Summary ───────────────────────────────────────────────────────
print()
if issues:
    print(f"FOUND {len(issues)} ISSUE(S):")
    for i, issue in enumerate(issues, 1):
        print(f"  {i}. {issue}")
    print()
    print("Fix: Replace the affected files with the versions from the tarball,")
    print("or apply the specific fixes noted above.")
    sys.exit(1)
else:
    print("ALL CHECKS PASSED — no known issues detected.")
    print("If problems persist, check the console output when launching the app")
    print("for import errors or exceptions.")
    sys.exit(0)
