#!/usr/bin/env python3
"""
SumoSim: Classify wrestler fighting styles from kimarite data.

Analyzes each wrestler's winning techniques to determine if they are:
  - oshi (pushing/thrusting) — majority wins by oshi techniques
  - yotsu (belt grappling) — majority wins by yotsu techniques
  - hybrid — mixed or neither dominant

Usage:
    python -m tools.classify_styles              # Preview classifications
    python -m tools.classify_styles --apply      # Write to database
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Technique classifications
OSHI_TECHNIQUES = {
    "oshidashi", "tsukidashi", "tsukiotoshi", "hatakikomi", "hikiotoshi",
    "okuridashi", "oshitaoshi", "tsukitaoshi", "hikkake",
}

YOTSU_TECHNIQUES = {
    "yorikiri", "uwatenage", "shitatenage", "kotenage", "sukuinage",
    "yoritaoshi", "uwatedashinage", "shitatedashinage", "sotogake",
    "uchigake", "uwatehineri", "shitatehineri", "koshinage",
    "kubinage", "abisetaoshi", "tsuridashi", "tsuriotoshi",
}

# Threshold: if one style accounts for >= this fraction of wins, classify as that style
DOMINANCE_THRESHOLD = 0.55


def classify_style(oshi_count: int, yotsu_count: int, total: int) -> str:
    """Determine fighting style from technique counts."""
    if total == 0:
        return "hybrid"

    oshi_pct = oshi_count / total
    yotsu_pct = yotsu_count / total

    if oshi_pct >= DOMINANCE_THRESHOLD:
        return "oshi"
    elif yotsu_pct >= DOMINANCE_THRESHOLD:
        return "yotsu"
    else:
        return "hybrid"


def main():
    parser = argparse.ArgumentParser(
        description="Classify wrestler fighting styles from kimarite data"
    )
    parser.add_argument("--apply", action="store_true",
                        help="Write classifications to database")
    parser.add_argument("--threshold", type=float, default=DOMINANCE_THRESHOLD,
                        help=f"Dominance threshold (default: {DOMINANCE_THRESHOLD})")
    args = parser.parse_args()

    # Load .env
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        import os
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

    import sqlite3
    from data.db import SumoDatabase

    db = SumoDatabase()
    conn = db._local_conn()

    # Get all wrestlers
    wrestlers = conn.execute(
        "SELECT wrestler_id, shikona, fighting_style FROM wrestlers"
    ).fetchall()

    # Analyze kimarite for each wrestler
    results = []
    for w in wrestlers:
        wid = w["wrestler_id"]
        shikona = w["shikona"]
        current_style = w["fighting_style"]

        # Count winning techniques
        rows = conn.execute(
            """SELECT kimarite, COUNT(*) as cnt
               FROM bout_records
               WHERE winner_id = ? AND kimarite IS NOT NULL AND kimarite != ''
               GROUP BY kimarite""",
            (wid,),
        ).fetchall()

        if not rows:
            continue

        oshi_count = 0
        yotsu_count = 0
        total = 0

        for r in rows:
            technique = r["kimarite"].lower()
            count = r["cnt"]
            total += count
            if technique in OSHI_TECHNIQUES:
                oshi_count += count
            elif technique in YOTSU_TECHNIQUES:
                yotsu_count += count

        new_style = classify_style(oshi_count, yotsu_count, total)

        if total >= 5:  # Only classify wrestlers with enough data
            results.append({
                "wrestler_id": wid,
                "shikona": shikona,
                "old_style": current_style,
                "new_style": new_style,
                "oshi_count": oshi_count,
                "yotsu_count": yotsu_count,
                "other_count": total - oshi_count - yotsu_count,
                "total": total,
                "oshi_pct": oshi_count / total * 100,
                "yotsu_pct": yotsu_count / total * 100,
                "changed": current_style != new_style,
            })

    # Summary
    styles = {"oshi": 0, "yotsu": 0, "hybrid": 0}
    changed = 0
    for r in results:
        styles[r["new_style"]] += 1
        if r["changed"]:
            changed += 1

    print(f"Classified {len(results)} wrestlers (threshold: {args.threshold:.0%})")
    print(f"  Oshi:   {styles['oshi']}")
    print(f"  Yotsu:  {styles['yotsu']}")
    print(f"  Hybrid: {styles['hybrid']}")
    print(f"  Changed from current: {changed}")

    # Show changes
    print(f"\nChanges:")
    for r in sorted(results, key=lambda x: -x["total"]):
        if r["changed"]:
            print(f"  {r['shikona']:<18} {r['old_style']:<8} -> {r['new_style']:<8} "
                  f"(oshi:{r['oshi_pct']:.0f}% yotsu:{r['yotsu_pct']:.0f}% "
                  f"n={r['total']})")

    if args.apply:
        print(f"\nApplying to database...")
        for r in results:
            conn.execute(
                "UPDATE wrestlers SET fighting_style = ? WHERE wrestler_id = ?",
                (r["new_style"], r["wrestler_id"]),
            )
        conn.commit()
        print(f"  Updated {len(results)} wrestlers in local DB")

        # Also push to Supabase
        if db.is_online:
            print("  Pushing to Supabase...")
            errors = 0
            for r in results:
                try:
                    resp = db._http.patch(
                        f"/wrestlers?wrestler_id=eq.{r['wrestler_id']}",
                        json={"fighting_style": r["new_style"]},
                    )
                    resp.raise_for_status()
                except Exception as e:
                    errors += 1
                    if errors <= 3:
                        print(f"    Error: {e}")
            if errors:
                print(f"  {errors} Supabase errors")
            print("  Supabase updated")

    conn.close()

    if not args.apply:
        print(f"\nDry run — use --apply to write changes")


if __name__ == "__main__":
    main()
