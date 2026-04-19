"""
Quick diagnostic: run this from your SumoSim project root to see
what data the Rikishi Dossier panel is working with.

Usage:  python diagnose_dossier.py
"""
import sqlite3
from pathlib import Path

# Try to find the database
candidates = [
    Path("data/sumosim_local.db"),
    Path("sumosim/data/sumosim_local.db"),
    Path("sumosim_local.db"),
]
db_path = None
for p in candidates:
    if p.exists():
        db_path = p
        break

if not db_path:
    print("ERROR: Cannot find sumosim_local.db")
    print("Tried:", [str(c) for c in candidates])
    exit(1)

print(f"Database: {db_path}  ({db_path.stat().st_size / 1024:.0f} KB)\n")

conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row

# 1. Schema check — what columns does the wrestlers table have?
cols = conn.execute("PRAGMA table_info(wrestlers)").fetchall()
col_names = [c[1] for c in cols]
print(f"=== WRESTLERS TABLE: {len(col_names)} columns ===")
migration_cols = ["rank_value", "rank_label", "division", "shikona_en",
                  "makuuchi_wins", "juryo_wins", "yusho_makuuchi", "yusho_juryo"]
for mc in migration_cols:
    status = "YES" if mc in col_names else "NO"
    print(f"  {mc}: {status}")

# 2. Wrestler counts
total = conn.execute("SELECT COUNT(*) FROM wrestlers").fetchone()[0]
active = conn.execute("SELECT COUNT(*) FROM wrestlers WHERE is_active = 1").fetchone()[0]
print(f"\nTotal wrestlers: {total}  |  Active: {active}")

# 3. Check what rank data looks like for active wrestlers
print("\n=== ACTIVE WRESTLER RANK DATA (first 15) ===")
print(f"{'ID':<8} {'Shikona':<18} {'current_rank':<14} {'rank_num':<9} {'side':<6}", end="")
if "rank_label" in col_names:
    print(f" {'rank_label':<24} {'rank_value':<10} {'division':<10}", end="")
print()
print("-" * 120)

query = "SELECT * FROM wrestlers WHERE is_active = 1 ORDER BY "
if "rank_value" in col_names:
    query += "rank_value, shikona LIMIT 15"
else:
    query += "current_rank, current_rank_number LIMIT 15"

for r in conn.execute(query).fetchall():
    print(f"{r['wrestler_id']:<8} {r['shikona']:<18} {str(r['current_rank'] or ''):<14} "
          f"{str(r['current_rank_number'] or ''):<9} {str(r['current_side'] or ''):<6}", end="")
    if "rank_label" in col_names:
        rl = r['rank_label'] if 'rank_label' in col_names else ''
        rv = r['rank_value'] if 'rank_value' in col_names else ''
        dv = r['division'] if 'division' in col_names else ''
        print(f" {str(rl or ''):<24} {str(rv or ''):<10} {str(dv or ''):<10}", end="")
    print()

# 4. Banzuke table check
print("\n=== BANZUKE TABLE ===")
basho_counts = conn.execute(
    "SELECT basho_id, COUNT(*) as cnt FROM banzuke GROUP BY basho_id ORDER BY basho_id DESC"
).fetchall()
if basho_counts:
    for row in basho_counts[:5]:
        print(f"  Basho {row['basho_id']}: {row['cnt']} entries")
    latest = basho_counts[0]['basho_id']

    # Check divisions in latest banzuke
    div_counts = conn.execute(
        "SELECT division, COUNT(*) as cnt FROM banzuke WHERE basho_id = ? GROUP BY division",
        (latest,)
    ).fetchall()
    print(f"\n  Latest banzuke ({latest}) divisions:")
    for d in div_counts:
        print(f"    {d['division']}: {d['cnt']}")

    # Show first few banzuke entries
    print(f"\n  First 10 entries for {latest}:")
    for r in conn.execute(
        "SELECT wrestler_id, rank, rank_number, side, division FROM banzuke WHERE basho_id = ? ORDER BY rank_number LIMIT 10",
        (latest,)
    ).fetchall():
        print(f"    {r['wrestler_id']:<8} {r['rank']:<14} {str(r['rank_number'] or ''):<4} {str(r['side'] or ''):<6} {r['division']}")
else:
    print("  EMPTY — no banzuke data at all!")

# 5. Check what get_all_wrestlers would return with the current logic
print("\n=== SIMULATION: What get_all_wrestlers would return ===")
has_rank_value = "rank_value" in col_names
has_division = "division" in col_names

latest_basho_row = conn.execute("SELECT MAX(basho_id) AS latest FROM banzuke").fetchone()
latest_basho = latest_basho_row["latest"] if latest_basho_row else None

banzuke_count = 0
if latest_basho:
    banzuke_count = conn.execute(
        "SELECT COUNT(*) FROM banzuke b JOIN wrestlers w ON b.wrestler_id = w.wrestler_id "
        "WHERE b.basho_id = ? AND lower(b.division) IN ('makuuchi', 'juryo')",
        (latest_basho,)
    ).fetchone()[0]

print(f"  Latest basho in banzuke: {latest_basho}")
print(f"  Banzuke JOIN match count: {banzuke_count}")
print(f"  Would use banzuke JOIN: {'YES' if banzuke_count >= 20 else 'NO'}")
print(f"  Has rank_value column: {has_rank_value}")
print(f"  Has division column: {has_division}")

if banzuke_count < 20:
    print("\n  Fallback query would be used. Testing it...")
    if has_rank_value:
        fallback_count = conn.execute(
            "SELECT COUNT(*) FROM wrestlers WHERE is_active = 1 AND rank_value IS NOT NULL"
        ).fetchone()[0]
        print(f"  Wrestlers with rank_value set: {fallback_count}")

        null_rv = conn.execute(
            "SELECT COUNT(*) FROM wrestlers WHERE is_active = 1 AND rank_value IS NULL"
        ).fetchone()[0]
        print(f"  Active wrestlers with NULL rank_value: {null_rv}")

    if has_division:
        div_match = conn.execute(
            "SELECT COUNT(*) FROM wrestlers WHERE is_active = 1 AND lower(division) IN ('makuuchi', 'juryo')"
        ).fetchone()[0]
        print(f"  Active with division Makuuchi/Juryo: {div_match}")

        null_div = conn.execute(
            "SELECT COUNT(*) FROM wrestlers WHERE is_active = 1 AND division IS NULL"
        ).fetchone()[0]
        print(f"  Active with NULL division: {null_div}")

    cr_match = conn.execute(
        "SELECT COUNT(*) FROM wrestlers WHERE is_active = 1 AND current_rank IS NOT NULL"
    ).fetchone()[0]
    print(f"  Active with current_rank set: {cr_match}")

conn.close()
print("\nDone.")
