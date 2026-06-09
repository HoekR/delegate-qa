"""
Apply corrections from cluster_misassigned_with_replacements.csv to approved_corrections.json.

Workflow:
  1. Edit cluster_misassigned_with_replacements.csv:
       - Confirm the right candidate by keeping/adjusting cand_id
       - Remove a row (or blank its cand_id) to skip that problem
       - Only one row per (delegate_id, offending_pattern) is used —
         if you kept multiple ranks, the one with the lowest cand_rank wins
  2. Run:  .venv/bin/python _apply_cluster_corrections.py
  3. Run:  .venv/bin/python _rebake_parquet.py

Matching logic:
  - For each confirmed row, find all baked rows where:
      delegate_id == problem delegate_id
      AND pattern starts with the offending_pattern first variant
      AND j (year) is in [viol_year_min, viol_year_max]
  - Write those row indices → {to_id: cand_id} into approved_corrections.json
"""

import json, pathlib, re
import pandas as pd
from datetime import datetime

_ws = pathlib.Path('.')

# --- load inputs ---
csv_path   = _ws / 'cluster_misassigned_with_replacements.csv'
baked_path = _ws / 'delegates_18ee_w_correcties_baked.parquet'
ac_path    = _ws / 'approved_corrections.json'

try:
    df = pd.read_csv(csv_path, sep=None, engine='python', encoding='utf-8-sig')
except Exception:
    df = pd.read_csv(csv_path, sep=None, engine='python', encoding='mac_roman')
baked = pd.read_parquet(baked_path)
ac   = json.loads(ac_path.read_text())

print(f'CSV rows: {len(df)}  |  baked rows: {len(baked):,}  |  existing corrections: {len(ac):,}')

# Keep only rows with a valid cand_id
df = df[df['cand_id'].notna() & (df['cand_id'].astype(str).str.strip() != '')].copy()
df['cand_id'] = df['cand_id'].astype(str).str.strip()

# Per (delegate_id, offending_pattern): take lowest cand_rank
df['cand_rank'] = pd.to_numeric(df['cand_rank'], errors='coerce').fillna(99)
df = df.sort_values('cand_rank').drop_duplicates(subset=['delegate_id', 'offending_pattern'])

print(f'Confirmed problems to apply: {len(df)}')

# Ensure year column is numeric
baked['_year'] = pd.to_numeric(baked['j'], errors='coerce')

now = datetime.utcnow().isoformat(timespec='seconds')
n_added = 0
n_skipped = 0
problems_applied = []

for _, row in df.iterrows():
    did      = str(row['delegate_id'])
    cand_id  = str(row['cand_id'])
    pat_first = str(row['offending_pattern']).split(';')[0].strip()
    viol_min = float(row['viol_year_min'])
    viol_max = float(row['viol_year_max'])

    # Match baked rows
    mask = (
        (baked['delegate_id'].astype(str) == did) &
        (baked['pattern'].astype(str).str.startswith(pat_first)) &
        (baked['_year'] >= viol_min) &
        (baked['_year'] <= viol_max)
    )
    matching = baked[mask]

    if matching.empty:
        print(f'  [WARN] No matching rows for {row["fullname"]} / {pat_first!r} ({viol_min:.0f}-{viol_max:.0f})')
        n_skipped += 1
        continue

    added_this = 0
    for idx in matching.index:
        key = str(idx)
        if key not in ac:
            ac[key] = {'to_id': cand_id, 'approved_at': now, 'source': 'cluster_bulk'}
            n_added += 1
            added_this += 1
        # if already exists, don't overwrite (manual corrections take precedence)

    problems_applied.append({
        'from': row['fullname'],
        'to':   row['cand_fullname'],
        'pattern': pat_first,
        'rows_corrected': added_this,
    })
    print(f'  {row["fullname"]!r:45s} → {row["cand_fullname"]!r:40s}  [{added_this} rows]')

# Backup + save
bak = ac_path.with_suffix(f'.bak_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
import shutil; shutil.copy2(ac_path, bak)
ac_path.write_text(json.dumps(ac, indent=2, ensure_ascii=False))

print()
print(f'Backup:  {bak.name}')
print(f'Added {n_added:,} new corrections across {len(problems_applied)} problems')
if n_skipped:
    print(f'Skipped (no match): {n_skipped}')
print(f'\nRun _rebake_parquet.py to apply.')
