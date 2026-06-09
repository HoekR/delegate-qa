"""
Fix corrupt cons_id_str values in the latest uq_delegates_baked parquet.

Root cause: _fill_orphans.py found delegate_ids in the baked file that were
correction-dict strings like {'to_id': '13604', ...} and used them verbatim
as cons_id_str.

Fix:
  - Parse the dict, extract to_id → use as the real cons_id_str
  - If that to_id already exists as a clean row: drop the corrupt duplicate
  - For rows with non-numeric to_id (-1, -20, republic_add_*): drop them
  - Write a new dated parquet
"""
import re, pathlib, ast
import pandas as pd
from datetime import date

_ws = pathlib.Path('.')
_uq_files = sorted(
    [p for p in _ws.glob('uq_delegates_baked_*.parquet')
     if re.fullmatch(r'uq_delegates_baked_\d{8}\.parquet', p.name)],
    reverse=True,
)
src = _uq_files[0]
print(f'Input: {src.name}  ({len(pd.read_parquet(src))} rows)')

uq = pd.read_parquet(src)

def parse_to_id(s):
    """Extract to_id from a correction-dict string."""
    try:
        d = ast.literal_eval(s)
        return str(d.get('to_id', ''))
    except Exception:
        m = re.search(r"'to_id':\s*'([^']+)'", s)
        return m.group(1) if m else ''

corrupt_mask = uq['cons_id_str'].astype(str).str.startswith('{')
clean = uq[~corrupt_mask].copy()
corrupt = uq[corrupt_mask].copy()

print(f'  Clean rows:   {len(clean)}')
print(f'  Corrupt rows: {len(corrupt)}')

corrupt['_to_id'] = corrupt['cons_id_str'].astype(str).apply(parse_to_id)

# Split: valid numeric to_id vs garbage
valid_mask = corrupt['_to_id'].str.match(r'^\d+$')
to_fix = corrupt[valid_mask].copy()
to_drop = corrupt[~valid_mask]

print(f'  → Fixable:    {len(to_fix)}  (to_id is numeric)')
print(f'  → Drop:       {len(to_drop)}  (to_id is garbage: {to_drop["_to_id"].unique()[:8].tolist()})')

# Replace cons_id_str with to_id
to_fix['cons_id_str'] = to_fix['_to_id']
to_fix = to_fix.drop(columns=['_to_id'])

# Also fix the delegate_id column if it matches the corrupt pattern
if 'delegate_id' in to_fix.columns:
    to_fix['delegate_id'] = to_fix['cons_id_str']

# Merge: prefer clean rows for any id that already exists cleanly
existing_ids = set(clean['cons_id_str'].astype(str))
new_only = to_fix[~to_fix['cons_id_str'].isin(existing_ids)]
already_covered = to_fix[to_fix['cons_id_str'].isin(existing_ids)]

print(f'  → New clean ids: {len(new_only)}')
print(f'  → Duplicate (already in clean): {len(already_covered)} — dropped')

uq_fixed = pd.concat([clean, new_only], ignore_index=True)
uq_fixed = uq_fixed.drop_duplicates(subset=['cons_id_str'])

today = date.today().strftime('%Y%m%d')
# bump date if same-day file already exists
out_path = _ws / f'uq_delegates_baked_{today}.parquet'
if out_path == src:
    # overwrite only if it's today's file already
    pass
elif out_path.exists():
    out_path = _ws / f'uq_delegates_baked_{today}b.parquet'

uq_fixed.to_parquet(out_path, index=False)
print(f'\nWrote {out_path.name}  ({len(uq_fixed)} rows, was {len(uq)})')

# Verify: no more corrupt rows
remaining = uq_fixed[uq_fixed['cons_id_str'].astype(str).str.startswith('{')]
print(f'Remaining corrupt rows: {len(remaining)}')
