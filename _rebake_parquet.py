"""
Directly fix delegates_18ee_w_correcties_baked.parquet by:
1. Parsing corrupt delegate_id values (dict strings) → extract real to_id
2. Dropping rows with non-numeric/invalid to_id (-1, -20, etc.) — keeping republic_add_* ids
3. Refreshing person columns (fullname, geboortejaar etc.) from the latest uq
4. Saving back in-place

This avoids needing to run the full Streamlit/make_parquet pipeline.
"""
import re, ast, pathlib, shutil
import pandas as pd
from datetime import datetime

_ws = pathlib.Path('.')

# --- load files ---
baked_path = _ws / 'delegates_18ee_w_correcties_baked.parquet'
uq_path = sorted(
    [p for p in _ws.glob('uq_delegates_baked_*.parquet')
     if re.fullmatch(r'uq_delegates_baked_\d{8}\.parquet', p.name)],
    reverse=True,
)[0]

print(f'Baked: {baked_path.name}')
print(f'UQ:    {uq_path.name}')

baked = pd.read_parquet(baked_path)
uq = pd.read_parquet(uq_path)

print(f'Baked rows: {len(baked):,}   uq rows: {len(uq)}')

# backup
bak = baked_path.with_name(f'delegates_18ee_w_correcties_baked.bak_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
shutil.copy2(baked_path, bak)
print(f'Backup: {bak.name}')

# --- fix corrupt delegate_ids ---
def parse_to_id(val):
    s = str(val)
    if not s.startswith('{'):
        return val
    try:
        d = ast.literal_eval(s)
        return str(d.get('to_id', val))
    except Exception:
        m = re.search(r"'to_id':\s*'([^']+)'", s)
        return m.group(1) if m else val

corrupt_mask = baked['delegate_id'].astype(str).str.startswith('{')
n_corrupt = corrupt_mask.sum()
print(f'Corrupt delegate_id rows: {n_corrupt:,}')

if n_corrupt:
    baked['delegate_id'] = baked['delegate_id'].astype(str).apply(parse_to_id)
    
    # Drop rows with truly invalid ids (negative ints, empty, unparseable dicts)
    # Keep republic_add_* ids — those are real delegates added to the republic file
    valid_mask = (
        baked['delegate_id'].str.match(r'^\d+$') |
        baked['delegate_id'].str.match(r'^republic_add_\w+$')
    )
    n_drop = (~valid_mask).sum()
    if n_drop:
        print(f'Dropping {n_drop} rows with invalid delegate_id')
        baked = baked[valid_mask].copy()
    print(f'After fix: {len(baked):,} rows')

# --- refresh person columns from uq ---
PERSON_COLS = [
    'fullname', 'voornaam', 'tussenvoegsel', 'geslachtsnaam',
    'geboortejaar', 'overlijdensjaar', 'provincie',
    'resolutie_refs', 'minjaar', 'maxjaar', 'heerlijkheid',
]

uq_id_col = 'cons_id_str'
uq[uq_id_col] = uq[uq_id_col].astype(str)
uq_idx = uq.set_index(uq_id_col)

person_cols_present = [c for c in PERSON_COLS if c in uq_idx.columns and c in baked.columns]
print(f'Refreshing person cols: {person_cols_present}')

baked['delegate_id'] = baked['delegate_id'].astype(str)

# Convert categorical columns to object before assignment
for col in person_cols_present:
    if hasattr(baked[col], 'cat'):
        baked[col] = baked[col].astype(object)

for col in person_cols_present:
    mapped = baked['delegate_id'].map(uq_idx[col])
    # Only fill where uq has a value
    has_value = mapped.notna()
    baked.loc[has_value, col] = mapped[has_value]

# Strip whitespace from all string columns
for col in baked.select_dtypes(include='object').columns:
    baked[col] = baked[col].str.strip()

# --- verify ---
uq_ids = set(uq_idx.index)
orphans = ~baked['delegate_id'].isin(uq_ids)
print(f'Orphan rows after fix: {orphans.sum():,}')

# --- save ---
baked.to_parquet(baked_path, index=False)
print(f'\nSaved {baked_path.name}  ({len(baked):,} rows)')
