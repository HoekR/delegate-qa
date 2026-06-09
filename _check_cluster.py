"""
Usage:  python _check_cluster.py <delegate_id> [delegate_id ...]
        python _check_cluster.py 14024 16307 19809 16189 13729 16120

Shows year distribution, death/birth boundary, pattern variety and resolutie_refs
for each cluster — to judge whether uq dates are wrong vs. a split is needed.
"""
import sys
import pandas as pd

import re, pathlib
_ws = pathlib.Path('.')
_uq_files = sorted(
    [p for p in _ws.glob('uq_delegates_baked_*.parquet')
     if re.fullmatch(r'uq_delegates_baked_\d{8}\.parquet', p.name)],
    reverse=True,
)
_uq_path = _uq_files[0] if _uq_files else 'uq_delegates_baked_20260423.parquet'

df = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet(_uq_path).set_index('cons_id_str')
print(f'[using {_uq_path.name if hasattr(_uq_path, "name") else _uq_path}  — {len(uq)} delegates]')

ids = sys.argv[1:] if len(sys.argv) > 1 else ['14024', '16307', '19809', '16189', '13729', '16120']

for did in ids:
    did = str(did)
    sub = df[df['delegate_id'].astype(str) == did]
    if sub.empty:
        print(f'[{did}] not found'); continue

    row = uq.loc[did] if did in uq.index else None
    birth = int(row['geboortejaar']) if row is not None and pd.notna(row['geboortejaar']) else None
    death = int(row['overlijdensjaar']) if row is not None and pd.notna(row['overlijdensjaar']) else None
    name  = row['fullname'] if row is not None else sub['delegate_name'].iloc[0]
    refs  = str(row['resolutie_refs']) if row is not None else 'None'

    years = sub['j'].dropna().astype(int)
    vc = years.value_counts().sort_index()

    print(f'\n{"="*70}')
    print(f'{name}  (id={did})  birth={birth}  death={death}')
    print(f'total rows: {len(sub)}  year range: {years.min()}–{years.max()}')
    if refs not in ('None', '', 'nan'):
        print(f'resolutie_refs: {refs[:120]}')

    # Year histogram — show decades
    decade_counts = years.floordiv(10).mul(10).value_counts().sort_index()
    print('per decade:')
    for dec, cnt in decade_counts.items():
        bar = '#' * min(cnt // 10, 60)
        marker = ' <-- AFTER DEATH' if death and dec > death else (' <-- BEFORE BIRTH' if birth and dec + 9 < birth else '')
        print(f'  {dec}s: {cnt:5d}  {bar}{marker}')

    # Possible split point: where does the gap / inflection fall?
    if death:
        before = (years <= death).sum()
        after  = (years > death).sum()
        print(f'rows <= death ({death}): {before},  rows > death: {after}')
    if birth:
        before_b = (years < birth).sum()
        after_b  = (years >= birth).sum()
        print(f'rows < birth ({birth}): {before_b},  rows >= birth: {after_b}')

    # Pattern variety sample
    patterns = sub['pattern'].dropna().value_counts().head(5)
    print('top patterns:', patterns.index.tolist())
