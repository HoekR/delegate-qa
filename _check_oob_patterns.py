"""
For each large violation cluster, show the top patterns in the out-of-range rows
and check whether those patterns appear in another delegate's known pattern list.
"""
import pandas as pd

df  = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq  = pd.read_parquet('uq_delegates_baked_20260423.parquet').set_index('cons_id_str')

# Build pattern -> delegate_id lookup from uq (patterns column)
pat_to_id = {}
for did, row in uq.iterrows():
    patterns_str = str(row.get('patterns', '') or '')
    for p in patterns_str.split(';'):
        p = p.strip().lower()
        if p:
            pat_to_id[p] = (did, row['fullname'])

clusters = [
    ('14024', 'post', 1720),   # Heinsius, death 1720
    ('16307', 'pre',  1728),   # Rouse, birth 1728
    ('19809', 'post', 1730),   # Reede, death 1730
    ('16189', 'post', 1780),   # Rechteren, death 1780
    ('13729', 'post', 1716),   # Merens, death 1716
    ('16120', 'post', 1759),   # Hoorn, death 1759
]

for did, direction, boundary in clusters:
    sub = df[df['delegate_id'].astype(str) == did].copy()
    if direction == 'post':
        oob = sub[sub['j'] > boundary]
    else:
        oob = sub[sub['j'] < boundary]

    name = uq.loc[did, 'fullname'] if did in uq.index else did
    print(f'\n{"="*65}')
    print(f'{name}  (id={did})  {direction}-{boundary}: {len(oob)} rows')

    top_pats = oob['pattern'].value_counts().head(10)
    print(f'{"Pattern":<35} {"count":>6}  {"→ belongs to"}')
    print('-'*65)
    for pat, cnt in top_pats.items():
        match = pat_to_id.get(str(pat).strip().lower(), None)
        if match:
            owner = f'{match[0]} {match[1][:30]}'
        else:
            owner = '(not found in uq)'
        print(f'{str(pat):<35} {cnt:>6}  {owner}')
