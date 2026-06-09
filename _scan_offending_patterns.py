"""
For each delegate with lifespan violations, identify patterns that appear
predominantly or exclusively in the out-of-lifespan rows.

Output: cluster_offending_patterns.csv with columns:
  delegate_id, fullname, birth, death,
  offending_pattern, total_in_pattern, in_violation, pct_violation,
  viol_year_min, viol_year_max, ok_year_min, ok_year_max
"""
import re, pathlib
import pandas as pd

_ws = pathlib.Path('.')
_uq_files = sorted(
    [p for p in _ws.glob('uq_delegates_baked_*.parquet')
     if re.fullmatch(r'uq_delegates_baked_\d{8}\.parquet', p.name)],
    reverse=True,
)
_uq_path = _uq_files[0] if _uq_files else 'uq_delegates_updated_20260225.parquet'
print(f'Using: {_uq_path}')

df  = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq  = pd.read_parquet(_uq_path)

id_col = 'cons_id_str' if 'cons_id_str' in uq.columns else 'delegate_id'
uq[id_col] = uq[id_col].astype(str)
df['delegate_id'] = df['delegate_id'].astype(str)

records = []

for did, grp in df.groupby('delegate_id'):
    uq_row = uq[uq[id_col] == did]
    if uq_row.empty:
        continue
    r = uq_row.iloc[0]

    try:
        birth = int(r['geboortejaar']) if pd.notna(r.get('geboortejaar')) else None
    except (ValueError, TypeError):
        birth = None
    try:
        death = int(r['overlijdensjaar']) if pd.notna(r.get('overlijdensjaar')) else None
    except (ValueError, TypeError):
        death = None

    if birth is None and death is None:
        continue

    viol_mask = pd.Series(False, index=grp.index)
    if birth:
        viol_mask |= grp['j'] < birth
    if death:
        viol_mask |= grp['j'] > death

    if viol_mask.sum() == 0:
        continue

    viol = grp[viol_mask]
    ok   = grp[~viol_mask]
    fullname = r.get('fullname', '')

    for pat, pat_grp in grp.groupby('pattern'):
        n_total = len(pat_grp)
        n_viol  = viol_mask.loc[pat_grp.index].sum()
        pct     = n_viol / n_total if n_total else 0

        # Only flag patterns where ≥80% of occurrences are violations
        # and there are at least 3 rows
        if pct < 0.8 or n_total < 3:
            continue

        viol_pat = pat_grp[viol_mask.loc[pat_grp.index]]
        ok_pat   = pat_grp[~viol_mask.loc[pat_grp.index]]

        records.append({
            'delegate_id':      did,
            'fullname':         fullname,
            'birth':            birth,
            'death':            death,
            'offending_pattern': pat,
            'total_in_pattern': n_total,
            'in_violation':     int(n_viol),
            'pct_violation':    round(pct, 3),
            'viol_year_min':    int(viol_pat['j'].min()) if len(viol_pat) else None,
            'viol_year_max':    int(viol_pat['j'].max()) if len(viol_pat) else None,
            'ok_year_min':      int(ok_pat['j'].min()) if len(ok_pat) else None,
            'ok_year_max':      int(ok_pat['j'].max()) if len(ok_pat) else None,
        })

out = pd.DataFrame(records).sort_values(
    ['in_violation', 'pct_violation'], ascending=False
)
out_path = 'cluster_offending_patterns.csv'
out.to_csv(out_path, index=False)
print(f'Written {out_path}  ({len(out)} offending pattern-delegate pairs)')
print()
# Show summary: total violation rows accounted for
total_viol_rows = out['in_violation'].sum()
print(f'Total violation rows accounted for: {total_viol_rows:,}')
print()
print(out[['delegate_id','fullname','birth','death',
           'offending_pattern','total_in_pattern','in_violation',
           'pct_violation','viol_year_min','viol_year_max']].head(30).to_string(index=False))
