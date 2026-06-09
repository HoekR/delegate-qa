"""
Scan all delegates for out-of-lifespan rows and write a ranked CSV.

Output columns:
  delegate_id, fullname, birth, death,
  total_rows, rows_before_birth, rows_after_death, violation_rows,
  top_patterns_violations  (semicolon-separated top-5 patterns from violation rows)
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
print(f'Using persons file: {_uq_path}')

df   = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq   = pd.read_parquet(_uq_path)

# Normalise id column
id_col = 'cons_id_str' if 'cons_id_str' in uq.columns else 'delegate_id'
uq[id_col] = uq[id_col].astype(str)
df['delegate_id'] = df['delegate_id'].astype(str)

records = []

for did, grp in df.groupby('delegate_id'):
    uq_row = uq[uq[id_col] == did]
    if uq_row.empty:
        continue
    r = uq_row.iloc[0]

    birth = r.get('geboortejaar', None)
    death = r.get('overlijdensjaar', None)
    try:
        birth = int(birth) if pd.notna(birth) else None
    except (ValueError, TypeError):
        birth = None
    try:
        death = int(death) if pd.notna(death) else None
    except (ValueError, TypeError):
        death = None

    if birth is None and death is None:
        continue

    years = grp['j'].dropna()
    before = int((years < birth).sum())  if birth else 0
    after  = int((years > death).sum())  if death else 0
    viols  = before + after

    if viols == 0:
        continue

    # Top patterns in violation rows
    viol_mask = pd.Series(False, index=grp.index)
    if birth:
        viol_mask |= grp['j'] < birth
    if death:
        viol_mask |= grp['j'] > death
    top_pats = (
        grp.loc[viol_mask, 'pattern']
        .dropna()
        .value_counts()
        .head(5)
        .index.tolist()
    )

    records.append({
        'delegate_id':           did,
        'fullname':              r.get('fullname', ''),
        'birth':                 birth,
        'death':                 death,
        'total_rows':            len(grp),
        'rows_before_birth':     before,
        'rows_after_death':      after,
        'violation_rows':        viols,
        'top_patterns_violations': '; '.join(str(p) for p in top_pats),
    })

out = pd.DataFrame(records).sort_values('violation_rows', ascending=False)
out_path = 'cluster_violations.csv'
out.to_csv(out_path, index=False)
print(f'Written {out_path}  ({len(out)} delegates with violations)')
print()
print(out.head(20).to_string(index=False))
