import pandas as pd

new = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')

date_lookup = uq[['cons_id_str','geboortejaar','overlijdensjaar']].rename(columns={
    'cons_id_str': 'delegate_id',
    'geboortejaar': 'birth_year_uq',
    'overlijdensjaar': 'death_year_uq'
})
date_lookup['delegate_id'] = date_lookup['delegate_id'].astype(str)

new2 = new.copy()
new2['did_str'] = new2['delegate_id'].astype(str)
new_clean = new2[~new2['did_str'].str.startswith('{')].copy()
new_clean['delegate_id'] = new_clean['did_str']
nw = new_clean.merge(date_lookup, on='delegate_id', how='left')

after = nw[(nw['death_year_uq'].notna()) & (nw['j'] > nw['death_year_uq'])]
before = nw[(nw['birth_year_uq'].notna()) & (nw['j'] < nw['birth_year_uq'])]
viols = pd.concat([after, before]).drop_duplicates('Kolom1')

by_del = viols.groupby('delegate_id').agg(
    n=('Kolom1','count'),
    delegate_name=('delegate_name','first'),
    birth=('birth_year_uq','first'), death=('death_year_uq','first'),
    j_min=('j','min'), j_max=('j','max'),
    correcties_nonull=('correcties', lambda x: x.notna().sum())
).reset_index().sort_values('n', ascending=False)

print(f'Total remaining violations: {len(viols)} rows across {len(by_del)} delegates')
print()
print('All remaining problem clusters:')
print(by_del.to_string())
