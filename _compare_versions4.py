import pandas as pd

old = pd.read_parquet('delegates_18ee_w_correcties_20260123_marked.parquet')
new = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')

# Build delegate -> birth/death year lookup from uq
date_lookup = uq[['cons_id_str','geboortejaar','overlijdensjaar']].rename(columns={
    'cons_id_str': 'delegate_id',
    'geboortejaar': 'birth_year',
    'overlijdensjaar': 'death_year'
})
date_lookup['delegate_id'] = date_lookup['delegate_id'].astype(str)

print(f'Delegates with birth year: {date_lookup["birth_year"].notna().sum()}')
print(f'Delegates with death year: {date_lookup["death_year"].notna().sum()}')
print()

# --- OLD dataset ---
old2 = old.copy()
old2['delegate_id'] = old2['delegate_id'].astype(str)
old_w_dates = old2.merge(date_lookup, on='delegate_id', how='left')

old_after = old_w_dates[(old_w_dates['death_year'].notna()) & (old_w_dates['j'] > old_w_dates['death_year'])]
old_before = old_w_dates[(old_w_dates['birth_year'].notna()) & (old_w_dates['j'] < old_w_dates['birth_year'])]
old_viols = set(old_after['Kolom1']) | set(old_before['Kolom1'])
print(f'OLD lifespan violations: {len(old_viols)} rows ({100*len(old_viols)/len(old2):.1f}%)')
print(f'  after death: {len(old_after)}, before birth: {len(old_before)}')

# --- NEW dataset (clean non-dict delegate_ids only) ---
new2 = new.copy()
new2['did_str'] = new2['delegate_id'].astype(str)
new_clean = new2[~new2['did_str'].str.startswith('{')].copy()
new_clean['delegate_id'] = new_clean['did_str']
new_w_dates = new_clean.merge(date_lookup, on='delegate_id', how='left')

new_after = new_w_dates[(new_w_dates['death_year_y'].notna()) & (new_w_dates['j'] > new_w_dates['death_year_y'])]
new_before = new_w_dates[(new_w_dates['birth_year_y'].notna()) & (new_w_dates['j'] < new_w_dates['birth_year_y'])]
new_viols = set(new_after['Kolom1']) | set(new_before['Kolom1'])
print(f'NEW lifespan violations: {len(new_viols)} rows ({100*len(new_viols)/len(new_clean):.1f}%)')
print(f'  after death: {len(new_after)}, before birth: {len(new_before)}')
print()

fixed = old_viols - new_viols
introduced = new_viols - old_viols
remained = old_viols & new_viols
print(f'FIXED by corrections:     {len(fixed):6d} rows')
print(f'INTRODUCED by corrections:{len(introduced):6d} rows')
print(f'Remained (still wrong):   {len(remained):6d} rows')
print()

# Top OLD clusters that were FIXED
fixed_rows = old_w_dates[old_w_dates['Kolom1'].isin(fixed)]
print('Top FIXED OLD clusters:')
print(fixed_rows.groupby('delegate_id').agg(
    n=('Kolom1','count'),
    delegate_name=('delegate_name','first'),
    j_min=('j','min'), j_max=('j','max'),
    birth=('birth_year','first'), death=('death_year','first')
).sort_values('n', ascending=False).head(15).to_string())
print()

# Top NEW clusters that were INTRODUCED
introd_rows = new_w_dates[new_w_dates['Kolom1'].isin(introduced)]
print('Top INTRODUCED NEW clusters (new errors):')
print(introd_rows.groupby('delegate_id').agg(
    n=('Kolom1','count'),
    delegate_name=('delegate_name','first'),
    j_min=('j','min'), j_max=('j','max'),
    birth=('birth_year_y','first'), death=('death_year_y','first')
).sort_values('n', ascending=False).head(15).to_string())
