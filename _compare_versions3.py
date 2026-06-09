import pandas as pd

old = pd.read_parquet('delegates_18ee_w_correcties_20260123_marked.parquet')
new = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')

# Bring birth/death years from new onto old rows via Kolom1
# (old doesn't have them; new has them from the delegate lookup)
date_lookup = new[['Kolom1','birth_year','death_year']].drop_duplicates('Kolom1')
old_w_dates = old.merge(date_lookup, on='Kolom1', how='inner')
print(f'Old rows with date info attached: {len(old_w_dates)}')

# Lifespan violations in OLD assignments
old_after_death = old_w_dates[(old_w_dates['death_year'].notna()) & (old_w_dates['j'] > old_w_dates['death_year'])]
old_before_birth = old_w_dates[(old_w_dates['birth_year'].notna()) & (old_w_dates['j'] < old_w_dates['birth_year'])]
old_violations = set(old_after_death['Kolom1']) | set(old_before_birth['Kolom1'])
print(f'Lifespan violations in OLD: {len(old_violations)} rows')

# Lifespan violations in NEW (already know ~28225 uncorrected; let's use all)
new_after_death = new[(new['death_year'].notna()) & (new['j'] > new['death_year'])]
new_before_birth = new[(new['birth_year'].notna()) & (new['j'] < new['birth_year'])]
new_violations = set(new_after_death['Kolom1']) | set(new_before_birth['Kolom1'])
print(f'Lifespan violations in NEW: {len(new_violations)} rows')
print()

# Fixed: was violation in OLD, no longer in NEW
fixed = old_violations - new_violations
print(f'FIXED by corrections: {len(fixed)} rows')

# Introduced: no violation in OLD, but violation in NEW
introduced = new_violations - old_violations
print(f'INTRODUCED by corrections: {len(introduced)} rows')

# Remained: violation in both
remained = old_violations & new_violations
print(f'Remained (still wrong): {len(remained)} rows')
print()

# Which delegates were fixed?
fixed_rows = old_w_dates[old_w_dates['Kolom1'].isin(fixed)]
print('Top fixed OLD clusters:')
print(fixed_rows.groupby('delegate_id').agg(
    n=('Kolom1','count'),
    delegate_name=('delegate_name','first'),
    j_min=('j','min'), j_max=('j','max')
).sort_values('n', ascending=False).head(15).to_string())
print()

# Which delegates were introduced (errors added)?
introduced_rows = new[new['Kolom1'].isin(introduced)]
print('Top INTRODUCED NEW clusters (errors added):')
print(introduced_rows.groupby('delegate_id').agg(
    n=('Kolom1','count'),
    delegate_name=('delegate_name','first'),
    j_min=('j','min'), j_max=('j','max')
).sort_values('n', ascending=False).head(15).to_string())
