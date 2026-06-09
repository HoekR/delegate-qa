import pandas as pd

uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')
new = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')

# Build delegate_name -> (birth, death) from baked parquet + uq
# Use uq for dates, new for delegate_name <-> cons_id_str mapping
uq_dates = uq[['cons_id_str','geboortejaar','overlijdensjaar']].rename(columns={
    'geboortejaar':'birth','overlijdensjaar':'death'})

# delegate_name -> delegate_id from baked parquet
name_to_id = new[['delegate_name','delegate_id']].dropna().copy()
name_to_id['delegate_id'] = name_to_id['delegate_id'].astype(str)
name_to_id = name_to_id[~name_to_id['delegate_id'].str.startswith('{')]
name_to_id = name_to_id.drop_duplicates('delegate_name')
name_to_id = name_to_id.merge(
    uq_dates.rename(columns={'cons_id_str':'delegate_id'}), on='delegate_id', how='left')
name_to_id['name_key'] = name_to_id['delegate_name'].astype(str).str.lower().str.strip()

print(f'Name->date lookup entries: {len(name_to_id)}, with dates: {name_to_id["birth"].notna().sum()}')

def viols_for_file(df, j_col, name_col, label):
    df2 = df.copy()
    df2['name_key'] = df2[name_col].astype(str).str.lower().str.strip()
    df2['j_n'] = pd.to_numeric(df2[j_col], errors='coerce')
    m = df2.merge(name_to_id[['name_key','birth','death']], on='name_key', how='left')
    with_dates = m[m['birth'].notna() | m['death'].notna()]
    after  = with_dates[(with_dates['death'].notna()) & (with_dates['j_n'] > with_dates['death'])]
    before = with_dates[(with_dates['birth'].notna()) & (with_dates['j_n'] < with_dates['birth'])]
    viols = len(set(after.index) | set(before.index))
    matchpct = 100*len(with_dates)/len(m)
    print(f'{label}:')
    print(f'  total rows: {len(df2):7d}  |  name-matched: {len(with_dates):7d} ({matchpct:.0f}%)')
    print(f'  lifespan violations: {viols:6d} ({100*viols/max(len(with_dates),1):.1f}% of matched)')
    print()
    return viols, len(with_dates)

print()
print('=== Lifespan violations across all versions ===')
print()

# Nov 2024 JSON
nov = pd.read_json('/Users/rikhoekstra/gedelegeerden/output/delegates_1705_1794_long_20241108.json')
nov['j'] = nov['date'].str[:4].astype(int)
viols_for_file(nov, 'j', 'delegate_name', 'Nov 2024 JSON (delegates_1705_1794_long)')

# Apr 2025 parquet — use delegate_name column
apr = pd.read_parquet('/Users/rikhoekstra/gedelegeerden/output/delegates_somewhat_cleaned_up_20250415.parquet')
viols_for_file(apr, 'j', 'delegate_name', 'Apr 2025 (somewhat_cleaned_up)')

# Jan 2026 marked parquet
old = pd.read_parquet('delegates_18ee_w_correcties_20260123_marked.parquet')
viols_for_file(old, 'j', 'delegate_name', 'Jan 2026 (marked parquet)')

# Apr 2026 current baked
new2 = new[~new['delegate_id'].astype(str).str.startswith('{')].copy()
viols_for_file(new2, 'j', 'delegate_name', 'Apr 2026 (current baked)')
