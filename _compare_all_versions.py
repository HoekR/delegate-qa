import pandas as pd, json

uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')
date_lookup = uq[['cons_id_str','fullname','geboortejaar','overlijdensjaar']].copy()
date_lookup['delegate_id_int'] = pd.to_numeric(date_lookup['cons_id_str'], errors='coerce')
date_lookup.rename(columns={'geboortejaar':'birth','overlijdensjaar':'death'}, inplace=True)

# Also build name-based lookup (lowercase stripped)
name_lookup = date_lookup[['fullname','birth','death']].dropna(subset=['fullname']).copy()
name_lookup['name_key'] = name_lookup['fullname'].str.lower().str.strip()

def count_violations(df, did_col, j_col, label, birth_col='birth', death_col='death'):
    df2 = df.copy()
    df2[j_col] = pd.to_numeric(df2[j_col], errors='coerce')
    after = df2[(df2[death_col].notna()) & (df2[j_col] > df2[death_col])]
    before = df2[(df2[birth_col].notna()) & (df2[j_col] < df2[birth_col])]
    viols = len(set(after.index) | set(before.index))
    total = len(df2)
    print(f'{label}: {viols:6d} / {total:7d} rows violate lifespan ({100*viols/total:.1f}%)')
    return viols, total

print('=== Lifespan violations by version ===')
print()

# --- CURRENT (April 2026 baked) ---
new = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
new2 = new[~new['delegate_id'].astype(str).str.startswith('{')].copy()
new2['delegate_id'] = new2['delegate_id'].astype(str)
new_m = new2.merge(date_lookup[['cons_id_str','birth','death']].rename(columns={'cons_id_str':'delegate_id'}), on='delegate_id', how='left')
count_violations(new_m, 'delegate_id', 'j', 'Apr 2026 (current baked)')

# --- JANUARY 2026 (marked parquet) ---
old = pd.read_parquet('delegates_18ee_w_correcties_20260123_marked.parquet')
old2 = old.copy()
old2['delegate_id'] = old2['delegate_id'].astype(str)
old_m = old2.merge(date_lookup[['cons_id_str','birth','death']].rename(columns={'cons_id_str':'delegate_id'}), on='delegate_id', how='left')
count_violations(old_m, 'delegate_id', 'j', 'Jan 2026 (marked parquet)')

# --- APRIL 2025 parquet ---
apr = pd.read_parquet('/Users/rikhoekstra/gedelegeerden/output/delegates_somewhat_cleaned_up_20250415.parquet')
apr['delegate_id_str'] = apr['delegate_id'].astype(str)
apr_m = apr.merge(date_lookup[['cons_id_str','birth','death']].rename(columns={'cons_id_str':'delegate_id_str'}), on='delegate_id_str', how='left')
count_violations(apr_m, 'delegate_id_str', 'j', 'Apr 2025 (somewhat cleaned)')

# --- NOVEMBER 2024 JSON — match by delegate_name ---
nov = pd.read_json('/Users/rikhoekstra/gedelegeerden/output/delegates_1705_1794_long_20241108.json')
nov['j'] = nov['date'].str[:4].astype(int)
nov['name_key'] = nov['delegate_name'].str.lower().str.strip()
nov_m = nov.merge(name_lookup[['name_key','birth','death']], on='name_key', how='left')
matched = nov_m[nov_m['birth'].notna() | nov_m['death'].notna()]
print(f'Nov 2024 JSON: {len(matched):6d} / {len(nov_m):7d} rows matched by name ({100*len(matched)/len(nov_m):.1f}%)')
count_violations(matched, 'name_key', 'j', 'Nov 2024 (name-matched subset)')

print()
print('=== Top violating clusters in April 2025 (before Jan 2026 corrections) ===')
after_apr = apr_m[(apr_m['death'].notna()) & (apr_m['j'] > apr_m['death'])]
before_apr = apr_m[(apr_m['birth'].notna()) & (apr_m['j'] < apr_m['birth'])]
viols_apr = pd.concat([after_apr, before_apr]).drop_duplicates()
by_del = viols_apr.groupby('delegate_id_str').agg(
    n=('j','count'),
    delegate_name=('delegate_name','first'),
    birth=('birth','first'), death=('death','first'),
    j_min=('j','min'), j_max=('j','max')
).sort_values('n', ascending=False)
print(by_del.head(20).to_string())
