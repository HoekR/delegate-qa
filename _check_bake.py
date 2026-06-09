import pandas as pd

baked = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')
id_to_name = dict(zip(uq['cons_id_str'].astype(str), uq['fullname']))
baked['expected'] = baked['delegate_id'].astype(str).map(id_to_name)

mask = baked['fullname'] != baked['expected']
mismatches = baked[mask]
nan_expected = mismatches['expected'].isna().sum()
real_mismatches = mismatches[mismatches['expected'].notna()]

print(f'Total mismatches: {len(mismatches)}')
print(f'  - expected is NaN (no uq entry): {nan_expected}')
print(f'  - real name mismatches: {len(real_mismatches)}')
if len(real_mismatches) > 0:
    print(real_mismatches[['delegate_id', 'fullname', 'expected']].head(10).to_string())
