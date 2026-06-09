import pandas as pd

baked = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')
id_to_name = dict(zip(uq['cons_id_str'].astype(str), uq['fullname']))
baked['expected'] = baked['delegate_id'].astype(str).map(id_to_name)

mask = baked['fullname'] != baked['expected']
real_mismatches = baked[mask & baked['expected'].notna()]

# Inspect repr of first example
row = real_mismatches.iloc[0]
print("fullname repr:", repr(row['fullname']))
print("expected repr:", repr(row['expected']))
print()

# Check if stripping fixes it
baked['fullname_stripped'] = baked['fullname'].str.strip()
baked['expected_stripped'] = baked['expected'].str.strip()
still_mismatch = baked[baked['fullname_stripped'] != baked['expected_stripped']]
still_nan = still_mismatch['expected_stripped'].isna().sum()
still_real = still_mismatch[still_mismatch['expected_stripped'].notna()]
print(f"After strip — real mismatches: {len(still_real)}")
if len(still_real) > 0:
    row2 = still_real.iloc[0]
    print("fullname repr:", repr(row2['fullname']))
    print("expected repr:", repr(row2['expected']))
