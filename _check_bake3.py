import pandas as pd

baked = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')
id_to_name = dict(zip(uq['cons_id_str'].astype(str), uq['fullname']))

baked['expected'] = baked['delegate_id'].astype(str).map(id_to_name)
mask = baked['fullname'] != baked['expected']
mismatches = baked[mask]

nan_expected = mismatches['expected'].isna().sum()
real = mismatches[mismatches['expected'].notna()]

print(f"Total mismatches: {len(mismatches)}")
print(f"  - expected NaN (not in uq): {nan_expected}")
print(f"  - real name differences: {len(real)}")
print()

# Check if 14024 is in uq
print("14024 in uq?", '14024' in id_to_name)
print("13978 in uq?", '13978' in id_to_name)
print()

# How many baked rows now have delegate_id 14024?
h_rows = baked[baked['delegate_id'].astype(str) == '14024']
print(f"Rows with delegate_id=14024 in baked: {len(h_rows)}")
print("fullname sample:", h_rows['fullname'].value_counts().head(5))

# Biggest NaN-expected delegate_ids
nan_rows = mismatches[mismatches['expected'].isna()]
print()
print("Top delegate_ids with NaN expected (not in uq):")
print(nan_rows['delegate_id'].value_counts().head(10))
