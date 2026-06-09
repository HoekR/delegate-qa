import pandas as pd, json, pathlib

orig = pd.read_parquet('delegates_18ee_w_correcties_20260123_marked.parquet')

# Rows assigned to Wassenaer but with Heinsius patterns
# (Heynsius/Heinsius/Hin-sius etc. — everything that is NOT Spanbroeck/Wassenaer)
mask_was = orig['delegate_id'].astype(str) == '13978'
sub = orig[mask_was].copy()

# Patterns that look like Heinsius (not Spanbroeck/Wassenaer)
heinsius_pat = sub[~sub['pattern'].str.contains(
    r'[Ss]pan|[Ww]ass', na=False, regex=True
)]
spanbroek_pat = sub[sub['pattern'].str.contains(
    r'[Ss]pan|[Ww]ass', na=False, regex=True
)]

print(f"Total rows for delegate 13978: {len(sub)}")
print(f"  Heinsius-pattern rows (to reassign to 14024): {len(heinsius_pat)}")
print(f"  Spanbroek/Wassenaer rows (keep as 13978): {len(spanbroek_pat)}")
print()
print("Year range of Heinsius-pattern rows:")
print(f"  min j={heinsius_pat['j'].min()}, max j={heinsius_pat['j'].max()}")
print("Year range of Spanbroek rows:")
print(f"  min j={spanbroek_pat['j'].min()}, max j={spanbroek_pat['j'].max()}")
print()
print("Top patterns in Heinsius group:")
print(heinsius_pat['pattern'].value_counts().head(15))
