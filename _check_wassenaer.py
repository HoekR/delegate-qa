import pandas as pd

# Check original (pre-bake) source
orig = pd.read_parquet('delegates_18ee_w_correcties_20260123_marked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')

# Find Wassenaer van Spanbroek delegate_id(s)
w_rows = uq[uq['fullname'].str.contains('Wassenaer', case=False, na=False)]
print("UQ rows matching Wassenaer:")
print(w_rows[['cons_id_str', 'delegate_id', 'fullname', 'pattern']].to_string())
print()

# Get their IDs
ids = set(w_rows['cons_id_str'].astype(str).tolist() + w_rows['delegate_id'].astype(str).tolist())
print("IDs to check:", ids)
print()

# Check in orig
for did in ids:
    sub = orig[orig['delegate_id'].astype(str) == did]
    if len(sub):
        print(f"delegate_id={did}  rows={len(sub)}")
        print(sub['pattern'].value_counts().head(10))
        print()
