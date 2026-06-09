# Bake integrity check.
# Corrections only change delegate_id, not fullname (occurrences keep their
# original transcription). So we check structural integrity, not name equality.
import pandas as pd

baked = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260424.parquet')
known_ids = set(uq['cons_id_str'].astype(str))

# 1. Rows whose delegate_id is unknown (not in uq at all)
orphans = baked[~baked['delegate_id'].astype(str).isin(known_ids)]
print(f"Orphan rows (delegate_id not in uq): {len(orphans)} / {len(baked)}")
if len(orphans):
    print("  Top orphan delegate_ids:", orphans['delegate_id'].value_counts().head(5).to_dict())

# 2. Duplicate assignments: same original index assigned to >1 delegate
dupes = baked.index.duplicated().sum()
print(f"Duplicate index rows: {dupes}")

# 3. Quick counts
print(f"Total rows: {len(baked):,}  |  Unique delegate_ids: {baked['delegate_id'].nunique():,}")