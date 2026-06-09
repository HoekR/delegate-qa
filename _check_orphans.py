import pandas as pd, glob

baked = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')
known_ids = set(uq['cons_id_str'].astype(str))

orphan_ids = set(baked[~baked['delegate_id'].astype(str).isin(known_ids)]['delegate_id'].astype(str).unique())
print(f"Orphan delegate_ids: {len(orphan_ids)}")
print()

# Check all other uq/delegate parquet files
for f in sorted(glob.glob('*.parquet')):
    try:
        df = pd.read_parquet(f)
        for col in ['cons_id_str', 'delegate_id', 'id_latest', 'id_persoon']:
            if col in df.columns:
                found = orphan_ids & set(df[col].astype(str).unique())
                if found:
                    print(f"{f}  [{col}]  covers {len(found)} orphan ids")
                    break
    except Exception:
        pass

print()
# Sample the top orphans and show their fullname/pattern in baked
top_orphan_ids = baked[~baked['delegate_id'].astype(str).isin(known_ids)]['delegate_id'].value_counts().head(10).index
sub = baked[baked['delegate_id'].astype(str).isin(top_orphan_ids.astype(str))]
print("Sample orphan rows (fullname + pattern):")
print(sub.groupby('delegate_id')[['fullname','pattern']].first().to_string())
