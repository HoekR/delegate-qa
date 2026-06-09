import pandas as pd
import glob

for path in ['delegates_18ee_w_correcties_baked.parquet',
             'uq_delegates_baked_20260423.parquet',
             'uq_delegates_baked_20260421.parquet',
             'uq_delegates_baked_20260420.parquet']:
    import os
    if not os.path.exists(path):
        continue
    df = pd.read_parquet(path)
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()
    df.to_parquet(path, index=False)
    print(f'Stripped: {path}  ({len(df):,} rows)')
