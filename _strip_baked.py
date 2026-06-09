import pandas as pd

path = 'delegates_18ee_w_correcties_baked.parquet'
df = pd.read_parquet(path)
for col in df.select_dtypes(include='object').columns:
    df[col] = df[col].str.strip()
df.to_parquet(path, index=False)
print(f'Stripped and saved: {len(df):,} rows')
