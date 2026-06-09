import pandas as pd, json

# --- April 2025 parquet ---
apr = pd.read_parquet('/Users/rikhoekstra/gedelegeerden/output/delegates_somewhat_cleaned_up_20250415.parquet')
print('=== April 2025 parquet ===')
print('shape:', apr.shape)
# j column
print('j dtype:', apr['j'].dtype)
apr['j_num'] = pd.to_numeric(apr['j'], errors='coerce')
print('j numeric range:', apr['j_num'].min(), '-', apr['j_num'].max())
print('delegate_id sample:', apr['delegate_id'].dropna().iloc[:5].tolist())
print('delegate_id dtype:', apr['delegate_id'].dtype)
print('persoon_id sample:', apr['persoon_id'].dropna().iloc[:5].tolist())
print('id sample:', apr['id'].dropna().iloc[:5].tolist())
print()

# --- November 2024 JSON ---
nov = pd.read_json('/Users/rikhoekstra/gedelegeerden/output/delegates_1705_1794_long_20241108.json')
print('=== November 2024 JSON ===')
print('shape:', nov.shape)
print('delegate range:', nov['delegate'].min(), '-', nov['delegate'].max())
print('delegate unique count:', nov['delegate'].nunique())
print('delegate_name unique count:', nov['delegate_name'].nunique())
# extract year from date
nov['j'] = nov['date'].str[:4].astype(int)
print('year range:', nov['j'].min(), '-', nov['j'].max())
print()
print('Sample delegate->name mapping (first 10):')
print(nov.groupby('delegate')['delegate_name'].first().head(10))
