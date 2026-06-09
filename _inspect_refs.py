import pandas as pd

uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')

mask = uq['resolutie_refs'].notna()
mask2 = uq['resolutie_refs'].astype(str).str.strip().isin(['', 'None', 'nan'])
has_refs = mask & ~mask2
print(f'delegates with resolutie_refs: {has_refs.sum()} / {len(uq)}')
print()

sample = uq[has_refs][['cons_id_str', 'fullname', 'resolutie_refs', 'geboortejaar', 'overlijdensjaar']].head(15)
for _, row in sample.iterrows():
    print(row['fullname'])
    print('  refs:', str(row['resolutie_refs'])[:400])
    print()
