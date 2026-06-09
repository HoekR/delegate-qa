import pandas as pd

uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')
h = uq[uq['fullname'].str.contains('Heinsius|Heynsius', case=False, na=False)]
print("Heinsius candidates in UQ:")
print(h[['cons_id_str', 'delegate_id', 'fullname', 'pattern']].to_string())
print()

# Also check patterns in uq that contain Heynsius/Heinsius
p = uq[uq['pattern'].str.contains('Heynsius|Heinsius', case=False, na=False)]
print("UQ rows with Heynsius/Heinsius in pattern:")
print(p[['cons_id_str', 'delegate_id', 'fullname', 'pattern']].to_string())
