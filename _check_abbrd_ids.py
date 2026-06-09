import pandas as pd

# Check abbrd for the shared old_id
abbrd = pd.read_parquet('abbrd.parquet')
print("Abbrd columns:", abbrd.columns.tolist())
print()

# Find entries for both delegates
for did in ['13978', '14024']:
    rows = abbrd[abbrd.apply(lambda r: did in str(r.values), axis=1)]
    print(f"Abbrd rows mentioning {did}:")
    print(rows.to_string())
    print()
