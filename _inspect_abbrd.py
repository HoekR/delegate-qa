import pandas as pd
p = "/Users/rikhoekstra/surfdrive (2)/Republic/gedelegeerden/abbrd.xlsx"
df = pd.read_excel(p, nrows=5)
print("Columns:", list(df.columns))
print()
print(df.head(3).to_string())

# Also check existing republic_add IDs in persons file
pf = "/Users/rikhoekstra/surfdrive (2)/Republic/gedelegeerden/output/uq_delegates_updated_20260225.xlsx"
dfp = pd.read_excel(pf)
print("\nPersons columns:", list(dfp.columns))
# Find any id column with republic_add pattern
id_col = next((c for c in dfp.columns if "id" in c.lower()), None)
if id_col:
    mask = dfp[id_col].astype(str).str.contains("republic_add", na=False)
    print(f"Existing republic_add entries in {id_col}:", dfp.loc[mask, id_col].tolist())
