import pandas as pd
from pathlib import Path

BASEDIR = Path("/Users/rikhoekstra/surfdrive (2)/Republic/gedelegeerden")

print("=== df_p columns ===")
df_p = pd.read_excel(BASEDIR / 'output/uq_delegates_updated_20260225.xlsx', nrows=3)
print(list(df_p.columns))
print(df_p.head(3).to_string())

print("\n=== df_i columns ===")
df_i = pd.read_excel(BASEDIR / 'output/delegates_18ee_w_correcties_20260123_marked.xlsx', nrows=3)
print(list(df_i.columns))
print(df_i.head(3).to_string())

print("\n=== looking for bio/add file ===")
for pattern in ['*add*', '*bio*', '*leven*', '*geboren*', '*1700*']:
    hits = list(BASEDIR.glob(pattern)) + list((BASEDIR / 'output').glob(pattern))
    if hits:
        print(pattern, [h.name for h in hits])
