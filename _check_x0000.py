import pandas as pd
from pathlib import Path

files = list(Path(".").glob("*.parquet")) + list(Path(".").glob("*.xlsx"))
for f in sorted(files):
    try:
        df = pd.read_parquet(f) if f.suffix == ".parquet" else pd.read_excel(f)
    except Exception as e:
        print(f"{f.name}: ERROR {e}")
        continue
    mask = df.astype(str).apply(lambda c: c.str.contains("_x0000_", na=False)).any(axis=1)
    if mask.sum():
        print(f"\n{f.name}: {mask.sum()} rows with _x0000_")
        hit = df[mask].head(2)
        for col in hit.columns:
            vals = hit[col].astype(str)
            if vals.str.contains("_x0000_").any():
                print(f"  col={col!r}: {vals[vals.str.contains('_x0000_')].tolist()[:2]}")
    else:
        print(f"{f.name}: clean")
