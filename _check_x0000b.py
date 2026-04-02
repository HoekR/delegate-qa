import pandas as pd
from utils import load_data, build_merged, enrich_persons_from_abbrd

df_p, df_i, df_bio = load_data()
df_p2, _ = enrich_persons_from_abbrd(df_p, df_bio)
df_merged, _, _, _ = build_merged(df_p2, df_i, df_bio)

for df, label in [(df_p2, "persons_enriched"), (df_merged, "df_merged")]:
    mask = df.astype(str).apply(lambda c: c.str.contains("_x0000_", na=False)).any(axis=1)
    if mask.sum():
        print(f"\n{label}: {mask.sum()} rows with _x0000_")
        hit = df[mask].head(3)
        for col in hit.columns:
            vals = hit[col].astype(str)
            if vals.str.contains("_x0000_").any():
                print(f"  col={col!r}: {vals[vals.str.contains('_x0000_')].tolist()[:3]}")
    else:
        print(f"{label}: clean")
