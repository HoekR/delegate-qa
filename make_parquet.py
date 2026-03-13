"""Convert all .xlsx files in the workspace to .parquet sidecars.

Run once (or whenever source Excel files change):

    python make_parquet.py

The Streamlit app will then automatically prefer the faster .parquet files.
"""
from pathlib import Path

import pandas as pd


def convert(root: Path = Path(".")) -> None:
    xlsx_files = sorted(root.glob("*.xlsx"))
    if not xlsx_files:
        print("No .xlsx files found in", root.resolve())
        return

    for f in xlsx_files:
        out = f.with_suffix(".parquet")
        print(f"Reading {f.name} ...", end=" ", flush=True)
        try:
            df = pd.read_excel(f)
            # Cast ID columns to str (they're identifiers, not numbers to sum)
            id_cols = [c for c in df.columns if "id" in c.lower()]
            for col in id_cols:
                df[col] = df[col].where(df[col].isna(), df[col].astype(str))
            # Parquet requires uniform types; cast remaining mixed object columns to str
            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].where(df[col].isna(), df[col].astype(str))
            df.to_parquet(out, index=False)
            if id_cols:
                print(f"  (id cols → str: {', '.join(id_cols)})")
            print(f"→ {out.name}  ({len(df):,} rows)")
        except Exception as exc:
            print(f"FAILED: {exc}")


if __name__ == "__main__":
    convert()
