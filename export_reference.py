"""Export reference parquet files from the streamlit_worksheet baked data.

The ``pattern`` column is rebuilt fresh from the canonical name fields
(geslachtsnaam + tussenvoegsel) rather than taken from the baked parquet's
stale occurrence-aggregated patterns.  This avoids contamination that arises
when corrections reassign occurrence rows between delegates without updating
the pattern string.

Usage (run from streamlit_worksheet dir):
    uv run python export_reference.py <output_dir>

Example:
    uv run python export_reference.py ../republic_ner_matching/data/
"""

import pathlib
import re
import sys

import pandas as pd

_WS = pathlib.Path(__file__).parent

DELEGATE_COLS = [
    "cons_id_str",
    "fullname",
    "voornaam",
    "tussenvoegsel",
    "geslachtsnaam",
    "heerlijkheid",
    "provincie",
    "geboortejaar",
    "overlijdensjaar",
    "minjaar",
    "maxjaar",
]


def _blank(val) -> bool:
    return pd.isna(val) or str(val).strip().lower() in ("", "nan", "none")


def _build_pattern(row) -> str:
    """Build a clean pattern string from canonical name fields.

    Produces semicolon-separated lowercase variants:
      - geslachtsnaam alone
      - tussenvoegsel + geslachtsnaam  (when tussenvoegsel is present)

    Intentionally excludes heerlijkheid and any occurrence-derived strings to
    prevent cross-delegate contamination from the baking pipeline.
    """
    gn = "" if _blank(row.get("geslachtsnaam")) else str(row["geslachtsnaam"]).strip()
    tv = "" if _blank(row.get("tussenvoegsel")) else str(row["tussenvoegsel"]).strip()

    parts: set[str] = set()
    if gn:
        parts.add(gn.lower())
        if tv:
            parts.add(f"{tv} {gn}".lower())

    return ";".join(sorted(parts))


def _find_latest_uq() -> pathlib.Path:
    candidates = sorted(
        [p for p in _WS.glob("uq_delegates_baked_*.parquet")
         if re.fullmatch(r"uq_delegates_baked_\d{8}\.parquet", p.name)],
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError("No uq_delegates_baked_YYYYMMDD.parquet found")
    return candidates[0]


def export_delegates(uq_path: pathlib.Path, out_dir: pathlib.Path) -> None:
    uq = pd.read_parquet(uq_path)
    available = [c for c in DELEGATE_COLS if c in uq.columns]
    missing_cols = [c for c in DELEGATE_COLS if c not in uq.columns]
    if missing_cols:
        print(f"  Warning: columns not found, skipping: {missing_cols}")
    df = uq[available].copy()
    df = df[df["cons_id_str"].notna()].reset_index(drop=True)

    # Rebuild pattern from canonical name fields — drop stale/contaminated column.
    df["pattern"] = df.apply(_build_pattern, axis=1)

    # Remove delegates for whom no pattern could be built (no surname data).
    no_pattern = df["pattern"].str.strip() == ""
    if no_pattern.any():
        print(f"  Dropping {no_pattern.sum()} delegates with no buildable pattern:")
        for _, r in df[no_pattern].iterrows():
            print(f"    {r['cons_id_str']}  {r.get('fullname', '')}")
        df = df[~no_pattern].reset_index(drop=True)

    out = out_dir / "delegates_reference.parquet"
    df.to_parquet(out, index=False)
    print(f"  delegates_reference: {len(df):,} rows → {out}")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: export_reference.py <output_dir>")
        sys.exit(1)

    out_dir = pathlib.Path(sys.argv[1]).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    uq_path = _find_latest_uq()
    print(f"Using: {uq_path.name}")

    export_delegates(uq_path, out_dir)
    print("Done.")


if __name__ == "__main__":
    main()
