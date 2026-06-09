"""
Refresh person columns in the latest uq_delegates_baked parquet from abbrd.

For every cons_id_str that matches an id_persoon in abbrd, the following
columns are overwritten with abbrd's authoritative values:
    fullname, voornaam, tussenvoegsel, geslachtsnaam,
    geboortejaar, overlijdensjaar, provincie, heerlijkheid

Columns that live only in uq (pattern, minjaar, maxjaar, resolutie_refs,
active_after_corrections, patterns, delegate_id) are kept unchanged.

IDs not found in abbrd (republic_add_*, low/legacy ids, etc.) are kept as-is.

Usage:
    python _refresh_uq_from_abbrd.py

Output: uq_delegates_baked_<today>.parquet  (current file is kept as backup)
"""
import pathlib, re
import pandas as pd
from datetime import date

_ws = pathlib.Path(".")

# ── load latest uq ──────────────────────────────────────────────────────────
_uq_files = sorted(
    [p for p in _ws.glob("uq_delegates_baked_*.parquet")
     if re.fullmatch(r"uq_delegates_baked_\d{8}\.parquet", p.name)],
    reverse=True,
)
if not _uq_files:
    raise FileNotFoundError("No uq_delegates_baked_YYYYMMDD.parquet found.")
src = _uq_files[0]
uq = pd.read_parquet(src)
print(f"Input uq:  {src.name}  ({len(uq)} rows)")

# ── load abbrd ───────────────────────────────────────────────────────────────
abbrd_path = _ws / "abbrd.parquet"
if not abbrd_path.exists():
    abbrd_path = _ws / "abbrd.xlsx"
abbrd_raw = (pd.read_parquet(abbrd_path) if abbrd_path.suffix == ".parquet"
             else pd.read_excel(abbrd_path))
print(f"Input abbrd: {abbrd_path.name}  ({len(abbrd_raw)} rows, "
      f"{abbrd_raw['id_persoon'].nunique()} unique persons)")

# ── build one-row-per-person lookup from abbrd ───────────────────────────────
ABBRD_PERSON_COLS = [
    "fullname", "voornaam", "tussenvoegsel", "geslachtsnaam",
    "geboortejaar", "overlijdensjaar", "provincie", "heerlijkheid",
]
present = [c for c in ABBRD_PERSON_COLS if c in abbrd_raw.columns]
missing_in_abbrd = set(ABBRD_PERSON_COLS) - set(present)
if missing_in_abbrd:
    print(f"  Warning: abbrd missing columns {missing_in_abbrd}, will keep uq values for those")

abbrd_persons = (
    abbrd_raw[["id_persoon"] + present]
    .drop_duplicates(subset=["id_persoon"])
    .copy()
)
# normalise id to clean integer string
abbrd_persons["id_persoon"] = (
    abbrd_persons["id_persoon"]
    .astype(str).str.strip()
    .str.replace(r"\.0$", "", regex=True)
)
abbrd_persons = abbrd_persons.dropna(subset=["id_persoon"])

# ── normalise uq id ──────────────────────────────────────────────────────────
uq = uq.copy()
uq["cons_id_str"] = uq["cons_id_str"].astype(str).str.strip()

# ── merge: left join uq ← abbrd_persons ─────────────────────────────────────
# Drop old person columns that will be refreshed (avoid _x/_y suffixes)
uq_base = uq.drop(columns=[c for c in present if c in uq.columns])

merged = uq_base.merge(
    abbrd_persons.rename(columns={"id_persoon": "cons_id_str"}),
    on="cons_id_str",
    how="left",
)

# For ids NOT in abbrd, fall back to the original uq values
not_matched = ~uq["cons_id_str"].isin(set(abbrd_persons["id_persoon"]))
n_matched   = (~not_matched).sum()
n_kept      = not_matched.sum()
print(f"\nRefreshed from abbrd: {n_matched}")
print(f"Kept from uq (no abbrd match): {n_kept}")

# Restore original person cols for unmatched rows
for col in present:
    if col in uq.columns:
        # Where abbrd gave NaN (unmatched), use original uq value
        na_mask = merged[col].isna() & not_matched.values
        merged.loc[na_mask, col] = uq.loc[na_mask, col].values

# ── reorder columns to match original uq ─────────────────────────────────────
original_cols = [c for c in uq.columns if c in merged.columns]
extra_cols    = [c for c in merged.columns if c not in uq.columns]
merged = merged[original_cols + extra_cols]

# ── write output ──────────────────────────────────────────────────────────────
today = date.today().strftime("%Y%m%d")
out_path = _ws / f"uq_delegates_baked_{today}.parquet"

# Don't overwrite the same file we read (it's the backup)
if out_path == src:
    import string, random
    suffix = "".join(random.choices(string.ascii_lowercase, k=2))
    out_path = _ws / f"uq_delegates_baked_{today}{suffix}.parquet"

merged.to_parquet(out_path, index=False)
print(f"\nWrote: {out_path.name}  ({len(merged)} rows)")
print(f"Backup kept: {src.name}")

# ── quick sanity check on the three Wentholt ids ─────────────────────────────
check_ids = ["18578", "19857", "19859"]
sample = merged[merged["cons_id_str"].isin(check_ids)][
    ["cons_id_str", "fullname", "geboortejaar", "overlijdensjaar"]
]
if not sample.empty:
    print("\nSanity check (Wentholt ids):")
    print(sample.to_string(index=False))
