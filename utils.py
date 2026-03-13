"""
Delegate QA – shared utilities
===============================
Pure / data-tier functions used by sheet.py and the individual tab modules.
The only streamlit import here is ``@st.cache_data`` (load_data, enrich_persons_from_abbrd, build_merged).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st  # only for @st.cache_data


# ---------------------------------------------------------------------------
# FAST DATAFRAME HASH  (used in hash_funcs= to avoid O(n) rehashing)
# ---------------------------------------------------------------------------

def _hash_df(df: "pd.DataFrame | None") -> int:
    """O(1)-ish fingerprint for @st.cache_data hash_funcs.

    Streamlit's default hasher walks every cell of the DataFrame to build a
    cache key — on a 400 k-row frame that takes hundreds of ms *per call*.
    This samples ≤5 rows spread across the frame to produce a fast, stable
    fingerprint that catches any realistic change while keeping lookup time
    under 1 ms.
    """
    if df is None:
        return 0
    h = hashlib.md5(usedforsecurity=False)  # type: ignore[call-arg]
    h.update(str(df.shape).encode())
    h.update("|".join(str(c) for c in df.columns).encode())
    if len(df) > 0:
        n = min(5, len(df))
        step = max(1, len(df) // n)
        try:
            s = int(pd.util.hash_pandas_object(df.iloc[::step].head(n), index=False).sum())
            h.update(s.to_bytes(8, "little", signed=False))
        except Exception:
            h.update(str(df.iloc[0]).encode()[:128])
    return int(h.hexdigest(), 16)


def _hash_ndarray(a: "np.ndarray | None") -> int:
    """Fast hash for a numpy integer position array used as a cache key.

    Hashes length + first/mid/last element so that distinct position arrays
    produce distinct keys without walking the whole array.
    """
    if a is None:
        return 0
    h = hashlib.md5(usedforsecurity=False)  # type: ignore[call-arg]
    h.update(len(a).to_bytes(8, "little"))
    if len(a) > 0:
        picks = np.array([a[0], a[len(a) // 2], a[-1]], dtype=np.int64)
        h.update(picks.tobytes())
    return int(h.hexdigest(), 16)


_HASH_FUNCS: dict = {pd.DataFrame: _hash_df, np.ndarray: _hash_ndarray}

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BASEDIR = Path("/Users/rikhoekstra/surfdrive (2)/Republic/gedelegeerden")

_WS = Path(__file__).parent   # workspace root — always checked first

# Persons (unique delegates)
PERSONS_CANDIDATES: list[Path] = [
    _WS / "uq_delegates_updated_20260225.xlsx",
    BASEDIR / "output" / "uq_delegates_updated_20260225.xlsx",
    BASEDIR / "uq_delegates_updated_20260225.xlsx",
]

# Occurrences (meeting records)
OCCURRENCES_CANDIDATES: list[Path] = [
    _WS / "delegates_18ee_w_correcties_20260123_marked.xlsx",
    BASEDIR / "output" / "delegates_18ee_w_correcties_20260123_marked.xlsx",
    BASEDIR / "delegates_18ee_w_correcties_20260123_marked.xlsx",
]

# Abbreviation / authority file — also bio source (birth_year, death_year, hlife)
ABBRD_CANDIDATES: list[Path] = [
    _WS / "abbrd.xlsx",
    BASEDIR / "abbrd.xlsx",
    BASEDIR / "output" / "abbrd.xlsx",
    BASEDIR / "input" / "abbrd.xlsx",
    BASEDIR.parent / "abbrd.xlsx",
]


def _resolved_file(candidates: list[Path]) -> Path:
    """Return the path that will actually be read: parquet sidecar > xlsx > fallback."""
    for p in candidates:
        if p.with_suffix(".parquet").exists():
            return p.with_suffix(".parquet")
        if p.exists():
            return p
    return candidates[1]  # fallback for display even if missing


PERSONS_FILE: Path = _resolved_file(PERSONS_CANDIDATES)
OCCURRENCES_FILE: Path = _resolved_file(OCCURRENCES_CANDIDATES)
ABBRD_FILE: Path = _resolved_file(ABBRD_CANDIDATES)


def source_mtimes() -> tuple[float, ...]:
    """Return the modification timestamps of all source data files.

    Passed as an argument to load_data() so that the disk-persisted cache
    entry is automatically invalidated whenever a source file changes on disk.
    Files that don't exist contribute 0.0.
    """
    candidates = [*PERSONS_CANDIDATES, *OCCURRENCES_CANDIDATES, *ABBRD_CANDIDATES]
    return tuple(p.stat().st_mtime if p.exists() else 0.0 for p in candidates)

# Persistence files
CORRECTIONS_FILE    = _WS / "corrections.json"
NEW_DELEGATES_FILE  = _WS / "new_delegates.json"
PROVINCE_ORDER_FILE = _WS / "province_order.json"
REMAPPINGS_FILE     = _WS / "remappings.json"
SANDBOXED_FILE      = _WS / "sandboxed.json"
REVIEWED_FILE       = _WS / "reviewed.json"

REPUBLIC_ADD_PREFIX = "republic_add_"

MIN_AGE     = 16
MAX_AGE     = 90
DEFAULT_GAP = 10

# ---------------------------------------------------------------------------
# CORRECTIONS PERSISTENCE
# ---------------------------------------------------------------------------

def load_corrections() -> dict:
    if CORRECTIONS_FILE.exists():
        try:
            raw = json.loads(CORRECTIONS_FILE.read_text())
            return {int(k): v for k, v in raw.items()}
        except Exception:
            return {}
    return {}


def save_corrections(corrections: dict) -> None:
    CORRECTIONS_FILE.write_text(
        json.dumps({str(k): v for k, v in corrections.items()}, indent=2)
    )


# ---------------------------------------------------------------------------
# REVIEWED IDS PERSISTENCE
# ---------------------------------------------------------------------------

def load_reviewed() -> set[str]:
    """Return the set of delegate_ids the user has marked as fully reviewed."""
    if REVIEWED_FILE.exists():
        try:
            return set(json.loads(REVIEWED_FILE.read_text()))
        except Exception:
            return set()
    return set()


def save_reviewed(reviewed: set[str]) -> None:
    REVIEWED_FILE.write_text(json.dumps(sorted(reviewed), indent=2))


def apply_corrections(df: pd.DataFrame, corrections: dict) -> pd.DataFrame:
    """Return a copy of *df* with all staged corrections applied.

    Only touches rows that exist in the index — silently skips stale keys.
    Does NOT modify the cached df_merged in place.
    """
    if not corrections:
        return df
    out = df.copy()
    valid = {ridx: nid for ridx, nid in corrections.items() if ridx in out.index}
    if valid:
        idxs = list(valid.keys())
        # Cast to the existing column dtype so parquet/pyarrow never sees a
        # mixed-type (str + int) object column.
        col_dtype = out["delegate_id"].dtype
        new_vals = [valid[i] for i in idxs]
        if col_dtype == object or str(col_dtype) == "string":
            new_vals = [str(v) for v in new_vals]
        out.loc[idxs, "delegate_id"] = new_vals
    return out


# ---------------------------------------------------------------------------
# NEW-DELEGATES PERSISTENCE
# ---------------------------------------------------------------------------

def load_new_delegates() -> list[dict]:
    if NEW_DELEGATES_FILE.exists():
        try:
            return json.loads(NEW_DELEGATES_FILE.read_text())
        except Exception:
            return []
    return []


def save_new_delegates(records: list[dict]) -> None:
    NEW_DELEGATES_FILE.write_text(json.dumps(records, indent=2, default=str))


# ---------------------------------------------------------------------------
# BULK REMAPPINGS PERSISTENCE
# ---------------------------------------------------------------------------

def load_remappings() -> list[dict]:
    """Return list of {from_id: str, to_id: str} dicts."""
    if REMAPPINGS_FILE.exists():
        try:
            return [
                {"from_id": str(r["from_id"]), "to_id": str(r["to_id"])}
                for r in json.loads(REMAPPINGS_FILE.read_text())
            ]
        except Exception:
            return []
    return []


def save_remappings(remappings: list[dict]) -> None:
    REMAPPINGS_FILE.write_text(json.dumps(remappings, indent=2, default=str))


# ---------------------------------------------------------------------------
# SANDBOXED IDS PERSISTENCE
# ---------------------------------------------------------------------------

def load_sandboxed() -> set[str]:
    """Return the set of delegate_ids marked as 'known wrong, can't fix'."""
    if SANDBOXED_FILE.exists():
        try:
            data = json.loads(SANDBOXED_FILE.read_text())
            # support both old list-of-ids and new list-of-{id,reason} formats
            return {
                str(r["id"]) if isinstance(r, dict) else str(r)
                for r in data
            }
        except Exception:
            return set()
    return set()


def load_sandboxed_records() -> list[dict]:
    """Return full records [{id, reason}] for display in Tab 6."""
    if SANDBOXED_FILE.exists():
        try:
            data = json.loads(SANDBOXED_FILE.read_text())
            return [
                r if isinstance(r, dict) else {"id": str(r), "reason": ""}
                for r in data
            ]
        except Exception:
            return []
    return []


def save_sandboxed(records: list[dict]) -> None:
    """Save list of {id, reason} records."""
    SANDBOXED_FILE.write_text(json.dumps(records, indent=2))


# ---------------------------------------------------------------------------
# PROVINCE ORDER
# ---------------------------------------------------------------------------

def load_province_order() -> list[str]:
    """Load ordered province list; falls back to canonical Republic sequence."""
    default = ["Gelderland", "Holland", "Zeeland", "Utrecht",
               "Friesland", "Overijssel", "Groningen"]
    if PROVINCE_ORDER_FILE.exists():
        try:
            data = json.loads(PROVINCE_ORDER_FILE.read_text())
            if isinstance(data, list) and data:
                return [str(p) for p in data]
        except Exception:
            pass
    return default


PROVINCE_ORDER: list[str] = load_province_order()
PROVINCE_RANK: dict[str, int] = {p.lower(): i for i, p in enumerate(PROVINCE_ORDER)}

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def next_republic_add_id(df_persons: pd.DataFrame, existing: list[dict]) -> str:
    """Return the next clash-free republic_add_<##> string ID."""
    used: set[int] = set()
    id_col = "delegate_id" if "delegate_id" in df_persons.columns else df_persons.columns[0]
    for val in df_persons[id_col].astype(str):
        if val.startswith(REPUBLIC_ADD_PREFIX):
            try:
                used.add(int(val[len(REPUBLIC_ADD_PREFIX):]))
            except ValueError:
                pass
    for rec in existing:
        rid = str(rec.get("delegate_id", ""))
        if rid.startswith(REPUBLIC_ADD_PREFIX):
            try:
                used.add(int(rid[len(REPUBLIC_ADD_PREFIX):]))
            except ValueError:
                pass
    seq = max(used, default=0) + 1
    return f"{REPUBLIC_ADD_PREFIX}{seq:02d}"

# ---------------------------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------------------------

def _read_df(candidates: list[Path]) -> tuple[pd.DataFrame, Path]:
    """Return (dataframe, resolved_path) using the first candidate that exists.

    For each Excel candidate, a sibling .parquet file is preferred when
    present — it loads 5-20x faster.  Generate sidecars once with:

        python -c "
        import pandas as pd, pathlib
        for f in pathlib.Path('.').glob('*.xlsx'):
            pd.read_excel(f).to_parquet(f.with_suffix('.parquet'), index=False)
        "
    """
    for p in candidates:
        parq = p.with_suffix(".parquet")
        if parq.exists():
            return pd.read_parquet(parq), parq
        if p.exists():
            return pd.read_excel(p), p
    raise FileNotFoundError(
        "File not found. Searched:\n" + "\n".join(f"  {p}" for p in candidates)
    )


@st.cache_data(persist="disk")
def load_data(source_mtimes: tuple[float, ...] = ()) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    """Returns (persons, occurrences, abbrd).

    ``source_mtimes`` is passed purely as a cache-busting sentinel: when any
    source file is modified its mtime changes, producing a new cache key and
    forcing a recompute.  With ``persist="disk"`` the result survives process
    restarts (e.g. after editing a tab file) as long as the source data is
    unchanged.

    Files are resolved from candidate lists; workspace root is always checked
    first so dropping / symlinking a file there is sufficient.
    A .parquet sidecar next to any .xlsx is used automatically if present.
    """
    df_p, _ = _read_df(PERSONS_CANDIDATES)
    if "delegate_id" in df_p.columns:
        df_p["delegate_id"] = pd.to_numeric(df_p["delegate_id"], errors="coerce").astype("Int64")

    df_i, _ = _read_df(OCCURRENCES_CANDIDATES)
    if "delegate_id" in df_i.columns:
        df_i["delegate_id"] = pd.to_numeric(df_i["delegate_id"], errors="coerce").astype("Int64")

    df_abbrd: pd.DataFrame | None = None
    abbrd_path = next(
        (p for p in ABBRD_CANDIDATES if p.with_suffix(".parquet").exists() or p.exists()), None
    )
    if abbrd_path is not None:
        parq = abbrd_path.with_suffix(".parquet")
        df_abbrd = pd.read_parquet(parq) if parq.exists() else pd.read_excel(abbrd_path)
        df_abbrd.columns = df_abbrd.columns.str.strip()
    return df_p, df_i, df_abbrd


def _parse_leefjaren(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    birth = series.str.extract(r"(\d{4})\s*[-–]\s*\d{4}")[0].astype("Int64")
    death = series.str.extract(r"\d{4}\s*[-–]\s*(\d{4})")[0].astype("Int64")
    return birth, death


# ---------------------------------------------------------------------------
# MODULE-LEVEL COUNTERS  (mutated by enrich_persons_from_abbrd / build_merged)
# ---------------------------------------------------------------------------

_n_placeholder_rows: int = 0
_n_remapped_rows:    int = 0
_n_enriched_persons: int = 0

# ---------------------------------------------------------------------------
# ENRICHMENT
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner="Enriching persons…", hash_funcs=_HASH_FUNCS, persist="disk")
def enrich_persons_from_abbrd(
    df_p: pd.DataFrame, df_abbrd: pd.DataFrame | None
) -> tuple[pd.DataFrame, int]:
    """Fill blank fields in df_p from abbrd using delegate_id ↔ id_persoon.

    Only fills where df_p already has a null / empty value; never overwrites.
    Returns ``(enriched_df, n_enriched_persons)``.
    """
    _n_enriched_persons: int = 0

    if df_abbrd is None or df_p.empty or "delegate_id" not in df_p.columns:
        return df_p, 0

    abbrd = df_abbrd.copy()
    abbrd.columns = abbrd.columns.str.strip()
    if "delegate_id" not in abbrd.columns and "id_persoon" in abbrd.columns:
        abbrd = abbrd.rename(columns={"id_persoon": "delegate_id"})
    if "delegate_id" not in abbrd.columns:
        return df_p, 0
    abbrd["delegate_id"] = abbrd["delegate_id"].astype(str)
    abbrd_idx = abbrd.drop_duplicates(subset=["delegate_id"]).set_index("delegate_id")

    result = df_p.copy()
    if "delegate_id" in result.columns:
        result["delegate_id"] = result["delegate_id"].astype(str)

    name_col_p = next(
        (c for c in ("fullname", "full_name", "naam", "name") if c in result.columns), None
    )
    if name_col_p:
        for abbrd_nc in ("naam", "name", "fullname", "full_name", "achternaam"):
            if abbrd_nc in abbrd_idx.columns:
                mapped = result["delegate_id"].map(abbrd_idx[abbrd_nc])
                blank = result[name_col_p].isna() | (
                    result[name_col_p].astype(str).str.strip() == ""
                )
                result.loc[blank & mapped.notna(), name_col_p] = mapped[blank & mapped.notna()]
                break

    shared = [
        c for c in result.columns
        if c in abbrd_idx.columns and c not in ("delegate_id", name_col_p)
    ]
    for col in shared:
        mapped = result["delegate_id"].map(abbrd_idx[col])
        null_mask = result[col].isna()
        result.loc[null_mask & mapped.notna(), col] = mapped[null_mask & mapped.notna()]

    try:
        changed = (result.fillna("__NA__") != df_p.fillna("__NA__")).any(axis=1)
        _n_enriched_persons = int(changed.sum())
    except Exception:
        _n_enriched_persons = 0

    return result, _n_enriched_persons


# ---------------------------------------------------------------------------
# BUILD MERGED
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner="Merging data…", hash_funcs=_HASH_FUNCS, persist="disk")
def build_merged(
    df_p: pd.DataFrame,
    df_i: pd.DataFrame,
    df_bio: pd.DataFrame | None,
    extra_delegates: list[dict] | None = None,
    remappings: list[dict] | None = None,
    name_col: str = "fullname",
) -> tuple[pd.DataFrame, int, int, pd.DataFrame]:
    """Merge occurrences + persons, apply remappings, filter placeholders.

    Returns ``(merged_df, n_placeholder_rows, n_remapped_rows, summary_df)``.
    The summary (one row per delegate) is computed here so it shares this
    function's cache entry — no second hashing of the large merged DataFrame.
    """
    _n_placeholder_rows: int = 0
    _n_remapped_rows: int = 0

    persons = df_p.copy()
    if "delegate_id" in persons.columns:
        persons["delegate_id"] = persons["delegate_id"].astype(str)
    if "delegate_id" in df_i.columns:
        df_i = df_i.copy()
        df_i["delegate_id"] = df_i["delegate_id"].astype(str)
    if df_bio is not None:
        df_bio = df_bio.copy()
        if "delegate_id" not in df_bio.columns and "id_persoon" in df_bio.columns:
            df_bio = df_bio.rename(columns={"id_persoon": "delegate_id"})
        if "delegate_id" in df_bio.columns:
            df_bio["delegate_id"] = df_bio["delegate_id"].astype(str)

    # append manually-added delegates (republic_add_*)
    if extra_delegates:
        extra_df = pd.DataFrame(extra_delegates)
        for col in persons.columns:
            if col not in extra_df.columns:
                extra_df[col] = pd.NA
        persons = pd.concat([persons, extra_df[persons.columns]], ignore_index=True)

    # attach birth/death year
    if "birth_year" not in persons.columns and df_bio is not None:
        for by in ("birth_year", "geboortejaar", "geboorte", "born", "birth"):
            if by in df_bio.columns and "delegate_id" in df_bio.columns:
                persons = persons.merge(
                    df_bio[["delegate_id", by]]
                    .rename(columns={by: "birth_year"})
                    .drop_duplicates(subset=["delegate_id"]),
                    on="delegate_id", how="left")
                break
        for dy in ("death_year", "sterfjaar", "overlijden", "died", "death"):
            if dy in df_bio.columns and "delegate_id" in df_bio.columns:
                persons = persons.merge(
                    df_bio[["delegate_id", dy]]
                    .rename(columns={dy: "death_year"})
                    .drop_duplicates(subset=["delegate_id"]),
                    on="delegate_id", how="left")
                break
        if "birth_year" not in persons.columns:
            for lj in ("leefjaren", "levensjaren", "leven"):
                if lj in df_bio.columns and "delegate_id" in df_bio.columns:
                    tmp = df_bio[["delegate_id", lj]].drop_duplicates(subset=["delegate_id"]).copy()
                    tmp["birth_year"], tmp["death_year"] = _parse_leefjaren(tmp[lj].astype(str))
                    persons = persons.merge(
                        tmp[["delegate_id", "birth_year", "death_year"]],
                        on="delegate_id", how="left")
                    break

    # hlife fallback
    if df_bio is not None and "hlife" in df_bio.columns and "delegate_id" in df_bio.columns:
        extra_bio_cols = ["hlife"]
        for c in ("min_year", "max_year"):
            if c in df_bio.columns:
                extra_bio_cols.append(c)
        hl = (
            df_bio[["delegate_id"] + extra_bio_cols]
            .drop_duplicates(subset=["delegate_id"])
            .copy()
        )
        for c in extra_bio_cols:
            hl[c] = pd.to_numeric(hl[c], errors="coerce")
        persons = persons.merge(hl, on="delegate_id", how="left")
        if "birth_year" not in persons.columns:
            persons["birth_year"] = pd.NA
        if "death_year" not in persons.columns:
            persons["death_year"] = pd.NA
        mask_no_birth = persons["birth_year"].isna() & persons["hlife"].notna()
        mask_no_death = persons["death_year"].isna() & persons["hlife"].notna()
        persons.loc[mask_no_birth, "birth_year"] = (
            persons.loc[mask_no_birth, "hlife"] - 40).astype("Int64")
        persons.loc[mask_no_death, "death_year"] = (
            persons.loc[mask_no_death, "hlife"] + 30).astype("Int64")
    elif df_bio is not None and "delegate_id" in df_bio.columns:
        active_cols = [c for c in ("min_year", "max_year") if c in df_bio.columns]
        if active_cols:
            act_df = (
                df_bio[["delegate_id"] + active_cols]
                .drop_duplicates(subset=["delegate_id"])
                .copy()
            )
            for c in active_cols:
                act_df[c] = pd.to_numeric(act_df[c], errors="coerce")
            persons = persons.merge(act_df, on="delegate_id", how="left")

    for col in ("birth_year", "death_year"):
        if col not in persons.columns:
            persons[col] = pd.NA

    # extract geslachtsnaam
    fname_col = next((c for c in ("fullname", "full_name", "naam") if c in persons.columns), None)
    if fname_col:
        persons["geslachtsnaam"] = (
            persons[fname_col].astype(str).str.split(",", n=1).str[0].str.strip().str.lower()
        )
    else:
        persons["geslachtsnaam"] = pd.NA

    # Precompute name_mismatch once per delegate from the persons frame.
    # geslachtsnaam and pattern are both person-level attributes — no need to
    # recheck on every occurrence row after the merge.
    if "geslachtsnaam" in persons.columns and "pattern" in persons.columns:
        g_arr = persons["geslachtsnaam"].fillna("").to_numpy(dtype=str)
        p_arr = persons["pattern"].fillna("").str.lower().to_numpy(dtype=str)
        # Use nullable Boolean so the column survives a left-merge without
        # silently becoming object dtype (bool + NaN → object → mis-categorised).
        persons["name_mismatch"] = pd.array(
            np.char.find(p_arr, g_arr) < 0, dtype="boolean"
        )
    else:
        persons["name_mismatch"] = pd.array([False] * len(persons), dtype="boolean")

    # apply bulk remappings
    remap_dict: dict[str, str] = {
        str(r["from_id"]): str(r["to_id"])
        for r in (remappings or [])
        if r.get("from_id") and r.get("to_id")
    }
    if remap_dict and "delegate_id" in df_i.columns:
        _remap_mask = df_i["delegate_id"].astype(str).isin(remap_dict)
        _n_remapped_rows = int(_remap_mask.sum())
        df_i = df_i.copy()
        df_i.loc[_remap_mask, "delegate_id"] = (
            df_i.loc[_remap_mask, "delegate_id"].astype(str).map(remap_dict)
        )
    else:
        _n_remapped_rows = 0

    # filter placeholder occurrences (delegate_id == -1 or -20)
    if "delegate_id" in df_i.columns:
        _placeholder_mask = df_i["delegate_id"].astype(str).isin({"-1", "-20"})
        _n_placeholder_rows = int(_placeholder_mask.sum())
        df_i = df_i[~_placeholder_mask].copy()
    else:
        _n_placeholder_rows = 0

    if "delegate_id" in df_i.columns:
        df_i["delegate_id"] = df_i["delegate_id"].astype(str)
    if "delegate_id" in persons.columns:
        persons["delegate_id"] = persons["delegate_id"].astype(str)

    # Safety dedup: if abbrd had multiple rows per delegate the bio merges
    # above may have multiplied persons rows — collapse back to one per id.
    if "delegate_id" in persons.columns:
        persons = persons.drop_duplicates(subset=["delegate_id"])

    # Remove null / na delegate ids from both sides before merging.
    # astype(str) converts NaN → "nan" / "<NA>"; these form a cartesian
    # product in the merge (all null-id occurrences × all null-id persons).
    _null_ids = {"nan", "none", "<na>", "", "nat"}
    if "delegate_id" in persons.columns:
        persons = persons[~persons["delegate_id"].str.lower().isin(_null_ids)]
    if "delegate_id" in df_i.columns:
        df_i = df_i[~df_i["delegate_id"].str.lower().isin(_null_ids)]

    df = df_i.merge(persons, on="delegate_id", how="left", suffixes=("", "_p"))

    if "j" not in df.columns and "date" in df.columns:
        df["j"] = pd.to_datetime(df["date"], errors="coerce").dt.year
    if "j" in df.columns:
        df["j"] = pd.to_numeric(df["j"], errors="coerce")
        by = pd.to_numeric(df["birth_year"], errors="coerce")
        df["age_at_event"] = df["j"] - by
    else:
        df["j"] = pd.NA
        df["age_at_event"] = pd.NA

    df = df.loc[:, ~df.columns.duplicated()].copy()

    # Convert repeated string columns to Categorical — encodes strings as
    # integer codes once so every subsequent groupby/nunique/value_counts uses
    # cheap int comparisons instead of string hashing.
    # Threshold: any object column whose unique count is < 50 % of total rows
    # (i.e. values repeat on average at least twice) is worth categorising.
    # Exclusions: boolean-valued columns (True/False +NaN) and columns whose
    # values are numeric-as-strings — both cause TypeError on sum/min/max.
    _n = len(df)
    if _n > 0:
        for _col in df.select_dtypes(include=["object", "string"]).columns:
            if df[_col].nunique() >= _n * 0.5:
                continue
            _sample = df[_col].dropna()
            if _sample.empty:
                continue
            # Skip if values look numeric (year strings, id numbers, …)
            if pd.to_numeric(_sample, errors="coerce").notna().mean() > 0.1:
                continue
            # Skip if values are boolean-like (True/False stored as Python objects)
            if set(_sample.unique()).issubset({True, False}):
                continue
            df[_col] = df[_col].astype("category")

    summary = _compute_delegate_summary(df, df_p, name_col)
    return df, _n_placeholder_rows, _n_remapped_rows, summary


# ---------------------------------------------------------------------------
# DELEGATE SUMMARY  (small table used as primary selection surface in Tab 0)
# ---------------------------------------------------------------------------

def _compute_delegate_summary(
    df_merged: pd.DataFrame,
    df_p: pd.DataFrame,
    name_col: str,
) -> pd.DataFrame:
    """Build per-delegate aggregation from df_merged.

    Called inside build_merged (which is already cached) so the 600k-row
    DataFrame is never hashed a second time by a separate cached function.

    Issue columns added (sortable in the overview grid):
      n_alive_flags     — rows where age_at_event < MIN_AGE, > MAX_AGE, or j > death_year
      n_name_mismatches — rows where geslachtsnaam is not found in pattern
      max_gap_years     — largest consecutive year gap between appearances
    """
    if df_merged.empty:
        return pd.DataFrame()
    agg: dict = {"j": ["count", "min", "max"]}
    if "pattern" in df_merged.columns:
        agg["pattern"] = "nunique"
    summary = df_merged.groupby("delegate_id", observed=True).agg(agg).reset_index()
    summary.columns = ["_".join(c).strip("_") for c in summary.columns]
    summary = summary.rename(columns={
        "j_count": "n_occurrences",
        "j_min": "first_year",
        "j_max": "last_year",
        "pattern_nunique": "n_patterns",
    })

    # ---- alive flags -------------------------------------------------------
    if "age_at_event" in df_merged.columns:
        age = pd.to_numeric(df_merged["age_at_event"], errors="coerce").to_numpy()
        flag = (age < MIN_AGE) | (age > MAX_AGE)
        if "death_year" in df_merged.columns and "j" in df_merged.columns:
            j_n  = pd.to_numeric(df_merged["j"],          errors="coerce").to_numpy()
            dy_n = pd.to_numeric(df_merged["death_year"],  errors="coerce").to_numpy()
            flag = flag | (j_n > dy_n)
        alive_counts = (
            pd.Series(flag, index=df_merged.index, name="n_alive_flags")
            .groupby(df_merged["delegate_id"], observed=True).sum()
            .astype(int)
        )
        summary = summary.merge(alive_counts, on="delegate_id", how="left")

    # ---- name mismatches (pre-computed column on df_merged) ---------------
    if "name_mismatch" in df_merged.columns:
        mm_counts = (
            df_merged.groupby("delegate_id", observed=True)["name_mismatch"]
            .sum()
            .astype(int)
            .rename("n_name_mismatches")
        )
        summary = summary.merge(mm_counts, on="delegate_id", how="left")

    # ---- max consecutive year gap -----------------------------------------
    if "j" in df_merged.columns:
        gd = (
            pd.DataFrame({
                "delegate_id": df_merged["delegate_id"].to_numpy(),
                "j": pd.to_numeric(df_merged["j"], errors="coerce").to_numpy(),
            })
            .dropna(subset=["j"])
            .sort_values(["delegate_id", "j"])
        )
        gd["_diff"] = gd.groupby("delegate_id")["j"].diff()
        max_gaps = (
            gd.groupby("delegate_id")["_diff"]
            .max()
            .rename("max_gap_years")
            .astype("Int64")
        )
        summary = summary.merge(max_gaps, on="delegate_id", how="left")

    # ---- name lookup & column ordering ------------------------------------
    if name_col in df_p.columns:
        summary = summary.merge(
            df_p[["delegate_id", name_col]].drop_duplicates(),
            on="delegate_id", how="left",
        )
    issue_cols = [c for c in ("n_alive_flags", "n_name_mismatches", "max_gap_years")
                  if c in summary.columns]
    front = [c for c in (name_col, "delegate_id") if c in summary.columns]
    mid   = [c for c in summary.columns if c not in front and c not in issue_cols]
    return summary[front + mid + issue_cols].reset_index(drop=True)


# ---------------------------------------------------------------------------
# FAST CACHED QUERIES  (avoid rescanning 400k-row df_merged on every rerun)
# ---------------------------------------------------------------------------

@st.cache_data(hash_funcs=_HASH_FUNCS)
def _province_positions(
    df_merged: pd.DataFrame,
    prov_col: str,
    sel_provinces: tuple[str, ...],
) -> np.ndarray:
    """Stage 1: integer row positions that pass the province filter.

    Scans the full frame once; result is a compact int64 position array.
    Cached independently of year so a year-slider change reuses this.
    """
    if not sel_provinces or not prov_col or prov_col not in df_merged.columns:
        return np.arange(len(df_merged), dtype=np.intp)
    return np.flatnonzero(df_merged[prov_col].isin(sel_provinces).to_numpy())


@st.cache_data(hash_funcs=_HASH_FUNCS)
def _year_positions_from(
    df_merged: pd.DataFrame,
    base_positions: np.ndarray,
    year_min: int,
    year_max: int,
) -> np.ndarray:
    """Stage 2: narrow *base_positions* to rows within the year range.

    Only scans the rows that survived stage 1 — if province narrowed
    400k → 60k, this scans 60k not 400k.  Cached independently of province
    so a province change reuses this when the year bounds are unchanged.
    """
    if "j" not in df_merged.columns or len(base_positions) == 0:
        return base_positions
    j_sub = pd.to_numeric(
        df_merged["j"].to_numpy()[base_positions], errors="coerce"
    )
    keep = (j_sub >= year_min) & (j_sub <= year_max)
    return base_positions[keep]


@st.cache_data(hash_funcs=_HASH_FUNCS)
def filter_occurrences(
    df_merged: pd.DataFrame,
    prov_col: str | None,
    sel_provinces: tuple[str, ...],
    year_min: int,
    year_max: int,
) -> pd.DataFrame:
    """Incrementally narrow df_merged: province stage then year stage.

    Each stage is cached separately:
    - Changing only the year reuses the province positions array.
    - Changing only the province reuses the year positions array
      (keyed on the new base_positions from stage 1).
    Final iloc is on the surviving position array — no full-frame copy.
    """
    pos = _province_positions(df_merged, prov_col or "", sel_provinces)
    pos = _year_positions_from(df_merged, pos, year_min, year_max)
    return df_merged.take(pos)


@st.cache_data(hash_funcs=_HASH_FUNCS)
def build_day_order(
    df_merged: pd.DataFrame,
    prov_col: str,
    sel_provinces: tuple[str, ...],
    year_min: int,
    year_max: int,
    max_rows: int | None,
    province_rank: dict[str, int],
) -> pd.DataFrame:
    """Compute per-day position and violations.

    Cache key is cheap primitives + df_merged (stable cached object from
    build_merged).  Changing only the selected delegate in the main app
    leaves all these args unchanged → cache hit, 0 ms recomputation.
    """
    df_view = filter_occurrences(df_merged, prov_col, sel_provinces, year_min, year_max)
    if max_rows is not None and not df_view.empty:
        df_view = df_view.head(max_rows)

    date_col = "date" if "date" in df_view.columns else None
    if date_col is None and "j" in df_view.columns:
        day = df_view["j"].astype("Int64").astype(str)
    else:
        day = pd.to_datetime(df_view[date_col], errors="coerce").dt.strftime("%Y-%m-%d")

    df5 = df_view.copy()
    df5["_day"] = day
    df5 = df5.dropna(subset=["_day"])
    df5["_prov_lower"] = (
        df5[prov_col].astype(str).str.lower().str.strip().replace({"nan": ""})
    )
    df5["prov_rank"] = df5["_prov_lower"].map(province_rank)
    df5 = df5.sort_values(["_day"]).reset_index(drop=False)
    df5["actual_pos"] = df5.groupby("_day").cumcount() + 1
    df5["expected_pos"] = (
        df5.groupby("_day")["prov_rank"]
        .rank(method="first", na_option="bottom")
        .astype("Int64")
    )
    df5["pos_diff"] = (df5["actual_pos"] - df5["expected_pos"]).abs()
    return df5


@st.cache_data(hash_funcs=_HASH_FUNCS)
def _build_delegate_index(df_merged: pd.DataFrame) -> dict[str, np.ndarray]:
    """Build a positional index: delegate_id → array of integer row positions.

    Uses pandas GroupBy .indices which does a single O(n) pass over the column
    and returns a ready-made dict.  All subsequent per-delegate lookups are O(1)
    dict access + fancy-index slice — no further column scans.
    """
    if df_merged.empty or "delegate_id" not in df_merged.columns:
        return {}
    # reset_index so iloc positions match .indices values
    return df_merged.reset_index(drop=True).groupby("delegate_id", sort=False, observed=True).indices  # type: ignore[return-value]


def get_delegate_slice(df_merged: pd.DataFrame, delegate_id: str) -> pd.DataFrame:
    """Return all rows for one delegate — O(1) dict lookup after index is built.

    The index itself is cached by _build_delegate_index; the slice is a fast
    iloc on pre-computed integer positions, no boolean column scan.
    """
    if not delegate_id or df_merged.empty or "delegate_id" not in df_merged.columns:
        return pd.DataFrame(columns=df_merged.columns)
    idx = _build_delegate_index(df_merged)
    positions = idx.get(delegate_id)
    if positions is None or len(positions) == 0:
        return pd.DataFrame(columns=df_merged.columns)
    return df_merged.take(positions)


@st.cache_data(hash_funcs=_HASH_FUNCS)
def build_name_to_id(df_p: pd.DataFrame, name_col: str) -> dict[str, str]:
    """Build a name → delegate_id lookup dict from the persons frame.

    Cached so the sidebar on_change callback pays O(1) for every lookup
    instead of scanning df_p each time.
    """
    if df_p.empty or name_col not in df_p.columns or "delegate_id" not in df_p.columns:
        return {}
    tmp = (
        df_p.dropna(subset=[name_col, "delegate_id"])
        .drop_duplicates(subset=[name_col])
    )
    return dict(zip(tmp[name_col].astype(str), tmp["delegate_id"].astype(str)))


@st.cache_data(hash_funcs=_HASH_FUNCS)
def build_sidebar_options(
    df_p: pd.DataFrame,
    df_merged: pd.DataFrame,
    name_col: str,
    prov_col: str | None,
) -> tuple[list[str], list[str], int, int]:
    """Pre-compute sidebar option lists — cached so they don't rerun on every interaction.

    Returns (delegate_options, provinces, year_min, year_max).
    These are derived entirely from the two DataFrames so they share the
    build_merged cache lifetime automatically.
    """
    delegate_options: list[str] = (
        sorted(df_p[name_col].dropna().astype(str).unique().tolist())
        if not df_p.empty and name_col in df_p.columns
        else []
    )
    provinces: list[str] = (
        sorted(df_merged[prov_col].dropna().unique().tolist())
        if prov_col and not df_merged.empty and prov_col in df_merged.columns
        else []
    )
    ymin: int = (
        int(df_merged["j"].dropna().min())
        if not df_merged.empty and "j" in df_merged.columns and df_merged["j"].notna().any()
        else 1700
    )
    ymax: int = (
        int(df_merged["j"].dropna().max())
        if not df_merged.empty and "j" in df_merged.columns and df_merged["j"].notna().any()
        else 1800
    )
    return delegate_options, provinces, ymin, ymax
