"""
Delegate QA – shared utilities
===============================
Pure / data-tier functions used by sheet.py and the individual tab modules.
The only streamlit import here is ``@st.cache_data`` (load_data, enrich_persons_from_abbrd, build_merged).
"""
from __future__ import annotations

import datetime
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st  # only for @st.cache_data


# ---------------------------------------------------------------------------
# FAST DATAFRAME HASH  (used in hash_funcs= to avoid O(n) rehashing)
# ---------------------------------------------------------------------------

def toggle_state_flag(key: str, default: bool = False) -> bool:
    """Toggle a bool in Streamlit session state and return new value."""
    current = bool(st.session_state.get(key, default))
    new = not current
    st.session_state[key] = new
    return new


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
CORRECTIONS_FILE        = _WS / "corrections.json"
STAGED_CORRECTIONS_FILE   = _WS / "staged_corrections.json"
APPROVED_CORRECTIONS_FILE = _WS / "approved_corrections.json"
NEW_DELEGATES_FILE        = _WS / "new_delegates.json"
MERGE_DISMISSALS_FILE     = _WS / "merge_dismissals.json"
PATTERN_SYNONYMS_FILE     = _WS / "pattern_synonyms.json"
PROVINCE_ORDER_FILE     = _WS / "province_order.json"
REMAPPINGS_FILE         = _WS / "remappings.json"
SANDBOXED_FILE          = _WS / "sandboxed.json"
REVIEWED_FILE           = _WS / "reviewed.json"
PATTERN_STATUS_FILE     = _WS / "pattern_status.json"
APP_CONFIG_FILE         = _WS / "app_config.toml"

REPUBLIC_ADD_PREFIX = "republic_add_"

MIN_AGE     = 25
MAX_AGE     = 70
DEFAULT_GAP = 10

DEFAULT_CORRECTION_FIELDS = ["to_id", "from_id", "name", "updated_at", "source"]

# ---------------------------------------------------------------------------
# CORRECTIONS PERSISTENCE
# ---------------------------------------------------------------------------

def _get_corrections_config(config: dict | None = None) -> dict:
    """Return the corrections configuration section with defaults."""
    if config is None:
        config = load_config()
    config = normalize_config(config)
    return config.get("corrections", {})


def _normalize_correction_entry(entry, config: dict | None = None):
    corr_cfg = _get_corrections_config(config)
    to_id_key = corr_cfg.get("to_id_key", "to_id")
    from_id_key = corr_cfg.get("from_id_key", "from_id")
    name_key = corr_cfg.get("name_key", "name")
    updated_at_key = corr_cfg.get("updated_at_key", "updated_at")
    source_key = corr_cfg.get("source_key", "source")
    source_default = corr_cfg.get("source_default", "manual")
    source_legacy = corr_cfg.get("source_legacy", "legacy")

    if isinstance(entry, dict):
        normalized = {
            to_id_key: str(entry.get(to_id_key, "")) if entry.get(to_id_key) is not None else "",
            from_id_key: str(entry.get(from_id_key, "")) if entry.get(from_id_key) is not None else "",
            name_key: str(entry.get(name_key, "")) if entry.get(name_key) is not None else "",
            updated_at_key: entry.get(updated_at_key) or datetime.datetime.now().isoformat(timespec="seconds"),
            source_key: str(entry.get(source_key, source_default)),
        }
    else:
        normalized = {
            to_id_key: str(entry),
            from_id_key: "",
            name_key: "",
            updated_at_key: datetime.datetime.now().isoformat(timespec="seconds"),
            source_key: source_legacy,
        }
    return normalized


def make_correction_entry(
    to_id: int | str,
    from_id: str | None = None,
    name: str | None = None,
    source: str | None = None,
    config: dict | None = None,
) -> dict:
    """Create a correction entry with fields coming from config defaults."""
    corr_cfg = _get_corrections_config(config)
    to_id_key = corr_cfg.get("to_id_key", "to_id")
    from_id_key = corr_cfg.get("from_id_key", "from_id")
    name_key = corr_cfg.get("name_key", "name")
    updated_at_key = corr_cfg.get("updated_at_key", "updated_at")
    source_key = corr_cfg.get("source_key", "source")
    source_default = corr_cfg.get("source_default", "manual")

    entry = {
        to_id_key: str(to_id),
        from_id_key: str(from_id) if from_id is not None else "",
        name_key: str(name) if name is not None else "",
        updated_at_key: datetime.datetime.now().isoformat(timespec="seconds"),
        source_key: str(source or source_default),
    }
    return entry


def load_corrections(config: dict | None = None) -> dict:
    corr_cfg = _get_corrections_config(config)
    if CORRECTIONS_FILE.exists():
        try:
            raw = json.loads(CORRECTIONS_FILE.read_text())
            out = {}
            for k, v in raw.items():
                try:
                    idx = int(k)
                except ValueError:
                    continue
                out[idx] = _normalize_correction_entry(v, config=corr_cfg)
            return out
        except Exception:
            return {}
    return {}


def save_corrections(corrections: dict, config: dict | None = None) -> None:
    corr_cfg = _get_corrections_config(config)
    safe = {}
    for k, v in corrections.items():
        safe[str(k)] = _normalize_correction_entry(v, config=corr_cfg)
    CORRECTIONS_FILE.write_text(json.dumps(safe, indent=2))


def load_staged_corrections() -> dict:
    if STAGED_CORRECTIONS_FILE.exists():
        try:
            raw = json.loads(STAGED_CORRECTIONS_FILE.read_text())
            return {int(k): v for k, v in raw.items()}
        except Exception:
            return {}
    return {}


def save_staged_corrections(corrections: dict) -> None:
    STAGED_CORRECTIONS_FILE.write_text(
        json.dumps({str(k): v for k, v in corrections.items()}, indent=2)
    )


def load_approved_corrections() -> dict:
    if APPROVED_CORRECTIONS_FILE.exists():
        try:
            raw = json.loads(APPROVED_CORRECTIONS_FILE.read_text())
            return {int(k): v for k, v in raw.items()}
        except Exception:
            return {}
    return {}


def save_approved_corrections(corrections: dict) -> None:
    APPROVED_CORRECTIONS_FILE.write_text(
        json.dumps({str(k): v for k, v in corrections.items()}, indent=2)
    )


# ---------------------------------------------------------------------------
# REVIEWED IDS PERSISTENCE
# ---------------------------------------------------------------------------

def load_reviewed() -> set[str]:
    """Return the set of delegate_ids the user has marked as fully reviewed."""
    if REVIEWED_FILE.exists():
        try:
            return set(str(x) for x in json.loads(REVIEWED_FILE.read_text()))
        except Exception:
            return set()
    return set()


def load_pattern_status() -> dict[str, bool]:
    """Return saved explicit pattern validity status per occurrence key."""
    if PATTERN_STATUS_FILE.exists():
        try:
            return {str(k): bool(v) for k, v in json.loads(PATTERN_STATUS_FILE.read_text()).items()}
        except Exception:
            return {}
    return {}


def save_pattern_status(status: dict[str, bool]) -> None:
    """Persist pattern validity status."""
    PATTERN_STATUS_FILE.write_text(json.dumps({str(k): bool(v) for k, v in status.items()}, indent=2))


# ---------------------------------------------------------------------------
# FLAGGED PATTERNS  (patterns that cannot be resolved to a delegate)
# ---------------------------------------------------------------------------

FLAGGED_PATTERNS_FILE = _WS / "flagged_patterns.json"


def load_flagged_patterns() -> set[str]:
    """Return the set of pattern strings flagged as unresolvable."""
    if FLAGGED_PATTERNS_FILE.exists():
        try:
            return set(json.loads(FLAGGED_PATTERNS_FILE.read_text()))
        except Exception:
            return set()
    return set()


def save_flagged_patterns(patterns: set[str]) -> None:
    """Persist the complete set of flagged patterns."""
    FLAGGED_PATTERNS_FILE.write_text(json.dumps(sorted(patterns), indent=2))


def save_reviewed(reviewed: set[str]) -> None:
    """Persist the reviewed delegate IDs.

    The file reflects exactly the provided set. If you want to merge
    with existing values, do that before calling this function.
    """
    REVIEWED_FILE.write_text(json.dumps(sorted(set(reviewed)), indent=2))


def apply_corrections(df: pd.DataFrame, corrections: dict, config: dict | None = None) -> pd.DataFrame:
    """Return a copy of *df* with all staged corrections applied.

    Only touches rows that exist in the index — silently skips stale keys.
    Does NOT modify the cached df_merged in place.
    """
    if not corrections:
        return df
    out = df.copy()
    corr_cfg = _get_corrections_config(config)
    to_id_key = corr_cfg.get("to_id_key", "to_id")

    valid = {}
    for ridx, entry in corrections.items():
        if ridx in out.index:
            if isinstance(entry, dict) and to_id_key in entry:
                valid[ridx] = entry[to_id_key]
            else:
                valid[ridx] = entry
    if valid:
        idxs = list(valid.keys())
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
# MERGE-ERROR DISMISSALS PERSISTENCE
# Dismissals are a set of (pattern, delegate_id) string pairs.
# They are stored as a JSON list of two-element lists for portability.
# ---------------------------------------------------------------------------

def load_merge_dismissals() -> set[tuple[str, str]]:
    """Return the set of (pattern, delegate_id) pairs dismissed as false positives."""
    if MERGE_DISMISSALS_FILE.exists():
        try:
            raw = json.loads(MERGE_DISMISSALS_FILE.read_text())
            return {(str(item[0]), str(item[1])) for item in raw if len(item) == 2}
        except Exception:
            return set()
    return set()


def save_merge_dismissals(dismissals: set[tuple[str, str]]) -> None:
    """Persist the full set of merge-error dismissals to disk."""
    MERGE_DISMISSALS_FILE.write_text(
        json.dumps(sorted(list(dismissals)), indent=2, default=str)
    )


# ---------------------------------------------------------------------------
# PATTERN SYNONYMS PERSISTENCE
# Each entry: {"delegate_id": str, "anchor": str,
#              "patterns": [fragment_a, fragment_b],
#              "freq_a": int, "freq_b": int}
# The less-frequent pattern is the "ghost" to suppress in n_patterns counts.
# ---------------------------------------------------------------------------

def load_pattern_synonyms() -> list[dict]:
    """Return the list of registered fragment-synonym pairs."""
    if PATTERN_SYNONYMS_FILE.exists():
        try:
            return json.loads(PATTERN_SYNONYMS_FILE.read_text())
        except Exception:
            return []
    return []


def save_pattern_synonyms(synonyms: list[dict]) -> None:
    """Persist the full list of pattern synonym pairs to disk."""
    PATTERN_SYNONYMS_FILE.write_text(json.dumps(synonyms, indent=2, default=str))


# ---------------------------------------------------------------------------
# CONFIGURATION PERSISTENCE
# ---------------------------------------------------------------------------

def load_config(default: dict | None = None) -> dict:
    """Load app configuration from disk.

    Returns a dict. If the file is missing or invalid, returns `default` (or {}).
    """
    if default is None:
        default = {}
    if APP_CONFIG_FILE.exists():
        try:
            import tomllib

            return tomllib.loads(APP_CONFIG_FILE.read_text())
        except Exception:
            return default
    return default


def normalize_config(config: dict | None = None) -> dict:
    """Ensure the config has all expected keys and sane defaults.

    This prevents missing config entries from causing runtime issues and
    normalizes types (e.g., numeric fields stored as strings).
    """
    if config is None:
        config = {}

    # Tab0 settings (Overview tab)
    tab0 = config.setdefault("tab0", {})
    tab0.setdefault("sort_mode", "Work queue (unreviewed first)")
    tab0.setdefault("sort_primary", "Work queue (unreviewed first)")
    tab0.setdefault("sort_secondary", "Delegate ID")
    tab0.setdefault("search_term", "")
    try:
        tab0["select_col_pos"] = int(tab0.get("select_col_pos", 0))
    except Exception:
        tab0["select_col_pos"] = 0

    # Abbrd config
    abbrd = config.setdefault("abbrd", {})
    abbrd.setdefault("sheet", "lookup")
    abbrd.setdefault("id_col", "id_persoon")
    abbrd.setdefault("name_col", "fullname")
    try:
        abbrd["max_preview_fields"] = int(abbrd.get("max_preview_fields", 6))
    except Exception:
        abbrd["max_preview_fields"] = 6
    abbrd["auto_refresh"] = bool(abbrd.get("auto_refresh", False))
    abbrd["disable_cache"] = bool(abbrd.get("disable_cache", False))

    # Corrections format defaults (allow these to be customized in app_config.toml)
    corr_cfg = config.setdefault("corrections", {})
    corr_cfg.setdefault("to_id_key", "to_id")
    corr_cfg.setdefault("from_id_key", "from_id")
    corr_cfg.setdefault("name_key", "name")
    corr_cfg.setdefault("updated_at_key", "updated_at")
    corr_cfg.setdefault("source_key", "source")
    corr_cfg.setdefault("fields", DEFAULT_CORRECTION_FIELDS.copy())
    corr_cfg.setdefault("source_default", "manual")
    corr_cfg.setdefault("source_legacy", "legacy")

    # Field map must be a dict; if it's stored as a string or missing, restore defaults
    default_field_map = {
        "fullname": "fullname",
        "id_persoon": "cons_id_str",
        "voornaam": "voornaam",
        "tussenvoegsel": "tussenvoegsel",
        "geslachtsnaam": "geslachtsnaam",
        "geboortejaar": "geboortejaar",
        "overlijden": "overlijdensjaar",
        "beginjaar": "minjaar",
        "eindjaar": "maxjaar",
        "hlife": "hlife",
        "provincie": "provincie",
    }

    fm = abbrd.get("field_map")
    if not isinstance(fm, dict):
        # Support legacy config where field_map is stored at the top level.
        legacy_fm = config.get("field_map")
        if isinstance(legacy_fm, dict):
            abbrd["field_map"] = legacy_fm
        else:
            abbrd["field_map"] = default_field_map
    else:
        # Ensure all essential keys exist
        for k, v in default_field_map.items():
            fm.setdefault(k, v)

    return config


# ---------------------------------------------------------------------------
# DELEGATE EDITS PERSISTENCE
# ---------------------------------------------------------------------------

def load_delegate_edits() -> dict[str, dict]:
    """Load staged delegate edits (applied to df_p before rendering)."""
    edits_file = _WS / "delegate_edits.json"
    if edits_file.exists():
        try:
            return {str(k): v for k, v in json.loads(edits_file.read_text()).items()}
        except Exception:
            return {}
    return {}


def save_delegate_edits(edits: dict[str, dict]) -> None:
    """Save staged delegate edits."""
    edits_file = _WS / "delegate_edits.json"
    edits_file.write_text(json.dumps(edits, indent=2, default=str))


def apply_delegate_edits(df: pd.DataFrame, edits: dict[str, dict]) -> pd.DataFrame:
    """Apply staged edits to a persons DataFrame."""
    if not edits:
        return df
    out = df.copy()
    out["delegate_id"] = out["delegate_id"].astype(str)
    # Update existing rows
    for did, changes in edits.items():
        mask = out["delegate_id"] == str(did)
        if mask.any():
            for k, v in changes.items():
                if k == "delegate_id":
                    continue
                out.loc[mask, k] = v
        else:
            # Add new row for unknown delegate_id
            row = {"delegate_id": str(did)}
            row.update(changes)
            out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)
    return out


def _dump_toml(obj: object, indent: int = 0) -> str:
    """Minimal TOML serializer for our simple config structure."""
    indent_str = "" if indent == 0 else " " * indent
    if isinstance(obj, dict):
        out = []
        for k, v in obj.items():
            if isinstance(v, dict):
                out.append(f"[{k}]")
                out.append(_dump_toml(v, indent=0))
            else:
                out.append(f"{k} = {_dump_toml(v, indent=0)}")
        return "\n".join(out)
    if isinstance(obj, bool):
        return "true" if obj else "false"
    if isinstance(obj, (int, float)):
        return str(obj)
    if isinstance(obj, str):
        # Escape backslashes and quotes
        return '"' + obj.replace('\\', '\\\\').replace('"', '\\"') + '"'
    if isinstance(obj, list):
        return "[" + ", ".join(_dump_toml(v, indent=0) for v in obj) + "]"
    # fallback to JSON string
    return '"' + str(obj).replace('"', '\\"') + '"'


def save_config(cfg: dict) -> None:
    """Save app configuration to disk."""
    APP_CONFIG_FILE.write_text(_dump_toml(cfg))


def rerun() -> None:
    """Trigger a Streamlit rerun in a version-compatible way."""
    # In modern Streamlit, button clicks already refresh.
    # For explicit rerun calls, raise the streamlit RerunException with proper data.
    try:
        from streamlit.runtime.scriptrunner_utils.script_requests import RerunData
        from streamlit.runtime.scriptrunner_utils.exceptions import RerunException

        rerun_data = RerunData()
        # Defensive: some code paths may have put a dict here, e.g. from older logic.
        if isinstance(rerun_data, dict):
            rerun_data = RerunData(**rerun_data)

        raise RerunException(rerun_data=rerun_data)
    except Exception:
        # streamlit runtime may not support this interface in old versions.
        # Silently continue and rely on user interaction to rerun.
        pass


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


def _strip_excel_escapes(df: pd.DataFrame) -> pd.DataFrame:
    """Remove openpyxl XML escape sequences from string columns.

    Excel cells that contain embedded carriage returns (Alt+Enter line breaks)
    are encoded by openpyxl as ``_x000D_`` followed by a newline.  Strip those
    sequences and collapse any resulting runs of whitespace so names like
    ``Cock_x000D_\\nCock, Bernardus`` become ``Cock, Bernardus``.
    """
    for col in df.select_dtypes(include="object").columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"_x000D_\s*", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .where(df[col].notna(), other=None)
        )
    return df


def _load_data_uncached(source_mtimes: tuple[float, ...] = ()) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    """Load data directly from disk without any Streamlit caching."""

    df_p, _ = _read_df(PERSONS_CANDIDATES)
    df_p = _strip_excel_escapes(df_p)
    if "delegate_id" in df_p.columns:
        df_p["delegate_id"] = pd.to_numeric(df_p["delegate_id"], errors="coerce").astype("Int64")
        df_p["delegate_id"] = df_p["delegate_id"].astype(str)

    df_i, _ = _read_df(OCCURRENCES_CANDIDATES)
    df_i = _strip_excel_escapes(df_i)
    if "delegate_id" in df_i.columns:
        df_i["delegate_id"] = pd.to_numeric(df_i["delegate_id"], errors="coerce").astype("Int64")
        df_i["delegate_id"] = df_i["delegate_id"].astype(str)

    df_abbrd: pd.DataFrame | None = None
    abbrd_path = next(
        (p for p in ABBRD_CANDIDATES if p.with_suffix(".parquet").exists() or p.exists()), None
    )
    if abbrd_path is not None:
        parq = abbrd_path.with_suffix(".parquet")

        # Allow overriding the lookup sheet name via config (e.g. use a sheet named "lookup").
        cfg = load_config()
        sheet_name = cfg.get("abbrd", {}).get("sheet", "lookup")

        if parq.exists():
            df_abbrd = pd.read_parquet(parq)
        else:
            try:
                df_abbrd = pd.read_excel(abbrd_path, sheet_name=sheet_name)
            except Exception:
                df_abbrd = pd.read_excel(abbrd_path)

        df_abbrd.columns = df_abbrd.columns.str.strip()
        df_abbrd = _strip_excel_escapes(df_abbrd)
    return df_p, df_i, df_abbrd


@st.cache_data(persist="disk")
def _load_data_cached(source_mtimes: tuple[float, ...] = ()) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    """Cached loader for the main data sources."""
    return _load_data_uncached(source_mtimes)


def load_data(source_mtimes: tuple[float, ...] = ()) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    """Returns (persons, occurrences, abbrd).

    This is the entry point used by the app. It will bypass Streamlit's cache
    when `abbrd.disable_cache` is enabled in `app_config.toml`.
    """
    cfg = load_config()
    if cfg.get("abbrd", {}).get("disable_cache"):
        return _load_data_uncached(source_mtimes)
    return _load_data_cached(source_mtimes)


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
        extra_sub = extra_df[persons.columns]
        # Avoid FutureWarning from concatenating columns that are all-NA.
        keep_cols = [c for c in extra_sub.columns if c == "delegate_id" or not extra_sub[c].isna().all()]
        if "delegate_id" not in keep_cols and "delegate_id" in extra_sub.columns:
            keep_cols.insert(0, "delegate_id")
        persons = pd.concat([persons, extra_sub[keep_cols]], ignore_index=True)

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

    # Pattern validity marker for filtering and corrections.
    df["pattern_is_valid"] = True
    if "pattern" in df.columns:
        invalid_patterns = {"invalid", "<invalid>", "none", "<none>"}
        inv_mask = df["pattern"].astype(str).str.lower().isin(invalid_patterns)
        df.loc[inv_mask, "pattern_is_valid"] = False

    persisted = load_pattern_status()
    if "delegate_id" in df.columns and "pattern" in df.columns:
        def _pattern_key(series):
            did = str(series.get("delegate_id", ""))
            pat = str(series.get("pattern", ""))
            year = "" if pd.isna(series.get("j")) else str(int(series.get("j")))
            return f"{did}|{pat}|{year}"

        keys = df.apply(_pattern_key, axis=1)
        for idx, key in keys.items():
            if key in persisted:
                df.at[idx, "pattern_is_valid"] = persisted[key]

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

    Issue columns added (sortable in the overview grid), listed in priority order:

      n_pattern_anomalies — occurrences whose pattern's Levenshtein normalised
                            distance from the delegate's modal pattern exceeds
                            PATTERN_ANOMALY_THRESHOLD (default 0.5).  Uses
                            rapidfuzz when available, falls back to character
                            overlap ratio otherwise.  This is the primary
                            quality signal: a delegate with many anomalous
                            patterns is the most likely candidate for a
                            mis-identification.

      n_alive_flags       — rows where age_at_event < MIN_AGE, > MAX_AGE, or
                            j > death_year.  Equal weight to pattern anomalies
                            because the two signals often coincide (a wrong
                            person inserted into a sequence produces both a
                            name-pattern outlier AND an impossible age).

      max_gap_years       — largest consecutive year gap between appearances.
                            Lower weight: a long gap can be legitimate
                            (illness, diplomatic mission, etc.), so it is a
                            weaker signal than the two above.

      n_name_mismatches   — occurrences where the delegate's geslachtsnaam is
                            entirely absent from the pattern string.  Kept for
                            completeness but excluded from the composite issue
                            score because it generates many false positives
                            (abbreviations, pre-marriage name changes, etc.).

    Issue score formula used by the overview grid's "Issue score (worst first)"
    sort and the suspicious-delegate filter:

        score = n_pattern_anomalies × 2
              + n_alive_flags        × 2
              + max_gap_years        × 1
              + n_name_mismatches    × 0   # visible in table, not in score

    Column order in the returned DataFrame places issue columns at the right
    in priority order: n_pattern_anomalies, n_alive_flags, max_gap_years,
    n_name_mismatches.
    """
    # Normalised-distance threshold for classifying a pattern as anomalous.
    # Matches the default value of the threshold slider in tab2_patterns.py.
    PATTERN_ANOMALY_THRESHOLD = 0.5
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

    # ---- n_patterns ghost-pattern correction ------------------------------
    # Fragment synonyms: the less-frequent variant of each pair is a "ghost"
    # pattern that inflates n_patterns.  Subtract the ghost count per delegate
    # so that each synonym pair contributes only 1 to the distinct-pattern tally.
    if "n_patterns" in summary.columns:
        try:
            _synonyms = load_pattern_synonyms()
            if _synonyms:
                # Build ghost_set: {(delegate_id, ghost_pattern)} where ghost is
                # the pattern with lower frequency in the pair.
                _ghost_set: set[tuple[str, str]] = set()
                for _syn in _synonyms:
                    _did = str(_syn.get("delegate_id", ""))
                    _pats = _syn.get("patterns", [])
                    _fa   = int(_syn.get("freq_a", 0))
                    _fb   = int(_syn.get("freq_b", 0))
                    if len(_pats) == 2:
                        _ghost = str(_pats[0]) if _fa <= _fb else str(_pats[1])
                        _ghost_set.add((_did, _ghost))
                if _ghost_set:
                    # Count how many ghosts each delegate has
                    _ghost_counts: dict[str, int] = {}
                    for (_did, _pat) in _ghost_set:
                        _ghost_counts[_did] = _ghost_counts.get(_did, 0) + 1
                    _summary_did = summary["delegate_id"].astype(str)
                    _deduct = _summary_did.map(_ghost_counts).fillna(0).astype(int)
                    summary["n_patterns"] = (summary["n_patterns"] - _deduct).clip(lower=1)
        except Exception:
            pass  # never break the pipeline over an optional correction

    # ---- pattern anomalies (primary quality signal) -----------------------
    # For each delegate, count occurrences whose pattern's normalised
    # Levenshtein distance from the delegate's modal pattern exceeds
    # PATTERN_ANOMALY_THRESHOLD.  Uses rapidfuzz when available.
    if "pattern" in df_merged.columns:
        try:
            from rapidfuzz import distance as _rfd
            def _norm_dist(a: str, b: str) -> float:
                return _rfd.Levenshtein.normalized_distance(a, b)
        except ImportError:
            def _norm_dist(a: str, b: str) -> float:  # type: ignore[misc]
                return 1 - sum(x == y for x, y in zip(a, b)) / max(len(a), len(b), 1)

        # .astype(object) escapes Categorical dtype so .fillna("") won't fail
        # when "" is not already in the category set.
        _pat_series = df_merged["pattern"].astype(object).fillna("").astype(str)
        _did_series = df_merged["delegate_id"].astype(object).astype(str)
        # Modal pattern per delegate
        _modal = (
            _pat_series.groupby(_did_series, observed=True)
            .agg(lambda s: s.mode().iloc[0] if not s.empty else "")
        )
        # Align modal pattern to every occurrence row
        _modal_aligned = _did_series.map(_modal).fillna("")
        _anomaly_flag = pd.Series(
            [_norm_dist(p, m) > PATTERN_ANOMALY_THRESHOLD
             for p, m in zip(_pat_series, _modal_aligned)],
            index=df_merged.index,
            dtype=bool,
        )
        anom_counts = (
            _anomaly_flag.groupby(_did_series, observed=True)
            .sum()
            .astype(int)
            .rename("n_pattern_anomalies")
        )
        summary = summary.merge(anom_counts, on="delegate_id", how="left")

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

    # Ensure delegates that appear in the persons file but have zero occurrences
    # still show up in the overview (with NaNs for counts).
    if "delegate_id" in df_p.columns:
        all_delegates = pd.DataFrame({"delegate_id": df_p["delegate_id"].astype(str).unique()})
        summary = all_delegates.merge(summary, on="delegate_id", how="left")

    # ---- name lookup & column ordering ------------------------------------
    if name_col in df_p.columns:
        summary = summary.copy()
        summary["delegate_id"] = summary["delegate_id"].astype(str)
        df_p_copy = df_p[["delegate_id", name_col]].copy()
        df_p_copy["delegate_id"] = df_p_copy["delegate_id"].astype(str)
        summary = summary.merge(
            df_p_copy.drop_duplicates(subset=["delegate_id"]),
            on="delegate_id", how="left",
        )
    issue_cols = [c for c in ("n_pattern_anomalies", "n_alive_flags", "max_gap_years", "n_name_mismatches")
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
    # Normalize delegate_id values to strings so lookups are consistent across types.
    # This avoids issues where the same ID is stored as int in the DataFrame but selected
    # as a string via the UI.
    df = df_merged.copy()

    def _normalize(v: object) -> str:
        if pd.isna(v):
            return ""
        s = str(v).strip()
        if s.endswith(".0") and s[:-2].isdigit():
            return s[:-2]
        return s

    df["delegate_id"] = df["delegate_id"].map(_normalize).astype(str)

    # reset_index so iloc positions match .indices values
    return df.reset_index(drop=True).groupby("delegate_id", sort=False, observed=True).indices  # type: ignore[return-value]


def get_delegate_slice(df_merged: pd.DataFrame, delegate_id: str) -> pd.DataFrame:
    """Return all rows for one delegate — O(1) dict lookup after index is built.

    The index itself is cached by _build_delegate_index; the slice is a fast
    iloc on pre-computed integer positions, no boolean column scan.
    """
    if not delegate_id or df_merged.empty or "delegate_id" not in df_merged.columns:
        return pd.DataFrame(columns=df_merged.columns)

    def _normalize(v: object) -> str:
        if pd.isna(v):
            return ""
        s = str(v).strip()
        if s.endswith(".0") and s[:-2].isdigit():
            return s[:-2]
        return s

    norm_id = _normalize(delegate_id)
    idx = _build_delegate_index(df_merged)
    positions = idx.get(norm_id)

    # Fallback: if the cached index doesn't contain this key, do a safe string
    # lookup so numeric/int mismatches don't prevent matching.
    if positions is None or len(positions) == 0:
        if "delegate_id" in df_merged.columns:
            mask = df_merged["delegate_id"].astype(str).map(_normalize) == norm_id
            if mask.any():
                return df_merged.loc[mask]
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


# ---------------------------------------------------------------------------
# Suggestion store  (Q-K-V pattern retrieval)
# ---------------------------------------------------------------------------

_KNOWN_PROVINCES: frozenset[str] = frozenset([
    "Gelderland", "Holland", "Zeeland", "Utrecht",
    "Friesland", "Overijssel", "Groningen",
])


@st.cache_data(hash_funcs=_HASH_FUNCS, show_spinner="Building suggestion store…")
def build_suggestion_store(df_merged: pd.DataFrame) -> dict:
    """Build a dual TF-IDF store from all labeled (delegate_id > 0) rows.

    Two complementary vectorizers are fitted on the same corpus:

    char_wb (ngram 2-4)  – character n-grams inside word boundaries.
      Catches OCR noise and spelling variants within a token
      (e.g. 'Gockinga' ↔ 'Gocknga', 'Goes' ↔ 'Goos').  High recall
      for corrupted patterns.

    word (ngram 1-2)  – split on whitespace/punctuation, lowercase.
      Catches exact surname token matches even when surrounding
      prepositional tokens ('van', 'de', 'der') vary.  High precision
      for intact tokens.  Subtoken matching: single tokens like 'Goes'
      from 'van der Goes' score well against any key document that
      contains 'Goes' as a word, regardless of the surrounding tokens.

    Scores are combined as: 0.6 × sim_char + 0.4 × sim_word.
    This gives full benefit of intra-token noise tolerance while still
    rewarding exact surname hits.

    Returns a plain dict (trivially picklable by @st.cache_data):
      {
        "vec_char":       TfidfVectorizer (char_wb, fitted),
        "vec_word":       TfidfVectorizer (word, fitted),
        "key_char":       scipy sparse [n_delegates × char_vocab],
        "key_word":       scipy sparse [n_delegates × word_vocab],
        "id_index":       list[str] – matrix row i → delegate_id,
        "meta":           DataFrame – delegate_id, first_year, last_year, provincie,
      }
    Returns {} if there is no usable labeled data.
    """
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
    except ImportError:
        return {}

    if df_merged.empty or "pattern" not in df_merged.columns:
        return {}

    ids_num = pd.to_numeric(df_merged["delegate_id"], errors="coerce")
    labeled = df_merged[ids_num > 0].copy()
    if labeled.empty:
        return {}

    # One document per delegate – join all pattern strings
    docs = (
        labeled.groupby("delegate_id", observed=True)["pattern"]
        .agg(lambda s: " ".join(s.dropna().astype(str)))
        .reset_index()
        .rename(columns={"pattern": "doc"})
    )

    # Character n-gram vectorizer – intra-token noise tolerance
    vec_char = TfidfVectorizer(
        analyzer="char_wb", ngram_range=(2, 4), min_df=1, sublinear_tf=True
    )
    key_char = vec_char.fit_transform(docs["doc"])

    # Word / subtoken vectorizer – exact surname token matching
    vec_word = TfidfVectorizer(
        analyzer="word",
        token_pattern=r"(?u)\b\w+\b",
        ngram_range=(1, 2),
        min_df=1,
        sublinear_tf=True,
        lowercase=True,
    )
    key_word = vec_word.fit_transform(docs["doc"])

    # Per-delegate metadata used for temporal gate + province constraint
    meta = labeled.groupby("delegate_id", observed=True).agg(
        first_year=("j", "min"),
        last_year=("j", "max"),
        provincie=(
            "provincie",
            lambda s: s.dropna().mode().iloc[0] if s.notna().any() else "",
        ),
    ).reset_index()
    meta = docs[["delegate_id"]].merge(meta, on="delegate_id", how="left")
    meta["provincie"] = meta["provincie"].fillna("")

    return {
        "vec_char": vec_char,
        "vec_word": vec_word,
        "key_char": key_char,
        "key_word": key_word,
        "id_index": docs["delegate_id"].tolist(),
        "meta": meta,
    }


def query_suggestions(
    store: dict,
    query_df: pd.DataFrame,
    top_k: int = 3,
    year_tolerance: int = 10,
    min_score: float = 0.0,
) -> pd.DataFrame:
    """Score unresolved occurrences against the key store.

    For each query row:
      1. Cosine similarity of pattern embedding vs all key vectors.
      2. Temporal gate  – zero candidates whose active range doesn't overlap
         query year ± year_tolerance.
      3. Province constraint (delegate rows only) – if namens is a known
         province, zero candidates whose provincie doesn't match.
         Skipped entirely when class == 'president'.
      4. Return top_k survivors (score > 0) as candidate columns.

    Returns a DataFrame with columns:
      orig_idx, pattern, j, class, namens,
      cand_1 … cand_{top_k}, score_1 … score_{top_k}
    Rows where all candidates score 0 are included with None candidates so
    the user can see them and mark them unresolvable.
    """
    try:
        from sklearn.metrics.pairwise import cosine_similarity
    except ImportError:
        return pd.DataFrame()

    if not store or query_df.empty:
        return pd.DataFrame()

    vec_char = store["vec_char"]
    vec_word = store["vec_word"]
    key_char = store["key_char"]
    key_word = store["key_word"]
    id_index = store["id_index"]
    meta: pd.DataFrame = store["meta"]

    first_years = pd.to_numeric(meta["first_year"], errors="coerce").to_numpy()
    last_years = pd.to_numeric(meta["last_year"], errors="coerce").to_numpy()
    provs = meta["provincie"].fillna("").to_numpy()

    patterns = query_df["pattern"].fillna("").astype(str).tolist()

    # Combined score: 60% character n-gram + 40% word subtoken
    sim_char = cosine_similarity(vec_char.transform(patterns), key_char)
    sim_word = cosine_similarity(vec_word.transform(patterns), key_word)
    sim = 0.6 * sim_char + 0.4 * sim_word  # [n_queries × n_delegates]

    rows = []
    for i, (orig_idx, occ) in enumerate(query_df.iterrows()):
        scores = sim[i].copy()

        # 1. Temporal gate
        q_year = pd.to_numeric(occ.get("j"), errors="coerce")
        if not pd.isna(q_year):
            scores[(q_year < (first_years - year_tolerance)) |
                   (q_year > (last_years + year_tolerance))] = 0.0

        # 2. Province constraint – skip for president rows
        if str(occ.get("class", "")).lower() != "president":
            q_prov = str(occ.get("namens", "")).strip()
            if q_prov in _KNOWN_PROVINCES:
                scores[provs != q_prov] = 0.0

        top_idx = scores.argsort()[::-1][:top_k]
        row: dict = {
            "orig_idx": orig_idx,
            "pattern": occ.get("pattern", ""),
            "j": occ.get("j"),
            "class": occ.get("class", ""),
            "namens": occ.get("namens", ""),
        }
        for rank in range(1, top_k + 1):
            k = top_idx[rank - 1]
            sc = float(scores[k])
            if sc >= min_score and sc > 0:
                row[f"cand_{rank}"] = id_index[k]
                row[f"score_{rank}"] = round(sc, 3)
            else:
                row[f"cand_{rank}"] = None
                row[f"score_{rank}"] = 0.0
        rows.append(row)

    return pd.DataFrame(rows)
