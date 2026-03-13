# Delegate QA

Multi-tab Streamlit application for detecting and correcting misidentified delegates in 18th-century Dutch Republic meeting records.

## What it does

Delegates from different provinces attended the States General in a fixed precedence order (Gelderland → Holland → Zeeland → Utrecht → Friesland → Overijssel → Groningen). In the identification process errors were introduced: wrong persons were assigned to occurrence rows, names were garbled, or the same person appeared under multiple IDs. This app surfaces those problems tab by tab and lets you correct them without touching the source files.

## Tabs

| Tab | Check |
|-----|-------|
| **📋 Overview** | Summary table of all delegates: occurrence counts, active year range, number of patterns. Work-queue sorted: unreviewed first, then corrected-but-not-reviewed, then done. |
| **🧬 Alive Check** | Flags occurrences where the delegate's age at the event was implausible (< 16, > 90, or after death year) |
| **🔤 Pattern Anomalies** | Detects name patterns that diverge strongly from a delegate's modal pattern (Levenshtein distance via `rapidfuzz`) |
| **📛 Name Mismatch** | Checks whether the occurrence pattern contains the delegate's surname (`geslachtsnaam`) |
| **⏳ Timeline Gaps** | Highlights delegates with unusually long gaps between appearances |
| **👤 Delegate Mgmt** | Fill unnamed persons from `abbrd.xlsx`, replace misidentified occurrences, add new delegates, manage bulk ID remapping rules |

The **📅 Day Order** tab lives at `pages/Day_Order.py` — accessible from the Streamlit sidebar-page list. It is kept separate because it scans all 430k+ rows and benefits from its own province/year/row-limit filter state.

## Data files

| Variable | File | Notes |
|----------|------|-------|
| Persons | `uq_delegates_updated_20260225.xlsx` | One row per unique delegate |
| Occurrences | `delegates_18ee_w_correcties_20260123_marked.xlsx` | One row per meeting appearance (~430k rows) |
| Authority / bio | `abbrd.xlsx` | Birth/death year, `hlife`, province, name |

Files are resolved from a candidate list (`PERSONS_CANDIDATES`, etc. in `utils.py`). The workspace root is checked first — drop or symlink a file there and the app picks it up. **Parquet sidecars are preferred automatically**: if `foo.parquet` exists next to `foo.xlsx`, the parquet is loaded (5–20× faster). Generate sidecars once with `python make_parquet.py`.

The `*_FILE` constants (`PERSONS_FILE`, `OCCURRENCES_FILE`, `ABBRD_FILE`) always reflect the path that will actually be read (parquet when available), so they are safe to display in startup logs.

## Corrections workflow

| Step | What happens |
|------|--------------|
| Select a delegate | Click a row in Overview or use the sidebar selectbox |
| Fix it | Use the **Bulk reassign** widget in any tab — enters values into `corrections.json` immediately |
| Mark done | Click **✅ Mark reviewed** in the sidebar — writes `reviewed.json` |
| Export | **⬇ Export corrected (parquet)** — full dataset with corrections applied (~1s). **⬇ Changed rows only (Excel)** — only the N corrected rows, instant |

Corrections are staged (JSON on disk) and never applied to the cached `df_merged`. The export step materialises them. This means:
- The heavy `build_merged` cache is never invalidated by corrections
- Re-opening the app after a crash loses nothing — `corrections.json` is written on every save
- The exported parquet can be dropped back as the new source file for the next session

## Persistence files

| File | Contents |
|------|----------|
| `corrections.json` | Row-level corrections `{row_index: new_delegate_id}` |
| `new_delegates.json` | Manually added delegates with auto-generated `republic_add_<##>` IDs |
| `remappings.json` | Bulk remap rules `[{from_id, to_id}]` applied at merge time |
| `province_order.json` | Ordered province list used by the Day Order page |
| `reviewed.json` | Set of `delegate_id` strings the user has marked as fully reviewed |
| `sandboxed.json` | Delegates marked "known wrong, cannot fix" — shown with 🔒 in the grid |

## Code layout

```
streamlit_worksheet/
├── sheet.py              # Entry point — page config, load, sidebar, tab dispatch
├── utils.py              # All data functions + persistence helpers
├── pages/
│   └── Day_Order.py      # Standalone page — province precedence violation scanner
├── tabs/
│   ├── __init__.py
│   ├── tab0_overview.py
│   ├── tab1_alive.py
│   ├── tab2_patterns.py
│   ├── tab3_names.py
│   ├── tab4_timeline.py
│   ├── tab5_dayorder.py  # render() called by pages/Day_Order.py; tab=None → no container
│   └── tab6_management.py
└── make_parquet.py       # One-off: generate .parquet sidecars from .xlsx files
```

Each `tabs/tab*.py` exports a single `render(tab, ...)` function. `sheet.py` calls them in sequence after building the `st.tabs(...)` tuple.

## Running

```bash
cd /Users/rikhoekstra/develop/streamlit_worksheet
uv run streamlit run sheet.py
```

To enable terminal timing output:

```bash
DELEGATE_QA_DEBUG=1 uv run streamlit run sheet.py
```

---

## Architecture decisions and gotchas

This section documents hard-won lessons so future developers (human or LLM) can avoid repeating them.

### 1. `@st.cache_data(persist="disk")` on all heavy functions

```python
@st.cache_data(persist="disk", hash_funcs=_HASH_FUNCS)
def build_merged(...) -> ...:
```

**Why:** Streamlit's in-memory cache is wiped every time the Python process restarts (i.e. on every code edit). For a function that takes 2–5 seconds, this makes development painful. `persist="disk"` writes the result to `~/.streamlit/cache/` so restarts load from disk in ~50ms.

**Gotcha:** `load_data()` takes no arguments, so its cache key would never change — even when source files are updated. Fix: pass `source_mtimes()` as an argument. `source_mtimes()` returns a tuple of file modification timestamps; when any source file changes, the tuple changes, the cache key changes, and all downstream cached functions (which take the DataFrames as arguments) also miss.

### 2. Custom `hash_funcs` to avoid O(n) DataFrame hashing

```python
def _hash_df(df):
    # sample ≤5 rows spread across the frame + shape/dtypes
    ...

_HASH_FUNCS = {pd.DataFrame: _hash_df, "pandas.core.frame.DataFrame": _hash_df}
```

**Why:** Streamlit's default hasher walks every cell of a 430k-row DataFrame to build a cache key — that takes hundreds of ms per call, per function. The custom hasher samples ≤5 rows, producing a stable fingerprint in <1ms. This is safe in practice: the only way the sample would collide is if exactly those rows were unchanged while everything else changed, which doesn't happen with real data mutations (appends, id reassignments, etc.).

### 3. Never pass large DataFrames where only scalars are needed

```python
# BAD — Streamlit hashes df_merged (430k rows) on every widget interaction
tab0_overview.render(tab0, df_merged=df_merged, ...)

# GOOD — pre-compute cheap scalars once per rerun
_n_occurrences     = len(df_merged)
_merged_columns    = list(df_merged.columns)
_has_bio           = "birth_year" in df_merged.columns and ...
tab0_overview.render(tab0, n_occurrences=_n_occurrences, merged_columns=_merged_columns, ...)
```

**Why:** Even with a custom hash function, passing a large DataFrame to a render function that only needs a count means the hash is computed on every rerun. Scalar arguments are hashed in nanoseconds.

### 4. Cap rows sent to the browser — this is where freezes actually come from

The Python timers showed ~75ms total render time, but the browser froze for seconds. The cause is Streamlit serializing the entire DataFrame to JSON and sending it over a WebSocket. The browser then renders it all into a DOM.

```python
# tab0 — filter + cap before AgGrid
_PAGE_SIZE = 200
summary_disp = summary_disp[name_filter].head(_PAGE_SIZE)

# tab3 — cap mismatch display
mismatch_display = mismatch_display.head(500)
```

**Rule of thumb:** Never send more than ~500 rows to `st.dataframe` or AgGrid. Add a Python-side search/filter so users can find rows without the browser having to hold all of them. The `st.dataframe` Python call returns almost instantly — the cost is serialization + DOM, which is invisible to Python timers.

### 5. `@st.fragment` does NOT work inside `with tab:` blocks

```python
# This FAILS with StreamlitFragmentWidgetsNotAllowedOutsideError
@st.fragment
def render(tab, ...):
    with tab:
        ...
```

Fragments cannot write into externally-created container objects (`tab`, `st.columns`, `st.expander`). The fragment must own its container from the inside. Since each tab render function wraps everything in `with tab:`, fragments are incompatible with this pattern. **Do not add `@st.fragment` to render functions.**

### 6. `name_mismatch` column must use nullable Boolean, not plain bool

```python
# BAD — after a left-merge, bool + NaN → object dtype → gets auto-categorised
persons["name_mismatch"] = np.char.find(p_arr, g_arr) < 0

# GOOD — nullable Boolean survives left-merge without becoming object
persons["name_mismatch"] = pd.array(
    np.char.find(p_arr, g_arr) < 0, dtype="boolean"
)
```

**Why:** `pd.merge(..., how="left")` introduces NaN for rows with no match. A plain `bool` column with NaN becomes `object` dtype. The categorization loop then tries to call `.sum()` on it and gets `TypeError: category type does not support sum operations`.

### 7. Categorization loop must exclude bool-valued and numeric-parseable columns

```python
for col in df.select_dtypes(include=["object", "string"]).columns:
    _sample = df[col].dropna()
    if pd.to_numeric(_sample, errors="coerce").notna().mean() > 0.1:
        continue  # skip: mostly numbers
    if set(_sample.unique()).issubset({True, False}):
        continue  # skip: bool disguised as object
    df[col] = df[col].astype("category")
```

### 8. `.fillna("")` on Categorical columns raises TypeError

```python
# BAD — fails with "Cannot setitem on a Categorical with a new category"
df[col].fillna("")

# GOOD
df[col].astype(str).replace({"nan": ""})
```

Use `.astype(str).replace({"nan": ""})` wherever a Categorical column might be displayed or filled.

### 9. `build_day_order` cache key must be primitives, not a filtered DataFrame

```python
# BAD — df_view is a new object each rerun even if data unchanged → cache miss always
def build_day_order(df_view: pd.DataFrame, ...):

# GOOD — filter inside the cached function; key is stable primitives
def build_day_order(df_merged, prov_col, sel_provinces, year_min, year_max, max_rows, province_rank):
    df_view = filter_occurrences(df_merged, prov_col, sel_provinces, year_min, year_max)
    ...
```

**Why:** A cached function's cache key is built from its arguments. If you pass a DataFrame that was created by filtering `df_merged` on every rerun, the DataFrame object is new each time even if the filter parameters didn't change, because pandas doesn't intern DataFrames. Passing the primitives (the filter params) instead gives a stable, cheap key.

### 10. Measuring browser render time

A tiny `st.components.v1.html()` block injected at the end of `sheet.py` measures first-paint and idle-frame latency using `requestAnimationFrame`. Results appear at the bottom of the page and in the browser console. Compare with Python terminal output to understand where time is actually spent:

- Python terminal shows serialization time (CPU-side)
- Browser timer shows transfer + DOM render time
- The gap between the two is the WebSocket transfer

### 11. `apply_corrections` must cast to match column dtype before writing parquet

```python
# Corrections dict stores int(new_id) when value is numeric.
# df_merged["delegate_id"] is str (object dtype).
# int + str in same column → pyarrow ArrowTypeError on to_parquet().

new_vals = [str(v) for v in new_vals]  # always coerce to str
out.loc[idxs, "delegate_id"] = new_vals
```

### 13. All timing output is gated behind `DELEGATE_QA_DEBUG`

```python
# sheet.py
DEBUG: bool = os.getenv("DELEGATE_QA_DEBUG", "0") == "1"
```

All `print(f" tab* ...")` calls and `_timed()` / `_render_timed()` wrappers are silent by default. Pass `debug=DEBUG` to each tab render function; inside tabs guard prints with `if debug: print(...)`. Enable with:

```bash
DELEGATE_QA_DEBUG=1 streamlit run sheet.py
```

### 12. `tab=None` pattern for render functions used both in tabs and standalone pages

```python
def render(tab=None, *, df_merged, ...):
    ctx = tab if tab is not None else contextlib.nullcontext()
    with ctx:
        ...
```

This allows the same render function to be called from `sheet.py` (passing a tab container) and from `pages/Day_Order.py` (passing nothing, rendering directly to the page).

---

## Performance profile (warm cache, ~430k rows)

```
load_data()                           58 ms   ← disk cache hit
enrich_persons_from_abbrd()            6 ms   ← disk cache hit
build_merged()                        46 ms   ← disk cache hit
build_sidebar_options()                6 ms
get_delegate_slice()                   4 ms
scalar pre-computation                13 ms
render tab0 (AgGrid, 200 rows)        61 ms
render tab1–4                        <15 ms each
render tab6                           11 ms
─────────────────────────────────────────────
Total Python                        ~224 ms
Browser first-paint                    5 ms
Browser idle-frame                    21 ms
```


## What it does

Delegates from different provinces attended the States General in a fixed precedence order (Gelderland → Holland → Zeeland → Utrecht → Friesland → Overijssel → Groningen). In the identification process errors were introduced: wrong persons were assigned to occurrence rows, names were garbled, or the same person appeared under multiple IDs. This app surfaces those problems tab by tab and lets you correct them without touching the source files.

## Tabs

| Tab | Check |
|-----|-------|
| **📋 Overview** | Summary table of all delegates: occurrence counts, active year range, number of patterns |
| **🧬 Alive Check** | Flags occurrences where the delegate's age at the event was implausible (< 16, > 90, or after death year) |
| **🔤 Pattern Anomalies** | Detects name patterns that diverge strongly from a delegate's modal pattern (Levenshtein distance via `rapidfuzz`) |
| **📛 Name Mismatch** | Checks whether the occurrence pattern contains the delegate's surname (`geslachtsnaam`) |
| **⏳ Timeline Gaps** | Highlights delegates with unusually long gaps between appearances |
| **📅 Day Order** | Detects meeting days where the province precedence order was violated |
| **👤 Delegate Mgmt** | Fill unnamed persons from `abbrd.xlsx`, replace misidentified occurrences, add new delegates, manage bulk ID remapping rules |

## Data files

| Variable | File | Notes |
|----------|------|-------|
| Persons | `uq_delegates_updated_20260225.xlsx` | One row per unique delegate |
| Occurrences | `delegates_18ee_w_correcties_20260123_marked.xlsx` | One row per meeting appearance |
| Authority / bio | `abbrd.xlsx` | Birth/death year, `hlife`, province, name — symlinked from surfdrive |

Files are resolved from a candidate list: the workspace root is checked first, so dropping or symlinking a file here is sufficient.

## Corrections workflow

- Every tab's **Bulk reassign** widget writes to `corrections.json` in the workspace root.
- Corrections persist across Streamlit restarts.
- The sidebar shows all pending corrections and offers an **Export corrections** button that writes a new Excel file with corrected `delegate_id` values.
- **Bulk ID remapping** (Tab 6 › Section 4) lets you define `old_id → new_id` rules that are applied at merge time; rules are stored in `remappings.json`.

## Persistence files

| File | Contents |
|------|----------|
| `corrections.json` | Row-level corrections `{row_index: new_delegate_id}` |
| `new_delegates.json` | Manually added delegates with auto-generated `republic_add_<##>` IDs |
| `remappings.json` | Bulk remap rules `[{from_id, to_id}]` |
| `province_order.json` | Ordered province list used by the Day Order tab |

## Code layout

```
streamlit_worksheet/
├── sheet.py              # Entry point — page config, load, sidebar, tab dispatch (~200 lines)
├── utils.py              # Pure data functions: load_data, build_merged, enrich_persons_from_abbrd,
│                         # persistence helpers, province order, next_republic_add_id
└── tabs/
    ├── __init__.py
    ├── tab0_overview.py
    ├── tab1_alive.py
    ├── tab2_patterns.py
    ├── tab3_names.py
    ├── tab4_timeline.py
    ├── tab5_dayorder.py
    └── tab6_management.py
```

Each `tabs/tab*.py` exports a single `render(tab, ...)` function that owns its `with tab:` block. `sheet.py` calls them in sequence after building the `st.tabs(...)` tuple.

## Running

```bash
cd /Users/rikhoekstra/develop/streamlit_worksheet
source .venv/bin/activate
streamlit run sheet.py
```

Or with `uv`:

```bash
uv run streamlit run sheet.py
```

## Dependencies

- `streamlit >= 1.54`
- `pandas >= 2.0`
- `plotly >= 5.0`
- `openpyxl >= 3.0`
- `rapidfuzz >= 3.0` (optional — graceful fallback to simple edit distance if absent)

Install via:

```bash
uv sync
```
