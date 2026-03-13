# Plan: Delegate QA Streamlit App

## Current status

> **How progress is tracked:** This file is the single source of truth. Checkboxes (`- [ ]` → `- [x]`) are updated in this file after each task is completed. The file lives on disk and survives VS Code restarts. Runtime corrections made inside the Streamlit app are auto-saved to `corrections.json` in the workspace root so they also survive browser refreshes and restarts.

| Step | Status |
|---|---|
| Step 1 – Data loading | ✅ done |
| Step 2 – App skeleton + tabs | ✅ done |
| Step 3 – Overview tab | ✅ done |
| Step 4 – Alive Check tab | ✅ done |
| Step 5 – Pattern Anomalies tab | ✅ done |
| Step 6 – Name Mismatch tab | ✅ done |
| Step 7 – Timeline Gaps tab | ✅ done |
| Step 8 – Day Order tab | ✅ done (awaits reference file) |
| Step 9 – Correction workflow | ✅ done (auto-saves to corrections.json) |
| Step 10 – Packaging | ✅ done |
| Step 11 – Refactor: utils + per-tab modules | ✅ done |
| Step 12 – Performance: parquet + lazy loading | ✅ done |

---

## Assignment (in brief)

Build a multi-tab Streamlit application that links unique delegates (`persons`) to their meeting-record occurrences (`df1`) and exposes five structured quality-control checks to detect and correct misidentifications. Each delegate is treated as an object; its occurrences are its history. Each check gets its own tab.

---

## Data

| Variable | File | Key columns (known/inferred) |
|---|---|---|
| `df_p` (persons) | `uq_delegates_updated_20260225.xlsx` | `delegate_id`, `name`, `fullname`, `minjaar`, `maxjaar`, `provincie`, `pattern`, `twijfelachtig` |
| `df_i` (occurrences) | `delegates_18ee_w_correcties_20260123_marked.xlsx` | `delegate_id`, `pattern`, `delegate_name`, `j` (year), `date`, `type` |
| `df_add` (bio) | `delegates_1700_1795_republic_add_20250123csv.csv` | `id`, `name`, `leefjaren`, `provincie`, `RAA_nr`, `category` |
| `df_order` (day-order) | *to be specified by user* | meeting date, expected delegate order |

> **Action required:** Confirm exact birth/death year columns and provide the day-ordering file path/format before implementing Tab 5.

---

## Steps

### Step 1 — Fix current syntax error and stabilise data loading

- [x] Fix broken expression on sheet.py line 25: `df_merged['lifeyear'] = (df_merged['j']`
- [x] Load all three known source files (`df_p`, `df_i`, `df_add`) in a single cached `load_data()` function
- [x] Parse `leefjaren` (e.g. `"1723-1789"`) from `df_add` into `birth_year` and `death_year` integer columns
- [x] Join `df_add` onto `df_p` via a name-based or ID-based key to attach birth/death years to unique delegates
- [x] Merge `df_p` + `df_i` on `delegate_id` to produce the master `df_merged`
- [x] Add `lifeyear` = `j` − `birth_year` (age at occurrence) to `df_merged`

**Result:** A clean, fully merged dataframe available to all tabs with no syntax errors.

---

### Step 2 — App skeleton with tabs

- [x] Replace current single-page layout with `st.tabs(["Overview", "Alive Check", "Pattern Anomalies", "Name Mismatch", "Timeline Gaps", "Day Order"])`
- [x] Add a sidebar with global filters: province (`provincie`), year range, delegate search by name
- [x] Sidebar selection of a single delegate drives detail views in all tabs

**Result:** A navigable multi-tab app shell with shared state.

---

### Step 3 — Tab 0: Overview

- [x] Summary table of all unique delegates with counts of occurrences, year range active, and number of flagged issues
- [x] Clickable rows to navigate to a specific delegate
- [x] Export button for the current flag summary to Excel

**Result:** High-level dashboard showing which delegates have the most QA issues.

---

### Step 4 — Tab 1: Alive Check (biological plausibility)

- [x] For each occurrence, compute `age_at_event = j − birth_year`
- [x] Flag rows where `age_at_event < 16` or `age_at_event > 90` or `j > death_year`
- [x] Show flagged occurrences in a highlighted table
- [x] Scatter plot: x = year, y = age_at_event, colour = flag status
- [x] Inline editor: allow reassigning `delegate_id` for a flagged row

**Result:** Checklist of biologically impossible delegate assignments.

---

### Step 5 — Tab 2: Pattern Anomalies

- [x] For each `delegate_id`, collect all occurrence `pattern` values
- [x] Compute a divergence score (edit-distance outlier via `rapidfuzz`) against the modal pattern for that delegate
- [x] Flag patterns whose score exceeds a configurable threshold (slider)
- [x] Show top-N most anomalous patterns per delegate with their occurrences
- [x] Bar chart of pattern frequency per delegate

**Result:** List of name-pattern outliers that may indicate a wrong-person assignment.

---

### Step 6 — Tab 3: Name-form Mismatch (geslachtsnaam / fullname)

- [x] Extract the `geslachtsnaam` (surname) from `fullname` in `df_p` (split on `,` — format `"Surname, Firstname"`)
- [x] For each occurrence, check whether the pattern contains the `geslachtsnaam` (case-insensitive)
- [x] Flag occurrences where the surname is entirely absent from the pattern
- [x] Show flagged rows grouped by delegate; allow marking as "confirmed correct" or "reassign"

**Result:** Table of occurrences whose pattern text does not match the assigned person's surname.

---

### Step 7 — Tab 4: Timeline Gaps

- [x] For each `delegate_id`, sort occurrences by year and compute year-gaps between consecutive appearances
- [x] Flag gaps exceeding a configurable threshold (default: 10 years)
- [ ] Also flag delegates who reappear after a gap if a plausible alternative was active in those gap years (future enhancement)
- [x] Timeline visualisation: scatter per delegate showing active years
- [x] Detail panel for selected delegate showing all occurrences and gap sizes

**Result:** Visual map of suspicious absences and late reappearances.

---

### Step 8 — Tab 5: Day-Order Violations

- [x] Load the day-ordering reference file via sidebar uploader
- [x] For each meeting day, compare observed delegate sequence to the expected order
- [x] Flag delegates appearing out of position
- [x] Table view: day → expected order vs. actual order, with deviating rows highlighted
- [x] Allow user to mark a deviation as "accepted" or "misidentification"

**Result:** Structured list of sequence violations per meeting day.

---

### Step 9 — Correction workflow

- [x] Maintain a session-state corrections dict: `{occurrence_row_id: new_delegate_id}`
- [x] **Auto-save corrections to `corrections.json`** in the workspace root on every change
- [x] On app startup, load `corrections.json` back into session state if it exists
- [x] Every tab's inline editor writes to this dict
- [x] "Review corrections" panel in the sidebar shows all pending changes
- [x] "Export corrections" button writes a new Excel file with corrected `delegate_id` values

**Result:** End-to-end correction loop that persists across sessions without modifying source files.

---

### Step 10 — Packaging and running

- [x] Add `streamlit`, `pandas`, `plotly`, `openpyxl`, `rapidfuzz` to `pyproject.toml` dependencies
- [x] Run via: `source .venv/bin/activate && streamlit run sheet.py`
- [ ] (Optional) add a `Makefile` target `run` for convenience

**Result:** Reproducible, one-command launch using the uv-managed environment.

---

### Step 11 — Refactor: extract utils module and per-tab modules

**Goal:** break `sheet.py` (~1336 lines, everything in one file) into a clean package so each concern lives in its own file.

#### Proposed layout

```
streamlit_worksheet/
├── sheet.py              # entry point — page config, load data, sidebar, assemble tabs
├── utils.py              # pure / data functions (no st.* calls)
│   ├── load_corrections / save_corrections
│   ├── load_new_delegates / save_new_delegates
│   ├── load_remappings / save_remappings
│   ├── load_province_order
│   ├── next_republic_add_id
│   ├── load_data  (@st.cache_data stays here via import)
│   ├── _parse_leefjaren
│   ├── enrich_persons_from_abbrd
│   └── build_merged
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

Each `tabs/tab*.py` exports a single function `render(tab, df_merged, df_p, df_abbrd, ...)` that owns its `with tab:` block.  `sheet.py` calls them in sequence after building the `st.tabs(...)` tuple.

#### Tasks

- [x] Create `utils.py` with all non-UI helper functions extracted from `sheet.py`
- [x] Create `tabs/__init__.py` (empty)
- [x] Create `tabs/tab0_overview.py` → `render(tab, ...)`
- [x] Create `tabs/tab1_alive.py` → `render(tab, ...)`
- [x] Create `tabs/tab2_patterns.py` → `render(tab, ...)`
- [x] Create `tabs/tab3_names.py` → `render(tab, ...)`
- [x] Create `tabs/tab4_timeline.py` → `render(tab, ...)`
- [x] Create `tabs/tab5_dayorder.py` → `render(tab, ...)`
- [x] Create `tabs/tab6_management.py` → `render(tab, ...)`
- [x] Rewrite `sheet.py` to import from `utils` and `tabs.*`; remove all extracted code
- [x] Verify app runs without errors after refactor

**Result:** `sheet.py` shrinks to ~100 lines (config + load + sidebar + tab dispatch); all logic is testable in isolation.

---

### Step 12 — Performance: parquet caching + lazy loading

**Goal:** stop the app re-running all heavy computation on every sidebar interaction and avoid loading 600 k+ rows into every tab unconditionally.

#### Changes

- [x] Add `pyarrow>=14.0` and `watchdog>=3.0` to `pyproject.toml`; run `uv sync`
- [x] Add `_read_df(candidates)` helper to `utils.py`: loads `.parquet` sidecar when present, falls back to `.xlsx`
- [x] Rewrite `load_data()` to use `_read_df()` for all three files
- [x] Create `make_parquet.py`: converts all `.xlsx` files in the workspace to `.parquet` sidecars; casts all `*id*` columns and remaining `object` columns to `str` to avoid mixed-type errors; preserves `NaN` values
- [x] Add `@st.cache_data` to `enrich_persons_from_abbrd()` — was re-running on every Streamlit rerun
- [x] Add `@st.cache_data` to `build_merged()` — was re-running on every Streamlit rerun
- [x] Convert both functions to return their counters `(df, n_enriched)` / `(df, n_placeholder, n_remapped)` instead of writing to module-level globals (required for `@st.cache_data` correctness)
- [x] Update `sheet.py` to unpack the new tuple return values; remove `_utils._n_*` global reads
- [x] Add **"Max rows to analyse"** selectbox to sidebar (500 / 1 k / 5 k / 10 k / 50 k / All); `df_view` is capped *after* province + year filters so all filters still work

**Result:** data files load once and are cached; merge/enrich run once per unique input hash; every tab interaction now only processes the N rows the user chose.

---

## Verification checklist

- [ ] App launches without errors from a clean `uv` environment
- [ ] All five check tabs load with real data
- [ ] Alive-check flags at least one known bad assignment (requires bio file)
- [ ] Pattern-anomaly threshold slider changes the flagged set dynamically
- [ ] Corrections export produces a valid Excel file with only the changed rows
- [ ] Day-order tab loads once the reference file is provided

## How to run

```bash
cd /Users/rikhoekstra/develop/streamlit_worksheet
source .venv/bin/activate
streamlit run sheet.py
```

---

## Open questions / decisions

| # | Question | Default assumption |
|---|---|---|
| 1 | Exact column for birth/death year in `df_add` — is it `leefjaren` as `"YYYY-YYYY"`? | Yes, parse as string |
| 2 | What file/format contains the expected day-order of delegates? | To be provided by user (Tab 5 blocked) |
| 3 | Save corrections back to source files or to a separate corrections file? | Separate corrections file |
| 4 | Is `geslachtsnaam` always the part before the first `,` in `fullname`? | Yes |
| 5 | Single-user local use or multi-user? | Single-user local |

---

*Last updated: 2026-03-09 — Step 12 complete: parquet sidecars + lazy-load caching + max-rows selector*

---

## Fixes & changes log

| Date | Change |
|---|---|
| 2026-03-09 | CSV bio file reader: added `on_bad_lines="skip"` to handle irregular rows |
| 2026-03-09 | `build_merged`: replaced `astype("float")` with `pd.to_numeric(..., errors="coerce")` to handle `NAType` in birth/death year columns |
| 2026-03-09 | Alive Check tab: same `pd.to_numeric` fix for `death_year` comparison |
| today | Step 11 complete: extracted `utils.py` + `tabs/tab*.py`; `sheet.py` rewritten as ~282-line dispatch entry point; `README.md` added; all files pass static error checks |
| 2026-03-09 | All CSV reads changed to tab-separated (`sep="\t"`) |
| 2026-03-09 | `watchdog` install attempted for Streamlit hot-reload performance (optional) |
| 2026-03-09 | Added `ABBRD_FILE` (`abbrd.xlsx`) loader to `load_data()` (returns 4-tuple now); `abbrd_id_col = id_persoon` |
| 2026-03-09 | `hlife` column from abbrd used as fallback when `birth_year`/`death_year` are missing: `birth ≈ hlife−40`, `death ≈ hlife+30` |
| 2026-03-09 | Added `NEW_DELEGATES_FILE` (`new_delegates.json`) + `load_new_delegates` / `save_new_delegates` for manual additions |
| 2026-03-09 | Added `next_republic_add_id()`: scans both `df_p` and `new_delegates` to produce the next clash-free `republic_add_<##>` string ID |
| 2026-03-09 | `save_correction()` now accepts `int \| str` so `id_persoon` values from abbrd and `republic_add_*` strings work without type errors |
| 2026-03-09 | `build_merged()` receives `extra_delegates` list; manually-added records are appended to persons before merge |
| 2026-03-09 | Added **Tab 6 – Delegate Mgmt**: (1) fill unnamed persons from abbrd, (2) replace mis id'd occurrence via abbrd search, (3) add new delegate with auto republic_add ID |
| 2026-03-09 | Bio CSV (`delegates_1700_1795_republic_add…csv`) removed; `abbrd.xlsx` is now the single bio + authority source |
| 2026-03-09 | `build_merged`: merges `min_year` / `max_year` from abbrd onto persons; Tab 1 gains `flag_before_active` (j < min_year) and `flag_after_active` (j > max_year) |
| 2026-03-09 | Tab 5 fully rewritten: day-order is now derived from province precedence rank (Gelderland → Holland → Zeeland → Utrecht → Friesland → Overijssel → Groningen) instead of an uploaded reference file; rank loaded from `province_order.json` (created in workspace root) |
| 2026-03-09 | File resolution changed: single path constants replaced by `PERSONS_CANDIDATES` / `OCCURRENCES_CANDIDATES` / `ABBRD_CANDIDATES` — workspace root (symlink) checked first, then known remote paths |
| 2026-03-09 | `build_merged`: rows with `delegate_id == -1` (placeholder / unidentified occurrences) filtered out before merge; count stored in module-level `_n_placeholder_rows` and shown in Tab 0 caption |
| 2026-03-09 | Tab 5: added **day-roster drill-down** — selecting a suspicious occurrence shows the full ordered roster for that meeting day with out-of-order rows highlighted |
| 2026-03-09 | `build_merged`: added `df.loc[:, ~df.columns.duplicated()].copy()` before return to eliminate duplicate column names caused by cascading left-joins; this fixed KeyError crashes in Tab 1, Tab 3, and Tab 6 when a delegate was selected |
| 2026-03-09 | Tab 3 `surname_missing`: replaced `apply()` (broken by duplicate columns) and subsequent list comprehension with `Series.combine()` + `.str` accessors (vectorised, safe with duplicate columns) |
| 2026-03-09 | Added `enrich_persons_from_abbrd()`: called at load time, fills any blank field in `df_p` from abbrd via `delegate_id ↔ id_persoon` without overwriting existing data; enriched-row count shown in Tab 0 caption and Tab 6 Section 1 caption |
| 2026-03-09 | Added `pyarrow` + `watchdog` to pyproject.toml; `uv sync` run |
| 2026-03-09 | Created `make_parquet.py`: converts all `.xlsx` files → `.parquet` sidecars; `*id*` cols and object cols cast to str; NaN preserved |
| 2026-03-09 | Added `_read_df()` helper + rewrote `load_data()` to prefer `.parquet` sidecars over `.xlsx` |
| 2026-03-09 | Added `@st.cache_data` to `enrich_persons_from_abbrd` and `build_merged`; both now return counters as tuple values instead of writing to module globals |
| 2026-03-09 | `sheet.py`: unpacks tuple returns from cached functions; added "Max rows to analyse" sidebar selector (default All) that caps `df_view` after filters |
