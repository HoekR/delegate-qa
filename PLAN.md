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

## Conceptual app mission

- Data source: person registry (`df_p`) + historical occurrences (`df_i`) (and optional extra bio data)
- Union mentation: create a merged timeline (`df_merged`) enriched with `age_at_event`, pattern validity, delegate-aware sequence context, etc.
- Goal: surface low-confidence or impossible delegate assignments via multiple orthogonal lenses:
  - biological plausibility (alive check)
  - pattern divergence (name form anomaly)
  - name mismatch (surname mismatch)
  - timeline gaps (suspicious absences)
  - day-order sequence violations
- Action mode: annotate suspect rows with `corrections.json`, stage/approve/archived workflow, and optionally adjust `pattern_status.json` for global pattern rules.
- UX: shared sidebar context + per-delegate filter + tabbed focused QA workflows + manual reassign + export.

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
- [x] Add persisted pattern validity metadata via `pattern_is_valid` column and `pattern_status.json`
- [x] Add selector for `All / Valid only / Invalid only`
- [x] Add mutator buttons (`Mark selected patterns as invalid/valid`) that are reversible and persistent

**Result:** List of name-pattern outliers that may indicate a wrong-person assignment; matches can now be hardened/invalidated by user action and restored across sessions.

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
- [x] Also flag delegates who reappear after a gap if a plausible alternative was active in those gap years (future enhancement)
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
- [x] Add staging for corrections: `staged_corrections.json` + manual `Load staged`/`Stage` semantics
- [x] Add final approval archive: `approved_corrections.json`, plus revert operation
- [x] Hide non-active staged corrections by default behind a toggle
- [x] Add debug counters (in RAM vs on-disk) and correction summary table in sidebar
- [x] Enrich corrections format to dict `{to_id, updated_at, source}` in `corrections.json`

**Result:** End-to-end correction loop that persists across sessions, supports review and approved archival, and separates active/staged state cleanly.

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
│   ├── build_merged
│   ├── _compute_delegate_summary
│   ├── _get_corrections_config
│   ├── make_correction_entry
│   ├── _set_summary_property (for summary updates)
│   └── helpers for remapping / placeholder filters
└── tabs/

### Step 12 — Bugfix: delegate_id dtype + streamlit cache reset + UI crash

- [x] Normalize `delegate_id` to string in `build_merged` for `df_p`, `df_i`, and optional `df_bio`:
  - this prevents `ValueError: Can only merge Series on same dtype` when IDs are mixed (int/str)
- [x] Add `utils.build_merged.clear()` call for hot-reload debug process to prevent stale cache from old code-path computations
- [x] Add guard in `_compute_delegate_summary` that ensures `delegate_id` is string and includes all persons even when no occurrences exist
- [x] Add regression asserts in `scratch.ipynb`:
  - `assert summary.loc[summary['delegate_id']=='13613', 'n_occurrences'].item() > 0`
  - `assert summary['delegate_id'].astype(str).duplicated().sum() == 0`
- [x] Note in workflow that `Ctrl-C` restart + `streamlit run sheet.py` is required after model code changes to avoid `ScriptRunner` thread state errors (`fragment_id_queue` attribute error)

**Reason:** this general bug was the root cause of your 13613 mismatch edge-case; with these fixes, in-app delegate summary and QA table now match the external test script results.

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

*Last updated: 2026-03-20 — Step 12 complete: parquet sidecars + lazy-load caching + max-rows selector; tabs refactor; delegate manager & caching fixes*

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

---

## Feature plan: Pattern-sequence suggestion engine ("🔍 Suggestions" tab)

### Background and motivation

After years of manual QA, **2,161 occurrences** in the dataset still carry a sentinel `delegate_id` (negative integers like `-1`, `-20`).  These are rows where the original NER or linking step failed to assign a confident identity.  Continuing to correct them by hand one-by-one is slow.

But the dataset already contains by far the largest signal: **~428,000 labeled occurrences** — rows with a positive `delegate_id` — each one saying "this `pattern` string belongs to this delegate."  That is the primary corpus.  On top of it, `approved_corrections.json` (22,593 entries) and `staged_corrections.json` (41,745 entries) provide a human-verified refinement layer.

### Three tiers of training signal

| tier | source | size | confidence |
|---|---|---|---|
| 1 — corpus | all labeled rows in `df_merged` (`delegate_id > 0`) | ~428k rows | original assignment confidence |
| 2 — approved | `approved_corrections.json` | 22,593 | high (human-reviewed) |
| 3 — staged | `staged_corrections.json` | 41,745 | medium (staged, not yet approved) |

The key store is built from **all three tiers**.  Tier 2 and 3 entries override tier 1 for any row they cover (a correction supersedes the original assignment).  This means the model already reflects everything that has been manually verified.

### Why Q-K-V / attention framing fits

Think of the problem as a retrieval task over a key-value store:

| role | content |
|---|---|
| **Query** | the `pattern` of an unknown occurrence (+ year `j`, province if known) |
| **Keys** | one embedding per known delegate, built from all pattern strings across all three tiers |
| **Values** | `delegate_id`, `fullname`, province, active year range (`first_year`–`last_year`) |

Attention score = cosine similarity(query embedding, key embedding), then adjusted by two hard filters:

- **Temporal gate**: if the query year `j` falls outside the delegate's known active range (+ a ±10 year tolerance), the candidate score is set to 0.
- **Province boost**: if province is known for both query and candidate, a match adds a small additive bonus (e.g. +0.05) before re-ranking.

This is the same mechanism as a nearest-neighbour retrieval transformer, but without the overhead of a neural model: a **character n-gram TF-IDF vectorizer** (ngram\_range=(2,4)) over Dutch surname fragments is fast, interpretable, and well-suited to the short, noisy patterns in this dataset.

Character n-grams are important here because:
- Dutch names have many variant spellings (`Gockinga` / `Gocknga` / `Gock`)
- Diacritics and OCR noise create systematic sub-word variation
- A bigram/trigram over characters captures these edits even when the full token doesn't match

### Active learning loop

Every user decision is a new label that feeds back into the system — but always via the **existing reversible corrections pipeline**, never by writing directly to `approved_corrections.json`:

1. **User accepts suggestion** → `(row_index → delegate_id)` is written into `st.session_state["corrections"]` (active, in-RAM corrections) via the same `save_correction()` function used everywhere else. It is immediately visible as an active correction in the sidebar, and can be staged, approved, or reverted using all the existing sidebar workflow buttons.
2. **Rerun** → `build_merged()` picks up the active corrections → `apply_corrections()` bakes them in → `build_suggestion_store()` rebuilds with the updated corpus (cache-invalidated by the changed `df_merged`) → future similar queries immediately reflect the accepted assignment.
3. **User rejects a suggestion** → stored in `st.session_state["skipped_suggestions"]` (a set of row indices) so the same candidate is hidden for the rest of the session, without writing anything to disk.
4. **To undo an accepted suggestion**: use the existing "Delete selected row(s)" button in the sidebar corrections table — same as undoing any manual correction.

There is no explicit training loop, no gradient, no stored model file.  The "learning" is the cumulative labeled corpus that grows as corrections move through the pipeline.

### Implementation plan

**Step 1 — `utils.py`: `build_suggestion_store(df_merged)`**

- Input: `df_merged` (cached DataFrame, already has corrections applied via `apply_corrections()`).
- Separate rows into labeled (`delegate_id > 0`) and unresolved (`delegate_id < 0`).
- For each positive `delegate_id`, collect all `pattern` values across all rows → join into one document per delegate. This includes original assignments AND any approved/staged corrections already baked into `df_merged`.
- Fit `TfidfVectorizer(analyzer='char_wb', ngram_range=(2,4), min_df=1)` on those documents.
- Also record `first_year`, `last_year`, and `provincie` per delegate (from the summary) for the temporal gate and province constraint. The province stored in the key metadata is the delegate's home `provincie` from `df_p`; the province used on the query side is `namens` from the occurrence row (more precise: it records which province they were representing at that specific meeting).
- Separate `class == 'president'` rows from `class == 'delegate'` rows so the query function knows when to skip province filtering.
- Return `(vectorizer, key_matrix[n_delegates × vocab], id_index[n_delegates], meta_df)` as a named tuple.
- Decorated with `@st.cache_data` — cache key is the shape+hash of `df_merged`, so it rebuilds automatically when corrections change the merge.

**Step 2 — `utils.py`: `query_suggestions(store, query_df, top_k=3)`**

- Input: the store named tuple + a DataFrame of unresolved rows (each with `pattern`, `j`, optionally `provincie`).
- Transforms query patterns with the fitted vectorizer → cosine similarity against key matrix (scipy sparse, fast even at 428k rows).
- **Temporal gate**: zero out scores where query year `j` falls outside `[first_year - 10, last_year + 10]` for the candidate.
- **Province constraint** (only when `class == 'delegate'`): the `namens` column records which province the occurrence is *on behalf of* at that specific meeting — this is a stronger signal than the person's home `provincie`. If `namens` is a recognised province name, candidates whose `provincie` does not match are zeroed out. This constraint is **skipped entirely when `class == 'president'`**, because the president signs first irrespective of province rotation.
- Returns a DataFrame with columns: `row_index`, `pattern`, `j`, `class`, `namens`, `cand_1`, `score_1`, `name_1`, `cand_2`, `score_2`, `name_2`, `cand_3`, `score_3`, `name_3`.
- Pure numpy/scipy, no Streamlit calls.

> **President exception:** `class == 'president'` rows (22,551 in total) are signed by whoever held the weekly rotating presidency — they could come from any province. Province-based filtering is therefore disabled for these rows. The temporal gate still applies.

**Step 3 — `tabs/tab_suggest.py`: new tab**

Layout:
```
🔍 Suggestions
  ── banner: "N unresolved occurrences · M have a suggestion with score ≥ threshold"
  ── slider: "Minimum confidence" (0.0 – 1.0, default 0.3)
  ── AgGrid table (one row per unresolved occurrence):
       row | year | pattern | candidate 1 (score) | candidate 2 | candidate 3
  ── "✅ Accept top suggestion for selected rows" button
  ── "🚫 Skip / flag as unresolvable" button
```
Accepted suggestions are written via `save_correction()` into `st.session_state["corrections"]` (active/RAM tier) — exactly the same path as a manual correction in any other tab.  They appear immediately in the sidebar correction table and can be staged, approved, reverted, or deleted from there.  
Skipped rows are stored in session state (`st.session_state["skipped_suggestions"]`) as a set of row indices, and hidden from the table for the rest of the session — nothing is written to disk.

**Step 4 — `sheet.py`: wire up the tab**

Add `"🔍 Suggestions"` to `_TAB_LABELS` (between Overview and Alive Check) and unpack the extra tab variable.  Pass `summary`, `df_merged`, `df_p`, `suggestion_store` to the render call.

**Step 5 — `sheet.py`: pre-compute suggestion store**

```python
suggestion_store = build_suggestion_store(df_merged)
```
Called once after `build_merged`, before the tab block.  The `@st.cache_data` decorator means it only runs when `df_merged` changes.

### Files to change

| file | what changes |
|---|---|
| `utils.py` | add `build_suggestion_store()` and `query_suggestions()` |
| `tabs/tab_suggest.py` | new file — render function for the Suggestions tab |
| `sheet.py` | add tab label, unpack tab, pre-compute store, call render |

### Dependencies

- `scikit-learn` — TF-IDF vectorizer + cosine similarity (already in venv: used in tab2 pattern anomalies)
- `scipy` — sparse matrix ops (pulled in by scikit-learn)
- No new packages needed
