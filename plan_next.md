# Plan: Next Steps — RAG, 17th-c. Cold-Start, Cleanup

> This document covers work discussed but not yet implemented after the initial suggestion engine was built.
> See PLAN.md for the completed implementation history.

---

## How to resume this work

### Run the app

```bash
cd /Users/rikhoekstra/develop/streamlit_worksheet
source .venv/bin/activate
streamlit run sheet.py
```

### Current codebase state (as of 2026-04-01)

All code below is **already implemented and working**. Nothing in this file touches it — these are
purely additive tasks.

| File | What it does |
|---|---|
| `utils.py` | All data functions: `load_data`, `build_merged`, `build_suggestion_store`, `query_suggestions`, `load/save_flagged_patterns` |
| `sheet.py` | Entry point: loads data, builds sidebar, dispatches 8 tabs |
| `tabs/tab_suggest.py` | "🔍 Suggestions" tab: shows unresolved rows, accept/skip/flag buttons, writes corrections via `save_correction()` |
| `tabs/tab0_overview.py` … `tab8_delegates.py` | Other QA tabs (alive check, patterns, names, timeline, day-order, delegate management, settings) |
| `corrections.json` | Active in-RAM corrections (auto-saved on every change) |
| `staged_corrections.json` | Staged corrections awaiting approval |
| `approved_corrections.json` | Approved/archived corrections (22,593 entries) |
| `flagged_patterns.json` | Patterns flagged as unresolvable (persistent, suppressed in suggestion tab) |

### Key functions to know before editing

```python
# utils.py
build_suggestion_store(df_merged) -> dict
#   keys: vec_char, vec_word, key_char, key_word, id_index, meta
#   decorated @st.cache_data — rebuilds automatically when df_merged changes

query_suggestions(store, query_df, top_k=3, year_tolerance=10, min_score=0.0) -> DataFrame
#   query_df needs columns: pattern, j (year), class, namens
#   returns: orig_idx, pattern, j, class, namens, cand_1..N, score_1..N

load_data() -> (df_p, df_i, df_abbrd)
#   prefers .parquet sidecars; falls back to .xlsx
#   df_p = persons registry, df_i = occurrences, df_abbrd = authority file
```

### Data files (workspace root or symlinked)

| File | Status | Needed for |
|---|---|---|
| `uq_delegates_updated_*.xlsx` / `.parquet` | ✅ present | all tasks |
| `delegates_18ee_w_correcties_*.xlsx` / `.parquet` | ✅ present | all tasks |
| `abbrd.xlsx` / `.parquet` | ✅ present | all tasks |
| NER output file (format TBD) | ❌ not yet | Task 4 |
| 17th-c. occurrences file | ❌ not yet | Task 2 |
| Officials DB / Repertorium van ambtsdragers | ❌ not yet | Tasks 2 & 5 |

### Suggested execution order

Tasks 3 (cleanup) and 4 (NER matching) can be done immediately with existing data.
Tasks 1 (RAG), 2 (cold-start), and 5 (officials DB) require external data or infrastructure first.

```
Task 3  →  Task 4  →  Task 1  →  (when data available) Task 5  →  Task 2
```

---

## Pending Task 1 — RAG explanation layer

> **Prerequisites:** Ollama installed locally (`brew install ollama`) OR an OpenAI-compatible API key. No new data files needed.

**Goal:** augment each suggestion with a human-readable LLM explanation.

### Architecture

- `utils.py`: add `rag_explain(store, query_df, top_k_results, llm_client)`
  - builds a prompt per unresolved row: candidate metadata + surrounding roster context
  - calls LLM (Ollama local or remote API)
  - returns structured JSON `{candidate_id, confidence, reasoning}`
- `tabs/tab_suggest.py`: add expandable "Reasoning" panel per row showing LLM output
- Batch mode: run on all ~2,161 rows overnight, cache results in `rag_cache.json`

### Files to change

| File | Change |
|---|---|
| `utils.py` | add `rag_explain()` |
| `tabs/tab_suggest.py` | expandable reasoning panel per suggestion row |
| `sheet.py` | load `rag_cache.json` at startup; pass to render |

---

## Pending Task 2 — 17th-century cold-start mode

> **Prerequisites:** 17th-c. occurrences file (format TBD) + officials DB (Task 5). **Blocked until both are available.**

**Background:** A 17th-century dataset with no prior pattern identification — no labeled corpus to seed the key store from. The Q-K-V approach must be bootstrapped purely from the person registry (`df_p` fullname + known variants).

### Key architectural insight

For 17th-c. dynasty disambiguation (e.g. two `Van Aerssen` delegates from the same family across different generations), TF-IDF similarity on name strings alone is insufficient — names are identical. The attention score must be augmented with **roster neighbourhood context**:

$$\text{score}(Q_i, K_j) = \text{cosine}(\text{TF-IDF}(Q_i),\ \text{TF-IDF}(K_j))\ \times\ \mathbf{1}[\text{temporal gate}]\ \times\ w_{\text{prov}}(Q_i, K_j)$$

Where $w_{\text{prov}}$ is **derived from the province of known neighbours** on the same meeting day (not from a `namens` column directly), creating a **cascading resolution effect**:

1. Resolve easy / unambiguous slots first (unique name strings, strong temporal gate)
2. Those known identities become context for adjacent unknown slots on the same meeting day
3. Work outward — each resolved slot narrows the province possibilities for the remaining unknowns

### Implementation plan

- `utils.py`: add `build_suggestion_store_cold(df_p, officials_db=None)`
  - seeds from registry fullname + variants instead of occurrence corpus
  - same dual vectorizer (char_wb + word)
  - if `officials_db` provided, **expands each delegate's key document with hypothetical patterns** (see below)

#### Hypothetical pattern generation from the officials database

When no labeled occurrence corpus exists, the key document for each delegate can be bootstrapped
by generating all plausible surface forms a 17th-c. secretary might have used, derived entirely
from structured biographical data.

**Source fields needed** (from Repertorium van ambtsdragers / officials DB):

| Field | Used for |
|---|---|
| `geslachtsnaam` | bare surname form |
| `voornaam` | given name + initial forms |
| `vader_voornaam` | patronymic (`Fransz.` / `Cornelisz.`) |
| `minjaar` / `maxjaar` | temporal gate (already used) |
| `titel` / `ambt` | office-prefix forms (`Advocaet`, `Raedtpensionaris`, etc.) |

**Generated forms per delegate** (concatenated into one key document):

```python
def _hypothetical_patterns(row) -> str:
    parts = []
    sur  = row.get("geslachtsnaam", "")
    giv  = row.get("voornaam", "")
    pat  = _patronymic(row.get("vader_voornaam", ""))   # e.g. "Cornelisz."
    tit  = row.get("titel", "")

    if sur:
        parts.append(sur)                               # Van Aerssen
    if giv and sur:
        parts.append(f"{giv[0]}. {sur}")                # F. van Aerssen
        parts.append(f"{giv} {sur}")                    # Frans van Aerssen
    if pat and sur:
        parts.append(f"{sur} {pat}")                    # Van Aerssen Cornelisz.
    if giv and pat and sur:
        parts.append(f"{giv} {pat} {sur}")              # Frans Cornelisz. van Aerssen
    if tit and sur:
        parts.append(f"{tit} {sur}")                    # Advocaet van Aerssen
    return " ".join(parts)
```

**Why this solves dynasty disambiguation:**
- Two `Van Aerssen` delegates have the same surname but different given names and cross-patronymics
  (father's given name = son's patronymic stem and vice versa).
- The hypothetical documents therefore contain different character n-grams even for identical surnames.
- Combined with the temporal gate (active year ranges), ambiguous overlap periods resolve correctly.

**Family graph extension (optional):**
- If the officials DB has father/son/brother relations, siblings sharing a patronymic can be
  distinguished by their own given names — the same mechanism, applied transitively.
- Priority order: direct family chains first → extended kin → same-province contemporaries.

- Augment `query_suggestions` with:
  - `context_window` parameter: list of already-resolved (`pattern`, `delegate_id`) pairs from the same meeting day
  - `w_prov(Q_i, K_j)`: province weight derived from known neighbours, not from `namens`
- Cascading resolution order: prioritise meeting days with the most already-known slots
- Two-column occurrence structure:
  - `raw_text` (original OCR string, never mutated)
  - `pattern` (accumulates as resolution proceeds)
  - `delegate_id` (starts as sentinel, updated as each slot resolves)

### Files to change

| File | Change |
|---|---|
| `utils.py` | `build_suggestion_store_cold()`, `context_window` param in `query_suggestions` |
| `tabs/tab_suggest.py` | cold-start mode toggle; roster-context panel |
| `sheet.py` | dataset selector / mode switch |

---

## Pending Task 3 — Minor cleanup

> **Prerequisites:** none — can be done immediately.

- [ ] Delete test scripts from workspace root: `_smoke_dual.py`, `_check_x0000.py`, `_check_x0000b.py`
- [ ] Update PLAN.md completed-items checkboxes: dual vectorizer, flagged patterns, Excel escape fix
- [ ] (Optional) Add `Makefile` target `run`: `source .venv/bin/activate && streamlit run sheet.py`

---

## Pending Task 4 — NER span matching

> **Prerequisites:** NER output file (format TBD — needs at minimum: span text + document year). The suggestion engine itself needs no changes.

**Goal:** run cleaned-up NER output (text spans from source documents) through the existing
suggestion engine to get probabilistic delegate identifications.

### Why it fits without pipeline changes

`query_suggestions` only requires a DataFrame with `pattern` (the span text) and optionally `j`
(document year), `class`, `namens`. NER output maps directly:

| NER field | Maps to |
|---|---|
| span text | `pattern` |
| document date / year | `j` |
| surrounding context (if known) | `namens` (province) |

The full 428k-occurrence + corrections key store is reused unchanged.

### What's different from sentinel rows

Sentinel rows (`delegate_id == -1/-20`) are already in `df_merged` and have a row index to write
corrections back to. NER spans from a separate source are **outside** `df_merged` — results are
output-only unless the spans are first imported as new rows in `df_i`.

Three scenarios:

| Scenario | Action |
|---|---|
| Extra occurrences of known delegates in the *same* source | Add to `df_i` with sentinel id, flow through existing suggestion tab |
| Spans from a **separate 17th-c. source** | Store results in `ner_matches.parquet`; review in new tab |
| **Alternative transcriptions** of already-linked rows | Append to per-delegate key document in `build_suggestion_store` |

### Score interpretation

Cosine similarity is relative, not calibrated. Empirical tiers in this dataset:

| Tier | Score | Meaning |
|---|---|---|
| High | ≥ 0.30 | Strong candidate, likely correct |
| Medium | 0.15 – 0.29 | Plausible, review recommended |
| Low | < 0.15 | Noise / insufficient signal |

### Files to change

| File | Change |
|---|---|
| `utils.py` | `match_ner_to_delegates(ner_df, store, min_score=0.15)` — wrapper around `query_suggestions` adding `confidence_tier` column |
| `sheet.py` | Load NER file at startup (optional, file-uploader fallback) |
| `tabs/tab_suggest.py` | NER mode toggle or separate export button |

---

## Pending Task 5 — Officials database integration for hypothetical patterns

> **Prerequisites:** Repertorium van ambtsdragers export (or equivalent) with at minimum: `geslachtsnaam`, `voornaam`, `vader_voornaam`, `minjaar`, `maxjaar`. Required before Task 2 can start.

**Goal:** connect the Repertorium van ambtsdragers (or equivalent officials DB) to
`build_suggestion_store_cold` so that 17th-c. cold-start keys are richer than bare fullname strings.

### Required fields from officials DB

| Field | Role |
|---|---|
| `geslachtsnaam` | bare surname (primary match surface) |
| `voornaam` | given name → initial form, full form |
| `vader_voornaam` | patronymic stem → `Fransz.` / `Cornelisz.` |
| `minjaar` / `maxjaar` | temporal gate (reuses existing mechanism) |
| `titel` / `ambt` | office-prefix forms (`Advocaet`, `Raedtpensionaris`) |
| family relations | optional — father/son/sibling chains for dynasty disambiguation |

### Confidence ordering for disambiguation

1. **Temporal gate** alone resolves most cases (non-overlapping careers)
2. **Patronymic distinction** resolves dynasty cases in overlap periods
3. **Province context** from roster neighbours resolves remaining ambiguity
4. **Office title** as tiebreaker when province is unknown

### Files to change

| File | Change |
|---|---|
| `utils.py` | `_hypothetical_patterns(row)` generator; `build_suggestion_store_cold(df_p, officials_db)` |
| `sheet.py` | officials DB file path constant + loader |

---

*Created: 2026-04-01 — covers RAG, 17th-c. cold-start, NER matching, hypothetical patterns, and cleanup.*