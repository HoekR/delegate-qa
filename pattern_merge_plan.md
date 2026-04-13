# Pattern Merge / Split Detection — Design Plan

## 1. Problem Statement

The name patterns in the occurrences file are the product of an automated
identification process applied to HTR (handwritten text recognition) output.
Because tokenization of handwritten 18th-century Dutch is imperfect, two
systematic errors occur:

**Error type A — Concatenation (two names fused into one)**
A single occurrence row carries a pattern that is really the names of two
adjacent delegates run together, with or without a separating space.
Example: `"AppelterreBoekhorst"` instead of `"Appelterre"` (delegate 1) on
one row and `"Boekhorst"` (delegate 2) on the next.

**Error type B — Fragmentation (one name split across two rows)**
A single name is split at a tokenization boundary, producing two occurrence
rows where there should be one.  Both rows are assigned to the same delegate.
The delegate ends up with many distinct patterns, inflating `n_patterns` and
triggering spurious anomaly flags in tab 2.
Example: `"Appel"` + `"terre"` instead of `"Appelterre"`.

Both errors are now tractable to hunt because the corpus is almost complete
and the correct modal patterns per delegate are stable.

---

## 2. Key Assumptions

- **"van", "van den", "van der", "van de" are noise** — strip them before all
  comparisons.  They're too frequent and carry no discriminating signal.
- **Modal as anchor** — the most-frequent pattern per delegate is a reasonable
  proxy for the "true" name pattern.  Where `geslachtsnaam` from `abbrd` is
  available it should be preferred as a second anchor.
- **Session ordering is the primary signal** — the data preserves the order in
  which delegates appeared on a given meeting day (grouped by the `date` /`j`
  column, sorted by actual row position within that group).  The States General
  province precedence means that on any given day, the sequence of delegates is
  highly constrained.  For a suspected concatenation at position N, the two
  best candidates for the two halves are the delegates at positions N-1 and N+1
  on the same day.

---

## 3. Detection Algorithm

### Step 0 — Preprocessing (shared)

```
norm(s) = strip "van *" prefixes, lowercase, strip whitespace
```

Build a lookup once:
```
modal[delegate_id] = most-frequent pattern in occurrences (normalised)
geslacht[delegate_id] = geslachtsnaam from abbrd (normalised), if available
anchor[delegate_id] = geslacht if present, else modal
```

### Step 1 — Error type A: Concatenation detection

**Pre-filter** (cheap, runs on all ~430k rows):
- Pattern length > 1.4 × len(anchor[delegate_id])  
  OR pattern contains a space and anchor does not

This typically reduces candidates to a few hundred rows.

**Offset lookup** (per candidate):
- Find the row's position within its session day (group by `date`/`j` day,
  sort by row order within the group, assign `actual_pos`).
- Collect the anchors of delegates at `actual_pos ± 1` and `actual_pos ± 2`.

**Fuzzy split test** (per candidate, per split point):
- Try all split points of the pattern (by space if space present; else every
  character position between 30% and 70% of pattern length).
- For each (left_half, right_half):
  - Compute `rapidfuzz.distance.Levenshtein.normalized_distance(left_half, anchor_k)` for each neighbor k.
  - Accept if both halves match some neighbor with distance ≤ 0.20 (tunable threshold `T_concat`).
- Record the best matching pair of neighbors as `candidate_left`, `candidate_right`.

**Output row** (one per confirmed candidate):
```
row_index | pattern | delegate_id | anchor | split_left | split_right |
candidate_left_id | candidate_right_id | score | n_occurrences_with_this_pattern
```

### Step 2 — Error type B: Fragmentation detection

**Pre-filter**:
- Only delegates with `n_patterns ≥ 3` (noise below this threshold).
- Only patterns where `len(pattern) < 0.6 × len(anchor)`.

**Pair test** (per delegate, per pair of rare patterns):
- For each pair (P1, P2) of sub-modal patterns for the same delegate:
  - Test concat1 = P1 + P2 and concat2 = P2 + P1 (and with a space).
  - If `normalized_distance(concat_i, anchor) ≤ T_frag` (default 0.15), flag
    as a fragment pair.
- Record which of P1, P2 is more frequent (keep that one; the other is the
  ghost fragment).

**Output row**:
```
delegate_id | anchor | fragment_a | freq_a | fragment_b | freq_b | concat_score
```

---

## 4. Thresholds and Tuning

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `T_concat` | 0.20 | Max normalised edit distance for each half of a concat |
| `T_frag` | 0.15 | Max normalised edit distance for reconstructed concat vs anchor |
| `min_len_ratio` | 1.4 | Minimum ratio pattern_len / anchor_len to be a concat candidate |
| `neighbor_window` | 2 | How many adjacent rows (±) to look up on the same day |
| `min_patterns_for_frag` | 3 | Ignore delegates with fewer distinct patterns |

All thresholds are exposed as parameters on the detection function so they
can be adjusted from the UI without changing code.

---

## 5. Interaction Design — New Tab

A new **"🔗 Merge Errors"** tab (or a collapsible section at the bottom of
tab 2) surfaces both candidate lists.

### 5a. Concat candidates table

Columns: pattern | delegate | anchor | best split | left candidate | right candidate | score | # occurrences

Actions (per selected row or in bulk):
- **Dismiss** — mark this pattern as a known false positive; it will not
  appear again.
- **Reassign left** — reassign all occurrences of this pattern to
  `candidate_left_id`.  Uses the existing corrections machinery.
- **Reassign right** — reassign all occurrences to `candidate_right_id`.
- **Split into two** — not directly implementable (would require inserting a
  new row), so for now just flags the row for manual inspection in the export.

### 5b. Fragment candidates table

Columns: delegate | anchor | fragment A | count A | fragment B | count B | score

Actions:
- **Dismiss** — false positive.
- **Mark as equivalent** — adds the fragment pair to a `pattern_synonyms.json`
  file. This suppresses them in the `n_patterns` count and in the tab 2
  anomaly list.

### 5c. Trigger and cost

- The scan is **not** run on every rerun.  A **"▶ Run scan"** button triggers
  it once; the result is cached in `st.session_state["merge_candidates"]`.
- A **province / year filter** above the button lets the user restrict the
  scan for faster iteration during tuning.
- A spinner with elapsed time is shown while the scan runs.
- The results remain in session state until the user reloads tables or clicks
  "Run scan" again.

---

## 6. File Layout

```
streamlit_worksheet/
├── pattern_merge.py          ← detection functions (no Streamlit dependency)
├── tabs/
│   └── tab9_merges.py        ← new tab: UI wrapping pattern_merge.py
├── sheet.py                  ← add tab9 to st.tabs(...)
└── pattern_merge_poc.ipynb   ← proof-of-concept notebook (this file)
```

New persistence files:

| File | Contents |
|------|----------|
| `merge_dismissals.json` | Set of `(pattern, delegate_id)` pairs dismissed as false positives |
| `pattern_synonyms.json` | List of `{delegate_id, patterns: [p1, p2]}` fragment equivalences |

---

## 7. Testing Strategy

1. **Notebook first** (`pattern_merge_poc.ipynb`) — run the detection functions
   against the real data with a single province and a single decade as a
   first pass.  Inspect the top-20 results manually to calibrate thresholds
   before wiring into the app.

2. **Synthetic unit test** — create a tiny artificial DataFrame with known
   concat and fragment errors, assert the function catches all of them and
   produces no false positives within that set.

3. **Timing benchmark** — run on the full ~430k rows with all provinces, log
   elapsed time per phase.  The pre-filter should reduce the fuzzy-match
   workload to < 1000 rows; if not, tighten the pre-filter.

4. **Precision spot-check** — sort output by score descending, manually verify
   the top 50 against the source document images (if available) or against the
   bio in `abbrd`.

---

## 8. Open Questions

- Does the occurrences file preserve intra-day row order, or does it need to
  be reconstructed from a positional column?  (Check for `volgnr`, `seq`, or
  rely on the DataFrame's physical row order within a `date` group.)
- For the split action — does it make sense to propose "insert a new delegate
  row" as an app capability, or is this always a post-hoc correction in the
  export step?
- Should `pattern_synonyms.json` feed back into the `n_patterns` computation
  in `build_summary` so the Overview tab shows a corrected count?
