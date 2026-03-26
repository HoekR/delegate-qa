# Delegate QA App - User Guide

## 1. Quick overview
- Use the tabs: Overview, Alive Check, Pattern Anomalies, Name Mismatch, Timeline Gaps, Day Order.
- A single selected delegate in the sidebar or in Overview controls detail views in Tab 1-4.
- Corrections are made on rows (per occurrence) and streamed into session-state.

## 2. Corrections workflow
### Active corrections
- In each tab, fix a row and store via `save_correction` (internal action). 
- Active corrections are instantly applied in UI and persisted to `corrections.json`.

### Staged corrections
- In sidebar, `⏳ Stage current corrections` moves active corrections into `staged_corrections.json` and clears active.
- Staged corrections are not applied unless you click `✅ Load staged corrections into active`.
- By default staged corrections are hidden; enable `Show staged corrections (off-active)` to inspect.
- Clearing staged is off-app (manual file deletion of `staged_corrections.json`) to avoid accidents.

### Approval and archive
- In sidebar, `✅ Approve and archive current corrections` writes to `approved_corrections.json` and clears active.
- This is treated as a final pass, but *revert* is available:
  - `↩ Revert approved corrections to active` rehydrates active corrections for adjustment.

## 3. Suspicious delegate filter (Overview tab)
- Expand `🔍 Suspicious delegate filter`.
- Choose criteria: alive/age flags, large gaps, diverging patterns.
- Set sliders and checkbox to narrow suspicious list.
- Optionally enable `Auto-select first suspicious delegate when filters change` for immediate selection when slider/filter changes.

## 4. Git-style review state
- `Mark reviewed` in sidebar toggles status for currently selected delegate.
- Reviewed delegates are counted and preserved in `reviewed.json`.

## 5. Data files
- `corrections.json`: active correction mapping row->new delegate.
- `staged_corrections.json`: saved staging buffer with `to_id`+`staged_at` metadata.
- `approved_corrections.json`: approved corrections archive with `approved_at` metadata.
- `suggestion:- use these files as your edit log and source-of-truth for shipping changes.`

## 6. Troubleshooting
- If theory: `A row isn’t updated` — ensure `Load staged corrections into active` was run.
- If `tab0 suspicious includes good delegates` — check checkboxes & thresholds, use `Select first suspicious delegate` or auto-select.
