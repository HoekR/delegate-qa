# Round-up Memo
> Running list of loose ends, deferred tasks, and known issues.
> Update as things come up; strike through when done.

---

## Deduplication (do in one pass at the end)

- `republic_add_13` (Quint, Cornelis Heemstede) is a phantom duplicate of `20303` (Quint, Cornelis):
  same person, same years (1666–1743), same province (Utrecht), same heerlijkheid (Heemstede), same refs.
  republic_add_13 has 0 baked rows — all 914 rows already live under 20303.
  **Action:** drop republic_add_13 from uq and optionally from new_delegates.json.

- Frisian deduplication: see `dedup_friezen.py`. Do in the same pass.

- Check remaining republic_add_* entries for similar phantoms (other delegates since identified in abbrd).

---

## Republic_add_* synthetic dates

- republic_add_* delegates are not in abbrd (that is why they have the republic_add id in the first place).
- Their geboortejaar/overlijdensjaar in uq are **synthetic** — derived from the h_life midpoint of their occurrences, not real biographical data.
- Better h_life estimation: use `hypothetical_life()` from `/Users/rikhoekstra/develop/republic_clean/republic/data/datamangler.py`:
  - birth_year = `minjaar - 34` (ordinary delegate) or `minjaar - 44` (gedeputeerde); offsets empirically derived from abbrd corpus
  - death_year = `maxjaar + 22`
  - No need to re-calibrate the offsets — same corpus/context, only 14 delegates affected.
  - Add `dates_synthetic = True` flag to these rows when applying.
- Consequence: bio-year validation checks (alive check, cluster analysis) are approximate for these delegates until hlife pass is done.

---

## Cluster corrections (in progress)

- `cluster_misassigned_with_replacements.csv` — 60 rows (36 problems), UTF-8 BOM, has `checked` column.
- User is reviewing; when done: run `_apply_cluster_corrections.py`, then `_rebake_parquet.py`, then `test_bake.py`.
- Scripts involved: `_find_replacements.py`, `_apply_cluster_corrections.py`, `_rebake_parquet.py`.

---

## Pattern merge / split detection

- Design is in `pattern_merge_plan.md`.
- POC is in `pattern_merge_poc.ipynb`.
- Implementation: `pattern_merge.py`.
- Not yet fully applied to the baked data — revisit after cluster corrections are done.

---

## Larger pending tasks (from plan_next.md)

| Task | Status | Blocker |
|---|---|---|
| Task 3 — cleanup (delete smoke/check scripts) | unblocked | — |
| Task 4 — NER span matching | unblocked | needs NER output file from user |
| Task 1 — RAG explanation layer | blocked | Ollama / API key |
| Task 5 — officials DB integration | blocked | Repertorium van ambtsdragers export |
| Task 2 — 17th-c. cold-start | blocked | Task 5 data + 17th-c. occurrences file |

---

## Data files still missing

- NER output file (span text + document year) — needed for Task 4
- 17th-c. occurrences file — needed for Task 2
- Repertorium van ambtsdragers export — needed for Tasks 2 & 5

---

## Minor / future

- `_rebake_parquet.py` always picks up the latest dated uq parquet automatically — good, no action needed.
- Correction report (`correction_report.md`) last generated 2026-04-23 — regenerate after next rebake.
- `approved_corrections.json` has 22,607 entries; `staged_corrections.json` has 75,980 — combined ~79,400 unique. Keep an eye on stale entries after reassignments.
