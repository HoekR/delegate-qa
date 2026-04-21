# Correction Report
Generated: 2026-04-17

## Dataset

| Item | Count |
|---|---|
| Total occurrence rows (`df_merged`) | 420,747 |

## Correction files

| Tier | File | Total entries | Applied to df_merged | Stale (row not found) | Unique from-ids | Unique to-ids |
|---|---|---:|---:|---:|---:|---:|
| active | `corrections.json` | 135 | 135 | 0 | 7 | 7 |
| approved | `approved_corrections.json` | 22,607 | 22,607 | 0 | 39 | 53 |
| staged | `staged_corrections.json` | 75,946 | 75,946 | 0 | 314 | 1,732 |
| **COMBINED** (union, active > approved > staged) | — | **79,360** | **79,360** | — | — | — |

Total entries across all three files: 98,688.  
Unique combined keys (overlap between tiers accounts for the reduction to 79,360).

**All corrections are valid** — zero stale entries across all tiers.

## Coverage

79,360 corrected rows out of 420,747 total = **18.9 %** of all occurrence rows have a correction assigned.

## Top 30 from_id → to_id remappings (by corrected row count)

| from_id | to_id | n_rows |
|---:|---:|---:|
| 16210 | 16307 | 4,202 |
| 16185 | 16189 | 2,893 |
| 13978 | 14024 | 2,491 |
| 13954 | 14226 | 2,064 |
| 13467 | 15127 | 1,942 |
| 15065 | 19809 | 1,836 |
| 13978 | 16887 | 1,732 |
| 16054 | 16120 | 1,131 |
| 13723 | 13729 | 787 |
| 14222 | 13481 | 506 |
| 13888 | 20710 | 478 |
| 14415 | 13679 | 451 |
| 21040 | 21797 | 403 |
| 15933 | 19955 | 372 |
| 13961 | 13604 | 318 |
| 13472 | 13554 | 317 |
| 17439 | 16094 | 289 |
| 13468 | 13553 | 281 |
| 18501 | 17663 | 255 |
| 14192 | 14009 | 210 |
| 18896 | 19973 | 200 |
| 16231 | 16297 | 200 |
| 14999 | 15140 | 200 |
| 15065 | 20332 | 200 |
| 14009 | 14192 | 196 |
| 20295 | 13632 | 189 |
| 15371 | 16055 | 188 |
| 13978 | 20303 | 186 |
| 14406 | 13613 | 181 |
| 13978 | 13977 | 178 |

Total unique (from_id, to_id) pairs across all tiers: **1,779**

## Notes

- `corrections.json` (active): row-level corrections added manually in the app, rich format `{to_id, from_id, name, updated_at, source}`.
- `approved_corrections.json` (approved): reviewed and approved corrections, format `{to_id, approved_at, source}`.
- `staged_corrections.json` (staged): bulk-staged corrections, plain `int` values (to_id only).
- Priority when keys overlap: active > approved > staged.
- "Baked" parquet (`delegates_18ee_w_correcties_baked.parquet`) is the output after all three tiers are applied via `apply_corrections()`.
