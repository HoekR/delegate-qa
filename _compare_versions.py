import pandas as pd

old = pd.read_parquet('delegates_18ee_w_correcties_20260123_marked.parquet')
new = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')

old_sub = old[['Kolom1','delegate_id','pattern','j']].rename(columns={'delegate_id':'did_old'})
new_sub = new[['Kolom1','delegate_id','pattern','j','birth_year','death_year']].rename(columns={'delegate_id':'did_new'})

merged = old_sub.merge(new_sub, on=['Kolom1','pattern','j'], how='inner')
print(f'Matched rows: {len(merged)}')

changed = merged[merged['did_old'].astype(str) != merged['did_new'].astype(str)]
print(f'Rows where delegate_id changed: {len(changed)} ({100*len(changed)/len(merged):.1f}%)')
print()

# For the lifespan-violating clusters in NEW - were they already wrong in OLD?
problem_new = ['16307','13573','16189','15127','14024','16887','19808']
for did in problem_new:
    rows_new = merged[merged['did_new'] == did]
    if len(rows_new) == 0:
        print(f'{did}: not in new')
        continue
    old_assignment = rows_new['did_old'].value_counts().head(3).to_dict()
    print(f'NEW cluster {did} ({len(rows_new)} rows): OLD assignment -> {old_assignment}')

print()
# Also check: how many of the lifespan violations already existed in OLD?
# Old doesn't have birth_year/death_year, but we can look at which ones were already in OLD
# with the same delegate_id (meaning the error was pre-existing)
print('=== Error source: pre-existing vs introduced ===')
for did in problem_new:
    rows_new = merged[merged['did_new'] == did]
    # Pre-existing: already assigned to same (wrong) delegate in old
    pre_existing = rows_new[rows_new['did_old'] == did]
    introduced = rows_new[rows_new['did_old'] != did]
    print(f'{did}: pre-existing={len(pre_existing)}, introduced_by_correction={len(introduced)}')

print()
# Broader: among all rows that violate lifespan in new, were they different in old?
new_w_dates = new[['Kolom1','delegate_id','birth_year','death_year','j']].copy()
new_w_dates['did_new'] = new_w_dates['delegate_id'].astype(str)
after_death_idx = new_w_dates[(new_w_dates['death_year'].notna()) & (new_w_dates['j'] > new_w_dates['death_year'])]['Kolom1']
before_birth_idx = new_w_dates[(new_w_dates['birth_year'].notna()) & (new_w_dates['j'] < new_w_dates['birth_year'])]['Kolom1']
all_violations = set(after_death_idx) | set(before_birth_idx)
print(f'Total lifespan-violating rows in new: {len(all_violations)}')

viol_merged = merged[merged['Kolom1'].isin(all_violations)]
same_as_old = viol_merged[viol_merged['did_old'].astype(str) == viol_merged['did_new'].astype(str)]
diff_from_old = viol_merged[viol_merged['did_old'].astype(str) != viol_merged['did_new'].astype(str)]
print(f'Of these, same as old (pre-existing): {len(same_as_old)} ({100*len(same_as_old)/len(viol_merged):.1f}%)')
print(f'Of these, changed from old (introduced): {len(diff_from_old)} ({100*len(diff_from_old)/len(viol_merged):.1f}%)')
