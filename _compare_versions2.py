import pandas as pd

old = pd.read_parquet('delegates_18ee_w_correcties_20260123_marked.parquet')
new = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')

# The corrections reassigned rows FROM these OLD clusters INTO the new (wrong) ones
# Let's understand what those old clusters were
old_sources = {
    '16210': ('16307 Rouse', 4202),  # 4202 rows from 16210 -> 16307
    '16185': ('16189 Rechteren', 2893),  # 2893 rows from 16185 -> 16189
    '13467': ('15127 van Wassenaer van Obdam', 1942),  # from 13467 -> 15127
    '13978': ('14024 Heinsius + 16887 van Wassenaer Spanbroek', 1732+2413),  # split to two!
}

print('=== What were the OLD source clusters? ===')
for old_id, (target, n) in old_sources.items():
    sub = old[old['delegate_id'] == old_id]
    print(f'OLD {old_id} -> now in NEW {target} ({n} rows moved):')
    print(f'  total rows in old: {len(sub)}')
    print(f'  j range: {sub["j"].min()}-{sub["j"].max()}')
    print(f'  top patterns: {sub["pattern"].value_counts().head(5).to_dict()}')
    print()

# Specifically 13978 - was split into both 14024 (Heinsius, born 1752) and 16887 (Spanbroek, born 1752)
# Both target persons born 1752 but events 1705-1720 -> clearly belongs to an EARLIER person
old_13978 = old[old['delegate_id'] == '13978']
print('OLD 13978 full:')
print(f'  rows: {len(old_13978)}, j range: {old_13978["j"].min()}-{old_13978["j"].max()}')
print(f'  top patterns: {old_13978["pattern"].value_counts().head(10).to_dict()}')

# In new: what happened to 13978?
new_13978 = new[new['delegate_id'].astype(str) == '13978']
print(f'\n  In NEW: {len(new_13978)} rows still have 13978')

print()
# What is NEW 13978? (the ones NOT moved)
print('=== NEW clusters for old 13978 rows ===')
old_sub = old[old['delegate_id'] == '13978'][['Kolom1','j','pattern']]
new_sub = new[['Kolom1','delegate_id','delegate_name','j','pattern']]
joined = old_sub.merge(new_sub, on=['Kolom1','j','pattern'], how='inner')
print(joined['delegate_id'].value_counts().to_dict())
print()

# Also check what happened to OLD 16210 (Rouse predecessor?)
old_16210 = old[old['delegate_id'] == '16210']
print(f'OLD 16210: {len(old_16210)} rows, j={old_16210["j"].min()}-{old_16210["j"].max()}')
print(f'  delegate_name: {old_16210["delegate_name"].value_counts().head(3).to_dict()}')
print(f'  top patterns: {old_16210["pattern"].value_counts().head(5).to_dict()}')
# What did NEW assign those rows to?
joined_16210 = old_16210[['Kolom1','j','pattern']].merge(new_sub, on=['Kolom1','j','pattern'], how='inner')
print(f'  In NEW assigned to: {joined_16210["delegate_id"].value_counts().to_dict()}')
