import pandas as pd

df = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet').set_index('cons_id_str')

did = '16189'
sub = df[df['delegate_id'].astype(str) == did].copy()
row = uq.loc[did]
birth, death = int(row['geboortejaar']), int(row['overlijdensjaar'])
print(f"{row['fullname']}  birth={birth}  death={death}")
print(f"Total rows: {len(sub)}")
print()

# Split by in-range vs out-of-range
in_range  = sub[(sub['j'] >= birth) & (sub['j'] <= death)]
post_death = sub[sub['j'] > death]
pre_birth  = sub[sub['j'] < birth]

print(f"In-range ({birth}–{death}): {len(in_range)}")
print(f"Post-death (>{death}): {len(post_death)}")
print(f"Pre-birth (<{birth}): {len(pre_birth)}")
print()

# What patterns appear in the post-death rows?
print("=== Post-death patterns (top 20) ===")
print(post_death['pattern'].value_counts().head(20).to_string())
print()

# What years are the post-death rows?
print("=== Post-death year distribution ===")
print(post_death['j'].value_counts().sort_index().to_string())
print()

# Are there related ids in uq with the same geslachtsnaam?
gn = row['geslachtsnaam']
kin = uq[uq['geslachtsnaam'].str.lower() == str(gn).lower()] if pd.notna(gn) else pd.DataFrame()
print(f"=== Other delegates with geslachtsnaam '{gn}' ===")
print(kin[['fullname','geboortejaar','overlijdensjaar','cons_id_str']].to_string())
