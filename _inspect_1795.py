import pandas as pd

df = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')

# Parse year from date column
df['year'] = pd.to_numeric(df['date'].str[:4], errors='coerce')

# Sessions per year 1793-1797
band = df[df['year'].between(1793, 1797)]
print('=== rows per year 1793-1797 ===')
print(band.groupby('year').size().to_string())
print()

# What does the pattern column look like pre/post 1795?
if 'pattern' in df.columns:
    pre = df[df['year'] < 1795]['pattern'].dropna()
    post = df[df['year'] >= 1795]['pattern'].dropna()
    print(f'Pattern non-null pre-1795: {len(pre)}, post-1795: {len(post)}')
    print('Pre-1795 sample patterns:', pre.sample(min(5,len(pre)), random_state=1).tolist())
    print('Post-1795 sample patterns:', post.sample(min(5,len(post)), random_state=1).tolist())
    print()

# How many rows are from 1795+?
post1795 = df[df['year'] >= 1795]
print(f'Rows 1795+: {len(post1795)} ({100*len(post1795)/len(df):.1f}%)')

# Lifespan violations 1795+
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')
birth = uq.set_index('cons_id_str')['geboortejaar'].dropna()
death = uq.set_index('cons_id_str')['overlijdensjaar'].dropna()

post1795['j'] = post1795['year']
post1795 = post1795.copy()
post1795['birth'] = post1795['delegate_id'].astype(str).map(birth)
post1795['death'] = post1795['delegate_id'].astype(str).map(death)

viol = post1795[
    (post1795['j'] > post1795['death']) |
    (post1795['j'] < post1795['birth'])
]
matched = post1795[post1795['birth'].notna() | post1795['death'].notna()]
print(f'Post-1795 violations: {len(viol)} / {len(matched)} matched ({100*len(viol)/max(1,len(matched)):.1f}%)')

pre = df[df['year'] < 1795].copy()
pre['j'] = pre['year']
pre['birth'] = pre['delegate_id'].astype(str).map(birth)
pre['death'] = pre['delegate_id'].astype(str).map(death)
viol_pre = pre[(pre['j'] > pre['death']) | (pre['j'] < pre['birth'])]
matched_pre = pre[pre['birth'].notna() | pre['death'].notna()]
print(f'Pre-1795 violations: {len(viol_pre)} / {len(matched_pre)} matched ({100*len(viol_pre)/max(1,len(matched_pre)):.1f}%)')
