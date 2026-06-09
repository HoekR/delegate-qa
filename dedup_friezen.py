import pandas as pd

# Load your data
persons_df = pd.read_parquet('persons.parquet')  # or .csv/.xlsx as needed
patterns_df = pd.read_parquet('patterns.parquet')

# 1. Identify Frisian delegates to keep
friezen = persons_df[persons_df['id_regent_orig'] == 'republiek_friezen']
friezen_names = set(friezen['fullname'])

# 2. Find duplicates to drop (same name, different id_regent_orig)
dupes = persons_df[
    (persons_df['fullname'].isin(friezen_names)) &
    (persons_df['id_regent_orig'] != 'republiek_friezen')
]

# 3. Build mapping: duplicate persoon_id → correct persoon_id
mapping = {}
for _, row in dupes.iterrows():
    name = row['fullname']
    dup_id = row['persoon_id']
    # Find the correct id in friezen
    target_id = friezen[friezen['fullname'] == name]['persoon_id'].iloc[0]
    mapping[dup_id] = target_id

# 4. Reassign patterns
patterns_df['delegate_id'] = patterns_df['delegate_id'].replace(mapping)

# 5. Drop duplicates from persons
persons_df = persons_df[~(
    (persons_df['fullname'].isin(friezen_names)) &
    (persons_df['id_regent_orig'] != 'republiek_friezen')
)]

# Save results
persons_df.to_parquet('persons_deduped.parquet', index=False)
patterns_df.to_parquet('patterns_relinked.parquet', index=False)