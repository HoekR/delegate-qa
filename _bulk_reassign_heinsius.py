import pandas as pd, json, pathlib
from datetime import datetime

orig = pd.read_parquet('delegates_18ee_w_correcties_20260123_marked.parquet')

mask_was = orig['delegate_id'].astype(str) == '13978'
sub = orig[mask_was]

# Rows that do NOT contain Span/Wass in pattern → belong to Heinsius
heinsius_idx = sub[~sub['pattern'].str.contains(r'[Ss]pan|[Ww]ass', na=False, regex=True)].index

print(f"Rows to reassign to Heinsius (14024): {len(heinsius_idx)}")

# Load existing approved corrections
corr_path = pathlib.Path('approved_corrections.json')
approved = json.loads(corr_path.read_text())
print(f"Existing approved corrections: {len(approved)}")

# Add bulk reassignment
now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
added = 0
for idx in heinsius_idx:
    key = str(idx)
    if key not in approved:
        approved[key] = {'to_id': '14024', 'approved_at': now, 'source': 'bulk_reassign_heinsius'}
        added += 1

corr_path.write_text(json.dumps(approved, indent=2))
print(f"Added {added} new corrections. Total now: {len(approved)}")
