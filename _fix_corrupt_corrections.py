"""
Fix approved_corrections.json entries where to_id is a nested dict string
like "{'to_id': '16811', 'from_id': '16307', ...}" instead of just '16811'.

Extracts the real to_id from the inner dict and updates the entry.
Creates a backup before modifying.
"""
import json, re, ast, shutil, pathlib
from datetime import datetime

src = pathlib.Path('approved_corrections.json')

# Backup
bak = src.with_name(f'approved_corrections.bak_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
shutil.copy2(src, bak)
print(f'Backup: {bak.name}')

with open(src) as f:
    corr = json.load(f)

def extract_to_id(val):
    """If val is a dict-string, extract its to_id. Otherwise return as-is."""
    s = str(val)
    if not s.startswith('{'):
        return val
    try:
        inner = ast.literal_eval(s)
        return str(inner['to_id'])
    except Exception:
        m = re.search(r"'to_id':\s*'([^']+)'", s)
        return m.group(1) if m else val

n_fixed = 0
for key, rec in corr.items():
    raw = rec.get('to_id', '')
    if str(raw).startswith('{'):
        fixed = extract_to_id(raw)
        corr[key]['to_id'] = fixed
        n_fixed += 1

print(f'Fixed {n_fixed} entries')

with open(src, 'w') as f:
    json.dump(corr, f, ensure_ascii=False, indent=None)

print(f'Saved {src.name}')

# Verify
remaining = sum(1 for v in corr.values() if str(v.get('to_id','')).startswith('{'))
print(f'Remaining corrupt entries: {remaining}')
