"""
Filter cluster_offending_patterns.csv to remove cases where
the dates in uq are probably wrong (pattern IS the correct person).

Keep only cases that are likely genuine misassignments.

Heuristics for "filter out / dates probably wrong":
  1. Pattern's first variant closely matches delegate's geslachtsnaam
     AND gap between violation period and birth/death is ≤ 10 years
  2. minjaar/maxjaar for the delegate in uq are NaN  (app skips these too)

Output:
  cluster_likely_wrong_dates.csv   — filtered out (dates probably off)
  cluster_likely_misassigned.csv   — genuine problems, worth fixing
"""
import re, pathlib, unicodedata
import pandas as pd

def normalize(s):
    """lowercase, strip diacritics, keep only letters/spaces"""
    if not isinstance(s, str):
        return ''
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^a-z ]', '', s.lower()).strip()

def name_in_pattern(surname, pattern_str):
    """True if first word(s) of surname appear in the first few variants of pattern."""
    if not isinstance(surname, str) or not isinstance(pattern_str, str):
        return False
    # Take the first variant only
    first_variant = pattern_str.split(';')[0]
    surname_norm = normalize(surname)
    variant_norm = normalize(first_variant)
    # match on first significant word of surname (drop 'van', 'de', 'der', 'den', 'vanden', etc.)
    stop = {'van', 'de', 'der', 'den', 'vanden', 'von', 'te', 'ter', 'ten', 'op', 'het', 'in', 't'}
    words = [w for w in surname_norm.split() if w not in stop]
    if not words:
        return False
    key_word = words[0]
    return key_word in variant_norm

_ws = pathlib.Path('.')
_uq_files = sorted(
    [p for p in _ws.glob('uq_delegates_baked_*.parquet')
     if re.fullmatch(r'uq_delegates_baked_\d{8}\.parquet', p.name)],
    reverse=True,
)
uq = pd.read_parquet(_uq_files[0])
uq['cons_id_str'] = uq['cons_id_str'].astype(str)
uq_idx = uq.set_index('cons_id_str')

df = pd.read_csv('cluster_offending_patterns.csv')
df['delegate_id'] = df['delegate_id'].astype(str)

reasons = []

for _, row in df.iterrows():
    did = str(row['delegate_id'])
    uq_row = uq_idx.loc[did] if did in uq_idx.index else None

    # --- heuristic 1: app skips when minjaar/maxjaar are NaN ---
    minjaar = uq_row['minjaar'] if uq_row is not None else None
    maxjaar = uq_row['maxjaar'] if uq_row is not None else None
    if uq_row is not None and pd.isna(minjaar) and pd.isna(maxjaar):
        reasons.append('no_minjaar_maxjaar')
        continue

    # --- heuristic 2: pattern name matches surname + gap is small ---
    surname = uq_row['geslachtsnaam'] if uq_row is not None and pd.notna(uq_row.get('geslachtsnaam')) else ''
    pattern_str = str(row['offending_pattern'])
    matches_name = name_in_pattern(surname, pattern_str)

    birth = row['birth'] if pd.notna(row['birth']) else None
    death = row['death'] if pd.notna(row['death']) else None
    viol_min = row['viol_year_min'] if pd.notna(row['viol_year_min']) else None
    viol_max = row['viol_year_max'] if pd.notna(row['viol_year_max']) else None

    # Calculate minimum gap between violation period and the lifespan boundary
    gap = None
    if birth and viol_max and viol_max < birth:
        gap = birth - viol_max   # violations before birth
    if death and viol_min and viol_min > death:
        g = viol_min - death     # violations after death
        gap = min(gap, g) if gap is not None else g

    if matches_name and gap is not None and gap <= 10:
        reasons.append(f'name_match_small_gap_{gap}')
        continue

    reasons.append(None)   # keep

df['filter_reason'] = reasons

wrong_dates = df[df['filter_reason'].notna()].drop(columns='filter_reason')
misassigned = df[df['filter_reason'].isna()].drop(columns='filter_reason')

wrong_dates.to_csv('cluster_likely_wrong_dates.csv', index=False)
misassigned.to_csv('cluster_likely_misassigned.csv', index=False)

print(f'Total:           {len(df):4d} pattern-delegate pairs')
print(f'Likely dates off: {len(wrong_dates):4d}  → cluster_likely_wrong_dates.csv')
print(f'Likely misassigned: {len(misassigned):4d}  → cluster_likely_misassigned.csv')
print()
print('=== Top likely misassigned (by violation_rows) ===')
cols = ['delegate_id','fullname','birth','death','offending_pattern','in_violation','viol_year_min','viol_year_max']
print(misassigned[cols].head(30).to_string(index=False))
