"""
For each likely-misassigned pattern-delegate pair, find candidate replacements
in the uq whose surname matches the offending pattern and whose active years
overlap the violation period.

Output: cluster_misassigned_with_replacements.csv
"""
import re, pathlib
import pandas as pd

_ws = pathlib.Path('.')

misassigned = pd.read_csv('cluster_likely_misassigned.csv')
uq_path = sorted(
    [p for p in _ws.glob('uq_delegates_baked_*.parquet')
     if re.fullmatch(r'uq_delegates_baked_\d{8}\.parquet', p.name)],
    reverse=True,
)[0]
uq = pd.read_parquet(uq_path)

# Ensure numeric year cols
for col in ('geboortejaar', 'overlijdensjaar', 'minjaar', 'maxjaar'):
    if col in uq.columns:
        uq[col] = pd.to_numeric(uq[col], errors='coerce')

def first_variant(pattern_str):
    """Return the first variant (before first semicolon, strip whitespace/newlines)."""
    return str(pattern_str).split(';')[0].strip().replace('\n', ' ')

def surname_tokens(s):
    """Lowercase tokens from a string, ignoring short particles."""
    tokens = re.sub(r'[^a-zA-Z ]', ' ', s).lower().split()
    return {t for t in tokens if len(t) > 2}

def candidate_score(row, viol_min, viol_max):
    """
    Score a uq row as a candidate replacement:
    - +3 if active/birth-death range overlaps violation years
    - +2 if minjaar/maxjaar (if set) overlaps violation years
    - +1 per matching token in geslachtsnaam vs pattern first variant
    """
    score = 0
    birth = row.get('geboortejaar')
    death = row.get('overlijdensjaar')
    minjaar = row.get('minjaar')
    maxjaar = row.get('maxjaar')

    # Lifespan overlap
    active_start = minjaar if pd.notna(minjaar) else birth
    active_end = maxjaar if pd.notna(maxjaar) else death

    if pd.notna(active_start) and pd.notna(active_end):
        if active_start <= viol_max and active_end >= viol_min:
            score += 3
    elif pd.notna(active_start) and active_start <= viol_max:
        score += 1
    elif pd.notna(active_end) and active_end >= viol_min:
        score += 1

    return score

rows_out = []

for _, mis in misassigned.iterrows():
    viol_min = mis['viol_year_min']
    viol_max = mis['viol_year_max']
    pattern = str(mis['offending_pattern'])
    first_v = first_variant(pattern)
    pat_tokens = surname_tokens(first_v)

    # Filter uq to candidates with matching surname tokens
    def name_score(uq_row):
        gesn = str(uq_row.get('geslachtsnaam', '') or '')
        fn   = str(uq_row.get('fullname', '') or '')
        combined = surname_tokens(gesn) | surname_tokens(fn)
        return len(pat_tokens & combined)

    # Score all uq rows
    scored = []
    for _, uq_row in uq.iterrows():
        ns = name_score(uq_row)
        if ns == 0:
            continue
        cs = candidate_score(uq_row, viol_min, viol_max)
        total = ns * 2 + cs
        if total >= 3:  # must have both name + time overlap
            scored.append((total, uq_row))

    scored.sort(key=lambda x: -x[0])
    top = scored[:3]

    base = {
        'delegate_id':       mis['delegate_id'],
        'fullname':          mis['fullname'],
        'birth':             mis['birth'],
        'death':             mis['death'],
        'offending_pattern': first_v,
        'in_violation':      mis['in_violation'],
        'viol_year_min':     viol_min,
        'viol_year_max':     viol_max,
    }

    if top:
        for rank, (score, cand) in enumerate(top, 1):
            row = dict(base)
            row['cand_rank']     = rank
            row['cand_id']       = cand['cons_id_str']
            row['cand_fullname'] = cand.get('fullname', '')
            row['cand_birth']    = cand.get('geboortejaar', '')
            row['cand_death']    = cand.get('overlijdensjaar', '')
            row['cand_minjaar']  = cand.get('minjaar', '')
            row['cand_maxjaar']  = cand.get('maxjaar', '')
            row['cand_score']    = score
            rows_out.append(row)
    else:
        row = dict(base)
        row['cand_rank'] = row['cand_id'] = row['cand_fullname'] = ''
        row['cand_birth'] = row['cand_death'] = row['cand_minjaar'] = row['cand_maxjaar'] = ''
        row['cand_score'] = 0
        rows_out.append(row)

out = pd.DataFrame(rows_out)

# ── carry over any user-added columns from the previous CSV ──────────────────
PRESERVE_COLS = ['checked', 'cand_id']   # cand_id: user may have overridden it
_prev_path = pathlib.Path('cluster_misassigned_with_replacements.csv')
if _prev_path.exists():
    # Try utf-8-sig (BOM) first (new format), fall back to mac_roman (old Excel save)
    for _enc in ('utf-8-sig', 'mac_roman'):
        try:
            prev = pd.read_csv(_prev_path, dtype=str, encoding=_enc,
                               sep=None, engine='python', on_bad_lines='skip')
            break
        except Exception:
            prev = pd.DataFrame()  # sentinel
    user_cols = [c for c in PRESERVE_COLS if c in prev.columns]
    if user_cols:
        _merge_key = ['delegate_id', 'offending_pattern', 'cand_rank']
        # normalise key cols to str for join
        for _df in (out, prev):
            for _c in _merge_key:
                if _c in _df.columns:
                    _df[_c] = _df[_c].astype(str).str.strip()
        # only bring across columns that user actually edited (checked + manual cand_id)
        prev_keep = prev[_merge_key + user_cols].drop_duplicates(subset=_merge_key)
        # for cand_id: only carry over rows where it differs from auto value
        if 'cand_id' in user_cols:
            # merge to compare
            _tmp = out.merge(prev_keep[_merge_key + ['cand_id']].rename(columns={'cand_id': '_prev_cand_id'}),
                             on=_merge_key, how='left')
            _override = _tmp['_prev_cand_id'].notna() & (_tmp['_prev_cand_id'] != _tmp['cand_id'].astype(str))
            out.loc[_override.values, 'cand_id'] = _tmp.loc[_override, '_prev_cand_id'].values
            # don't double-merge cand_id below
            user_cols = [c for c in user_cols if c != 'cand_id']
        if user_cols:
            out = out.merge(prev_keep[_merge_key + user_cols], on=_merge_key, how='left')
        print(f'Carried over columns from previous file: {["checked"] + (["cand_id"] if "cand_id" in PRESERVE_COLS else [])}')

out.to_csv('cluster_misassigned_with_replacements.csv', index=False, encoding='utf-8-sig')
print(f'Written cluster_misassigned_with_replacements.csv  ({len(out)} rows, {misassigned.shape[0]} problems)')
print(f'Problems with ≥1 candidate: {(out.groupby("delegate_id")["cand_score"].max() > 0).sum()}')
print(f'Problems with no candidate:  {(out.groupby("delegate_id")["cand_score"].max() == 0).sum()}')
