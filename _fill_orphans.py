"""
Add orphan delegates to uq_delegates_baked_20260423.parquet.

Priority for person data:
  1. uq_delegates_baked_20260423 (already there)
  2. uq_delegates_updated_20260225 (older full registry, 1008 rows)
  3. baked occurrences (fullname + pattern from delegate's own rows)

Adds orphan delegates, writes updated uq parquet and reports what was added.
"""
import pandas as pd
from datetime import date

baked = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')
uq_full = pd.read_parquet('uq_delegates_updated_20260225.parquet')

known_ids = set(uq['cons_id_str'].astype(str))
orphan_ids = set(
    baked[~baked['delegate_id'].astype(str).isin(known_ids)]['delegate_id'].astype(str).unique()
)
print(f"Orphan ids to resolve: {len(orphan_ids)}")

# --- source 1: uq_full (has 'cons_id_str' key) ---
uq_full['cons_id_str'] = uq_full['cons_id_str'].astype(str)
from_full = uq_full[uq_full['cons_id_str'].isin(orphan_ids)].copy()
# Add missing columns present in uq but not uq_full
for col in ['active_after_corrections', 'patterns']:
    if col not in from_full.columns:
        from_full[col] = None
from_full = from_full[uq.columns]
covered_by_full = set(from_full['cons_id_str'].astype(str))
print(f"  Covered by uq_full: {len(covered_by_full)}")

# --- source 2: baked occurrences for the rest ---
still_orphan = orphan_ids - covered_by_full
# Build one row per delegate from their occurrences
occ = baked[baked['delegate_id'].astype(str).isin(still_orphan)].copy()
occ['delegate_id'] = occ['delegate_id'].astype(str)

def first_nonempty(s):
    v = s.dropna()
    return v.iloc[0] if len(v) else None

rows = []
for did, grp in occ.groupby('delegate_id'):
    # Collect all unique patterns into semicolon string
    pats = ';'.join(grp['pattern'].dropna().unique())
    # Pick the most common fullname as canonical
    fn = grp['fullname'].mode()
    fn = fn.iloc[0] if len(fn) else None
    rows.append({
        'cons_id_str': did,
        'delegate_id': did,
        'fullname': fn,
        'voornaam': first_nonempty(grp['voornaam']) if 'voornaam' in grp.columns else None,
        'tussenvoegsel': first_nonempty(grp['tussenvoegsel']) if 'tussenvoegsel' in grp.columns else None,
        'geslachtsnaam': first_nonempty(grp['geslachtsnaam']) if 'geslachtsnaam' in grp.columns else None,
        'geboortejaar': first_nonempty(grp['geboortejaar']) if 'geboortejaar' in grp.columns else None,
        'overlijdensjaar': first_nonempty(grp['overlijdensjaar']) if 'overlijdensjaar' in grp.columns else None,
        'provincie': grp['provincie'].mode().iloc[0] if 'provincie' in grp.columns and len(grp['provincie'].dropna()) else None,
        'resolutie_refs': None,
        'minjaar': grp['j'].min() if 'j' in grp.columns else None,
        'maxjaar': grp['j'].max() if 'j' in grp.columns else None,
        'pattern': pats,
        'heerlijkheid': first_nonempty(grp['heerlijkheid']) if 'heerlijkheid' in grp.columns else None,
        'active_after_corrections': None,
        'patterns': None,
    })
from_occ = pd.DataFrame(rows)[uq.columns]
print(f"  Covered by occurrences: {len(from_occ)}")

# --- combine ---
uq_new = pd.concat([uq, from_full, from_occ], ignore_index=True)
uq_new = uq_new.drop_duplicates(subset=['cons_id_str'])

today = date.today().strftime('%Y%m%d')
out_path = f'uq_delegates_baked_{today}.parquet'
uq_new.to_parquet(out_path, index=False)
print(f"\nWrote {out_path}  ({len(uq_new)} rows, was {len(uq)})")
print(f"Still missing: {len(orphan_ids) - len(covered_by_full) - len(from_occ)}")
