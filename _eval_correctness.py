"""
Estimate correction quality against manually-labelled ground truth sets.

Ground truth files:
  ground_truth_concat.csv       – 21 cases labelled for concatenated-name pairs
  ground_truth_frag.csv         – 34 cases labelled for fragmented names
  ground_truth_concat_candidates_labeled.csv – 200 pattern-level labels

Correction files evaluated:
  staged_corrections.json    {from_id: to_id}
  approved_corrections.json  {from_id: to_id}
  corrections.json           list of {from_id, to_id, ...}
  staged_splits.json         list of {delegate_id, ...}
"""
import json, pandas as pd

# ── Load correction maps ───────────────────────────────────────────────────────
staged   = json.loads(open('staged_corrections.json').read())
approved = json.loads(open('approved_corrections.json').read())
active   = json.loads(open('corrections.json').read())
splits   = json.loads(open('staged_splits.json').read())

# Unified map: from_id -> to_id (string keys, priority: active > approved > staged)
corr_map = {}
for k, v in staged.items():
    corr_map[str(k)] = str(v)
for k, v in approved.items():
    corr_map[str(k)] = str(v)
if isinstance(active, list):
    for entry in active:
        fid = str(entry.get('from_id', entry.get('delegate_id', '')))
        tid = str(entry.get('to_id', ''))
        if fid and tid and fid != tid:
            corr_map[fid] = tid
elif isinstance(active, dict):
    for k, v in active.items():
        corr_map[str(k)] = str(v)

split_ids = set()
if isinstance(splits, list):
    for s in splits:
        if isinstance(s, dict):
            did = s.get('delegate_id') or s.get('from_id')
            if did:
                split_ids.add(str(did))

print(f"Correction map entries : {len(corr_map):,}")
print(f"Split ids              : {len(split_ids):,}")

# ── Helper: are two ids merged? ────────────────────────────────────────────────
def are_merged(a, b):
    """Return True if a→b or b→a in the correction map."""
    a, b = str(a), str(b)
    return corr_map.get(a) == b or corr_map.get(b) == a


# ══════════════════════════════════════════════════════════════════════════════
# 1. GT CONCAT (21 rows): genuine_concat → should be merged
# ══════════════════════════════════════════════════════════════════════════════
gt_concat = pd.read_csv('ground_truth_concat.csv')
print(f"\n── GT concat ({len(gt_concat)} rows) ──")
print("Label distribution:", gt_concat['label'].value_counts().to_dict())

tp_c = fp_c = fn_c = tn_c = 0
missed = []
wrong  = []

for _, row in gt_concat.iterrows():
    left   = row['left_delegate_id']
    right  = row['right_delegate_id']
    if pd.isna(left) or pd.isna(right):
        continue
    merged = are_merged(int(left), int(right))
    genuine = row['label'] == 'genuine_concat'
    if genuine and merged:
        tp_c += 1
    elif genuine and not merged:
        fn_c += 1
        missed.append(row['pattern'])
    elif not genuine and merged:
        fp_c += 1
        wrong.append(f"{row['pattern']} ({row['label']})")
    else:
        tn_c += 1

precision_c = tp_c / (tp_c + fp_c) if (tp_c + fp_c) else float('nan')
recall_c    = tp_c / (tp_c + fn_c) if (tp_c + fn_c) else float('nan')
f1_c        = 2*precision_c*recall_c/(precision_c+recall_c) if (precision_c+recall_c) else float('nan')

print(f"  TP={tp_c}  FP={fp_c}  FN={fn_c}  TN={tn_c}")
print(f"  Precision={precision_c:.3f}  Recall={recall_c:.3f}  F1={f1_c:.3f}")
if missed:
    print(f"  Missed genuine merges ({len(missed)}): {missed}")
if wrong:
    print(f"  Wrong merges ({len(wrong)}): {wrong}")


# ══════════════════════════════════════════════════════════════════════════════
# 2. GT FRAG (34 rows): genuine_concat → should be merged; compound → separate
# ══════════════════════════════════════════════════════════════════════════════
gt_frag = pd.read_csv('ground_truth_frag.csv')
print(f"\n── GT frag ({len(gt_frag)} rows) ──")
print("Label distribution:", gt_frag['label'].value_counts().to_dict())

# Each row has a single delegate_id; genuine_concat means it is a concat of two
# people and should have been split/corrected. We check via split_ids.
tp_f = fp_f = fn_f = tn_f = 0
missed_f = []
wrong_f  = []

for _, row in gt_frag.iterrows():
    did     = str(row['delegate_id'])
    genuine = row['label'] == 'genuine_concat'
    is_split = did in split_ids or did in corr_map
    if genuine and is_split:
        tp_f += 1
    elif genuine and not is_split:
        fn_f += 1
        missed_f.append(f"{row['anchor']} (id={did})")
    elif not genuine and is_split:
        fp_f += 1
        wrong_f.append(f"{row['anchor']} (id={did}, label={row['label']})")
    else:
        tn_f += 1

precision_f = tp_f / (tp_f + fp_f) if (tp_f + fp_f) else float('nan')
recall_f    = tp_f / (tp_f + fn_f) if (tp_f + fn_f) else float('nan')
f1_f        = 2*precision_f*recall_f/(precision_f+recall_f) if (precision_f+recall_f) else float('nan')

print(f"  TP={tp_f}  FP={fp_f}  FN={fn_f}  TN={tn_f}")
print(f"  Precision={precision_f:.3f}  Recall={recall_f:.3f}  F1={f1_f:.3f}")
if missed_f:
    print(f"  Missed genuine splits ({len(missed_f)}): {missed_f[:10]}")
if wrong_f:
    print(f"  Wrong splits ({len(wrong_f)}): {wrong_f[:10]}")


# ══════════════════════════════════════════════════════════════════════════════
# 3. GT CANDIDATES LABELED (200 rows): pattern-level
#    genuine_concat marked → pattern should appear in staged/approved corrections
#    false_positive marked → should NOT be corrected
# ══════════════════════════════════════════════════════════════════════════════
gt_cand = pd.read_csv('ground_truth_concat_candidates_labeled.csv', sep=';')
print(f"\n── GT candidates labeled ({len(gt_cand)} rows) ──")
for col in ['genuine_concat','compound','false_positive','fragmented_compound']:
    n = gt_cand[col].notna().sum() if col in gt_cand.columns else 0
    print(f"  {col}: {n}")

# For genuine_concat rows: delegate_id should be corrected
genuine_cand = gt_cand[gt_cand['genuine_concat'].notna()].copy() if 'genuine_concat' in gt_cand.columns else pd.DataFrame()
tp_p = fn_p = 0
for _, row in genuine_cand.iterrows():
    did = str(int(row['delegate_id']))
    if did in corr_map or did in split_ids:
        tp_p += 1
    else:
        fn_p += 1

# For false_positive rows: should NOT be in correction map
fp_cand = gt_cand[gt_cand['false_positive'].notna()].copy() if 'false_positive' in gt_cand.columns else pd.DataFrame()
fp_p = 0
for _, row in fp_cand.iterrows():
    did = str(int(row['delegate_id']))
    if did in corr_map:
        fp_p += 1
        print(f"  FP in corrections: delegate_id={did} pattern={row['pattern']}")

total_p = len(genuine_cand)
precision_p = tp_p / (tp_p + fp_p) if (tp_p + fp_p) else float('nan')
recall_p    = tp_p / (tp_p + fn_p) if (tp_p + fn_p) else float('nan')
f1_p        = 2*precision_p*recall_p/(precision_p+recall_p) if (precision_p+recall_p) else float('nan')
print(f"  Genuine marked: {total_p}, corrected: {tp_p}, missed: {fn_p}, false positives: {fp_p}")
print(f"  Precision={precision_p:.3f}  Recall={recall_p:.3f}  F1={f1_p:.3f}")


# ══════════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════════
print("\n══ SUMMARY ══")
print(f"{'Evaluation':<35} {'Precision':>10} {'Recall':>10} {'F1':>10}  {'N'}")
print(f"{'─'*70}")
print(f"{'GT concat (21 rows)':<35} {precision_c:>10.3f} {recall_c:>10.3f} {f1_c:>10.3f}  {tp_c+fp_c+fn_c+tn_c}")
print(f"{'GT frag (34 rows)':<35} {precision_f:>10.3f} {recall_f:>10.3f} {f1_f:>10.3f}  {tp_f+fp_f+fn_f+tn_f}")
print(f"{'GT candidates labeled (200 rows)':<35} {precision_p:>10.3f} {recall_p:>10.3f} {f1_p:>10.3f}  {total_p+fp_p}")
