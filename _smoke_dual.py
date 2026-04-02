import pandas as pd
from utils import build_suggestion_store, query_suggestions, load_data, build_merged

# load_data() returns (persons, occurrences, abbrd) — same order as sheet.py
df_p, df_i, df_bio = load_data()

# Capture sentinel rows from df_i BEFORE build_merged filters them (mirrors sheet.py)
sentinel_mask = df_i["delegate_id"].astype(str).isin({"-1", "-20"})
df_unresolved = df_i[sentinel_mask].copy()
print(f"df_i shape: {df_i.shape}   sentinel rows: {len(df_unresolved)}")

df_merged, n_ph, _, _ = build_merged(df_p, df_i, df_bio)
print(f"df_merged shape: {df_merged.shape}   placeholder rows counted: {n_ph}")

store = build_suggestion_store(df_merged)
print("Keys:", list(store.keys()))
print("Delegates indexed:", len(store["id_index"]))
print("key_char shape:", store["key_char"].shape)
print("key_word shape:", store["key_word"].shape)

sample = df_unresolved.head(5)
res = query_suggestions(store, sample, top_k=3, min_score=0.0)
print(f"\nSuggestions for {len(sample)} sentinel rows:")
print(res[["orig_idx", "pattern", "j", "cand_1", "score_1", "cand_2", "score_2"]].to_string())
