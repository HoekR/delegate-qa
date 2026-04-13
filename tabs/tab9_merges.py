"""Tab 9 – Merge / Split Errors.

Surfaces two candidate lists produced by pattern_merge.py:

  A) Concat candidates  — patterns that look like two names fused together.
     Actions: Reassign left, Reassign right, Dismiss.

  B) Fragment candidates — delegates whose rare sub-patterns look like
     fragments of their modal name.
     Actions: Mark as equivalent (writes pattern_synonyms.json), Dismiss.

The scan is NOT run automatically; a "▶ Run scan" button triggers it once
and stores the result in st.session_state["merge_candidates"].  This keeps
every Streamlit rerun fast.

Dismissals are persisted to merge_dismissals.json (via utils.py helpers).
Synonym pairs are persisted to pattern_synonyms.json (via utils.py helpers).
Reassignments use the standard save_correction() path (same as tabs 2/3/4).
"""
from __future__ import annotations

import time
from typing import Callable

import pandas as pd
import streamlit as st

from pattern_merge import build_anchor_table, detect_concat_errors, detect_fragment_errors
from utils import (
    load_merge_dismissals,
    save_merge_dismissals,
    load_pattern_synonyms,
    save_pattern_synonyms,
)

# ── Column sets shown in the two result tables ─────────────────────────────
_CONCAT_DISPLAY_COLS = [
    "pattern", "delegate_id", "anchor",
    "split_left", "split_right",
    "left_delegate_id", "right_delegate_id",
    "combined_score", "n_occurrences",
]
_FRAG_DISPLAY_COLS = [
    "delegate_id", "anchor",
    "fragment_a", "freq_a",
    "fragment_b", "freq_b",
    "concat_score",
]


def render(
    tab,
    *,
    df_merged: pd.DataFrame,
    name_col: str,
    save_correction: Callable[[int, str | int], None],
) -> None:
    """Render the merge/split error detection tab.

    Parameters
    ----------
    tab :
        The st.tab context object returned by st.tabs().
    df_merged :
        The fully corrected merged DataFrame (corrections already applied by
        sheet.py before this render call).
    name_col :
        Column name used for human-readable delegate names (e.g. 'naam').
    save_correction :
        Callable(row_index, new_delegate_id) — the standard correction
        dispatcher from sheet.py.
    """
    with tab:
        st.title("🔗 Merge / Split Errors")
        st.caption(
            "Detect occurrences whose pattern looks like two names fused together "
            "(concat) or a single name split across rows (fragment). "
            "The scan is run on demand — click ▶ Run scan to start."
        )

        if df_merged.empty:
            st.info("No data loaded. Load occurrences data first.")
            return

        # ── Scan configuration ─────────────────────────────────────────────
        with st.expander("⚙️ Scan settings", expanded=False):
            col_prov, col_ya, col_yb = st.columns(3)
            with col_prov:
                provinces = sorted(
                    df_merged["provincie"].dropna().astype(str).unique().tolist()
                ) if "provincie" in df_merged.columns else []
                province = st.selectbox(
                    "Province filter",
                    options=["(all)"] + provinces,
                    index=0,
                    key="mrg_province",
                )
                province_val: str | None = None if province == "(all)" else province

            with col_ya:
                year_min_val = int(df_merged["j"].min()) if "j" in df_merged.columns else None
                year_min = st.number_input(
                    "Year from", value=year_min_val or 1700, step=1, key="mrg_year_min"
                )
            with col_yb:
                year_max_val = int(df_merged["j"].max()) if "j" in df_merged.columns else None
                year_max = st.number_input(
                    "Year to", value=year_max_val or 1800, step=1, key="mrg_year_max"
                )

            col_tc, col_tf, col_lr, col_nw = st.columns(4)
            with col_tc:
                t_concat = st.slider(
                    "T_concat (max edit distance per half)",
                    0.05, 0.50, 0.20, 0.05, key="mrg_t_concat",
                    help="Lower = stricter. 0.20 is a good starting point.",
                )
            with col_tf:
                t_frag = st.slider(
                    "T_frag (max edit distance for fragment pair)",
                    0.05, 0.40, 0.15, 0.05, key="mrg_t_frag",
                    help="Lower = stricter. 0.15 is a good starting point.",
                )
            with col_lr:
                min_len_ratio = st.slider(
                    "Min length ratio (pattern / anchor)",
                    1.1, 3.0, 1.4, 0.1, key="mrg_min_len_ratio",
                )
            with col_nw:
                neighbor_window = st.slider(
                    "Neighbor window (±N rows on same day)",
                    1, 5, 2, 1, key="mrg_nbr_window",
                )

        # ── Trigger button ─────────────────────────────────────────────────
        col_btn, col_status = st.columns([2, 5])
        with col_btn:
            run_scan = st.button("▶ Run scan", type="primary", key="mrg_run")
        with col_status:
            if "merge_scan_elapsed" in st.session_state:
                st.caption(
                    f"Last scan: {st.session_state['merge_scan_elapsed']:.1f} s  "
                    f"— {st.session_state.get('merge_scan_n_concat', 0)} concat  "
                    f"+ {st.session_state.get('merge_scan_n_frag', 0)} fragment candidates"
                )

        if run_scan:
            with st.spinner("Running scan …"):
                t0 = time.perf_counter()
                # Build anchor table once; reused by both detectors.
                at = build_anchor_table(df_merged)
                concat_df = detect_concat_errors(
                    df_merged, at=at,
                    t_concat=t_concat,
                    min_len_ratio=min_len_ratio,
                    neighbor_window=neighbor_window,
                    province=province_val,
                    year_min=int(year_min),
                    year_max=int(year_max),
                )
                frag_df = detect_fragment_errors(
                    df_merged, at=at,
                    t_frag=t_frag,
                    province=province_val,
                    year_min=int(year_min),
                    year_max=int(year_max),
                )
                elapsed = time.perf_counter() - t0

            st.session_state["merge_candidates"] = {
                "concat": concat_df,
                "frag":   frag_df,
                "at":     at,
            }
            st.session_state["merge_scan_elapsed"]   = elapsed
            st.session_state["merge_scan_n_concat"]  = len(concat_df)
            st.session_state["merge_scan_n_frag"]    = len(frag_df)
            st.rerun()

        # ── Results ────────────────────────────────────────────────────────
        candidates = st.session_state.get("merge_candidates")
        if candidates is None:
            st.info("No scan results yet. Click ▶ Run scan above.")
            return

        concat_df: pd.DataFrame = candidates["concat"]
        frag_df:   pd.DataFrame = candidates["frag"]
        at = candidates["at"]

        # Load current dismissals so we can hide already-dismissed candidates.
        dismissals = load_merge_dismissals()  # set of (pattern, delegate_id) tuples

        tab_c, tab_f = st.tabs(["🔀 Concat candidates", "🧩 Fragment candidates"])

        # ══════════════════════════════════════════════════════════════════
        # A) CONCAT CANDIDATES
        # ══════════════════════════════════════════════════════════════════
        with tab_c:
            st.subheader("Concat candidates")
            st.caption(
                "Patterns that appear to be two names fused into one occurrence. "
                "The proposed split and the matched neighbor delegates are shown. "
                "Select a row, then choose an action below."
            )

            # Filter out already-dismissed rows.
            if not concat_df.empty:
                dismissed_mask = concat_df.apply(
                    lambda r: (str(r["pattern"]), str(r["delegate_id"])) in dismissals,
                    axis=1,
                )
                concat_visible = concat_df[~dismissed_mask].copy()
            else:
                concat_visible = concat_df.copy()

            # Enrich with human-readable names for the two split candidates.
            if not concat_visible.empty and name_col in df_merged.columns:
                name_map = (
                    df_merged[["delegate_id", name_col]]
                    .drop_duplicates("delegate_id")
                    .set_index("delegate_id")[name_col]
                    .astype(str)
                    .to_dict()
                )
                concat_visible["left_name"]  = concat_visible["left_delegate_id"].map(
                    lambda d: name_map.get(str(d), str(d))
                )
                concat_visible["right_name"] = concat_visible["right_delegate_id"].map(
                    lambda d: name_map.get(str(d), str(d))
                )

            st.metric("Candidates (after dismissals)", len(concat_visible))

            if concat_visible.empty:
                st.success("No concat candidates — all clear (or all dismissed).")
            else:
                display_cols = [c for c in _CONCAT_DISPLAY_COLS if c in concat_visible.columns]
                if "left_name" in concat_visible.columns:
                    display_cols = display_cols + ["left_name", "right_name"]

                sel_c = st.dataframe(
                    concat_visible[display_cols].reset_index(drop=True),
                    height=350,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="mrg_concat_sel",
                )
                sel_c_rows = sel_c.get("selection", {}).get("rows", [])

                if sel_c_rows:
                    selected_row = concat_visible.iloc[sel_c_rows[0]]
                    st.markdown(
                        f"**Selected:** `{selected_row['pattern']}` "
                        f"(delegate `{selected_row['delegate_id']}`, "
                        f"{selected_row.get('n_occurrences', '?')} occurrence(s))"
                    )
                    st.markdown(
                        f"Proposed split: **`{selected_row['split_left']}`** ← left "
                        f"(→ `{selected_row['left_delegate_id']}`)"
                        f" / **`{selected_row['split_right']}`** → right "
                        f"(→ `{selected_row['right_delegate_id']}`)"
                    )

                    col_a, col_b, col_d = st.columns(3)

                    with col_a:
                        if st.button(
                            "⬅ Reassign ALL to left candidate",
                            key="mrg_reassign_left",
                            help=(
                                f"Set delegate_id = {selected_row['left_delegate_id']} "
                                f"for all {selected_row.get('n_occurrences','?')} "
                                "occurrences of this pattern."
                            ),
                        ):
                            # Find all row indices in df_merged that match this
                            # (pattern, delegate_id) pair and reassign them.
                            mask = (
                                (df_merged["pattern"].astype(str) == str(selected_row["pattern"]))
                                & (df_merged["delegate_id"].astype(str) == str(selected_row["delegate_id"]))
                            )
                            affected = df_merged.index[mask].tolist()
                            new_id = str(selected_row["left_delegate_id"])
                            for ridx in affected:
                                save_correction(ridx, new_id)
                            st.success(f"Reassigned {len(affected)} row(s) → {new_id}")
                            st.rerun()

                    with col_b:
                        if st.button(
                            "➡ Reassign ALL to right candidate",
                            key="mrg_reassign_right",
                            help=(
                                f"Set delegate_id = {selected_row['right_delegate_id']} "
                                f"for all {selected_row.get('n_occurrences','?')} "
                                "occurrences of this pattern."
                            ),
                        ):
                            mask = (
                                (df_merged["pattern"].astype(str) == str(selected_row["pattern"]))
                                & (df_merged["delegate_id"].astype(str) == str(selected_row["delegate_id"]))
                            )
                            affected = df_merged.index[mask].tolist()
                            new_id = str(selected_row["right_delegate_id"])
                            for ridx in affected:
                                save_correction(ridx, new_id)
                            st.success(f"Reassigned {len(affected)} row(s) → {new_id}")
                            st.rerun()

                    with col_d:
                        if st.button(
                            "🚫 Dismiss (false positive)",
                            key="mrg_dismiss_concat",
                            help="Mark this (pattern, delegate_id) pair as a known false positive.",
                        ):
                            key = (str(selected_row["pattern"]), str(selected_row["delegate_id"]))
                            dismissals.add(key)
                            save_merge_dismissals(dismissals)
                            st.success("Dismissed — will not appear in future scans.")
                            st.rerun()
                else:
                    st.caption("Select a row above to see available actions.")

        # ══════════════════════════════════════════════════════════════════
        # B) FRAGMENT CANDIDATES
        # ══════════════════════════════════════════════════════════════════
        with tab_f:
            st.subheader("Fragment candidates")
            st.caption(
                "Delegates with rare sub-patterns that together reconstruct the "
                "modal name — suggesting a tokenisation split error. "
                "Marking two patterns as equivalent removes the ghost fragment "
                "from the n_patterns count and pattern anomaly lists."
            )

            # Load current synonyms so we can hide already-handled pairs.
            synonyms = load_pattern_synonyms()
            # synonyms is a list of {"delegate_id": str, "patterns": [p1, p2]}

            def _is_already_synonym(row: pd.Series) -> bool:
                did = str(row["delegate_id"])
                fa  = str(row["fragment_a"])
                fb  = str(row["fragment_b"])
                for entry in synonyms:
                    if str(entry.get("delegate_id")) == did:
                        pats = set(entry.get("patterns", []))
                        if fa in pats and fb in pats:
                            return True
                return False

            if not frag_df.empty:
                dismissed_mask_f = frag_df.apply(
                    lambda r: (str(r["fragment_a"]), str(r["delegate_id"])) in dismissals
                    or (str(r["fragment_b"]), str(r["delegate_id"])) in dismissals,
                    axis=1,
                )
                synonym_mask_f = frag_df.apply(_is_already_synonym, axis=1)
                frag_visible = frag_df[~dismissed_mask_f & ~synonym_mask_f].copy()
            else:
                frag_visible = frag_df.copy()

            if not frag_visible.empty and name_col in df_merged.columns:
                name_map_f = (
                    df_merged[["delegate_id", name_col]]
                    .drop_duplicates("delegate_id")
                    .set_index("delegate_id")[name_col]
                    .astype(str)
                    .to_dict()
                )
                frag_visible["name"] = frag_visible["delegate_id"].map(
                    lambda d: name_map_f.get(str(d), str(d))
                )

            st.metric("Candidates (after dismissals / already handled)", len(frag_visible))

            if frag_visible.empty:
                st.success("No fragment candidates — all clear (or all handled).")
            else:
                display_cols_f = [c for c in _FRAG_DISPLAY_COLS if c in frag_visible.columns]
                if "name" in frag_visible.columns:
                    display_cols_f = ["name"] + display_cols_f

                sel_f = st.dataframe(
                    frag_visible[display_cols_f].reset_index(drop=True),
                    height=350,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="mrg_frag_sel",
                )
                sel_f_rows = sel_f.get("selection", {}).get("rows", [])

                if sel_f_rows:
                    selected_frow = frag_visible.iloc[sel_f_rows[0]]
                    st.markdown(
                        f"**Selected:** delegate `{selected_frow['delegate_id']}` "
                        f"(anchor `{selected_frow['anchor']}`)"
                    )
                    st.markdown(
                        f"Fragment A: **`{selected_frow['fragment_a']}`** "
                        f"({selected_frow.get('freq_a','?')} occurrences)  "
                        f"/ Fragment B: **`{selected_frow['fragment_b']}`** "
                        f"({selected_frow.get('freq_b','?')} occurrences)  "
                        f"— concat score {selected_frow.get('concat_score', '?')}"
                    )

                    col_eq, col_df = st.columns(2)

                    with col_eq:
                        if st.button(
                            "✅ Mark as equivalent (suppress ghost)",
                            key="mrg_mark_synonym",
                            help=(
                                "Adds this pair to pattern_synonyms.json. "
                                "The less-frequent fragment will be excluded "
                                "from n_patterns and pattern anomaly counts."
                            ),
                        ):
                            entry = {
                                "delegate_id": str(selected_frow["delegate_id"]),
                                "anchor": str(selected_frow["anchor"]),
                                "patterns": [
                                    str(selected_frow["fragment_a"]),
                                    str(selected_frow["fragment_b"]),
                                ],
                                # freq_a / freq_b let downstream code decide
                                # which is the ghost fragment (lower frequency).
                                "freq_a": int(selected_frow.get("freq_a", 0)),
                                "freq_b": int(selected_frow.get("freq_b", 0)),
                            }
                            synonyms.append(entry)
                            save_pattern_synonyms(synonyms)
                            st.success(
                                f"Saved synonym pair for delegate "
                                f"{selected_frow['delegate_id']}. "
                                "Reload the app to see updated n_patterns counts."
                            )
                            st.rerun()

                    with col_df:
                        if st.button(
                            "🚫 Dismiss (false positive)",
                            key="mrg_dismiss_frag",
                            help="Mark this fragment pair as a known false positive.",
                        ):
                            # Dismiss both fragment patterns for this delegate.
                            dismissals.add(
                                (str(selected_frow["fragment_a"]), str(selected_frow["delegate_id"]))
                            )
                            dismissals.add(
                                (str(selected_frow["fragment_b"]), str(selected_frow["delegate_id"]))
                            )
                            save_merge_dismissals(dismissals)
                            st.success("Dismissed — will not appear in future scans.")
                            st.rerun()
                else:
                    st.caption("Select a row above to see available actions.")

        # ── Documentation note ─────────────────────────────────────────────
        with st.expander("📄 Synonym register (for export appendix)", expanded=False):
            synonyms_now = load_pattern_synonyms()
            if not synonyms_now:
                st.info("No pattern synonyms registered yet.")
            else:
                st.caption(
                    "These fragment pairs are suppressed in n_patterns counts. "
                    "Include this table as an appendix in your final dataset documentation."
                )
                st.dataframe(
                    pd.DataFrame(synonyms_now).assign(
                        patterns=lambda df: df["patterns"].apply(
                            lambda p: " ≡ ".join(p) if isinstance(p, list) else str(p)
                        )
                    ),
                    use_container_width=True,
                )
