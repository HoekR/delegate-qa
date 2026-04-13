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

Name lookups use df_p (the persons registry) rather than df_merged, so
they reflect the authoritative names rather than raw HTR patterns.
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
    load_corrections,
    save_corrections,
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


def _build_name_map(df_p: pd.DataFrame, name_col: str) -> dict[str, str]:
    """Return {delegate_id: human_readable_name} from the persons registry.

    df_p is used rather than df_merged so names reflect the authoritative
    registry rather than raw HTR output.
    """
    if df_p.empty or "delegate_id" not in df_p.columns or name_col not in df_p.columns:
        return {}
    return (
        df_p[["delegate_id", name_col]]
        .drop_duplicates("delegate_id")
        .set_index("delegate_id")[name_col]
        .astype(str)
        .to_dict()
    )


def render(
    tab,
    *,
    df_merged: pd.DataFrame,
    df_p: pd.DataFrame,
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
    df_p :
        The persons registry DataFrame.  Used for authoritative name lookups
        so that left_name/right_name show proper names, not HTR patterns.
    name_col :
        Column in df_p containing the human-readable delegate name.
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

        # Build name map once from the authoritative persons registry.
        name_map = _build_name_map(df_p, name_col)

        def _name(delegate_id: str) -> str:
            return name_map.get(str(delegate_id), str(delegate_id))

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
                # Load synonyms before building anchors so ghost patterns are
                # collapsed and delegates with both fragment and concat errors
                # get a clean anchor for the concat detector.
                _synonyms_for_scan = load_pattern_synonyms()
                at = build_anchor_table(df_merged, synonyms=_synonyms_for_scan)
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
            st.session_state["merge_scan_elapsed"]  = elapsed
            st.session_state["merge_scan_n_concat"] = len(concat_df)
            st.session_state["merge_scan_n_frag"]   = len(frag_df)
            st.rerun()

        # ── Results ────────────────────────────────────────────────────────
        candidates = st.session_state.get("merge_candidates")
        if candidates is None:
            st.info("No scan results yet. Click ▶ Run scan above.")
            return

        concat_df: pd.DataFrame = candidates["concat"]
        frag_df:   pd.DataFrame = candidates["frag"]

        # ── Undo last action ───────────────────────────────────────────────
        last_action = st.session_state.get("mrg_last_action")
        if last_action:
            col_undo_lbl, col_undo_btn = st.columns([5, 2])
            with col_undo_lbl:
                st.info(f"↩ Last action: {last_action['label']}")
            with col_undo_btn:
                if st.button("↩ Undo", key="mrg_undo", type="secondary"):
                    corr = st.session_state.get("corrections", {})
                    for ridx in last_action["row_indices"]:
                        corr.pop(ridx, None)
                    st.session_state["corrections"] = corr
                    cfg = st.session_state.get("config", {})
                    save_corrections(corr, config=cfg)
                    del st.session_state["mrg_last_action"]
                    st.success("Undone — corrections removed.")
                    st.rerun()

        # Load dismissals / synonyms once; shared by both sub-tabs.
        dismissals = load_merge_dismissals()
        synonyms   = load_pattern_synonyms()

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

            # Filter dismissed rows.
            if not concat_df.empty:
                dismissed_mask = concat_df.apply(
                    lambda r: (str(r["pattern"]), str(r["delegate_id"])) in dismissals,
                    axis=1,
                )
                concat_visible = concat_df[~dismissed_mask].copy()
            else:
                concat_visible = concat_df.copy()

            # Enrich with authoritative names from df_p — injected next to id cols.
            if not concat_visible.empty:
                concat_visible["delegate_name"] = concat_visible["delegate_id"].map(_name)
                concat_visible["left_name"]     = concat_visible["left_delegate_id"].map(_name)
                concat_visible["right_name"]    = concat_visible["right_delegate_id"].map(_name)

            st.metric("Candidates (after dismissals)", len(concat_visible))

            if concat_visible.empty:
                st.success("No concat candidates — all clear (or all dismissed).")
            else:
                # Build display column list; insert name columns next to their id.
                enriched_cols: list[str] = []
                for c in _CONCAT_DISPLAY_COLS:
                    if c not in concat_visible.columns:
                        continue
                    enriched_cols.append(c)
                    if c == "delegate_id":
                        enriched_cols.append("delegate_name")
                    elif c == "left_delegate_id":
                        enriched_cols.append("left_name")
                    elif c == "right_delegate_id":
                        enriched_cols.append("right_name")

                sel_c = st.dataframe(
                    concat_visible[enriched_cols].reset_index(drop=True),
                    height=350,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="mrg_concat_sel",
                )
                sel_c_rows = sel_c.get("selection", {}).get("rows", [])
                # Guard against stale selection index after a dismissal/reassignment
                # shrinks the visible list on the next rerun.
                if sel_c_rows and sel_c_rows[0] >= len(concat_visible):
                    sel_c_rows = []

                if sel_c_rows:
                    row = concat_visible.iloc[sel_c_rows[0]]
                    did      = str(row["delegate_id"])
                    left_id  = str(row["left_delegate_id"])
                    right_id = str(row["right_delegate_id"])

                    st.markdown(
                        f"**Selected:** `{row['pattern']}` "
                        f"→ delegate `{did}` (*{_name(did)}*), "
                        f"{row.get('n_occurrences', '?')} occurrence(s)"
                    )
                    st.markdown(
                        f"Proposed split:  "
                        f"**`{row['split_left']}`** → `{left_id}` (*{_name(left_id)}*, "
                        f"score {row['left_score']:.3f})  /  "
                        f"**`{row['split_right']}`** → `{right_id}` (*{_name(right_id)}*, "
                        f"score {row['right_score']:.3f})"
                    )

                    col_a, col_b, col_d = st.columns(3)

                    with col_a:
                        if st.button(
                            f"⬅ Reassign ALL → {_name(left_id)}",
                            key="mrg_reassign_left",
                            help=f"Set delegate_id = {left_id} for every occurrence of this pattern.",
                        ):
                            mask = (
                                (df_merged["pattern"].astype(str) == str(row["pattern"]))
                                & (df_merged["delegate_id"].astype(str) == did)
                            )
                            affected = df_merged.index[mask].tolist()
                            for ridx in affected:
                                save_correction(ridx, left_id)
                            st.session_state["mrg_last_action"] = {
                                "row_indices": affected,
                                "label": f"Reassigned {len(affected)} row(s) of \u00ab{row['pattern']}\u00bb \u2192 {left_id} ({_name(left_id)})",
                            }
                            st.success(f"Reassigned {len(affected)} row(s) → {left_id} ({_name(left_id)})")
                            st.rerun()

                    with col_b:
                        if st.button(
                            f"➡ Reassign ALL → {_name(right_id)}",
                            key="mrg_reassign_right",
                            help=f"Set delegate_id = {right_id} for every occurrence of this pattern.",
                        ):
                            mask = (
                                (df_merged["pattern"].astype(str) == str(row["pattern"]))
                                & (df_merged["delegate_id"].astype(str) == did)
                            )
                            affected = df_merged.index[mask].tolist()
                            for ridx in affected:
                                save_correction(ridx, right_id)
                            st.session_state["mrg_last_action"] = {
                                "row_indices": affected,
                                "label": f"Reassigned {len(affected)} row(s) of \u00ab{row['pattern']}\u00bb \u2192 {right_id} ({_name(right_id)})",
                            }
                            st.success(f"Reassigned {len(affected)} row(s) → {right_id} ({_name(right_id)})")
                            st.rerun()

                    with col_d:
                        if st.button(
                            "🚫 Dismiss (false positive)",
                            key="mrg_dismiss_concat",
                            help="Mark this (pattern, delegate_id) pair as a known false positive.",
                        ):
                            dismissals.add((str(row["pattern"]), did))
                            save_merge_dismissals(dismissals)
                            st.success("Dismissed — will not appear in future scans.")
                            st.rerun()

                    # ── Manual override: assign left or right split to any ID ──
                    st.markdown("---")
                    st.caption(
                        "Override the proposed left or right delegate with a custom ID "
                        "(use when the split is correct but the matched delegate is wrong):"
                    )
                    col_inp, col_oa, col_ob = st.columns([3, 2, 2])
                    with col_inp:
                        custom_id = st.text_input(
                            "Delegate ID",
                            value="",
                            placeholder="e.g. 1234",
                            key="mrg_custom_id",
                            label_visibility="collapsed",
                        )
                    cid = custom_id.strip()
                    _mask_all = (
                        (df_merged["pattern"].astype(str) == str(row["pattern"]))
                        & (df_merged["delegate_id"].astype(str) == did)
                    )
                    with col_oa:
                        if st.button(
                            f"⬅ Left → custom",
                            key="mrg_reassign_custom_left",
                            disabled=not cid,
                            help=f"Reassign all occurrences to {cid} (override proposed left: {left_id})",
                        ):
                            affected = df_merged.index[_mask_all].tolist()
                            for ridx in affected:
                                save_correction(ridx, cid)
                            st.session_state["mrg_last_action"] = {
                                "row_indices": affected,
                                "label": f"Reassigned {len(affected)} row(s) of \u00ab{row['pattern']}\u00bb \u2192 {cid} ({_name(cid)}) [left override; was {left_id}]",
                            }
                            st.success(
                                f"Reassigned {len(affected)} row(s) → {cid} ({_name(cid)})  "
                                f"[left override; was {left_id}]"
                            )
                            st.rerun()
                    with col_ob:
                        if st.button(
                            f"➡ Right → custom",
                            key="mrg_reassign_custom_right",
                            disabled=not cid,
                            help=f"Reassign all occurrences to {cid} (override proposed right: {right_id})",
                        ):
                            affected = df_merged.index[_mask_all].tolist()
                            for ridx in affected:
                                save_correction(ridx, cid)
                            st.session_state["mrg_last_action"] = {
                                "row_indices": affected,
                                "label": f"Reassigned {len(affected)} row(s) of \u00ab{row['pattern']}\u00bb \u2192 {cid} ({_name(cid)}) [right override; was {right_id}]",
                            }
                            st.success(
                                f"Reassigned {len(affected)} row(s) → {cid} ({_name(cid)})  "
                                f"[right override; was {right_id}]"
                            )
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
                    lambda r: (
                        (str(r["fragment_a"]), str(r["delegate_id"])) in dismissals
                        or (str(r["fragment_b"]), str(r["delegate_id"])) in dismissals
                    ),
                    axis=1,
                )
                synonym_mask_f = frag_df.apply(_is_already_synonym, axis=1)
                frag_visible = frag_df[~dismissed_mask_f & ~synonym_mask_f].copy()
            else:
                frag_visible = frag_df.copy()

            # Enrich with authoritative name from df_p.
            if not frag_visible.empty:
                frag_visible.insert(0, "name", frag_visible["delegate_id"].map(_name))

            st.metric("Candidates (after dismissals / already handled)", len(frag_visible))

            if frag_visible.empty:
                st.success("No fragment candidates — all clear (or all handled).")
            else:
                display_cols_f = ["name"] + [
                    c for c in _FRAG_DISPLAY_COLS if c in frag_visible.columns
                ]

                sel_f = st.dataframe(
                    frag_visible[display_cols_f].reset_index(drop=True),
                    height=350,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="mrg_frag_sel",
                )
                sel_f_rows = sel_f.get("selection", {}).get("rows", [])

                if sel_f_rows:
                    frow = frag_visible.iloc[sel_f_rows[0]]
                    frag_did = str(frow["delegate_id"])
                    st.markdown(
                        f"**Selected:** `{frag_did}` — *{_name(frag_did)}* "
                        f"(anchor `{frow['anchor']}`)"
                    )
                    st.markdown(
                        f"Fragment A: **`{frow['fragment_a']}`** "
                        f"({frow.get('freq_a','?')} occurrences)  "
                        f"/ Fragment B: **`{frow['fragment_b']}`** "
                        f"({frow.get('freq_b','?')} occurrences)  "
                        f"— concat score {frow.get('concat_score', '?')}"
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
                                "delegate_id": frag_did,
                                "anchor":      str(frow["anchor"]),
                                "patterns":    [
                                    str(frow["fragment_a"]),
                                    str(frow["fragment_b"]),
                                ],
                                "freq_a": int(frow.get("freq_a", 0)),
                                "freq_b": int(frow.get("freq_b", 0)),
                            }
                            synonyms.append(entry)
                            save_pattern_synonyms(synonyms)
                            st.success(
                                f"Saved synonym pair for *{_name(frag_did)}*. "
                                "Reload the app to see updated n_patterns counts."
                            )
                            st.rerun()

                    with col_df:
                        if st.button(
                            "🚫 Dismiss (false positive)",
                            key="mrg_dismiss_frag",
                        ):
                            dismissals.add((str(frow["fragment_a"]), frag_did))
                            dismissals.add((str(frow["fragment_b"]), frag_did))
                            save_merge_dismissals(dismissals)
                            st.success("Dismissed — will not appear in future scans.")
                            st.rerun()
                else:
                    st.caption("Select a row above to see available actions.")

        # ── Synonym register ───────────────────────────────────────────────
        with st.expander("📄 Synonym register (for export appendix)", expanded=False):
            synonyms_now = load_pattern_synonyms()
            if not synonyms_now:
                st.info("No pattern synonyms registered yet.")
            else:
                st.caption(
                    "These fragment pairs are suppressed in n_patterns counts. "
                    "Include this table as an appendix in your final dataset documentation."
                )
                syn_df = pd.DataFrame(synonyms_now)
                if "patterns" in syn_df.columns:
                    syn_df["patterns"] = syn_df["patterns"].apply(
                        lambda p: " ≡ ".join(p) if isinstance(p, list) else str(p)
                    )
                if "delegate_id" in syn_df.columns:
                    syn_df.insert(0, "name", syn_df["delegate_id"].map(_name))
                st.dataframe(syn_df, use_container_width=True)
