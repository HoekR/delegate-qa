"""Tab 2 – Pattern Anomalies: divergent name patterns per delegate."""
from __future__ import annotations

from typing import Callable

import re
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

from utils import load_pattern_status, save_pattern_status, rerun


def render(
    tab,
    *,
    df_delegate: pd.DataFrame,
    name_col: str,
    save_correction: Callable,
    corrected_indices: set = frozenset(),
    debug: bool = False,
) -> None:
    with tab:
        st.title("🔤 Pattern Anomalies")
        st.caption("Name patterns that diverge strongly from the modal pattern for each delegate.")

        if not df_delegate.empty and "delegate_id" in df_delegate.columns:
            _id = str(df_delegate["delegate_id"].iloc[0])
            _nm = str(df_delegate[name_col].iloc[0]) if name_col in df_delegate.columns else _id
            st.info(f"🔍 Inspecting: **{_nm}** — ID `{_id}`")

        if df_delegate.empty:
            st.info("👆 Select a delegate in the Overview tab or the sidebar to see results.")
            return

        if "pattern" not in df_delegate.columns:
            st.warning("No `pattern` column in occurrences file.")
            return

        # Filter by user-visible pattern validity status.
        pat_filter = st.selectbox(
            "Pattern validity", ["All", "Valid only", "Invalid only"], key="pat_validity",
        )
        if "pattern_is_valid" in df_delegate.columns:
            if pat_filter == "Valid only":
                df_delegate = df_delegate[df_delegate["pattern_is_valid"] == True]
            elif pat_filter == "Invalid only":
                df_delegate = df_delegate[df_delegate["pattern_is_valid"] == False]

        try:
            from rapidfuzz import distance as rfd
            has_rf = True
        except ImportError:
            has_rf = False
            st.info("Install `rapidfuzz` for better scoring: `uv pip install rapidfuzz`")

        threshold = st.slider("Anomaly threshold (0–1)", 0.0, 1.0, 0.5, 0.05, key="pat_thresh")
        top_n = st.slider("Show top N", 5, 200, 200, key="pat_topn")

        import time as _t
        _t0 = _t.perf_counter()
        records = []

        def _split_patterns(pat: str) -> list[str]:
            # Some datasets store multiple patterns in a single cell (e.g. separated by ';' or '|').
            # If so, split them so each pattern appears as its own row in the anomaly table.
            pat = str(pat).strip()
            if not pat:
                return []
            # Common delimiters used in pattern lists
            parts = [p.strip() for p in re.split(r"[;|\\/]+", pat) if p.strip()]
            return parts if len(parts) > 1 else [pat]

        for del_id, grp in df_delegate.groupby("delegate_id", observed=True):
            pats = grp["pattern"].dropna().astype(str)
            if pats.empty:
                continue
            modal = pats.mode().iloc[0]
            for idx, pat in pats.items():
                for pat_part in _split_patterns(pat):
                    if has_rf:
                        score = rfd.Levenshtein.normalized_distance(pat_part, modal)
                    else:
                        score = 1 - sum(a == b for a, b in zip(pat_part, modal)) / max(
                            len(pat_part), len(modal), 1
                        )
                    records.append({
                        "delegate_id": del_id,
                        name_col: grp.loc[idx, name_col] if name_col in grp.columns else del_id,
                        "pattern": pat_part,
                        "modal_pattern": modal,
                        "norm_dist": round(score, 3),
                        "year": grp.loc[idx, "j"] if "j" in grp.columns else pd.NA,
                        "row_index": idx,
                    })

        if not records:
            st.info("No pattern data available for the current selection.")
            return

        anom_df = pd.DataFrame(records).sort_values("norm_dist", ascending=False)
        if debug: print(f"  tab2 anomaly loop              {(_t.perf_counter()-_t0)*1000:8.1f} ms  records={len(records)}")

        # ── All unique patterns table ────────────────────────────────────────
        st.subheader("All unique patterns for this delegate")
        unique_pats = (
            anom_df.groupby("pattern", observed=True)
            .agg(
                count=("row_index", "count"),
                avg_dist=("norm_dist", "mean"),
                modal_pattern=("modal_pattern", "first"),
            )
            .reset_index()
            .sort_values("avg_dist", ascending=False)
        )
        unique_pats["avg_dist"] = unique_pats["avg_dist"].round(3)
        unique_pats["already_corrected"] = unique_pats["pattern"].apply(
            lambda p: int(anom_df.loc[anom_df["pattern"] == p, "row_index"].isin(corrected_indices).sum())
        )
        st.caption("Select a pattern row to filter the anomaly grid below to only those occurrences.")
        gb_unique = GridOptionsBuilder.from_dataframe(unique_pats)
        gb_unique.configure_default_column(resizable=True, sortable=True, filter=True)
        gb_unique.configure_selection(selection_mode="multiple", use_checkbox=True)
        unique_grid_options = gb_unique.build()
        sel_pat = AgGrid(
            unique_pats,
            gridOptions=unique_grid_options,
            width="100%",
            height=min(50 + 35 * len(unique_pats), 300),
            fit_columns_on_grid_load=False,
            key="pat_unique_sel",
        )
        sel_rows_pat = sel_pat.get("selected_rows", [])
        if isinstance(sel_rows_pat, pd.DataFrame):
            sel_rows_pat = sel_rows_pat.to_dict("records")
        if not isinstance(sel_rows_pat, list):
            sel_rows_pat = []
        _active_patterns = [r.get("pattern") for r in sel_rows_pat if r.get("pattern")]

        st.text_input(
            "Selected patterns",
            value=", ".join(_active_patterns),
            key="tab2_selected_patterns",
            disabled=True,
        )

        st.markdown("---")

        # ── Anomaly rows grid ────────────────────────────────────────────────
        above = anom_df[anom_df["norm_dist"] >= threshold].copy()
        # If patterns are selected in the unique-patterns table, filter to them
        if _active_patterns:
            above = anom_df[anom_df["pattern"].isin(_active_patterns)].copy()
            st.caption(f"Filtered to pattern(s) **{', '.join(_active_patterns)}** — {len(above)} row(s)")
        _n_done2 = above["row_index"].isin(corrected_indices).sum()
        if _n_done2:
            above = above[~above["row_index"].isin(corrected_indices)]
        st.metric(
            "Rows shown" if _active_patterns else "Anomalous patterns above threshold",
            len(above),
            delta=f"-{_n_done2} already corrected" if _n_done2 else None
        )

        above_top = above.head(top_n).reset_index(drop=True)
        st.caption("Click rows to select them, then bulk-reassign below.")

        # Debug banner: show disk vs in-memory corrections count
        st.markdown(
            f"**Corrections loaded:** {st.session_state.get('corrections_disk_loaded', 0)} | "
            f"**In-memory corrections:** {len(st.session_state.get('corrections', {}))}"
        )

        st.info("Pattern match filter applied; click rows for bulk action (or use select-box in header).", icon="ℹ️")

        _t0 = _t.perf_counter()

        st.caption(f"Selected patterns: {', '.join(_active_patterns)}" if _active_patterns else "No patterns selected")

        # Alive tab style: use st.dataframe row selection.
        above_top_display = above_top.copy().reset_index(drop=True)
        sel2 = st.dataframe(
            above_top_display,
            width="stretch",
            height=350,
            on_select="rerun",
            selection_mode="multi-row",
        )

        if debug: print(f"  tab2 dataframe()              {(_t.perf_counter()-_t0)*1000:8.1f} ms  rows={len(above_top_display)}")

        selected_rows2 = []
        if sel2 is not None and hasattr(sel2, "selection") and sel2.selection:
            selected_rows2 = sel2.selection.rows

        # If no explicit row selection, but patterns are active, fallback to all filtered rows.
        if not selected_rows2 and _active_patterns:
            selected_rows2 = above_top_display.index.tolist()

        # Convert selected display row indices to original row_index values
        selected_rows2 = [above_top_display.loc[i, "row_index"] for i in selected_rows2 if i is not None and 0 <= i < len(above_top_display)]

        fig2 = px.histogram(anom_df, x="norm_dist", nbins=40,
                            title="Distribution of normalised edit distances")
        fig2.add_vline(x=threshold, line_dash="dash", line_color="red")
        _t0 = _t.perf_counter()
        st.plotly_chart(fig2, width="stretch")
        if debug: print(f"  tab2 st.plotly_chart()         {(_t.perf_counter()-_t0)*1000:8.1f} ms")

        if not above.empty:
            st.subheader("Bulk reassign selected rows")
            st.caption(f"**{len(selected_rows2)}** row(s) selected")
            nid2 = st.text_input("New delegate_id (apply to all selected)", key="anom_nid")
            if st.button("💾 Save corrections", key="anom_save", disabled=not selected_rows2):
                orig_indices = selected_rows2
                for ridx in orig_indices:
                    save_correction(ridx, nid2.strip())
                st.toast(f"Saved {len(orig_indices)} correction(s): → {nid2.strip()}", icon="✅")
                st.rerun()

            # Pattern validity toggles (reversible)
            if st.button("Mark selected patterns as invalid", key="mark_invalid", disabled=not selected_rows2):
                pattern_status = load_pattern_status()
                for row_idx in selected_rows2:
                    row = above_top.loc[above_top["row_index"] == row_idx].iloc[0]
                    key = f"{row['delegate_id']}|{row['pattern']}|{row['year']}"
                    pattern_status[key] = False
                    if nid2.strip():
                        save_correction(row_idx, nid2.strip())
                save_pattern_status(pattern_status)
                st.toast("Marked selected patterns invalid and applied corrections (if delegate_id set).", icon="✅")
                st.rerun()

            if st.button("Mark selected patterns as valid", key="mark_valid", disabled=not selected_rows2):
                pattern_status = load_pattern_status()
                for row_idx in selected_rows2:
                    row = above_top.loc[above_top["row_index"] == row_idx].iloc[0]
                    key = f"{row['delegate_id']}|{row['pattern']}|{row['year']}"
                    pattern_status[key] = True
                    if nid2.strip():
                        save_correction(row_idx, nid2.strip())
                save_pattern_status(pattern_status)
                st.toast("Marked selected patterns valid and applied corrections (if delegate_id set).", icon="✅")
                st.rerun()
