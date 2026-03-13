"""Tab 2 – Pattern Anomalies: divergent name patterns per delegate."""
from __future__ import annotations

from typing import Callable

import pandas as pd
import plotly.express as px
import streamlit as st


def render(
    tab,
    *,
    df_delegate: pd.DataFrame,
    name_col: str,
    save_correction: Callable,
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

        try:
            from rapidfuzz import distance as rfd
            has_rf = True
        except ImportError:
            has_rf = False
            st.info("Install `rapidfuzz` for better scoring: `uv pip install rapidfuzz`")

        threshold = st.slider("Anomaly threshold (0–1)", 0.0, 1.0, 0.5, 0.05, key="pat_thresh")
        top_n = st.slider("Show top N", 5, 100, 20, key="pat_topn")

        import time as _t
        _t0 = _t.perf_counter()
        records = []
        for del_id, grp in df_delegate.groupby("delegate_id", observed=True):
            pats = grp["pattern"].dropna().astype(str)
            if pats.empty:
                continue
            modal = pats.mode().iloc[0]
            for idx, pat in pats.items():
                if has_rf:
                    score = rfd.Levenshtein.normalized_distance(pat, modal)
                else:
                    score = 1 - sum(a == b for a, b in zip(pat, modal)) / max(len(pat), len(modal), 1)
                records.append({
                    "delegate_id": del_id,
                    name_col: grp.loc[idx, name_col] if name_col in grp.columns else del_id,
                    "pattern": pat,
                    "modal_pattern": modal,
                    "norm_dist": round(score, 3),
                    "year": grp.loc[idx, "j"] if "j" in grp.columns else pd.NA,
                    "row_index": idx,
                })

        if not records:
            st.info("No pattern data available for the current selection.")
            return

        anom_df = pd.DataFrame(records).sort_values("norm_dist", ascending=False)
        print(f"  tab2 anomaly loop              {(_t.perf_counter()-_t0)*1000:8.1f} ms  records={len(records)}")
        above = anom_df[anom_df["norm_dist"] >= threshold]
        st.metric("Anomalous patterns above threshold", len(above))

        above_top = above.head(top_n).reset_index(drop=True)
        st.caption("Click rows to select them, then bulk-reassign below.")
        _t0 = _t.perf_counter()
        sel2 = st.dataframe(above_top, width="stretch", height=350,
                            on_select="rerun", selection_mode="multi-row")
        print(f"  tab2 st.dataframe()            {(_t.perf_counter()-_t0)*1000:8.1f} ms  rows={len(above_top)}")
        selected_rows2 = sel2.selection.rows if sel2 and sel2.selection else []

        fig2 = px.histogram(anom_df, x="norm_dist", nbins=40,
                            title="Distribution of normalised edit distances")
        fig2.add_vline(x=threshold, line_dash="dash", line_color="red")
        _t0 = _t.perf_counter()
        st.plotly_chart(fig2, width="stretch")
        print(f"  tab2 st.plotly_chart()         {(_t.perf_counter()-_t0)*1000:8.1f} ms")

        if not above.empty:
            st.subheader("Bulk reassign selected rows")
            st.caption(f"**{len(selected_rows2)}** row(s) selected")
            nid2 = st.text_input("New delegate_id (apply to all selected)", key="anom_nid")
            if st.button("💾 Save corrections", key="anom_save", disabled=not selected_rows2):
                orig_indices = above_top.iloc[selected_rows2]["row_index"].tolist()
                for ridx in orig_indices:
                    save_correction(ridx, nid2.strip())
                st.success(f"Saved {len(orig_indices)} correction(s): → {nid2.strip()}")
