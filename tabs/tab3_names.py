"""Tab 3 – Name-form Mismatch: pattern vs geslachtsnaam."""
from __future__ import annotations

from typing import Callable

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st


def render(
    tab,
    *,
    df_delegate: pd.DataFrame,
    has_surname_data: bool,
    df_p: pd.DataFrame,
    name_col: str,
    save_correction: Callable,
    corrected_indices: set = frozenset(),
    debug: bool = False,
) -> None:
    with tab:
        st.title("📛 Name-form Mismatch")
        st.caption("Checks whether the occurrence pattern contains the delegate's surname (geslachtsnaam).")

        if not df_delegate.empty and "delegate_id" in df_delegate.columns:
            _id = str(df_delegate["delegate_id"].iloc[0])
            _nm = str(df_delegate[name_col].iloc[0]) if name_col in df_delegate.columns else _id
            st.info(f"🔍 Inspecting: **{_nm}** — ID `{_id}`")

        if df_delegate.empty:
            st.info("👆 Select a delegate in the Overview tab or the sidebar to see results.")
            return

        if not has_surname_data:
            st.warning("No surname data — ensure `fullname` (or `naam`) column exists in the persons file.")
            return
        if "pattern" not in df_delegate.columns:
            st.warning("No `pattern` column in occurrences file.")
            return

        # name_mismatch is precomputed per delegate in build_merged — no need
        # to recompute it here.  Fall back to inline check only if missing.
        if "name_mismatch" in df_delegate.columns:
            mismatch = df_delegate.loc[df_delegate["name_mismatch"].astype(bool)]
        else:
            _gn = df_delegate["geslachtsnaam"].astype(str).replace({"nan": ""})
            _pl = df_delegate["pattern"].astype(str).str.lower().replace({"nan": ""})
            _gn_arr, _pl_arr = _gn.to_numpy(dtype=str), _pl.to_numpy(dtype=str)
            mismatch = df_delegate.loc[
                (_gn.str.len() > 0) & (np.char.find(_pl_arr, _gn_arr) < 0)
            ]

        st.metric("Pattern–surname mismatches", len(mismatch), delta=f"of {len(df_delegate)} shown")

        import time as _t
        show3 = [c for c in ["delegate_id", name_col, "geslachtsnaam", "pattern", "j"]
                 if c in mismatch.columns]
        # Keep the original df_merged index as a column so Save can recover it.
        mismatch_display = mismatch[show3].reset_index(names="_orig_idx")
        _n_done3 = mismatch_display["_orig_idx"].isin(corrected_indices).sum()
        if _n_done3:
            mismatch_display = mismatch_display[~mismatch_display["_orig_idx"].isin(corrected_indices)]
        _cap3 = 500
        _total3 = len(mismatch_display)
        _done_note = f", {_n_done3} already corrected hidden" if _n_done3 else ""
        if _total3 > _cap3:
            st.caption(f"Showing first {_cap3} of {_total3} remaining mismatches{_done_note} — use the save button to fix them in bulk.")
            mismatch_display = mismatch_display.head(_cap3)
        elif _n_done3:
            st.caption(f"{_n_done3} already-corrected row(s) hidden.")
        st.caption("Click rows to select them, then bulk-reassign below.")
        _t0 = _t.perf_counter()
        sel3 = st.dataframe(mismatch_display, width="stretch", height=350,
                            on_select="rerun", selection_mode="multi-row")
        if debug: print(f"  tab3 st.dataframe()            {(_t.perf_counter()-_t0)*1000:8.1f} ms  rows={len(mismatch_display)}")
        selected_rows3 = sel3.selection.rows if sel3 and sel3.selection else []

        if not mismatch.empty:
            _t0 = _t.perf_counter()
            breakdown = (
                mismatch.groupby("delegate_id", observed=True)
                .agg(n_mismatches=("name_mismatch", "sum") if "name_mismatch" in mismatch.columns else ("j", "count"))
                .reset_index()
            )
            if name_col in df_p.columns:
                breakdown = breakdown.merge(
                    df_p[["delegate_id", name_col]].drop_duplicates(),
                    on="delegate_id", how="left"
                )
            nc_b = name_col if name_col in breakdown.columns else "delegate_id"
            if debug: print(f"  tab3 breakdown groupby         {(_t.perf_counter()-_t0)*1000:8.1f} ms")
            fig3 = px.bar(
                breakdown.sort_values("n_mismatches", ascending=False).head(30),
                x=nc_b, y="n_mismatches",
                title="Pattern–surname mismatches per delegate",
            )
            _t0 = _t.perf_counter()
            st.plotly_chart(fig3, width="stretch")
            if debug: print(f"  tab3 st.plotly_chart()         {(_t.perf_counter()-_t0)*1000:8.1f} ms")

            st.subheader("Bulk reassign selected rows")
            st.caption(f"**{len(selected_rows3)}** row(s) selected")
            nid3 = st.text_input("New delegate_id (apply to all selected)", key="nm_nid")
            if st.button("💾 Save corrections", key="nm_save", disabled=not selected_rows3):
                orig_indices = mismatch_display.loc[selected_rows3, "_orig_idx"].tolist()
                for ridx in orig_indices:
                    save_correction(ridx, nid3.strip())
                st.toast(f"Saved {len(orig_indices)} correction(s): → {nid3.strip()}", icon="✅")
                st.rerun()
