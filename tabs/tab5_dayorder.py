"""Tab 5 – Day-Order Violations: province precedence sequence checks."""
from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Callable

import pandas as pd
import plotly.express as px
import streamlit as st

from utils import build_day_order


def render(
    tab=None,
    *,
    df_merged: pd.DataFrame,
    prov_col: str | None,
    sel_provinces: tuple[str, ...],
    year_min: int,
    year_max: int,
    max_rows: int | None,
    name_col: str,
    PROVINCE_ORDER: list[str],
    PROVINCE_RANK: dict[str, int],
    PROVINCE_ORDER_FILE: Path,
    save_correction: Callable,
) -> None:
    ctx = tab if tab is not None else contextlib.nullcontext()
    with ctx:
        st.title("📅 Day-Order Violations")
        st.caption(
            "Delegates were expected to sign / appear in province precedence order. "
            "This tab detects days where the actual sequence diverges from that order."
        )

        with st.expander("🏙️ Province precedence order (edit province_order.json to change)",
                         expanded=False):
            for rank, prov in enumerate(PROVINCE_ORDER, 1):
                st.write(f"{rank}. {prov}")
            st.caption(f"Loaded from `{PROVINCE_ORDER_FILE}`")

        if prov_col is None:
            st.warning(
                "No province column found in the merged data. "
                "Expected one of: `provincie`, `provincie_p`, `province`."
            )
            return

        df5 = build_day_order(
            df_merged, prov_col, sel_provinces, year_min, year_max, max_rows, PROVINCE_RANK
        )
        if df5.empty:
            st.warning("No data in current filter selection.")
            return
        violations5 = df5[df5["pos_diff"] > 0].copy()

        n_days = df5["_day"].nunique()
        n_viol_days = violations5["_day"].nunique()
        st.metric("Days with order violations", n_viol_days, delta=f"of {n_days} meeting days")
        st.metric("Total occurrence violations", len(violations5))

        if violations5.empty:
            st.success("No province-order violations in the current selection.")
            return

        thresh5 = st.slider("Show only violations with position diff ≥", 1, 20, 1, key="ord_thresh")
        show5 = violations5[violations5["pos_diff"] >= thresh5]

        disp_cols = [c for c in [
            "index", "_day", "delegate_id", name_col,
            prov_col, "prov_rank", "actual_pos", "expected_pos", "pos_diff", "pattern",
        ] if c in show5.columns]
        show5_display = (
            show5[disp_cols]
            .rename(columns={"index": "orig_row", "_day": "day"})
            .sort_values("pos_diff", ascending=False)
            .reset_index(drop=True)
        )
        st.caption("Click rows to select them for bulk reassign, or pick one below to inspect its full day roster.")
        sel5 = st.dataframe(show5_display, width="stretch", height=350,
                            on_select="rerun", selection_mode="multi-row")
        selected_rows5 = sel5.selection.rows if sel5 and sel5.selection else []

        # Day-roster drill-down
        st.subheader("🔍 Full day roster for a suspicious occurrence")
        _drill_options = list(range(len(show5_display)))
        if _drill_options:
            def _viol_label(i: int) -> str:
                try:
                    row_s  = show5_display.iloc[i]
                    day_s  = row_s.get("day", "?")
                    name_s = row_s.get(name_col, row_s.get("delegate_id", "?"))
                    prov_s = row_s.get(prov_col, "?")
                    diff_s = int(row_s.get("pos_diff", 0))
                    return f"{day_s} | {name_s} ({prov_s}) diff={diff_s}"
                except Exception:
                    return str(i)

            drill_row = st.selectbox(
                "Inspect full day roster for row:",
                _drill_options,
                format_func=_viol_label,
                key="ord_drill",
            )
            _sel_day = show5_display.iloc[drill_row].get("day")
            if _sel_day:
                day_roster = df5[df5["_day"] == _sel_day].copy().sort_values("actual_pos")
                roster_cols = [c for c in [
                    "actual_pos", "expected_pos", "pos_diff",
                    "delegate_id", name_col, prov_col, "prov_rank", "pattern",
                ] if c in day_roster.columns]
                st.caption(f"Full roster for **{_sel_day}** — {len(day_roster)} delegates")

                def _highlight_suspicious(row):
                    if row.get("pos_diff", 0) > 0:
                        return ["background-color: #fff3cd"] * len(row)
                    return [""] * len(row)

                styled = (
                    day_roster[roster_cols]
                    .rename(columns={"_day": "day"})
                    .style.apply(_highlight_suspicious, axis=1)
                )
                st.dataframe(styled, width="stretch",
                             height=min(50 + 35 * len(day_roster), 600))

        # Province-level breakdown
        breakdown5 = (
            violations5.groupby(prov_col, observed=True)
            .agg(n_violations=("pos_diff", "count"), mean_diff=("pos_diff", "mean"))
            .reset_index()
            .sort_values("n_violations", ascending=False)
        )
        fig5 = px.bar(
            breakdown5, x=prov_col, y="n_violations",
            color="mean_diff", color_continuous_scale="Reds",
            title="Violations by province",
            labels={prov_col: "Province", "n_violations": "# violations", "mean_diff": "mean pos diff"},
        )
        st.plotly_chart(fig5, width="stretch")

        # Bulk reassign
        st.subheader("Bulk reassign selected rows")
        st.caption(f"**{len(selected_rows5)}** row(s) selected from the violations table above.")
        nid5 = st.text_input("New delegate_id (apply to all selected)", key="ord_nid")
        if st.button("💾 Save corrections", key="ord_save", disabled=not selected_rows5):
            orig_col = "orig_row" if "orig_row" in show5_display.columns else None
            if orig_col:
                orig_indices = show5_display.iloc[selected_rows5][orig_col].tolist()
            else:
                orig_indices = show5_display.iloc[selected_rows5].index.tolist()
            for ridx in orig_indices:
                save_correction(ridx, nid5.strip())
            st.success(f"Saved {len(orig_indices)} correction(s): → {nid5.strip()}")
