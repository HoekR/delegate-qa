"""Tab 4 – Timeline Gaps: gaps / late reappearances per delegate."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def render(
    tab,
    *,
    df_delegate: pd.DataFrame,
    name_col: str,
    sel_delegate_id: str | None,
    DEFAULT_GAP: int,
) -> None:
    with tab:
        st.title("⏳ Timeline Gaps")
        st.caption("Delegates with unusually long gaps between appearances or suspicious late reappearances.")

        if not df_delegate.empty and "delegate_id" in df_delegate.columns:
            _id = str(df_delegate["delegate_id"].iloc[0])
            _nm = str(df_delegate[name_col].iloc[0]) if name_col in df_delegate.columns else _id
            st.info(f"🔍 Inspecting: **{_nm}** — ID `{_id}`")

        if df_delegate.empty:
            st.info("👆 Select a delegate in the Overview tab or the sidebar to see results.")
            return

        if "j" not in df_delegate.columns or df_delegate["j"].isna().all():
            st.warning("No year (`j`) data available.")
            return

        gap_thresh = st.slider("Flag gaps larger than (years)", 3, 30, DEFAULT_GAP, key="gap_thresh")

        gap_rows = []
        for del_id, grp in df_delegate.groupby("delegate_id", observed=True):
            years = grp["j"].dropna().sort_values().astype(int).tolist()
            disp_name = grp[name_col].iloc[0] if name_col in grp.columns else del_id
            for a, b in zip(years, years[1:]):
                gap = b - a
                if gap > gap_thresh:
                    gap_rows.append({
                        "delegate_id": del_id,
                        name_col: disp_name,
                        "gap_start": a, "gap_end": b, "gap_years": gap,
                    })

        if gap_rows:
            gap_df = pd.DataFrame(gap_rows).sort_values("gap_years", ascending=False)
            st.metric("Delegates with gaps", gap_df["delegate_id"].nunique())
            st.dataframe(gap_df, width="stretch", height=300)

            nc_g = name_col if name_col in gap_df.columns else "delegate_id"
            fig4 = px.bar(
                gap_df.head(30), x=nc_g, y="gap_years", color="gap_years",
                title="Largest timeline gaps per delegate",
                labels={"gap_years": "Gap (years)"},
            )
            st.plotly_chart(fig4, width="stretch")
        else:
            st.success(f"No gaps > {gap_thresh} years in current selection.")

        # Per-delegate timeline
        if sel_delegate_id and not df_delegate.empty:
            disp = (
                str(df_delegate[name_col].iloc[0])
                if name_col in df_delegate.columns else sel_delegate_id
            )
            st.subheader(f"Timeline: {disp}")
            tl = df_delegate[["j"]].dropna().copy()
            tl["delegate"] = disp
            fig4b = px.scatter(tl, x="j", y="delegate",
                               title=f"Appearances of {disp}",
                               labels={"j": "Year", "delegate": ""})
            st.plotly_chart(fig4b, width="stretch")
