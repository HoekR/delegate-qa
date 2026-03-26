"""Tab 1 – Alive Check: biological plausibility flags."""
from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd
import plotly.express as px
import streamlit as st


def render(
    tab,
    *,
    df_delegate: pd.DataFrame,
    has_bio: bool,
    name_col: str,
    ABBRD_FILE: Path,
    MIN_AGE: int,
    MAX_AGE: int,
    save_correction: Callable,
    corrected_indices: set = frozenset(),
) -> None:
    with tab:
        st.title("🧬 Alive Check")
        st.caption("Flags occurrences where the delegate was biologically implausible.")

        if not df_delegate.empty and "delegate_id" in df_delegate.columns:
            _id = str(df_delegate["delegate_id"].iloc[0])
            _nm = str(df_delegate[name_col].iloc[0]) if name_col in df_delegate.columns else _id
            st.info(f"🔍 Inspecting: **{_nm}** — ID `{_id}`")

        if df_delegate.empty:
            st.info("👆 Select a delegate in the Overview tab or the sidebar to see results.")
            return

        if not has_bio:
            st.warning(
                f"No birth/death year data found in `abbrd.xlsx` (loaded from `{ABBRD_FILE}`).\n\n"
                "Expected column names: `birth_year`, `geboortejaar`, `geboorte`, or `born`; "
                "and `death_year`, `sterfjaar`, `overlijden`, or `died`.\n\n"
                "As a last resort, `hlife` will be used to estimate birth/death (hlife±30/40).\n\n"
                "Check the 'abbrd.xlsx columns' expander in **Tab 6** to see what columns were loaded."
            )
            return

        min_age = st.slider("Min plausible age to serve", 10, 25, MIN_AGE, key="min_age")
        max_age = st.slider("Max plausible age to serve", 70, 100, MAX_AGE, key="max_age")

        df_alive = df_delegate.copy()

        # Ensure age_at_event exists (it may be missing for delegates with no occurrences).
        if "age_at_event" not in df_alive.columns:
            df_alive["age_at_event"] = pd.NA
        df_alive["age_at_event"] = pd.to_numeric(df_alive["age_at_event"], errors="coerce")

        # Allow filtering by year so users can zoom in on specific ranges
        if "j" in df_alive.columns and not df_alive["j"].isna().all():
            year_min = int(df_alive["j"].min())
            year_max = int(df_alive["j"].max())
            year_range = st.slider(
                "Year range", year_min, year_max, (year_min, year_max), key="alive_year_range"
            )
            df_alive = df_alive[(df_alive["j"] >= year_range[0]) & (df_alive["j"] <= year_range[1])]

        df_alive["flag_young"] = df_alive["age_at_event"] < min_age
        df_alive["flag_old"]   = df_alive["age_at_event"] > max_age
        df_alive["flag_dead"]  = (
            df_alive["j"] > pd.to_numeric(df_alive["death_year"], errors="coerce")
            if "death_year" in df_alive.columns else False
        )

        has_min_year = "min_year" in df_alive.columns and df_alive["min_year"].notna().any()
        has_max_year = "max_year" in df_alive.columns and df_alive["max_year"].notna().any()
        df_alive["flag_before_active"] = (
            df_alive["j"] < pd.to_numeric(df_alive["min_year"], errors="coerce")
            if has_min_year else False
        )
        df_alive["flag_after_active"] = (
            df_alive["j"] > pd.to_numeric(df_alive["max_year"], errors="coerce")
            if has_max_year else False
        )

        flag_cols = ["flag_young", "flag_old", "flag_dead", "flag_before_active", "flag_after_active"]
        df_alive["flagged"] = df_alive[flag_cols].any(axis=1)

        flagged = df_alive[df_alive["flagged"]]
        st.metric("Flagged occurrences", len(flagged), delta=f"of {len(df_alive)} shown")

        if has_min_year or has_max_year:
            st.caption(
                "📌 Active-period bounds (`min_year` / `max_year`) loaded from abbrd — "
                "used in addition to biological age flags."
            )

        show_cols = [c for c in [
            "delegate_id", name_col, "j", "age_at_event",
            "birth_year", "death_year", "hlife", "min_year", "max_year", "pattern",
            "flag_young", "flag_old", "flag_dead", "flag_before_active", "flag_after_active",
        ] if c in df_alive.columns]
        flagged_reset1 = flagged[show_cols].reset_index(names="_orig_idx")
        _n_done1 = flagged_reset1["_orig_idx"].isin(corrected_indices).sum()
        if _n_done1:
            flagged_reset1 = flagged_reset1[~flagged_reset1["_orig_idx"].isin(corrected_indices)]
            st.caption(f"{_n_done1} already-corrected row(s) hidden. Click rows to select them, then bulk-reassign below.")
        else:
            st.caption("Click rows to select them, then bulk-reassign below.")
        sel1 = st.dataframe(flagged_reset1, width="stretch", height=300,
                            on_select="rerun", selection_mode="multi-row")
        selected_rows1 = sel1.selection.rows if sel1 and sel1.selection else []

        if not df_alive["age_at_event"].isna().all():
            fig1 = px.scatter(
                df_alive, x="j", y="age_at_event", color="flagged",
                color_discrete_map={True: "red", False: "steelblue"},
                hover_data=[c for c in [name_col, "pattern"] if c in df_alive.columns],
                title="Age at event by year",
                labels={"j": "Year", "age_at_event": "Age"},
            )
            fig1.add_hline(y=min_age, line_dash="dash", line_color="orange")
            fig1.add_hline(y=max_age, line_dash="dash", line_color="red")
            st.plotly_chart(fig1, width="stretch")

        if not flagged.empty:
            st.subheader("Bulk reassign selected rows")
            st.caption(f"**{len(selected_rows1)}** row(s) selected")
            nid1 = st.text_input("New delegate_id (apply to all selected)", key="alive_nid")
            if st.button("💾 Save corrections", key="alive_save", disabled=not selected_rows1):
                orig_indices = flagged_reset1.iloc[selected_rows1]["_orig_idx"].tolist()
                for ridx in orig_indices:
                    save_correction(ridx, nid1.strip())
                st.toast(f"Saved {len(orig_indices)} correction(s): → {nid1.strip()}", icon="✅")
                st.rerun()
