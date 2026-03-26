"""Tab 4 – Timeline Gaps: gaps / late reappearances per delegate."""
from __future__ import annotations

from typing import Callable

import pandas as pd
import plotly.express as px
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
from utils import rerun, _get_corrections_config


def render(
    tab,
    *,
    df_delegate: pd.DataFrame,
    name_col: str,
    sel_delegate_id: str | None,
    DEFAULT_GAP: int,
    save_correction: Callable,
    corrected_indices: set = frozenset(),
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
            grp_nonnull = grp[grp["j"].notna()].copy()
            if grp_nonnull.empty:
                continue
            grp_nonnull["j_numeric"] = pd.to_numeric(grp_nonnull["j"], errors="coerce")
            grp_nonnull = grp_nonnull[grp_nonnull["j_numeric"].notna()].copy()
            if grp_nonnull.empty:
                continue
            grp_sorted = grp_nonnull.sort_values("j_numeric")
            years = grp_sorted["j_numeric"].astype(int).tolist()
            idxs = grp_sorted.index.tolist()
            disp_name = grp[name_col].iloc[0] if name_col in grp.columns else del_id
            for (a, ai), (b, bi) in zip(zip(years, idxs), zip(years[1:], idxs[1:])):
                gap = b - a
                if gap > gap_thresh:
                    gap_rows.append({
                        "delegate_id": del_id,
                        name_col: disp_name,
                        "gap_start": a, "gap_start_idx": ai,
                        "gap_end": b, "gap_end_idx": bi,
                        "gap_years": gap,
                    })

        if gap_rows:
            gap_df = pd.DataFrame(gap_rows).sort_values("gap_years", ascending=False)
            st.metric("Delegates with gaps", gap_df["delegate_id"].nunique())

            gi = st.selectbox("Gap delegate to inspect", sorted(gap_df["delegate_id"].unique()), key="gap_delegate_sel")
            selected_gaps = gap_df[gap_df["delegate_id"] == gi].copy()

            st.subheader("Gap candidates")
            gb = GridOptionsBuilder.from_dataframe(selected_gaps)
            gb.configure_default_column(resizable=True, sortable=True, filter=True)
            gb.configure_selection(selection_mode="multiple", use_checkbox=True)
            grid_opts = gb.build()
            grid = AgGrid(
                selected_gaps,
                gridOptions=grid_opts,
                height=min(50 + 35 * len(selected_gaps), 300),
                fit_columns_on_grid_load=False,
                key="gap_grid",
            )
            sel_rows = grid.get("selected_rows", [])
            if sel_rows is None:
                sel_rows = []
            elif isinstance(sel_rows, pd.DataFrame):
                sel_rows = sel_rows.to_dict("records")
            elif not isinstance(sel_rows, list):
                # Guard against unusual API returns
                sel_rows = []

            candidates = st.session_state.setdefault("gap_reassign_candidates", [])

            # Keep row-level candidate data for table display
            candidate_entries = st.session_state.setdefault("gap_reassign_candidate_rows", [])
            candidate_keys = {entry["key"] for entry in candidate_entries}

            # mark selected_gaps rows as candidate-status
            selected_gaps["is_candidate"] = selected_gaps.apply(
                lambda r: f"{r.get('delegate_id')}|{r.get('gap_start')}|{r.get('gap_end')}" in candidate_keys,
                axis=1,
            )

            if st.button("Mark selected gap rows as candidate for split/reassign", key="mark_gap_candidate", disabled=not bool(sel_rows)):
                for r in sel_rows:
                    key = f"{r.get('delegate_id')}|{r.get('gap_start')}|{r.get('gap_end')}"
                    if key not in candidates:
                        candidates.append(key)
                        candidate_entries.append({
                            "key": key,
                            "delegate_id": r.get("delegate_id"),
                            "gap_start": r.get("gap_start"),
                            "gap_end": r.get("gap_end"),
                            "gap_years": r.get("gap_years"),
                            "gap_start_idx": r.get("gap_start_idx"),
                            "gap_end_idx": r.get("gap_end_idx"),
                        })
                st.session_state["gap_reassign_candidates"] = candidates
                st.session_state["gap_reassign_candidate_rows"] = candidate_entries
                st.success(f"{len(sel_rows)} gap candidate(s) marked")
                rerun()

            if candidates:
                st.caption("Current gap-based reassign candidates:")

                candidate_entries = st.session_state.get("gap_reassign_candidate_rows", [])
                if candidate_entries:
                    st.subheader("Candidate row details")
                    st.dataframe(pd.DataFrame(candidate_entries), height=200)

                    if st.button("Load candidate delegate into timeline", key="load_gap_candidate"):
                        # pick the first candidate's delegate for now
                        candidate_delegate = candidate_entries[0].get("delegate_id")
                        if candidate_delegate:
                            st.session_state["pending_sel_delegate_id"] = str(candidate_delegate)
                            st.success(f"Loading delegate {candidate_delegate} for updated timeline view.")
                            rerun()
                        else:
                            st.warning("Could not determine delegate_id from candidate rows.")

                if st.button("Clear candidate markers", key="clear_gap_candidates"):
                    st.session_state["gap_reassign_candidates"] = []
                    st.session_state["gap_reassign_candidate_rows"] = []
                    captured = toggle_state_flag("gap_reassign_candidates_cleared", default=False)
                    st.success(f"Gap candidates cleared (toggled marker: {captured})")


            st.subheader("Reassign gap boundary occurrences")
            st.caption("Choose which boundary occurrences to assign for selected gaps.")

            if "gap_reassign_target" not in st.session_state:
                st.session_state["gap_reassign_target"] = "gap_end"

            reassign_target = st.radio(
                "Apply correction to:",
                options=["gap_end", "gap_start", "both"],
                key="gap_reassign_target",
                help="gap_end = late reappearance row; gap_start = pre-gap row; both = both rows",
            )

            new_delegate = st.text_input("New delegate_id", key="gap_reassign_delegate")
            if st.button("Save selected gap reassignments", key="gap_reassign_save", disabled=(not bool(sel_rows) or not bool(new_delegate.strip()))):
                selected_delegate = new_delegate.strip()
                if not selected_delegate:
                    st.warning("Please provide a new delegate_id.")
                else:
                    if reassign_target not in ("gap_end", "gap_start", "both"):
                        st.warning("Invalid target choice; choose gap_end, gap_start, or both.")
                    else:
                        applied = 0
                        applied_rows = []
                        for r in sel_rows:
                            targets = []
                            if reassign_target in ("gap_end", "both"):
                                targets.append(r.get("gap_end_idx"))
                            if reassign_target in ("gap_start", "both"):
                                targets.append(r.get("gap_start_idx"))

                            for idx in targets:
                                if idx is not None:
                                    save_correction(idx, selected_delegate)
                                    applied += 1
                                    applied_rows.append(idx)

                        if applied:
                            st.toast(f"Applied {applied} corrections to delegate_id {selected_delegate}", icon="✅")
                            affected_rows = sorted(set(applied_rows))
                            st.info(f"Affected rows: {affected_rows}")
                            st.metric("Gap-edits applied", f"{applied} updates")
                            st.write("### Applied gap correction details")
                            st.write(
                                pd.DataFrame([
                                    {
                                        "delegate_id": r.get("delegate_id"),
                                        "gap_start": r.get("gap_start"),
                                        "gap_end": r.get("gap_end"),
                                        "target": reassign_target,
                                        "new_delegate_id": selected_delegate,
                                    }
                                    for r in sel_rows
                                ])
                            )
                            st.info(
                                "Re-run after manual tab refresh to evaluate remaining gaps."
                            )
                            # Do not attempt to mutate `gap_reassign_target` here when the radio widget exists.
                            # Streamlit enforces widget state immutability after instantiation.
                        else:
                            st.warning("No valid row indexes found for selected gaps.")
                # Avoid automated rerun to prevent ScriptRunner rerun_data crash
                # statements; rely on user action to refresh tabs.


            nc_g = name_col if name_col in gap_df.columns else "delegate_id"
            fig4 = px.bar(
                gap_df.head(30), x=nc_g, y="gap_years", color="gap_years",
                title="Largest timeline gaps per delegate",
                labels={"gap_years": "Gap (years)"},
            )
            st.plotly_chart(fig4, width="stretch")

        # Debug banner: show disk vs in-memory corrections count
        st.markdown(
            f"**Corrections loaded:** {st.session_state.get('corrections_disk_loaded', 0)} | "
            f"**In-memory corrections:** {len(st.session_state.get('corrections', {}))}"
        )

        # Show a summary table of corrections applied to currently selected delegate occurrences
        corrections = st.session_state.get("corrections", {})
        if corrections:
            corr_cfg = _get_corrections_config(st.session_state.get("config", {}))
            to_id_key = corr_cfg.get("to_id_key", "to_id")
            from_id_key = corr_cfg.get("from_id_key", "from_id")
            name_key = corr_cfg.get("name_key", "name")
            updated_at_key = corr_cfg.get("updated_at_key", "updated_at")
            source_key = corr_cfg.get("source_key", "source")

            corr_rows = []
            for ridx, new_entry in corrections.items():
                if ridx in df_delegate.index:
                    row = df_delegate.loc[ridx]
                    if isinstance(new_entry, dict):
                        to_id = new_entry.get(to_id_key)
                        from_id = new_entry.get(from_id_key)
                        name = new_entry.get(name_key)
                        updated_at = new_entry.get(updated_at_key)
                        source = new_entry.get(source_key)
                    else:
                        to_id = new_entry
                        from_id = None
                        name = None
                        updated_at = None
                        source = corr_cfg.get("source_default", "manual")

                    to_id_val = str(to_id) if to_id is not None else ""
                    from_id_val = (
                        str(from_id)
                        if from_id is not None and str(from_id).strip() != ""
                        else str(row.get("delegate_id", ""))
                    )
                    name_val = (
                        str(name)
                        if name is not None and str(name).strip() != ""
                        else str(row.get(name_col, ""))
                    )
                    year_val = row.get("j", "")

                    corr_rows.append({
                        "row": ridx,
                        "delegate_id": row.get("delegate_id"),
                        name_col: row.get(name_col) if name_col in df_delegate.columns else "",
                        "pattern": row.get("pattern", "") if "pattern" in df_delegate.columns else "",
                        "year": year_val,
                        from_id_key: from_id_val,
                        name_key: name_val,
                        "new_delegate_id": to_id_val,
                        updated_at_key: updated_at,
                        source_key: source,
                    })
            if corr_rows:
                st.subheader("Corrections on this delegate")
                st.dataframe(pd.DataFrame(corr_rows), width="stretch", height=250)

                # Table of gap-region corrections (on this delegate)
                gap_corr_df = pd.DataFrame(corr_rows)
                st.subheader("Gap-region corrections")
                st.dataframe(gap_corr_df, width="stretch", height=200)
        else:
            st.caption("No corrections currently applied.")
        
        if not gap_rows:
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
