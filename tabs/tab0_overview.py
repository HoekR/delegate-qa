"""Tab 0 – Overview: delegate summary table + issue counts."""
from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode


def render(
    tab,
    *,
    summary: pd.DataFrame,
    df_p: pd.DataFrame,
    n_occurrences: int,
    merged_columns: list,
    df_bio,
    load_error: str | None,
    corrections: dict,
    name_col: str,
    PERSONS_FILE: Path,
    OCCURRENCES_FILE: Path,
    ABBRD_FILE: Path,
    n_placeholder_rows: int,
    n_remapped_rows: int,
    n_enriched_persons: int,
    sandboxed: set[str],
    reviewed: set[str] | None = None,
    corrected_delegate_ids: set[str] | None = None,
) -> None:
    with tab:
        st.title("👥 Delegate QA — Overview")

        if load_error:
            st.error(f"Could not load data:\n\n{load_error}")
            st.stop()

        st.caption(
            f"Persons: **{len(df_p)}** | Occurrences: **{n_occurrences}** | "
            f"Placeholders excluded: **{n_placeholder_rows}** | "
            f"Bulk-remapped: **{n_remapped_rows}** | "
            f"Auto-enriched from abbrd: **{n_enriched_persons}** | "
            f"Bio data: {'✅' if df_bio is not None else '❌ not found'} | "
            f"Corrections on disk: **{len(corrections)}**"
        )

        with st.expander("Column names & resolved file paths"):
            st.caption(
                f"**Persons:** `{PERSONS_FILE}`  \n"
                f"**Occurrences:** `{OCCURRENCES_FILE}`  \n"
                f"**abbrd:** `{ABBRD_FILE}`"
            )
            c1, c2 = st.columns(2)
            c1.write("**Persons**"); c1.write(list(df_p.columns))
            c2.write("**Occurrences**"); c2.write(merged_columns)

        if n_occurrences == 0:
            st.warning("No data after merging. Ensure `delegate_id` exists in both files.")
            st.stop()

        # ------------------------------------------------------------------ #
        # Delegate summary grid (AgGrid)                                       #
        # Row-click persists across reruns and keeps the highlight.           #
        # Selection is written to session_state["sel_delegate_id"] so all     #
        # other tabs update on the next interaction.                          #
        # ------------------------------------------------------------------ #
        st.subheader("Delegate summary")

        # Mark sandboxed rows in a display copy — original summary is untouched
        summary_disp = summary.copy()
        _reviewed = reviewed or set()
        _corrected = corrected_delegate_ids or set()
        # Status column: drives sort order and visual scanning
        def _status(did: str) -> str:
            did = str(did)
            if did in _reviewed:
                return "✅"
            if did in _corrected:
                return "⚠️"
            return "—"
        summary_disp["status"] = summary_disp["delegate_id"].apply(_status)
        # Show unreviewed first (most useful for a work queue)
        _status_order = {"—": 0, "⚠️": 1, "✅": 2}
        summary_disp = summary_disp.sort_values(
            "status", key=lambda s: s.map(_status_order), kind="stable"
        ).reset_index(drop=True)
        if sandboxed and name_col in summary_disp.columns:
            mask_sb = summary_disp["delegate_id"].isin(sandboxed)
            summary_disp.loc[mask_sb, name_col] = (
                "🔒 " + summary_disp.loc[mask_sb, name_col].astype(str)
            )
        if sandboxed:
            summary_disp["sandboxed"] = summary_disp["delegate_id"].isin(sandboxed)

        # ---- Python-side search: only send matching rows to the browser ----
        search_col, page_col = st.columns([3, 1])
        search_term = search_col.text_input(
            "🔍 Filter delegates", value="", placeholder="name or ID…",
            key="tab0_search", label_visibility="collapsed",
        )
        _PAGE_SIZE = 200
        if search_term.strip():
            _mask = (
                summary_disp[name_col].astype(str).str.contains(search_term, case=False, na=False)
                | summary_disp["delegate_id"].astype(str).str.contains(search_term, case=False, na=False)
            ) if name_col in summary_disp.columns else (
                summary_disp["delegate_id"].astype(str).str.contains(search_term, case=False, na=False)
            )
            summary_disp = summary_disp[_mask]
        _total = len(summary_disp)
        summary_disp = summary_disp.head(_PAGE_SIZE).reset_index(drop=True)
        if _total > _PAGE_SIZE:
            page_col.caption(f"Showing {_PAGE_SIZE} of {_total} — type a name to narrow")
        else:
            page_col.caption(f"{_total} delegate(s)")
        sel_id = st.session_state.get("sel_delegate_id")
        def _clear_selection() -> None:
            """on_click callback — runs before the next re-render, safe to
            write to widget-bound session_state keys here."""
            st.session_state["sel_delegate_id"] = None
            st.session_state["sidebar_delegate_name"] = "(none)"

        if sel_id:
            row_match = summary[summary["delegate_id"] == sel_id]
            label = (
                str(row_match.iloc[0][name_col])
                if not row_match.empty and name_col in row_match.columns
                else sel_id
            )
            col_info, col_clear = st.columns([6, 1])
            col_info.info(f"Selected: **{label}**")
            col_clear.button("✖ Clear", key="tab0_clear_sel", on_click=_clear_selection)
        else:
            st.caption("👆 Click a row to select a delegate — tabs 1–4 will show only that person.")

        # Build AgGrid options
        import time as _t
        _t0 = _t.perf_counter()
        gb = GridOptionsBuilder.from_dataframe(summary_disp)
        gb.configure_default_column(resizable=True, sortable=True, filter=True, minWidth=80)
        # Issue-count columns: narrow, right-aligned, highlighted header
        for _col, _header in (
            ("n_alive_flags",     "⚠ Alive"),
            ("n_name_mismatches", "⚠ Name"),
            ("max_gap_years",     "⚠ Gap yr"),
        ):
            if _col in summary_disp.columns:
                gb.configure_column(
                    _col,
                    header_name=_header,
                    width=90,
                    type=["numericColumn"],
                    filter="agNumberColumnFilter",
                )
        gb.configure_selection(
            selection_mode="single",
            use_checkbox=False,
            pre_selected_rows=(
                [int(summary_disp.index[summary_disp["delegate_id"] == sel_id][0])]
                if sel_id is not None and sel_id in summary_disp["delegate_id"].values
                else []
            ),
        )
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=25)
        grid_opts = gb.build()

        print(f"  tab0 GridOptionsBuilder        {(_t.perf_counter()-_t0)*1000:8.1f} ms  rows={len(summary_disp)}")
        _t0 = _t.perf_counter()
        response = AgGrid(
            summary_disp,
            gridOptions=grid_opts,
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            height=440,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=False,
            key="tab0_aggrid",
        )
        print(f"  tab0 AgGrid()                  {(_t.perf_counter()-_t0)*1000:8.1f} ms")

        # Resolve selected rows (list-of-dicts in 0.3.x; DataFrame in 1.x)
        raw_sel = response.get("selected_rows", [])
        if isinstance(raw_sel, pd.DataFrame):
            rows = raw_sel.to_dict("records")
        else:
            rows = list(raw_sel) if raw_sel is not None else []

        if rows:
            chosen_id = str(rows[0].get("delegate_id", ""))
            if chosen_id and chosen_id != st.session_state.get("sel_delegate_id"):
                st.session_state["sel_delegate_id"] = chosen_id
                # Rerun so df_delegate (computed at sheet.py top) picks up the
                # new id before tabs 1-4 render.  The != guard prevents loops.
                # NOTE: do NOT write to sidebar_delegate_name here — it's a
                # widget-bound key and Streamlit forbids post-render writes.
                st.rerun()

        buf_sum = io.BytesIO()
        summary.to_excel(buf_sum, index=False)
        buf_sum.seek(0)
        st.download_button(
            "⬇ Download summary", buf_sum, "delegate_summary.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

