"""Tab 0 – Overview: delegate summary table + issue counts."""
from __future__ import annotations

import io
import re
from pathlib import Path

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

import os

from utils import REVIEWED_FILE, save_reviewed, save_config, save_corrections



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
        # Ensure delegate_id comparisons are string-based so reviewed IDs match
        if "delegate_id" in summary_disp.columns:
            summary_disp["delegate_id"] = summary_disp["delegate_id"].astype(str)
        _reviewed = {str(x) for x in (reviewed or set())}
        _corrected = corrected_delegate_ids or set()

        # Add a dedicated column to persist review status (checkbox).
        # This mirrors 'status' but makes it easy to toggle and persists to disk.
        summary_disp["done"] = summary_disp["delegate_id"].isin(_reviewed)
        # Status column: drives visual scanning and can be used for "work-queue" ordering
        def _status(did: str) -> str:
            did = str(did)
            if did in _reviewed:
                return "✅"
            if did in _corrected:
                return "⚠️"
            return "—"

        summary_disp["status"] = summary_disp["delegate_id"].apply(_status)

        # Persist UI settings (sort/search/select position) via app config.
        config = st.session_state.get("config", {})
        tab0_cfg = config.setdefault("tab0", {})
        tab0_cfg.setdefault("sort_primary", "Work queue (unreviewed first)")
        tab0_cfg.setdefault("sort_secondary", "Delegate ID")
        tab0_cfg.setdefault("search_term", "")
        tab0_cfg.setdefault("select_col_pos", 0)

        # Ensure widget defaults are loaded from config the first time.
        if "tab0_sort_primary" not in st.session_state:
            st.session_state["tab0_sort_primary"] = tab0_cfg["sort_primary"]
        if "tab0_sort_secondary" not in st.session_state:
            st.session_state["tab0_sort_secondary"] = tab0_cfg["sort_secondary"]
        if "tab0_search" not in st.session_state:
            st.session_state["tab0_search"] = tab0_cfg["search_term"]
        if "tab0_select_col_pos" not in st.session_state:
            st.session_state["tab0_select_col_pos"] = tab0_cfg["select_col_pos"]

        def _set_state_if_unbound(key: str, value):
            """Safely set a session_state value without overwriting an instantiated widget key."""
            if key not in st.session_state:
                st.session_state[key] = value

        def _reset_view() -> None:
            """Reset selection/search/sort to configured defaults."""
            cfg = st.session_state.get("config", {})
            tab0_cfg = cfg.get("tab0", {})
            st.session_state["sel_delegate_id"] = None
            st.session_state["tab0_search_force"] = tab0_cfg.get("search_term", "")
            _set_state_if_unbound(
                "tab0_sort_primary",
                tab0_cfg.get("sort_primary", "Work queue (unreviewed first)"),
            )
            _set_state_if_unbound(
                "tab0_sort_secondary",
                tab0_cfg.get("sort_secondary", "Delegate ID"),
            )
            _set_state_if_unbound(
                "tab0_select_col_pos",
                tab0_cfg.get("select_col_pos", 0),
            )
            # Keep config in sync (in case defaults were changed in config file)
            st.session_state["config"] = cfg
            save_config(cfg)

        # Add/select only column (used for row selection) and let user configure its position.
        # This must exist before GridOptionsBuilder.from_dataframe() so AgGrid respects the column order.
        # Keep the cell values blank so only the AgGrid selection checkbox is visible.
        summary_disp["_select"] = ""
        cols = list(summary_disp.columns)
        max_index = max(0, len(cols) - 1)
        select_pos = st.number_input(
            "Select column position (0 = first)",
            min_value=0,
            max_value=max_index,
            step=1,
            key="tab0_select_col_pos",
        )
        # Reorder columns so _select appears at the chosen index
        cols = [c for c in cols if c != "_select"]
        cols.insert(select_pos, "_select")
        summary_disp = summary_disp[cols]

        st.markdown(
            "**Tip:** The green checkbox is for marking delegates as *reviewed* (persisted to disk),\n"
            "and is separate from row selection (which controls which delegate shows in tabs 1–4)."
        )

        # Sorting mode for the grid (help understand why selection appears to 'move')
        sort_options = [
            "Work queue (unreviewed first)",
            "Delegate ID",
            "Name",
            "Reviewed (✅ first)",
            "Issue score (worst first)",
        ]

        sort_primary = st.selectbox(
            "Primary sort", sort_options, key="tab0_sort_primary",
        )
        sort_secondary = st.selectbox(
            "Secondary sort", [o for o in sort_options if o != sort_primary],
            key="tab0_sort_secondary",
        )

        def _issue_score(df: pd.DataFrame) -> pd.Series:
            cols = ["n_alive_flags", "n_name_mismatches", "max_gap_years"]
            score = pd.Series(0, index=df.index, dtype=int)
            for c in cols:
                if c in df.columns:
                    score = score.add(df[c].fillna(0).astype(int), fill_value=0)
            return score

        if sort_primary == "Work queue (unreviewed first)":
            _status_order = {"—": 0, "⚠️": 1, "✅": 2}
            summary_disp = summary_disp.sort_values(
                "status",
                key=lambda s: s.map(_status_order),
                kind="stable",
            )
        elif sort_primary == "Reviewed (✅ first)":
            _status_order = {"✅": 0, "⚠️": 1, "—": 2}
            summary_disp = summary_disp.sort_values(
                "status",
                key=lambda s: s.map(_status_order),
                kind="stable",
            )
        elif sort_primary == "Issue score (worst first)":
            summary_disp = summary_disp.assign(_issue_score=_issue_score(summary_disp))
            summary_disp = summary_disp.sort_values("_issue_score", ascending=False)
        elif sort_primary == "Delegate ID" and "delegate_id" in summary_disp.columns:
            summary_disp = summary_disp.sort_values("delegate_id")
        elif sort_primary == "Name" and name_col in summary_disp.columns:
            summary_disp = summary_disp.sort_values(name_col)

        # Secondary sort (stable sort to preserve primary ordering)
        if sort_secondary == "Delegate ID" and "delegate_id" in summary_disp.columns:
            summary_disp = summary_disp.sort_values("delegate_id", kind="stable")
        elif sort_secondary == "Name" and name_col in summary_disp.columns:
            summary_disp = summary_disp.sort_values(name_col, kind="stable")
        elif sort_secondary == "Reviewed (✅ first)":
            _status_order = {"✅": 0, "⚠️": 1, "—": 2}
            summary_disp = summary_disp.sort_values(
                "status", key=lambda s: s.map(_status_order), kind="stable"
            )
        elif sort_secondary == "Issue score (worst first)":
            if "_issue_score" not in summary_disp.columns:
                summary_disp = summary_disp.assign(_issue_score=_issue_score(summary_disp))
            summary_disp = summary_disp.sort_values("_issue_score", ascending=False)

        summary_disp = summary_disp.reset_index(drop=True)

        if sandboxed and name_col in summary_disp.columns:
            mask_sb = summary_disp["delegate_id"].isin(sandboxed)
            summary_disp.loc[mask_sb, name_col] = (
                "🔒 " + summary_disp.loc[mask_sb, name_col].astype(str)
            )
        if sandboxed:
            summary_disp["sandboxed"] = summary_disp["delegate_id"].isin(sandboxed)

        # ---- Suspicious config + search (preselect work-queue candidates) ----
        with st.expander("🔍 Suspicious delegate filter", expanded=True):
            search_col, page_col = st.columns([3, 1])

            # Persist search term in config
            cfg = st.session_state.get("config", {})
            tab0_cfg = cfg.setdefault("tab0", {})
            if "tab0_search_force" in st.session_state:
                search_term = st.session_state.pop("tab0_search_force")
                st.session_state["tab0_search"] = search_term
            else:
                search_term = st.session_state.get("tab0_search", tab0_cfg.get("search_term", ""))

            search_input = st.session_state.get("tab0_search_input", "")
            applied_search = st.session_state.get("tab0_search", "")

            search_input = search_col.text_input(
                "Filter delegates",
                value=search_input,
                placeholder="name or ID…",
                key="tab0_search_input",
                label_visibility="collapsed",
            )

            if search_col.button("Apply search", key="tab0_apply_search"):
                st.session_state["tab0_search"] = str(search_input).strip()
                # Preserve existing selected delegate to avoid auto-clearing when search changes.
                if hasattr(st, "experimental_rerun"):
                    st.experimental_rerun()
                elif hasattr(st, "rerun"):
                    st.rerun()

            if search_col.button("Clear name and selection", key="tab0_clear_name"):
                # Avoid writing widget key directly; it is controlled by the input widget.
                st.session_state["tab0_search"] = ""
                st.session_state["sel_delegate_id"] = None
                if hasattr(st, "experimental_rerun"):
                    st.experimental_rerun()
                elif hasattr(st, "rerun"):
                    st.rerun()

            # Use applied search term to filter table, not instant input
            search_term = applied_search

            # Reinforce explicit search actions with a visible button in case the column button is missed.
            if st.button("Apply delegate search", key="tab0_apply_search_fallback"):
                st.session_state["tab0_search"] = str(search_input).strip()
                # Preserve existing selected delegate on fallback apply, same as main apply path.
                if hasattr(st, "experimental_rerun"):
                    st.experimental_rerun()
                elif hasattr(st, "rerun"):
                    st.rerun()

            show_suspicious = search_col.checkbox(
                "Show only suspicious delegates", value=False, key="tab0_only_suspicious"
            )

            # Criteria for suspicious delegates
            suspicious_alive = search_col.checkbox(
                "Include alive/age flags", value=True, key="tab0_suspicious_alive"
            )
            suspicious_gaps = search_col.checkbox(
                "Include large gaps", value=True, key="tab0_suspicious_gaps"
            )
            gap_years_threshold = search_col.slider(
                "Gap years threshold", 1, 30, 5, key="tab0_suspicious_gap_thresh"
            )
            suspicious_patterns = search_col.checkbox(
                "Include diverging patterns", value=True, key="tab0_suspicious_patterns"
            )
            pattern_thresh = search_col.slider(
                "Min unique patterns", 1, 10, 4, key="tab0_suspicious_pattern_thresh"
            )
            auto_select_suspicious = search_col.checkbox(
                "Auto-select first suspicious delegate when filters change",
                value=False,
                key="tab0_auto_select_suspicious",
            )

            if search_col.button("Clear filter", key="tab0_clear_search"):
                _reset_view()
                # Button press already causes a rerun; no explicit call needed.


        if show_suspicious:
            mask_suspicious = pd.Series(False, index=summary_disp.index)
            if suspicious_alive and "n_alive_flags" in summary_disp.columns:
                mask_suspicious |= summary_disp["n_alive_flags"].fillna(0) > 0
            if suspicious_gaps and "max_gap_years" in summary_disp.columns:
                mask_suspicious |= summary_disp["max_gap_years"].fillna(0) >= gap_years_threshold
            if suspicious_patterns and "n_patterns" in summary_disp.columns:
                mask_suspicious |= summary_disp["n_patterns"].fillna(0) >= pattern_thresh

            # Always keep reviewed rows visible (even if not suspicious)
            mask_reviewed = summary_disp["status"] == "✅"
            summary_disp = summary_disp[mask_suspicious | mask_reviewed]

        _PAGE_SIZE = 200
        if search_term.strip():
            # Support wildcard tokens * and ? in search input.
            wildcard_search = "*" in search_term or "?" in search_term
            if wildcard_search:
                pattern = re.escape(search_term.strip())
                pattern = pattern.replace(r"\*", ".*").replace(r"\?", ".")
                # Regex should match anywhere in the string by default
                regex = f"{pattern}"
                if name_col in summary_disp.columns:
                    _mask = (
                        summary_disp[name_col].astype(str).str.contains(regex, case=False, na=False, regex=True)
                        | summary_disp["delegate_id"].astype(str).str.contains(regex, case=False, na=False, regex=True)
                    )
                else:
                    _mask = summary_disp["delegate_id"].astype(str).str.contains(regex, case=False, na=False, regex=True)
            else:
                if name_col in summary_disp.columns:
                    _mask = (
                        summary_disp[name_col].astype(str).str.contains(search_term, case=False, na=False)
                        | summary_disp["delegate_id"].astype(str).str.contains(search_term, case=False, na=False)
                    )
                else:
                    _mask = summary_disp["delegate_id"].astype(str).str.contains(search_term, case=False, na=False)

            summary_disp = summary_disp[_mask]
        _total = len(summary_disp)
        summary_disp = summary_disp.head(_PAGE_SIZE).reset_index(drop=True)
        if _total > _PAGE_SIZE:
            page_col.caption(f"Showing {_PAGE_SIZE} of {_total} — type a name to narrow")
        else:
            page_col.caption(f"{_total} delegate(s)")
        sel_id = st.session_state.get("sel_delegate_id")

        if sel_id:
            row_match = summary[summary["delegate_id"] == sel_id]
            label = (
                str(row_match.iloc[0][name_col])
                if not row_match.empty and name_col in row_match.columns
                else sel_id
            )
            col_info, col_clear = st.columns([6, 1])
            col_info.info(f"Selected: **{label}**")
            col_clear.button("✖ Clear", key="tab0_clear_sel", on_click=_reset_view)
        else:
            st.caption("👆 Click a row to select a delegate — tabs 1–4 will show only that person.")

        # Quick jump button for a suspicious delegate (first row in filtered view)
        if show_suspicious and _total > 0:
            if st.button("Select first suspicious delegate", key="tab0_select_first_suspicious"):
                first_id = str(summary_disp.iloc[0]["delegate_id"])
                if first_id:
                    st.session_state["sel_delegate_id"] = first_id
                    st.rerun()

            if st.session_state.get("tab0_auto_select_suspicious"):
                first_id = str(summary_disp.iloc[0]["delegate_id"])
                if first_id and st.session_state.get("sel_delegate_id") != first_id:
                    st.session_state["sel_delegate_id"] = first_id
                    st.rerun()

        # Build AgGrid options
        import time as _t
        _t0 = _t.perf_counter()
        gb = GridOptionsBuilder.from_dataframe(summary_disp)
        gb.configure_default_column(resizable=True, sortable=True, filter=True, minWidth=80)
        gb.configure_grid_options(singleClickEdit=True)
        # Configure the select checkbox column (position is controlled above).
        gb.configure_column(
            "_select",
            header_name="Select",
            editable=False,
            checkboxSelection=True,
            width=80,
        )

        # Ensure delegate_id is visible
        if "delegate_id" in summary_disp.columns:
            gb.configure_column(
                "delegate_id",
                header_name="Delegate ID",
                width=150,
                editable=False,
            )

        # Add checkbox for 'done' column (review status)
        if "done" in summary_disp.columns:
            gb.configure_column(
                "done",
                header_name="Reviewed",
                editable=True,
                type=["booleanColumn"],
                cellRenderer="agCheckboxCellRenderer",
                cellEditor="agCheckboxCellEditor",
                suppressRowClickSelection=True,
                width=90,
            )

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
        # When `sel_id` is stored as a string but the delegate_id column is numeric,
        # match by string form so pre-selection works reliably.
        sel_id_str = str(sel_id) if sel_id is not None else None
        pre_sel_idx = []
        if sel_id_str is not None:
            matches = summary_disp[summary_disp["delegate_id"].astype(str) == sel_id_str]
            if not matches.empty:
                pre_sel_idx = [int(matches.index[0])]

        gb.configure_selection(
            selection_mode="single",
            use_checkbox=False,
            pre_selected_rows=pre_sel_idx,
        )
        gb.configure_grid_options(suppressRowClickSelection=True)
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=25)
        grid_opts = gb.build()

        # Highlight the selected row with a red border for clarity.
        grid_opts["getRowStyle"] = JsCode(
            "function(params){ return params.node.isSelected() ? {border: '2px solid #d32f2f'} : {}; }"
        )

        response = AgGrid(
            summary_disp,
            gridOptions=grid_opts,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            height=440,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            key="tab0_aggrid",
        )

        # Sync the 'done' checkbox column back to the persisted reviewed set.
        # The grid sends back the full row data under 'data'.
        raw_data = response.get("data", [])
        if isinstance(raw_data, pd.DataFrame):
            raw_data = raw_data.to_dict("records")
        if raw_data is not None:
            # Only apply changes for rows currently visible in the grid.
            # The grid only sends back the current page, so we merge updates
            # rather than overwriting the whole reviewed set.
            modified = False
            for r in raw_data:
                if not isinstance(r, dict):
                    continue
                did = str(r.get("delegate_id", ""))
                done = r.get("done") in (True, "true", "True", 1, "1")
                if done and did not in reviewed:
                    reviewed.add(did)
                    modified = True
                if not done and did in reviewed:
                    reviewed.discard(did)
                    modified = True
            if modified:
                st.session_state["reviewed"] = reviewed
                save_reviewed(reviewed)

        # Resolve selected rows (list-of-dicts in 0.3.x; DataFrame in 1.x)
        raw_sel = response.get("selected_rows", [])
        if isinstance(raw_sel, pd.DataFrame):
            rows = raw_sel.to_dict("records")
        else:
            rows = list(raw_sel) if raw_sel is not None else []

        if rows:
            first_row = rows[0]
            if isinstance(first_row, dict):
                chosen_id = str(first_row.get("delegate_id", ""))
            else:
                chosen_id = str(first_row)
            if chosen_id and chosen_id != st.session_state.get("sel_delegate_id"):
                st.session_state["sel_delegate_id"] = chosen_id
                rerun_fn = None
                if hasattr(st, "experimental_rerun"):
                    rerun_fn = st.experimental_rerun
                elif hasattr(st, "rerun"):
                    rerun_fn = st.rerun

                if rerun_fn is not None:
                    try:
                        rerun_fn()
                    except Exception as e:
                        st.warning(
                            "Selected delegate updated, but auto rerun failed. "
                            "Please click another tab or refresh the browser page. "
                            f"(Detail: {type(e).__name__}: {e})"
                        )
                        st.info("Selected delegate updated; refresh tab status by clicking any tab or pressing browser refresh.")
                else:
                    st.warning(
                        "Selected delegate updated; automatic rerun is unavailable in this Streamlit version. "
                        "Please click another tab or refresh the browser page."
                    )

        # Persist UI settings to disk so they survive restarts
        cfg = st.session_state.get("config", {})
        tab0_cfg = cfg.setdefault("tab0", {})
        tab0_cfg["sort_primary"] = st.session_state.get("tab0_sort_primary")
        tab0_cfg["sort_secondary"] = st.session_state.get("tab0_sort_secondary")
        tab0_cfg["search_term"] = st.session_state.get("tab0_search")
        tab0_cfg["select_col_pos"] = st.session_state.get("tab0_select_col_pos")
        st.session_state["config"] = cfg
        save_config(cfg)

        buf_sum = io.BytesIO()
        summary.to_excel(buf_sum, index=False)
        buf_sum_bytes = buf_sum.getvalue()
        st.download_button(
            "⬇ Download summary",
            buf_sum_bytes,
            "delegate_summary.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Show when reviewed.json was last written
        if REVIEWED_FILE.exists():
            ts = os.path.getmtime(REVIEWED_FILE)
            st.caption(f"Reviewed file last saved: {pd.to_datetime(ts, unit='s')}")
        else:
            st.caption("Reviewed file not found; it will be created when you tick a box.")

        # Export a markdown report for delegates marked as done (reviewed)
        reviewed_ids = reviewed or set()
        reviewed_rows = summary[summary["delegate_id"].isin(reviewed_ids)]
        report_md = "# Reviewed delegates report\n"
        report_md += f"Generated: {pd.Timestamp.now().isoformat()}\n\n"
        report_md += f"Total reviewed: {len(reviewed_rows)}\n\n"
        if reviewed_rows.empty:
            report_md += "No reviewed delegates yet.\n"
        else:
            cols = ["delegate_id"]
            if name_col in reviewed_rows.columns:
                cols.append(name_col)
            for c in ("status", "n_alive_flags", "n_name_mismatches", "max_gap_years"):
                if c in reviewed_rows.columns:
                    cols.append(c)

            # Build a simple markdown table without requiring `tabulate`.
            def _row_to_md(row: dict) -> str:
                return "| " + " | ".join(str(row.get(c, "")) for c in cols) + " |\n"

            report_md += "| " + " | ".join(cols) + " |\n"
            report_md += "| " + " | ".join(["---"] * len(cols)) + " |\n"
            for _, row in reviewed_rows[cols].iterrows():
                report_md += _row_to_md(row)

        buf_md_bytes = report_md.encode("utf-8")
        st.download_button(
            "⬇ Download reviewed report (Markdown)",
            buf_md_bytes,
            "reviewed_delegates.md",
            "text/markdown",
        )

