"""Tab – Pattern Suggestions: Q-K-V retrieval for unresolved occurrences.

Each occurrence with a sentinel delegate_id (< 0) is scored against a
TF-IDF key store built from all labeled rows in df_merged.  The top-3
candidates are shown with their cosine-similarity scores.

Temporal gating and province constraints (bypassed for president rows)
narrow the candidate set before ranking.  Accepting a suggestion writes
the row → delegate_id pair into the *active (RAM) corrections* layer via
save_correction() — the same path as every manual correction in other tabs.
It can be staged, approved, or reverted through the sidebar like any other
correction.  Nothing is written directly to disk from this tab.
"""
from __future__ import annotations

from typing import Callable

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

from utils import query_suggestions, load_flagged_patterns, save_flagged_patterns


def render(
    tab,
    *,
    df_unresolved: pd.DataFrame,
    df_merged: pd.DataFrame,
    suggestion_store: dict,
    save_correction: Callable,
    df_p: pd.DataFrame,
    name_col: str,
    corrected_indices: set = frozenset(),
) -> None:
    with tab:
        st.title("🔍 Suggestions")
        st.caption(
            "Candidate delegates for occurrences with an unresolved (sentinel) ID. "
            "Scores combine character n-gram and word-token TF-IDF cosine similarity, "
            "narrowed by year range and province. Accepting a suggestion adds it as "
            "an **active correction** — visible in the sidebar and fully reversible."
        )

        if not suggestion_store:
            st.warning(
                "Suggestion store could not be built. "
                "Make sure `scikit-learn` is installed (`uv add scikit-learn`)."
            )
            return

        if df_unresolved is None or df_unresolved.empty:
            st.success("🎉 No unresolved occurrences — all delegate IDs are positive.")
            return

        # Hide rows already accepted or skipped in this session, or with flagged patterns
        unresolved = df_unresolved.copy()
        skipped: set = st.session_state.setdefault("skipped_suggestions", set())
        flagged_patterns: set[str] = st.session_state.setdefault(
            "flagged_patterns", load_flagged_patterns()
        )
        unresolved = unresolved[~unresolved.index.isin(skipped)]
        unresolved = unresolved[
            ~unresolved["pattern"].astype(str).isin(flagged_patterns)
        ]

        # ── Controls ───────────────────────────────────────────────────────
        col_thresh, col_tol = st.columns(2)
        min_score = col_thresh.slider(
            "Minimum confidence score", 0.0, 1.0, 0.15, 0.05,
            key="suggest_min_score",
            help="Hide candidates whose cosine similarity is below this threshold.",
        )
        year_tol = col_tol.number_input(
            "Year tolerance (±)", min_value=0, max_value=50, value=10, step=5,
            key="suggest_year_tol",
            help="Extend each delegate's active range by this many years on each side.",
        )

        # ── Run retrieval ──────────────────────────────────────────────────
        with st.spinner("Scoring candidates…"):
            suggestions = query_suggestions(
                suggestion_store,
                unresolved,
                top_k=3,
                year_tolerance=int(year_tol),
                min_score=float(min_score),
            )

        if suggestions.empty:
            st.info("No suggestions generated. Try lowering the confidence threshold.")
            return

        # ── Build name lookup ──────────────────────────────────────────────
        _id_to_name: dict[str, str] = {}
        if not df_p.empty and "delegate_id" in df_p.columns and name_col in df_p.columns:
            for _, row in df_p.iterrows():
                _id_to_name[str(row["delegate_id"])] = str(row[name_col])

        def _label(cand_id) -> str:
            if cand_id is None or (isinstance(cand_id, float) and pd.isna(cand_id)):
                return ""
            s = str(cand_id)
            nm = _id_to_name.get(s, "")
            return f"{nm} ({s})" if nm else s

        # Build display frame
        disp = suggestions.copy()
        for rank in range(1, 4):
            disp[f"candidate_{rank}"] = disp[f"cand_{rank}"].apply(_label)
            disp[f"score_{rank}"] = disp[f"score_{rank}"].apply(
                lambda v: f"{v:.3f}" if v else ""
            )

        display_cols = ["orig_idx", "pattern", "j", "class", "namens",
                        "candidate_1", "score_1",
                        "candidate_2", "score_2",
                        "candidate_3", "score_3"]
        display_cols = [c for c in display_cols if c in disp.columns]
        disp_show = disp[display_cols].rename(columns={"orig_idx": "row"})
        disp_show = disp_show[disp_show["score_1"] != ""]  # hide rows with no candidates

        n_total = len(df_unresolved)
        n_flagged_hidden = (
            df_unresolved["pattern"].astype(str).isin(flagged_patterns).sum()
        )
        n_with_suggestion = len(disp_show)
        st.info(
            f"**{n_total}** unresolved occurrences · "
            f"**{n_with_suggestion}** have at least one candidate above the threshold · "
            f"**{len(skipped)}** skipped this session · "
            f"**{n_flagged_hidden}** hidden (invalid pattern)"
        )

        if disp_show.empty:
            st.info("No rows have any candidate above the current threshold.")
            return

        # ── AgGrid ────────────────────────────────────────────────────────
        gb = GridOptionsBuilder.from_dataframe(disp_show)
        gb.configure_default_column(resizable=True, sortable=True, filter=True, wrapText=True)
        gb.configure_selection(selection_mode="multiple", use_checkbox=True)
        gb.configure_column("row", hide=True)
        gb.configure_column("pattern", pinned="left")
        grid_opts = gb.build()

        grid_result = AgGrid(
            disp_show,
            gridOptions=grid_opts,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            height=min(80 + 35 * len(disp_show), 500),
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            key="suggest_grid",
        )

        selected = grid_result.get("selected_rows", [])
        if isinstance(selected, pd.DataFrame):
            selected = selected.to_dict("records")
        if not isinstance(selected, list):
            selected = []

        # ── Action buttons ─────────────────────────────────────────────────
        btn_accept, btn_skip, btn_flag = st.columns(3)

        if btn_accept.button(
            "✅ Accept top suggestion for selected rows",
            disabled=not selected,
            key="suggest_accept",
        ):
            n_saved = 0
            for sel_row in selected:
                # orig_idx is hidden in grid; recover from suggestions df by pattern+j match
                cand_id = None
                # find matching row in suggestions
                _pat = sel_row.get("pattern", "")
                _j = sel_row.get("j")
                # cand_1 from suggestions (not disp_show which has formatted strings)
                match = suggestions[
                    (suggestions["pattern"].astype(str) == str(_pat)) &
                    (suggestions["j"].astype(str) == str(_j))
                ]
                if not match.empty:
                    cand_id = match.iloc[0]["cand_1"]
                    orig_idx = int(match.iloc[0]["orig_idx"])
                if cand_id is not None and not (
                    isinstance(cand_id, float) and pd.isna(cand_id)
                ):
                    save_correction(orig_idx, cand_id)
                    skipped.add(orig_idx)
                    n_saved += 1
            if n_saved:
                st.success(
                    f"Added {n_saved} suggestion(s) as active corrections. "
                    "They appear in the sidebar and can be staged or reverted."
                )
                st.rerun()
            else:
                st.warning("Could not map selected rows back to suggestions — try reloading.")

        if btn_skip.button(
            "🚫 Skip selected rows (this session)",
            disabled=not selected,
            key="suggest_skip",
        ):
            for sel_row in selected:
                _pat = sel_row.get("pattern", "")
                _j = sel_row.get("j")
                match = suggestions[
                    (suggestions["pattern"].astype(str) == str(_pat)) &
                    (suggestions["j"].astype(str) == str(_j))
                ]
                if not match.empty:
                    skipped.add(int(match.iloc[0]["orig_idx"]))
            st.rerun()

        if btn_flag.button(
            "⚠️ Flag as invalid pattern",
            disabled=not selected,
            key="suggest_flag",
            help="Mark the pattern text itself as unresolvable. All occurrences sharing "
                 "this pattern will be hidden permanently (stored in flagged_patterns.json).",
        ):
            n_flagged = 0
            for sel_row in selected:
                pat_text = str(sel_row.get("pattern", "")).strip()
                if pat_text:
                    flagged_patterns.add(pat_text)
                    n_flagged += 1
            if n_flagged:
                save_flagged_patterns(flagged_patterns)
                st.success(
                    f"Flagged {n_flagged} pattern(s) as invalid. "
                    "All occurrences with those patterns are now hidden."
                )
                st.rerun()

        st.caption(
            "Skipped rows are hidden for the current browser session only. "
            "Flagged patterns are stored in **flagged_patterns.json** and persist across sessions. "
            "Neither action modifies the source data."
        )
