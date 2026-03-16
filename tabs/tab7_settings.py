"""Tab 7 — Application settings.

This tab lets the user view and tweak persisted settings (stored in `app_config.toml`).
It is intentionally lightweight and only exposes the config values that are meaningful
in the UI (sort mode, search term, select column position, etc.).
"""

from __future__ import annotations

import streamlit as st

from utils import save_config


def render(tab) -> None:
    with tab:
        st.title("⚙️ Settings")
        st.caption("Manage persistent UI settings stored in `app_config.toml`.")

        cfg = st.session_state.get("config", {})
        tab0_cfg = cfg.setdefault("tab0", {})

        st.subheader("Overview tab")
        st.markdown("Settings here are saved immediately and applied on next tab switch.")

        sort_options = [
            "Work queue (unreviewed first)",
            "Delegate ID",
            "Name",
            "Reviewed (✅ first)",
            "Issue score (worst first)",
        ]

        sort_primary = st.selectbox(
            "Default primary sort",
            sort_options,
            index=sort_options.index(tab0_cfg.get("sort_primary", "Work queue (unreviewed first)")),
        )
        tab0_cfg["sort_primary"] = sort_primary

        sort_secondary = st.selectbox(
            "Default secondary sort",
            [o for o in sort_options if o != sort_primary],
            index=[o for o in sort_options if o != sort_primary].index(
                tab0_cfg.get("sort_secondary", "Delegate ID")
            ),
        )
        tab0_cfg["sort_secondary"] = sort_secondary

        select_col_pos = st.number_input(
            "Default select-column position (0 = first)",
            min_value=0,
            max_value=10,
            value=int(tab0_cfg.get("select_col_pos", 0)),
        )
        tab0_cfg["select_col_pos"] = int(select_col_pos)

        st.text_input(
            "Default search filter",
            value=str(tab0_cfg.get("search_term", "")),
            key="tab0_default_search",
            on_change=lambda: _on_search_changed(tab0_cfg),
        )

        st.markdown("---")
        st.button("Reset to defaults", on_click=_reset_defaults)

        st.write("**Current config (app_config.toml)**")
        st.json(cfg)

        # Persist to disk on every render (cheap, and keeps config in sync)
        st.session_state["config"] = cfg
        save_config(cfg)


def _on_search_changed(tab0_cfg: dict) -> None:
    tab0_cfg["search_term"] = st.session_state.get("tab0_default_search", "")


def _reset_defaults() -> None:
    cfg = st.session_state.get("config", {})
    cfg["tab0"] = {
        "sort_mode": "Work queue (unreviewed first)",
        "search_term": "",
        "select_col_pos": 0,
    }
    st.session_state["config"] = cfg
    save_config(cfg)
    st.experimental_rerun()
