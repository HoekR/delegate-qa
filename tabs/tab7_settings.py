"""Tab 7 — Application settings.

This tab lets the user view and tweak persisted settings (stored in `app_config.toml`).
It is intentionally lightweight and only exposes the config values that are meaningful
in the UI (sort mode, search term, select column position, etc.).
"""

from __future__ import annotations

import json

import streamlit as st

from utils import rerun, save_config


def render(tab) -> None:
    with tab:
        st.title("⚙️ Settings")
        st.caption("Manage persistent UI settings stored in `app_config.toml`.")

        cfg = st.session_state.get("config", {})
        tab0_cfg = cfg.setdefault("tab0", {})

        st.subheader("Overview tab")
        st.markdown("Settings here are saved immediately and applied on next tab switch.")

        sort_options = [
            "Issue score → unreviewed → name",
            "Work queue (unreviewed first)",
            "Delegate ID",
            "Name",
            "Reviewed (✅ first)",
            "Issue score (worst first)",
        ]
        _sort_default = "Issue score → unreviewed → name"

        sort_primary = st.selectbox(
            "Default primary sort",
            sort_options,
            index=sort_options.index(tab0_cfg.get("sort_primary", _sort_default)
                                     if tab0_cfg.get("sort_primary", _sort_default) in sort_options
                                     else _sort_default),
        )
        tab0_cfg["sort_primary"] = sort_primary

        _sec_opts = [o for o in sort_options if o != sort_primary]
        _sec_default = tab0_cfg.get("sort_secondary", "Delegate ID")
        sort_secondary = st.selectbox(
            "Default secondary sort",
            _sec_opts,
            index=_sec_opts.index(_sec_default) if _sec_default in _sec_opts else 0,
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
        st.subheader("Age plausibility thresholds")
        st.caption("Used in the Alive Check tab to flag delegates whose computed age falls outside this range.")
        alive_cfg = cfg.setdefault("alive", {})
        min_age = st.number_input(
            "Minimum age",
            min_value=0,
            max_value=100,
            value=int(alive_cfg.get("min_age", 25)),
            key="alive_min_age",
        )
        alive_cfg["min_age"] = int(min_age)
        max_age = st.number_input(
            "Maximum age",
            min_value=0,
            max_value=150,
            value=int(alive_cfg.get("max_age", 70)),
            key="alive_max_age",
        )
        alive_cfg["max_age"] = int(max_age)

        st.markdown("---")
        st.subheader("Abbrd file location")
        st.caption(
            "Override which abbrd.xlsx file is loaded. Leave blank to use the built-in search paths."
        )
        abbrd_cfg = cfg.setdefault("abbrd", {})
        abbrd_path = st.text_input(
            "Abbrd file path",
            value=str(abbrd_cfg.get("path", "")),
            key="abbrd_path",
        )
        abbrd_cfg["path"] = abbrd_path.strip()

        st.markdown("---")
        st.subheader("Abbrd sheet & columns")
        st.caption(
            "Specify which sheet and which columns in the abbrd workbook are used for lookup and enrichment."
        )
        abbrd_sheet = st.text_input(
            "Abbrd sheet name",
            value=str(abbrd_cfg.get("sheet", "lookup")),
            key="abbrd_sheet",
        )
        abbrd_cfg["sheet"] = abbrd_sheet.strip()

        abbrd_id_col = st.text_input(
            "Abbrd ID column",
            value=str(abbrd_cfg.get("id_col", "id_persoon")),
            key="abbrd_id_col",
        )
        abbrd_cfg["id_col"] = abbrd_id_col.strip()

        abbrd_name_col = st.text_input(
            "Abbrd name column",
            value=str(abbrd_cfg.get("name_col", "fullname")),
            key="abbrd_name_col",
        )
        abbrd_cfg["name_col"] = abbrd_name_col.strip()

        st.markdown("---")
        st.subheader("Abbrd field mapping")
        st.caption(
            "Map abbrd columns to the delegate/person columns used in the app. "
            "Enter a JSON object, e.g. {\"id_persoon\": \"cons_id_str\"}."
        )

        default_field_map = abbrd_cfg.get("field_map", {
            "fullname": "fullname",
            "id_persoon": "cons_id_str",
            "voornaam": "voornaam",
            "tussenvoegsel": "tussenvoegsel",
            "geslachtsnaam": "geslachtsnaam",
            "geboortejaar": "geboortejaar",
            "overlijden": "overlijdensjaar",
            "beginjaar": "minjaar",
            "eindjaar": "maxjaar",
            "hlife": "hlife",
            "provincie": "provincie",
        })

        field_map_text = st.text_area(
            "Field mapping (JSON)",
            value=json.dumps(default_field_map, indent=2, ensure_ascii=False),
            key="abbrd_field_map",
            height=240,
        )
        try:
            parsed_map = json.loads(field_map_text)
            if isinstance(parsed_map, dict):
                abbrd_cfg["field_map"] = parsed_map
            else:
                st.warning("Field map must be a JSON object (dict).")
        except Exception as e:
            st.warning(f"Invalid JSON: {e}")

        st.markdown("---")
        st.subheader("Abbrd advanced options")
        st.caption("Additional tuning options for how the abbrd lookup behaves.")

        abbrd_auto_refresh = st.checkbox(
            "Auto-refresh supplemented delegates on load",
            value=bool(abbrd_cfg.get("auto_refresh", False)),
            key="abbrd_auto_refresh",
        )
        abbrd_cfg["auto_refresh"] = bool(abbrd_auto_refresh)

        abbrd_disable_cache = st.checkbox(
            "Disable data cache (always reload from disk)",
            value=bool(abbrd_cfg.get("disable_cache", False)),
            key="abbrd_disable_cache",
        )
        abbrd_cfg["disable_cache"] = bool(abbrd_disable_cache)

        abbrd_max_preview = st.number_input(
            "Max debug preview fields",
            min_value=1,
            max_value=50,
            value=int(abbrd_cfg.get("max_preview_fields", 6)),
            key="abbrd_max_preview_fields",
        )
        abbrd_cfg["max_preview_fields"] = int(abbrd_max_preview)

        st.markdown("---")
        st.subheader("Correction format")
        corr_cfg = cfg.setdefault("corrections", {})

        corr_to_id_key = st.text_input(
            "Correction to_id key",
            value=str(corr_cfg.get("to_id_key", "to_id")),
            key="corr_to_id_key",
        )
        corr_cfg["to_id_key"] = corr_to_id_key.strip() or "to_id"

        corr_from_id_key = st.text_input(
            "Correction from_id key",
            value=str(corr_cfg.get("from_id_key", "from_id")),
            key="corr_from_id_key",
        )
        corr_cfg["from_id_key"] = corr_from_id_key.strip() or "from_id"

        corr_name_key = st.text_input(
            "Correction name key",
            value=str(corr_cfg.get("name_key", "name")),
            key="corr_name_key",
        )
        corr_cfg["name_key"] = corr_name_key.strip() or "name"

        corr_updated_at_key = st.text_input(
            "Correction updated_at key",
            value=str(corr_cfg.get("updated_at_key", "updated_at")),
            key="corr_updated_at_key",
        )
        corr_cfg["updated_at_key"] = corr_updated_at_key.strip() or "updated_at"

        corr_source_key = st.text_input(
            "Correction source key",
            value=str(corr_cfg.get("source_key", "source")),
            key="corr_source_key",
        )
        corr_cfg["source_key"] = corr_source_key.strip() or "source"

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
    cfg["alive"] = {"min_age": 25, "max_age": 70}
    cfg["tab0"] = {
        "sort_primary": "Work queue (unreviewed first)",
        "sort_secondary": "Delegate ID",
        "search_term": "",
        "select_col_pos": 0,
    }
    cfg["abbrd"] = {
        "sheet": "lookup",
        "id_col": "id_persoon",
        "name_col": "fullname",
        "max_preview_fields": 6,
        "auto_refresh": False,
        "disable_cache": False,
    }
    cfg["corrections"] = {
        "to_id_key": "to_id",
        "from_id_key": "from_id",
        "name_key": "name",
        "updated_at_key": "updated_at",
        "source_key": "source",
        "fields": ["to_id", "from_id", "name", "updated_at", "source"],
        "source_default": "manual",
        "source_legacy": "legacy",
    }
    st.session_state["config"] = cfg
    save_config(cfg)
    rerun()
