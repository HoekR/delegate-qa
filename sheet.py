"""
Delegate QA Streamlit App — entry point
========================================
Page config · data loading · sidebar · tab dispatch.
All logic lives in utils.py and tabs/tab*.py.

Run:
    source .venv/bin/activate
    streamlit run sheet.py
"""
from __future__ import annotations

import datetime
import io
import os

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

from utils import (
    ABBRD_CANDIDATES,
    ABBRD_FILE,
    DEFAULT_GAP,
    MAX_AGE,
    MIN_AGE,
    OCCURRENCES_FILE,
    PERSONS_FILE,
    PROVINCE_ORDER,
    PROVINCE_ORDER_FILE,
    PROVINCE_RANK,
    apply_corrections,
    apply_delegate_edits,
    build_merged,
    build_name_to_id,
    build_sidebar_options,
    build_suggestion_store,
    enrich_persons_from_abbrd,
    get_delegate_slice,
    load_config,
    _compute_delegate_summary,
    load_corrections,
    load_staged_corrections,
    load_data,
    load_delegate_edits,
    load_new_delegates,
    load_remappings,
    load_reviewed,
    load_sandboxed,
    load_approved_corrections,
    load_pattern_status,
    normalize_config,
    rerun,
    save_config,
    save_corrections,
    save_staged_corrections,
    save_approved_corrections,
    save_delegate_edits,
    save_reviewed,
    save_pattern_status,
    source_mtimes,
    make_correction_entry,
)
from tabs import (
    tab0_overview,
    tab1_alive,
    tab2_patterns,
    tab3_names,
    tab4_timeline,
    tab7_settings,
    tab8_delegates,
    tab9_merges,
    tab_suggest,
)
# ---------------------------------------------------------------------------
# DEBUG FLAG  — set env var to enable timing output in the terminal
#   DELEGATE_QA_DEBUG=1 streamlit run sheet.py
# ---------------------------------------------------------------------------
DEBUG: bool = os.getenv("DELEGATE_QA_DEBUG", "0") == "1"
TAB8_LEGACY_RERUN: bool = os.getenv("TAB8_LEGACY_RERUN", "0") == "1"
if "debug_history" not in st.session_state:
    st.session_state["debug_history"] = []

# ---------------------------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------------------------

st.set_page_config(layout="wide", page_title="Delegate QA", page_icon="📜")

# ---------------------------------------------------------------------------
# SESSION STATE
# ---------------------------------------------------------------------------

if "corrections" not in st.session_state:
    cfg = load_config()
    st.session_state["corrections"] = load_corrections(config=cfg)
    st.session_state["corrections_disk_loaded"] = len(st.session_state["corrections"])
if "staged_corrections" not in st.session_state:
    st.session_state["staged_corrections"] = load_staged_corrections()
if "approved_corrections" not in st.session_state:
    st.session_state["approved_corrections"] = load_approved_corrections()
if "delegate_edits" not in st.session_state:
    st.session_state["delegate_edits"] = load_delegate_edits()
if "new_delegates" not in st.session_state:
    st.session_state["new_delegates"] = load_new_delegates()
if "remappings" not in st.session_state:
    st.session_state["remappings"] = load_remappings()
if "sel_delegate_id" not in st.session_state:
    st.session_state["sel_delegate_id"] = None
if "sidebar_delegate_name" not in st.session_state:
    st.session_state["sidebar_delegate_name"] = "(none)"
if "sandboxed" not in st.session_state:
    st.session_state["sandboxed"] = load_sandboxed()
if "reviewed" not in st.session_state:
    st.session_state["reviewed"] = load_reviewed()

# Application settings (persisted to disk)
if "config" not in st.session_state:
    cfg = load_config(
        {
            "tab0": {
                "sort_mode": "Work queue (unreviewed first)",
                "sort_primary": "Work queue (unreviewed first)",
                "sort_secondary": "Delegate ID",
                "search_term": "",
                "select_col_pos": 0,
            },
            "alive": {
                "min_age": 25,
                "max_age": 70,
            },
            "abbrd": {
                "sheet": "lookup",
                "id_col": "id_persoon",
                "name_col": "fullname",
                "max_preview_fields": 6,
                "auto_refresh": False,
                "disable_cache": False,
                "field_map": {
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
                },
            },
        }
    )
    st.session_state["config"] = normalize_config(cfg)

corrections:    dict       = st.session_state["corrections"]
new_delegates:  list[dict] = st.session_state["new_delegates"]
reviewed:       set[str]   = st.session_state["reviewed"]

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def save_correction(row_idx: int, new_id: int | str) -> None:
    """new_id may be an integer delegate_id or a string like 'republic_add_05'."""
    val = int(new_id) if str(new_id).lstrip("-").isdigit() else str(new_id)
    corrections_local = load_corrections() if not st.session_state.get("corrections") else st.session_state["corrections"]

    from_id = None
    name_value = None
    if ("df_merged" in globals()) and (row_idx in globals()["df_merged"].index):
        try:
            from_id = str(globals()["df_merged"].at[row_idx, "delegate_id"])
        except Exception:
            from_id = None
        if "name_col" in globals() and globals()["name_col"] in globals()["df_merged"].columns:
            try:
                name_value = str(globals()["df_merged"].at[row_idx, globals()["name_col"]])
            except Exception:
                name_value = None

    cfg = st.session_state.get("config", {})
    entry = make_correction_entry(
        to_id=val,
        from_id=from_id,
        name=name_value,
        source="manual",
        config=cfg,
    )

    corrections_local[row_idx] = entry
    st.session_state["corrections"] = corrections_local
    st.session_state["corrections_disk_loaded"] = len(corrections_local)
    save_corrections(corrections_local, config=cfg)

# ---------------------------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------------------------

import time as _time

if "startup_logged" not in st.session_state:
    def _fmt(p: "Path") -> str:
        return f"{p}  [{'EXISTS' if p.exists() else 'MISSING'}]"
    print("=" * 60)
    print("Delegate QA — startup")
    print(f"  persons     : {_fmt(PERSONS_FILE)}")
    print(f"  occurrences : {_fmt(OCCURRENCES_FILE)}")
    print(f"  abbrd       : {_fmt(ABBRD_FILE)}")
    print("=" * 60)
    st.session_state["startup_logged"] = True

def _timed(name, fn):
    if not DEBUG:
        return fn()
    _t0 = _time.perf_counter()
    result = fn()
    print(f"  {name:<32} {(_time.perf_counter()-_t0)*1000:8.1f} ms")
    return result

if DEBUG:
    print(f"--- rerun {_time.strftime('%H:%M:%S')} sel={st.session_state.get('sel_delegate_id')} active_tab={st.session_state.get('active_tab')!r} ---")

try:
    df_p, df_i, df_abbrd = _timed("load_data()", lambda: load_data(source_mtimes()))
    df_p, n_enriched_persons = _timed("enrich_persons_from_abbrd()", lambda: enrich_persons_from_abbrd(df_p, df_abbrd))
    for _df in (df_p, df_i):
        if "delegate_id" in _df.columns:
            _df["delegate_id"] = _df["delegate_id"].astype(str)

    # Apply any staged edits to the person dataset so all tabs reflect them.
    df_p = apply_delegate_edits(df_p, st.session_state.get("delegate_edits", {}))

    df_bio = df_abbrd
    name_col = next(
        (c for c in ("fullname", "full_name", "naam", "name") if c in df_p.columns),
        df_p.columns[0] if not df_p.empty else "fullname",
    )

    # Apply RAM corrections to df_i BEFORE build_merged filters sentinel rows.
    # This ensures occurrences where a suggestion was accepted no longer appear
    # as unresolved on the next rerun (they get a positive delegate_id and flow
    # through the normal merge path).
    _corrections = st.session_state.get("corrections", {})
    _cfg = st.session_state.get("config", {})
    if _corrections:
        df_i = apply_corrections(df_i, _corrections, config=_cfg)

    # Capture unresolved rows (sentinel IDs -1/-20) before build_merged removes them.
    _sentinel_mask = df_i["delegate_id"].astype(str).isin({"-1", "-20"})
    df_unresolved = df_i[_sentinel_mask].copy()

    df_merged, n_placeholder_rows, n_remapped_rows, summary = _timed(
        "build_merged()",
        lambda: build_merged(
            df_p, df_i, df_abbrd, new_delegates,
            remappings=st.session_state["remappings"],
            name_col=name_col,
        ),
    )
    # Apply pending corrections to all derived views so tabs reflect changes immediately.
    if _corrections:
        df_merged = apply_corrections(df_merged, _corrections, config=_cfg)
        # Recompute summary from corrected merged rows (do not apply corrections by summary index)
        summary = _compute_delegate_summary(df_merged, df_p, name_col)
    load_error: str | None = None
except Exception as exc:
    load_error = str(exc)
    df_p = df_i = df_merged = df_unresolved = pd.DataFrame()
    df_bio = None
    df_abbrd = None
    n_enriched_persons = n_placeholder_rows = n_remapped_rows = 0
    summary = pd.DataFrame()
    name_col = "fullname"

prov_col = next(
    (c for c in ("provincie_p", "provincie", "province") if c in df_merged.columns), None
)

# Pre-compute sidebar options — result is cached by build_sidebar_options
_delegate_options, *_ = _timed(
    "build_sidebar_options()",
    lambda: build_sidebar_options(df_p, df_merged, name_col, prov_col),
)

# Pre-compute suggestion store — cached; rebuilds only when df_merged changes
suggestion_store = _timed(
    "build_suggestion_store()",
    lambda: build_suggestion_store(df_merged),
)

# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------

st.sidebar.title("📜 Delegate QA")

if load_error:
    st.sidebar.error(f"Load error:\n{load_error}")

# Pre-built name → id dict: O(1) lookup in on_change callback instead of df_p scan
_name_to_id: dict[str, str] = build_name_to_id(df_p, name_col)

# Keep the sidebar selector in sync with any existing sel_delegate_id.
# This is done before the widget is created to avoid Streamlit API errors.
_sel_id = st.session_state.get("sel_delegate_id")
if _sel_id:
    # reverse lookup from ID → name (if available)
    _id_to_name = {v: k for k, v in _name_to_id.items()}
    if _sel_id in _id_to_name:
        st.session_state["sidebar_delegate_name"] = _id_to_name[_sel_id]
else:
    st.session_state["sidebar_delegate_name"] = "(none)"


def _on_sidebar_delegate_change() -> None:
    """Sync sidebar name selectbox → sel_delegate_id in session state."""
    name = st.session_state.get("sidebar_delegate_name", "(none)")
    if name == "(none)":
        st.session_state["sel_delegate_id"] = None
    else:
        st.session_state["sel_delegate_id"] = _name_to_id.get(name)

st.sidebar.selectbox(
    "Inspect delegate",
    ["(none)"] + _delegate_options,
    key="sidebar_delegate_name",
    on_change=_on_sidebar_delegate_change,
    help="Or click a row in the Overview tab.",
)

# Corrections panel
with st.sidebar:
    st.markdown("---")
    if st.button("🔄 Reload all tables", help="Clear caches and rerun to refresh all tables."):
        st.cache_data.clear()
        # A button press triggers a rerun automatically.

    # ── Task progress ──────────────────────────────────────────────────────
    with st.expander("📊 Task progress", expanded=True):
        _n_active   = len(st.session_state.get("corrections", {}))
        _n_staged   = len(st.session_state.get("staged_corrections", {}))
        _n_approved = len(st.session_state.get("approved_corrections", {}))
        _n_reviewed = len(st.session_state.get("reviewed", []))
        _n_total    = len(df_merged) if not df_merged.empty else 0
        _n_done     = _n_staged + _n_approved
        _pct        = round(100 * _n_done / _n_total, 1) if _n_total else 0.0

        _pc1, _pc2 = st.columns(2)
        _pc1.metric("🖊 Active (RAM)", _n_active)
        _pc2.metric("⏳ Staged", _n_staged)
        _pc1.metric("✅ Approved", _n_approved)
        _pc2.metric("👁 Reviewed delegates", _n_reviewed)
        st.progress(
            min(_pct / 100, 1.0),
            text=f"{_pct}% corrected — {_n_done:,} / {_n_total:,} occurrences",
        )

    st.subheader("Corrections workflow")
    st.caption("Active corrections influence all tabs; staged corrections are saved separately and can be applied manually.")
    if st.session_state.get("debug_last_action"):
        st.info(f"Debug action: {st.session_state.get('debug_last_action')}")
    if DEBUG and st.session_state.get("debug_history"):
        st.markdown("### Debug history (last 20)")
        for entry in st.session_state["debug_history"][-20:]:
            st.write(f"- {entry}")
    # st.caption(f"active_tab in session: {st.session_state.get('active_tab')!r}")

    current_corr = st.session_state.get("corrections", {})
    disk_loaded = st.session_state.get("corrections_disk_loaded", 0)

    st.markdown(
        f"**Corr in RAM:** {len(current_corr)}  |  **Corr on disk (last read):** {disk_loaded}"
    )
    st.markdown(
        f"`corrections.json` currently has {len(load_corrections())} entries (refresh on disk load)."
    )
    if current_corr:
        corr_rows = []
        for ridx, corr_val in current_corr.items():
            row_data = {
                "row": ridx,
                "from_id": "",
                "name": "",
                "pattern": "",
                "to_id": "",
            }
            # Prefer the explicit correction objects (enriched dicts)
            if isinstance(corr_val, dict):
                row_data["to_id"] = str(corr_val.get("to_id", ""))
                row_data["from_id"] = str(corr_val.get("from_id", ""))
                row_data["name"] = str(corr_val.get("name", ""))
            else:
                row_data["to_id"] = str(corr_val)

            # Fill in row context from original merged data if available.
            if not row_data["from_id"] and isinstance(ridx, (int, str)) and str(ridx).isdigit():
                ridx_int = int(ridx)
                if "df_merged" in globals() and ridx_int in globals()["df_merged"].index:
                    row_data["from_id"] = str(globals()["df_merged"].at[ridx_int, "delegate_id"])
                    if "name_col" in globals() and globals()["name_col"] in globals()["df_merged"].columns:
                        row_data["name"] = str(globals()["df_merged"].at[ridx_int, globals()["name_col"]])
                    if "pattern" in globals()["df_merged"].columns:
                        row_data["pattern"] = str(globals()["df_merged"].at[ridx_int, "pattern"])

            corr_rows.append(row_data)

        st.sidebar.dataframe(pd.DataFrame(corr_rows), height=180)
    else:
        st.info("No active corrections in RAM. Select rows in tabs and apply corrections.")

    if st.session_state.get("corrections"):
        if st.button("⏳ Stage current corrections"):
            staged = st.session_state.get("staged_corrections", {})
            now = datetime.datetime.now().isoformat(timespec="seconds")
            for _ridx, _nid in st.session_state["corrections"].items():
                staged[str(_ridx)] = {
                    "to_id": str(_nid),
                    "staged_at": now,
                    "source": "manual",
                }
            st.session_state["staged_corrections"] = staged
            save_staged_corrections(staged)
            st.session_state["corrections"] = {}
            save_corrections({})
            st.success("Active corrections staged and cleared (timestamped).")
            st.rerun()

        if st.button("✅ Approve and archive current corrections"):
            approved = st.session_state.get("approved_corrections", {})
            now = datetime.datetime.now().isoformat(timespec="seconds")
            for _ridx, _nid in st.session_state["corrections"].items():
                approved[str(_ridx)] = {
                    "to_id": str(_nid),
                    "approved_at": now,
                    "source": "approved_manual",
                }
            st.session_state["approved_corrections"] = approved
            save_approved_corrections(approved)
            st.session_state["corrections"] = {}
            save_corrections({})
            st.success("Approvals committed and hidden from active/staged.")
            st.rerun()

    if st.session_state.get("staged_corrections"):
        if st.button("✅ Load staged corrections into active"):
            active = {}
            for _ridx, _obj in st.session_state["staged_corrections"].items():
                if isinstance(_obj, dict) and "to_id" in _obj:
                    active[int(_ridx)] = _obj["to_id"]
                else:
                    active[int(_ridx)] = _obj
            st.session_state["corrections"] = active
            save_corrections(st.session_state["corrections"])
            st.success("Staged corrections loaded as active corrections.")
            st.rerun()
        st.markdown("*Staged corrections can only be cleared by removing `staged_corrections.json` from disk.*")

    # Approved corrections section with revert capability
    if st.session_state.get("approved_corrections"):
        st.markdown("---")
        st.subheader("Approved corrections")
        if st.button("↩ Revert approved corrections to active"):
            active = st.session_state.get("corrections", {})
            for _ridx, _obj in st.session_state["approved_corrections"].items():
                if isinstance(_obj, dict) and "to_id" in _obj:
                    active[int(_ridx)] = _obj["to_id"]
                else:
                    active[int(_ridx)] = _obj
            st.session_state["corrections"] = active
            save_corrections(active)
            st.success("Reverted approved corrections to active corrections.")
            st.rerun()
        st.caption("Approved corrections are retained in approved_corrections.json and can be reloaded anytime.")

    if corrections:
        # Enrich with original delegate_id + name so user can see from→to
        _corr_rows = []
        for _ridx, _nid in corrections.items():
            _orig_id = ""
            _orig_nm = ""
            if not df_merged.empty and _ridx in df_merged.index:
                _orig_id = str(df_merged.at[_ridx, "delegate_id"])
                if name_col in df_merged.columns:
                    _orig_nm = str(df_merged.at[_ridx, name_col])
            _orig_pat = ""
            if not df_merged.empty and _ridx in df_merged.index and "pattern" in df_merged.columns:
                _orig_pat = str(df_merged.at[_ridx, "pattern"])
            _corr_rows.append({
                "row": _ridx,
                "from_id": _orig_id,
                "name": _orig_nm,
                "pattern": _orig_pat,
                "to_id": str(_nid),
            })
        _corr_df = pd.DataFrame(_corr_rows)
        # Make corrections table resizable and user-friendly via AgGrid.
        gb = GridOptionsBuilder.from_dataframe(_corr_df)
        gb.configure_default_column(resizable=True, sortable=True, filter=True)
        gb.configure_selection(selection_mode="multiple", use_checkbox=True)
        grid_opts = gb.build()
        _corr_sel = AgGrid(
            _corr_df,
            gridOptions=grid_opts,
            height=min(50 + 35 * len(_corr_df), 300),
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            key="corr_table_sel",
        )
        selected_rows = _corr_sel.get("selected_rows", [])
        if isinstance(selected_rows, pd.DataFrame):
            selected_rows = selected_rows.to_dict("records")
        if not isinstance(selected_rows, list):
            selected_rows = []
        _sel_corr_meta = selected_rows if selected_rows else None

        _btn_col, _rev_col = st.columns(2)
        if _btn_col.button("🗑 Delete selected row(s)", key="corr_del_btn", disabled=not _sel_corr_meta):
            if _sel_corr_meta:
                for meta in _sel_corr_meta:
                    if isinstance(meta, dict) and "row" in meta:
                        st.session_state["corrections"].pop(meta["row"], None)
                save_corrections(st.session_state["corrections"])
                st.rerun()
            else:
                st.warning("No correction row(s) selected.")

        # Revise individual correction (change to_id)
        if _sel_corr_meta:
            # Use the first selected row as the default value in the textbox.
            first_meta = _sel_corr_meta[0] if isinstance(_sel_corr_meta, list) else _sel_corr_meta
            default_to_id = first_meta.get("to_id", "") if isinstance(first_meta, dict) else ""
            _new_to = st.text_input(
                "Revise to_id:", value=default_to_id, key="corr_rev_new"
            )
            if st.button("✏️ Update", key="corr_rev_btn", help="Apply the revised delegate_id to selected correction(s)."):
                if _new_to.strip():
                    for meta in _sel_corr_meta:
                        if isinstance(meta, dict) and "row" in meta:
                            save_correction(meta["row"], _new_to.strip())
                    st.rerun()

    # Pattern validity overrides (from tab2 actions)
    pattern_status = load_pattern_status()
    if pattern_status:
        st.markdown("---")
        st.subheader("Pattern validity overrides")
        st.caption("Explicit pattern validity flags saved in pattern_status.json")

        _pat_rows = []
        for key, valid in sorted(pattern_status.items()):
            parts = key.split("|")
            _pat_rows.append({
                "delegate_id": parts[0] if len(parts) >= 1 else "",
                "pattern": parts[1] if len(parts) >= 2 else "",
                "year": parts[2] if len(parts) >= 3 else "",
                "is_valid": bool(valid),
            })
        pat_df = pd.DataFrame(_pat_rows)
        st.sidebar.dataframe(pat_df, height=min(50 + 30 * len(pat_df), 240))

        if st.button("🧹 Clear all pattern overrides", key="clear_pattern_status"):
            save_pattern_status({})
            st.success("Cleared explicit pattern validity overrides.")
            st.rerun()

    # ── Finalize: export corrected dataset ──────────────────────────────────
    # Parquet (fast even for 430k rows, ~1–2s) — use for archiving / next run
    buf_parq = io.BytesIO()
    if not df_merged.empty:
        apply_corrections(df_merged, corrections).to_parquet(buf_parq, index=False)
        buf_parq.seek(0)
        st.sidebar.download_button(
            "⬇ Export corrected (parquet)",
            data=buf_parq,
            file_name="corrections_export.parquet",
            mime="application/octet-stream",
        )
    # Changed rows only — tiny Excel, instant
    changed_idxs = [ridx for ridx in corrections if ridx in df_merged.index]
    if changed_idxs:
        buf_chg = io.BytesIO()
        chg_df = df_merged.loc[changed_idxs].copy()
        chg_df["new_delegate_id"] = [str(corrections[i]) for i in changed_idxs]
        chg_df.to_excel(buf_chg, index=True)
        buf_chg.seek(0)
        st.sidebar.download_button(
            "⬇ Changed rows only (Excel)",
            data=buf_chg,
            file_name="corrections_changed_rows.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    if st.sidebar.button("Clear all corrections"):
        st.session_state["corrections"] = {}
        save_corrections({})
        st.rerun()

    staged = st.session_state.get("staged_corrections", {})
    show_staged = st.sidebar.checkbox("Show staged corrections (off-active)", value=False, key="show_staged_corrections")
    if staged and show_staged:
        st.sidebar.markdown("---")
        st.sidebar.subheader("Staged corrections (not active)")
        _staged_rows = []
        for _ridx, _obj in staged.items():
            if not df_merged.empty and int(_ridx) in df_merged.index:
                row_data = {
                    "row": int(_ridx),
                    "from_id": str(df_merged.at[int(_ridx), "delegate_id"]),
                    "name": str(df_merged.at[int(_ridx), name_col]) if name_col in df_merged.columns else "",
                    "staged_at": _obj.get("staged_at") if isinstance(_obj, dict) else "",
                }
                if isinstance(_obj, dict) and "to_id" in _obj:
                    row_data["to_id"] = str(_obj["to_id"])
                else:
                    row_data["to_id"] = str(_obj)
                _staged_rows.append(row_data)
        if _staged_rows:
            st.sidebar.dataframe(pd.DataFrame(_staged_rows), height=220)

# Work-queue: mark the currently selected delegate as reviewed
st.sidebar.markdown("---")
_sel_id_sidebar = st.session_state.get("sel_delegate_id")
if _sel_id_sidebar:
    _already = str(_sel_id_sidebar) in reviewed
    _btn_label = "✅ Mark reviewed" if not _already else "↩ Unmark reviewed"
    if st.sidebar.button(_btn_label, key="sidebar_mark_reviewed"):
        _sid = str(_sel_id_sidebar)
        if _already:
            reviewed.discard(_sid)
        else:
            reviewed.add(_sid)
        save_reviewed(reviewed)
        st.rerun()
    st.sidebar.caption(
        f"Reviewed: **{len(reviewed)}** / {len(df_merged['delegate_id'].unique()) if not df_merged.empty else '?'} delegates"
    )

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

# Per-delegate slice — cached; O(1) after first click on a given delegate
# Handle pending selection updates from tab0 selection without forcing st.rerun() directly.
if st.session_state.get("pending_sel_delegate_id"):
    sel_id_pending = st.session_state.pop("pending_sel_delegate_id")
    st.session_state["sel_delegate_id"] = sel_id_pending

_sel_id = st.session_state.get("sel_delegate_id")
if _sel_id and not df_merged.empty:
    df_delegate = _timed("get_delegate_slice()", lambda: get_delegate_slice(df_merged, _sel_id))
    # If a delegate has no occurrences, fall back to persons data so tabs still
    # show something useful (e.g., name, IDs, etc.).
    if df_delegate.empty and not df_p.empty and "delegate_id" in df_p.columns:
        df_delegate = df_p[df_p["delegate_id"].astype(str) == str(_sel_id)].copy()
else:
    df_delegate = pd.DataFrame(columns=df_merged.columns)

# Pre-compute corrected_delegate_ids: which delegates have ≥1 staged correction
_t0 = _time.perf_counter() if DEBUG else 0.0
_n_occurrences     = len(df_merged)
_merged_columns    = list(df_merged.columns)
_has_bio           = "birth_year" in df_merged.columns and df_merged["birth_year"].notna().any()
_has_surname_data  = "geslachtsnaam" in df_merged.columns and not df_merged["geslachtsnaam"].isna().all()
_known_delegate_ids = (
    df_merged["delegate_id"].dropna().unique().tolist() if not df_merged.empty else []
)
_corrected_delegate_ids: set[str] = set()
_corrected_indices: set = set(corrections.keys())
if corrections and not df_merged.empty:
    _valid_idxs = [r for r in corrections if r in df_merged.index]
    if _valid_idxs:
        _corrected_delegate_ids = set(
            df_merged.loc[_valid_idxs, "delegate_id"].astype(str).tolist()
        )
if DEBUG: print(f"  {'scalar pre-computation':<32} {(_time.perf_counter()-_t0)*1000:8.1f} ms")

def _render_timed(name, fn):
    if not DEBUG:
        fn(); return
    _t0 = _time.perf_counter()
    fn()
    print(f"  render {name:<25} {(_time.perf_counter()-_t0)*1000:8.1f} ms")

_TAB_LABELS = [
    "📋 Overview",
    "🧬 Alive Check",
    "🔤 Pattern Anomalies",
    "📛 Name Mismatch",
    "⏳ Timeline Gaps",
    "🧾 Delegates",
    "🔍 Suggestions",
    "🔗 Merge Errors",
    "⚙️ Settings",
]

# Initialise active_tab on first load only; on reruns the widget keeps its own state.
if "active_tab" not in st.session_state:
    st.session_state["active_tab"] = _TAB_LABELS[0]

tab0, tab1, tab2, tab3, tab4, tab8, tab_sug, tab9, tab7 = st.tabs(_TAB_LABELS, key="active_tab")

_render_timed("tab0", lambda: tab0_overview.render(
    tab0,
    summary=summary,
    df_p=df_p,
    n_occurrences=_n_occurrences,
    merged_columns=_merged_columns,
    df_bio=df_bio,
    load_error=load_error,
    corrections=corrections,
    name_col=name_col,
    PERSONS_FILE=PERSONS_FILE,
    OCCURRENCES_FILE=OCCURRENCES_FILE,
    ABBRD_FILE=ABBRD_FILE,
    n_placeholder_rows=n_placeholder_rows,
    n_remapped_rows=n_remapped_rows,
    n_enriched_persons=n_enriched_persons,
    sandboxed=st.session_state["sandboxed"],
    reviewed=reviewed,
    corrected_delegate_ids=_corrected_delegate_ids,
))

_alive_cfg = st.session_state.get("config", {}).get("alive", {})
_MIN_AGE = int(_alive_cfg.get("min_age", MIN_AGE))
_MAX_AGE = int(_alive_cfg.get("max_age", MAX_AGE))
_render_timed("tab1", lambda: tab1_alive.render(
    tab1,
    df_delegate=df_delegate,
    has_bio=_has_bio,
    name_col=name_col,
    ABBRD_FILE=ABBRD_FILE,
    MIN_AGE=_MIN_AGE,
    MAX_AGE=_MAX_AGE,
    save_correction=save_correction,
    corrected_indices=_corrected_indices,
))

_render_timed("tab2", lambda: tab2_patterns.render(
    tab2,
    df_delegate=df_delegate,
    name_col=name_col,
    save_correction=save_correction,
    corrected_indices=_corrected_indices,
    debug=DEBUG,
))

_render_timed("tab3", lambda: tab3_names.render(
    tab3,
    df_delegate=df_delegate,
    has_surname_data=_has_surname_data,
    df_p=df_p,
    name_col=name_col,
    save_correction=save_correction,
    corrected_indices=_corrected_indices,
    debug=DEBUG,
))

_render_timed("tab4", lambda: tab4_timeline.render(
    tab4,
    df_delegate=df_delegate,
    name_col=name_col,
    sel_delegate_id=st.session_state.get("sel_delegate_id"),
    DEFAULT_GAP=DEFAULT_GAP,
    save_correction=save_correction,
    corrected_indices=_corrected_indices,
))

_render_timed("tab8", lambda: tab8_delegates.render(
    tab8,
    df_p=df_p,
    df_abbrd=df_abbrd,
    name_col=name_col,
    summary=summary,
))

_render_timed("tab7", lambda: tab7_settings.render(tab7))

_render_timed("tab9", lambda: tab9_merges.render(
    tab9,
    df_merged=df_merged,
    name_col=name_col,
    save_correction=save_correction,
))

_render_timed("tab_sug", lambda: tab_suggest.render(
    tab_sug,
    df_unresolved=df_unresolved,
    df_merged=df_merged,
    suggestion_store=suggestion_store,
    save_correction=save_correction,
    df_p=df_p,
    name_col=name_col,
    corrected_indices=_corrected_indices,
))

# end of sheet.py — all tab logic lives in tabs/tab*.py

# ── Browser-side paint timer ——————————————————————————————————─
# Injected at the *end* of every Python render pass.  The script runs when
# the browser inserts this iframe, records a baseline timestamp, then uses
# a double-rAF to catch the first idle frame after all tabs have painted.
# Results appear as a small caption at the bottom of the page.
import streamlit.components.v1 as _components
_components.html("""
<style>
  body{margin:0;font:11px/1.4 monospace;color:#888;background:transparent}
  #out{padding:2px 6px}
</style>
<div id="out">⏳ measuring browser render…</div>
<script>
const t0 = performance.now();
function rAF2(cb){ requestAnimationFrame(()=>requestAnimationFrame(cb)); }
rAF2(() => {
  const paint = (performance.now() - t0).toFixed(1);
  // LCP if available
  let lcp = null;
  try {
    new PerformanceObserver((list) => {
      const e = list.getEntries().at(-1);
      if (e) lcp = e.startTime.toFixed(1);
    }).observe({type:'largest-contentful-paint', buffered:true});
  } catch(e){}
  rAF2(() => {
    const idle = (performance.now() - t0).toFixed(1);
    const lcpStr = lcp ? ` | LCP ${lcp} ms` : '';
    document.getElementById('out').textContent =
      `⌛ browser: first-paint ~${paint} ms | idle-frame ~${idle} ms${lcpStr}`;
    // Also push to parent console for terminal-style logging
    try{ window.parent.console.info(
      `[browser] first-paint=${paint}ms idle=${idle}ms${lcpStr}`); }catch(e){}
  });
});
</script>
""", height=22)
_SENTINEL = None  # pragma: no cover — marker so nothing follows

