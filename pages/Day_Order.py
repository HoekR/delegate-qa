"""Day-Order Violations — standalone Streamlit page."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from utils import (
    PROVINCE_ORDER,
    PROVINCE_ORDER_FILE,
    PROVINCE_RANK,
    build_merged,
    build_sidebar_options,
    enrich_persons_from_abbrd,
    load_corrections,
    load_data,
    load_new_delegates,
    load_remappings,
    load_sandboxed,
    save_corrections,
    source_mtimes,
)
from tabs import tab5_dayorder

st.set_page_config(layout="wide", page_title="Day Order — Delegate QA", page_icon="📅")

# ── Session state ────────────────────────────────────────────────────────────
if "corrections" not in st.session_state:
    st.session_state["corrections"] = load_corrections()
if "new_delegates" not in st.session_state:
    st.session_state["new_delegates"] = load_new_delegates()
if "remappings" not in st.session_state:
    st.session_state["remappings"] = load_remappings()
if "sandboxed" not in st.session_state:
    st.session_state["sandboxed"] = load_sandboxed()

# ── Data loading (hits same @st.cache_data cache as main app) ────────────────
_load_error: str | None = None
df_p = pd.DataFrame()
try:
    df_p, df_i, df_abbrd = load_data(source_mtimes())
    df_p, _ = enrich_persons_from_abbrd(df_p, df_abbrd)
    for _df in (df_p, df_i):
        if "delegate_id" in _df.columns:
            _df["delegate_id"] = _df["delegate_id"].astype(str)
    name_col = next(
        (c for c in ("fullname", "full_name", "naam", "name") if c in df_p.columns),
        df_p.columns[0] if not df_p.empty else "fullname",
    )
    df_merged, _n_ph, _n_re, _summary = build_merged(
        df_p, df_i, df_abbrd,
        st.session_state["new_delegates"],
        remappings=st.session_state["remappings"],
        name_col=name_col,
    )
except Exception as exc:
    _load_error = str(exc)
    df_merged = pd.DataFrame()
    name_col = "fullname"

prov_col = next(
    (c for c in ("provincie_p", "provincie", "province") if c in df_merged.columns),
    None,
)
_delegate_options, _provinces, _ymin, _ymax = build_sidebar_options(
    df_p, df_merged, name_col, prov_col,
)

# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.title("📅 Day Order")
if _load_error:
    st.sidebar.error(f"Load error:\n{_load_error}")

sel_provinces = st.sidebar.multiselect("Province", _provinces, help="Leave empty = all")
year_range = st.sidebar.slider("Year range", _ymin, _ymax, (_ymin, _ymax))

_row_opts: list = [500, 1_000, 5_000, 10_000, 50_000, "All"]
max_rows_sel = st.sidebar.selectbox(
    "Max rows to analyse",
    _row_opts,
    index=len(_row_opts) - 1,
)
max_rows = None if max_rows_sel == "All" else int(max_rows_sel)


# ── Save helper ──────────────────────────────────────────────────────────────
def save_correction(row_idx: int, new_id: int | str) -> None:
    val = int(new_id) if str(new_id).lstrip("-").isdigit() else str(new_id)
    st.session_state["corrections"][row_idx] = val
    save_corrections(st.session_state["corrections"])


# ── Render ───────────────────────────────────────────────────────────────────
tab5_dayorder.render(
    df_merged=df_merged,
    prov_col=prov_col,
    sel_provinces=tuple(sel_provinces),
    year_min=year_range[0],
    year_max=year_range[1],
    max_rows=max_rows,
    name_col=name_col,
    PROVINCE_ORDER=PROVINCE_ORDER,
    PROVINCE_RANK=PROVINCE_RANK,
    PROVINCE_ORDER_FILE=PROVINCE_ORDER_FILE,
    save_correction=save_correction,
)
