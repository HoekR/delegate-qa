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

import io

import pandas as pd
import streamlit as st

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
    build_merged,
    build_name_to_id,
    build_sidebar_options,
    enrich_persons_from_abbrd,
    get_delegate_slice,
    load_corrections,
    load_data,
    load_new_delegates,
    load_remappings,
    load_reviewed,
    load_sandboxed,
    save_corrections,
    save_reviewed,
    source_mtimes,
)
from tabs import (
    tab0_overview,
    tab1_alive,
    tab2_patterns,
    tab3_names,
    tab4_timeline,
    tab6_management,
)
# ---------------------------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------------------------

st.set_page_config(layout="wide", page_title="Delegate QA", page_icon="📜")

# ---------------------------------------------------------------------------
# SESSION STATE
# ---------------------------------------------------------------------------

if "corrections" not in st.session_state:
    st.session_state["corrections"] = load_corrections()
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

corrections:    dict       = st.session_state["corrections"]
new_delegates:  list[dict] = st.session_state["new_delegates"]
reviewed:       set[str]   = st.session_state["reviewed"]

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
    _t0 = _time.perf_counter()
    result = fn()
    print(f"  {name:<32} {(_time.perf_counter()-_t0)*1000:8.1f} ms")
    return result

print(f"--- rerun {_time.strftime('%H:%M:%S')} sel={st.session_state.get('sel_delegate_id')} ---")

try:
    df_p, df_i, df_abbrd = _timed("load_data()", lambda: load_data(source_mtimes()))
    df_p, n_enriched_persons = _timed("enrich_persons_from_abbrd()", lambda: enrich_persons_from_abbrd(df_p, df_abbrd))
    for _df in (df_p, df_i):
        if "delegate_id" in _df.columns:
            _df["delegate_id"] = _df["delegate_id"].astype(str)
    df_bio = df_abbrd
    name_col = next(
        (c for c in ("fullname", "full_name", "naam", "name") if c in df_p.columns),
        df_p.columns[0] if not df_p.empty else "fullname",
    )
    df_merged, n_placeholder_rows, n_remapped_rows, summary = _timed(
        "build_merged()",
        lambda: build_merged(
            df_p, df_i, df_abbrd, new_delegates,
            remappings=st.session_state["remappings"],
            name_col=name_col,
        ),
    )
    load_error: str | None = None
except Exception as exc:
    load_error = str(exc)
    df_p = df_i = df_merged = pd.DataFrame()
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

# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------

st.sidebar.title("📜 Delegate QA")

if load_error:
    st.sidebar.error(f"Load error:\n{load_error}")

# Pre-built name → id dict: O(1) lookup in on_change callback instead of df_p scan
_name_to_id: dict[str, str] = build_name_to_id(df_p, name_col)

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
st.sidebar.markdown("---")
st.sidebar.subheader(f"Pending corrections: {len(corrections)}")
if corrections:
    st.sidebar.dataframe(
        pd.DataFrame([{"row": k, "new_delegate_id": v} for k, v in corrections.items()]),
        width="stretch",
    )
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

def save_correction(row_idx: int, new_id: int | str) -> None:
    """new_id may be an integer delegate_id or a string like 'republic_add_05'."""
    val = int(new_id) if str(new_id).lstrip("-").isdigit() else str(new_id)
    st.session_state["corrections"][row_idx] = val
    save_corrections(st.session_state["corrections"])


# Per-delegate slice — cached; O(1) after first click on a given delegate
_sel_id = st.session_state.get("sel_delegate_id")
if _sel_id and not df_merged.empty:
    df_delegate = _timed("get_delegate_slice()", lambda: get_delegate_slice(df_merged, _sel_id))
else:
    df_delegate = pd.DataFrame(columns=df_merged.columns)

# Pre-compute corrected_delegate_ids: which delegates have ≥1 staged correction
_t0 = _time.perf_counter()
_n_occurrences     = len(df_merged)
_merged_columns    = list(df_merged.columns)
_has_bio           = "birth_year" in df_merged.columns and df_merged["birth_year"].notna().any()
_has_surname_data  = "geslachtsnaam" in df_merged.columns and not df_merged["geslachtsnaam"].isna().all()
_known_delegate_ids = (
    df_merged["delegate_id"].dropna().unique().tolist() if not df_merged.empty else []
)
_corrected_delegate_ids: set[str] = set()
if corrections and not df_merged.empty:
    _valid_idxs = [r for r in corrections if r in df_merged.index]
    if _valid_idxs:
        _corrected_delegate_ids = set(
            df_merged.loc[_valid_idxs, "delegate_id"].astype(str).tolist()
        )
print(f"  {'scalar pre-computation':<32} {(_time.perf_counter()-_t0)*1000:8.1f} ms")

def _render_timed(name, fn):
    _t0 = _time.perf_counter()
    fn()
    print(f"  render {name:<25} {(_time.perf_counter()-_t0)*1000:8.1f} ms")

tab0, tab1, tab2, tab3, tab4, tab6 = st.tabs([
    "📋 Overview",
    "🧬 Alive Check",
    "🔤 Pattern Anomalies",
    "📛 Name Mismatch",
    "⏳ Timeline Gaps",
    "👤 Delegate Mgmt",
])

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

_render_timed("tab1", lambda: tab1_alive.render(
    tab1,
    df_delegate=df_delegate,
    has_bio=_has_bio,
    name_col=name_col,
    ABBRD_FILE=ABBRD_FILE,
    MIN_AGE=MIN_AGE,
    MAX_AGE=MAX_AGE,
    save_correction=save_correction,
))

_render_timed("tab2", lambda: tab2_patterns.render(
    tab2,
    df_delegate=df_delegate,
    name_col=name_col,
    save_correction=save_correction,
))

_render_timed("tab3", lambda: tab3_names.render(
    tab3,
    df_delegate=df_delegate,
    has_surname_data=_has_surname_data,
    df_p=df_p,
    name_col=name_col,
    save_correction=save_correction,
))

_render_timed("tab4", lambda: tab4_timeline.render(
    tab4,
    df_delegate=df_delegate,
    name_col=name_col,
    sel_delegate_id=st.session_state.get("sel_delegate_id"),
    DEFAULT_GAP=DEFAULT_GAP,
))

_render_timed("tab6", lambda: tab6_management.render(
    tab6,
    df_abbrd=df_abbrd,
    df_p=df_p,
    known_delegate_ids=_known_delegate_ids,
    name_col=name_col,
    ABBRD_CANDIDATES=ABBRD_CANDIDATES,
    save_correction=save_correction,
    n_enriched_persons=n_enriched_persons,
    n_remapped_rows=n_remapped_rows,
    sandboxed=st.session_state["sandboxed"],
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

