"""
profile_app.py – standalone profiler for the Delegate QA pipeline.
===================================================================
Runs outside of Streamlit so there is no widget overhead, no HTTP round-trips
and no cache decorator (we time the pure Python work directly).

Usage
-----
    # Quick text report (default top-40 by cumtime):
    python profile_app.py

    # Write a .prof binary for SnakeViz / PyCharm:
    python profile_app.py --save profile.prof

    # Only show functions that match a substring:
    python profile_app.py --filter utils

    # Per-line profiling with line_profiler (pip install line-profiler):
    python profile_app.py --line

    # Wall-clock timing only (no cProfile overhead), N repetitions:
    python profile_app.py --timeit N

Architecture
------------
The Streamlit rerun loop executes EVERY line of sheet.py on each interaction.
Cached functions (load_data, build_merged, filter_occurrences, …) are fast the
second time — the bottleneck is usually:

  1. Cold start  – load_data / build_merged / enrich_persons_from_abbrd
  2. Hot rerun   – filter_occurrences, get_delegate_slice, tab rendering logic
                   (these run with already-loaded DataFrames)

This script profiles both phases separately so you can tell which is slow.
"""
from __future__ import annotations

import argparse
import cProfile
import io
import pstats
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Minimal stubs so utils.py can be imported without a running Streamlit server
# ---------------------------------------------------------------------------
import sys
import types
import unittest.mock as mock

# Stub streamlit before importing utils / tabs
_st_stub = types.ModuleType("streamlit")

# Provide a no-op cache_data that just calls the wrapped function directly
def _passthrough_cache(*args, **kwargs):
    """Drop-in for @st.cache_data — returns the function unchanged."""
    if len(args) == 1 and callable(args[0]):
        return args[0]
    def decorator(fn):
        return fn
    return decorator

_st_stub.cache_data = _passthrough_cache
_st_stub.session_state = {}
sys.modules["streamlit"] = _st_stub

# Now safe to import utils
import utils  # noqa: E402  (must come after stub)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

DIVIDER = "─" * 72


def _print_stats(profiler: cProfile.Profile, n: int = 40, filter_str: str = "") -> None:
    buf = io.StringIO()
    stats = pstats.Stats(profiler, stream=buf).sort_stats("cumulative")
    if filter_str:
        stats.print_stats(filter_str, n)
    else:
        stats.print_stats(n)
    print(buf.getvalue())


def _timeit(fn, label: str, n: int = 1):
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    avg = sum(times) / len(times)
    mn  = min(times)
    mx  = max(times)
    print(f"  {label:<45}  avg={avg*1000:>8.1f} ms  "
          f"min={mn*1000:>8.1f} ms  max={mx*1000:>8.1f} ms  (n={n})")
    return result


# ---------------------------------------------------------------------------
# PHASE 1: cold-start (data loading + merging)
# ---------------------------------------------------------------------------

def profile_cold_start(n_timeit: int, save_path: str | None, filter_str: str) -> dict:
    print(f"\n{DIVIDER}")
    print("PHASE 1 – cold start (load_data · enrich · build_merged)")
    print(DIVIDER)

    if n_timeit:
        print(f"\n  Wall-clock timing, {n_timeit} repetition(s):\n")
        df_p_raw, df_i_raw, df_abbrd = _timeit(utils.load_data, "load_data()", n_timeit)
        df_p, n_enriched = _timeit(
            lambda: utils.enrich_persons_from_abbrd(df_p_raw, df_abbrd),
            "enrich_persons_from_abbrd()", n_timeit,
        )
        merged, n_ph, n_remap, summary = _timeit(
            lambda: utils.build_merged(df_p, df_i_raw, df_abbrd, name_col="fullname"),
            "build_merged()", n_timeit,
        )
        return dict(df_p=df_p, df_i=df_i_raw, df_merged=merged, summary=summary)

    prof = cProfile.Profile()
    prof.enable()

    df_p_raw, df_i_raw, df_abbrd = utils.load_data()
    df_p, _n_enriched = utils.enrich_persons_from_abbrd(df_p_raw, df_abbrd)
    df_merged, n_ph, n_remap, summary = utils.build_merged(
        df_p, df_i_raw, df_abbrd, name_col="fullname"
    )

    prof.disable()

    print(f"\n  df_merged shape: {df_merged.shape}")
    print(f"  summary shape:   {summary.shape}")
    print(f"  placeholder rows filtered: {n_ph}   remapped rows: {n_remap}\n")

    if save_path:
        prof.dump_stats(save_path)
        print(f"  Profile saved → {save_path}\n")

    print("  Top functions by cumulative time:")
    _print_stats(prof, n=30, filter_str=filter_str)

    return dict(df_p=df_p, df_i=df_i_raw, df_merged=df_merged, summary=summary)


# ---------------------------------------------------------------------------
# PHASE 2: hot rerun (simulates subsequent Streamlit reruns)
# ---------------------------------------------------------------------------

def profile_hot_rerun(
    df_p, df_merged, summary,
    n_timeit: int,
    save_path: str | None,
    filter_str: str,
) -> None:
    print(f"\n{DIVIDER}")
    print("PHASE 2 – hot rerun (filter · delegate slice · name-to-id · summary)")
    print(DIVIDER)

    prov_col = next(
        (c for c in ("provincie_p", "provincie", "province") if c in df_merged.columns), None
    )
    name_col = next(
        (c for c in ("fullname", "full_name", "naam", "name") if c in df_p.columns),
        df_p.columns[0],
    )

    all_provinces = (
        sorted(df_merged[prov_col].dropna().unique().tolist()) if prov_col else []
    )
    ymin = int(df_merged["j"].dropna().min()) if "j" in df_merged.columns else 1700
    ymax = int(df_merged["j"].dropna().max()) if "j" in df_merged.columns else 1800

    # Pick a representative delegate
    top_id = df_merged["delegate_id"].value_counts().index[0] if "delegate_id" in df_merged.columns else None

    # Build name → id dict once (same as sheet.py startup)
    name_to_id = utils.build_name_to_id(df_p, name_col)

    if n_timeit:
        print(f"\n  Wall-clock timing, {n_timeit} repetition(s):\n")
        _timeit(
            lambda: utils.filter_occurrences(
                df_merged, prov_col, tuple(all_provinces[:3] if len(all_provinces) >= 3 else all_provinces),
                ymin + 10, ymax - 10,
            ),
            "filter_occurrences() [3 provinces, narrow year]",
            n_timeit,
        )
        _timeit(
            lambda: utils.filter_occurrences(df_merged, prov_col, tuple(all_provinces), ymin, ymax),
            "filter_occurrences() [all provinces, full range]",
            n_timeit,
        )
        if top_id:
            _timeit(
                lambda: utils.get_delegate_slice(df_merged, str(top_id)),
                f"get_delegate_slice({top_id!r})",
                n_timeit,
            )
        _timeit(lambda: utils.build_name_to_id(df_p, name_col), "build_name_to_id()", n_timeit)
        _timeit(lambda: utils._build_delegate_index(df_merged), "_build_delegate_index()", n_timeit)

        # Cached sidebar options (build_sidebar_options — now cached in utils)
        _timeit(
            lambda: utils.build_sidebar_options(df_p, df_merged, name_col, prov_col),
            "build_sidebar_options() [cached]",
            n_timeit,
        )
        return

    prof = cProfile.Profile()
    prof.enable()

    # Simulate what happens on every Streamlit rerun:
    # 1. Province + year filter (Tab 5 / day order view)
    for sel_p in (tuple(all_provinces), tuple(all_provinces[:3] if len(all_provinces) >= 3 else all_provinces)):
        utils.filter_occurrences(df_merged, prov_col, sel_p, ymin + 5, ymax - 5)

    # 2. Delegate slice (shown in tabs 1–4)
    if top_id:
        utils.get_delegate_slice(df_merged, str(top_id))

    # 3. Name lookup (sidebar on_change callback)
    utils.build_name_to_id(df_p, name_col)

    # 4. Delegate index build
    utils._build_delegate_index(df_merged)

    # 5. Cached sidebar options (was bare sorted/unique, now cached)
    utils.build_sidebar_options(df_p, df_merged, name_col, prov_col)

    prof.disable()

    if save_path:
        base = Path(save_path).stem
        hot_path = str(Path(save_path).parent / f"{base}_hot.prof")
        prof.dump_stats(hot_path)
        print(f"\n  Profile saved → {hot_path}\n")

    print("\n  Top functions by cumulative time:")
    _print_stats(prof, n=30, filter_str=filter_str)


# ---------------------------------------------------------------------------
# PHASE 3: optional line_profiler
# ---------------------------------------------------------------------------

def profile_lines(df_p, df_merged) -> None:
    try:
        from line_profiler import LineProfiler  # type: ignore
    except ImportError:
        print("\n  line_profiler not installed. Run: pip install line-profiler")
        return

    print(f"\n{DIVIDER}")
    print("PHASE 3 – line_profiler (per-line timings for hot functions)")
    print(DIVIDER)

    prov_col = next(
        (c for c in ("provincie_p", "provincie", "province") if c in df_merged.columns), None
    )
    name_col = next(
        (c for c in ("fullname", "full_name", "naam", "name") if c in df_p.columns),
        df_p.columns[0],
    )
    all_provinces = (
        sorted(df_merged[prov_col].dropna().unique().tolist()) if prov_col else []
    )
    ymin = int(df_merged["j"].dropna().min()) if "j" in df_merged.columns else 1700
    ymax = int(df_merged["j"].dropna().max()) if "j" in df_merged.columns else 1800
    top_id = df_merged["delegate_id"].value_counts().index[0] if "delegate_id" in df_merged.columns else None

    lp = LineProfiler()
    lp.add_function(utils._province_positions)
    lp.add_function(utils._year_positions_from)
    lp.add_function(utils.filter_occurrences)
    lp.add_function(utils.get_delegate_slice)
    lp.add_function(utils._build_delegate_index)
    lp.add_function(utils.build_name_to_id)

    @lp
    def _run():
        utils.filter_occurrences(df_merged, prov_col, tuple(all_provinces[:3] if all_provinces else ()), ymin + 10, ymax - 10)
        utils.filter_occurrences(df_merged, prov_col, tuple(all_provinces), ymin, ymax)
        if top_id:
            utils.get_delegate_slice(df_merged, str(top_id))
        utils.build_name_to_id(df_p, name_col)

    _run()
    buf = io.StringIO()
    lp.print_stats(buf)
    print(buf.getvalue())


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Profile Delegate QA pipeline")
    p.add_argument("--save",   default=None,  help="Save .prof file to PATH (e.g. profile.prof)")
    p.add_argument("--filter", default="",    help="Only show functions matching this substring")
    p.add_argument("--line",   action="store_true", help="Run line_profiler on hot functions")
    p.add_argument("--timeit", type=int, default=0, metavar="N",
                   help="Wall-clock timing only, N repetitions (no cProfile overhead)")
    args = p.parse_args()

    t_total = time.perf_counter()

    data = profile_cold_start(args.timeit, args.save, args.filter)
    profile_hot_rerun(
        data["df_p"], data["df_merged"], data["summary"],
        n_timeit=args.timeit,
        save_path=args.save,
        filter_str=args.filter,
    )

    if args.line:
        profile_lines(data["df_p"], data["df_merged"])

    elapsed = time.perf_counter() - t_total
    print(f"\n{DIVIDER}")
    print(f"Total profiling time: {elapsed:.2f}s")
    print(DIVIDER)

    if args.save:
        print(f"\nTo visualise: snakeviz {args.save}")
        print("Install snakeviz: pip install snakeviz\n")


if __name__ == "__main__":
    main()
