"""
pattern_merge.py — Detect concatenation and fragmentation errors in delegate
name patterns that result from imperfect HTR tokenization.

Two error types:
  A) Concat  — one row carries a pattern that is two names fused together.
  B) Fragment — one delegate has patterns that are sub-string fragments of
                their true name, inflating their n_patterns count.

No Streamlit dependency.  Call from a notebook or from tab9_merges.py.
"""

from __future__ import annotations

import re
import time
from typing import NamedTuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------

# Session-header patterns (Batavian Republic date/session headers and president
# lines) that appear as patterns in the occurrences file but are NOT delegate
# names.  Rows whose pattern matches this regex are excluded from all detection.
# Patterns can be truncated mid-word by the HTR tokenizer, so we match on
# short unambiguous prefixes rather than whole words.
_SESSION_HEADER_RE = re.compile(
    # Day-of-week openings (Latin) plus confirmed HTR variants found in data:
    #   Luna (Lunae), Joris (Jovis), Martie (Martis), Mereurii/Mercurit (Mercurii),
    #   Dominica (Dominicae).  All followed by \b so "Martina" etc. are safe.
    r"^(lunae?|martis|martie|mercurii|mereurii|mercurit|jovis|joris|veneris|sabbathi|dominicae?)\b"
    # President line (Batavian Republic: "PRAESIDE Den Burger X")
    r"|^praeside\b"
    # PRAESENTIBUS appended to a name after a newline, e.g. "van de Lier\nPRAESENTIBUS"
    r"|\npraesent"
    # "Het / Hot / Hit / Hlot / Bet eerste jaar" — various HTR confusions, may be
    # truncated; the key signal is [hb]+vowel+t followed by "eerst"
    r"|\b[hb][il]?[oea]t\s+eerst"
    # "Bataafsche" and HTR variants (Bataafscte, Bataafscke, Pataafsche)
    r"|\b[bp]ataafs"
    # Resumption clause (confirmed form): "IS na voorgaande deliberatie…"
    r"|^is\s+na\s+v[oe]orgaande",
    re.IGNORECASE,
)

# Default thresholds (all overrideable via function parameters)
DEFAULT_T_CONCAT        = 0.20   # max normalised edit dist for each half of a concat
DEFAULT_T_FRAG          = 0.15   # max normalised edit dist for reconstructed vs anchor
DEFAULT_MIN_LEN_RATIO   = 1.40   # pattern must be ≥ this × anchor to be a concat candidate
DEFAULT_NEIGHBOR_WINDOW = 2      # how many adjacent rows (±) to inspect per day
DEFAULT_MIN_PATTERNS    = 3      # min distinct patterns a delegate needs for frag check


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    """Lowercase and collapse whitespace.  Van/van-der prefixes are kept
    because they are part of the name and help with fuzzy matching."""
    s = str(s).strip().lower()
    return re.sub(r"\s+", " ", s)


def _is_session_header(s: str) -> bool:
    """Return True if s looks like a session header or Batavian president line
    rather than a delegate name.  These rows should be excluded from detection."""
    return bool(_SESSION_HEADER_RE.search(str(s)))


def _lev_dist(a: str, b: str) -> float:
    """Return normalised Levenshtein distance [0, 1].

    Uses rapidfuzz if available (C extension, ~100× faster); falls back to a
    pure-Python implementation so the module works without rapidfuzz installed.
    """
    if not a and not b:
        return 0.0
    try:
        from rapidfuzz.distance import Levenshtein
        return Levenshtein.normalized_distance(a, b)
    except ImportError:
        pass
    # Pure-Python fallback (Wagner–Fischer)
    la, lb = len(a), len(b)
    if la == 0 or lb == 0:
        return 1.0
    prev = list(range(lb + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * lb
        for j, cb in enumerate(b, 1):
            curr[j] = min(prev[j] + 1, curr[j - 1] + 1,
                          prev[j - 1] + (0 if ca == cb else 1))
        prev = curr
    return prev[lb] / max(la, lb)


def _split_points(s: str) -> list[int]:
    """Return candidate split positions for concat detection.

    Space positions are always included.  For patterns without spaces, every
    position from 30% to 70% of the string length is included.
    """
    n = len(s)
    if n < 4:
        return []
    positions: set[int] = set()
    # Always try space boundaries first
    for i, ch in enumerate(s):
        if ch == " ":
            positions.add(i)
    # Add character-level splits in the middle third
    lo = max(2, int(n * 0.30))
    hi = min(n - 2, int(n * 0.70))
    positions.update(range(lo, hi + 1))
    return sorted(positions)


# ---------------------------------------------------------------------------
# Step 0 — Build anchor lookup
# ---------------------------------------------------------------------------

class AnchorTable(NamedTuple):
    """Pre-computed per-delegate anchors and day-ordered position table."""
    anchors: dict[str, str]          # delegate_id → normalised anchor pattern
    modals:  dict[str, str]          # delegate_id → raw modal pattern
    day_pos: pd.DataFrame            # df with _day, _pos, delegate_id, pattern, row_index


def build_anchor_table(df_merged: pd.DataFrame) -> AnchorTable:
    """Build the anchor lookup and the day-position table.

    Parameters
    ----------
    df_merged : pd.DataFrame
        The merged occurrences+persons DataFrame from build_merged().

    Returns
    -------
    AnchorTable
        anchors  — {delegate_id: normalised_anchor}
        modals   — {delegate_id: raw_modal_pattern}
        day_pos  — DataFrame with columns [_day, _pos, _rank, delegate_id, pattern, row_index]
                   _pos  = raw character offset in the per-day HTR source file (-1 for presidents)
                   _rank = 0-based position within the day sorted by _pos (use this for ±window lookup)
    """
    needed = {"delegate_id", "pattern"}
    if not needed.issubset(df_merged.columns):
        raise ValueError(f"df_merged must have columns {needed}")

    # ── Exclude session headers and president rows from anchor computation ─
    # Session headers (Batavian date lines, PRAESIDE Den Burger …) would corrupt
    # the modal pattern and inflate anchor lengths.
    df_clean = df_merged.copy()
    _pat_col = df_clean["pattern"].astype(str)
    _header_mask = _pat_col.map(_is_session_header)
    df_clean = df_clean[~_header_mask]
    if "class" in df_clean.columns:
        df_clean = df_clean[df_clean["class"].astype(str) != "president"]
    elif "offset" in df_clean.columns:
        df_clean = df_clean[df_clean["offset"].fillna(0) >= 0]

    # ── Modal pattern per delegate ─────────────────────────────────────────
    pat_series = df_merged["pattern"].astype(str)  # full series for day_pos
    did_series = df_merged["delegate_id"].astype(str)
    pat_clean  = df_clean["pattern"].astype(str)
    did_clean  = df_clean["delegate_id"].astype(str)

    modal_raw: dict[str, str] = (
        df_clean.assign(_did=did_clean, _pat=pat_clean)
        .groupby("_did")["_pat"]
        .agg(lambda s: s.mode().iloc[0] if not s.empty else "")
        .to_dict()
    )

    # Use the normalised modal pattern as the anchor.
    # The modal is the most frequent raw pattern for the delegate; it is the
    # best available representative of how the HTR actually rendered the name.
    anchors: dict[str, str] = {
        did: _norm(modal)
        for did, modal in modal_raw.items()
        if modal
    }

    # ── Day-position table ─────────────────────────────────────────────────
    # Determine a "day" key from the data.  Prefer an explicit date column;
    # fall back to the year column "j" (coarser but better than nothing).
    df_work = df_merged.copy()
    df_work["_did"]       = did_series
    df_work["_pat"]       = pat_series
    df_work["row_index"]  = df_work.index  # preserve original index

    if "date" in df_work.columns:
        df_work["_day"] = pd.to_datetime(df_work["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    elif all(c in df_work.columns for c in ("d", "m", "j")):
        df_work["_day"] = (
            df_work["j"].astype(str) + "-"
            + df_work["m"].astype(str).str.zfill(2) + "-"
            + df_work["d"].astype(str).str.zfill(2)
        )
    elif "j" in df_work.columns:
        df_work["_day"] = df_work["j"].astype("Int64").astype(str)
    else:
        df_work["_day"] = "?"

    # Within each day, position is the `offset` column (character offset in the
    # per-day HTR source file).  Presidents have offset = -1 (lowest = first).
    # Offsets reset for every day so they are a reliable intra-day ordering key.
    if "offset" in df_work.columns:
        df_work["_pos"] = df_work["offset"].astype("Int64")
    else:
        # Fallback: use physical row order within each day
        df_work = df_work.sort_values("_day")
        df_work["_pos"] = df_work.groupby("_day", sort=False).cumcount()

    # Compute rank within each day (0-based) for use in ±window neighbor lookup.
    # Sort by (_day, _pos) so rank is ascending by offset.
    df_work = df_work.sort_values(["_day", "_pos"])
    df_work["_rank"] = df_work.groupby("_day", sort=False).cumcount()

    day_pos = df_work[["_day", "_pos", "_rank", "_did", "_pat", "row_index"]].copy()
    day_pos.columns = ["_day", "_pos", "_rank", "delegate_id", "pattern", "row_index"]
    day_pos = day_pos.reset_index(drop=True)

    return AnchorTable(anchors=anchors, modals=modal_raw, day_pos=day_pos)


# ---------------------------------------------------------------------------
# Error type A — Concat detection
# ---------------------------------------------------------------------------

class ConcatCandidate(NamedTuple):
    row_index:         int
    pattern:           str
    delegate_id:       str
    anchor:            str
    split_left:        str
    split_right:       str
    left_delegate_id:  str
    right_delegate_id: str
    left_score:        float
    right_score:       float
    combined_score:    float   # average of left_score + right_score (lower = better match)
    n_occurrences:     int     # how many rows share this (pattern, delegate_id)


def detect_concat_errors(
    df_merged:        pd.DataFrame,
    at:               AnchorTable | None = None,
    t_concat:         float = DEFAULT_T_CONCAT,
    min_len_ratio:    float = DEFAULT_MIN_LEN_RATIO,
    neighbor_window:  int   = DEFAULT_NEIGHBOR_WINDOW,
    province:         str | None = None,
    year_min:         int | None = None,
    year_max:         int | None = None,
) -> pd.DataFrame:
    """Detect occurrences whose pattern looks like two names fused together.

    Parameters
    ----------
    df_merged :
        Merged occurrences+persons DataFrame.
    at :
        Pre-built AnchorTable. If None, built internally.
    t_concat :
        Maximum normalised edit distance for each half of the split to match a
        neighbor anchor.  Lower = stricter.
    min_len_ratio :
        Minimum ratio of pattern length to anchor length to be considered a
        concat candidate.
    neighbor_window :
        Number of adjacent rows (±) to look up within the same day.
    province :
        If given, restrict to this province only (uses 'provincie' column).
    year_min / year_max :
        If given, restrict to this year range (uses 'j' column).

    Returns
    -------
    pd.DataFrame with columns matching ConcatCandidate fields, sorted by
    combined_score ascending (best matches first).
    """
    df = df_merged.copy()

    # Optional filters
    if province and "provincie" in df.columns:
        df = df[df["provincie"].astype(str).str.lower() == province.lower()]
    if year_min is not None and "j" in df.columns:
        df = df[df["j"].fillna(0) >= year_min]
    if year_max is not None and "j" in df.columns:
        df = df[df["j"].fillna(9999) <= year_max]

    if df.empty:
        return pd.DataFrame(columns=list(ConcatCandidate._fields))

    if at is None:
        at = build_anchor_table(df_merged)

    anchors   = at.anchors
    day_pos   = at.day_pos

    # Restrict day_pos to the filtered rows
    day_pos_f = day_pos[day_pos["row_index"].isin(df.index)]

    # Build (day, rank) → delegate_id lookup for neighbor resolution.
    # _rank is the 0-based position within the day sorted by offset, so ±window
    # means ±N adjacent delegates in meeting order.
    day_lookup: dict[tuple[str, int], str] = dict(
        zip(zip(day_pos_f["_day"], day_pos_f["_rank"]), day_pos_f["delegate_id"])
    )

    # ── Pre-filter: find candidate rows ───────────────────────────────────
    dids = df["delegate_id"].astype(str)
    pats = df["pattern"].astype(str)

    # Exclude session headers (Batavian date lines, PRAESIDE Den Burger …)
    session_mask = pats.map(_is_session_header)
    df   = df[~session_mask]
    dids = dids[~session_mask]
    pats = pats[~session_mask]

    # Also exclude president rows (offset == -1 / class == "president")
    if "class" in df.columns:
        df   = df[df["class"].astype(str) != "president"]
        dids = df["delegate_id"].astype(str)
        pats = df["pattern"].astype(str)
    elif "offset" in df.columns:
        df   = df[df["offset"].fillna(0) >= 0]
        dids = df["delegate_id"].astype(str)
        pats = df["pattern"].astype(str)

    if df.empty:
        return pd.DataFrame(columns=list(ConcatCandidate._fields))

    norm_pats    = pats.map(_norm)
    anchor_lens  = dids.map(lambda d: len(anchors.get(d, "d")) or 1)
    pat_lens     = norm_pats.map(len)

    has_space    = norm_pats.str.contains(" ", regex=False)
    anchor_has_space = dids.map(lambda d: " " in anchors.get(d, ""))

    # Candidate if: long relative to anchor, OR has extra space anchor doesn't
    candidate_mask = (
        (pat_lens / anchor_lens >= min_len_ratio)
        | (has_space & ~anchor_has_space)
    )
    # Exclude very short patterns (noise)
    candidate_mask &= pat_lens >= 6

    df_cands = df[candidate_mask].copy()
    df_cands["_pat_norm"] = norm_pats[candidate_mask]
    df_cands["_did"]      = dids[candidate_mask]
    df_cands["_anchor"]   = df_cands["_did"].map(anchors)

    # Pre-compute occurrence counts per (pattern, delegate_id)
    occ_counts = (
        df_merged.groupby(["delegate_id", "pattern"], observed=True)
        .size()
        .to_dict()
    )

    results: list[ConcatCandidate] = []

    # Merge positional info onto candidates
    df_cands = df_cands.join(
        day_pos_f.set_index("row_index")[["_day", "_rank"]],
        how="left"
    )

    for row_index, row in df_cands.iterrows():
        pat_norm  = row["_pat_norm"]
        did       = row["_did"]
        anchor    = row["_anchor"] or ""
        raw_pat   = row["pattern"]
        day       = row.get("_day", "?")
        rank      = row.get("_rank", -1)

        # Collect neighbor delegate anchors
        neighbor_anchors: dict[str, str] = {}
        if pd.notna(rank):
            rank = int(rank)
            for delta in range(-neighbor_window, neighbor_window + 1):
                if delta == 0:
                    continue
                nbr_id = day_lookup.get((day, rank + delta))
                if nbr_id and nbr_id != did:
                    nbr_anchor = anchors.get(nbr_id, "")
                    if nbr_anchor:
                        neighbor_anchors[nbr_id] = nbr_anchor

        if not neighbor_anchors:
            continue

        # Try all split points
        best: tuple[float, str, str, str, str, float, float] | None = None
        own_score = _lev_dist(_norm(anchor), _norm(anchor))  # always 0 — anchor vs itself
        for sp in _split_points(pat_norm):
            left  = pat_norm[:sp].strip()
            right = pat_norm[sp:].strip()
            if len(left) < 3 or len(right) < 3:
                continue

            # Score of each half against own anchor
            left_vs_own  = _lev_dist(left,  anchor) if anchor else 1.0
            right_vs_own = _lev_dist(right, anchor) if anchor else 1.0

            # Find best-matching neighbor for each half
            best_left_id, best_left_score   = "", 1.0
            best_right_id, best_right_score = "", 1.0
            for nbr_id, nbr_anchor in neighbor_anchors.items():
                ld = _lev_dist(left, nbr_anchor)
                rd = _lev_dist(right, nbr_anchor)
                if ld < best_left_score:
                    best_left_score  = ld
                    best_left_id     = nbr_id
                if rd < best_right_score:
                    best_right_score = rd
                    best_right_id    = nbr_id

            # Case A: both halves match different neighbors (original logic)
            if (best_left_score  <= t_concat
                    and best_right_score <= t_concat
                    and best_left_id != best_right_id):
                combined = (best_left_score + best_right_score) / 2
                if best is None or combined < best[0]:
                    best = (combined, left, right,
                            best_left_id, best_right_id,
                            best_left_score, best_right_score)

            # Case B: left half ≈ own anchor, right half ≈ a neighbor
            # (concat where current delegate's name comes first)
            if (left_vs_own  <= t_concat
                    and best_right_score <= t_concat
                    and best_right_id):
                combined = (left_vs_own + best_right_score) / 2
                if best is None or combined < best[0]:
                    best = (combined, left, right,
                            did, best_right_id,
                            left_vs_own, best_right_score)

            # Case C: right half ≈ own anchor, left half ≈ a neighbor
            # (concat where current delegate's name comes second)
            if (right_vs_own <= t_concat
                    and best_left_score  <= t_concat
                    and best_left_id):
                combined = (best_left_score + right_vs_own) / 2
                if best is None or combined < best[0]:
                    best = (combined, left, right,
                            best_left_id, did,
                            best_left_score, right_vs_own)

        if best is None:
            continue

        combined, sl, sr, lid, rid, ls, rs = best
        occ_n = occ_counts.get((did, raw_pat), 1)
        results.append(ConcatCandidate(
            row_index         = int(row_index),
            pattern           = raw_pat,
            delegate_id       = did,
            anchor            = anchor,
            split_left        = sl,
            split_right       = sr,
            left_delegate_id  = lid,
            right_delegate_id = rid,
            left_score        = round(ls, 4),
            right_score       = round(rs, 4),
            combined_score    = round(combined, 4),
            n_occurrences     = int(occ_n),
        ))

    if not results:
        return pd.DataFrame(columns=list(ConcatCandidate._fields))

    df_out = pd.DataFrame(results)
    # Deduplicate: one row per (pattern, delegate_id) — keep the best-scoring
    df_out = (
        df_out.sort_values("combined_score")
        .drop_duplicates(subset=["pattern", "delegate_id"])
        .reset_index(drop=True)
    )
    return df_out


# ---------------------------------------------------------------------------
# Error type B — Fragment detection
# ---------------------------------------------------------------------------

class FragmentCandidate(NamedTuple):
    delegate_id:    str
    anchor:         str
    fragment_a:     str
    freq_a:         int
    fragment_b:     str
    freq_b:         int
    concat_norm:    str   # which concat (a+b or b+a or with space) was best
    concat_score:   float


def detect_fragment_errors(
    df_merged:     pd.DataFrame,
    at:            AnchorTable | None = None,
    t_frag:        float = DEFAULT_T_FRAG,
    min_patterns:  int   = DEFAULT_MIN_PATTERNS,
    province:      str | None = None,
    year_min:      int | None = None,
    year_max:      int | None = None,
) -> pd.DataFrame:
    """Detect delegates whose pattern list contains fragments of their true name.

    Parameters
    ----------
    df_merged :
        Merged occurrences+persons DataFrame.
    at :
        Pre-built AnchorTable.  If None, built internally.
    t_frag :
        Maximum normalised edit distance for reconstructed concat to match anchor.
    min_patterns :
        Minimum number of distinct patterns a delegate must have to be checked.
    province / year_min / year_max :
        Optional scope filters.

    Returns
    -------
    pd.DataFrame with columns matching FragmentCandidate fields, sorted by
    concat_score ascending (best matches first).
    """
    df = df_merged.copy()

    if province and "provincie" in df.columns:
        df = df[df["provincie"].astype(str).str.lower() == province.lower()]
    if year_min is not None and "j" in df.columns:
        df = df[df["j"].fillna(0) >= year_min]
    if year_max is not None and "j" in df.columns:
        df = df[df["j"].fillna(9999) <= year_max]

    if df.empty:
        return pd.DataFrame(columns=list(FragmentCandidate._fields))

    if at is None:
        at = build_anchor_table(df_merged)

    anchors = at.anchors

    # Per-delegate, per-pattern frequency
    freq_df = (
        df.assign(
            _did=df["delegate_id"].astype(str),
            _pat=df["pattern"].astype(str),
        )
        .groupby(["_did", "_pat"], observed=True)
        .size()
        .reset_index(name="freq")
    )

    results: list[FragmentCandidate] = []

    for did, grp in freq_df.groupby("_did", observed=True):
        if len(grp) < min_patterns:
            continue
        anchor = anchors.get(did, "")
        if not anchor:
            continue
        anchor_len = len(anchor)

        # Only consider sub-modal patterns (shorter than anchor)
        sub_pats = grp[grp["_pat"].map(_norm).map(len) < anchor_len * 0.75]
        if len(sub_pats) < 2:
            continue

        pats_list = sub_pats["_pat"].tolist()
        freqs_dict = dict(zip(sub_pats["_pat"], sub_pats["freq"]))

        # Test all pairs
        for i in range(len(pats_list)):
            for j in range(i + 1, len(pats_list)):
                pa = _norm(pats_list[i])
                pb = _norm(pats_list[j])
                if len(pa) < 2 or len(pb) < 2:
                    continue
                # Try all four concat variants
                candidates = [pa + pb, pb + pa, pa + " " + pb, pb + " " + pa]
                best_score = 1.0
                best_concat = ""
                for c in candidates:
                    d = _lev_dist(c, anchor)
                    if d < best_score:
                        best_score  = d
                        best_concat = c
                if best_score <= t_frag:
                    results.append(FragmentCandidate(
                        delegate_id  = did,
                        anchor       = anchor,
                        fragment_a   = pats_list[i],
                        freq_a       = int(freqs_dict.get(pats_list[i], 0)),
                        fragment_b   = pats_list[j],
                        freq_b       = int(freqs_dict.get(pats_list[j], 0)),
                        concat_norm  = best_concat,
                        concat_score = round(best_score, 4),
                    ))

    if not results:
        return pd.DataFrame(columns=list(FragmentCandidate._fields))

    df_out = (
        pd.DataFrame(results)
        .sort_values("concat_score")
        .reset_index(drop=True)
    )
    return df_out


# ---------------------------------------------------------------------------
# Convenience wrapper: run both detections and time them
# ---------------------------------------------------------------------------

def detect_all(
    df_merged: pd.DataFrame,
    *,
    t_concat:        float = DEFAULT_T_CONCAT,
    t_frag:          float = DEFAULT_T_FRAG,
    min_len_ratio:   float = DEFAULT_MIN_LEN_RATIO,
    neighbor_window: int   = DEFAULT_NEIGHBOR_WINDOW,
    min_patterns:    int   = DEFAULT_MIN_PATTERNS,
    province:        str | None = None,
    year_min:        int | None = None,
    year_max:        int | None = None,
    verbose:         bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run both concat and fragment detection.  Returns (concat_df, frag_df).

    Parameters mirror those of detect_concat_errors / detect_fragment_errors.
    """
    t0 = time.perf_counter()
    at = build_anchor_table(df_merged)
    t1 = time.perf_counter()
    if verbose:
        print(f"build_anchor_table  {(t1-t0)*1000:.0f} ms  "
              f"({len(at.anchors)} delegates, {len(at.day_pos)} rows)")

    concat_df = detect_concat_errors(
        df_merged, at=at,
        t_concat=t_concat, min_len_ratio=min_len_ratio,
        neighbor_window=neighbor_window,
        province=province, year_min=year_min, year_max=year_max,
    )
    t2 = time.perf_counter()
    if verbose:
        print(f"detect_concat_errors {(t2-t1)*1000:.0f} ms  → {len(concat_df)} candidates")

    frag_df = detect_fragment_errors(
        df_merged, at=at,
        t_frag=t_frag, min_patterns=min_patterns,
        province=province, year_min=year_min, year_max=year_max,
    )
    t3 = time.perf_counter()
    if verbose:
        print(f"detect_fragment_errors {(t3-t2)*1000:.0f} ms  → {len(frag_df)} candidates")
        print(f"total {(t3-t0)*1000:.0f} ms")

    return concat_df, frag_df
