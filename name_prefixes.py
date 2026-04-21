"""name_prefixes.py — Dutch/French name-prefix (tussenvoegsel) utilities
with HTR/OCR error-variant awareness for Batavian Republic delegate data.

Public API
----------
PREFIX_VARIANTS : dict[str, list[str]]
    Canonical prefix → list of known HTR/OCR variants (lowercase).
    Edit this table to add new variants discovered in the data.

VARIANT_TO_CANONICAL : dict[str, str]
    Reverse lookup: any variant (or canonical) → canonical form.

DutchPrefixMatcher
    Class that encapsulates compiled regexes and provides:
        .normalize(s)         → str          — replace OCR variants with canonical
        .strip(s)             → (prefix, str) — remove leading prefix
        .is_prefix_only(s)    → bool         — True if s is nothing but prefix tokens
        .core(s)              → str          — normalize + strip (convenience)
        .similarity(a, b)     → float        — lev-distance after prefix normalisation

Module-level convenience instance: ``matcher`` (uses default PREFIX_VARIANTS).
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import NamedTuple

# ---------------------------------------------------------------------------
# Canonical prefix → OCR/HTR variant table
# ---------------------------------------------------------------------------
# Key   : canonical lowercase form (what the text *should* say)
# Value : list of known HTR/OCR variant spellings (also lowercase).
#         Do NOT include the canonical itself — it is always checked first.
#
# HTR confusions observed in 18th-century Dutch secretary hand:
#   v ↔ u / r        ("van" → "vau", "ran")
#   n ↔ u            ("van" → "vau", "den" → "deu")
#   t ↔ l            ("ten" → "len", "ter" → "ler")
#   e ↔ c            ("de"  → "dc")
#   initial dropped  ("van" → "an", "v")
#   space dropped    ("van de" → "vande", "van den" → "vanden")
#   apostrophe drop  ("'t"  → "t", "d'"  → "d")
#   diacritic add    ("de"  → "dé", "ter" → "tèr")
#   capital V ↔ U    (handled by lowercasing before lookup)

PREFIX_VARIANTS: dict[str, list[str]] = {
    # ── Multi-word prefixes (must be listed before their shorter sub-prefixes) ──
    "van der": [
        "vau der",   # n→u
        "van den",   # r→n (very common)
        "vander",    # space dropped
        "vau den",   # n→u + r→n
        "ran der",   # v→r
        "uan der",   # V→U misread
    ],
    "van den": [
        "vau den",   # n→u
        "van der",   # n→r
        "vanden",    # space dropped
        "vau der",   # n→u + n→r
        "ran den",
    ],
    "van de": [
        "vau de",    # n→u
        "van dc",    # e→c
        "vande",     # space dropped
        "vau dc",
        "ran de",
    ],
    "van 't": [
        "van t",     # apostrophe dropped
        "vau t",
        "van't",     # space dropped
        "vau't",
    ],
    "van het": [
        "vau het",
        "van het",
    ],
    "in 't": [
        "in t",      # apostrophe dropped
        "in't",      # space dropped
    ],
    # ── Single-word prefixes ───────────────────────────────────────────────
    "van": [
        "vau",       # n→u (most common OCR error in dataset)
        "ran",       # v→r
        "uan",       # V→U misread
        "vn",        # e omitted
        # NOTE: "an", "v", "u" omitted — too short, would match real surnames
    ],
    "ten": [
        "teu",       # n→u
        "len",       # t→l
    ],
    "ter": [
        "ler",       # t→l
        "tèr",       # diacritic added
    ],
    "te": [
        "le",        # t→l
    ],
    "den": [
        "deu",       # n→u
    ],
    "der": [
        "dèr",       # diacritic
    ],
    "des": [],
    "de": [
        "dc",        # e→c
        "dé",        # diacritic
    ],
    "di": [],
    "thoe": [
        "toe",       # h dropped
        "thoë",      # diacritic
    ],
    "tot": [],
    "du": [],
    "le": [],
    "la": [],
    # ── Apostrophe prefixes ────────────────────────────────────────────────
    "d'": ["d"],
    "l'": ["l"],
    # ── Article prefixes ──────────────────────────────────────────────────
    "'s-": ["s-", "s "],
    "'s ": ["s "],
    "'t ": ["t "],
    "t'":  ["t"],
    # ── Less common ───────────────────────────────────────────────────────
    "à":   ["\xe0", "a"],
    "en":  [],
    "het": [],
    "of":  [],
}

# ---------------------------------------------------------------------------
# Reverse lookup: variant (or canonical) → canonical
# ---------------------------------------------------------------------------
VARIANT_TO_CANONICAL: dict[str, str] = {}
for _canonical, _variants in PREFIX_VARIANTS.items():
    VARIANT_TO_CANONICAL[_canonical] = _canonical
    for _v in _variants:
        # Only add if not already claimed by a longer canonical (longest-first wins)
        if _v not in VARIANT_TO_CANONICAL:
            VARIANT_TO_CANONICAL[_v] = _canonical


# ---------------------------------------------------------------------------
# Helper: Levenshtein distance (reuses pattern_merge if available)
# ---------------------------------------------------------------------------
def _lev(a: str, b: str) -> float:
    """Normalised Levenshtein [0, 1]. Uses rapidfuzz if installed."""
    if not a and not b:
        return 0.0
    try:
        from rapidfuzz.distance import Levenshtein
        return Levenshtein.normalized_distance(a, b)
    except ImportError:
        pass
    la, lb = len(a), len(b)
    if la == 0 or lb == 0:
        return 1.0
    prev = list(range(lb + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * lb
        for j, cb in enumerate(b, 1):
            curr[j] = min(prev[j] + 1, curr[j - 1] + 1,
                          prev[j - 1] + (ca != cb))
        prev = curr
    return prev[lb] / max(la, lb)


# ---------------------------------------------------------------------------
# DutchPrefixMatcher
# ---------------------------------------------------------------------------

class StripResult(NamedTuple):
    prefix: str     # canonical prefix found (empty string if none)
    core:   str     # remainder after stripping the prefix


class DutchPrefixMatcher:
    """Compile-once matcher for Dutch/French name prefixes including HTR variants.

    Parameters
    ----------
    variants : dict[str, list[str]] | None
        Override PREFIX_VARIANTS with a custom table.  None → use module default.
    """

    def __init__(self, variants: dict[str, list[str]] | None = None) -> None:
        table = variants if variants is not None else PREFIX_VARIANTS

        # Build canonical→variants map and reverse lookup
        self._table = table
        self._v2c: dict[str, str] = {}
        for canonical, var_list in table.items():
            self._v2c[canonical] = canonical
            for v in var_list:
                if v not in self._v2c:
                    self._v2c[v] = canonical

        # All tokens to match (canonical + all variants), longest first
        all_tokens = sorted(self._v2c.keys(), key=len, reverse=True)

        # Regex to match any prefix token at the START of a string.
        # Requires whitespace or end-of-string after the token so short variants
        # like "vn" don't eat the first letter of a real surname.
        self._strip_re = re.compile(
            r"^(?:" + "|".join(re.escape(t) for t in all_tokens) + r")(?=\s|$)\s*",
            re.IGNORECASE,
        )
        # Regex to match any prefix token ANYWHERE (for normalization scan)
        self._token_re = re.compile(
            r"\b(?:" + "|".join(re.escape(t) for t in all_tokens) + r")\b",
            re.IGNORECASE,
        )
        # Set of all known tokens (lower) for fast membership test
        self._token_set: frozenset[str] = frozenset(t.lower() for t in all_tokens)

    # ── Public API ────────────────────────────────────────────────────────

    def normalize(self, s: str) -> str:
        """Replace any leading OCR/HTR prefix variant with its canonical form.

        Only the *leading* prefix is normalised (prefixes appear at the start
        of a name, not in the middle of a surname).

        Example: "vau Wassenaer" → "van Wassenaer"
        """
        s = str(s).strip()
        m = self._strip_re.match(s.lower())
        if not m:
            return s
        variant_token = s[:m.end()].strip()
        canonical = self._v2c.get(variant_token.lower(), variant_token)
        rest = s[m.end():]
        return canonical + (" " if rest else "") + rest

    def strip(self, s: str) -> StripResult:
        """Remove leading prefix from s (canonical or OCR variant).

        Returns a StripResult(prefix, core) where prefix is the *canonical*
        form of the prefix found (empty if none) and core is the remainder.

        Example: "vau Wassenaer" → StripResult("van", "Wassenaer")
        """
        s_norm = str(s).strip()
        m = self._strip_re.match(s_norm.lower())
        if not m:
            return StripResult("", s_norm)
        variant_token = s_norm[:m.end()].strip()
        canonical = self._v2c.get(variant_token.lower(), variant_token)
        core = s_norm[m.end():].strip()
        return StripResult(canonical, core)

    def is_prefix_only(self, s: str, min_core_len: int = 2) -> bool:
        """Return True if *s* consists entirely of prefix tokens with no
        meaningful core surname remaining.

        Parameters
        ----------
        s             : string to test
        min_core_len  : minimum characters a core must have to count as real
        """
        remaining = str(s).strip().lower()
        while remaining:
            m = self._strip_re.match(remaining)
            if not m:
                break
            remaining = remaining[m.end():].strip()
        return len(remaining) < min_core_len

    def core(self, s: str) -> str:
        """Normalize OCR variant then strip prefix — returns bare surname."""
        return self.strip(self.normalize(s)).core

    @lru_cache(maxsize=4096)
    def similarity(self, a: str, b: str) -> float:
        """Levenshtein similarity after normalising and stripping prefixes.

        Computes the minimum of four scores:
          1. raw a vs raw b
          2. core(a) vs core(b)           — both stripped
          3. core(a) vs raw b             — strip only a
          4. raw a   vs core(b)           — strip only b

        This handles the case where one string has an OCR'd prefix and the
        other does not, without penalising legitimate compound names.
        """
        na = self.normalize(str(a).lower().strip())
        nb = self.normalize(str(b).lower().strip())
        ca = self.strip(na).core
        cb = self.strip(nb).core
        scores = [
            _lev(na, nb),
            _lev(ca, cb) if ca and cb else 1.0,
            _lev(ca, nb) if ca else 1.0,
            _lev(na, cb) if cb else 1.0,
        ]
        return min(scores)

    def variants(self, canonical: str) -> list[str]:
        """Return all known variants for a canonical prefix (including itself)."""
        c = canonical.lower()
        return [c] + list(self._table.get(c, []))

    def canonical(self, token: str) -> str | None:
        """Return the canonical form of a prefix token, or None if not known."""
        return self._v2c.get(token.lower())

    def __repr__(self) -> str:  # pragma: no cover
        return f"DutchPrefixMatcher({len(self._table)} prefixes, {len(self._v2c)} tokens)"


# ---------------------------------------------------------------------------
# Module-level default instance
# ---------------------------------------------------------------------------
matcher = DutchPrefixMatcher()
