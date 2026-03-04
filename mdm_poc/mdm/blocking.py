"""Step 3: Candidate pair generation via blocking to avoid O(n²) comparisons."""
from __future__ import annotations
from collections import defaultdict
from itertools import combinations
import pandas as pd

try:
    import jellyfish
    def _soundex(s: str) -> str:
        return jellyfish.soundex(s) if s else "0000"
except ImportError:
    # Minimal Soundex fallback (American Soundex)
    _SOUNDEX_MAP = {
        "bfpv": "1", "cgjkqsxyz": "2", "dt": "3",
        "l": "4", "mn": "5", "r": "6",
    }

    def _soundex(s: str) -> str:
        if not s:
            return "0000"
        s = s.upper()
        code = s[0]
        prev = _char_code(s[0])
        for ch in s[1:]:
            c = _char_code(ch)
            if c and c != prev:
                code += c
            prev = c if c else prev
            if len(code) == 4:
                break
        return code.ljust(4, "0")

    def _char_code(ch: str) -> str:
        for group, digit in _SOUNDEX_MAP.items():
            if ch.lower() in group:
                return digit
        return ""


def build_blocks(df: pd.DataFrame, phone_prefix_length: int = 4) -> dict[str, list[int]]:
    """
    Block key = soundex(first token of norm_name) + "|" + first N digits of norm_phone.
    Returns mapping of block_key → list of DataFrame row indices.
    """
    blocks: dict[str, list[int]] = defaultdict(list)
    for i, row in df.iterrows():
        name = row["_norm_name"]
        phone = row["_norm_phone"]
        first_token = name.split()[0] if name.split() else ""
        sdx = _soundex(first_token)
        prefix = phone[:phone_prefix_length] if phone else "XXXX"
        key = f"{sdx}|{prefix}"
        blocks[key].append(i)
    return dict(blocks)


def generate_candidate_pairs(
    blocks: dict[str, list[int]], df: pd.DataFrame
) -> list[tuple[int, int]]:
    """Return deduplicated (i, j) pairs where i < j, from all blocks."""
    seen: set[tuple[int, int]] = set()
    pairs: list[tuple[int, int]] = []
    for indices in blocks.values():
        if len(indices) < 2:
            continue
        for a, b in combinations(indices, 2):
            pair = (min(a, b), max(a, b))
            if pair not in seen:
                seen.add(pair)
                pairs.append(pair)
    return pairs
