"""Step 5: Union-Find merge groups and golden record construction."""
from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from .matching import MatchResult, MatchTier


# ---------------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------------

class UnionFind:
    def __init__(self, elements: list):
        self._parent = {e: e for e in elements}
        self._rank = {e: 0 for e in elements}

    def find(self, x):
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]  # path compression
            x = self._parent[x]
        return x

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1

    def groups(self) -> dict[Any, list]:
        out: dict[Any, list] = {}
        for e in self._parent:
            root = self.find(e)
            out.setdefault(root, []).append(e)
        return out


def build_merge_groups(
    match_results: list[MatchResult],
) -> dict[str, list[str]]:
    """
    Use Union-Find on AUTO_MERGE pairs (by record_id).
    Returns {root_record_id: [list of record_ids in group]}.
    """
    all_ids: set[str] = set()
    for mr in match_results:
        all_ids.add(mr.record_id_a)
        all_ids.add(mr.record_id_b)

    uf = UnionFind(list(all_ids))

    for mr in match_results:
        if mr.tier == MatchTier.AUTO_MERGE:
            uf.union(mr.record_id_a, mr.record_id_b)

    return uf.groups()


# ---------------------------------------------------------------------------
# Golden record construction
# ---------------------------------------------------------------------------

_FIELDS = ["full_name", "email", "phone", "address"]


def _pick_winner(records: pd.DataFrame, field: str):
    """
    Among rows that have a non-null value for `field`, pick the one
    with the latest `updated_at`. Returns (value, source_id, updated_at).
    """
    candidates = records[records[field].notna() & (records[field] != "")]
    if candidates.empty:
        return None, None, pd.NaT
    # Sort by updated_at descending (NaT treated as oldest via na_position='last')
    sorted_c = candidates.sort_values("updated_at", ascending=False, na_position="last")
    best = sorted_c.iloc[0]
    return best[field], best["_source_id"], best["updated_at"]


def build_golden_record(
    group_record_ids: list[str],
    df: pd.DataFrame,
    best_composite: float,
) -> tuple[dict, list[dict]]:
    """
    Build one golden record dict and a list of conflict dicts.
    df must be indexed by _record_id for fast lookup.
    """
    records = df[df["_record_id"].isin(group_record_ids)]
    golden_id = str(uuid.uuid4())

    golden: dict[str, Any] = {"golden_id": golden_id}
    conflicts: list[dict] = []

    for field in _FIELDS:
        winning_val, winning_src, winning_ts = _pick_winner(records, field)
        golden[field] = winning_val

        # Log conflicts: any other source with a different non-null value
        others = records[
            records[field].notna()
            & (records[field] != "")
            & (records["_source_id"] != winning_src)
            & (records[field] != winning_val)
        ]
        for _, oth in others.iterrows():
            conflicts.append(
                {
                    "golden_id": golden_id,
                    "field": field,
                    "winning_value": winning_val,
                    "winning_source": winning_src,
                    "winning_updated_at": winning_ts,
                    "overridden_value": oth[field],
                    "overridden_source": oth["_source_id"],
                    "overridden_updated_at": oth["updated_at"],
                }
            )

    golden["_source_count"] = len(records)
    golden["_source_ids"] = ",".join(records["_source_id"].tolist())
    golden["_source_record_ids"] = ",".join(records["_record_id"].tolist())
    golden["_match_score"] = round(best_composite, 2)
    golden["_created_at"] = datetime.now(timezone.utc).isoformat()

    return golden, conflicts


def build_all_golden_records(
    merge_groups: dict[str, list[str]],
    df: pd.DataFrame,
    match_results: list[MatchResult],
) -> tuple[list[dict], list[dict]]:
    """
    Build golden records for AUTO_MERGE groups. Singletons (records not in
    any AUTO_MERGE pair) each become their own golden record.
    Returns (golden_records, conflicts).
    """
    # Map record_id → best composite score for the group
    score_map: dict[str, float] = {}
    for mr in match_results:
        if mr.tier == MatchTier.AUTO_MERGE:
            for rid in (mr.record_id_a, mr.record_id_b):
                score_map[rid] = max(score_map.get(rid, 0), mr.composite_score)

    all_goldens: list[dict] = []
    all_conflicts: list[dict] = []

    merged_ids: set[str] = set()
    for root, group in merge_groups.items():
        if len(group) == 1:
            continue  # singletons handled below
        merged_ids.update(group)
        best_score = max(score_map.get(rid, 0.0) for rid in group)
        golden, conflicts = build_golden_record(group, df, best_score)
        all_goldens.append(golden)
        all_conflicts.extend(conflicts)

    # Singletons
    all_record_ids = set(df["_record_id"].tolist())
    singleton_ids = all_record_ids - merged_ids
    for rid in singleton_ids:
        golden, _ = build_golden_record([rid], df, 0.0)
        all_goldens.append(golden)

    return all_goldens, all_conflicts
