"""Step 4: Scoring engine with MatchResult and tier classification."""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import List

import pandas as pd
from rapidfuzz import fuzz
from rapidfuzz.distance import Levenshtein


class MatchTier(str, Enum):
    AUTO_MERGE = "AUTO_MERGE"
    REVIEW = "REVIEW"
    NO_MATCH = "NO_MATCH"


@dataclass
class MatchResult:
    idx_a: int
    idx_b: int
    record_id_a: str
    record_id_b: str
    name_score: float
    email_score: float
    phone_score: float
    address_score: float
    composite_score: float
    hard_match: bool
    tier: MatchTier
    field_scores: dict = field(default_factory=dict)


def _score_name(a: str, b: str) -> float:
    if not a and not b:
        return 100.0
    if not a or not b:
        return 0.0
    return fuzz.WRatio(a, b)


def _score_email(a: str, b: str) -> float:
    if not a and not b:
        return 100.0
    if not a or not b:
        return 0.0
    if a == b:
        return 100.0
    return fuzz.ratio(a, b)


def _score_phone(a: str, b: str) -> float:
    if not a and not b:
        return 100.0
    if not a or not b:
        return 0.0
    if a == b:
        return 100.0
    return fuzz.ratio(a, b)


def _score_address(a: str, b: str) -> float:
    if not a and not b:
        return 100.0
    if not a or not b:
        return 0.0
    return Levenshtein.normalized_similarity(a, b) * 100


def score_candidate_pairs(
    pairs: list[tuple[int, int]],
    df: pd.DataFrame,
    weights: dict,
    thresholds: dict,
) -> List[MatchResult]:
    """Score each candidate pair. Returns only REVIEW and AUTO_MERGE results."""
    auto_threshold = thresholds["auto_merge"]
    review_threshold = thresholds["review"]

    results: List[MatchResult] = []

    for idx_a, idx_b in pairs:
        row_a = df.loc[idx_a]
        row_b = df.loc[idx_b]

        # Hard match: exact normalized name AND exact normalized phone (both non-empty)
        hard = (
            bool(row_a["_norm_name"])
            and row_a["_norm_name"] == row_b["_norm_name"]
            and bool(row_a["_norm_phone"])
            and row_a["_norm_phone"] == row_b["_norm_phone"]
        )

        ns = _score_name(row_a["_norm_name"], row_b["_norm_name"])
        es = _score_email(row_a["_norm_email"], row_b["_norm_email"])
        ps = _score_phone(row_a["_norm_phone"], row_b["_norm_phone"])
        as_ = _score_address(row_a["_norm_address"], row_b["_norm_address"])

        composite = (
            ns * weights.get("full_name", 0.35)
            + es * weights.get("email", 0.30)
            + ps * weights.get("phone", 0.20)
            + as_ * weights.get("address", 0.15)
        )

        if hard or composite >= auto_threshold:
            tier = MatchTier.AUTO_MERGE
        elif composite >= review_threshold:
            tier = MatchTier.REVIEW
        else:
            # Skip NO_MATCH entirely
            continue

        results.append(
            MatchResult(
                idx_a=idx_a,
                idx_b=idx_b,
                record_id_a=row_a["_record_id"],
                record_id_b=row_b["_record_id"],
                name_score=round(ns, 2),
                email_score=round(es, 2),
                phone_score=round(ps, 2),
                address_score=round(as_, 2),
                composite_score=round(composite, 2),
                hard_match=hard,
                tier=tier,
                field_scores={
                    "name": round(ns, 2),
                    "email": round(es, 2),
                    "phone": round(ps, 2),
                    "address": round(as_, 2),
                },
            )
        )

    return results
