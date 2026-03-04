"""Step 6: Write output CSV files."""
from __future__ import annotations
import os
import pandas as pd
from .matching import MatchResult, MatchTier


def write_outputs(
    golden_records: list[dict],
    match_results: list[MatchResult],
    conflicts: list[dict],
    df: pd.DataFrame,
    output_dir: str,
) -> None:
    os.makedirs(output_dir, exist_ok=True)

    # --- golden_records.csv ---
    golden_df = pd.DataFrame(golden_records)[
        [
            "golden_id", "full_name", "email", "phone", "address",
            "_source_count", "_source_ids", "_source_record_ids", "_match_score", "_created_at",
        ]
    ]
    golden_df.to_csv(os.path.join(output_dir, "golden_records.csv"), index=False)

    # --- match_report.csv ---
    # Build golden_id lookup: record_id → golden_id
    rid_to_golden: dict[str, str] = {}
    for g in golden_records:
        for rid in g.get("_source_record_ids", "").split(","):
            rid_to_golden[rid.strip()] = g["golden_id"]

    report_rows = []
    for mr in match_results:
        for rid in (mr.record_id_a, mr.record_id_b):
            rec = df[df["_record_id"] == rid].iloc[0]
            report_rows.append(
                {
                    "golden_id": rid_to_golden.get(rid, ""),
                    "source_record_id": rid,
                    "source_system": rec["_source_id"],
                    "source_row_index": rec["_source_row"],
                    "match_tier": mr.tier.value,
                    "composite_score": mr.composite_score,
                }
            )
    # Deduplicate (a record can appear in multiple pairs)
    report_df = pd.DataFrame(report_rows).drop_duplicates(subset=["source_record_id"])
    report_df.to_csv(os.path.join(output_dir, "match_report.csv"), index=False)

    # --- conflicts.csv ---
    conflicts_df = pd.DataFrame(conflicts) if conflicts else pd.DataFrame(
        columns=[
            "golden_id", "field", "winning_value", "winning_source",
            "winning_updated_at", "overridden_value", "overridden_source",
            "overridden_updated_at",
        ]
    )
    conflicts_df.to_csv(os.path.join(output_dir, "conflicts.csv"), index=False)

    # --- review_queue.csv ---
    review_rows = []
    for mr in match_results:
        if mr.tier != MatchTier.REVIEW:
            continue
        rec_a = df[df["_record_id"] == mr.record_id_a].iloc[0]
        rec_b = df[df["_record_id"] == mr.record_id_b].iloc[0]
        review_rows.append(
            {
                "record_id_a": mr.record_id_a,
                "source_a": rec_a["_source_id"],
                "name_a": rec_a["full_name"],
                "email_a": rec_a["email"],
                "phone_a": rec_a["phone"],
                "address_a": rec_a["address"],
                "record_id_b": mr.record_id_b,
                "source_b": rec_b["_source_id"],
                "name_b": rec_b["full_name"],
                "email_b": rec_b["email"],
                "phone_b": rec_b["phone"],
                "address_b": rec_b["address"],
                "composite_score": mr.composite_score,
                "name_score": mr.name_score,
                "email_score": mr.email_score,
                "phone_score": mr.phone_score,
                "address_score": mr.address_score,
            }
        )
    review_df = pd.DataFrame(review_rows)
    review_df.to_csv(os.path.join(output_dir, "review_queue.csv"), index=False)

    print(f"\n=== Output written to '{output_dir}' ===")
    print(f"  golden_records.csv : {len(golden_df)} records")
    print(f"  match_report.csv   : {len(report_df)} source rows linked")
    print(f"  conflicts.csv      : {len(conflicts_df)} field conflicts")
    print(f"  review_queue.csv   : {len(review_df)} pairs for review")
