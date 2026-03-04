"""
MDM pipeline entry point.

Usage:
    python run_mdm.py [--config config.yaml]
"""
import argparse
import os
import sys
import time

import yaml

# Ensure we resolve relative paths from the script's directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_config(path: str) -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    # Resolve relative paths relative to config file location
    cfg_dir = os.path.dirname(os.path.abspath(path))
    for source in cfg.get("sources", []):
        if not os.path.isabs(source["path"]):
            source["path"] = os.path.join(cfg_dir, source["path"])
    if not os.path.isabs(cfg.get("output_dir", "output/")):
        cfg["output_dir"] = os.path.join(cfg_dir, cfg["output_dir"])
    return cfg


def main(config_path: str) -> None:
    print("=" * 60)
    print("  MDM Pipeline — Customer/Contact Golden Record Builder")
    print("=" * 60)

    # ── Load config ──────────────────────────────────────────────
    cfg = load_config(config_path)
    weights = cfg["matching"]["weights"]
    thresholds = cfg["matching"]["thresholds"]
    phone_prefix_len = cfg["blocking"].get("phone_prefix_length", 4)
    output_dir = cfg["output_dir"]

    # ── Step 1: Ingest ───────────────────────────────────────────
    from mdm.ingest import load_all_sources
    print("\n[1/6] Ingesting source files...")
    t0 = time.time()
    df = load_all_sources(cfg)
    print(f"      Loaded {len(df)} records from {len(cfg['sources'])} sources "
          f"({time.time()-t0:.2f}s)")

    # ── Step 2: Normalize ────────────────────────────────────────
    from mdm.normalize import normalize_dataframe
    print("[2/6] Normalizing fields...")
    t0 = time.time()
    df = normalize_dataframe(df)
    print(f"      Normalized {len(df)} records ({time.time()-t0:.2f}s)")

    # ── Step 3: Block ────────────────────────────────────────────
    from mdm.blocking import build_blocks, generate_candidate_pairs
    print("[3/6] Building blocks and generating candidate pairs...")
    t0 = time.time()
    blocks = build_blocks(df, phone_prefix_length=phone_prefix_len)
    pairs = generate_candidate_pairs(blocks, df)
    print(f"      {len(blocks)} blocks -> {len(pairs)} candidate pairs ({time.time()-t0:.2f}s)")

    # ── Step 4: Score ────────────────────────────────────────────
    from mdm.matching import score_candidate_pairs
    print("[4/6] Scoring candidate pairs...")
    t0 = time.time()
    match_results = score_candidate_pairs(pairs, df, weights, thresholds)
    auto = sum(1 for m in match_results if m.tier.value == "AUTO_MERGE")
    review = sum(1 for m in match_results if m.tier.value == "REVIEW")
    print(f"      {len(match_results)} matches: {auto} AUTO_MERGE, {review} REVIEW "
          f"({time.time()-t0:.2f}s)")

    # ── Step 5: Merge ────────────────────────────────────────────
    from mdm.merging import build_merge_groups, build_all_golden_records
    print("[5/6] Building merge groups and golden records...")
    t0 = time.time()
    merge_groups = build_merge_groups(match_results)
    golden_records, conflicts = build_all_golden_records(merge_groups, df, match_results)
    print(f"      {len(golden_records)} golden records, {len(conflicts)} field conflicts "
          f"({time.time()-t0:.2f}s)")

    # ── Step 6: Output ───────────────────────────────────────────
    from mdm.output import write_outputs
    print("[6/6] Writing output files...")
    write_outputs(golden_records, match_results, conflicts, df, output_dir)

    # ── Summary ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  Source records     : {len(df)}")
    print(f"  Candidate pairs    : {len(pairs)}")
    print(f"  AUTO_MERGE matches : {auto}")
    print(f"  REVIEW matches     : {review}")
    print(f"  Golden records     : {len(golden_records)}")
    print(f"  Field conflicts    : {len(conflicts)}")
    reduction = (1 - len(golden_records) / len(df)) * 100 if len(df) else 0
    print(f"  Record reduction   : {reduction:.1f}%")
    print("=" * 60)

    if review > 0:
        print(f"\n  ! {review} pair(s) need human review → output/review_queue.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MDM pipeline")
    parser.add_argument(
        "--config",
        default=os.path.join(BASE_DIR, "config.yaml"),
        help="Path to config.yaml (default: config.yaml next to this script)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"ERROR: Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    main(args.config)
