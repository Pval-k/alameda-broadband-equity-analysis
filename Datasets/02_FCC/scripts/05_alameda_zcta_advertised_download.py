"""Derive one advertised download speed per ZCTA from FCC block-level data (Option A).

Two-step aggregation:
  1. Per block: take the max of `max_ad_down` across TechCategory rows
     (best technology buyable at that block — fiber wins over DSL, etc.).
  2. Per ZCTA: take the median of those per-block bests across blocks
     (the "typical block" experience for the ZCTA).

Median is preferred over mean because one fiber-heavy street can pull a mean
to a number nobody else in the ZCTA actually receives.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Aggregate FCC block+tech rows to one advertised download speed per ZCTA "
            "using max-across-tech per block, then median across blocks."
        )
    )
    p.add_argument(
        "--input-csv",
        default="Datasets/02_FCC/csv/03_FCC_alameda_2020_block_tech_collapsed.csv",
        help="Block-level FCC CSV with `zcta`, `BlockCode`, `TechCategory`, `max_ad_down`.",
    )
    p.add_argument(
        "--output-csv",
        default="Datasets/02_FCC/csv/05_alameda_zcta_advertised_download.csv",
        help="Output one-row-per-ZCTA advertised download CSV.",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    in_path = Path(args.input_csv)
    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(f"Block-level FCC CSV not found: {in_path}")

    df = pd.read_csv(
        in_path,
        dtype={"zcta": str, "BlockCode": str},
        low_memory=False,
    )
    required = {"zcta", "BlockCode", "max_ad_down"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Input CSV missing required columns: {sorted(missing)}. "
            f"Available: {list(df.columns)}"
        )

    df["zcta"] = df["zcta"].astype(str).str.strip().str.zfill(5)
    df["BlockCode"] = df["BlockCode"].astype(str).str.strip().str.zfill(15)
    df["max_ad_down"] = pd.to_numeric(df["max_ad_down"], errors="coerce")
    df = df.dropna(subset=["max_ad_down"])
    df = df[df["zcta"].str.match(r"^\d{5}$", na=False)]

    # Step 1: per block, best tech available.
    block_best = (
        df.groupby(["zcta", "BlockCode"], as_index=False)
        .agg(block_best_ad_down=("max_ad_down", "max"))
    )

    # Step 2: per ZCTA, the typical block's best tech (+ context stats).
    zcta_summary = (
        block_best.groupby("zcta", as_index=False)
        .agg(
            advertised_download_mbps=("block_best_ad_down", "median"),
            mean_block_best_ad_down=("block_best_ad_down", "mean"),
            p25_block_best_ad_down=(
                "block_best_ad_down",
                lambda x: x.quantile(0.25),
            ),
            p75_block_best_ad_down=(
                "block_best_ad_down",
                lambda x: x.quantile(0.75),
            ),
            block_count=("BlockCode", "nunique"),
        )
        .sort_values("zcta")
    )

    zcta_summary.to_csv(out_path, index=False, float_format="%.6f")
    print(
        f"Wrote {len(zcta_summary)} ZCTAs to {out_path} "
        f"(from {len(block_best):,} unique zcta+block rows)."
    )


if __name__ == "__main__":
    main()
