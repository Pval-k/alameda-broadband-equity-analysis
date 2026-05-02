import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate collapsed FCC rows to final zcta + TechCategory metrics."
        )
    )
    parser.add_argument(
        "--collapsed-csv",
        default="Datasets/02_FCC/csv/03_FCC_alameda_2020_block_tech_collapsed.csv",
        help="Path to collapsed intermediate CSV from script 03.",
    )
    parser.add_argument(
        "--output-csv",
        default="Datasets/02_FCC/csv/04_FCC_alameda_2020_zcta_tech_metrics.csv",
        help="Path to final zcta + tech metrics CSV.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    collapsed_path = Path(args.collapsed_csv)
    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not collapsed_path.exists():
        raise FileNotFoundError(f"Collapsed CSV not found: {collapsed_path}")

    df = pd.read_csv(collapsed_path, low_memory=False)
    required = {"zcta", "BlockCode", "TechCategory", "max_ad_down", "max_ad_up"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Collapsed CSV missing required columns: {sorted(missing)}")

    df["zcta"] = df["zcta"].astype(str).str.zfill(5)
    df = df[df["zcta"].str.match(r"^\d{5}$", na=False)]

    # Final level:
    # one row per zcta + TechCategory with median/p75 summaries of block-level maxima.
    final_df = (
        df.groupby(["zcta", "TechCategory"], as_index=False)
        .agg(
            block_count=("BlockCode", "nunique"),
            median_advertised_download_mbps=("max_ad_down", "median"),
            median_advertised_upload_mbps=("max_ad_up", "median"),
            p75_advertised_download_mbps=("max_ad_down", lambda x: x.quantile(0.75)),
            p75_advertised_upload_mbps=("max_ad_up", lambda x: x.quantile(0.75)),
            max_advertised_download_mbps=("max_ad_down", "max"),
            max_advertised_upload_mbps=("max_ad_up", "max"),
        )
        .sort_values(["zcta", "TechCategory"])
        .reset_index(drop=True)
    )

    final_df.to_csv(output_path, index=False)
    print(f"Saved final zcta+tech metrics: {output_path} ({len(final_df)} rows)")


if __name__ == "__main__":
    main()
