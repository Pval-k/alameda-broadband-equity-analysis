import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Collapse mapped FCC rows to one row per "
            "zcta + BlockCode + TechCategory before final aggregation."
        )
    )
    parser.add_argument(
        "--mapped-csv",
        default="FCC/csv/02_FCC_alameda_2020_block_zcta_mapped.csv",
        help="Path to mapped block->ZCTA CSV from script 02.",
    )
    parser.add_argument(
        "--output-csv",
        default="FCC/csv/03_FCC_alameda_2020_block_tech_collapsed.csv",
        help="Path to collapsed intermediate CSV.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    mapped_path = Path(args.mapped_csv)
    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not mapped_path.exists():
        raise FileNotFoundError(f"Mapped CSV not found: {mapped_path}")

    df = pd.read_csv(mapped_path, low_memory=False)
    required = {"zcta", "BlockCode", "TechCategory", "max_ad_down", "max_ad_up"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Mapped CSV missing required columns: {sorted(missing)}")

    # Safety step:
    # Keep exactly one value per zcta + block + tech so no repeated rows
    # can overweight the final zcta + tech metrics.
    collapsed = (
        df.groupby(["zcta", "BlockCode", "TechCategory"], as_index=False)
        .agg(
            max_ad_down=("max_ad_down", "max"),
            max_ad_up=("max_ad_up", "max"),
        )
        .sort_values(["zcta", "BlockCode", "TechCategory"])
        .reset_index(drop=True)
    )

    collapsed.to_csv(output_path, index=False)
    print(f"Saved collapsed intermediate: {output_path} ({len(collapsed)} rows)")


if __name__ == "__main__":
    main()
