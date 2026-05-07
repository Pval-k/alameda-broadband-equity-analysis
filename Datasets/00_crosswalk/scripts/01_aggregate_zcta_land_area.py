import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aggregate Alameda block-to-ZCTA crosswalk to total land area by ZCTA."
    )
    parser.add_argument(
        "--input-csv",
        default="Datasets/00_crosswalk/csv/00_alameda_block_to_zcta_cleaned.csv",
        help="Path to cleaned Alameda block-to-ZCTA crosswalk CSV.",
    )
    parser.add_argument(
        "--output-csv",
        default="Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv",
        help="Path to ZCTA-level land-area output CSV.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Crosswalk CSV not found: {input_path}")

    df = pd.read_csv(
        input_path,
        dtype={"GEOID": "string", "ZCTA": "string"},
        low_memory=False,
    )
    required = {"GEOID", "ZCTA", "LAND_AREA"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input CSV missing required columns: {sorted(missing)}")

    df["GEOID"] = df["GEOID"].astype("string").str.strip().str.zfill(15)
    df["ZCTA"] = df["ZCTA"].astype("string").str.strip().str.zfill(5)
    df["LAND_AREA"] = pd.to_numeric(df["LAND_AREA"], errors="coerce").fillna(0)

    out = (
        df.groupby("ZCTA", as_index=False)
        .agg(
            total_land_area=("LAND_AREA", "sum"),
            block_count=("GEOID", "nunique"),
        )
        .sort_values("ZCTA")
        .reset_index(drop=True)
    )

    out.to_csv(output_path, index=False)
    print(f"Saved ZCTA land-area output: {output_path} ({len(out)} rows)")


if __name__ == "__main__":
    main()
