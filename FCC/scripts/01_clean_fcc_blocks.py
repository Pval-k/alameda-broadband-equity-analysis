import argparse
from pathlib import Path

import pandas as pd


TECH_CATEGORY_MAP = {
    10: "Copper (DSL/VDSL)",
    11: "Copper (DSL/VDSL)",
    12: "Copper (DSL/VDSL)",
    41: "Cable",
    42: "Cable",
    43: "Cable",
    50: "Fiber to the Premises",
    60: "Satellite (GSO/NGSO)",
    70: "Fixed Wireless",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Clean and aggregate FCC Dec 2020 fixed broadband data to Alameda block level."
    )
    parser.add_argument(
        "--input-csv",
        default="FCC/CA_FCC_fixed_Dec2020.csv",
        help="Path to raw FCC CSV.",
    )
    parser.add_argument(
        "--output-csv",
        default="FCC/fcc_alameda_2020_block_level.csv",
        help="Path to block-level output CSV.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Input FCC file not found: {input_path}")

   
    # Residential alignment:
    # Consumer == 1 keeps service tiers closer to M-Lab consumer-initiated tests.
    usecols = ["BlockCode", "TechCode", "Consumer", "Business", "MaxAdDown", "MaxAdUp"]
    df = pd.read_csv(input_path, usecols=usecols, low_memory=False)

    # Keep only residential fixed broadband observations.
    # - Consumer == 1
    # - exclude Business == 1 when possible
    # - Alameda County GEOID prefix: 06001
    df["BlockCode"] = df["BlockCode"].astype(str).str.strip().str.zfill(15)
    df["Consumer"] = pd.to_numeric(df["Consumer"], errors="coerce")
    df["Business"] = pd.to_numeric(df["Business"], errors="coerce")
    df = df[(df["Consumer"] == 1) & (df["BlockCode"].str.startswith("06001"))]
    business_excluded = df[df["Business"] != 1]
    if len(business_excluded) > 0:
        df = business_excluded
    else:
        print(
            "Warning: Business flag overlaps all consumer rows in this FCC file; "
            "keeping Consumer==1 rows to preserve residential availability records."
        )

    # Keep required fields only.
    df = df[["BlockCode", "TechCode", "MaxAdDown", "MaxAdUp"]].copy()

    # Clean numeric fields.
    # Coerce to numeric Mbps and drop null/negative values.
    df["TechCode"] = pd.to_numeric(df["TechCode"], errors="coerce")
    df["MaxAdDown"] = pd.to_numeric(df["MaxAdDown"], errors="coerce")
    df["MaxAdUp"] = pd.to_numeric(df["MaxAdUp"], errors="coerce")
    df = df.dropna(subset=["BlockCode", "TechCode", "MaxAdDown", "MaxAdUp"])
    df = df[(df["MaxAdDown"] >= 0) & (df["MaxAdUp"] >= 0)]

    # Remove likely empty/water-like blocks.
    # Bay-adjacent water blocks can appear in county geographies; requiring positive
    # advertised speed reduces empty coverage artifacts before ZCTA summaries.
    df = df[df["MaxAdDown"] > 0]

    # Block-level aggregation:
    # keep tech codes by summarizing at BlockCode + TechCode.
    def summarize_blocks(block_df: pd.DataFrame) -> pd.DataFrame:
        grouped = block_df.groupby(["BlockCode", "TechCode"], as_index=False).agg(
            max_ad_down=("MaxAdDown", "max"),
            max_ad_up=("MaxAdUp", "max"),
        )
        return grouped

    out_df = summarize_blocks(df)
    out_df["TechCode"] = out_df["TechCode"].astype(int)
    out_df["TechCategory"] = out_df["TechCode"].map(TECH_CATEGORY_MAP).fillna("Other/Unknown")
    out_df = out_df[
        [
            "BlockCode",
            "TechCode",
            "TechCategory",
            "max_ad_down",
            "max_ad_up",
        ]
    ]

    out_df.to_csv(output_path, index=False)
    print(f"Saved block-level FCC output: {output_path} ({len(out_df)} rows)")


if __name__ == "__main__":
    main()
