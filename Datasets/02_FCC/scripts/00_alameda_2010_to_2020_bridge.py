"""Build a 1-to-1 Alameda County bridge from 2010 tabulation block GEOIDs to 2020 GEOIDs."""

import argparse
from pathlib import Path

import pandas as pd


def _geoid15_col(
    state: pd.Series, county: pd.Series, tract: pd.Series, blk: pd.Series
) -> pd.Series:
    return (
        state.str.strip().str.zfill(2)
        + county.str.strip().str.zfill(3)
        + tract.str.strip().str.zfill(6)
        + blk.str.strip().str.zfill(4)
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Filter California Census 2010→2020 block relationship file to Alameda County "
            "and keep one 2020 block per 2010 block (largest AREALAND_INT wins)."
        )
    )
    parser.add_argument(
        "--input-txt",
        default="Datasets/02_FCC/csv/raw_translate_fcc_2010_to_2020.txt",
        help="Pipe-delimited Census block relationship file for California.",
    )
    parser.add_argument(
        "--output-csv",
        default="Datasets/02_FCC/csv/00_alameda_2010_to_2020_bridge.csv",
        help="Output 1-to-1 bridge CSV.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    input_path = Path(args.input_txt)
    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Relationship file not found: {input_path}")

    df = pd.read_csv(input_path, sep="|", dtype=str, low_memory=False)

    required = {
        "STATE_2010",
        "COUNTY_2010",
        "TRACT_2010",
        "BLK_2010",
        "STATE_2020",
        "COUNTY_2020",
        "TRACT_2020",
        "BLK_2020",
        "AREALAND_INT",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Relationship file missing columns: {sorted(missing)}")

    df = df[
        (df["STATE_2010"].str.strip() == "06")
        & (df["COUNTY_2010"].str.strip() == "001")
    ].copy()

    df["geoid_2010"] = _geoid15_col(
        df["STATE_2010"], df["COUNTY_2010"], df["TRACT_2010"], df["BLK_2010"]
    )
    df["geoid_2020"] = _geoid15_col(
        df["STATE_2020"], df["COUNTY_2020"], df["TRACT_2020"], df["BLK_2020"]
    )
    df["AREALAND_INT"] = pd.to_numeric(df["AREALAND_INT"], errors="coerce").fillna(0)

    df = df.sort_values("AREALAND_INT", ascending=False)
    df = df.drop_duplicates(subset=["geoid_2010"], keep="first")

    out = df[["geoid_2010", "geoid_2020", "AREALAND_INT"]].sort_values("geoid_2010")
    out.to_csv(output_path, index=False)
    print(f"Saved bridge: {output_path} ({len(out)} rows)")


if __name__ == "__main__":
    main()
