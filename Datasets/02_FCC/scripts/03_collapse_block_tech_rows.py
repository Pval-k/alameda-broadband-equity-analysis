import argparse
from pathlib import Path

import pandas as pd


def normalize_block_geoid(series: pd.Series) -> pd.Series:
    """Preserve 15-digit Census block GEOIDs with leading zeros (e.g. 06001...)."""
    out = []
    for v in series:
        if pd.isna(v) or (isinstance(v, float) and pd.isna(v)):
            out.append(pd.NA)
            continue
        s = str(v).strip()
        if s.endswith(".0"):
            s = s[:-2]
        digits = "".join(c for c in s if c.isdigit())
        if not digits:
            out.append(s)
        else:
            out.append(digits.zfill(15))
    return pd.Series(out, dtype="string", index=series.index)


def normalize_zcta(series: pd.Series) -> pd.Series:
    out = []
    for v in series:
        if pd.isna(v) or (isinstance(v, float) and pd.isna(v)):
            out.append(pd.NA)
            continue
        s = str(v).strip()
        if s.endswith(".0"):
            s = s[:-2]
        digits = "".join(c for c in s if c.isdigit())
        if not digits:
            out.append(s)
        else:
            out.append(digits.zfill(5))
    return pd.Series(out, dtype="string", index=series.index)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Collapse mapped FCC rows to one row per "
            "zcta + BlockCode + TechCategory before final aggregation."
        )
    )
    parser.add_argument(
        "--mapped-csv",
        default="Datasets/02_FCC/csv/02_FCC_alameda_2020_block_zcta_mapped.csv",
        help="Path to mapped block->ZCTA CSV from script 02.",
    )
    parser.add_argument(
        "--output-csv",
        default="Datasets/02_FCC/csv/03_FCC_alameda_2020_block_tech_collapsed.csv",
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

    df = pd.read_csv(
        mapped_path,
        dtype={"BlockCode": "string", "zcta": "string"},
        low_memory=False,
    )
    required = {"zcta", "BlockCode", "TechCategory", "max_ad_down", "max_ad_up"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Mapped CSV missing required columns: {sorted(missing)}")

    # Force 15-digit block GEOIDs and 5-digit ZCTAs (leading zeros preserved).
    df["BlockCode"] = normalize_block_geoid(df["BlockCode"])
    df["zcta"] = normalize_zcta(df["zcta"])

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

    collapsed["BlockCode"] = normalize_block_geoid(collapsed["BlockCode"])
    collapsed["zcta"] = normalize_zcta(collapsed["zcta"])

    collapsed.to_csv(output_path, index=False)
    print(f"Saved collapsed intermediate: {output_path} ({len(collapsed)} rows)")


if __name__ == "__main__":
    main()
