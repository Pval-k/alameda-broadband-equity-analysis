import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Map FCC block-level output to ZCTAs using a 2020 block-to-ZCTA crosswalk."
    )
    parser.add_argument(
        "--block-csv",
        default="Datasets/02_FCC/csv/01_FCC_alameda_2020_block_level.csv",
        help="Path to block-level FCC CSV from script 01.",
    )
    parser.add_argument(
        "--crosswalk-csv",
        default="Datasets/00_crosswalk/csv/00_alameda_block_to_zcta_cleaned.csv",
        help="Path to Alameda block-to-ZCTA cleaned CSV.",
    )
    parser.add_argument(
        "--output-csv",
        default="Datasets/02_FCC/csv/02_FCC_alameda_2020_block_zcta_mapped.csv",
        help="Path to mapped block->ZCTA output CSV.",
    )
    return parser


def pick_column(df: pd.DataFrame, options: list[str]) -> str | None:
    lower_map = {c.lower(): c for c in df.columns}
    for opt in options:
        if opt.lower() in lower_map:
            return lower_map[opt.lower()]
    return None


def main() -> None:
    args = build_parser().parse_args()
    block_path = Path(args.block_csv)
    crosswalk_path = Path(args.crosswalk_csv)
    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not block_path.exists():
        raise FileNotFoundError(f"Block-level FCC file not found: {block_path}")
    if not crosswalk_path.exists():
        raise FileNotFoundError(f"Crosswalk file not found: {crosswalk_path}")

    block_df = pd.read_csv(block_path, low_memory=False)
    crosswalk_df = pd.read_csv(crosswalk_path, low_memory=False)
    required_block_cols = {"BlockCode", "TechCode", "TechCategory", "max_ad_down", "max_ad_up"}
    missing_block_cols = required_block_cols - set(block_df.columns)
    if missing_block_cols:
        raise ValueError(
            "Block-level CSV is missing required columns: "
            f"{sorted(missing_block_cols)}"
        )

    # Join blocks to Alameda block->ZCTA crosswalk.
    # We support both cleaned names and common relationship-file names.
    block_col = pick_column(
        crosswalk_df, ["GEOID", "GEOID_TABBLOCK_20", "BlockCode", "block_geoid", "GEOID20", "geoid", "geoid20", "BLOCKID20"]
    )
    zcta_col = pick_column(crosswalk_df, ["ZCTA", "GEOID_ZCTA5_20", "zcta", "ZCTA5CE20", "zip", "zip_code"])
    land_col = pick_column(crosswalk_df, ["LAND_AREA", "AREALAND_PART", "AREALAND", "aland"])
    housing_col = pick_column(crosswalk_df, ["housing_units", "hu", "HU20", "tot_hu"])

    if block_col is None or zcta_col is None:
        raise ValueError(
            "Crosswalk must include a block GEOID column and a ZCTA column. "
            f"Available columns: {list(crosswalk_df.columns)}"
        )
    if land_col is None:
        raise ValueError(
            "Crosswalk must include LAND_AREA (or AREALAND_PART) for tie-breaking duplicate block mappings."
        )

    cw = crosswalk_df.copy()
    cw["BlockCode"] = cw[block_col].astype(str).str.strip().str.zfill(15)
    cw["zcta"] = cw[zcta_col].astype(str).str.strip().str.zfill(5)
    cw["LAND_AREA"] = pd.to_numeric(cw[land_col], errors="coerce").fillna(0)

    if housing_col is not None:
        cw["housing_units"] = pd.to_numeric(cw[housing_col], errors="coerce")
    else:
        cw["housing_units"] = pd.NA

    # Restrict to Alameda-related block identifiers and keep only valid ZCTAs.
    cw = cw[cw["BlockCode"].str.startswith("06001")]
    cw = cw[cw["zcta"].str.match(r"^\d{5}$", na=False)]

    # Validate crosswalk uniqueness on BlockCode and resolve multi-mapped blocks.
    # If one block maps to multiple ZCTAs, keep the row with largest LAND_AREA.
    cw = cw.sort_values(["BlockCode", "LAND_AREA"], ascending=[True, False])
    cw = cw.drop_duplicates(subset=["BlockCode"], keep="first")

    # Optional housing-unit safeguard against water/empty blocks.
    if cw["housing_units"].notna().any():
        cw = cw[cw["housing_units"].fillna(0) > 0]

    mapped = block_df.copy()
    mapped["BlockCode"] = mapped["BlockCode"].astype(str).str.strip().str.zfill(15)
    mapped = mapped.merge(cw[["BlockCode", "zcta"]], on="BlockCode", how="left")

    # QA checks: unmatched blocks and duplicate keys.
    unmatched = mapped["zcta"].isna().sum()
    n_in = len(mapped)
    if unmatched > 0:
        pct = 100.0 * unmatched / n_in
        print(
            f"Warning: {unmatched} of {n_in} block rows ({pct:.1f}%) did not map to a ZCTA. "
            "Confirm FCC BlockCode values match the block GEOIDs in the crosswalk file."
        )

    mapped = mapped.dropna(subset=["zcta"])
    mapped = mapped[
        ["zcta", "BlockCode", "TechCode", "TechCategory", "max_ad_down", "max_ad_up"]
    ]
    mapped.to_csv(output_path, index=False)
    print(f"Saved block->ZCTA mapped output: {output_path} ({len(mapped)} rows)")


if __name__ == "__main__":
    main()
