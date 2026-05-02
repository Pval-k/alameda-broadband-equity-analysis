import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create a lean Alameda-only block-to-ZCTA crosswalk from the national relationship text file."
        )
    )
    parser.add_argument(
        "--input-txt",
        default="Datasets/00_crosswalk/csv/raw_block_to_zcta.txt",
        help="Path to the raw national block-to-ZCTA relationship .txt file.",
    )
    parser.add_argument(
        "--output-csv",
        default="Datasets/00_crosswalk/csv/00_alameda_block_to_zcta_cleaned.csv",
        help="Path to output Alameda-only cleaned crosswalk CSV.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=500_000,
        help="Rows per chunk while reading large raw file.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    input_path = Path(args.input_txt)
    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Raw relationship file not found: {input_path}")

    # Keep required columns only for a fast, lean crosswalk.
    # Read all as strings to preserve leading zeros (e.g., CA prefix 06001).
    usecols = ["GEOID_TABBLOCK_20", "GEOID_ZCTA5_20", "AREALAND_PART"]
    filtered_chunks: list[pd.DataFrame] = []
    total_rows = 0
    alameda_rows = 0

    reader = pd.read_csv(
        input_path,
        sep="|",
        dtype=str,
        usecols=usecols,
        encoding="utf-8-sig",
        chunksize=args.chunksize,
        low_memory=False,
    )

    for chunk in reader:
        total_rows += len(chunk)
        chunk["GEOID_TABBLOCK_20"] = chunk["GEOID_TABBLOCK_20"].astype(str).str.strip().str.zfill(15)
        chunk["GEOID_ZCTA5_20"] = chunk["GEOID_ZCTA5_20"].astype(str).str.strip().str.zfill(5)
        chunk["AREALAND_PART"] = pd.to_numeric(chunk["AREALAND_PART"], errors="coerce").fillna(0)

        # Alameda County block prefix in California: 06001
        chunk = chunk[chunk["GEOID_TABBLOCK_20"].str.startswith("06001")]
        chunk = chunk[chunk["GEOID_ZCTA5_20"].str.match(r"^\d{5}$", na=False)]
        alameda_rows += len(chunk)
        filtered_chunks.append(chunk)

    if not filtered_chunks:
        raise ValueError("No Alameda rows found. Check input file and GEOID prefixes.")

    alameda_df = pd.concat(filtered_chunks, ignore_index=True)

    # Tie-breaker: if a block maps to multiple ZCTAs, keep the one with largest AREALAND_PART.
    # This keeps the ZCTA row with the largest AREALAND_PART for that block.
    alameda_df = alameda_df.sort_values(
        ["GEOID_TABBLOCK_20", "AREALAND_PART"],
        ascending=[True, False],
    )
    before_dedupe = len(alameda_df)
    alameda_df = alameda_df.drop_duplicates(subset=["GEOID_TABBLOCK_20"], keep="first")
    duplicates_removed = before_dedupe - len(alameda_df)

    alameda_df = alameda_df[
        ["GEOID_TABBLOCK_20", "GEOID_ZCTA5_20", "AREALAND_PART"]
    ].rename(
        columns={
            "GEOID_TABBLOCK_20": "GEOID",
            "GEOID_ZCTA5_20": "ZCTA",
            "AREALAND_PART": "LAND_AREA",
        }
    ).sort_values("GEOID")

    alameda_df.to_csv(output_path, index=False)

    print(f"Scanned rows: {total_rows}")
    print(f"Alameda candidate rows: {alameda_rows}")
    print(f"Duplicate block mappings removed: {duplicates_removed}")
    print(f"Saved cleaned Alameda crosswalk: {output_path} ({len(alameda_df)} rows)")


if __name__ == "__main__":
    main()
