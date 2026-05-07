from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Filter a raw M-Lab CSV (with a test_date column like YYYY-MM-DD) "
            "to a single month (and optional year), writing a new CSV."
        )
    )
    p.add_argument(
        "--input-csv",
        default="Datasets/01_MLAB/mlab_raw_alameda_2020.csv",
        help="Path to the raw M-Lab CSV.",
    )
    p.add_argument(
        "--output-csv",
        default="Datasets/01_MLAB/mlab_raw_alameda_2020_12.csv",
        help="Path to write the filtered CSV.",
    )
    p.add_argument(
        "--month",
        type=int,
        default=12,
        help="Month to keep (1-12). Default: 12 (December).",
    )
    p.add_argument(
        "--year",
        type=int,
        default=2020,
        help="Optional year to keep (e.g. 2020). Default: 2020.",
    )
    p.add_argument(
        "--chunksize",
        type=int,
        default=500_000,
        help="Rows per chunk to process. Increase if you have plenty of RAM.",
    )
    p.add_argument(
        "--limit-rows",
        type=int,
        default=None,
        help="Optional max number of input rows to scan (for quick tests).",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    in_path = Path(args.input_csv)
    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {in_path}")

    if not (1 <= args.month <= 12):
        raise ValueError("--month must be between 1 and 12")

    month_str = f"{args.month:02d}"
    prefix = f"{int(args.year)}-{month_str}-"

    # Stream the file to avoid loading ~millions of rows into memory.
    reader = pd.read_csv(in_path, chunksize=args.chunksize)

    wrote_header = False
    scanned = 0
    kept = 0

    for chunk in reader:
        scanned += len(chunk)
        if "test_date" not in chunk.columns:
            raise ValueError(
                f"Expected a 'test_date' column in {in_path.name}, got columns: {list(chunk.columns)}"
            )

        # test_date is ISO YYYY-MM-DD in this project; string prefix match is fast.
        test_date = chunk["test_date"].astype(str)
        mask = test_date.str.startswith(prefix)
        out = chunk.loc[mask]

        if len(out):
            out.to_csv(out_path, mode="a", index=False, header=not wrote_header)
            wrote_header = True
            kept += len(out)

        if args.limit_rows is not None and scanned >= args.limit_rows:
            break

    print(f"Scanned {scanned:,} rows, kept {kept:,} rows -> {out_path}")


if __name__ == "__main__":
    main()

