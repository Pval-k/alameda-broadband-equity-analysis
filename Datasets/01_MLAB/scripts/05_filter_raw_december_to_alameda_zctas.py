from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Filter the December raw M-Lab CSV to Alameda ZCTAs only, "
            "based on the Alameda ZCTA allowlist."
        )
    )
    p.add_argument(
        "--input-csv",
        default="Datasets/01_MLAB/mlab_raw_alameda_2020_12.csv",
        help="December raw M-Lab CSV to filter (will be overwritten by default).",
    )
    p.add_argument(
        "--alameda-zcta-csv",
        default="Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv",
        help="Alameda ZCTA allowlist (uses column ZCTA).",
    )
    p.add_argument(
        "--output-csv",
        default=None,
        help=(
            "Optional separate output path. If omitted, input CSV is overwritten in place "
            "with only Alameda ZCTAs."
        ),
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    in_path = Path(args.input_csv)
    allow_path = Path(args.alameda_zcta_csv)

    if not in_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {in_path}")
    if not allow_path.exists():
        raise FileNotFoundError(f"Alameda ZCTA CSV not found: {allow_path}")

    df = pd.read_csv(in_path, dtype={"zcta": str})
    allow = pd.read_csv(allow_path, dtype={"ZCTA": str})

    df["zcta"] = df["zcta"].astype(str).str.strip().str.zfill(5)
    allowed = set(allow["ZCTA"].dropna().astype(str).str.strip().str.zfill(5))

    before = len(df)
    df = df[df["zcta"].isin(allowed)].copy()
    after = len(df)

    out_path = Path(args.output_csv) if args.output_csv else in_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(
        f"Filtered raw December M-Lab rows from {before:,} to {after:,} "
        f"for Alameda ZCTAs ({len(allowed)} allowed)."
    )
    print(f"Wrote -> {out_path}")


if __name__ == "__main__":
    main()

