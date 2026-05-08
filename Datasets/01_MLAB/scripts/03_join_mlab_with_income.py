from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Join ZCTA-level M-Lab measured download metrics with Census ACS5 income. "
            "Outputs a merged CSV suitable for correlation/plots."
        )
    )
    p.add_argument(
        "--mlab-zcta-csv",
        default="Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv",
        help="ZCTA-level M-Lab download metrics.",
    )
    p.add_argument(
        "--income-csv",
        default="Datasets/03_CENSUS/income/csv/01_alameda_zcta_income.csv",
        help="Census income CSV (ACS5 B19013).",
    )
    p.add_argument(
        "--output-csv",
        default="Datasets/01_MLAB/csvs/03_alameda_zcta_mlab_2020_12_with_income.csv",
        help="Output merged CSV.",
    )
    p.add_argument(
        "--how",
        choices=["inner", "left"],
        default="inner",
        help="Join type on zcta. inner keeps only ZCTAs present in both datasets.",
    )
    return p


def _clean_zcta(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.zfill(5)


def main() -> None:
    args = build_parser().parse_args()

    mlab_path = Path(args.mlab_zcta_csv)
    income_path = Path(args.income_csv)
    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not mlab_path.exists():
        raise FileNotFoundError(f"M-Lab ZCTA metrics CSV not found: {mlab_path}")
    if not income_path.exists():
        raise FileNotFoundError(f"Income CSV not found: {income_path}")

    mlab = pd.read_csv(mlab_path, dtype={"zcta": str})
    income = pd.read_csv(income_path, dtype={"zcta": str})

    if "zcta" not in mlab.columns:
        raise ValueError("M-Lab metrics CSV must contain a 'zcta' column.")
    if "zcta" not in income.columns:
        raise ValueError("Income CSV must contain a 'zcta' column.")

    mlab["zcta"] = _clean_zcta(mlab["zcta"])
    income["zcta"] = _clean_zcta(income["zcta"])

    # Ensure income column is numeric and missing values are recognized.
    income["median_household_income"] = pd.to_numeric(
        income.get("median_household_income"), errors="coerce"
    )
    income["median_household_income_moe"] = pd.to_numeric(
        income.get("median_household_income_moe"), errors="coerce"
    )

    merged = mlab.merge(
        income[["zcta", "median_household_income", "median_household_income_moe", "acs_end_year"]],
        on="zcta",
        how=args.how,
        validate="one_to_one",
    )

    # Drop rows with missing income for correlation/regression work.
    before = len(merged)
    merged = merged.dropna(subset=["median_household_income"]).copy()
    dropped = before - len(merged)

    merged = merged.sort_values("zcta")
    merged.to_csv(out_path, index=False)

    print(f"Merged rows: {before:,} (dropped {dropped:,} missing-income rows)")
    print(f"Wrote -> {out_path}")


if __name__ == "__main__":
    main()

