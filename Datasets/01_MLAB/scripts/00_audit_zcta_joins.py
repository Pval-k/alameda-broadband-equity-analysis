from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Audit ZCTA joins across M-Lab, Census income, and Alameda crosswalk allowlist."
    )
    p.add_argument(
        "--mlab-zcta-csv",
        default="Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv",
        help="ZCTA-level M-Lab metrics CSV.",
    )
    p.add_argument(
        "--income-csv",
        default="Datasets/03_CENSUS/income/csv/01_alameda_zcta_income.csv",
        help="Census income CSV (Alameda-only).",
    )
    p.add_argument(
        "--alameda-zcta-csv",
        default="Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv",
        help="Alameda ZCTA allowlist (uses column ZCTA).",
    )
    p.add_argument(
        "--max-list",
        type=int,
        default=30,
        help="Max items to print for each mismatch list.",
    )
    return p


def _zcta_set_from_col(df: pd.DataFrame, col: str) -> set[str]:
    return set(df[col].dropna().astype(str).str.strip().str.zfill(5))


def main() -> None:
    args = build_parser().parse_args()

    mlab_path = Path(args.mlab_zcta_csv)
    income_path = Path(args.income_csv)
    allow_path = Path(args.alameda_zcta_csv)

    for pth in (mlab_path, income_path, allow_path):
        if not pth.exists():
            raise FileNotFoundError(f"Missing required file: {pth}")

    mlab = pd.read_csv(mlab_path, dtype={"zcta": str})
    income = pd.read_csv(income_path, dtype={"zcta": str})
    allow = pd.read_csv(allow_path, dtype={"ZCTA": str})

    mlab_z = _zcta_set_from_col(mlab, "zcta")
    income_z = _zcta_set_from_col(income, "zcta")
    allow_z = _zcta_set_from_col(allow, "ZCTA")

    def show(name: str, items: set[str]) -> None:
        lst = sorted(items)
        head = lst[: args.max_list]
        suffix = "" if len(lst) <= args.max_list else f" ... (+{len(lst)-args.max_list} more)"
        print(f"{name}: {len(lst)}")
        if head:
            print("  " + ", ".join(head) + suffix)

    print("== ZCTA join audit ==")
    print(f"M-Lab ZCTAs: {len(mlab_z)}")
    print(f"Income ZCTAs: {len(income_z)}")
    print(f"Alameda allowlist ZCTAs: {len(allow_z)}")
    print("")

    show("M-Lab but not in allowlist", mlab_z - allow_z)
    show("M-Lab but not in income", mlab_z - income_z)
    show("Income but not in M-Lab", income_z - mlab_z)
    show("Allowlist but not in M-Lab", allow_z - mlab_z)
    print("")

    # Duplicate check in M-Lab metrics (should be one row per ZCTA).
    if mlab["zcta"].duplicated().any():
        dups = sorted(mlab.loc[mlab["zcta"].duplicated(), "zcta"].unique().tolist())
        print(f"WARNING: duplicate ZCTAs in M-Lab metrics: {dups[:args.max_list]}")
    else:
        print("OK: no duplicate ZCTAs in M-Lab metrics.")

    # Missing income values (should be handled before correlation).
    if "median_household_income" in income.columns:
        miss = income[pd.to_numeric(income["median_household_income"], errors="coerce").isna()]
        print(f"Income rows with missing median_household_income: {len(miss)}")
        if len(miss):
            show("ZCTAs with missing income", set(miss["zcta"].astype(str).str.zfill(5)))


if __name__ == "__main__":
    main()

