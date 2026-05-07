"""Download ACS 5-year median household income for ZCTAs, filtered to Alameda County ZCTAs.

This uses the Census API ACS 5-year endpoint for a single ACS "end year" (e.g., 2020 ACS5
represents the 2016–2020 5-year period).

Outputs one row per Alameda ZCTA with:
  - zcta (5-digit string)
  - median_household_income (estimate, dollars)
  - median_household_income_moe (margin of error, dollars)
  - acs_end_year
  - name (Census geography label)

API docs: https://api.census.gov/data.html
Variable:
  - B19013_001E: Median household income (in the past 12 months) (in inflation-adjusted dollars)
  - B19013_001M: Margin of error for B19013_001E
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Fetch ACS5 median household income for all ZCTAs via Census API, "
            "filter to Alameda ZCTAs using the crosswalk list, and write a CSV."
        )
    )
    p.add_argument(
        "--acs-end-year",
        type=int,
        default=2020,
        help="ACS 5-year end year to query (e.g., 2020 for 2016–2020 ACS5).",
    )
    p.add_argument(
        "--alameda-zcta-csv",
        default="Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv",
        help="One row per Alameda ZCTA (uses column ZCTA).",
    )
    p.add_argument(
        "--census-api-key",
        default=None,
        help=(
            "Optional Census API key. If omitted, the API may still work but with "
            "stricter rate limits."
        ),
    )
    p.add_argument(
        "--output-csv",
        default="Datasets/03_CENSUS/income/csv/01_alameda_zcta_income.csv",
        help="Output path.",
    )
    return p


def load_alameda_zctas(path: Path) -> set[str]:
    df = pd.read_csv(path, dtype={"ZCTA": str})
    return {str(z).strip().zfill(5) for z in df["ZCTA"].dropna()}


def fetch_acs5_zcta_income(acs_end_year: int, api_key: str | None) -> pd.DataFrame:
    base = f"https://api.census.gov/data/{acs_end_year}/acs/acs5"
    params = {
        "get": "NAME,B19013_001E,B19013_001M",
        "for": "zip code tabulation area:*",
    }
    if api_key:
        params["key"] = api_key

    url = f"{base}?{urlencode(params)}"
    with urlopen(url) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    header, *rows = payload
    df = pd.DataFrame(rows, columns=header)
    df = df.rename(
        columns={
            "zip code tabulation area": "zcta",
            "B19013_001E": "median_household_income",
            "B19013_001M": "median_household_income_moe",
            "NAME": "name",
        }
    )
    df["zcta"] = df["zcta"].astype(str).str.strip().str.zfill(5)
    df["median_household_income"] = pd.to_numeric(
        df["median_household_income"], errors="coerce"
    )
    df["median_household_income_moe"] = pd.to_numeric(
        df["median_household_income_moe"], errors="coerce"
    )
    # Census API sometimes returns negative sentinel values for missing estimates/MOE.
    # Income and MOE should never be negative, so treat any negative as missing.
    df.loc[df["median_household_income"] < 0, "median_household_income"] = pd.NA
    df.loc[df["median_household_income_moe"] < 0, "median_household_income_moe"] = pd.NA
    df["acs_end_year"] = int(acs_end_year)
    return df


def main() -> None:
    args = build_parser().parse_args()
    zcta_path = Path(args.alameda_zcta_csv)
    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not zcta_path.exists():
        raise FileNotFoundError(f"Alameda ZCTA list not found: {zcta_path}")

    allowed = load_alameda_zctas(zcta_path)
    all_income = fetch_acs5_zcta_income(args.acs_end_year, args.census_api_key)
    out = all_income[all_income["zcta"].isin(allowed)].copy().sort_values("zcta")

    missing = allowed - set(out["zcta"])
    if missing:
        print(
            "Warning: these Alameda crosswalk ZCTAs were absent from the ACS response: "
            f"{sorted(missing)}"
        )

    out[
        [
            "zcta",
            "median_household_income",
            "median_household_income_moe",
            "acs_end_year",
            "name",
        ]
    ].to_csv(out_path, index=False, float_format="%.0f", na_rep="")
    print(f"Wrote {len(out)} rows to {out_path}")


if __name__ == "__main__":
    main()

