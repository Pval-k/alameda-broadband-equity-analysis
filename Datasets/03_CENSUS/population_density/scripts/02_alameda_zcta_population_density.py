"""Join Alameda ZCTA population to crosswalk land area and compute population density.

Land area from the crosswalk is square meters (Census AREALAND_PART).

Density in people per square mile (common U.S. reporting unit):

    land_area_sq_mi = land_area_sq_m / SQ_M_PER_SQ_MI
    population_per_sq_mi = population / land_area_sq_mi

where SQ_M_PER_SQ_MI is the exact conversion (1 international mile = 1609.344 m,
so 1 mi^2 = (1609.344)^2 m^2).
"""

import argparse
from pathlib import Path

import pandas as pd

# Crosswalk LAND_AREA / total_land_area comes from Census block–ZCTA AREALAND_PART (square meters).
SQ_M_PER_SQ_KM = 1_000_000.0
# 1 square mile = exactly 2,589,988.110336 square meters (international mile definition).
SQ_M_PER_SQ_MI = 2589988.110336


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Merge Alameda ZCTA population (01_alameda_zcta_population) with ZCTA land area "
            "(crosswalk 01_alameda_zcta_land_area). Computes density using land area in square meters."
        )
    )
    parser.add_argument(
        "--population-csv",
        default="Datasets/03_CENSUS/population_density/csv/01_alameda_zcta_population.csv",
        help="Output from 01_alameda_zcta_population.py (columns zcta, population).",
    )
    parser.add_argument(
        "--land-area-csv",
        default="Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv",
        help="Crosswalk aggregate with columns ZCTA, total_land_area (square meters).",
    )
    parser.add_argument(
        "--output-csv",
        default="Datasets/03_CENSUS/population_density/csv/02_alameda_zcta_population_density.csv",
        help="Output path.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    pop_path = Path(args.population_csv)
    land_path = Path(args.land_area_csv)
    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not pop_path.exists():
        raise FileNotFoundError(f"Population CSV not found: {pop_path}")
    if not land_path.exists():
        raise FileNotFoundError(f"Land area CSV not found: {land_path}")

    pop = pd.read_csv(pop_path, dtype={"zcta": str})
    land = pd.read_csv(land_path, dtype={"ZCTA": str})

    pop["zcta"] = pop["zcta"].astype(str).str.strip().str.zfill(5)
    land["zcta"] = land["ZCTA"].astype(str).str.strip().str.zfill(5)

    land_sq_m = pd.to_numeric(land["total_land_area"], errors="coerce")
    land_small = pd.DataFrame(
        {
            "zcta": land["zcta"],
            "land_area_sq_m": land_sq_m,
        }
    )

    merged = pop.merge(land_small, on="zcta", how="inner", validate="one_to_one")

    area = merged["land_area_sq_m"]
    pop_n = pd.to_numeric(merged["population"], errors="coerce")
    with_area = area.gt(0) & area.notna()
    merged["land_area_sq_mi"] = (area / SQ_M_PER_SQ_MI).where(with_area)
    merged["population_per_sq_km"] = (pop_n / (area / SQ_M_PER_SQ_KM)).where(with_area)
    merged["population_per_sq_mi"] = (pop_n / merged["land_area_sq_mi"]).where(with_area)

    out = merged[
        [
            "zcta",
            "population",
            "land_area_sq_m",
            "land_area_sq_mi",
            "population_per_sq_km",
            "population_per_sq_mi",
        ]
    ].sort_values("zcta")

    out.to_csv(out_path, index=False, float_format="%.6f")
    print(f"Wrote {len(out)} rows to {out_path}")


if __name__ == "__main__":
    main()
