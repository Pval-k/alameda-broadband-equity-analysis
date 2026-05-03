"""Filter Census ZCTA population extract to Alameda County ZCTAs from the crosswalk."""

import argparse
from pathlib import Path

import pandas as pd

ZCTA_GEO_PREFIX = "860Z200US"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read raw ZCTA population CSV (Census API-style export), keep rows whose "
            "ZCTA appears in the Alameda crosswalk list, output zcta + population."
        )
    )
    parser.add_argument(
        "--raw-population-csv",
        default="Datasets/03_CENSUS/population_density/csv/raw_ZCTA_population.csv",
        help="Raw Census ZCTA population CSV (includes repeated header row).",
    )
    parser.add_argument(
        "--alameda-zcta-csv",
        default="Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv",
        help="One row per Alameda ZCTA (uses column ZCTA).",
    )
    parser.add_argument(
        "--output-csv",
        default="Datasets/03_CENSUS/population_density/csv/01_alameda_zcta_population.csv",
        help="Output path with columns zcta, population.",
    )
    return parser


def load_alameda_zctas(path: Path) -> set[str]:
    df = pd.read_csv(path, dtype={"ZCTA": str})
    return {str(z).strip().zfill(5) for z in df["ZCTA"].dropna()}


def main() -> None:
    args = build_parser().parse_args()
    raw_path = Path(args.raw_population_csv)
    zcta_path = Path(args.alameda_zcta_csv)
    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw population CSV not found: {raw_path}")
    if not zcta_path.exists():
        raise FileNotFoundError(f"Alameda ZCTA list not found: {zcta_path}")

    allowed = load_alameda_zctas(zcta_path)

    # Second row is a Census label row ("Geography", ...); skip it.
    df = pd.read_csv(raw_path, skiprows=[1], dtype=str)
    df = df[df["GEO_ID"].str.startswith(ZCTA_GEO_PREFIX, na=False)].copy()
    df["zcta"] = df["GEO_ID"].str[len(ZCTA_GEO_PREFIX) :].str.zfill(5)
    df = df[df["zcta"].isin(allowed)]

    out = pd.DataFrame(
        {
            "zcta": df["zcta"],
            "population": pd.to_numeric(df["P1_001N"], errors="coerce").astype("Int64"),
        }
    ).sort_values("zcta")

    missing = allowed - set(out["zcta"])
    if missing:
        print(
            "Warning: these Alameda crosswalk ZCTAs were absent from the raw "
            f"population file: {sorted(missing)}"
        )

    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out)} rows to {out_path}")


if __name__ == "__main__":
    main()
