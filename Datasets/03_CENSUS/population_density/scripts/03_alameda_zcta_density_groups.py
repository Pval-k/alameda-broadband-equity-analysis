"""Bucket Alameda ZCTAs into low/medium/high density groups using tertile cutoffs."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Read ZCTA-level population density and assign each ZCTA to a low/medium/high "
            "group using tertile (qcut q=3) cutoffs on population_per_sq_mi."
        )
    )
    p.add_argument(
        "--input-csv",
        default="Datasets/03_CENSUS/population_density/csv/02_alameda_zcta_population_density.csv",
        help="Per-ZCTA density CSV with `zcta` and `population_per_sq_mi`.",
    )
    p.add_argument(
        "--output-csv",
        default="Datasets/03_CENSUS/population_density/csv/03_alameda_zcta_density_groups.csv",
        help="Output CSV with the low/medium/high label per ZCTA.",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    in_path = Path(args.input_csv)
    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(f"Input density CSV not found: {in_path}")

    df = pd.read_csv(in_path, dtype={"zcta": str})
    df["zcta"] = df["zcta"].astype(str).str.strip().str.zfill(5)
    df["population_per_sq_mi"] = pd.to_numeric(
        df["population_per_sq_mi"], errors="coerce"
    )
    df = df.dropna(subset=["population_per_sq_mi"]).copy()
    if df.empty:
        raise ValueError("No usable rows after dropping missing population_per_sq_mi.")

    # Tertile cutoffs:
    #   q33 = 33.33rd percentile, q67 = 66.67th percentile of population_per_sq_mi.
    # Boundary rule (matches pandas.qcut with three equal-frequency bins):
    #   low    : population_per_sq_mi <= q33
    #   medium : q33 < population_per_sq_mi <= q67
    #   high   : population_per_sq_mi > q67
    q33 = float(df["population_per_sq_mi"].quantile(1 / 3))
    q67 = float(df["population_per_sq_mi"].quantile(2 / 3))

    df["density_group"] = pd.qcut(
        df["population_per_sq_mi"],
        q=3,
        labels=["low", "medium", "high"],
    )
    df["low_cutoff_per_sq_mi"] = q33
    df["high_cutoff_per_sq_mi"] = q67

    out = df[
        [
            "zcta",
            "population_per_sq_mi",
            "density_group",
            "low_cutoff_per_sq_mi",
            "high_cutoff_per_sq_mi",
        ]
    ].sort_values(["density_group", "population_per_sq_mi", "zcta"])

    out.to_csv(out_path, index=False, float_format="%.6f")

    counts = out["density_group"].value_counts().reindex(["low", "medium", "high"])
    print(
        f"Tertile cutoffs (people per sq mi): q33 = {q33:.2f}, q67 = {q67:.2f}"
    )
    print("Per-group counts:")
    for label, n in counts.items():
        print(f"  {label}: {int(n)}")
    print(f"Wrote {len(out)} rows to {out_path}")


if __name__ == "__main__":
    main()
