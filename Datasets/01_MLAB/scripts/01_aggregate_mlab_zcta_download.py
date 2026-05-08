from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Aggregate raw M-Lab tests to ZCTA-level measured download metrics "
            "(median/mean/std/count). Expects columns: zcta, test_date, test_type, speed_mbps, latency_ms."
        )
    )
    p.add_argument(
        "--input-csv",
        default="Datasets/01_MLAB/mlab_raw_alameda_2020_12.csv",
        help="Raw M-Lab CSV (typically December-only).",
    )
    p.add_argument(
        "--output-csv",
        default="Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv",
        help="Output CSV path for ZCTA-level download metrics.",
    )
    p.add_argument(
        "--month",
        type=int,
        default=12,
        help="Month label to store in output (e.g. 12 for December).",
    )
    p.add_argument(
        "--year",
        type=int,
        default=2020,
        help="Year label to store in output (e.g. 2020).",
    )
    p.add_argument(
        "--alameda-zcta-csv",
        default="Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv",
        help="Alameda ZCTA allowlist (uses column ZCTA). If provided, M-Lab rows are filtered to these ZCTAs.",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    in_path = Path(args.input_csv)
    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {in_path}")

    # December-only file is ~37MB (~0.8M rows) so reading into memory is reasonable.
    df = pd.read_csv(in_path)

    expected_cols = {"zcta", "test_type", "speed_mbps", "latency_ms"}
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"Input CSV missing expected columns {sorted(missing)}. Found columns: {list(df.columns)}"
        )

    df["zcta"] = df["zcta"].astype(str).str.strip().str.zfill(5)
    df["speed_mbps"] = pd.to_numeric(df["speed_mbps"], errors="coerce")
    df["latency_ms"] = pd.to_numeric(df["latency_ms"], errors="coerce")

    # Filter to Alameda ZCTAs so downstream joins don't drop unrelated Bay Area ZCTAs.
    allow_path = Path(args.alameda_zcta_csv)
    if allow_path.exists():
        allow = pd.read_csv(allow_path, dtype={"ZCTA": str})
        allowed = set(allow["ZCTA"].dropna().astype(str).str.strip().str.zfill(5))
        df = df[df["zcta"].isin(allowed)].copy()
    else:
        print(f"Warning: Alameda ZCTA allowlist not found at {allow_path}; not filtering.")

    # Negative speeds are not valid; treat them as missing.
    df.loc[df["speed_mbps"] < 0, "speed_mbps"] = pd.NA

    downloads = df[df["test_type"] == "download"].copy()
    downloads = downloads.dropna(subset=["speed_mbps"])

    grouped = downloads.groupby("zcta", as_index=False)
    out = grouped.agg(
        median_download_mbps=("speed_mbps", "median"),
        mean_download_mbps=("speed_mbps", "mean"),
        std_download_mbps=("speed_mbps", "std"),
        download_test_count=("speed_mbps", "count"),
    )

    # Optional: keep latency summary as well (often useful, not required by current todo).
    if "latency_ms" in downloads.columns:
        latency_summ = downloads.groupby("zcta", as_index=False).agg(
            median_download_latency_ms=("latency_ms", "median"),
            mean_download_latency_ms=("latency_ms", "mean"),
        )
        out = out.merge(latency_summ, on="zcta", how="left")

    out["mlab_year"] = args.year
    out["mlab_month"] = args.month

    out = out.sort_values("zcta")
    out.to_csv(out_path, index=False)

    print(f"Read {len(df):,} raw rows; {len(downloads):,} download rows")
    print(f"Wrote ZCTA download metrics to {out_path} ({len(out):,} ZCTAs)")


if __name__ == "__main__":
    main()

