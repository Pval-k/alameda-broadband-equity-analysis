from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Aggregate raw M-Lab tests to ZCTA-level measured UPLOAD metrics "
            "(median/mean/std/count). Expects columns: zcta, test_type, speed_mbps, latency_ms."
        )
    )
    p.add_argument(
        "--input-csv",
        default="Datasets/01_MLAB/mlab_raw_alameda_2020_12.csv",
        help="Raw M-Lab CSV (Alameda-only preferred).",
    )
    p.add_argument(
        "--output-csv",
        default="Datasets/01_MLAB/csvs/06_alameda_zcta_mlab_2020_12_upload_metrics.csv",
        help="Output CSV path for ZCTA-level upload metrics.",
    )
    p.add_argument("--month", type=int, default=12)
    p.add_argument("--year", type=int, default=2020)
    return p


def main() -> None:
    args = build_parser().parse_args()
    in_path = Path(args.input_csv)
    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {in_path}")

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

    # Remove invalid values
    df.loc[df["speed_mbps"] < 0, "speed_mbps"] = pd.NA
    df.loc[df["latency_ms"] < 0, "latency_ms"] = pd.NA

    uploads = df[df["test_type"] == "upload"].copy()
    uploads = uploads.dropna(subset=["speed_mbps"])

    out = uploads.groupby("zcta", as_index=False).agg(
        median_upload_mbps=("speed_mbps", "median"),
        mean_upload_mbps=("speed_mbps", "mean"),
        std_upload_mbps=("speed_mbps", "std"),
        upload_test_count=("speed_mbps", "count"),
        median_upload_latency_ms=("latency_ms", "median"),
        mean_upload_latency_ms=("latency_ms", "mean"),
    )

    out["mlab_year"] = args.year
    out["mlab_month"] = args.month

    out = out.sort_values("zcta")
    out.to_csv(out_path, index=False)

    print(f"Read {len(df):,} raw rows; {len(uploads):,} upload rows")
    print(f"Wrote ZCTA upload metrics to {out_path} ({len(out):,} ZCTAs)")


if __name__ == "__main__":
    main()

