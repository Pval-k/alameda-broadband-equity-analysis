from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Compute descriptive statistics (mean/median/std/IQR) and empirical CDF "
            "for ZCTA-level measured download speeds."
        )
    )
    p.add_argument(
        "--input-csv",
        default="Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv",
        help="ZCTA-level M-Lab download metrics CSV.",
    )
    p.add_argument(
        "--value-col",
        default="median_download_mbps",
        help="Column containing the measured download speed to summarize.",
    )
    p.add_argument(
        "--out-summary-csv",
        default="Datasets/01_MLAB/csvs/02_alameda_zcta_mlab_2020_12_download_descriptive_summary.csv",
        help="Where to write the single-row summary stats CSV.",
    )
    p.add_argument(
        "--out-ecdf-csv",
        default="Datasets/01_MLAB/csvs/02_alameda_zcta_mlab_2020_12_download_ecdf.csv",
        help="Where to write ECDF points (x, cdf).",
    )
    p.add_argument(
        "--out-cdf-plot",
        default="Datasets/01_MLAB/plots/02_alameda_zcta_mlab_2020_12_download_cdf.png",
        help="Where to write a CDF plot image.",
    )
    return p


def ecdf(x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Simple empirical CDF for an array x (ignoring NaNs already removed).
    Returns (sorted_x, cumulative_prob).
    """
    xs = np.sort(x)
    n = len(xs)
    # Use plotting positions: i/n, so max value approaches 1.
    cdf = np.arange(1, n + 1) / n
    return xs, cdf


def main() -> None:
    args = build_parser().parse_args()

    in_path = Path(args.input_csv)
    if not in_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {in_path}")

    df = pd.read_csv(in_path)
    if args.value_col not in df.columns:
        raise ValueError(
            f"Column '{args.value_col}' not found in {in_path.name}. "
            f"Available columns: {list(df.columns)}"
        )

    values = pd.to_numeric(df[args.value_col], errors="coerce").dropna()
    x = values.to_numpy()
    if len(x) == 0:
        raise ValueError("No valid (non-null) values found to summarize.")

    q25 = float(np.quantile(x, 0.25))
    q75 = float(np.quantile(x, 0.75))
    iqr = q75 - q25

    mean = float(np.mean(x))
    median = float(np.median(x))
    std = float(np.std(x, ddof=1)) if len(x) > 1 else float("nan")

    xs, cdf = ecdf(x)

    # Summary (single-row) output for easy inclusion in the report.
    summary = pd.DataFrame(
        [
            {
                "n_zctas": len(x),
                "mean_mbps": mean,
                "median_mbps": median,
                "std_mbps": std,
                "q25_mbps": q25,
                "q75_mbps": q75,
                "iqr_mbps": iqr,
                "min_mbps": float(np.min(x)),
                "max_mbps": float(np.max(x)),
            }
        ]
    )

    out_summary_path = Path(args.out_summary_csv)
    out_summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_summary_path, index=False)

    out_ecdf_path = Path(args.out_ecdf_csv)
    out_ecdf_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"download_mbps": xs, "ecdf": cdf}).to_csv(
        out_ecdf_path, index=False
    )

    # CDF plot
    out_plot_path = Path(args.out_cdf_plot)
    out_plot_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(7, 5))
    plt.plot(xs, cdf, linewidth=2)
    plt.xlabel("Measured download speed (median Mbps) across ZCTAs")
    plt.ylabel("CDF")
    plt.title("CDF of ZCTA measured download speeds")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_plot_path, dpi=200)
    plt.close()

    print(f"Wrote summary -> {out_summary_path}")
    print(f"Wrote ECDF points -> {out_ecdf_path}")
    print(f"Wrote CDF plot -> {out_plot_path}")


if __name__ == "__main__":
    main()

