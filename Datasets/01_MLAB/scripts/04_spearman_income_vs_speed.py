from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Compute Spearman rank correlation between median household income and "
            "median measured download speed (ZCTA-level), and write a scatter plot."
        )
    )
    p.add_argument(
        "--input-csv",
        default="Datasets/01_MLAB/csvs/03_alameda_zcta_mlab_2020_12_with_income.csv",
        help="Merged M-Lab + income CSV.",
    )
    p.add_argument(
        "--x-col",
        default="median_household_income",
        help="Income column.",
    )
    p.add_argument(
        "--y-col",
        default="median_download_mbps",
        help="Measured speed column.",
    )
    p.add_argument(
        "--out-results-csv",
        default="Datasets/01_MLAB/csvs/04_spearman_income_vs_download_results.csv",
        help="Output CSV with rho/p-value and sample size.",
    )
    p.add_argument(
        "--out-plot",
        default="Datasets/01_MLAB/plots/04_income_vs_median_download_scatter.png",
        help="Output scatter plot path.",
    )
    p.add_argument(
        "--log-x",
        action="store_true",
        help="Use log10 scale for income axis (visualization only).",
    )
    return p


def _spearman(x: np.ndarray, y: np.ndarray) -> tuple[float, float | None]:
    """
    Returns (rho, p_value). p_value is None if SciPy is unavailable.
    """
    try:
        from scipy.stats import spearmanr  # type: ignore

        res = spearmanr(x, y)
        return float(res.statistic), float(res.pvalue)
    except Exception:
        # Fallback: compute rho as Pearson correlation of ranks (no p-value).
        rx = pd.Series(x).rank(method="average").to_numpy()
        ry = pd.Series(y).rank(method="average").to_numpy()
        rho = float(np.corrcoef(rx, ry)[0, 1])
        return rho, None


def main() -> None:
    args = build_parser().parse_args()

    in_path = Path(args.input_csv)
    if not in_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {in_path}")

    df = pd.read_csv(in_path)
    for col in (args.x_col, args.y_col):
        if col not in df.columns:
            raise ValueError(
                f"Missing required column '{col}' in {in_path.name}. "
                f"Available columns: {list(df.columns)}"
            )

    x = pd.to_numeric(df[args.x_col], errors="coerce")
    y = pd.to_numeric(df[args.y_col], errors="coerce")
    keep = x.notna() & y.notna()
    x = x[keep].to_numpy()
    y = y[keep].to_numpy()

    if len(x) < 3:
        raise ValueError("Need at least 3 non-missing points for Spearman correlation.")

    rho, pval = _spearman(x, y)

    # Write results
    out_results = Path(args.out_results_csv)
    out_results.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "n": int(len(x)),
                "x_col": args.x_col,
                "y_col": args.y_col,
                "spearman_rho": rho,
                "p_value": pval,
            }
        ]
    ).to_csv(out_results, index=False)

    # Plot scatter + linear trend line (visual guide only; Spearman is rank-based).
    out_plot = Path(args.out_plot)
    out_plot.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(7, 5))
    plt.scatter(x, y, alpha=0.75)

    # Trend line on the plotted x-scale
    if args.log_x:
        x_plot = np.log10(x)
        xlabel = "Median household income (log10 dollars)"
    else:
        x_plot = x
        xlabel = "Median household income (dollars)"

    # Fit line in plotted coordinates, then render on sorted x for a clean line.
    b1, b0 = np.polyfit(x_plot, y, 1)
    xs = np.linspace(float(np.min(x_plot)), float(np.max(x_plot)), 100)
    ys = b1 * xs + b0
    if args.log_x:
        plt.plot(10**xs, ys, linewidth=2)
        plt.xscale("log")
    else:
        plt.plot(xs, ys, linewidth=2)

    plt.xlabel(xlabel)
    plt.ylabel("Median measured download speed (Mbps)")

    title = f"Income vs measured download speed (Spearman ρ = {rho:.3f}"
    if pval is not None:
        title += f", p = {pval:.3g}"
    title += f", n = {len(x)})"
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_plot, dpi=200)
    plt.close()

    print(f"Spearman rho={rho:.6f}, p={pval} (n={len(x)})")
    print(f"Wrote results -> {out_results}")
    print(f"Wrote plot -> {out_plot}")


if __name__ == "__main__":
    main()

