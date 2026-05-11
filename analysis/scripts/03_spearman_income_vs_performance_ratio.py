"""Spearman correlation between median household income and the performance ratio.

Tests H3 (the "broadband truth gap" hypothesis): if lower-income ZCTAs receive a
smaller share of the speed ISPs advertised, then income should be positively
rank-correlated with the performance ratio (= measured / advertised).

Inputs
------
* ``analysis/csv/02_alameda_zcta_performance_ratio.csv`` — per-ZCTA
  ``performance_ratio`` (median-based) and, if available, ``performance_ratio_p75``.
* ``Datasets/03_CENSUS/income/csv/01_alameda_zcta_income.csv`` — ACS5 B19013
  ``median_household_income``.

Outputs
-------
* ``analysis/csv/03_spearman_income_vs_performance_ratio_results.csv`` — one row
  per ratio variant (median, P75) with ``n``, ``spearman_rho``, ``p_value``.
* ``analysis/plots/03_income_vs_performance_ratio_scatter.png`` — income vs
  median-based ratio with horizontal reference lines at ratio = 1.0 (delivered)
  and ratio = 0.5 (major gap).
* ``analysis/plots/03_income_vs_performance_ratio_p75_scatter.png`` — same but
  for the P75-based ratio (only written if ``performance_ratio_p75`` is present
  in the input).

Interpretation
--------------
``spearman_rho > 0`` with a small p-value supports H3 — higher-income ZCTAs
receive a higher share of the advertised speed (and lower-income ZCTAs see a
larger "truth gap"). ``rho`` near zero means no monotonic relationship; ``rho
< 0`` would actually mean lower-income ZCTAs get more of what was promised.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Spearman correlation between median household income and the "
            "performance ratio (measured / advertised) per ZCTA."
        )
    )
    p.add_argument(
        "--ratio-csv",
        default="analysis/csv/02_alameda_zcta_performance_ratio.csv",
        help="Per-ZCTA performance ratio CSV (output of 02_performance_ratio.py).",
    )
    p.add_argument(
        "--income-csv",
        default="Datasets/03_CENSUS/income/csv/01_alameda_zcta_income.csv",
        help="ACS5 income CSV (column: median_household_income).",
    )
    p.add_argument(
        "--out-results-csv",
        default=(
            "analysis/csv/03_spearman_income_vs_performance_ratio_results.csv"
        ),
        help="Where to write rho/p-value/n per ratio variant.",
    )
    p.add_argument(
        "--out-plot",
        default="analysis/plots/03_income_vs_performance_ratio_scatter.png",
        help="Scatter plot for the median-based ratio.",
    )
    p.add_argument(
        "--out-plot-p75",
        default="analysis/plots/03_income_vs_performance_ratio_p75_scatter.png",
        help="Scatter plot for the P75-based ratio (if column is present).",
    )
    p.add_argument(
        "--log-x",
        action="store_true",
        help="Use log10 scale for income axis (visualization only).",
    )
    return p


def _spearman(x: np.ndarray, y: np.ndarray) -> tuple[float, float | None]:
    """Return (rho, p_value); p_value is None if SciPy is unavailable."""
    try:
        from scipy.stats import spearmanr  # type: ignore

        res = spearmanr(x, y)
        return float(res.statistic), float(res.pvalue)
    except Exception:
        rx = pd.Series(x).rank(method="average").to_numpy()
        ry = pd.Series(y).rank(method="average").to_numpy()
        rho = float(np.corrcoef(rx, ry)[0, 1])
        return rho, None


def _plot_scatter(
    x: np.ndarray,
    y: np.ndarray,
    rho: float,
    pval: float | None,
    out_path: Path,
    *,
    ratio_label: str,
    log_x: bool,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(7.5, 5))

    if log_x:
        xs_fit = np.log10(x)
        xlabel = "Median household income (log10 dollars)"
    else:
        xs_fit = x
        xlabel = "Median household income (dollars)"

    b1, b0 = np.polyfit(xs_fit, y, 1)
    xs_line = np.linspace(float(np.min(xs_fit)), float(np.max(xs_fit)), 100)
    ys_line = b1 * xs_line + b0

    if log_x:
        ax.plot(10**xs_line, ys_line, linewidth=2, color="tab:blue", label="Linear fit (on ranks: see ρ)")
        ax.set_xscale("log")
    else:
        ax.plot(xs_line, ys_line, linewidth=2, color="tab:blue", label="Linear fit (on ranks: see ρ)")

    ax.scatter(x, y, alpha=0.8, edgecolor="black", linewidth=0.4)

    ax.axhline(1.0, color="green", linestyle="--", linewidth=1.2, label="ratio = 1.0 (delivered as advertised)")
    ax.axhline(0.5, color="red", linestyle="--", linewidth=1.2, label="ratio = 0.5 (major performance gap)")

    ax.set_xlabel(xlabel)
    ax.set_ylabel(f"Performance ratio ({ratio_label})")
    title = f"Income vs performance ratio ({ratio_label}) — Spearman ρ = {rho:.3f}"
    if pval is not None:
        title += f", p = {pval:.3g}"
    title += f", n = {len(x)}"
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)


def _correlate_and_plot(
    df: pd.DataFrame,
    ratio_col: str,
    label: str,
    plot_path: Path,
    log_x: bool,
) -> dict | None:
    """Run Spearman + scatter for one ratio column. Returns a results row dict."""
    if ratio_col not in df.columns:
        return None
    income = pd.to_numeric(df["median_household_income"], errors="coerce")
    ratio = pd.to_numeric(df[ratio_col], errors="coerce")
    keep = income.notna() & ratio.notna()
    x = income[keep].to_numpy()
    y = ratio[keep].to_numpy()
    if len(x) < 3:
        print(
            f"Skipping {ratio_col}: only {len(x)} non-missing points "
            "(need >= 3 for Spearman)."
        )
        return None

    rho, pval = _spearman(x, y)
    _plot_scatter(
        x,
        y,
        rho,
        pval,
        plot_path,
        ratio_label=label,
        log_x=log_x,
    )
    print(
        f"{ratio_col}: n={len(x)}, rho={rho:+.4f}"
        + (f", p={pval:.3g}" if pval is not None else ", p=NA (SciPy missing)")
    )
    print(f"  wrote scatter -> {plot_path}")
    return {
        "ratio_col": ratio_col,
        "ratio_label": label,
        "n": int(len(x)),
        "spearman_rho": rho,
        "p_value": pval,
    }


def main() -> None:
    args = build_parser().parse_args()
    ratio_path = Path(args.ratio_csv)
    income_path = Path(args.income_csv)
    out_results = Path(args.out_results_csv)
    out_plot = Path(args.out_plot)
    out_plot_p75 = Path(args.out_plot_p75)

    if not ratio_path.exists():
        raise FileNotFoundError(f"Performance ratio CSV not found: {ratio_path}")
    if not income_path.exists():
        raise FileNotFoundError(f"Income CSV not found: {income_path}")

    ratio_df = pd.read_csv(ratio_path, dtype={"zcta": str})
    income_df = pd.read_csv(income_path, dtype={"zcta": str})
    ratio_df["zcta"] = ratio_df["zcta"].astype(str).str.strip().str.zfill(5)
    income_df["zcta"] = income_df["zcta"].astype(str).str.strip().str.zfill(5)

    merged = ratio_df.merge(
        income_df[["zcta", "median_household_income"]],
        on="zcta",
        how="inner",
    )
    print(
        f"Joined {len(merged)} ZCTAs (ratio: {len(ratio_df)}, income: {len(income_df)})."
    )

    rows: list[dict] = []
    median_row = _correlate_and_plot(
        merged,
        ratio_col="performance_ratio",
        label="median measured / advertised",
        plot_path=out_plot,
        log_x=args.log_x,
    )
    if median_row is not None:
        rows.append(median_row)

    p75_row = _correlate_and_plot(
        merged,
        ratio_col="performance_ratio_p75",
        label="P75 measured / advertised",
        plot_path=out_plot_p75,
        log_x=args.log_x,
    )
    if p75_row is not None:
        rows.append(p75_row)

    if not rows:
        raise RuntimeError(
            "No valid ratio column found to correlate against income. "
            "Re-run 02_performance_ratio.py first."
        )

    out_results.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_results, index=False)
    print(f"Wrote results -> {out_results}")


if __name__ == "__main__":
    main()
