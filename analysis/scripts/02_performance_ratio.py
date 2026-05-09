"""Per-ZCTA performance ratio: M-Lab measured download / FCC advertised download.

Inner-joins FCC advertised (Option A: max-tech per block, then median across
blocks) to M-Lab ZCTA-level metrics (`median_download_mbps`).

Ratio is computed with explicit zero-safety:

    np.where(advertised > 0, measured / advertised, np.nan)

so a 0 / missing FCC value never crashes the script. Skipped rows are tagged
with `ratio_skip_reason = "advertised_zero_or_missing"` for easy auditing.

Outputs a per-ZCTA CSV, a summary CSV, a histogram (with reference lines at
0.5 and 1.0), and a scatter (advertised vs measured) with a `y = x` line and a
shaded `y < 0.5x` "Major Performance Gap" region.
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
            "Compute per-ZCTA performance ratio (M-Lab measured / FCC advertised) "
            "and write summary + plots."
        )
    )
    p.add_argument(
        "--advertised-csv",
        default="Datasets/02_FCC/csv/05_alameda_zcta_advertised_download.csv",
        help="Per-ZCTA FCC advertised download CSV (Option A output).",
    )
    p.add_argument(
        "--mlab-csv",
        default="Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv",
        help="Per-ZCTA M-Lab download metrics CSV.",
    )
    p.add_argument(
        "--out-csv",
        default="analysis/csv/02_alameda_zcta_performance_ratio.csv",
        help="Per-ZCTA ratio output CSV.",
    )
    p.add_argument(
        "--out-summary-csv",
        default="analysis/csv/02_alameda_performance_ratio_summary.csv",
        help="Single-row summary stats CSV.",
    )
    p.add_argument(
        "--out-hist",
        default="analysis/plots/02_performance_ratio_hist.png",
        help="Histogram plot path.",
    )
    p.add_argument(
        "--out-scatter",
        default="analysis/plots/02_advertised_vs_measured_scatter.png",
        help="Scatter plot path (advertised vs measured).",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    adv_path = Path(args.advertised_csv)
    mlab_path = Path(args.mlab_csv)
    out_csv = Path(args.out_csv)
    out_summary = Path(args.out_summary_csv)
    out_hist = Path(args.out_hist)
    out_scatter = Path(args.out_scatter)
    for p in (out_csv, out_summary, out_hist, out_scatter):
        p.parent.mkdir(parents=True, exist_ok=True)

    if not adv_path.exists():
        raise FileNotFoundError(f"FCC advertised CSV not found: {adv_path}")
    if not mlab_path.exists():
        raise FileNotFoundError(f"M-Lab metrics CSV not found: {mlab_path}")

    adv = pd.read_csv(adv_path, dtype={"zcta": str})
    mlab = pd.read_csv(mlab_path, dtype={"zcta": str})
    adv["zcta"] = adv["zcta"].astype(str).str.strip().str.zfill(5)
    mlab["zcta"] = mlab["zcta"].astype(str).str.strip().str.zfill(5)

    keep_mlab_cols = ["zcta", "median_download_mbps"]
    if "download_test_count" in mlab.columns:
        keep_mlab_cols.append("download_test_count")
    keep_adv_cols = ["zcta", "advertised_download_mbps"]
    if "block_count" in adv.columns:
        keep_adv_cols.append("block_count")

    merged = adv[keep_adv_cols].merge(
        mlab[keep_mlab_cols], on="zcta", how="inner"
    )
    print(
        f"Joined {len(merged)} ZCTAs (FCC advertised: {len(adv)}, M-Lab: {len(mlab)})."
    )

    measured = pd.to_numeric(merged["median_download_mbps"], errors="coerce")
    advertised = pd.to_numeric(merged["advertised_download_mbps"], errors="coerce")

    valid = (advertised > 0) & advertised.notna() & measured.notna()
    merged["performance_ratio"] = np.where(valid, measured / advertised, np.nan)
    merged["ratio_skip_reason"] = np.where(valid, "", "advertised_zero_or_missing")

    cols = [
        "zcta",
        "median_download_mbps",
        "advertised_download_mbps",
        "performance_ratio",
        "ratio_skip_reason",
    ]
    if "download_test_count" in merged.columns:
        cols.append("download_test_count")
    if "block_count" in merged.columns:
        cols.append("block_count")
    out_df = merged[cols].sort_values("zcta")
    out_df.to_csv(out_csv, index=False, float_format="%.6f")
    print(f"Wrote per-ZCTA ratios to {out_csv}")

    ratios = pd.to_numeric(out_df["performance_ratio"], errors="coerce").dropna()
    if len(ratios) == 0:
        print("Warning: no valid ratios to summarize/plot; skipping plots.")
        return

    summary = pd.DataFrame(
        [
            {
                "n": int(len(ratios)),
                "mean_ratio": float(ratios.mean()),
                "median_ratio": float(ratios.median()),
                "std_ratio": (
                    float(ratios.std(ddof=1)) if len(ratios) > 1 else float("nan")
                ),
                "min_ratio": float(ratios.min()),
                "max_ratio": float(ratios.max()),
                "p25_ratio": float(ratios.quantile(0.25)),
                "p75_ratio": float(ratios.quantile(0.75)),
                "n_below_0_5": int((ratios < 0.5).sum()),
                "n_above_1_0": int((ratios > 1.0).sum()),
            }
        ]
    )
    summary.to_csv(out_summary, index=False)
    print(f"Wrote summary to {out_summary}")

    # Histogram
    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.hist(ratios, bins=20, edgecolor="black", alpha=0.85)
    ax.axvline(0.5, color="red", linestyle="--", linewidth=1.5, label="ratio = 0.5 (major gap)")
    ax.axvline(1.0, color="green", linestyle="--", linewidth=1.5, label="ratio = 1.0 (delivered)")
    ax.set_xlabel("Performance ratio = measured / advertised")
    ax.set_ylabel("Number of ZCTAs")
    ax.set_title(
        f"Performance ratio distribution across Alameda ZCTAs (n = {len(ratios)})"
    )
    ax.legend(loc="best")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_hist, dpi=200)
    plt.close(fig)
    print(f"Wrote histogram to {out_hist}")

    # Scatter (advertised vs measured), with y=x and shaded y < 0.5*x.
    plot_df = out_df.dropna(subset=["performance_ratio"]).copy()
    x = pd.to_numeric(plot_df["advertised_download_mbps"], errors="coerce").to_numpy()
    y = pd.to_numeric(plot_df["median_download_mbps"], errors="coerce").to_numpy()

    x_max = float(np.nanmax(x)) if len(x) else 1.0
    y_max = float(np.nanmax(y)) if len(y) else 1.0
    upper = max(x_max, y_max) * 1.05

    fig, ax = plt.subplots(figsize=(7.5, 6))
    xs_line = np.linspace(0, upper, 100)
    ax.fill_between(
        xs_line,
        0,
        0.5 * xs_line,
        color="red",
        alpha=0.10,
        label="Major performance gap (measured < 0.5 x advertised)",
    )
    ax.plot(xs_line, xs_line, linestyle="--", color="black", linewidth=1.5, label="y = x (delivered as advertised)")
    ax.plot(
        xs_line,
        0.5 * xs_line,
        linestyle=":",
        color="red",
        linewidth=1.2,
        label="y = 0.5 x",
    )
    ax.scatter(x, y, alpha=0.8, edgecolor="black", linewidth=0.4)
    ax.set_xlim(0, upper)
    ax.set_ylim(0, upper)
    ax.set_xlabel("FCC advertised download speed (Mbps, Option A)")
    ax.set_ylabel("M-Lab measured median download speed (Mbps)")
    ax.set_title(
        f"Advertised vs measured download per ZCTA (n = {len(plot_df)})"
    )
    ax.legend(loc="lower right")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_scatter, dpi=200)
    plt.close(fig)
    print(f"Wrote scatter to {out_scatter}")


if __name__ == "__main__":
    main()
