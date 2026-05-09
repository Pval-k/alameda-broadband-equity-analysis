"""One-way ANOVA: measured download speed across low/medium/high density ZCTA groups.

Inner-joins density-group labels (`density_group` from
`03_alameda_zcta_density_groups.csv`) to the M-Lab ZCTA-level metrics
(`median_download_mbps`), runs scipy.stats.f_oneway across the three groups,
and writes a CSV summary plus a boxplot with jittered ZCTA points.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

GROUPS = ["low", "medium", "high"]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "One-way ANOVA comparing M-Lab measured download speed across "
            "low/medium/high density groups (Alameda ZCTAs)."
        )
    )
    p.add_argument(
        "--density-csv",
        default="Datasets/03_CENSUS/population_density/csv/03_alameda_zcta_density_groups.csv",
        help="ZCTA density-group CSV with `zcta` and `density_group`.",
    )
    p.add_argument(
        "--mlab-csv",
        default="Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv",
        help="ZCTA M-Lab metrics CSV with `zcta` and `median_download_mbps`.",
    )
    p.add_argument(
        "--out-csv",
        default="analysis/csv/01_anova_density_vs_measured_download.csv",
        help="Output CSV for per-group stats + ANOVA result row.",
    )
    p.add_argument(
        "--out-plot",
        default="analysis/plots/01_density_group_vs_measured_download_box.png",
        help="Output boxplot path (with jittered ZCTA points overlaid).",
    )
    return p


def _f_oneway(*groups: np.ndarray) -> tuple[float, float | None]:
    """Return (F, p). Uses scipy.stats if available, else manual SSB/SSW fallback (no p)."""
    try:
        from scipy.stats import f_oneway  # type: ignore

        res = f_oneway(*groups)
        return float(res.statistic), float(res.pvalue)
    except Exception:
        all_vals = np.concatenate(groups)
        grand = float(np.mean(all_vals))
        k = len(groups)
        n = sum(len(g) for g in groups)
        ssb = sum(len(g) * (np.mean(g) - grand) ** 2 for g in groups)
        ssw = sum(((g - np.mean(g)) ** 2).sum() for g in groups)
        if k < 2 or n - k <= 0 or ssw == 0:
            return float("nan"), None
        msb = ssb / (k - 1)
        msw = ssw / (n - k)
        return float(msb / msw), None


def main() -> None:
    args = build_parser().parse_args()
    density_path = Path(args.density_csv)
    mlab_path = Path(args.mlab_csv)
    out_csv = Path(args.out_csv)
    out_plot = Path(args.out_plot)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_plot.parent.mkdir(parents=True, exist_ok=True)

    if not density_path.exists():
        raise FileNotFoundError(f"Density groups CSV not found: {density_path}")
    if not mlab_path.exists():
        raise FileNotFoundError(f"M-Lab download metrics CSV not found: {mlab_path}")

    density = pd.read_csv(density_path, dtype={"zcta": str})
    mlab = pd.read_csv(mlab_path, dtype={"zcta": str})

    density["zcta"] = density["zcta"].astype(str).str.strip().str.zfill(5)
    mlab["zcta"] = mlab["zcta"].astype(str).str.strip().str.zfill(5)

    if "density_group" not in density.columns:
        raise ValueError("Density CSV missing required column `density_group`.")
    if "median_download_mbps" not in mlab.columns:
        raise ValueError("M-Lab CSV missing required column `median_download_mbps`.")

    n_density = len(density)
    n_mlab = len(mlab)
    merged = density.merge(
        mlab[["zcta", "median_download_mbps"]], on="zcta", how="inner"
    )
    merged["median_download_mbps"] = pd.to_numeric(
        merged["median_download_mbps"], errors="coerce"
    )
    merged = merged.dropna(subset=["median_download_mbps"]).copy()
    dropped = n_density - len(merged)
    print(
        f"Joined {len(merged)} ZCTAs (density rows: {n_density}, M-Lab rows: {n_mlab}; "
        f"dropped {dropped} density ZCTAs without M-Lab download metrics)."
    )

    merged["density_group"] = pd.Categorical(
        merged["density_group"], categories=GROUPS, ordered=True
    )
    merged = merged.dropna(subset=["density_group"])

    grouped: dict[str, np.ndarray] = {}
    rows: list[dict[str, object]] = []
    for label in GROUPS:
        vals = merged.loc[
            merged["density_group"] == label, "median_download_mbps"
        ].to_numpy(dtype=float)
        grouped[label] = vals
        rows.append(
            {
                "row_type": "group",
                "density_group": label,
                "n": int(len(vals)),
                "mean_mbps": float(np.mean(vals)) if len(vals) else float("nan"),
                "median_mbps": float(np.median(vals)) if len(vals) else float("nan"),
                "std_mbps": (
                    float(np.std(vals, ddof=1)) if len(vals) > 1 else float("nan")
                ),
                "f_statistic": "",
                "p_value": "",
            }
        )

    if all(len(grouped[g]) >= 2 for g in GROUPS):
        f_stat, p_val = _f_oneway(grouped["low"], grouped["medium"], grouped["high"])
    else:
        f_stat, p_val = float("nan"), None
        print(
            "Warning: at least one density group has fewer than 2 ZCTAs; "
            "F-statistic not meaningful."
        )

    rows.append(
        {
            "row_type": "anova",
            "density_group": "all",
            "n": int(sum(len(grouped[g]) for g in GROUPS)),
            "mean_mbps": "",
            "median_mbps": "",
            "std_mbps": "",
            "f_statistic": f_stat,
            "p_value": p_val if p_val is not None else "",
        }
    )

    out_df = pd.DataFrame(
        rows,
        columns=[
            "row_type",
            "density_group",
            "n",
            "mean_mbps",
            "median_mbps",
            "std_mbps",
            "f_statistic",
            "p_value",
        ],
    )
    out_df.to_csv(out_csv, index=False)
    print(f"Wrote ANOVA summary to {out_csv}")

    # Boxplot with jittered points
    fig, ax = plt.subplots(figsize=(7.5, 5))
    box_data = [grouped[g] for g in GROUPS]
    ax.boxplot(box_data, labels=GROUPS, showfliers=False)

    rng = np.random.default_rng(seed=0)
    for i, g in enumerate(GROUPS, start=1):
        vals = grouped[g]
        if len(vals) == 0:
            continue
        jitter = rng.uniform(-0.12, 0.12, size=len(vals))
        ax.scatter(
            np.full_like(vals, i, dtype=float) + jitter,
            vals,
            s=22,
            alpha=0.7,
            edgecolor="black",
            linewidth=0.4,
        )

    ax.set_xlabel("Density group (tertiles of population_per_sq_mi)")
    ax.set_ylabel("Median measured download speed (Mbps)")
    title = "ZCTA measured download speed by density group"
    if not np.isnan(f_stat):
        title += f"\nANOVA F = {f_stat:.3f}"
        if p_val is not None:
            title += f", p = {p_val:.3g}"
    title += f", n = {int(sum(len(grouped[g]) for g in GROUPS))}"
    ax.set_title(title)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_plot, dpi=200)
    plt.close(fig)

    print(f"Wrote boxplot to {out_plot}")
    if not np.isnan(f_stat):
        msg = f"F-statistic={f_stat:.6f}"
        if p_val is not None:
            msg += f", p-value={p_val:.6g}"
        print(msg)


if __name__ == "__main__":
    main()
