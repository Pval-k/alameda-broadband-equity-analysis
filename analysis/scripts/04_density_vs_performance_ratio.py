"""Density vs broadband performance ratio (tests the density half of H3).

Inner-joins per-ZCTA density (``population_per_sq_mi`` + ``density_group``) from
``Datasets/03_CENSUS/population_density/csv/03_alameda_zcta_density_groups.csv``
to the per-ZCTA performance ratios from
``analysis/csv/02_alameda_zcta_performance_ratio.csv``.

For each available ratio column (``performance_ratio`` and, when present,
``performance_ratio_p75``) the script runs:

1. **Spearman rank correlation** between ``population_per_sq_mi`` and the ratio,
   directly probing the H3 prediction that lower-density ZCTAs see a larger
   "truth gap".
2. **One-way ANOVA** comparing the ratio across the three tertile groups
   (``low`` / ``medium`` / ``high``), so the test mirrors the level-based ANOVA
   from ``01_anova_density_vs_measured_download.py``.

Outputs
-------
* ``analysis/csv/04_density_vs_performance_ratio_spearman.csv`` — one row per
  ratio column with ``n``, ``spearman_rho``, ``p_value``.
* ``analysis/csv/04_density_vs_performance_ratio_anova.csv`` — per-group rows
  (``density_group``, ``n``, ``mean_ratio``, ``median_ratio``, ``std_ratio``)
  plus one ANOVA row per ratio column.
* ``analysis/plots/04_density_vs_performance_ratio_scatter.png`` — log-x scatter
  of density vs the median-based ratio with reference lines at 0.5 and 1.0.
* ``analysis/plots/04_density_vs_performance_ratio_p75_scatter.png`` — same plot
  for the P75-based ratio (only written when ``performance_ratio_p75`` exists).
* ``analysis/plots/04_density_group_vs_performance_ratio_box.png`` — boxplot of
  the median-based ratio by density tertile with one dot per ZCTA overlaid.

Interpretation
--------------
``spearman_rho > 0`` and small p-value supports H3 — denser ZCTAs receive a
higher share of advertised speed (i.e. lower-density ZCTAs face a larger
performance gap). ``rho`` near zero means no monotonic density effect on the
gap. The ANOVA is a complementary group-wise check using the same tertile cut
as ``03_alameda_zcta_density_groups.py``.
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
            "Spearman correlation + one-way ANOVA between population density and the "
            "performance ratio (measured / advertised) per ZCTA. Tests density half of H3."
        )
    )
    p.add_argument(
        "--density-csv",
        default="Datasets/03_CENSUS/population_density/csv/03_alameda_zcta_density_groups.csv",
        help="ZCTA density CSV with `zcta`, `population_per_sq_mi`, `density_group`.",
    )
    p.add_argument(
        "--ratio-csv",
        default="analysis/csv/02_alameda_zcta_performance_ratio.csv",
        help="Per-ZCTA performance ratio CSV (output of 02_performance_ratio.py).",
    )
    p.add_argument(
        "--out-spearman-csv",
        default="analysis/csv/04_density_vs_performance_ratio_spearman.csv",
        help="Where to write Spearman results (one row per ratio column).",
    )
    p.add_argument(
        "--out-anova-csv",
        default="analysis/csv/04_density_vs_performance_ratio_anova.csv",
        help="Where to write ANOVA per-group + summary rows.",
    )
    p.add_argument(
        "--out-scatter",
        default="analysis/plots/04_density_vs_performance_ratio_scatter.png",
        help="Scatter plot path for median-based ratio.",
    )
    p.add_argument(
        "--out-scatter-p75",
        default="analysis/plots/04_density_vs_performance_ratio_p75_scatter.png",
        help="Scatter plot path for P75-based ratio (skipped if column absent).",
    )
    p.add_argument(
        "--out-box",
        default="analysis/plots/04_density_group_vs_performance_ratio_box.png",
        help="Boxplot path comparing median-based ratio across density tertiles.",
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


def _scatter(
    x: np.ndarray,
    y: np.ndarray,
    rho: float,
    pval: float | None,
    out_path: Path,
    *,
    ratio_label: str,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.scatter(x, y, alpha=0.8, edgecolor="black", linewidth=0.4)
    ax.set_xscale("log")
    ax.axhline(1.0, color="green", linestyle="--", linewidth=1.2, label="ratio = 1.0 (delivered as advertised)")
    ax.axhline(0.5, color="red", linestyle="--", linewidth=1.2, label="ratio = 0.5 (major performance gap)")
    ax.set_xlabel("Population density (people per square mile, log scale)")
    ax.set_ylabel(f"Performance ratio ({ratio_label})")
    title = f"Density vs performance ratio ({ratio_label}) — Spearman ρ = {rho:.3f}"
    if pval is not None:
        title += f", p = {pval:.3g}"
    title += f", n = {len(x)}"
    ax.set_title(title)
    ax.grid(True, alpha=0.3, which="both")
    ax.legend(loc="best", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)


def _correlate(
    df: pd.DataFrame, ratio_col: str, label: str, scatter_path: Path
) -> dict | None:
    if ratio_col not in df.columns:
        return None
    x = pd.to_numeric(df["population_per_sq_mi"], errors="coerce")
    y = pd.to_numeric(df[ratio_col], errors="coerce")
    keep = x.notna() & y.notna() & (x > 0)
    x = x[keep].to_numpy()
    y = y[keep].to_numpy()
    if len(x) < 3:
        print(
            f"Skipping Spearman for {ratio_col}: only {len(x)} non-missing points "
            "(need >= 3)."
        )
        return None

    rho, pval = _spearman(x, y)
    _scatter(x, y, rho, pval, scatter_path, ratio_label=label)
    print(
        f"Spearman {ratio_col}: n={len(x)}, rho={rho:+.4f}"
        + (f", p={pval:.3g}" if pval is not None else ", p=NA (SciPy missing)")
    )
    print(f"  wrote scatter -> {scatter_path}")
    return {
        "ratio_col": ratio_col,
        "ratio_label": label,
        "n": int(len(x)),
        "spearman_rho": rho,
        "p_value": pval,
    }


def _anova_for_ratio(
    df: pd.DataFrame, ratio_col: str, label: str
) -> tuple[list[dict], dict[str, np.ndarray]]:
    """Return (per-group rows + ANOVA row, grouped arrays). Empty list if column missing."""
    if ratio_col not in df.columns:
        return [], {}

    rows: list[dict] = []
    grouped: dict[str, np.ndarray] = {}
    for g in GROUPS:
        vals = pd.to_numeric(
            df.loc[df["density_group"] == g, ratio_col], errors="coerce"
        ).dropna().to_numpy(dtype=float)
        grouped[g] = vals
        rows.append(
            {
                "row_type": "group",
                "ratio_col": ratio_col,
                "ratio_label": label,
                "density_group": g,
                "n": int(len(vals)),
                "mean_ratio": float(np.mean(vals)) if len(vals) else float("nan"),
                "median_ratio": (
                    float(np.median(vals)) if len(vals) else float("nan")
                ),
                "std_ratio": (
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
            f"Warning: at least one density group has < 2 ZCTAs for {ratio_col}; "
            "ANOVA not meaningful."
        )

    rows.append(
        {
            "row_type": "anova",
            "ratio_col": ratio_col,
            "ratio_label": label,
            "density_group": "all",
            "n": int(sum(len(grouped[g]) for g in GROUPS)),
            "mean_ratio": "",
            "median_ratio": "",
            "std_ratio": "",
            "f_statistic": f_stat,
            "p_value": p_val if p_val is not None else "",
        }
    )
    print(
        f"ANOVA {ratio_col}: F={f_stat:.4f}"
        + (f", p={p_val:.3g}" if p_val is not None else ", p=NA (SciPy missing)")
    )
    return rows, grouped


def _boxplot(
    grouped: dict[str, np.ndarray],
    f_stat: float,
    p_val: float | None,
    out_path: Path,
    *,
    ratio_label: str,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(7.5, 5))
    box_data = [grouped[g] for g in GROUPS]
    ax.boxplot(box_data, labels=GROUPS, showfliers=False)

    rng = np.random.default_rng(seed=0)
    for i, g in enumerate(GROUPS, start=1):
        vals = grouped[g]
        if len(vals) == 0:
            continue
        offset = rng.uniform(-0.12, 0.12, size=len(vals))
        ax.scatter(
            np.full_like(vals, i, dtype=float) + offset,
            vals,
            s=22,
            alpha=0.7,
            edgecolor="black",
            linewidth=0.4,
        )

    ax.axhline(1.0, color="green", linestyle="--", linewidth=1.0)
    ax.axhline(0.5, color="red", linestyle="--", linewidth=1.0)
    ax.set_xlabel("Density group (tertiles of population_per_sq_mi)")
    ax.set_ylabel(f"Performance ratio ({ratio_label})")
    title = "Performance ratio by density group"
    if not np.isnan(f_stat):
        title += f"\nANOVA F = {f_stat:.3f}"
        if p_val is not None:
            title += f", p = {p_val:.3g}"
    title += f", n = {int(sum(len(grouped[g]) for g in GROUPS))}"
    ax.set_title(title)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)


def main() -> None:
    args = build_parser().parse_args()
    density_path = Path(args.density_csv)
    ratio_path = Path(args.ratio_csv)
    out_spearman = Path(args.out_spearman_csv)
    out_anova = Path(args.out_anova_csv)
    out_scatter = Path(args.out_scatter)
    out_scatter_p75 = Path(args.out_scatter_p75)
    out_box = Path(args.out_box)

    if not density_path.exists():
        raise FileNotFoundError(f"Density CSV not found: {density_path}")
    if not ratio_path.exists():
        raise FileNotFoundError(f"Performance ratio CSV not found: {ratio_path}")

    density = pd.read_csv(density_path, dtype={"zcta": str})
    ratios = pd.read_csv(ratio_path, dtype={"zcta": str})
    density["zcta"] = density["zcta"].astype(str).str.strip().str.zfill(5)
    ratios["zcta"] = ratios["zcta"].astype(str).str.strip().str.zfill(5)

    required_density = {"zcta", "population_per_sq_mi", "density_group"}
    missing_density = required_density - set(density.columns)
    if missing_density:
        raise ValueError(
            f"Density CSV missing required columns: {sorted(missing_density)}"
        )
    if "performance_ratio" not in ratios.columns:
        raise ValueError(
            "Performance ratio CSV missing required column `performance_ratio`."
        )

    keep_ratio_cols = ["zcta", "performance_ratio"]
    if "performance_ratio_p75" in ratios.columns:
        keep_ratio_cols.append("performance_ratio_p75")

    merged = density.merge(
        ratios[keep_ratio_cols], on="zcta", how="inner"
    )
    merged["density_group"] = pd.Categorical(
        merged["density_group"], categories=GROUPS, ordered=True
    )
    merged = merged.dropna(subset=["density_group"]).copy()
    print(
        f"Joined {len(merged)} ZCTAs (density: {len(density)}, ratio: {len(ratios)})."
    )

    spearman_rows: list[dict] = []
    median_row = _correlate(
        merged,
        ratio_col="performance_ratio",
        label="median measured / advertised",
        scatter_path=out_scatter,
    )
    if median_row is not None:
        spearman_rows.append(median_row)

    p75_row = _correlate(
        merged,
        ratio_col="performance_ratio_p75",
        label="P75 measured / advertised",
        scatter_path=out_scatter_p75,
    )
    if p75_row is not None:
        spearman_rows.append(p75_row)

    if not spearman_rows:
        raise RuntimeError(
            "No valid ratio column found to correlate with density. "
            "Re-run 02_performance_ratio.py first."
        )

    out_spearman.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(spearman_rows).to_csv(out_spearman, index=False)
    print(f"Wrote Spearman results -> {out_spearman}")

    anova_rows: list[dict] = []
    median_anova_rows, median_grouped = _anova_for_ratio(
        merged, "performance_ratio", "median measured / advertised"
    )
    anova_rows.extend(median_anova_rows)
    p75_anova_rows, _ = _anova_for_ratio(
        merged, "performance_ratio_p75", "P75 measured / advertised"
    )
    anova_rows.extend(p75_anova_rows)

    out_anova.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        anova_rows,
        columns=[
            "row_type",
            "ratio_col",
            "ratio_label",
            "density_group",
            "n",
            "mean_ratio",
            "median_ratio",
            "std_ratio",
            "f_statistic",
            "p_value",
        ],
    ).to_csv(out_anova, index=False)
    print(f"Wrote ANOVA summary -> {out_anova}")

    if median_grouped:
        median_anova = next(
            (r for r in median_anova_rows if r["row_type"] == "anova"), None
        )
        f_stat = (
            float(median_anova["f_statistic"]) if median_anova is not None else float("nan")
        )
        p_val_raw = median_anova["p_value"] if median_anova is not None else ""
        p_val = float(p_val_raw) if p_val_raw not in ("", None) else None
        _boxplot(
            median_grouped,
            f_stat,
            p_val,
            out_box,
            ratio_label="median measured / advertised",
        )
        print(f"Wrote boxplot -> {out_box}")


if __name__ == "__main__":
    main()
