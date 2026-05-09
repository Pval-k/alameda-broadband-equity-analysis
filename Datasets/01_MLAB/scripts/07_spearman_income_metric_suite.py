from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Compute a suite of Spearman correlations between income and multiple M-Lab metrics "
            "(download mean/median, upload mean/median, latency)."
        )
    )
    p.add_argument(
        "--download-metrics-csv",
        default="Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv",
        help="ZCTA-level download metrics.",
    )
    p.add_argument(
        "--upload-metrics-csv",
        default="Datasets/01_MLAB/csvs/06_alameda_zcta_mlab_2020_12_upload_metrics.csv",
        help="ZCTA-level upload metrics.",
    )
    p.add_argument(
        "--income-csv",
        default="Datasets/03_CENSUS/income/csv/01_alameda_zcta_income.csv",
        help="Income CSV (Alameda-only).",
    )
    p.add_argument(
        "--out-results-csv",
        default="Datasets/01_MLAB/csvs/07_spearman_income_metric_suite.csv",
        help="Output results CSV (one row per metric).",
    )
    return p


def _clean_zcta(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.zfill(5)


def _spearman(x: np.ndarray, y: np.ndarray) -> tuple[float, float | None]:
    try:
        from scipy.stats import spearmanr  # type: ignore

        res = spearmanr(x, y)
        return float(res.statistic), float(res.pvalue)
    except Exception:
        rx = pd.Series(x).rank(method="average").to_numpy()
        ry = pd.Series(y).rank(method="average").to_numpy()
        rho = float(np.corrcoef(rx, ry)[0, 1])
        return rho, None


def _run_one(df: pd.DataFrame, x_col: str, y_col: str) -> dict:
    x = pd.to_numeric(df[x_col], errors="coerce")
    y = pd.to_numeric(df[y_col], errors="coerce")
    keep = x.notna() & y.notna()
    x = x[keep].to_numpy()
    y = y[keep].to_numpy()
    rho, p = _spearman(x, y)
    return {
        "metric": y_col,
        "n": int(len(x)),
        "spearman_rho": rho,
        "p_value": p,
    }


def main() -> None:
    args = build_parser().parse_args()

    dl_path = Path(args.download_metrics_csv)
    ul_path = Path(args.upload_metrics_csv)
    inc_path = Path(args.income_csv)
    out_path = Path(args.out_results_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    for p in (dl_path, ul_path, inc_path):
        if not p.exists():
            raise FileNotFoundError(f"Missing required file: {p}")

    dl = pd.read_csv(dl_path, dtype={"zcta": str})
    ul = pd.read_csv(ul_path, dtype={"zcta": str})
    inc = pd.read_csv(inc_path, dtype={"zcta": str})

    dl["zcta"] = _clean_zcta(dl["zcta"])
    ul["zcta"] = _clean_zcta(ul["zcta"])
    inc["zcta"] = _clean_zcta(inc["zcta"])

    inc["median_household_income"] = pd.to_numeric(inc["median_household_income"], errors="coerce")

    # Build one wide table of M-Lab metrics by ZCTA.
    # Drop duplicate label columns so merge doesn't collide on mlab_year/mlab_month.
    ul = ul.drop(columns=[c for c in ["mlab_year", "mlab_month"] if c in ul.columns], errors="ignore")
    mlab = dl.merge(ul, on="zcta", how="outer")
    wide = mlab.merge(inc[["zcta", "median_household_income"]], on="zcta", how="inner")
    wide = wide.dropna(subset=["median_household_income"]).copy()

    x_col = "median_household_income"

    metrics = [
        "median_download_mbps",
        "mean_download_mbps",
        "median_upload_mbps",
        "mean_upload_mbps",
        "median_download_latency_ms",
        "mean_download_latency_ms",
        "median_upload_latency_ms",
        "mean_upload_latency_ms",
    ]
    present = [m for m in metrics if m in wide.columns]
    missing = [m for m in metrics if m not in wide.columns]
    if missing:
        print(f"Warning: missing metrics not found in merged table: {missing}")

    rows: list[dict] = []
    for m in present:
        rows.append(_run_one(wide, x_col=x_col, y_col=m))

    out = pd.DataFrame(rows).sort_values("metric")
    out.to_csv(out_path, index=False)

    # Print for convenience (copy/paste into report).
    print(out.to_string(index=False))
    print(f"\nWrote -> {out_path}")


if __name__ == "__main__":
    main()

