# Broadband Performance and Equity in Alameda County, California

## Overview
This project analyzes broadband performance disparities across Alameda County, California by comparing real-world internet speed measurements with ISP-advertised speeds. It also investigates how broadband performance relates to socioeconomic and geographic factors such as median household income and population density.

The goal is to understand whether meaningful digital inequities exist within a single county and how closely advertised broadband speeds reflect real-world performance.

---

## Research Questions
1. How does broadband performance vary across ZCTAs in Alameda County?
2. Is there a relationship between median household income and internet speed?
3. Do urban and rural areas experience different broadband performance levels?
4. How accurate are ISP-advertised speeds compared to real-world measurements?

---

## Hypotheses
- Higher-income ZCTAs will have faster download and upload speeds.
- Urban (high-density) areas will have better broadband performance than lower-density areas.
- ISP-advertised speeds will generally exceed measured speeds.
- The gap between advertised and actual speeds will be larger in lower-income areas.

---

## Datasets

### 1. M-Lab Internet Speed Tests
- Provides real-world download speed, upload speed, and latency data
- Aggregated at the ZCTA level for 2020

### 2. FCC Broadband Data
- Contains ISP-advertised maximum download and upload speeds
- Used to compare advertised vs actual performance

### 3. U.S. Census Data
- Median household income
- ZCTA population and population density (land area from the Alameda block-to-ZCTA crosswalk; see scripts below)

---

## Methodology

### Data Processing
- All datasets filtered to year **2020**
- Data aggregated at **ZCTA level**
- Datasets merged using geographic alignment across M-Lab, FCC, and Census data

---

## Crosswalk scripts (Census block → ZCTA, Alameda)

The block-to-ZCTA crosswalk is shared by the FCC pipeline (mapping blocks to ZCTAs), population density (ZCTA land area totals), and other ZCTA-level joins.

**Raw inputs (not tracked in git):**
- `Datasets/00_crosswalk/csv/raw_block_to_zcta.txt` — national Census block-to-ZCTA relationship file

### `Datasets/00_crosswalk/scripts/00_alameda_block_to_zcta.py`
- Builds an Alameda-only block-to-ZCTA crosswalk from the national relationship file.
- Keeps key columns only (`GEOID`, `ZCTA`, `LAND_AREA`), where `LAND_AREA` is the land area corresponding to the block relationship row (block-part area), not full ZCTA area.
- If one `GEOID` appears with multiple `ZCTA` values, it keeps the row with the largest `LAND_AREA` and drops the rest.
- Output: `Datasets/00_crosswalk/csv/00_alameda_block_to_zcta_cleaned.csv`

### `Datasets/00_crosswalk/scripts/01_aggregate_zcta_land_area.py`
- Aggregates the cleaned block-to-ZCTA crosswalk to ZCTA-level land area totals.
- Sums `LAND_AREA` across all blocks in each ZCTA to produce `total_land_area`.
- Output: `Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv`

**Important `LAND_AREA` note:**  
- In script `00`, `LAND_AREA` is used as a **tie-break** (keep max) only when the same block maps to multiple ZCTAs.  
- In script `01`, `LAND_AREA` is **summed** across blocks to get total land area per ZCTA (for density calculations).

### Crosswalk run order
1. `00_alameda_block_to_zcta.py`
2. `01_aggregate_zcta_land_area.py`

---

## FCC Pipeline Scripts

The FCC workflow is split into small scripts so each step is easy to verify. It expects the crosswalk outputs above (`00_alameda_block_to_zcta_cleaned.csv`) before `02_map_blocks_to_zcta.py`.

**Raw inputs (not tracked in git):**
- `Datasets/02_FCC/csv/raw_CA_FCC_fixed_Dec2020.csv` — statewide FCC fixed broadband extract (Dec 2020)
- `Datasets/02_FCC/csv/raw_translate_fcc_2010_to_2020.txt` (or similar name) — California Census 2010→2020 tabulation block relationship file (pipe-delimited), used to build `00_alameda_2010_to_2020_bridge.csv`.

### `Datasets/02_FCC/scripts/00_alameda_2010_to_2020_bridge.py`
- Filters the California block relationship file to Alameda County (`STATE_2010` 06, `COUNTY_2010` 001), builds 15-digit `geoid_2010` and `geoid_2020`, and keeps one row per 2010 block by sorting `AREALAND_INT` descending (largest intersecting land area wins when a 2010 block splits into several 2020 blocks).
- Output: `Datasets/02_FCC/csv/00_alameda_2010_to_2020_bridge.csv` (`geoid_2010`, `geoid_2020`, `AREALAND_INT`).

### `Datasets/02_FCC/scripts/01_clean_fcc_blocks.py`

**Why we added `00_alameda_2010_to_2020_bridge.csv`:** The Dec 2020 FCC data still labels blocks with **2010** Census block IDs. Our crosswalk and Census layers use **2020** block IDs. Those IDs do not always match line by line, so linking FCC straight to the crosswalk dropped a large share of rows. This CSV lists each Alameda 2010 block next to its matching 2020 block (from Census’s relationship file), so FCC rows can be rewritten to the same block IDs the crosswalk uses.

- Reads raw FCC fixed broadband data for Dec 2020 (`Datasets/02_FCC/csv/raw_CA_FCC_fixed_Dec2020.csv` by default).
- If `00_alameda_2010_to_2020_bridge.csv` is present, each FCC `BlockCode` is looked up as a 2010 block and replaced with the matching 2020 block ID from that CSV. Rows that have no match keep the original `BlockCode`.
- Filters to Alameda County blocks (`BlockCode` starts with `06001`) and residential records.
- Maps `TechCode` to readable `TechCategory`.
- Produces one row per `BlockCode + TechCategory` by keeping the maximum advertised download/upload (`max_ad_down`, `max_ad_up`) within that group.
- Output: `Datasets/02_FCC/csv/01_FCC_alameda_2020_block_level.csv`

### `Datasets/02_FCC/scripts/02_map_blocks_to_zcta.py`
- Joins FCC block-level rows to the Alameda block-to-ZCTA crosswalk.
- If the same block appears multiple times in the crosswalk with different ZCTAs, it keeps the row with the largest `LAND_AREA` and drops the others.
- Writes mapped rows so each FCC row has a `zcta`.
- Output: `Datasets/02_FCC/csv/02_FCC_alameda_2020_block_zcta_mapped.csv`

### `Datasets/02_FCC/scripts/03_collapse_block_tech_rows.py`
- Collapses mapped rows to one row per `zcta + BlockCode + TechCategory`.
- Script 02 can contain duplicate keys (same block + tech repeated). This step removes duplicates so medians in script 04 are not biased.
- If duplicate rows exist within that key, it keeps the maximum `max_ad_down` and `max_ad_up` for the key.
- Writes `BlockCode` and `zcta` as zero-padded text so CSV tools do not drop leading zeros.
- Output: `Datasets/02_FCC/csv/03_FCC_alameda_2020_block_tech_collapsed.csv`

### `Datasets/02_FCC/scripts/04_aggregate_zcta_tech.py`
- Aggregates collapsed rows to final `zcta + TechCategory` metrics.
- For each `zcta + TechCategory`, it summarizes the distribution of block-level max speeds (median, p75, max, and **coefficient of variation** = sample standard deviation / mean) for advertised download/upload speeds.
- CV is undefined for a single block or when the mean is zero; use it to spot uneven speeds within a ZCTA when medians look similar.
- Output: `Datasets/02_FCC/csv/04_FCC_alameda_2020_zcta_tech_metrics.csv`

### `Datasets/02_FCC/scripts/05_alameda_zcta_advertised_download.py`
- Derives **one advertised download speed per ZCTA** from `03_FCC_alameda_2020_block_tech_collapsed.csv` using **Option A** (the standard "best buyable, typical block" approach):
  1. **Per block**: take the **max** of `max_ad_down` across `TechCategory` (the best tech available at that block — if a block has both DSL and Fiber, Fiber wins).
  2. **Per ZCTA**: take the **median** of those per-block bests across blocks in the ZCTA (the typical block's ceiling).
- **Why median, not mean**: one fiber-heavy street can pull a mean up to a number nobody else in the ZCTA actually receives; the median reflects what the typical block can really purchase.
- Also writes `mean_block_best_ad_down`, `p25_block_best_ad_down`, `p75_block_best_ad_down`, and `block_count` for context.
- Output: `Datasets/02_FCC/csv/05_alameda_zcta_advertised_download.csv` — `zcta`, `advertised_download_mbps`, plus the context columns.

### FCC script run order
Run the **Crosswalk** scripts first, then:

1. `00_alameda_2010_to_2020_bridge.py` (whenever the Census relationship file is updated)
2. `01_clean_fcc_blocks.py`
3. `02_map_blocks_to_zcta.py`
4. `03_collapse_block_tech_rows.py`
5. `04_aggregate_zcta_tech.py`
6. `05_alameda_zcta_advertised_download.py`

---

## Census population density (ZCTA)

These steps build Alameda-only ZCTA population and density using Census population counts and ZCTA land area from the **Crosswalk** section (`01_alameda_zcta_land_area.csv`).

**Raw inputs (not tracked in git):**
- `Datasets/03_CENSUS/population_density/csv/raw_ZCTA_population.csv` — Census-style ZCTA population export (`GEO_ID`, `NAME`, `P1_001N` total population).

### `Datasets/03_CENSUS/population_density/scripts/01_alameda_zcta_population.py`
- Reads the raw ZCTA population CSV (skips the duplicate label row used in Census API downloads).
- Keeps rows that are ZCTA geographies (`GEO_ID` prefix `860Z200US`) and restricts to ZCTAs listed in `Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv`.
- Output: `Datasets/03_CENSUS/population_density/csv/01_alameda_zcta_population.csv` — columns `zcta`, `population`.

### `Datasets/03_CENSUS/population_density/scripts/02_alameda_zcta_population_density.py`
- Joins `01_alameda_zcta_population.csv` to `01_alameda_zcta_land_area.csv` on ZCTA.
- Land area is taken from `total_land_area` in **square meters** (Census `AREALAND_PART`). The script converts to square miles with the standard factor **1 mi² = 2,589,988.110336 m²** (international mile) and writes `land_area_sq_mi` plus `population_per_sq_mi` (people per square mile, the usual U.S. convention) and `population_per_sq_km`.
- Leaves density blank if land area is zero or missing.
- Output: `Datasets/03_CENSUS/population_density/csv/02_alameda_zcta_population_density.csv`

### `Datasets/03_CENSUS/population_density/scripts/03_alameda_zcta_density_groups.py`
- Reads `02_alameda_zcta_population_density.csv` and assigns each ZCTA to a **`low`**, **`medium`**, or **`high`** density group using **tertile cutoffs** on `population_per_sq_mi` via `pandas.qcut(df["population_per_sq_mi"], q=3, labels=["low","medium","high"])`.
- **How the labels were created**:
  - Compute the 33.33rd percentile (`q33`) and 66.67th percentile (`q67`) of `population_per_sq_mi` across the 53 Alameda ZCTAs.
  - `low`: `population_per_sq_mi <= q33` (most rural ZCTAs, e.g. 94550, 94586).
  - `medium`: `q33 < population_per_sq_mi <= q67` (suburban / mid-density).
  - `high`: `population_per_sq_mi > q67` (densest urban ZCTAs, e.g. 94606, 94704).
- **Why tertiles, not fixed thresholds**: with only 53 ZCTAs, fixed cutoffs like "1,000 / 5,000 ppl per mi²" produce uneven groups; tertiles guarantee ~17–18 ZCTAs per bucket, which keeps ANOVA sample sizes balanced and the rule data-driven.
- The actual numeric `q33` and `q67` cutoffs are **printed at run time and copied into every row of the output CSV** (`low_cutoff_per_sq_mi`, `high_cutoff_per_sq_mi`) so the rule is recoverable from the file alone. Current values: **`q33 ≈ 3,720.12`** and **`q67 ≈ 8,714.46`** people per sq mi (low: 18 ZCTAs, medium: 17, high: 18).
- Output: `Datasets/03_CENSUS/population_density/csv/03_alameda_zcta_density_groups.csv` — `zcta`, `population_per_sq_mi`, `density_group`, `low_cutoff_per_sq_mi`, `high_cutoff_per_sq_mi`.

### Population density script order
1. Complete **Crosswalk run order** so `01_alameda_zcta_land_area.csv` exists.
2. `01_alameda_zcta_population.py`
3. `02_alameda_zcta_population_density.py`
4. `03_alameda_zcta_density_groups.py`

---

## Census income (ZCTA, ACS 5-year)

Median household income is pulled from the Census **ACS 5-year** API (table `B19013`).

- **Estimate**: `B19013_001E`
- **Margin of error**: `B19013_001M`

### `Datasets/03_CENSUS/income/scripts/00_alameda_zcta_income_acs5.py`
- Downloads income for **all ZCTAs** for a given ACS end year (default **2020**, i.e. 2016–2020 ACS5).
- Filters to Alameda County ZCTAs using `Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv` (column `ZCTA`).
- Output: `Datasets/03_CENSUS/income/csv/01_alameda_zcta_income.csv`
- Optional flags: `--acs-end-year 2020`, `--census-api-key YOUR_KEY`, `--output-csv ...`. Missing Census estimates are written as blank cells (not negative sentinels).

---

## M-Lab pipeline (measured speeds → ZCTA → stats, income join, Spearman)

Run these from the **repository root**. The crosswalk file `Datasets/00_crosswalk/csv/01_alameda_zcta_land_area.csv` must exist first; aggregation and raw filtering use it so only **Alameda ZCTAs** are kept.

### `Datasets/01_MLAB/fetch_mlab_data.py`
- Fetches raw NDT download and upload tests from BigQuery for a bounding box over Alameda County, **month by month** for 2020.
- **How to run**: set `YOUR_GCP_PROJECT_ID` in the script, authenticate (`gcloud auth application-default login`), then `python "Datasets/01_MLAB/fetch_mlab_data.py"`.
- **Output**: `mlab_raw_alameda_2020.csv` in the current working directory by default (large file; often not committed).

### `Datasets/01_MLAB/filter_mlab_month.py`
- Streams the full-year raw CSV and keeps rows whose `test_date` falls in a chosen calendar month (default **December 2020**).
- **How to run**:

```bash
python "Datasets/01_MLAB/filter_mlab_month.py" \
  --input-csv "Datasets/01_MLAB/mlab_raw_alameda_2020.csv" \
  --output-csv "Datasets/01_MLAB/mlab_raw_alameda_2020_12.csv"
```

- **Output**: `Datasets/01_MLAB/mlab_raw_alameda_2020_12.csv` — raw tests for that month only (`zcta`, `test_date`, `test_type`, `speed_mbps`, `latency_ms`).

### `Datasets/01_MLAB/scripts/05_filter_raw_december_to_alameda_zctas.py`
- Restricts the December raw file to ZCTAs listed in the Alameda crosswalk (overwrites the input path by default).
- **How to run**:

```bash
python "Datasets/01_MLAB/scripts/05_filter_raw_december_to_alameda_zctas.py"
```

- **Output**: same path as `--input-csv` (default `mlab_raw_alameda_2020_12.csv`), now Alameda-only rows only.

### `Datasets/01_MLAB/scripts/01_aggregate_mlab_zcta_download.py`
- Aggregates **download** tests to one row per `zcta`: `median_download_mbps`, `mean_download_mbps`, `p75_download_mbps` (75th percentile of within-ZCTA tests), `std_download_mbps`, `var_download_mbps`, `download_test_count`, plus median/mean download latency and `mlab_year`/`mlab_month` labels. Filters to Alameda ZCTAs via `--alameda-zcta-csv` (default crosswalk).
- **How to run**:

```bash
python "Datasets/01_MLAB/scripts/01_aggregate_mlab_zcta_download.py" \
  --input-csv "Datasets/01_MLAB/mlab_raw_alameda_2020_12.csv" \
  --output-csv "Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv"
```

- **Output**: `Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv`.

### `Datasets/01_MLAB/scripts/06_aggregate_mlab_zcta_upload.py`
- Same pattern for **upload** tests (median/mean/std upload Mbps, test count, upload latency summaries).
- **How to run**: `python "Datasets/01_MLAB/scripts/06_aggregate_mlab_zcta_upload.py"`.
- **Output**: `Datasets/01_MLAB/csvs/06_alameda_zcta_mlab_2020_12_upload_metrics.csv`.

### `Datasets/01_MLAB/scripts/02_descriptive_stats_download_mbps_by_zcta.py`
- Summarizes the **distribution of ZCTA-level** `median_download_mbps` (mean, median, std, quartiles, IQR, min/max) and writes an empirical CDF table + plot.
- **How to run**:

```bash
python "Datasets/01_MLAB/scripts/02_descriptive_stats_download_mbps_by_zcta.py" \
  --input-csv "Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv"
```

- **Outputs**:
  - `Datasets/01_MLAB/csvs/02_alameda_zcta_mlab_2020_12_download_descriptive_summary.csv` — one row of summary statistics.
  - `Datasets/01_MLAB/csvs/02_alameda_zcta_mlab_2020_12_download_ecdf.csv` — ECDF points.
  - `Datasets/01_MLAB/plots/02_alameda_zcta_mlab_2020_12_download_cdf.png` — CDF figure.

### `Datasets/01_MLAB/scripts/03_join_mlab_with_income.py`
- Inner-joins ZCTA download metrics to `01_alameda_zcta_income.csv` on `zcta`; drops rows with missing `median_household_income`.
- **How to run**: `python "Datasets/01_MLAB/scripts/03_join_mlab_with_income.py"`.
- **Output**: `Datasets/01_MLAB/csvs/03_alameda_zcta_mlab_2020_12_with_income.csv` — merged table for correlation and plots.

### `Datasets/01_MLAB/scripts/04_spearman_income_vs_speed.py`
- **Spearman** correlation between `median_household_income` and `median_download_mbps` on the merged file; scatter plot with a linear trend line (visual only).
- **How to run**: `python "Datasets/01_MLAB/scripts/04_spearman_income_vs_speed.py"`. Add `--log-x` for log-scaled income on the plot.
- **Outputs**:
  - `Datasets/01_MLAB/csvs/04_spearman_income_vs_download_results.csv` — `n`, `spearman_rho`, `p_value`.
  - `Datasets/01_MLAB/plots/04_income_vs_median_download_scatter.png`.

### `Datasets/01_MLAB/scripts/07_spearman_income_metric_suite.py`
- Runs Spearman for income vs several ZCTA metrics at once (median/mean download and upload, download/upload latency). Requires download + upload metric CSVs.
- **How to run**: `python "Datasets/01_MLAB/scripts/07_spearman_income_metric_suite.py"`.
- **Output**: `Datasets/01_MLAB/csvs/07_spearman_income_metric_suite.csv` — one row per metric.

### `Datasets/01_MLAB/scripts/00_audit_zcta_joins.py`
- Prints coverage diagnostics: ZCTAs in M-Lab vs income vs crosswalk allowlist, and missing-income ZCTAs. Does not write files.
- **How to run**: `python "Datasets/01_MLAB/scripts/00_audit_zcta_joins.py"`.

### Suggested M-Lab order (December slice, Alameda-only)
1. (Optional) `fetch_mlab_data.py` → full-year raw.
2. `filter_mlab_month.py` → December raw.
3. `05_filter_raw_december_to_alameda_zctas.py` → Alameda-only December raw.
4. `01_aggregate_mlab_zcta_download.py` → ZCTA download metrics.
5. (Optional) `06_aggregate_mlab_zcta_upload.py` → ZCTA upload metrics.
6. `02_descriptive_stats_download_mbps_by_zcta.py` → summary + ECDF + plot.
7. `03_join_mlab_with_income.py` → merged CSV.
8. `04_spearman_income_vs_speed.py` and/or `07_spearman_income_metric_suite.py`.
9. `00_audit_zcta_joins.py` anytime to verify ZCTA alignment.

---

## Analysis (urbanicity, advertised vs measured)

Inputs each script consumes:
- `Datasets/03_CENSUS/population_density/csv/03_alameda_zcta_density_groups.csv` (low/medium/high tertile labels — the rule is documented under **Census population density**).
- `Datasets/02_FCC/csv/05_alameda_zcta_advertised_download.csv` (per-ZCTA advertised speed via Option A — see the FCC pipeline section).
- `Datasets/01_MLAB/csvs/01_alameda_zcta_mlab_2020_12_download_metrics.csv` (per-ZCTA measured download).

### `analysis/scripts/01_anova_density_vs_measured_download.py`
- Inner-joins density groups to M-Lab metrics on `zcta`, drops ZCTAs with missing `median_download_mbps`, and prints how many were dropped.
- Uses the same `low` / `medium` / `high` labels written by `03_alameda_zcta_density_groups.py` (tertile rule documented in the population-density section).
- Runs `scipy.stats.f_oneway` on `median_download_mbps` across the three groups (with a manual SSB/SSW fallback if SciPy is unavailable, mirroring the M-Lab Spearman script).
- The boxplot overlays each ZCTA as a single dot on its group. Each dot gets a small random horizontal offset so dots that share the same value do not stack on top of each other. This means the reader can see every ZCTA in the group, not just the median and quartiles drawn by the box.
- Outputs:
  - `analysis/csv/01_anova_density_vs_measured_download.csv` — per-group rows (`density_group`, `n`, `mean_mbps`, `median_mbps`, `std_mbps`) plus a final row with `f_statistic`, `p_value`, total `n`.
  - `analysis/plots/01_density_group_vs_measured_download_box.png` — boxplot with one dot per ZCTA overlaid (small random horizontal offset so dots do not stack) and F/p in the title.

### `analysis/scripts/02_performance_ratio.py`
- Inner-joins FCC advertised (`05_alameda_zcta_advertised_download.csv`) to M-Lab download metrics on `zcta`.
- Computes two ratios per ZCTA, both with `np.where(advertised > 0, ..., np.nan)` so a 0/missing FCC value never crashes the script. Skipped rows get `ratio_skip_reason = "advertised_zero_or_missing"` for easy auditing.
  - `performance_ratio = median_download_mbps / advertised_download_mbps` (primary; "typical resident").
  - `performance_ratio_p75 = p75_download_mbps / advertised_download_mbps` (faster end of the within-ZCTA distribution). Only written if the M-Lab input has a `p75_download_mbps` column.
- How to read the ratio: **~1.0** = ISPs deliver what they advertised; **< 0.5** = "Major Performance Gap" (measured under half of advertised); **> 1.0** = measured beats advertised (rare, usually over-provisioning).
- Outputs:
  - `analysis/csv/02_alameda_zcta_performance_ratio.csv` — `zcta`, `median_download_mbps`, `p75_download_mbps`, `advertised_download_mbps`, `performance_ratio`, `performance_ratio_p75`, `ratio_skip_reason`, `download_test_count`, `block_count`.
  - `analysis/csv/02_alameda_performance_ratio_summary.csv` — median-ratio stats (`n`, `mean_ratio`, `median_ratio`, `std_ratio`, `min_ratio`, `max_ratio`, `p25_ratio`, `p75_ratio`, `n_below_0_5`, `n_above_1_0`) plus matching `*_p75` columns when the P75 ratio is computed.
  - `analysis/plots/02_performance_ratio_hist.png` — histogram with vertical lines at **0.5** and **1.0**.
  - `analysis/plots/02_advertised_vs_measured_scatter.png` — scatter with the **`y = x` reference line** (delivered as advertised) and a **light-red shaded region below `y = 0.5x`** flagging the Major Performance Gap zone.

### `analysis/scripts/03_spearman_income_vs_performance_ratio.py`
- Tests the "broadband truth gap" hypothesis: do lower-income ZCTAs receive a smaller share of the speed ISPs advertised?
- Inner-joins per-ZCTA performance ratios (`analysis/csv/02_alameda_zcta_performance_ratio.csv`) to Census ACS5 income (`Datasets/03_CENSUS/income/csv/01_alameda_zcta_income.csv`) on `zcta` and drops rows missing either `median_household_income` or the ratio.
- Computes Spearman ρ + p-value between `median_household_income` and **both** ratio variants when available:
  - `performance_ratio` (median measured / advertised) — primary.
  - `performance_ratio_p75` (P75 measured / advertised) — matches the abstract's "P75 measured / median advertised" framing.
- Uses `scipy.stats.spearmanr` with a manual rank-correlation fallback if SciPy is missing (same pattern as the M-Lab Spearman script).
- **Interpretation**: ρ > 0 with a small p-value supports the hypothesis (higher-income ZCTAs receive a higher share of advertised speed; lower-income ZCTAs see a larger truth gap). ρ near 0 means no monotonic relationship.
- Outputs:
  - `analysis/csv/03_spearman_income_vs_performance_ratio_results.csv` — one row per ratio variant with `ratio_col`, `ratio_label`, `n`, `spearman_rho`, `p_value`.
  - `analysis/plots/03_income_vs_performance_ratio_scatter.png` — income vs median-based ratio, with horizontal reference lines at **1.0** (delivered as advertised) and **0.5** (major performance gap) and a linear fit for visual guidance.
  - `analysis/plots/03_income_vs_performance_ratio_p75_scatter.png` — same plot for the P75-based ratio (only written when `performance_ratio_p75` exists in the input).

### `analysis/scripts/04_density_vs_performance_ratio.py`
- Tests the **density half** of the "broadband truth gap" hypothesis: do lower-density ZCTAs receive a smaller share of the speed ISPs advertised?
- Inner-joins per-ZCTA density (`Datasets/03_CENSUS/population_density/csv/03_alameda_zcta_density_groups.csv` — provides `population_per_sq_mi` and the tertile `density_group` from `03_alameda_zcta_density_groups.py`) to the per-ZCTA performance ratios (`analysis/csv/02_alameda_zcta_performance_ratio.csv`).
- For each available ratio column (`performance_ratio` and, when present, `performance_ratio_p75`) it runs:
  - **Spearman rank correlation** between `population_per_sq_mi` and the ratio.
  - **One-way ANOVA** comparing the ratio across the three tertile groups (`low` / `medium` / `high`), mirroring `01_anova_density_vs_measured_download.py` so the level-based and gap-based ANOVAs share a cut.
- Uses `scipy.stats.spearmanr` / `scipy.stats.f_oneway` with manual rank-correlation and SSB/SSW fallbacks when SciPy is missing (same pattern as the other analysis scripts).
- **Interpretation**: Spearman ρ > 0 with a small p-value supports the hypothesis (denser ZCTAs receive a higher share of advertised speed; lower-density ZCTAs face a larger gap). ρ near 0 means no monotonic density effect on the gap. The ANOVA is a complementary group-wise check using the same tertile cut as `03_alameda_zcta_density_groups.py`.
- Outputs:
  - `analysis/csv/04_density_vs_performance_ratio_spearman.csv` — one row per ratio variant with `ratio_col`, `ratio_label`, `n`, `spearman_rho`, `p_value`.
  - `analysis/csv/04_density_vs_performance_ratio_anova.csv` — per-group rows (`density_group`, `n`, `mean_ratio`, `median_ratio`, `std_ratio`) plus one ANOVA row per ratio variant (`f_statistic`, `p_value`).
  - `analysis/plots/04_density_vs_performance_ratio_scatter.png` — log-x scatter of density vs the median-based ratio with horizontal reference lines at **0.5** and **1.0**.
  - `analysis/plots/04_density_vs_performance_ratio_p75_scatter.png` — same scatter for the P75-based ratio (only written when `performance_ratio_p75` exists).
  - `analysis/plots/04_density_group_vs_performance_ratio_box.png` — boxplot of the median-based ratio by density tertile, with one dot per ZCTA overlaid (small random horizontal offset so dots that share a value do not stack) and the ANOVA F/p in the title.

### Analysis run order
1. Run **Census population density** through step 4 (`03_alameda_zcta_density_groups.py`).
2. Run the **FCC pipeline** through step 6 (`05_alameda_zcta_advertised_download.py`).
3. Run **M-Lab** through `01_aggregate_mlab_zcta_download.py` so `01_alameda_zcta_mlab_2020_12_download_metrics.csv` exists.
4. Run **Census income** (`00_alameda_zcta_income_acs5.py`) so `01_alameda_zcta_income.csv` exists.
5. `01_anova_density_vs_measured_download.py`
6. `02_performance_ratio.py`
7. `03_spearman_income_vs_performance_ratio.py`
8. `04_density_vs_performance_ratio.py`


