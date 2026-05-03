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

### FCC script run order
Run the **Crosswalk** scripts first, then:

1. `00_alameda_2010_to_2020_bridge.py` (whenever the Census relationship file is updated)
2. `01_clean_fcc_blocks.py`
3. `02_map_blocks_to_zcta.py`
4. `03_collapse_block_tech_rows.py`
5. `04_aggregate_zcta_tech.py`

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

### Population density script order
1. Complete **Crosswalk run order** so `01_alameda_zcta_land_area.csv` exists.
2. `01_alameda_zcta_population.py`
3. `02_alameda_zcta_population_density.py`

