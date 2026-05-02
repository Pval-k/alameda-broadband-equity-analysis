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
- Population density (used to define urbanicity)

---

## Methodology

### Data Processing
- All datasets filtered to year **2020**
- Data aggregated at **ZCTA level**
- Datasets merged using geographic alignment across M-Lab, FCC, and Census data

---

## FCC Pipeline Scripts

The FCC workflow is split into small scripts so each step is easy to verify.

**Raw inputs (not tracked in git):** 
- `FCC/csv/raw_CA_FCC_fixed_Dec2020.csv` — statewide FCC fixed broadband extract (Dec 2020)
- `FCC/crosswalk/csv/raw_block_to_zcta.txt` — national Census block-to-ZCTA relationship file 

### `FCC/crosswalk/scripts/00_alameda_block_to_zcta.py`
- Builds an Alameda-only block-to-ZCTA crosswalk from the national relationship file.
- Keeps key columns only (`GEOID`, `ZCTA`, `LAND_AREA`).
- If one `GEOID` appears with multiple `ZCTA` values, it keeps the row with the largest `LAND_AREA` and drops the rest.
- Output: `FCC/crosswalk/csv/00_alameda_block_to_zcta_cleaned.csv`

### `FCC/scripts/01_clean_fcc_blocks.py`
- Reads raw FCC fixed broadband data for Dec 2020 (`FCC/csv/raw_CA_FCC_fixed_Dec2020.csv` by default).
- Filters to Alameda County blocks (`BlockCode` starts with `06001`) and residential records.
- Maps `TechCode` to readable `TechCategory`.
- Produces one row per `BlockCode + TechCategory` by keeping the maximum advertised download/upload (`max_ad_down`, `max_ad_up`) within that group.
- Output: `FCC/csv/01_FCC_alameda_2020_block_level.csv`

### `FCC/scripts/02_map_blocks_to_zcta.py`
- Joins FCC block-level rows to the Alameda block-to-ZCTA crosswalk.
- If the same block appears multiple times in the crosswalk with different ZCTAs, it keeps the row with the largest `LAND_AREA` and drops the others.
- Writes mapped rows so each FCC row has a `zcta`.
- Output: `FCC/csv/02_FCC_alameda_2020_block_zcta_mapped.csv`

### `FCC/scripts/03_collapse_block_tech_rows.py`
- Collapses mapped rows to one row per `zcta + BlockCode + TechCategory`.
- Script 02 can contain duplicate keys (same block + tech repeated); this step deduplicates so medians in script 04 are not biased.
- If duplicate rows exist within that key, it keeps the maximum `max_ad_down` and `max_ad_up` for the key.
- Writes `BlockCode` and `zcta` as zero-padded text so CSV tools do not drop leading zeros.
- Output: `FCC/csv/03_FCC_alameda_2020_block_tech_collapsed.csv`

### `FCC/scripts/04_aggregate_zcta_tech.py`
- Aggregates collapsed rows to final `zcta + TechCategory` metrics.
- For each `zcta + TechCategory`, it summarizes the distribution of block-level max speeds (median, p75, and max) instead of keeping only a single block.
- Calculates median, p75, and max for advertised download/upload speeds.
- Output: `FCC/csv/04_FCC_alameda_2020_zcta_tech_metrics.csv`

### Script Run Order
1. `00_alameda_block_to_zcta.py`
2. `01_clean_fcc_blocks.py`
3. `02_map_blocks_to_zcta.py`
4. `03_collapse_block_tech_rows.py`
5. `04_aggregate_zcta_tech.py`

