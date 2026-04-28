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

