# HMDA Mortgage Lending Fairness Pipeline
### Detecting Racial and Geographic Disparities in U.S. Mortgage Lending (2023–2024)

**Course:** SEIS 745 — Data Lakes & Advanced Analytics  
**Team:** Rohith Ambarish & Manoj Kumar Ravi Kumar  
**Institution:** University of St. Thomas

---

## Project Overview

This project builds an end-to-end data engineering pipeline using Apache Spark on Databricks to analyze 11.7 million mortgage loan applications from the Home Mortgage Disclosure Act (HMDA) dataset. The pipeline detects racial, ethnic, and geographic disparities in U.S. mortgage lending decisions across 10 states for the years 2023 and 2024.

The analysis directly addresses Community Reinvestment Act (CRA) compliance risks and provides regulators, researchers, and community advocates with data-driven evidence of lending disparities.

---

## Dataset

| Property | Details |
|---|---|
| Source | FFIEC HMDA Data Browser |
| Years | 2023 & 2024 |
| Coverage | CA, TX, FL, NY, PA, IL, OH, GA, NC, MN |
| Raw Size | ~4.4 GB (20 CSV files) |
| Total Records | 11,730,625 loan applications |
| Fields | 99 columns per record |
| Access | [FFIEC HMDA Data Browser](https://ffiec.cfpb.gov/data-browser/) |

---

## Architecture

```
FFIEC API
    ↓
Python Collection Script (requests + boto3)
    ↓
AWS S3 (Raw Data Lake)
    ↓
Databricks Volume
    ↓
Apache Spark — Ingestion → Cleaning → Enrichment
    ↓
Delta Lake (Bronze → Silver → Gold)
    ↓
Spark SQL Analysis (5 Research Questions)
    ↓
Matplotlib Visualizations (7 Charts)
```

### Medallion Architecture

| Layer | Table | Records | Description |
|---|---|---|---|
| Bronze | `raw_hmda` | 11,730,625 | Raw ingested data |
| Silver | `cleaned_hmda` | 8,007,205 | Typed, filtered, no outliers |
| Gold | `enriched_hmda` | 8,007,205 | + derived features for analysis |

---

## Tech Stack

| Component | Technology |
|---|---|
| Distributed Processing | Apache Spark (PySpark) on Databricks |
| Storage | AWS S3 + Databricks Volumes |
| Table Format | Delta Lake (ACID transactions + time travel) |
| File Formats | Raw CSV → Parquet/Delta |
| Data Collection | Python `requests` + `boto3` |
| SQL Layer | Spark SQL |
| Visualization | matplotlib |
| Version Control | GitHub |
| Cloud | AWS (S3, EC2, Cloud9) |

---

## Repository Structure

```
hmda-datalake-pipeline/
├── hmda_download.py           # Step 1: API collection script
├── s3_to_databricks.py        # Step 2: S3 → Databricks transfer
├── notebooks/
│   ├── 01_Ingestion.ipynb     # Schema definition + Spark ingestion + Delta save
│   ├── 02_Cleaning.ipynb      # Type casting + null handling + outlier removal
│   ├── 03_Analysis.ipynb      # 5 research questions answered with Spark SQL
│   └── 04_Visualization.ipynb # 7 matplotlib charts
├── charts/
│   ├── chart1_denial_heatmap.png
│   ├── chart2_denial_gap.png
│   ├── chart3_rate_spread.png
│   ├── chart4_lender_fairness.png
│   ├── chart5_executive_dashboard.png
│   ├── chart6_credit_deserts.png
│   └── chart7_fed_rate_cycle.png
└── README.md
```

---

## Pipeline Steps

### Step 1 — Data Collection
Python script hits the FFIEC HMDA API with retry logic, exponential backoff, and resume support. Streams data directly to AWS S3 via multipart upload — zero local disk usage.

```bash
python3 hmda_download.py
```

### Step 2 — Ingestion (01_Ingestion.ipynb)
Reads all 20 CSV files into a single Spark DataFrame using an explicit 99-field schema. Saves as Delta table (Bronze layer).

**Spark operations:** `spark.read`, explicit `StructType` schema, `.write.format("delta")`

### Step 3 — Cleaning (02_Cleaning.ipynb)
- Casts string columns to numeric using `try_cast` (handles "NA" and "Exempt" values safely)
- Filters to relevant action codes (originated, approved, denied)
- Removes outliers (interest rates outside 1-20%, LTV outside 0-200%)
- Derives `is_denied`, `income_bracket`, `race_group`, `is_lmi_tract`, `is_minority` columns

**Spark operations:** `withColumn`, `expr("try_cast")`, `filter`, `when/otherwise`

### Step 4 — Enrichment (02_Cleaning.ipynb)
Derives neighborhood-level context from embedded FFIEC Census supplement fields:
- LMI tract classification (`tract_to_msa_income_percentage < 80`)
- Minority tract flag (`tract_minority_population_percent >= 50`)
- Simplified race groups for analysis

### Step 5 — Analysis (03_Analysis.ipynb)
Five research questions answered using Spark SQL with CTEs, window functions, and aggregations.

### Step 6 — Visualization (04_Visualization.ipynb)
Seven matplotlib charts saved to Databricks Volume and GitHub.

---

## Research Questions & Key Findings

### Q1 — Do racial denial rate disparities exist, and how large are they?

**Answer: Yes — the gap persists across every income bracket.**

| Race | High Income Denial Rate | vs White | Ratio |
|---|---|---|---|
| Native American | 30.16% | +14.93 pts | 1.98x |
| Black | 29.02% | +13.79 pts | 1.91x |
| Hispanic | 28.88% | +13.65 pts | 1.90x |
| White | 15.23% | — | 1.00x |
| Asian | 14.11% | -1.12 pts | 0.93x |

A high-income Black applicant is denied at nearly **twice the rate** of a high-income White applicant.

---

### Q2 — Are minority borrowers charged higher interest rate spreads?

**Answer: Yes — Hispanic borrowers pay $25,921 more over a 30-year mortgage.**

| Race | Rate Spread Gap vs White | Extra Cost (30yr) |
|---|---|---|
| Hispanic | +0.25 pts | **+$25,921** |
| Native American | +0.19 pts | **+$21,612** |
| Black | +0.05 pts | **+$14,519** |
| Asian | -0.28 pts | -$50,622 (pays less) |

---

### Q3 — Which lenders best and worst serve minority communities?

**Answer: Major banks show alarming denial gaps.**

| Lender | Minority Denial Rate | White Denial Rate | Gap |
|---|---|---|---|
| Citibank | 42.42% | 17.30% | +25.12 pts 🚨 |
| M&T Bank | 53.62% | 29.07% | +24.55 pts 🚨 |
| PNC Bank | 55.49% | 32.06% | +23.43 pts 🚨 |
| Kiavi Funding | 0% minority apps | — | Complete exclusion 🚨 |
| Paramount Residential | 50.1% minority apps | 1.71 pt gap | Best performer ✅ |

---

### Q4 — Which census tracts represent mortgage credit deserts?

**Answer: 23,794 LMI tracts analyzed. FL-12086 has a 98.3 desert score with 100% denial rate.**

| State | County | Denial Rate | Minority % | Desert Score |
|---|---|---|---|---|
| FL | 12086 | 100% | 95.28% | 98.3 🚨 |
| TX | 48201 | 92.86% | 84.81% | 89.2 🚨 |
| IL | 17163 | 90% | 99.67% | 88.0 🚨 |
| NC | 37195 | 90% | 89.72% | 86.0 🚨 |

County with worst LMI denial rate: **NC-37139 at 71.2%**

---

### Q5 — Did the Fed's 2023–2024 rate cycle affect all demographic groups equally?

**Answer: Minority groups improved more in denial rates but the absolute gap remains large.**

| Race | Denial Rate Change | Application Growth | Origination Rate Change |
|---|---|---|---|
| Native American | -4.54 pts ✅ | +1.63% | +3.49 pts |
| Hispanic | -4.19 pts ✅ | +10.89% | +3.76 pts |
| Black | -1.87 pts ✅ | +3.07% | +1.35 pts |
| White | -1.25 pts | +4.77% | +0.97 pts |

Despite Fed rate cuts, mortgage interest rates actually **increased** in 2024 for all groups — because mortgage rates track 10-year Treasury bonds, not the Fed funds rate. The absolute denial gap between minority and white applicants remains 13-14 percentage points.

---

## Big Data Characteristics

**Volume:** 11.7M loan-level records, 99 fields, ~4.4GB — requires distributed processing via Apache Spark.

**Variety:** 99 fields spanning numeric, categorical, geographic (FIPS/census tract), date, and multi-table join structures requiring diverse preparation techniques.

---

## How to Run

### Prerequisites
- Databricks account (Free Edition or above)
- AWS account with S3 access
- Python 3.9+

### Setup

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/hmda-datalake-pipeline.git
cd hmda-datalake-pipeline

# Install dependencies
pip install requests boto3 databricks-cli

# Configure AWS credentials
aws configure

# Configure Databricks CLI
databricks configure --token
```

### Data Collection

```bash
# Download HMDA data from FFIEC API → S3
python3 hmda_download.py

# Transfer from S3 → Databricks Volume
python3 s3_to_databricks.py
```

### Analysis
Open notebooks in order in Databricks:
1. `01_Ingestion.ipynb`
2. `02_Cleaning.ipynb`
3. `03_Analysis.ipynb`
4. `04_Visualization.ipynb`

---

## Visualizations

| Chart | Description |
|---|---|
| Chart 1 | Denial rate heatmap by race and income bracket |
| Chart 2 | Denial rate gap vs White across income brackets |
| Chart 3 | Rate spread disparity + 30-year dollar impact |
| Chart 4 | Lender fairness scorecard (top 50 lenders) |
| Chart 5 | Executive summary dashboard |
| Chart 6 | Mortgage credit deserts — census tract analysis |
| Chart 7 | Fed rate cycle impact across demographic groups |

---

## Policy Implications

The findings of this analysis are directly relevant to:

- **CRA Compliance** — Citibank, PNC, M&T Bank, and US Bank show denial gaps of 20+ points, warranting regulatory scrutiny
- **Fair Lending** — Consistent rate spread overcharges to Hispanic and Black borrowers constitute potential ECOA violations
- **Credit Deserts** — FL-12086 and TX-48201 census tracts show near-100% denial rates for LMI minority applicants
- **Rate Policy** — Fed rate cuts disproportionately benefited minority applicants in denial rate improvement, but the absolute gap remains unacceptably large

---

## Data Sources

- [FFIEC HMDA Data Browser](https://ffiec.cfpb.gov/data-browser/) — Loan Application Register (LAR) 2023 & 2024
- [GLEIF API](https://api.gleif.org) — Legal Entity Identifier (LEI) to institution name mapping
- FFIEC Census Supplement — embedded in HMDA LAR dataset

---

*This project was completed as part of SEIS 745 — Data Lakes & Advanced Analytics at the University of St. Thomas.*
