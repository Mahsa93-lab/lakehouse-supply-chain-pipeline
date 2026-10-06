# Lakehouse Supply Chain Pipeline (Databricks · PySpark · Delta Lake)

**English** · [Deutsch](README.de.md)

> A daily, tested and documented pipeline from raw files and an external API to business-ready KPI tables – reconciled to the cent with a second, independent implementation.

![Architecture](docs/architecture.png)

## Business problem
Reports are only as good as the pipeline behind them. When data comes from several sources with different quality, teams need a process that cleans, converts and checks the data automatically every day – and stops before wrong numbers reach a report. This project rebuilds the data model of [project 1](https://github.com/Mahsa93-lab/supplier-delivery-performance-powerbi) (SQL Server + Power BI) as a lakehouse pipeline, adds EUR conversion with official ECB rates and keeps a history of every data-quality run.

## Key results
1. **Two implementations, identical numbers:** SQL Server (project 1) and Databricks produce the same 99,441 orders, BRL 13,591,643.70 revenue, 6,535 late orders, 10,423 late handovers and 4,542 data-quality findings. A quality gate with 17 tests fails the job if any number drifts.
2. **Currency hides 37 percentage points of growth:** revenue Jan–Aug 2018 vs. 2017 grew **+137 % in BRL but only +100 % in EUR** – the Brazilian real weakened from 3.51 to 4.24 BRL per EUR (average). A European management team would read the BRL figure wrongly.
3. **Re-runs are safe:** exchange rates are loaded incrementally with `MERGE`; a second run adds nothing, data-quality results are appended as history.

## Data
- [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle, CC BY-NC-SA 4.0) – 7 CSV files in a Unity Catalog volume
- [ECB Data Portal](https://data.ecb.europa.eu/) – daily BRL/EUR reference rates, series `EXR.D.BRL.EUR.SP00.A`. Databricks Free Edition restricts outbound internet, so the notebook falls back to `data/ecb_brl_eur_2016_2018.csv`, created with `python src/ecb_ingest.py`

## Architecture (medallion)
| Layer | Tables | What happens |
|---|---|---|
| Bronze | `bronze_*` (8) | raw copy, all columns as text, `_ingested_at`, `_source_file`; FX rates via incremental `MERGE` |
| Silver | `silver_*` (7) | typed with `try_cast` (no failed jobs on bad values), deduplicated, worst review per order, EUR with the ECB rate of the purchase day |
| Gold | `gold_fact_orders`, `gold_fact_order_items`, `gold_weekly_kpis`, `gold_supplier_scorecard` | two fact grains as in project 1; weekly and monthly KPI tables for Power BI and the AI agent (project 4) |
| Quality | `dq_results`, `04_validate` | 13 checks appended per run (DQ score 99.62 %); reconciliation gate with 17 tests |

## Engineering practices
- **Logic in tested functions, notebooks only orchestrate:** `src/transforms.py` and `src/dq_checks.py` are plain PySpark functions (DataFrame in, DataFrame out)
- **16 unit tests** (`pytest`) pin every business rule on tiny hand-made data – e.g. "weekend purchases use Friday's exchange rate", "orders without items stay in the order fact", "processing time is NULL if the carrier pickup is before approval"
- **CI with GitHub Actions:** `ruff` + `pytest` on PySpark 4.0 for every push
- **Lakeflow Job** with four tasks on serverless compute, scheduled daily
- **Governance:** table and column comments in Unity Catalog, lineage visible in Catalog Explorer

## Results
| Metric | Value |
|---|---|
| Rows read per run | 446,873 (7 files) + 767 ECB days |
| Tables written | 8 bronze · 7 silver · 4 gold · 1 DQ history |
| Data-quality checks per run | 13 (4,542 findings, DQ score 99.62 %) |
| Quality gate | 17 / 17 PASS |
| Unit tests | 16 / 16 PASS |
| Runtime of the daily job | 3 min 47 s on serverless (bronze 1:33 · silver 1:00 · gold 0:44 · validate 0:26) |

## Screenshots
| Daily job: four tasks, all green | Unity Catalog lineage |
|---|---|
| ![Job run](images/01_job_run.png) | ![Lineage](images/02_lineage.png) |

Table and column comments in Catalog Explorer: [images/03_table_docs.png](images/03_table_docs.png)

## How to run
1. Create a free account: [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
2. Workspace → **Create → Git folder** → clone this repository
3. Run `notebooks/01_bronze` once (it creates the schema and the volume) → upload the 7 Olist CSVs to `/Volumes/workspace/olist/raw` → run it again
4. Run `02_silver` → `03_gold` → `04_validate` (or create a job with four tasks)
5. Local tests (Linux/macOS or CI; needs Java 17): `pip install -r requirements.txt pyspark==4.0.*` → `pytest`

Expected numbers for every step: [docs/expected-results.md](docs/expected-results.md)

## Project structure
```
src/          ecb_ingest.py (API + CLI) · transforms.py (bronze → silver → gold) · dq_checks.py (13 checks)
notebooks/    01_bronze · 02_silver · 03_gold · 04_validate (Databricks source format)
tests/        pytest: ECB parser, DQ checks, transformation rules
data/         ecb_brl_eur_2016_2018.csv (767 ECB business days)
docs/         architecture, expected results
images/       Databricks screenshots (job run, lineage, catalog)
.github/      CI workflow
```

## What I would do next
- Orders as an incremental `MERGE` source instead of full overwrite (change data feed)
- Alert when a DQ check gets worse than the last run – the KPI agent in project 4 reads `dq_results`
- Deploy the job as code with Databricks Asset Bundles

---
*Author: Mahsa Ahmadi · Public data only; no company-internal data was used.*
