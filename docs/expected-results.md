# Expected results (test reference)

Computed from the original Olist CSVs and the ECB reference rates with exactly the code in `src/`
(PySpark 4.0, ANSI mode). Your numbers in Databricks must match. `notebooks/04_validate` checks the
most important ones automatically.

## Step 0 – ECB rates (`python src/ecb_ingest.py`)

| Check | Expected |
|---|---:|
| ECB business days 2016-01-01 … 2018-12-31 | 767 |
| First / last line | 2016-01-04 · 4.4023 / 2018-12-31 · 4.444 |
| Rate on Fri 2018-01-05 / Mon 2018-01-08 | 3.9057 / 3.8825 |

## Step 1 – Bronze (`01_bronze`)

| Table | Rows |
|---|---:|
| bronze_orders | 99,441 |
| bronze_order_items | 112,650 |
| bronze_customers | 99,441 |
| bronze_sellers | 3,095 |
| bronze_products | 32,951 |
| bronze_order_reviews | 99,224 |
| bronze_category_translation | 71 |
| bronze_fx_rates (first run) | 767 days, `_source` = "ECB API" or "repo CSV" |
| bronze_fx_rates (second run) | still 767 – "source: up to date · new days offered: 0" |

## Step 2 – Silver (`02_silver`)

| Table | Rows |
|---|---:|
| silver_fx_daily | 1,093 (326 of them weekend/holiday days, forward-filled) |
| silver_orders | 99,441 |
| silver_order_items | 112,650 (0 without EUR price) |
| silver_order_reviews | 98,673 (one row per reviewed order) |
| silver_products | 32,951 |
| silver_customers | 99,441 |
| silver_sellers | 3,095 |

Data-quality checks (identical to project 1, `dq.v_checks`):

| # | Check | Failed rows | Total rows |
|---:|---|---:|---:|
| 1 | Delivered order without delivery date | 8 | 99,441 |
| 2 | Order without any item | 775 | 99,441 |
| 3 | Order without review | 768 | 99,441 |
| 4 | Purchase timestamp not convertible | 0 | 99,441 |
| 5 | Handed to carrier before approval | 1,359 | 99,441 |
| 6 | Handed to carrier before purchase | 166 | 99,441 |
| 7 | Delivered to customer before carrier pickup | 23 | 99,441 |
| 8 | Not delivered, but delivery date set | 6 | 99,441 |
| 9 | Price missing or <= 0 | 0 | 112,650 |
| 10 | Duplicate review_id rows | 814 | 99,224 |
| 11 | Product without category | 610 | 32,951 |
| 12 | Category missing in translation file | 13 | 32,951 |
| 13 | Item with unknown seller | 0 | 112,650 |

Total: 4,542 failed of 1,185,954 checked rows → **DQ score 99.62 %**. Every run appends 13 rows to `dq_results`.

## Step 3 – Gold (`03_gold`)

| Table | Rows |
|---|---:|
| gold_fact_orders | 99,441 |
| gold_fact_order_items | 112,650 |
| gold_weekly_kpis | 101 (87 in the analysis period) |
| gold_supplier_scorecard | 16,441 |

| Figure | Value |
|---|---:|
| Revenue BRL (all) | 13,591,643.70 (= project 1) |
| Revenue EUR (all) | 3,450,018.83 |
| Revenue EUR, analysis period 2017-01 … 2018-08 | 3,436,194.14 |
| Revenue Jan–Aug 2017 → Jan–Aug 2018, BRL | 3,113,000.32 → 7,385,905.80 (**+137.3 %**) |
| Revenue Jan–Aug 2017 → Jan–Aug 2018, EUR | 878,398.11 → 1,758,375.95 (**+100.2 %**) |
| Average ECB rate Jan–Aug 2017 / Jan–Aug 2018 | 3.51 / 4.24 BRL per EUR |
| Week with most revenue | 2017-11-20 (Black Friday): 3,008 orders · 106,137.78 EUR · customer OTD 82.7 % |
| Week with lowest customer OTD | 2018-02-26: 1,903 orders · customer OTD 73.7 % · 487 late orders |

## Step 4 – Quality gate (`04_validate`)

All 17 tests **PASS**: row counts, revenue BRL/EUR, canceled orders 1,234, late orders 6,535,
late handovers 10,423, customer OTD 93.23 %, supplier OTD 90.65 %, 13 DQ checks with 4,542 failed rows,
101 weekly rows.

## Unit tests (`pytest`)

16 tests pass (3 ECB parser, 5 data-quality checks, 8 transformation rules).
