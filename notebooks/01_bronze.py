# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze – raw data, unchanged
# MAGIC * 7 Olist CSV files from a Unity Catalog volume → one Delta table each, every column as text
# MAGIC * ECB daily BRL/EUR reference rates → `bronze_fx_rates`, loaded **incrementally with MERGE** (re-runs never create duplicates)
# MAGIC
# MAGIC Every row gets `_ingested_at` and `_source_file` so each value can be traced back to its file.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "olist")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"


def T(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

# COMMAND ----------

from pyspark.sql import functions as F

FILES = {
    "olist_orders_dataset.csv": "bronze_orders",
    "olist_order_items_dataset.csv": "bronze_order_items",
    "olist_customers_dataset.csv": "bronze_customers",
    "olist_sellers_dataset.csv": "bronze_sellers",
    "olist_products_dataset.csv": "bronze_products",
    "olist_order_reviews_dataset.csv": "bronze_order_reviews",
    "product_category_name_translation.csv": "bronze_category_translation",
}

uploaded = {f.name for f in dbutils.fs.ls(RAW)}
missing = set(FILES) - uploaded
assert not missing, f"Upload these files to {RAW} first: {sorted(missing)}"

for file_name, table in FILES.items():
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)  # review comments contain line breaks
        .option("escape", '"')
        .csv(f"{RAW}/{file_name}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(T(table))
    print(f"{table:<30} {spark.table(T(table)).count():>9,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ECB exchange rates – incremental MERGE
# MAGIC 1. Try the ECB Data Portal API (only the days after the last loaded day).
# MAGIC 2. No outbound internet (Databricks Free Edition allows only a few trusted domains)? → use the CSV in the
# MAGIC    repository (`data/ecb_brl_eur_2016_2018.csv`, created with `python src/ecb_ingest.py`).
# MAGIC 3. `MERGE` on `rate_date`: new days are inserted, existing days are left alone → the job can run any number of times.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(".."))  # Git folder: repository root → `src` is importable
import pandas as pd

from src.ecb_ingest import fetch_rates

START, END = "2016-01-01", "2018-12-31"  # Olist period; for a live source use date.today()
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {T('bronze_fx_rates')} "
    "(rate_date DATE, currency STRING, rate_per_eur DOUBLE, _source STRING, _ingested_at TIMESTAMP)"
)
last = spark.table(T("bronze_fx_rates")).agg(F.max("rate_date")).first()[0]
start = (pd.Timestamp(last) + pd.Timedelta(days=1)).date().isoformat() if last else START

if start > END:
    rates, source = pd.DataFrame(), "up to date"
else:
    try:
        rates, source = fetch_rates("BRL", start, END, timeout=15), "ECB API"
    except Exception as err:  # no internet access from the workspace
        print(f"ECB API not reachable ({type(err).__name__}) → using the CSV in the repository")
        rates = pd.read_csv(os.path.abspath("../data/ecb_brl_eur_2016_2018.csv"))
        rates = rates[rates["rate_date"] >= start]
        source = "repo CSV"

if len(rates):
    (
        spark.createDataFrame(rates[["rate_date", "currency", "rate_per_eur"]].astype({"rate_date": "str"}))
        .select(F.to_date("rate_date").alias("rate_date"), "currency", F.col("rate_per_eur").cast("double"))
        .withColumn("_source", F.lit(source))
        .withColumn("_ingested_at", F.current_timestamp())
        .createOrReplaceTempView("new_rates")
    )
    spark.sql(f"""
        MERGE INTO {T('bronze_fx_rates')} AS t
        USING new_rates AS s
        ON t.rate_date = s.rate_date AND t.currency = s.currency
        WHEN NOT MATCHED THEN INSERT *
    """)
print(f"source: {source} · new days offered: {len(rates)} · table now: {spark.table(T('bronze_fx_rates')).count()} days")
