# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold – business tables
# MAGIC | Table | Grain | Used by |
# MAGIC |---|---|---|
# MAGIC | `gold_fact_orders` | one row per order (customer view) | Power BI |
# MAGIC | `gold_fact_order_items` | one row per order item (supplier view) | Power BI |
# MAGIC | `gold_weekly_kpis` | one row per ISO week | management report, KPI agent (project 4) |
# MAGIC | `gold_supplier_scorecard` | one row per supplier and month | supplier trends, KPI agent |
# MAGIC
# MAGIC Two fact grains, exactly as in project 1: an item-level table alone would lose the 775 orders without items.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "olist")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")


def T(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


import os
import sys

sys.path.insert(0, os.path.abspath(".."))
from src import transforms as t

# COMMAND ----------

s = lambda name: spark.table(T(f"silver_{name}"))  # noqa: E731
orders, items, reviews = s("orders"), s("order_items"), s("order_reviews")

gold = {"gold_fact_orders": t.gold_fact_orders(orders, items, reviews),
        "gold_fact_order_items": t.gold_fact_order_items(items, orders, reviews)}
for name, df in gold.items():
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(T(name))

fo, fi = spark.table(T("gold_fact_orders")), spark.table(T("gold_fact_order_items"))
t.gold_weekly_kpis(fo, fi).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(T("gold_weekly_kpis"))
t.gold_supplier_scorecard(fi).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(
    T("gold_supplier_scorecard")
)
for name in ["gold_fact_orders", "gold_fact_order_items", "gold_weekly_kpis", "gold_supplier_scorecard"]:
    print(f"{name:<25} {spark.table(T(name)).count():>9,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation in Unity Catalog (governance)
# MAGIC Comments show up in Catalog Explorer, in Genie and for every analyst who queries the tables.

# COMMAND ----------

TABLE_COMMENTS = {
    "gold_fact_orders": "One row per order (customer view). Includes orders without items. Source: Olist, ECB.",
    "gold_fact_order_items": "One row per order item (supplier view). Prices in BRL and EUR (ECB rate of purchase day).",
    "gold_weekly_kpis": "One row per ISO week (Monday). Customer KPIs on order grain, supplier KPIs on item grain.",
    "gold_supplier_scorecard": "One row per supplier and month. Input for supplier trends and the KPI agent.",
}
COLUMN_COMMENTS = {
    "gold_weekly_kpis": {
        "customer_otd": "Share of delivered orders that arrived on or before the promised date (order grain)",
        "supplier_otd": "Share of items handed to the carrier before the shipping limit (item grain)",
        "complaint_rate": "Share of reviewed orders whose worst review score is 1 or 2",
        "cancellation_rate": "Share of orders with status canceled or unavailable",
        "revenue_eur": "Sum of item prices converted with the ECB BRL/EUR reference rate of the purchase day",
        "is_analysis_period": "TRUE for weeks between 2017-01-01 and 2018-08-31 (edges have too few orders)",
    },
    "gold_fact_order_items": {
        "handover_on_time": "1 = handed to carrier before shipping limit, 0 = late, NULL = not handed over",
        "seller_processing_days": "Days from approval to carrier handover; NULL if handover is before approval",
        "price_eur": "price_brl / ECB rate (BRL per EUR) of the purchase day; weekends use the previous rate",
    },
}
for table, text in TABLE_COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {T(table)} IS '{text}'")
for table, cols in COLUMN_COMMENTS.items():
    for col, text in cols.items():
        spark.sql(f"ALTER TABLE {T(table)} ALTER COLUMN {col} COMMENT '{text}'")

display(spark.table(T("gold_weekly_kpis")).where("is_analysis_period").orderBy("week_start"))
