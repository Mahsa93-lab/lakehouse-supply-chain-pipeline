# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Validate – quality gate
# MAGIC Reconciles the lakehouse with **project 1** (SQL Server): same source, same rules → the same numbers.
# MAGIC Any FAIL raises an error, so the job run turns red and nobody reports on wrong data.
# MAGIC Expected values: `docs/expected-results.md`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "olist")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")


def T(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


# COMMAND ----------

latest_dq = f"(SELECT * FROM {T('dq_results')} WHERE run_ts = (SELECT max(run_ts) FROM {T('dq_results')}))"
actual = spark.sql(f"""
SELECT 'bronze_orders rows' AS test, CAST(count(*) AS DECIMAL(18,2)) AS actual FROM {T('bronze_orders')}
UNION ALL SELECT 'bronze_order_items rows', count(*) FROM {T('bronze_order_items')}
UNION ALL SELECT 'bronze_order_reviews rows', count(*) FROM {T('bronze_order_reviews')}
UNION ALL SELECT 'bronze_fx_rates days (ECB)', count(*) FROM {T('bronze_fx_rates')}
UNION ALL SELECT 'gold_fact_orders rows', count(*) FROM {T('gold_fact_orders')}
UNION ALL SELECT 'gold_fact_order_items rows', count(*) FROM {T('gold_fact_order_items')}
UNION ALL SELECT 'items without EUR price', count_if(price_eur IS NULL) FROM {T('gold_fact_order_items')}
UNION ALL SELECT 'revenue BRL (= project 1)', sum(price_brl) FROM {T('gold_fact_order_items')}
UNION ALL SELECT 'revenue EUR', sum(price_eur) FROM {T('gold_fact_order_items')}
UNION ALL SELECT 'canceled orders (= project 1)', sum(is_canceled) FROM {T('gold_fact_orders')}
UNION ALL SELECT 'late orders (= project 1)', count_if(delivery_on_time = 0) FROM {T('gold_fact_orders')}
UNION ALL SELECT 'late handovers (= project 1)', count_if(handover_on_time = 0) FROM {T('gold_fact_order_items')}
UNION ALL SELECT 'customer OTD % (= project 1)',
          round(100 * sum(delivery_on_time) / count(delivery_on_time), 2) FROM {T('gold_fact_orders')}
UNION ALL SELECT 'supplier OTD % (= project 1)',
          round(100 * sum(handover_on_time) / count(handover_on_time), 2) FROM {T('gold_fact_order_items')}
UNION ALL SELECT 'DQ checks (latest run)', count(*) FROM {latest_dq}
UNION ALL SELECT 'DQ failed rows (= project 1)', sum(failed_rows) FROM {latest_dq}
UNION ALL SELECT 'weekly KPI rows', count(*) FROM {T('gold_weekly_kpis')}
""")

expected = spark.createDataFrame(
    [
        ("bronze_orders rows", 99441.00),
        ("bronze_order_items rows", 112650.00),
        ("bronze_order_reviews rows", 99224.00),
        ("bronze_fx_rates days (ECB)", 767.00),
        ("gold_fact_orders rows", 99441.00),
        ("gold_fact_order_items rows", 112650.00),
        ("items without EUR price", 0.00),
        ("revenue BRL (= project 1)", 13591643.70),
        ("revenue EUR", 3450018.83),
        ("canceled orders (= project 1)", 1234.00),
        ("late orders (= project 1)", 6535.00),
        ("late handovers (= project 1)", 10423.00),
        ("customer OTD % (= project 1)", 93.23),
        ("supplier OTD % (= project 1)", 90.65),
        ("DQ checks (latest run)", 13.00),
        ("DQ failed rows (= project 1)", 4542.00),
        ("weekly KPI rows", 101.00),
    ],
    "test string, expected double",
)

from pyspark.sql import functions as F

report = (
    expected.join(actual, "test", "left")
    .withColumn("actual", F.col("actual").cast("double"))
    .withColumn("result", F.when(F.abs(F.col("actual") - F.col("expected")) < 0.005, "PASS").otherwise("FAIL"))
    .orderBy("result", "test")  # FAIL before PASS
)
display(report)

failed = [r["test"] for r in report.collect() if r["result"] == "FAIL"]
assert not failed, f"Quality gate failed: {failed}"
print(f"All {report.count()} tests PASS")
