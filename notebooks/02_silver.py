# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver – typed, deduplicated, in EUR, checked
# MAGIC Same rules as the SQL `core` views of project 1 – now as tested PySpark functions in `src/transforms.py`.
# MAGIC New: every item is converted to EUR with the ECB rate of its purchase day (weekends forward-filled),
# MAGIC and the 13 data-quality checks are **appended** to `dq_results`, so every run stays in the history.

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
from src.dq_checks import olist_checks, to_dataframe

# COMMAND ----------

b = lambda name: spark.table(T(f"bronze_{name}"))  # noqa: E731

fx = t.fx_daily(spark.table(T("bronze_fx_rates")))
orders = t.silver_orders(b("orders"))
silver = {
    "silver_fx_daily": fx,
    "silver_orders": orders,
    "silver_order_items": t.silver_order_items(b("order_items"), orders, fx),
    "silver_order_reviews": t.silver_order_reviews(b("order_reviews")),
    "silver_products": t.silver_products(b("products"), b("category_translation")),
    "silver_customers": t.silver_customers(b("customers")),
    "silver_sellers": t.silver_sellers(b("sellers")),
}
for name, df in silver.items():
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(T(name))
    print(f"{name:<22} {spark.table(T(name)).count():>9,} rows")

# COMMAND ----------

# MAGIC %md ## Data-quality checks → `dq_results` (append = history of every run)

# COMMAND ----------

s = lambda name: spark.table(T(f"silver_{name}"))  # noqa: E731
results = olist_checks(
    orders=s("orders"),
    items=s("order_items"),
    reviews_raw=b("order_reviews"),
    reviews=s("order_reviews"),
    products=s("products"),
    sellers=s("sellers"),
)
dq = to_dataframe(spark, results)
dq.write.mode("append").saveAsTable(T("dq_results"))

failed, checked = sum(r.failed_rows for r in results), sum(r.total_rows for r in results)
print(f"{len(results)} checks · {failed:,} failed of {checked:,} rows · DQ score {1 - failed / checked:.2%}")
display(dq.orderBy("check_id"))
