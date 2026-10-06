"""Reusable data-quality checks for PySpark DataFrames.

Each check returns one CheckResult (failed rows / total rows). `to_dataframe` turns a list of results into a
DataFrame that the silver notebook APPENDS to the Delta table `dq_results` – so every run is kept and the
history can be charted (Power BI) or read by the AI agent in project 4.

`olist_checks` reproduces the 13 checks of project 1 (SQL view dq.v_checks) – same rules, same expected
numbers – so both projects can be reconciled.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CheckResult:
    check_id: int
    table_name: str
    check_name: str
    dimension: str
    failed_rows: int
    total_rows: int

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0


def check_condition(df: DataFrame, check_id: int, table: str, name: str, dimension: str, failed: Column) -> CheckResult:
    """Generic check: rows where `failed` is TRUE fail (NULL counts as passed). One pass over the data."""
    row = df.agg(F.count("*").alias("total"), F.sum(F.when(failed, 1).otherwise(0)).alias("failed")).first()
    return CheckResult(check_id, table, name, dimension, int(row["failed"] or 0), int(row["total"]))


def check_not_null(df: DataFrame, table: str, column: str, check_id: int = 0) -> CheckResult:
    return check_condition(df, check_id, table, f"not_null:{column}", "Completeness", F.col(column).isNull())


def check_unique(df: DataFrame, table: str, columns: list[str], check_id: int = 0) -> CheckResult:
    total = df.count()
    distinct = df.select(*columns).distinct().count()
    return CheckResult(check_id, table, f"unique:{'+'.join(columns)}", "Uniqueness", total - distinct, total)


def check_range(
    df: DataFrame, table: str, column: str, min_value=None, max_value=None, check_id: int = 0
) -> CheckResult:
    cond = F.lit(False)
    if min_value is not None:
        cond = cond | (F.col(column) < F.lit(min_value))
    if max_value is not None:
        cond = cond | (F.col(column) > F.lit(max_value))
    return check_condition(
        df, check_id, table, f"range:{column}[{min_value},{max_value}]", "Validity", F.col(column).isNotNull() & cond
    )


def check_not_before(df: DataFrame, table: str, earlier: str, later: str, check_id: int = 0) -> CheckResult:
    """Rows where `later` < `earlier` (e.g. delivered before purchased) fail."""
    failed = F.col(later) < F.col(earlier)
    return check_condition(df, check_id, table, f"order:{earlier}<={later}", "Consistency", failed)


def olist_checks(
    orders: DataFrame,
    items: DataFrame,
    reviews_raw: DataFrame,
    reviews: DataFrame,
    products: DataFrame,
    sellers: DataFrame,
) -> list[CheckResult]:
    """The 13 checks of project 1 (dq.v_checks), on the silver tables. Expected total: 4,542 failed rows."""
    has_items = items.select("order_id").distinct().withColumn("_has_item", F.lit(True))
    has_review = reviews.select("order_id").withColumn("_has_review", F.lit(True))
    o = orders.join(has_items, "order_id", "left").join(has_review, "order_id", "left")
    known_seller = sellers.select("seller_id").withColumn("_known", F.lit(True))
    i = items.join(known_seller, "seller_id", "left")
    c = F.col
    return [
        check_condition(o, 1, "orders", "Delivered order without delivery date", "Completeness",
                        (c("order_status") == "delivered") & c("delivered_customer_ts").isNull()),
        check_condition(o, 2, "orders", "Order without any item", "Completeness", c("_has_item").isNull()),
        check_condition(o, 3, "orders", "Order without review", "Completeness", c("_has_review").isNull()),
        check_condition(o, 4, "orders", "Purchase timestamp not convertible", "Validity", c("purchase_ts").isNull()),
        check_condition(o, 5, "orders", "Handed to carrier before approval", "Consistency",
                        c("delivered_carrier_ts") < c("approved_ts")),
        check_condition(o, 6, "orders", "Handed to carrier before purchase", "Consistency",
                        c("delivered_carrier_ts") < c("purchase_ts")),
        check_condition(o, 7, "orders", "Delivered to customer before carrier pickup", "Consistency",
                        c("delivered_customer_ts") < c("delivered_carrier_ts")),
        check_condition(o, 8, "orders", "Not delivered, but delivery date set", "Consistency",
                        (c("order_status") != "delivered") & c("delivered_customer_ts").isNotNull()),
        check_condition(items, 9, "order_items", "Price missing or <= 0", "Validity",
                        c("price_brl").isNull() | (c("price_brl") <= 0)),
        CheckResult(10, "order_reviews", "Duplicate review_id rows", "Uniqueness",
                    reviews_raw.count() - reviews_raw.select("review_id").distinct().count(), reviews_raw.count()),
        check_condition(products, 11, "products", "Product without category", "Completeness",
                        c("category") == "unknown"),
        check_condition(products, 12, "products", "Category missing in translation file", "Completeness",
                        c("category_not_translated") == 1),
        check_condition(i, 13, "order_items", "Item with unknown seller", "Integrity", c("_known").isNull()),
    ]


RESULT_SCHEMA = (
    "run_ts timestamp, check_id int, table_name string, check_name string, dimension string, "
    "failed_rows long, total_rows long, passed boolean"
)


def to_dataframe(spark: SparkSession, results: list[CheckResult], run_ts: datetime | None = None) -> DataFrame:
    """One row per check, stamped with the run time (UTC) → append to dq_results."""
    run_ts = run_ts or datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    rows = [
        (run_ts, r.check_id, r.table_name, r.check_name, r.dimension, r.failed_rows, r.total_rows, r.passed)
        for r in results
    ]
    return spark.createDataFrame(rows, schema=RESULT_SCHEMA)


Check = Callable[[DataFrame], CheckResult]


def run_checks(spark: SparkSession, df: DataFrame, checks: list[Check]) -> DataFrame:
    """Convenience wrapper: run several checks on ONE DataFrame and return the results DataFrame."""
    return to_dataframe(spark, [check(df) for check in checks])
