"""Pure PySpark transformations for the medallion pipeline (bronze → silver → gold).

Every function takes DataFrames and returns a DataFrame – no reading, no writing. That keeps the
logic unit-testable on a laptop / in CI, while the Databricks notebooks only orchestrate
(read table → call function → write table).

The business rules are the same as in the SQL views of project 1 (supplier-delivery-performance),
so both projects must produce identical counts and BRL totals (checked in notebooks/04_validate).
Casts use try_cast / try_to_timestamp – the PySpark twin of T-SQL TRY_CONVERT: invalid values become
NULL instead of failing the job (Spark 4 / Databricks serverless run in ANSI mode).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ANALYSIS_START, ANALYSIS_END = "2017-01-01", "2018-08-31"


def _ts(col: str) -> F.Column:
    return F.try_to_timestamp(F.col(col), F.lit("yyyy-MM-dd HH:mm:ss"))


def _try_cast(col: str, dtype: str) -> F.Column:
    return F.expr(f"try_cast(`{col}` AS {dtype})")


# ---------------------------------------------------------------- FX rates ----------------------------------------
def fx_daily(fx: DataFrame) -> DataFrame:
    """ECB publishes no rates on weekends/holidays → one row per calendar day, gaps forward-filled.

    Input: rate_date (date or 'yyyy-MM-dd'), rate_per_eur (BRL per 1 EUR).
    Output: rate_date, rate_per_eur, is_filled.
    """
    rates = fx.select(F.to_date("rate_date").alias("rate_date"), F.col("rate_per_eur").cast("double"))
    calendar = rates.agg(F.min("rate_date").alias("lo"), F.max("rate_date").alias("hi")).select(
        F.explode(F.sequence("lo", "hi", F.expr("INTERVAL 1 DAY"))).alias("rate_date")
    )
    w = Window.orderBy("rate_date").rowsBetween(Window.unboundedPreceding, 0)
    return (
        calendar.join(rates, "rate_date", "left")
        .withColumn("is_filled", F.col("rate_per_eur").isNull())
        .withColumn("rate_per_eur", F.last("rate_per_eur", ignorenulls=True).over(w))
    )


# ---------------------------------------------------------------- Silver ------------------------------------------
def silver_orders(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        "order_id",
        "customer_id",
        F.lower(F.trim("order_status")).alias("order_status"),
        _ts("order_purchase_timestamp").alias("purchase_ts"),
        _ts("order_approved_at").alias("approved_ts"),
        _ts("order_delivered_carrier_date").alias("delivered_carrier_ts"),
        _ts("order_delivered_customer_date").alias("delivered_customer_ts"),
        F.to_date(F.substring("order_estimated_delivery_date", 1, 10)).alias("estimated_delivery_date"),
    ).dropDuplicates(["order_id"])


def silver_order_items(bronze: DataFrame, orders: DataFrame, fx: DataFrame) -> DataFrame:
    """Typed items + EUR conversion with the ECB rate of the purchase date."""
    purchase_day = orders.select("order_id", F.to_date("purchase_ts").alias("rate_date"))
    return (
        bronze.select(
            "order_id",
            _try_cast("order_item_id", "int").alias("order_item_id"),
            "product_id",
            "seller_id",
            _ts("shipping_limit_date").alias("shipping_limit_ts"),
            _try_cast("price", "decimal(12,2)").alias("price_brl"),
            _try_cast("freight_value", "decimal(12,2)").alias("freight_brl"),
        )
        .dropDuplicates(["order_id", "order_item_id"])
        .join(purchase_day, "order_id", "left")
        .join(fx.select("rate_date", "rate_per_eur"), "rate_date", "left")
        .withColumn("price_eur", F.round(F.col("price_brl") / F.col("rate_per_eur"), 2).cast("decimal(12,2)"))
        .withColumn("freight_eur", F.round(F.col("freight_brl") / F.col("rate_per_eur"), 2).cast("decimal(12,2)"))
        .withColumnRenamed("rate_per_eur", "fx_brl_per_eur")
        .drop("rate_date")
    )


def silver_order_reviews(bronze: DataFrame) -> DataFrame:
    """One order can have several reviews (review_id is not unique) → one row per order, WORST score."""
    score = _try_cast("review_score", "int")
    return bronze.groupBy("order_id").agg(
        F.min(score).alias("review_score_min"),
        F.round(F.avg(score), 2).alias("review_score_avg"),
        F.count("*").alias("review_count"),
    )


# 2 categories are missing in Olist's translation file (same fix as project 1)
_MANUAL_TRANSLATIONS = {
    "pc_gamer": "pc_gamer",
    "portateis_cozinha_e_preparadores_de_alimentos": "portable_kitchen_and_food_preparers",
}


def silver_products(products: DataFrame, translation: DataFrame) -> DataFrame:
    manual = F.create_map(*[F.lit(x) for kv in _MANUAL_TRANSLATIONS.items() for x in kv])
    t = translation.select("product_category_name", "product_category_name_english")
    return (
        products.join(t, "product_category_name", "left")
        .select(
            "product_id",
            F.coalesce(
                "product_category_name_english", manual[F.col("product_category_name")], F.lit("unknown")
            ).alias("category"),
            (F.col("product_category_name_english").isNull() & F.col("product_category_name").isNotNull())
            .cast("int")
            .alias("category_not_translated"),
            _try_cast("product_weight_g", "int").alias("weight_g"),
        )
    )


def silver_customers(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        "customer_id", "customer_unique_id", F.col("customer_city").alias("city"),
        F.upper("customer_state").alias("state"),
    )


def silver_sellers(bronze: DataFrame) -> DataFrame:
    return bronze.select("seller_id", F.col("seller_city").alias("city"), F.upper("seller_state").alias("state"))


# ---------------------------------------------------------------- Gold (two grains, as in project 1) --------------
def _delivery_on_time() -> F.Column:
    return (
        F.when(F.col("delivered_customer_ts").isNull() | F.col("estimated_delivery_date").isNull(), None)
        .when(F.to_date("delivered_customer_ts") <= F.col("estimated_delivery_date"), 1)
        .otherwise(0)
    )


def _is_complaint() -> F.Column:
    return F.when(F.col("review_score_min").isNull(), None).when(F.col("review_score_min") <= 2, 1).otherwise(0)


def gold_fact_orders(orders: DataFrame, items: DataFrame, reviews: DataFrame) -> DataFrame:
    """One row per ORDER (customer view). Keeps the orders without items (canceled/unavailable) –
    an item-level table would lose them and the cancellation rate would be wrong."""
    per_order = items.groupBy("order_id").agg(
        F.count("*").alias("item_count"),
        F.sum("price_brl").alias("order_value_brl"),
        F.sum("price_eur").alias("order_value_eur"),
        F.sum("freight_brl").alias("freight_brl"),
    )
    return (
        orders.join(reviews.select("order_id", "review_score_min"), "order_id", "left")
        .join(per_order, "order_id", "left")
        .select(
            "order_id",
            "customer_id",
            "order_status",
            F.to_date("purchase_ts").alias("purchase_date"),
            "estimated_delivery_date",
            F.to_date("delivered_customer_ts").alias("delivered_date"),
            (F.col("order_status") == "delivered").cast("int").alias("is_delivered"),
            F.col("order_status").isin("canceled", "unavailable").cast("int").alias("is_canceled"),
            _delivery_on_time().alias("delivery_on_time"),
            F.when(
                F.col("delivered_customer_ts") >= F.col("purchase_ts"),
                F.datediff(F.to_date("delivered_customer_ts"), F.to_date("purchase_ts")),
            ).alias("lead_time_days"),
            F.when(
                F.col("delivered_customer_ts").isNotNull(),
                F.datediff(F.to_date("delivered_customer_ts"), "estimated_delivery_date"),
            ).alias("days_late"),
            "review_score_min",
            _is_complaint().alias("is_complaint"),
            F.coalesce("item_count", F.lit(0)).alias("item_count"),
            F.coalesce("order_value_brl", F.lit(0).cast("decimal(22,2)")).alias("order_value_brl"),
            F.coalesce("order_value_eur", F.lit(0).cast("decimal(22,2)")).alias("order_value_eur"),
            F.coalesce("freight_brl", F.lit(0).cast("decimal(22,2)")).alias("freight_brl"),
        )
    )


def gold_fact_order_items(items: DataFrame, orders: DataFrame, reviews: DataFrame) -> DataFrame:
    """One row per ORDER ITEM (supplier view)."""
    return (
        items.join(orders, "order_id")
        .join(reviews.select("order_id", "review_score_min"), "order_id", "left")
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "customer_id",
            "order_status",
            F.to_date("purchase_ts").alias("purchase_date"),
            "price_brl",
            "freight_brl",
            "price_eur",
            "freight_eur",
            # handed to the carrier before the agreed shipping limit
            F.when(F.col("delivered_carrier_ts").isNull() | F.col("shipping_limit_ts").isNull(), None)
            .when(F.col("delivered_carrier_ts") <= F.col("shipping_limit_ts"), 1)
            .otherwise(0)
            .alias("handover_on_time"),
            # approval → carrier; illogical sequences (carrier before approval) → NULL
            F.when(
                F.col("delivered_carrier_ts") >= F.col("approved_ts"),
                F.datediff(F.to_date("delivered_carrier_ts"), F.to_date("approved_ts")),
            ).alias("seller_processing_days"),
            _delivery_on_time().alias("delivery_on_time"),
            "review_score_min",
            _is_complaint().alias("is_complaint"),
        )
    )


def _rate(flag: str) -> F.Column:
    """Share of 1s among non-NULL flags (NULL = not measurable, e.g. not delivered yet)."""
    return F.round(F.sum(flag) / F.count(flag), 4)


def gold_weekly_kpis(fact_orders: DataFrame, fact_items: DataFrame) -> DataFrame:
    """One row per ISO week (Monday). Customer KPIs on order grain, supplier KPIs on item grain."""
    week = F.date_trunc("week", "purchase_date").cast("date").alias("week_start")
    customer = fact_orders.groupBy(week).agg(
        F.count("*").alias("orders"),
        _rate("is_canceled").alias("cancellation_rate"),
        _rate("delivery_on_time").alias("customer_otd"),
        F.sum(F.when(F.col("delivery_on_time") == 0, 1).otherwise(0)).alias("late_orders"),
        F.round(F.avg("lead_time_days"), 2).alias("avg_lead_time_days"),
        _rate("is_complaint").alias("complaint_rate"),
    )
    supplier = fact_items.groupBy(week).agg(
        F.count("*").alias("order_items"),
        F.sum("price_eur").alias("revenue_eur"),
        F.sum("price_brl").alias("revenue_brl"),
        _rate("handover_on_time").alias("supplier_otd"),
        F.sum(F.when(F.col("handover_on_time") == 0, 1).otherwise(0)).alias("late_handovers"),
        F.round(F.avg("seller_processing_days"), 2).alias("avg_seller_processing_days"),
        F.countDistinct("seller_id").alias("active_suppliers"),
    )
    return (
        customer.join(supplier, "week_start", "full")
        .withColumn(
            "is_analysis_period",
            F.col("week_start").between(F.lit(ANALYSIS_START).cast("date"), F.lit(ANALYSIS_END).cast("date")),
        )
        .orderBy("week_start")
    )


def gold_supplier_scorecard(fact_items: DataFrame) -> DataFrame:
    """One row per supplier and month – input for trend analysis and the KPI agent (project 4)."""
    month = F.date_trunc("month", "purchase_date").cast("date").alias("month")
    return fact_items.groupBy("seller_id", month).agg(
        F.count("*").alias("order_items"),
        F.sum("price_eur").alias("revenue_eur"),
        _rate("handover_on_time").alias("supplier_otd"),
        F.sum(F.when(F.col("handover_on_time") == 0, 1).otherwise(0)).alias("late_handovers"),
        F.round(F.avg("seller_processing_days"), 2).alias("avg_seller_processing_days"),
        _rate("is_complaint").alias("complaint_rate"),
    )
