"""Unit tests for src/transforms.py – each test pins one business rule on a tiny, hand-made dataset.
Runs in GitHub Actions (Java 17 + PySpark). Locally on Windows: pytest -k "not spark" skips them."""
import datetime as dt
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from src import transforms as t  # noqa: E402

ORDER_COLS = (
    "order_id string, customer_id string, order_status string, order_purchase_timestamp string, "
    "order_approved_at string, order_delivered_carrier_date string, order_delivered_customer_date string, "
    "order_estimated_delivery_date string"
)
ITEM_COLS = (
    "order_id string, order_item_id string, product_id string, seller_id string, "
    "shipping_limit_date string, price string, freight_value string"
)


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("transform-tests").getOrCreate()
    yield s
    s.stop()


@pytest.fixture(scope="module")
def data(spark):
    orders = spark.createDataFrame(
        [
            # on time: delivered before the promised date, handed over before the shipping limit
            ("o1", "c1", "delivered", "2018-01-05 10:00:00", "2018-01-05 11:00:00", "2018-01-08 09:00:00",
             "2018-01-12 15:00:00", "2018-01-20 00:00:00"),
            # late + carrier BEFORE approval (data error) → processing time must be NULL
            ("o2", "c2", "delivered", "2018-01-06 10:00:00", "2018-01-09 10:00:00", "2018-01-08 10:00:00",
             "2018-01-30 10:00:00", "2018-01-25 00:00:00"),
            # canceled, no items
            ("o3", "c3", "Canceled ", "2018-01-07 10:00:00", None, None, None, "2018-01-30 00:00:00"),
            # invalid timestamp → NULL instead of a failed job (try_to_timestamp)
            ("o4", "c4", "shipped", "not a date", None, None, None, "2018-02-01 00:00:00"),
        ],
        ORDER_COLS,
    )
    items = spark.createDataFrame(
        [
            ("o1", "1", "p1", "s1", "2018-01-09 00:00:00", "100.00", "10.00"),
            ("o2", "1", "p2", "s2", "2018-01-07 00:00:00", "50.00", "5.00"),
            ("o2", "1", "p2", "s2", "2018-01-07 00:00:00", "50.00", "5.00"),  # exact duplicate
        ],
        ITEM_COLS,
    )
    # Friday 5 Jan and Monday 8 Jan only – Saturday 6 Jan must reuse Friday's rate
    fx = spark.createDataFrame([("2018-01-05", 4.0), ("2018-01-08", 5.0)], "rate_date string, rate_per_eur double")
    reviews = spark.createDataFrame(
        [("r1", "o1", "5"), ("r2", "o2", "4"), ("r3", "o2", "1")],
        "review_id string, order_id string, review_score string",
    )
    o = t.silver_orders(orders)
    fxd = t.fx_daily(fx)
    i = t.silver_order_items(items, o, fxd)
    r = t.silver_order_reviews(reviews)
    return {"o": o, "fx": fxd, "i": i, "r": r}


def row(df, **key):
    (k, v), = key.items()
    rows = df.filter(df[k] == v).collect()
    assert len(rows) == 1
    return rows[0]


def test_fx_weekend_uses_previous_business_day(data):
    sat = row(data["fx"], rate_date=dt.date(2018, 1, 6))
    assert sat["rate_per_eur"] == 4.0 and sat["is_filled"] is True
    assert data["fx"].count() == 4  # Fri, Sat, Sun, Mon


def test_status_is_normalised_and_bad_timestamp_becomes_null(data):
    assert row(data["o"], order_id="o3")["order_status"] == "canceled"
    assert row(data["o"], order_id="o4")["purchase_ts"] is None


def test_items_deduplicated_and_converted_with_purchase_day_rate(data):
    assert data["i"].count() == 2
    o2 = row(data["i"], order_id="o2")  # bought on Saturday → Friday's rate 4.0
    assert o2["price_eur"] == Decimal("12.50")


def test_reviews_keep_worst_score(data):
    assert row(data["r"], order_id="o2")["review_score_min"] == 1


def test_fact_orders_keeps_orders_without_items(data):
    fo = t.gold_fact_orders(data["o"], data["i"], data["r"])
    assert fo.count() == 4
    o3 = row(fo, order_id="o3")
    assert (o3["is_canceled"], o3["item_count"], o3["delivery_on_time"]) == (1, 0, None)


def test_fact_orders_delivery_and_complaint_flags(data):
    fo = t.gold_fact_orders(data["o"], data["i"], data["r"])
    o1, o2 = row(fo, order_id="o1"), row(fo, order_id="o2")
    assert (o1["delivery_on_time"], o1["lead_time_days"], o1["is_complaint"]) == (1, 7, 0)
    assert (o2["delivery_on_time"], o2["days_late"], o2["is_complaint"]) == (0, 5, 1)


def test_fact_items_handover_and_processing_time(data):
    fi = t.gold_fact_order_items(data["i"], data["o"], data["r"])
    o1, o2 = row(fi, order_id="o1"), row(fi, order_id="o2")
    assert (o1["handover_on_time"], o1["seller_processing_days"]) == (1, 3)
    assert o2["handover_on_time"] == 0
    assert o2["seller_processing_days"] is None  # carrier before approval → excluded


def test_weekly_kpis_use_order_grain_for_customer_kpis(data):
    fo = t.gold_fact_orders(data["o"], data["i"], data["r"])
    fi = t.gold_fact_order_items(data["i"], data["o"], data["r"])
    wk = row(t.gold_weekly_kpis(fo, fi), week_start=dt.date(2018, 1, 1))
    assert wk["orders"] == 3  # o4 has no valid purchase date
    assert wk["cancellation_rate"] == pytest.approx(1 / 3, abs=1e-4)
    assert wk["customer_otd"] == 0.5 and wk["late_orders"] == 1
    assert wk["order_items"] == 2 and wk["supplier_otd"] == 0.5
