"""Runs in GitHub Actions (Java + PySpark). Locally on Windows you can skip it:
pytest -k "not dq"  – the CI pipeline runs everything."""
import datetime as dt

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from src.dq_checks import (  # noqa: E402
    check_not_before,
    check_not_null,
    check_range,
    check_unique,
    run_checks,
)


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("dq-tests").getOrCreate()
    yield s
    s.stop()


@pytest.fixture()
def orders(spark):
    rows = [
        ("o1", dt.datetime(2018, 1, 1), dt.datetime(2018, 1, 5), 10.0),
        ("o2", dt.datetime(2018, 1, 2), dt.datetime(2018, 1, 1), 20.0),   # delivered before purchase
        ("o2", dt.datetime(2018, 1, 2), None, -5.0),                        # duplicate id, negative price
        ("o4", None, None, None),
    ]
    return spark.createDataFrame(rows, "order_id string, purchase_ts timestamp, delivered_ts timestamp, price double")


def test_not_null(orders):
    r = check_not_null(orders, "orders", "purchase_ts")
    assert (r.failed_rows, r.total_rows) == (1, 4)


def test_unique(orders):
    assert check_unique(orders, "orders", ["order_id"]).failed_rows == 1


def test_range(orders):
    assert check_range(orders, "orders", "price", min_value=0).failed_rows == 1


def test_not_before(orders):
    assert check_not_before(orders, "orders", "purchase_ts", "delivered_ts").failed_rows == 1


def test_run_checks_returns_one_row_per_check(spark, orders):
    result = run_checks(
        spark,
        orders,
        [
            lambda d: check_not_null(d, "orders", "order_id"),
            lambda d: check_unique(d, "orders", ["order_id"]),
        ],
    )
    rows = {r["check_name"]: r for r in result.collect()}
    assert rows["not_null:order_id"]["passed"] is True
    assert rows["unique:order_id"]["passed"] is False
