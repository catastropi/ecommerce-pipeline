"""
dags/scripts/spark_aggregations.py에 대한 단위 테스트.

Window Function(row_number/rank/dense_rank/lag/running total/moving average)이
실제로 의도한 값을 내는지 숫자로 직접 검증한다.
"""
import sys
import os
from datetime import date


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from scripts.spark_aggregations import (  # noqa: E402
    daily_sales,
    seller_sales,
    top_products,
    repurchase_rate,
    refund_rate,
)


_FACT_COLUMNS = [
    "order_id", "order_date", "item_total_value",
    "seller_id", "seller_state", "product_category_name_english", "product_id",
]


def _fact_row(order_id, order_date, item_total_value, seller_id="s1",
              seller_state="SP", category="cat", product_id="p1"):
    return (order_id, order_date, item_total_value, seller_id, seller_state, category, product_id)


# ---------------------------------------------------------------------------
# daily_sales: lag(전일 대비 증감) / running total
# ---------------------------------------------------------------------------
def test_daily_sales_running_total_and_lag(spark):
    fact = spark.createDataFrame(
        [
            _fact_row("o1", date(2026, 1, 1), 100.0),
            _fact_row("o2", date(2026, 1, 2), 150.0),
            _fact_row("o3", date(2026, 1, 3), 50.0),
        ],
        _FACT_COLUMNS,
    )

    result = {row["order_date"]: row for row in daily_sales(fact).collect()}

    day1, day2, day3 = result[date(2026, 1, 1)], result[date(2026, 1, 2)], result[date(2026, 1, 3)]

    assert day1["prev_day_revenue"] is None  # 1일차는 이전 날이 없음
    assert day1["running_total_revenue"] == 100.0

    assert day2["prev_day_revenue"] == 100.0
    assert round(day2["revenue_change_pct"], 1) == 50.0  # 100 -> 150, +50%
    assert day2["running_total_revenue"] == 250.0

    assert day3["running_total_revenue"] == 300.0


# ---------------------------------------------------------------------------
# seller_sales: rank() vs dense_rank() 동점자 처리 차이
# ---------------------------------------------------------------------------
def test_seller_sales_rank_and_dense_rank_differ_on_ties(spark):
    fact = spark.createDataFrame(
        [
            _fact_row("o1", date(2026, 1, 1), 300.0, seller_id="s1"),
            _fact_row("o2", date(2026, 1, 1), 300.0, seller_id="s2"),  # s1과 공동 1위
            _fact_row("o3", date(2026, 1, 1), 100.0, seller_id="s3"),
        ],
        _FACT_COLUMNS,
    )

    result = {row["seller_id"]: row for row in seller_sales(fact).collect()}

    assert result["s1"]["revenue_rank"] == 1
    assert result["s2"]["revenue_rank"] == 1
    assert result["s1"]["revenue_dense_rank"] == 1

    # rank()는 동점자 2명을 건너뛰고 3위, dense_rank()는 건너뛰지 않고 2위
    assert result["s3"]["revenue_rank"] == 3
    assert result["s3"]["revenue_dense_rank"] == 2


# ---------------------------------------------------------------------------
# top_products: row_number() 기반 카테고리별 1위 상품
# ---------------------------------------------------------------------------
def test_top_products_row_number_picks_one_best_per_category(spark):
    fact = spark.createDataFrame(
        [
            _fact_row("o1", date(2026, 1, 1), 500.0, category="electronics", product_id="p1"),
            _fact_row("o2", date(2026, 1, 1), 100.0, category="electronics", product_id="p2"),
            _fact_row("o3", date(2026, 1, 1), 200.0, category="toys", product_id="p3"),
        ],
        _FACT_COLUMNS,
    )

    _, best_in_category = top_products(fact, top_n=10)
    result = {row["product_category_name_english"]: row["product_id"] for row in best_in_category.collect()}

    assert result["electronics"] == "p1"  # 500 > 100
    assert result["toys"] == "p3"
    assert len(result) == 2  # 카테고리마다 정확히 1개씩만 남아야 함


# ---------------------------------------------------------------------------
# repurchase_rate / refund_rate: 비율 계산 + 0-division 경계값
# ---------------------------------------------------------------------------
def test_repurchase_rate_computes_ratio(spark):
    customer_features = spark.createDataFrame(
        [("cu1", True), ("cu2", False), ("cu3", True), ("cu4", False)],
        ["customer_unique_id", "is_repeat_customer"],
    )
    assert repurchase_rate(customer_features) == 0.5


def test_repurchase_rate_returns_zero_for_empty_input(spark):
    empty = spark.createDataFrame([], "customer_unique_id STRING, is_repeat_customer BOOLEAN")
    assert repurchase_rate(empty) == 0.0


def test_refund_rate_treats_canceled_as_refund(spark):
    orders_status = spark.createDataFrame(
        [("o1", "delivered"), ("o2", "canceled"), ("o3", "delivered"), ("o4", "canceled")],
        ["order_id", "order_status"],
    )
    assert refund_rate(orders_status) == 0.5
