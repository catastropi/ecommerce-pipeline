"""
dags/scripts/spark_transformations.py에 대한 단위 테스트.

실제 함수(clean_customers, clean_order_items, build_sales_fact,
build_customer_features)를 import해서 결과값을 assert로 직접 확인한다.
"""
import sys
import os
from datetime import datetime


# dags/ 를 sys.path에 추가해야 "scripts.xxx" 형태로 import 가능
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from scripts.spark_transformations import (  # noqa: E402
    clean_customers,
    clean_order_items,
    build_sales_fact,
    build_customer_features,
)


# ---------------------------------------------------------------------------
# clean_customers: null 제거 / 중복 제거 / 대소문자 정규화
# ---------------------------------------------------------------------------
def test_clean_customers_drops_null_id_and_dedups_and_normalizes_case(spark):
    df = spark.createDataFrame(
        [
            ("c1", "  Sao Paulo  ", "sp"),
            ("c1", "sao paulo", "sp"),  # 중복 customer_id -> 하나만 남아야 함
            (None, "rio", "rj"),  # customer_id null -> 제거되어야 함
        ],
        ["customer_id", "customer_city", "customer_state"],
    )

    result = clean_customers(df).collect()

    assert len(result) == 1
    row = result[0]
    assert row["customer_id"] == "c1"
    assert row["customer_city"] == "sao paulo"  # trim + lower
    assert row["customer_state"] == "SP"  # trim + upper


# ---------------------------------------------------------------------------
# clean_order_items: price<=0 제거, IQR 상한 캡핑 (Act + Assert가 실제로 있는 버전)
# ---------------------------------------------------------------------------
def test_clean_order_items_filters_non_positive_price(spark):
    df = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", 100.0, 10.0),
            ("o2", 1, "p2", "s1", 0.0, 5.0),   # price=0 -> 제거
            ("o3", 1, "p3", "s1", -5.0, 5.0),  # 음수 price -> 제거
        ],
        ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
    )

    result = clean_order_items(df).collect()

    assert len(result) == 1
    assert result[0]["order_id"] == "o1"


def test_clean_order_items_caps_price_outlier_instead_of_dropping(spark):
    """이상치를 드랍하지 않고 IQR 상한값으로 캡핑한다는 것이 이 함수의
    핵심 설계 포인트(README에도 명시)라, 행 개수는 그대로 유지되면서
    극단값만 낮아지는지를 실제로 확인한다.
    """
    normal_rows = [("o%d" % i, 1, "p1", "s1", 100.0, 5.0) for i in range(10)]
    outlier_row = ("o_outlier", 1, "p1", "s1", 100000.0, 5.0)
    df = spark.createDataFrame(
        normal_rows + [outlier_row],
        ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
    )

    result = clean_order_items(df).collect()

    # 드랍되지 않고 행 개수는 그대로 유지되어야 한다
    assert len(result) == len(normal_rows) + 1

    outlier_after = [r for r in result if r["order_id"] == "o_outlier"][0]
    # 캡핑되었으므로 원래 값(100000.0)보다는 훨씬 작아야 한다
    assert outlier_after["price"] < 100000.0
    assert outlier_after["price"] >= 100.0


# ---------------------------------------------------------------------------
# build_sales_fact: 조인 + item_total_value / delivery_days 계산
# ---------------------------------------------------------------------------
def test_build_sales_fact_computes_item_total_value_and_delivery_days(spark):
    orders = spark.createDataFrame(
        [("o1", "c1", "delivered",
          datetime(2026, 1, 1, 10, 0, 0), datetime(2026, 1, 5, 10, 0, 0))],
        ["order_id", "customer_id", "order_status",
         "order_purchase_timestamp", "order_delivered_customer_date"],
    )
    order_items = spark.createDataFrame(
        [("o1", 1, "p1", "s1", 100.0, 20.0)],
        ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
    )
    products = spark.createDataFrame(
        [("p1", "eletronicos")], ["product_id", "product_category_name"],
    )
    category_translation = spark.createDataFrame(
        [("eletronicos", "electronics")],
        ["product_category_name", "product_category_name_english"],
    )
    sellers = spark.createDataFrame([("s1", "SP")], ["seller_id", "seller_state"])
    customers = spark.createDataFrame([("c1", "SP")], ["customer_id", "customer_state"])

    fact = build_sales_fact(
        orders=orders,
        order_items=order_items,
        products=products,
        sellers=sellers,
        customers=customers,
        category_translation=category_translation,
    ).collect()

    assert len(fact) == 1
    row = fact[0]
    assert row["item_total_value"] == 120.0  # price(100) + freight(20)
    assert row["delivery_days"] == 4  # 1/1 -> 1/5
    assert row["product_category_name_english"] == "electronics"


def test_build_sales_fact_handles_missing_delivery_date(spark):
    """아직 배송되지 않은 주문(order_delivered_customer_date가 null)은
    delivery_days가 에러 없이 null이 되어야 한다.

    스키마를 명시하지 않고 리스트만 넘기면, 이 한 줄짜리 데이터에서는
    order_delivered_customer_date 컬럼 값이 전부 None이라 Spark가 타입을
    추론하지 못해 CANNOT_DETERMINE_TYPE 에러가 난다. 이 테스트가 원래
    검증하려는 건 build_sales_fact의 null 처리 로직이지 Spark의 스키마
    추론 동작이 아니므로, 스키마를 명시해서 그 문제를 피해간다.
    """
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType

    orders_schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_purchase_timestamp", TimestampType()),
        StructField("order_delivered_customer_date", TimestampType()),
    ])
    orders = spark.createDataFrame(
        [("o1", "c1", "processing", datetime(2026, 1, 1, 10, 0, 0), None)],
        orders_schema,
    )
    order_items = spark.createDataFrame(
        [("o1", 1, "p1", "s1", 50.0, 5.0)],
        ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
    )
    products = spark.createDataFrame([("p1", "cat")], ["product_id", "product_category_name"])
    category_translation = spark.createDataFrame(
        [("cat", "cat_en")], ["product_category_name", "product_category_name_english"]
    )
    sellers = spark.createDataFrame([("s1", "SP")], ["seller_id", "seller_state"])
    customers = spark.createDataFrame([("c1", "SP")], ["customer_id", "customer_state"])

    fact = build_sales_fact(
        orders=orders, order_items=order_items, products=products,
        sellers=sellers, customers=customers, category_translation=category_translation,
    ).collect()

    assert fact[0]["delivery_days"] is None


# ---------------------------------------------------------------------------
# build_customer_features: 재구매 여부 / 누적 지출액
# ---------------------------------------------------------------------------
def test_build_customer_features_marks_repeat_customer(spark):
    fact = spark.createDataFrame(
        [
            ("cu1", "o1", 100.0, datetime(2026, 1, 1)),
            ("cu1", "o2", 50.0, datetime(2026, 2, 1)),   # cu1 2번 주문 -> 재구매 고객
            ("cu2", "o3", 200.0, datetime(2026, 1, 15)),  # cu2 1번 주문
        ],
        ["customer_unique_id", "order_id", "item_total_value", "order_purchase_timestamp"],
    )
    payments = spark.createDataFrame([], "order_id STRING, payment_value DOUBLE")

    features = {
        row["customer_unique_id"]: row
        for row in build_customer_features(fact, payments).collect()
    }

    assert features["cu1"]["order_count"] == 2
    assert features["cu1"]["is_repeat_customer"] is True
    assert features["cu1"]["total_order_value"] == 150.0

    assert features["cu2"]["order_count"] == 1
    assert features["cu2"]["is_repeat_customer"] is False
