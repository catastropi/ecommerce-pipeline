"""
Spark 정제/조인/피처 엔지니어링 로직 모음.

spark_job.py가 세션 생성과 읽기/쓰기를 담당하고, "어떻게 정제할지"는
전부 이 파일에 함수 단위로 모아뒀다. 정제 로직만 따로 테스트하기도 쉽다.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from config.pipeline_config import PRICE_OUTLIER_IQR_MULTIPLIER
from scripts.logging_utils import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# 개별 테이블 정제
# ---------------------------------------------------------------------------
def clean_customers(df: DataFrame) -> DataFrame:
    before = df.count()
    cleaned = (
        df.dropna(subset=["customer_id"])
        .dropDuplicates(["customer_id"])
        .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    )
    logger.info("customers 정제: %d -> %d", before, cleaned.count())
    return cleaned


def clean_sellers(df: DataFrame) -> DataFrame:
    before = df.count()
    cleaned = (
        df.dropna(subset=["seller_id"])
        .dropDuplicates(["seller_id"])
        .withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    )
    logger.info("sellers 정제: %d -> %d", before, cleaned.count())
    return cleaned


def clean_products(df: DataFrame) -> DataFrame:
    before = df.count()
    cleaned = (
        df.dropna(subset=["product_id"])
        .dropDuplicates(["product_id"])
        .withColumn(
            "product_category_name",
            F.when(F.col("product_category_name").isNull(), F.lit("unknown"))
            .otherwise(F.trim(F.col("product_category_name"))),
        )
        .withColumn("product_weight_g", F.col("product_weight_g").cast(DoubleType()))
    )
    logger.info("products 정제: %d -> %d", before, cleaned.count())
    return cleaned


def clean_orders(df: DataFrame) -> DataFrame:
    before = df.count()
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    cleaned = df.dropna(subset=["order_id", "customer_id"]).dropDuplicates(["order_id"])
    for c in timestamp_cols:
        if c in cleaned.columns:
            cleaned = cleaned.withColumn(c, F.col(c).cast(TimestampType()))

    cleaned = cleaned.withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    logger.info("orders 정제: %d -> %d", before, cleaned.count())
    return cleaned


def _iqr_bounds(df: DataFrame, column: str, multiplier: float):
    q1, q3 = df.approxQuantile(column, [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr
    return lower, upper


def clean_order_items(df: DataFrame) -> DataFrame:
    """order_items는 fact 테이블 grain이라 가장 신경 써서 정제한다.

    필수 키 null/중복 제거, price·freight_value 캐스팅, price<=0 제거,
    price 상한을 벗어나는 값은 IQR 기준으로 드롭하지 않고 상한값으로
    캡핑한다(드롭하면 매출 총합이 왜곡될 수 있어서 클리핑을 택함).
    """
    before = df.count()
    cleaned = (
        df.dropna(subset=["order_id", "order_item_id", "product_id", "seller_id", "price"])
        .dropDuplicates(["order_id", "order_item_id"])
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
        .filter(F.col("price") > 0)
    )

    lower, upper = _iqr_bounds(cleaned, "price", PRICE_OUTLIER_IQR_MULTIPLIER)
    outlier_count = cleaned.filter(F.col("price") > upper).count()
    if outlier_count:
        logger.warning("order_items price 이상치 %d건을 상한값 %.2f로 캡핑", outlier_count, upper)

    cleaned = cleaned.withColumn(
        "price", F.when(F.col("price") > upper, F.lit(upper)).otherwise(F.col("price"))
    )
    logger.info("order_items 정제: %d -> %d (하한 %.2f / 상한 %.2f)", before, cleaned.count(), lower, upper)
    return cleaned


def clean_payments(df: DataFrame) -> DataFrame:
    before = df.count()
    cleaned = (
        df.dropna(subset=["order_id", "payment_value"])
        .withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
        .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
        .filter(F.col("payment_value") >= 0)
    )
    logger.info("payments 정제: %d -> %d", before, cleaned.count())
    return cleaned


def clean_reviews(df: DataFrame) -> DataFrame:
    before = df.count()
    cleaned = (
        df.dropna(subset=["review_id", "order_id", "review_score"])
        .dropDuplicates(["review_id"])
        .withColumn("review_score", F.col("review_score").cast(IntegerType()))
        .filter((F.col("review_score") >= 1) & (F.col("review_score") <= 5))
    )
    logger.info("reviews 정제: %d -> %d", before, cleaned.count())
    return cleaned


# ---------------------------------------------------------------------------
# Fact 테이블 조립 (order_items grain 기준으로 조인)
# ---------------------------------------------------------------------------
def build_sales_fact(
    orders: DataFrame,
    order_items: DataFrame,
    products: DataFrame,
    sellers: DataFrame,
    customers: DataFrame,
    category_translation: DataFrame,
) -> DataFrame:
    """order_items를 grain으로 두고 나머지 차원을 붙여서 판매 fact를 만든다."""
    products_translated = products.join(category_translation, on="product_category_name", how="left")

    fact = (
        order_items.join(orders, on="order_id", how="inner")
        .join(products_translated, on="product_id", how="left")
        .join(sellers, on="seller_id", how="left")
        .join(customers, on="customer_id", how="left")
    )

    fact = (
        fact.withColumn("order_date", F.to_date("order_purchase_timestamp"))
        .withColumn("order_year", F.year("order_purchase_timestamp"))
        .withColumn("order_month", F.month("order_purchase_timestamp"))
        .withColumn("order_day", F.dayofmonth("order_purchase_timestamp"))
        .withColumn("item_total_value", F.col("price") + F.coalesce(F.col("freight_value"), F.lit(0.0)))
        .withColumn(
            "delivery_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull() & F.col("order_purchase_timestamp").isNotNull(),
                F.datediff("order_delivered_customer_date", "order_purchase_timestamp")
            ).otherwise(F.lit(None)).cast(IntegerType())
        )
    )
    return fact


# ---------------------------------------------------------------------------
# 고객 단위 피처 엔지니어링
# ---------------------------------------------------------------------------
def build_customer_features(fact: DataFrame, payments: DataFrame) -> DataFrame:
    """고객별 총 주문금액, 재구매 여부, 평균 주문금액, 대략적인 CLTV를 계산한다.

    fact는 order_item grain이라 order 단위 금액을 먼저 만든 뒤 고객 단위로
    다시 묶는 2단계 집계가 필요하다.
    """
    order_level = (
        fact.groupBy("customer_unique_id", "order_id")
        .agg(
            F.sum("item_total_value").alias("order_value"),
            F.min("order_purchase_timestamp").alias("order_purchase_timestamp"),
        )
    )

    customer_window = Window.partitionBy("customer_unique_id").orderBy("order_purchase_timestamp")

    order_level = order_level.withColumn(
        "purchase_seq", F.row_number().over(customer_window)
    )

    customer_features = (
        order_level.groupBy("customer_unique_id")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("order_value").alias("total_order_value"),
            F.avg("order_value").alias("avg_order_value"),
            F.min("order_purchase_timestamp").alias("first_purchase_at"),
            F.max("order_purchase_timestamp").alias("last_purchase_at"),
        )
        .withColumn("is_repeat_customer", F.col("order_count") > 1)
        .withColumn("first_purchase_flag", F.col("order_count") == 1)
        # 마진율 데이터가 없어서 누적 지출액을 CLTV 근사치로 사용
        .withColumn("cltv_estimate", F.col("total_order_value"))
    )
    return customer_features
