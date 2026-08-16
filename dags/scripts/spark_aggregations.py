"""
집계 마트 생성 로직.

fact_sales를 입력으로 받아 대시보드/Tableau에서 바로 쓸 수 있는 요약
테이블을 만든다. row_number(카테고리별 1위 상품), rank/dense_rank(판매자
순위), lag(전일 대비 증감), running total(누적 매출), moving average
(7일 이동평균) 같은 Window Function이 대부분 여기 몰려 있다.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config.pipeline_config import MOVING_AVERAGE_WINDOW_DAYS
from scripts.logging_utils import get_logger

logger = get_logger(__name__)


def daily_sales(fact: DataFrame) -> DataFrame:
    """일별 매출 + 전일 대비 증감(lag) + 누적 매출(running total) + 이동평균."""
    daily = (
        fact.groupBy("order_date")
        .agg(
            F.sum("item_total_value").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy("order_date")
    )

    date_window = Window.orderBy("order_date")
    running_total_window = Window.orderBy("order_date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    moving_avg_window = Window.orderBy("order_date").rowsBetween(
        -(MOVING_AVERAGE_WINDOW_DAYS - 1), Window.currentRow
    )

    daily = (
        daily.withColumn("prev_day_revenue", F.lag("total_revenue", 1).over(date_window))
        .withColumn(
            "revenue_change_pct",
            F.when(
                F.col("prev_day_revenue").isNotNull() & (F.col("prev_day_revenue") != 0),
                (F.col("total_revenue") - F.col("prev_day_revenue")) / F.col("prev_day_revenue") * 100,
            ),
        )
        .withColumn("running_total_revenue", F.sum("total_revenue").over(running_total_window))
        .withColumn(
            "moving_avg_revenue",
            F.avg("total_revenue").over(moving_avg_window),
        )
    )
    return daily


def monthly_sales(fact: DataFrame) -> DataFrame:
    monthly = (
        fact.groupBy("order_year", "order_month")
        .agg(
            F.sum("item_total_value").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy("order_year", "order_month")
    )
    month_window = Window.orderBy("order_year", "order_month")
    monthly = monthly.withColumn(
        "prev_month_revenue", F.lag("total_revenue", 1).over(month_window)
    ).withColumn(
        "revenue_change_pct",
        F.when(
            F.col("prev_month_revenue").isNotNull() & (F.col("prev_month_revenue") != 0),
            (F.col("total_revenue") - F.col("prev_month_revenue")) / F.col("prev_month_revenue") * 100,
        ),
    )
    return monthly


def category_sales(fact: DataFrame) -> DataFrame:
    return (
        fact.groupBy("product_category_name_english")
        .agg(
            F.sum("item_total_value").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.count("*").alias("item_count"),
        )
        .orderBy(F.desc("total_revenue"))
    )


def region_sales(fact: DataFrame) -> DataFrame:
    return (
        fact.groupBy("customer_state")
        .agg(
            F.sum("item_total_value").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("customer_count"),
        )
        .orderBy(F.desc("total_revenue"))
    )


def seller_sales(fact: DataFrame) -> DataFrame:
    """판매자별 매출과 rank()/dense_rank() 순위를 함께 계산한다.

    rank()는 동점자를 건너뛰고 매기고(공동 1위가 2명이면 다음은 3위),
    dense_rank()는 안 건너뛴다(공동 1위 다음이 2위). 둘 다 대시보드에 남겨둠.
    """
    agg = (
        fact.groupBy("seller_id", "seller_state")
        .agg(
            F.sum("item_total_value").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )
    revenue_window = Window.orderBy(F.desc("total_revenue"))
    return (
        agg.withColumn("revenue_rank", F.rank().over(revenue_window))
        .withColumn("revenue_dense_rank", F.dense_rank().over(revenue_window))
        .orderBy("revenue_rank")
    )


def top_products(fact: DataFrame, top_n: int = 10) -> DataFrame:
    """전체 Top N 상품과, 카테고리 내 1위 상품(row_number 활용)을 함께 반환한다."""
    product_agg = (
        fact.groupBy("product_id", "product_category_name_english")
        .agg(
            F.sum("item_total_value").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )

    category_window = Window.partitionBy("product_category_name_english").orderBy(F.desc("total_revenue"))
    product_agg = product_agg.withColumn("rank_in_category", F.row_number().over(category_window))

    overall_top = product_agg.orderBy(F.desc("total_revenue")).limit(top_n)
    best_in_category = product_agg.filter(F.col("rank_in_category") == 1)

    return overall_top, best_in_category


def repurchase_rate(customer_features: DataFrame) -> float:
    total_customers = customer_features.count()
    if total_customers == 0:
        return 0.0
    repeat_customers = customer_features.filter(F.col("is_repeat_customer")).count()
    rate = repeat_customers / total_customers
    logger.info("재구매율: %d / %d = %.2f%%", repeat_customers, total_customers, rate * 100)
    return rate


def avg_order_value(fact: DataFrame) -> float:
    order_totals = fact.groupBy("order_id").agg(F.sum("item_total_value").alias("order_value"))
    result = order_totals.agg(F.avg("order_value").alias("aov")).collect()[0]["aov"]
    return float(result) if result is not None else 0.0


def avg_delivery_time(fact: DataFrame) -> float:
    result = (
        fact.filter(F.col("delivery_days").isNotNull())
        .agg(F.avg("delivery_days").alias("avg_days"))
        .collect()[0]["avg_days"]
    )
    return float(result) if result is not None else 0.0


def refund_rate(orders: DataFrame) -> float:
    """환불률. Olist 데이터셋에는 별도 환불 상태가 없어서 'canceled' 상태를
    환불/취소로 간주해 근사치로 계산한다.
    """
    total = orders.count()
    if total == 0:
        return 0.0
    canceled = orders.filter(F.col("order_status") == "canceled").count()
    return canceled / total


def hourly_order_distribution(fact: DataFrame) -> DataFrame:
    return (
        fact.withColumn("order_hour", F.hour("order_purchase_timestamp"))
        .groupBy("order_hour")
        .agg(F.countDistinct("order_id").alias("order_count"))
        .orderBy("order_hour")
    )
