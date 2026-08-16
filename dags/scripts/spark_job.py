"""
Spark ETL 진입점.

DAG에서 Transform과 Aggregation을 별도 Task로 나누기 때문에 이 모듈도
run_transform_job() / run_aggregation_job() 두 개로 쪼갰다. 하나의
SparkSession에서 다 처리하면 더 빠르지만, Task 단위 재시도/모니터링을
살리려면 Task 경계와 Spark 처리 단위를 맞추는 쪽이 낫다.

    run_transform_job()      raw CSV -> 정제 -> fact_sales/dim 조립 -> processed/ Parquet
    run_aggregation_job()    processed/fact_sales -> 집계 마트/윈도우 함수 -> curated/marts, kpis.json
"""
import json
import os

from pyspark.sql import SparkSession

from config.pipeline_config import (
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
    CURATED_DATA_PATH,
    SOURCE_FILES,
    SPARK_APP_NAME,
    SPARK_SHUFFLE_PARTITIONS,
)
from scripts import spark_transformations as tf
from scripts import spark_aggregations as agg
from scripts.logging_utils import get_logger, log_duration

logger = get_logger(__name__)


def _build_spark_session(app_suffix: str) -> SparkSession:
    return (
        SparkSession.builder.appName(f"{SPARK_APP_NAME}_{app_suffix}")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _read_source(spark: SparkSession, table_key: str):
    path = os.path.join(RAW_DATA_PATH, SOURCE_FILES[table_key])
    logger.info("읽는 중: %s (%s)", table_key, path)
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


@log_duration(logger)
def run_transform_job():
    """원본 CSV를 정제해서 fact_sales / dim_* / customer_features를 processed/ 에 저장한다."""
    spark = _build_spark_session("transform")
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("원본 데이터 로드 시작")
        raw = {key: _read_source(spark, key) for key in SOURCE_FILES.keys()}

        logger.info("테이블별 정제 시작")
        customers = tf.clean_customers(raw["customers"])
        sellers = tf.clean_sellers(raw["sellers"])
        products = tf.clean_products(raw["products"])
        orders = tf.clean_orders(raw["orders"])
        order_items = tf.clean_order_items(raw["order_items"])
        payments = tf.clean_payments(raw["payments"])
        category_translation = raw["category_translation"]

        logger.info("fact_sales 조립 시작")
        fact_sales = tf.build_sales_fact(
            orders=orders,
            order_items=order_items,
            products=products,
            sellers=sellers,
            customers=customers,
            category_translation=category_translation,
        )
        fact_sales.cache()
        fact_row_count = fact_sales.count()
        logger.info("fact_sales row count = %d", fact_row_count)

        customer_features = tf.build_customer_features(fact_sales, payments)

        logger.info("processed/ 에 fact_sales Parquet 저장 시작")
        (
            fact_sales.coalesce(2)  # 파일 2개로 병합해서 저장 (파티셔닝은 아직 미적용)
            .write.mode("overwrite")
            .parquet(os.path.join(PROCESSED_DATA_PATH, "fact_sales"))
        )
        customers.write.mode("overwrite").parquet(os.path.join(PROCESSED_DATA_PATH, "dim_customer"))
        products.write.mode("overwrite").parquet(os.path.join(PROCESSED_DATA_PATH, "dim_product"))
        sellers.write.mode("overwrite").parquet(os.path.join(PROCESSED_DATA_PATH, "dim_seller"))
        payments.write.mode("overwrite").parquet(os.path.join(PROCESSED_DATA_PATH, "payments_clean"))
        customer_features.write.mode("overwrite").parquet(
            os.path.join(PROCESSED_DATA_PATH, "customer_features")
        )

        fact_sales.unpersist()
        logger.info("Transform 단계 완료")
        return {"fact_row_count": fact_row_count}

    finally:
        spark.stop()


@log_duration(logger)
def run_aggregation_job():
    """processed/fact_sales 를 다시 읽어서 집계 마트와 핵심 KPI를 curated/ 에 저장한다."""
    spark = _build_spark_session("aggregation")
    spark.sparkContext.setLogLevel("WARN")

    try:
        fact_path = os.path.join(PROCESSED_DATA_PATH, "fact_sales")
        if not os.path.exists(fact_path):
            raise FileNotFoundError(
                f"{fact_path} 가 없습니다. Transform 단계가 먼저 성공해야 합니다."
            )

        fact_sales = spark.read.parquet(fact_path)
        customer_features = spark.read.parquet(os.path.join(PROCESSED_DATA_PATH, "customer_features"))
        orders_status = fact_sales.select("order_id", "order_status").dropDuplicates(["order_id"])
        fact_sales.cache()

        logger.info("집계 마트 생성 시작")
        marts_dir = os.path.join(CURATED_DATA_PATH, "marts")
        os.makedirs(marts_dir, exist_ok=True)

        daily = agg.daily_sales(fact_sales)
        monthly = agg.monthly_sales(fact_sales)
        by_category = agg.category_sales(fact_sales)
        by_region = agg.region_sales(fact_sales)
        by_seller = agg.seller_sales(fact_sales)
        overall_top, best_in_category = agg.top_products(fact_sales)
        hourly_orders = agg.hourly_order_distribution(fact_sales)

        daily.write.mode("overwrite").parquet(os.path.join(marts_dir, "daily_sales"))
        monthly.write.mode("overwrite").parquet(os.path.join(marts_dir, "monthly_sales"))
        by_category.write.mode("overwrite").parquet(os.path.join(marts_dir, "category_sales"))
        by_region.write.mode("overwrite").parquet(os.path.join(marts_dir, "region_sales"))
        by_seller.write.mode("overwrite").parquet(os.path.join(marts_dir, "seller_sales"))
        overall_top.write.mode("overwrite").parquet(os.path.join(marts_dir, "top_products"))
        best_in_category.write.mode("overwrite").parquet(os.path.join(marts_dir, "best_product_per_category"))
        hourly_orders.write.mode("overwrite").parquet(os.path.join(marts_dir, "hourly_orders"))

        logger.info("핵심 KPI 계산 시작")
        kpis = {
            "fact_row_count": fact_sales.count(),
            "repurchase_rate": agg.repurchase_rate(customer_features),
            "avg_order_value": agg.avg_order_value(fact_sales),
            "avg_delivery_time_days": agg.avg_delivery_time(fact_sales),
            "refund_rate": agg.refund_rate(orders_status),
        }
        kpi_path = os.path.join(CURATED_DATA_PATH, "kpis.json")
        with open(kpi_path, "w", encoding="utf-8") as f:
            json.dump(kpis, f, ensure_ascii=False, indent=2)
        logger.info("KPI 저장 완료: %s -> %s", kpi_path, kpis)

        fact_sales.unpersist()
        logger.info("Aggregation 단계 완료")
        return kpis

    finally:
        spark.stop()


if __name__ == "__main__":
    run_transform_job()
    run_aggregation_job()
