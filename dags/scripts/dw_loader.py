"""
Parquet(Spark 출력) -> PostgreSQL Star Schema 적재.

fact_sales / dim_* / fact_payment는 PK 기준 upsert로 적재해서 재실행해도
중복이 쌓이지 않게 한다. mart.* 아래 집계 테이블은 이번 배치 기준 전체
재계산 값이라 upsert가 의미 없어서 truncate 후 통째로 다시 넣는다.
"""
import json
import os

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

from config.pipeline_config import (
    PROCESSED_DATA_PATH,
    CURATED_DATA_PATH,
    get_sqlalchemy_url,
)
from scripts.logging_utils import get_logger, log_duration

logger = get_logger(__name__)

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(get_sqlalchemy_url(), pool_pre_ping=True)
    return _engine


def _read_parquet(relative_path: str) -> pd.DataFrame:
    full_path = os.path.join(PROCESSED_DATA_PATH, relative_path)
    if not os.path.exists(full_path):
        logger.warning("parquet 경로가 없어서 건너뜁니다: %s", full_path)
        return pd.DataFrame()
    return pd.read_parquet(full_path)


def _read_mart_parquet(relative_path: str) -> pd.DataFrame:
    full_path = os.path.join(CURATED_DATA_PATH, "marts", relative_path)
    if not os.path.exists(full_path):
        logger.warning("mart parquet 경로가 없어서 건너뜁니다: %s", full_path)
        return pd.DataFrame()
    return pd.read_parquet(full_path)


def _build_upsert_sql(table: str, columns: list, conflict_columns: list, schema: str = "public") -> str:
    """upsert_dataframe이 실행할 INSERT ... ON CONFLICT SQL 문자열을 만든다.

    DB 커넥션과 분리해뒀기 때문에 실제 Postgres 없이 tests/test_dw_loader.py에서
    바로 검증할 수 있다.
    """
    update_columns = [c for c in columns if c not in conflict_columns]

    qualified_table = f'"{schema}"."{table}"' if schema else f'"{table}"'
    quoted_columns = ", ".join(f'"{c}"' for c in columns)
    quoted_conflict_columns = ", ".join(f'"{c}"' for c in conflict_columns)
    set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_columns) if update_columns else None

    insert_sql = f'INSERT INTO {qualified_table} ({quoted_columns}) VALUES %s '
    insert_sql += f'ON CONFLICT ({quoted_conflict_columns}) '
    insert_sql += f"DO UPDATE SET {set_clause}" if set_clause else "DO NOTHING"
    return insert_sql


def upsert_dataframe(df: pd.DataFrame, table: str, conflict_columns: list, schema: str = "public") -> int:
    """PK 충돌 시 UPDATE, 아니면 INSERT. 컬럼 순서는 df 기준으로 맞춘다."""
    if df.empty:
        logger.info("[%s] upsert 대상 없음 (빈 DataFrame)", table)
        return 0

    df = df.where(pd.notnull(df), None)  # NaN -> None (psycopg2가 NaN을 못 받음)
    columns = list(df.columns)
    insert_sql = _build_upsert_sql(table, columns, conflict_columns, schema)

    raw_conn = get_engine().raw_connection()
    try:
        with raw_conn.cursor() as cur:
            execute_values(cur, insert_sql, df.to_numpy().tolist(), page_size=1000)
        raw_conn.commit()
    finally:
        raw_conn.close()

    logger.info("[%s] upsert 완료: %d행", table, len(df))
    return len(df)


def _refresh_table(df: pd.DataFrame, table: str, schema: str) -> int:
    """마트 테이블은 배치마다 통째로 재계산된 스냅샷이라 truncate 후 재적재한다.
    category_sales처럼 "지금까지 전체 매출을 다시 합산한 값"은 이전 배치 결과와
    행 단위로 병합할 이유가 없다.
    """
    if df.empty:
        logger.info("[%s.%s] 재적재 대상 없음", schema, table)
        return 0

    engine = get_engine()
    qualified = f'{schema}.{table}' if schema else table
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {qualified}"))
        df.to_sql(table, con=conn, schema=schema, if_exists="append", index=False, method="multi")

    logger.info("[%s] 재적재 완료: %d행", qualified, len(df))
    return len(df)


def _build_dim_date(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty or "order_date" not in fact_df.columns:
        return pd.DataFrame()
    dates = pd.to_datetime(fact_df["order_date"].dropna().unique())
    dim_date = pd.DataFrame({"date_id": dates})
    dim_date["year"] = dim_date["date_id"].dt.year
    dim_date["month"] = dim_date["date_id"].dt.month
    dim_date["day"] = dim_date["date_id"].dt.day
    dim_date["day_of_week"] = dim_date["date_id"].dt.dayofweek + 1  # 1=월요일
    dim_date["is_weekend"] = dim_date["day_of_week"].isin([6, 7])
    return dim_date


@log_duration(logger)
def load_dimensions_and_fact():
    """dim_customer / dim_product / dim_seller / dim_date / fact_payment / fact_sales 를 upsert 한다."""
    customers = _read_parquet("dim_customer")
    products = _read_parquet("dim_product")
    sellers = _read_parquet("dim_seller")
    payments = _read_parquet("payments_clean")
    fact = _read_parquet("fact_sales")
    customer_features = _read_parquet("customer_features")

    if not customers.empty:
        customer_cols = [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ]
        upsert_dataframe(
            customers[customer_cols],
            table="dim_customer",
            conflict_columns=["customer_id"],
        )

    if not products.empty:
        cols = [
            "product_id", "product_category_name", "product_category_name_english",
            "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
        ]
        upsert_dataframe(products[[c for c in cols if c in products.columns]], "dim_product", ["product_id"])

    if not sellers.empty:
        upsert_dataframe(
            sellers[["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]],
            table="dim_seller",
            conflict_columns=["seller_id"],
        )

    dim_date = _build_dim_date(fact)
    if not dim_date.empty:
        upsert_dataframe(dim_date, table="dim_date", conflict_columns=["date_id"])

    if not payments.empty:
        payment_cols = ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
        upsert_dataframe(
            payments[[c for c in payment_cols if c in payments.columns]],
            table="fact_payment",
            conflict_columns=["order_id", "payment_sequential"],
        )

    if not fact.empty:
        fact_cols = [
            "order_id", "order_item_id", "customer_id", "product_id", "seller_id",
            "order_date", "order_status", "price", "freight_value", "item_total_value", "delivery_days",
        ]
        upsert_dataframe(fact[[c for c in fact_cols if c in fact.columns]], "fact_sales", ["order_id", "order_item_id"])

    if not customer_features.empty:
        upsert_dataframe(customer_features, "customer_features", ["customer_unique_id"])

    return {
        "customers": len(customers),
        "products": len(products),
        "sellers": len(sellers),
        "fact_rows": len(fact),
    }


@log_duration(logger)
def load_marts():
    """curated/marts 아래의 Spark 집계 결과를 mart 스키마 테이블로 재적재한다."""
    mart_tables = {
        "daily_sales": "daily_sales",
        "monthly_sales": "monthly_sales",
        "category_sales": "category_sales",
        "region_sales": "region_sales",
        "seller_sales": "seller_sales",
        "top_products": "top_products",
        "best_product_per_category": "best_product_per_category",
        "hourly_orders": "hourly_orders",
    }
    loaded_rows = {}
    for parquet_name, table_name in mart_tables.items():
        df = _read_mart_parquet(parquet_name)
        loaded_rows[table_name] = _refresh_table(df, table_name, schema="mart")

    kpi_path = os.path.join(CURATED_DATA_PATH, "kpis.json")
    if os.path.exists(kpi_path):
        with open(kpi_path, "r", encoding="utf-8") as f:
            kpis = json.load(f)
        kpi_df = pd.DataFrame([kpis])
        with get_engine().begin() as conn:
            kpi_df.to_sql("kpi_snapshot", con=conn, schema="mart", if_exists="append", index=False)
        logger.info("KPI 스냅샷 적재 완료: %s", kpis)

    return loaded_rows


def load_to_postgres():
    """Airflow 태스크에서 호출하는 진입점."""
    fact_result = load_dimensions_and_fact()
    mart_result = load_marts()
    return {"fact": fact_result, "marts": mart_result}


if __name__ == "__main__":
    load_to_postgres()
