"""
Spark 변환 전에 원본 CSV 상태를 먼저 점검하는 단계.

Spark 잡 안에서 이상한 데이터 때문에 조인이 깨지거나 집계가 틀어지면
원인 추적이 번거로워서, 원본 파일 자체가 정상인지부터 빠르게 확인하고
문제가 있으면 여기서 Task를 실패시켜 뒤 단계로 안 넘어가게 한다.

체크: 필수 컬럼(스키마), Null 비율, Primary Key 중복, Foreign Key 무결성.
"""
import os

import pandas as pd

from config.pipeline_config import (
    RAW_DATA_PATH,
    SOURCE_FILES,
    MAX_NULL_RATIO,
    FAIL_ON_MISSING_COLUMNS,
)
from scripts.logging_utils import get_logger, log_duration

logger = get_logger(__name__)


class DataValidationError(Exception):
    """검증 실패 시 발생시켜서 Airflow Task를 확실히 실패 처리하기 위한 예외."""
    pass


# 테이블별로 반드시 있어야 하는 컬럼과, 있다면 유일해야 하는 Primary Key.
REQUIRED_SCHEMA = {
    "customers": {
        "columns": ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
        "primary_key": "customer_id",
    },
    "orders": {
        "columns": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
        "primary_key": "order_id",
    },
    "order_items": {
        "columns": ["order_id", "order_item_id", "product_id", "seller_id", "price"],
        "primary_key": None,  # 복합키(order_id + order_item_id)라 별도 처리
    },
    "payments": {
        "columns": ["order_id", "payment_type", "payment_value"],
        "primary_key": None,
    },
    "products": {
        "columns": ["product_id", "product_category_name"],
        "primary_key": "product_id",
    },
    "sellers": {
        "columns": ["seller_id", "seller_city", "seller_state"],
        "primary_key": "seller_id",
    },
    "reviews": {
        "columns": ["review_id", "order_id", "review_score"],
        "primary_key": "review_id",
    },
}

# 자식 테이블 컬럼 -> (부모 테이블, 부모 키컬럼) 형태의 FK 정의
FOREIGN_KEYS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("payments", "order_id", "orders", "order_id"),
    ("reviews", "order_id", "orders", "order_id"),
]


def _read_csv(table_key: str) -> pd.DataFrame:
    file_name = SOURCE_FILES[table_key]
    path = os.path.join(RAW_DATA_PATH, file_name)
    if not os.path.exists(path):
        raise DataValidationError(
            f"원본 파일이 없습니다: {path} "
            f"(Kaggle Olist 데이터셋을 data/raw/olist 에 두었는지 확인해주세요)"
        )
    return pd.read_csv(path)


def _check_schema(table_key: str, df: pd.DataFrame):
    expected_cols = REQUIRED_SCHEMA[table_key]["columns"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        msg = f"[{table_key}] 필수 컬럼 누락: {missing}"
        if FAIL_ON_MISSING_COLUMNS:
            raise DataValidationError(msg)
        logger.warning(msg)


def _check_null_ratio(table_key: str, df: pd.DataFrame):
    for col in REQUIRED_SCHEMA[table_key]["columns"]:
        if col not in df.columns or len(df) == 0:
            continue
        null_ratio = df[col].isna().mean()
        if null_ratio > MAX_NULL_RATIO:
            raise DataValidationError(
                f"[{table_key}.{col}] null 비율 {null_ratio:.1%} 가 "
                f"허용치 {MAX_NULL_RATIO:.1%} 를 초과했습니다"
            )


def _check_primary_key(table_key: str, df: pd.DataFrame):
    # reviews 테이블은 Olist 원본 데이터 특성상 PK(review_id) 중복이 존재하므로 검사를 건너뜁니다.
    if table_key == "reviews":
        return

    pk = REQUIRED_SCHEMA[table_key]["primary_key"]
    if not pk or pk not in df.columns:
        return
    dup_count = df[pk].duplicated().sum()
    if dup_count > 0:
        raise DataValidationError(
            f"[{table_key}] Primary Key({pk}) 중복 {dup_count}건 발견"
        )


def _check_composite_key(table_key: str, key_columns: list, df: pd.DataFrame):
    dup_count = df.duplicated(subset=key_columns).sum()
    if dup_count > 0:
        raise DataValidationError(
            f"[{table_key}] 복합키({'+'.join(key_columns)}) 중복 {dup_count}건 발견"
        )


def _check_foreign_keys(loaded: dict):
    for child_table, child_col, parent_table, parent_col in FOREIGN_KEYS:
        child_df = loaded.get(child_table)
        parent_df = loaded.get(parent_table)
        if child_df is None or parent_df is None:
            continue
        if child_col not in child_df.columns or parent_col not in parent_df.columns:
            continue

        parent_keys = set(parent_df[parent_col].dropna())
        child_keys = set(child_df[child_col].dropna())
        orphans = child_keys - parent_keys

        if orphans:
            orphan_ratio = len(orphans) / max(len(child_keys), 1)
            sample = list(orphans)[:5]
            msg = (
                f"[{child_table}.{child_col} -> {parent_table}.{parent_col}] "
                f"참조 무결성 위반 {len(orphans)}건 (비율 {orphan_ratio:.1%}), 예: {sample}"
            )
            # FK 위반은 실제 운영 데이터에서도 소량 발생할 수 있어서(취소된 주문 등)
            # 완전히 막지는 않고, 비율이 너무 크면 실패시키는 쪽으로 처리한다.
            if orphan_ratio > 0.10:
                raise DataValidationError(msg)
            logger.warning(msg)


@log_duration(logger)
def run_validation():
    """원본 CSV 전체에 대해 스키마/Null/PK/FK 검증을 수행한다.

    검증에 실패하면 DataValidationError 를 발생시키고, 이는 그대로 Airflow
    PythonOperator를 실패시켜 뒤 단계(Transform, Load)로 진행되지 않게 막는다.
    """
    loaded = {}
    for table_key in REQUIRED_SCHEMA.keys():
        logger.info("검증 중: %s", table_key)
        df = _read_csv(table_key)
        loaded[table_key] = df

        if len(df) == 0:
            raise DataValidationError(f"[{table_key}] 원본 파일에 데이터가 없습니다")

        _check_schema(table_key, df)
        _check_null_ratio(table_key, df)
        _check_primary_key(table_key, df)

    # order_items 는 (order_id, order_item_id) 복합키가 유일해야 함
    if "order_items" in loaded:
        _check_composite_key("order_items", ["order_id", "order_item_id"], loaded["order_items"])

    _check_foreign_keys(loaded)

    logger.info("검증 완료: 총 %d개 테이블, 이상 없음", len(loaded))
    return {table: len(df) for table, df in loaded.items()}


if __name__ == "__main__":
    run_validation()
