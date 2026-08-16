"""
data_validation.py 의 순수 로직만 떼어서 테스트한다.

Spark나 실제 Postgres 연결이 필요한 부분(spark_job, dw_loader)은 CI 환경에서
그대로 돌리기엔 무겁기도 하고 Java/DB 컨테이너가 필요해서, 여기서는 pandas로
빠르게 검증 가능한 data_validation 모듈 위주로 테스트를 작성했다.
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from scripts.data_validation import (  # noqa: E402
    DataValidationError,
    _check_schema,
    _check_null_ratio,
    _check_primary_key,
    _check_composite_key,
)


def test_check_schema_passes_when_all_columns_present():
    df = pd.DataFrame({"customer_id": [1, 2], "customer_unique_id": ["a", "b"],
                        "customer_city": ["sp", "rj"], "customer_state": ["SP", "RJ"]})
    _check_schema("customers", df)  # 예외 없이 통과해야 함


def test_check_schema_raises_when_column_missing():
    df = pd.DataFrame({"customer_id": [1, 2]})
    with pytest.raises(DataValidationError):
        _check_schema("customers", df)


def test_check_null_ratio_raises_when_exceeds_threshold():
    # customer_city 의 절반이 null -> 기본 임계치(5%)를 훨씬 초과
    df = pd.DataFrame({
        "customer_id": [1, 2, 3, 4],
        "customer_unique_id": ["a", "b", "c", "d"],
        "customer_city": ["sp", None, "rj", None],
        "customer_state": ["SP", "RJ", "SP", "RJ"],
    })
    with pytest.raises(DataValidationError):
        _check_null_ratio("customers", df)


def test_check_primary_key_detects_duplicates():
    df = pd.DataFrame({"customer_id": [1, 1, 2]})
    with pytest.raises(DataValidationError):
        _check_primary_key("customers", df)


def test_check_composite_key_detects_duplicates():
    df = pd.DataFrame({
        "order_id": ["o1", "o1", "o2"],
        "order_item_id": [1, 1, 1],
    })
    with pytest.raises(DataValidationError):
        _check_composite_key("order_items", ["order_id", "order_item_id"], df)


def test_check_composite_key_passes_when_unique():
    df = pd.DataFrame({
        "order_id": ["o1", "o1", "o2"],
        "order_item_id": [1, 2, 1],
    })
    _check_composite_key("order_items", ["order_id", "order_item_id"], df)  # 예외 없이 통과
