"""
scripts/dw_loader.py 테스트.

실제 Postgres 커넥션이 필요한 부분(upsert_dataframe의 실행 자체)은 CI에서
DB 컨테이너 없이 돌리기 무겁기 때문에 검증하지 않는다. 대신 DB 연결과
분리되어 있는 순수 로직 두 가지를 검증한다:

1. _build_upsert_sql: INSERT ... ON CONFLICT SQL 문자열 생성
2. _build_dim_date: fact_sales의 order_date로부터 날짜 차원 테이블을 만드는 로직
"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from scripts.dw_loader import _build_dim_date, _build_upsert_sql  # noqa: E402


def test_build_upsert_sql_single_conflict_column():
    sql = _build_upsert_sql(
        table="dim_customer",
        columns=["customer_id", "customer_city", "customer_state"],
        conflict_columns=["customer_id"],
        schema="public",
    )

    assert 'INSERT INTO "public"."dim_customer"' in sql
    assert '"customer_id", "customer_city", "customer_state"' in sql
    assert 'ON CONFLICT ("customer_id")' in sql
    assert 'DO UPDATE SET "customer_city" = EXCLUDED."customer_city", ' \
           '"customer_state" = EXCLUDED."customer_state"' in sql


def test_build_upsert_sql_composite_conflict_columns():
    sql = _build_upsert_sql(
        table="fact_sales",
        columns=["order_id", "order_item_id", "price"],
        conflict_columns=["order_id", "order_item_id"],
        schema="public",
    )

    assert 'ON CONFLICT ("order_id", "order_item_id")' in sql
    assert 'DO UPDATE SET "price" = EXCLUDED."price"' in sql


def test_build_upsert_sql_falls_back_to_do_nothing_when_no_update_columns():
    """conflict_columns가 전체 컬럼을 다 덮으면 업데이트할 컬럼이 없으므로 DO NOTHING이어야 한다."""
    sql = _build_upsert_sql(
        table="dim_date",
        columns=["date_id"],
        conflict_columns=["date_id"],
        schema="public",
    )
    assert sql.strip().endswith("DO NOTHING")


def test_build_upsert_sql_without_schema():
    sql = _build_upsert_sql(
        table="dim_seller",
        columns=["seller_id", "seller_city"],
        conflict_columns=["seller_id"],
        schema="",
    )
    assert 'INSERT INTO "dim_seller"' in sql
    assert '"public"' not in sql


def test_build_dim_date_derives_calendar_fields():
    fact_df = pd.DataFrame({
        "order_date": ["2026-08-10", "2026-08-15", "2026-08-10"],  # 2026-08-10 is a Monday
    })

    dim_date = _build_dim_date(fact_df)

    assert len(dim_date) == 2  # 중복 날짜는 하나로 합쳐져야 함
    assert set(dim_date["year"]) == {2026}
    assert set(dim_date["month"]) == {8}

    monday_row = dim_date[dim_date["day"] == 10].iloc[0]
    assert monday_row["day_of_week"] == 1  # 월요일 = 1
    assert bool(monday_row["is_weekend"]) is False

    saturday_row = dim_date[dim_date["day"] == 15].iloc[0]
    assert bool(saturday_row["is_weekend"]) is True  # 2026-08-15 는 토요일


def test_build_dim_date_returns_empty_frame_when_no_order_date_column():
    assert _build_dim_date(pd.DataFrame({"other_col": [1, 2]})).empty


def test_build_dim_date_returns_empty_frame_for_empty_input():
    assert _build_dim_date(pd.DataFrame()).empty
