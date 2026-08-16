"""
scripts/incremental_ingest.py 테스트.

실제 data/raw/olist 원본 CSV를 절대 건드리지 않도록, 매 테스트마다
tmp_path 아래 임시 raw/incremental 디렉터리로 모듈 전역 경로를
monkeypatch 한다 (원본 CSV를 실수로 덮어쓰면 로컬 개발 데이터가 날아간다).
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from scripts import incremental_ingest as inc  # noqa: E402


@pytest.fixture
def isolated_paths(tmp_path, monkeypatch):
    """RAW_DATA_PATH / INCREMENTAL_DIR 를 임시 디렉터리로 바꿔치기."""
    raw_dir = tmp_path / "raw" / "olist"
    incremental_dir = tmp_path / "raw" / "incremental"
    raw_dir.mkdir(parents=True)

    monkeypatch.setattr(inc, "RAW_DATA_PATH", str(raw_dir))
    monkeypatch.setattr(inc, "INCREMENTAL_DIR", str(incremental_dir))
    monkeypatch.setattr(inc, "SOURCE_FILES", {
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
    })
    return raw_dir, incremental_dir


def _sample_event(order_id=None):
    return {
        "order_id": order_id,
        "customer_id": "cust-001",
        "order_status": "created",
        "order_purchase_timestamp": "2026-08-10 12:00:00",
        "items": [
            {"product_id": "prod-a", "seller_id": "seller-a", "price": 100.0, "freight_value": 10.0},
            {"product_id": "prod-b", "seller_id": "seller-b", "price": 50.0},
        ],
        "payment": {"payment_type": "credit_card", "payment_installments": 3, "payment_value": 160.0},
    }


def test_stage_order_event_writes_all_three_staging_files(isolated_paths):
    raw_dir, incremental_dir = isolated_paths

    result = inc.stage_order_event(_sample_event())

    assert result["staged_items"] == 2
    assert result["staged_payment"] is True
    assert result["order_id"].startswith("stream-")

    orders_df = pd.read_csv(incremental_dir / "orders_stream.csv")
    items_df = pd.read_csv(incremental_dir / "order_items_stream.csv")
    payments_df = pd.read_csv(incremental_dir / "payments_stream.csv")

    assert len(orders_df) == 1
    assert len(items_df) == 2
    assert len(payments_df) == 1
    assert list(items_df["order_item_id"]) == [1, 2]


def test_stage_order_event_uses_provided_order_id(isolated_paths):
    result = inc.stage_order_event(_sample_event(order_id="my-order-123"))
    assert result["order_id"] == "my-order-123"


def test_stage_order_event_without_payment_is_optional(isolated_paths):
    event = _sample_event()
    event["payment"] = None
    result = inc.stage_order_event(event)
    assert result["staged_payment"] is False


def test_has_staged_data_false_when_nothing_staged(isolated_paths):
    assert inc.has_staged_data() is False


def test_merge_staged_sources_noop_when_nothing_staged(isolated_paths):
    """스테이징 파일이 없으면 raw CSV를 전혀 건드리지 않아야 한다 (기존 배치 동작 보존)."""
    raw_dir, _ = isolated_paths
    assert inc.merge_staged_sources() == {}
    assert list(raw_dir.iterdir()) == []


def test_merge_staged_sources_appends_into_existing_raw_csv(isolated_paths):
    raw_dir, _ = isolated_paths

    existing_orders = pd.DataFrame([
        {"order_id": "existing-1", "customer_id": "c1", "order_status": "delivered",
         "order_purchase_timestamp": "2026-01-01 00:00:00", "order_approved_at": None,
         "order_delivered_carrier_date": None, "order_delivered_customer_date": None,
         "order_estimated_delivery_date": None},
    ])
    existing_orders.to_csv(raw_dir / "olist_orders_dataset.csv", index=False)

    inc.stage_order_event(_sample_event(order_id="new-order-1"))
    merged = inc.merge_staged_sources()

    assert merged["orders"] == 1
    assert merged["order_items"] == 2
    assert merged["payments"] == 1

    result_orders = pd.read_csv(raw_dir / "olist_orders_dataset.csv")
    assert set(result_orders["order_id"]) == {"existing-1", "new-order-1"}


def test_merge_staged_sources_creates_raw_csv_when_missing(isolated_paths):
    """raw CSV가 아직 없는 경우(최초 실행)에도 병합이 CSV를 새로 만들어야 한다."""
    raw_dir, _ = isolated_paths
    inc.stage_order_event(_sample_event(order_id="first-order"))

    merged = inc.merge_staged_sources()

    assert merged["orders"] == 1
    assert (raw_dir / "olist_orders_dataset.csv").exists()


def test_merge_staged_sources_is_idempotent(isolated_paths):
    """병합 후 스테이징 파일이 비워지므로, 다시 호출해도 중복 병합되면 안 된다."""
    raw_dir, _ = isolated_paths
    inc.stage_order_event(_sample_event(order_id="dup-check"))

    first = inc.merge_staged_sources()
    second = inc.merge_staged_sources()

    assert first["orders"] == 1
    assert second == {}  # 스테이징 파일이 비어있으므로 아무 것도 병합되지 않아야 함

    result_orders = pd.read_csv(raw_dir / "olist_orders_dataset.csv")
    assert len(result_orders[result_orders["order_id"] == "dup-check"]) == 1


def test_merge_staged_sources_dedupes_on_conflict_key(isolated_paths):
    """같은 order_id로 두 번 이벤트가 들어와도 병합 후에는 최신 값 1건만 남아야 한다."""
    raw_dir, _ = isolated_paths
    inc.stage_order_event(_sample_event(order_id="dedupe-me"))
    inc.merge_staged_sources()

    # 같은 order_id로 상태만 다르게 다시 스테이징 (예: created -> shipped 업데이트 이벤트)
    second_event = _sample_event(order_id="dedupe-me")
    second_event["order_status"] = "shipped"
    inc.stage_order_event(second_event)
    inc.merge_staged_sources()

    result_orders = pd.read_csv(raw_dir / "olist_orders_dataset.csv")
    matching = result_orders[result_orders["order_id"] == "dedupe-me"]
    assert len(matching) == 1
    assert matching.iloc[0]["order_status"] == "shipped"
