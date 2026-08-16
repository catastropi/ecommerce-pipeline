"""
주문 이벤트를 스테이징하고, 배치 파이프라인의 extract 단계에서 원본
CSV(raw)에 병합하는 모듈.

Kaggle Olist 데이터셋은 고정 파일이라 실시간 수집을 보여줄 방법이 없어서,
FastAPI 쪽 `POST /ingest/orders` 엔드포인트(api/incremental_ingest.py)로
들어온 이벤트를 `data/raw/incremental/`에 CSV로 쌓아뒀다가 다음 배치의
extract_task가 원본 CSV에 병합하는 식으로 만들었다.

API와 Airflow는 서로 다른 이미지라 코드를 직접 import하지 않고, 같은
data/raw/incremental 경로를 도커 볼륨으로 공유해서 파일로 주고받는다.
스테이징 파일이 비어있으면 merge_staged_sources()가 바로 빈 dict를
반환하니, API 이벤트가 없는 상황에서는 기존 정적 배치와 동일하게 돈다.
"""
import os
import uuid
from datetime import datetime, timezone

import pandas as pd

from config.pipeline_config import RAW_DATA_PATH, SOURCE_FILES, PROJECT_ROOT
from scripts.logging_utils import get_logger

logger = get_logger(__name__)

INCREMENTAL_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "incremental")

# table_key -> (스테이징 파일명, 원본 CSV와 동일한 컬럼 순서)
STAGED_FILES = {
    "orders": (
        "orders_stream.csv",
        [
            "order_id", "customer_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ],
    ),
    "order_items": (
        "order_items_stream.csv",
        ["order_id", "order_item_id", "product_id", "seller_id",
         "shipping_limit_date", "price", "freight_value"],
    ),
    "payments": (
        "payments_stream.csv",
        ["order_id", "payment_sequential", "payment_type",
         "payment_installments", "payment_value"],
    ),
}

# 원본 CSV와 병합할 때 중복 제거 기준이 되는 키 (마지막 값 우선)
MERGE_KEYS = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
}


def _staged_path(table_key: str) -> str:
    file_name, _ = STAGED_FILES[table_key]
    return os.path.join(INCREMENTAL_DIR, file_name)


def stage_order_event(order_event: dict) -> dict:
    """API에서 받은 주문 이벤트 1건을 orders/order_items/payments 스테이징 CSV에 append.

    order_event 형태 (api.schemas.OrderEventIn):
        {
            "order_id": Optional[str],
            "customer_id": str,
            "order_status": Optional[str],
            "order_purchase_timestamp": Optional[str],
            "items": [{"product_id": str, "seller_id": str, "price": float,
                       "freight_value": Optional[float]}, ...],
            "payment": Optional[{"payment_type": str, "payment_installments": Optional[int],
                                  "payment_value": float}],
        }
    """
    os.makedirs(INCREMENTAL_DIR, exist_ok=True)

    order_id = order_event.get("order_id") or f"stream-{uuid.uuid4().hex[:24]}"
    now_utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    purchase_ts = order_event.get("order_purchase_timestamp") or now_utc_naive.isoformat(sep=" ", timespec="seconds")

    order_row = {
        "order_id": order_id,
        "customer_id": order_event["customer_id"],
        "order_status": order_event.get("order_status", "created"),
        "order_purchase_timestamp": purchase_ts,
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": None,
    }
    _append_rows("orders", [order_row])

    item_rows = []
    for idx, item in enumerate(order_event.get("items", []), start=1):
        item_rows.append({
            "order_id": order_id,
            "order_item_id": idx,
            "product_id": item["product_id"],
            "seller_id": item["seller_id"],
            "shipping_limit_date": item.get("shipping_limit_date"),
            "price": item["price"],
            "freight_value": item.get("freight_value", 0.0),
        })
    _append_rows("order_items", item_rows)

    payment = order_event.get("payment")
    staged_payment = False
    if payment:
        payment_row = {
            "order_id": order_id,
            "payment_sequential": payment.get("payment_sequential", 1),
            "payment_type": payment["payment_type"],
            "payment_installments": payment.get("payment_installments", 1),
            "payment_value": payment["payment_value"],
        }
        _append_rows("payments", [payment_row])
        staged_payment = True

    logger.info("주문 이벤트 스테이징 완료: order_id=%s, items=%d", order_id, len(item_rows))
    return {"order_id": order_id, "staged_items": len(item_rows), "staged_payment": staged_payment}


def _append_rows(table_key: str, rows: list):
    if not rows:
        return
    path = _staged_path(table_key)
    _, columns = STAGED_FILES[table_key]
    new_df = pd.DataFrame(rows, columns=columns)
    # 병합 후 파일이 "존재하지만 크기 0"으로 남기 때문에 존재 여부만으로
    # 헤더 유무를 판단하면 안 되고 크기도 같이 봐야 한다.
    header = not os.path.exists(path) or os.path.getsize(path) == 0
    new_df.to_csv(path, mode="a", header=header, index=False)


def has_staged_data() -> bool:
    return any(
        os.path.exists(_staged_path(k)) and os.path.getsize(_staged_path(k)) > 0
        for k in STAGED_FILES
    )


def merge_staged_sources() -> dict:
    """스테이징된 이벤트를 raw CSV(data/raw/olist)에 병합하고 스테이징 파일을 비운다.

    extract_task에서 원본 CSV 존재 확인 직전에 호출된다. 반환값은
    {"orders": 3, "order_items": 5, ...} 형태로 테이블별 병합 행 수.
    """
    if not has_staged_data():
        return {}

    merged = {}
    for table_key, (_, columns) in STAGED_FILES.items():
        staged_path = _staged_path(table_key)
        if not os.path.exists(staged_path) or os.path.getsize(staged_path) == 0:
            continue

        staged_df = pd.read_csv(staged_path)
        if staged_df.empty:
            continue

        raw_path = os.path.join(RAW_DATA_PATH, SOURCE_FILES[table_key])
        if os.path.exists(raw_path):
            raw_df = pd.read_csv(raw_path)
        else:
            raw_df = pd.DataFrame(columns=columns)

        combined = pd.concat([raw_df, staged_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=MERGE_KEYS[table_key], keep="last")
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        combined.to_csv(raw_path, index=False)

        merged[table_key] = len(staged_df)
        # 병합 완료 후 스테이징 파일을 비워 다음 배치에서 같은 이벤트가 중복 병합되지 않게 한다.
        open(staged_path, "w").close()
        logger.info("[%s] 스테이징된 %d건을 raw CSV에 병합 완료 (%s)", table_key, len(staged_df), raw_path)

    return merged


if __name__ == "__main__":
    print(merge_staged_sources())
