"""
POST /ingest/orders로 들어온 주문 이벤트를 스테이징 CSV로 쌓는 모듈.
dags/scripts/incremental_ingest.py와 짝을 이루는 API 쪽 절반이다.

API와 Airflow는 서로 다른 이미지로 빌드되기 때문에 코드를 직접 import하지
않고, data/raw/incremental 경로를 도커 볼륨으로 공유해서 파일로 데이터를
주고받는다. 스테이징 CSV 컬럼 정의는 Airflow 쪽과 반드시 같아야 하니 한쪽을
바꾸면 다른 쪽도 같이 바꿔야 한다.

DATA_ROOT는 도커 컴포즈에서 /app/data로 주입되고, 별도 값이 없으면 그
경로를 기본값으로 쓴다.
"""
import os
import uuid
from datetime import datetime, timezone

import pandas as pd

DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")
INCREMENTAL_DIR = os.path.join(DATA_ROOT, "raw", "incremental")

# dags/scripts/incremental_ingest.py의 STAGED_FILES와 반드시 동일하게 유지한다.
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


def _staged_path(table_key: str) -> str:
    file_name, _ = STAGED_FILES[table_key]
    return os.path.join(INCREMENTAL_DIR, file_name)


def _append_rows(table_key: str, rows: list):
    if not rows:
        return
    os.makedirs(INCREMENTAL_DIR, exist_ok=True)
    path = _staged_path(table_key)
    _, columns = STAGED_FILES[table_key]
    new_df = pd.DataFrame(rows, columns=columns)
    # Airflow 쪽이 병합 후 파일을 "존재하지만 크기 0"으로 비우므로
    # 존재 여부만이 아니라 크기도 같이 봐야 헤더 유무를 정확히 판단한다.
    header = not os.path.exists(path) or os.path.getsize(path) == 0
    new_df.to_csv(path, mode="a", header=header, index=False)


def stage_order_event(order_event: dict) -> dict:
    """주문 이벤트 1건을 orders/order_items/payments 스테이징 CSV에 append.

    order_id를 지정하지 않으면 `stream-<uuid>` 형태로 자동 생성한다.
    Airflow의 extract_task가 다음 배치 실행 시 이 파일을 읽어 원본 CSV에
    병합한다 (dags/scripts/incremental_ingest.merge_staged_sources).
    """
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

    return {"order_id": order_id, "staged_items": len(item_rows), "staged_payment": staged_payment}
