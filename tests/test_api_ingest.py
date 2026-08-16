"""
api/main.py 의 POST /ingest/orders 엔드포인트 테스트.

이 엔드포인트는 DB에 접근하지 않고 파일 스테이징만 하므로, Postgres 없이도
FastAPI TestClient로 바로 검증할 수 있다. main.py가 StaticFiles(directory="static")
를 프로세스 working directory 기준 상대경로로 마운트하기 때문에, import 시점에
api/ 디렉터리로 cwd를 옮겨야 한다(실제 배포 환경 - uvicorn을 api/에서 실행하거나
Docker WORKDIR /app에 api/ 내용이 그대로 있는 것 - 과 동일한 조건을 맞추는 것).
"""
import importlib
import os
import sys

import pytest

API_DIR = os.path.join(os.path.dirname(__file__), "..", "api")


@pytest.fixture
def api_client(tmp_path, monkeypatch):
    monkeypatch.chdir(API_DIR)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    sys.path.insert(0, os.path.abspath(API_DIR))

    import incremental_ingest
    importlib.reload(incremental_ingest)  # DATA_ROOT를 새로 읽도록 재로드

    import main
    importlib.reload(main)

    from fastapi.testclient import TestClient
    yield TestClient(main.app), incremental_ingest, tmp_path

    sys.path.remove(os.path.abspath(API_DIR))


def _valid_payload(**overrides):
    payload = {
        "customer_id": "cust-001",
        "items": [
            {"product_id": "prod-a", "seller_id": "seller-a", "price": 99.9, "freight_value": 15.0},
        ],
        "payment": {"payment_type": "credit_card", "payment_installments": 2, "payment_value": 99.9},
    }
    payload.update(overrides)
    return payload


def test_ingest_order_returns_201_and_generated_order_id(api_client):
    client, _, _ = api_client
    response = client.post("/ingest/orders", json=_valid_payload())

    assert response.status_code == 201
    body = response.json()
    assert body["order_id"].startswith("stream-")
    assert body["staged_items"] == 1
    assert body["staged_payment"] is True


def test_ingest_order_stages_file_under_data_root(api_client):
    import pandas as pd

    client, _, tmp_path = api_client
    client.post("/ingest/orders", json=_valid_payload(order_id="fixed-id-1"))

    staged_path = tmp_path / "raw" / "incremental" / "orders_stream.csv"
    assert staged_path.exists()
    df = pd.read_csv(staged_path)
    assert df.iloc[0]["order_id"] == "fixed-id-1"


def test_ingest_order_rejects_empty_items(api_client):
    client, _, _ = api_client
    payload = _valid_payload()
    payload["items"] = []

    response = client.post("/ingest/orders", json=payload)
    assert response.status_code == 422  # Pydantic min_length=1 검증에서 걸려야 함


def test_ingest_order_rejects_missing_customer_id(api_client):
    client, _, _ = api_client
    payload = _valid_payload()
    del payload["customer_id"]

    response = client.post("/ingest/orders", json=payload)
    assert response.status_code == 422


def test_ingest_order_rejects_non_positive_price(api_client):
    client, _, _ = api_client
    payload = _valid_payload()
    payload["items"][0]["price"] = 0

    response = client.post("/ingest/orders", json=payload)
    assert response.status_code == 422


def test_ingest_order_without_payment_is_optional(api_client):
    client, _, _ = api_client
    payload = _valid_payload()
    del payload["payment"]

    response = client.post("/ingest/orders", json=payload)
    assert response.status_code == 201
    assert response.json()["staged_payment"] is False


def test_root_endpoint_does_not_require_db(api_client):
    client, _, _ = api_client
    response = client.get("/")
    assert response.status_code == 200
