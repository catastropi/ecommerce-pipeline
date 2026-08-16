"""
scripts/quality_check.py 테스트.

실제 Postgres 없이 검증하기 위해 create_engine을 몽키패치해서 가짜 커넥션을
주입한다. 목적은 SQL 자체가 아니라 "row count가 min_rows 미만이면 실패한다",
"null key가 있으면 실패한다" 같은 판정 로직이 맞는지 확인하는 것이다.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

import pytest  # noqa: E402

from scripts import quality_check as qc  # noqa: E402


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _FakeConnection:
    """query -> 반환값 매핑을 받아서 execute(text(query))를 흉내낸다."""

    def __init__(self, responses: dict):
        self._responses = responses

    def execute(self, clause, params=None):
        query = str(clause).strip()
        if query not in self._responses:
            raise AssertionError(f"예상하지 못한 쿼리: {query}")
        return _FakeResult(self._responses[query])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    def __init__(self, responses: dict):
        self._responses = responses

    def connect(self):
        return _FakeConnection(self._responses)


def _patch_engine(monkeypatch, responses: dict):
    monkeypatch.setattr(qc, "create_engine", lambda *a, **k: _FakeEngine(responses))


def _healthy_responses(fact_rows=100):
    return {
        "SELECT COUNT(*) FROM fact_sales": fact_rows,
        "SELECT COUNT(*) FROM dim_customer": 10,
        "SELECT COUNT(*) FROM dim_product": 10,
        "SELECT COUNT(*) FROM mart.daily_sales": 5,
        "SELECT COUNT(*) FROM fact_sales WHERE order_id IS NULL": 0,
        "SELECT COUNT(*) FROM fact_sales WHERE product_id IS NULL": 0,
        "SELECT COUNT(*) FROM fact_sales WHERE customer_id IS NULL": 0,
    }


def test_run_quality_check_passes_when_all_counts_healthy(monkeypatch):
    _patch_engine(monkeypatch, _healthy_responses())

    result = qc.run_quality_check()

    assert result["fact_sales"] == 100
    assert result["mart.daily_sales"] == 5


def test_run_quality_check_fails_when_row_count_below_minimum(monkeypatch):
    responses = _healthy_responses()
    responses["SELECT COUNT(*) FROM fact_sales"] = 0
    _patch_engine(monkeypatch, responses)

    with pytest.raises(qc.QualityCheckError, match="fact_sales"):
        qc.run_quality_check()


def test_run_quality_check_fails_when_count_is_none(monkeypatch):
    """테이블 자체가 없어서 COUNT가 None으로 오는 경우도 실패 처리되어야 한다."""
    responses = _healthy_responses()
    responses["SELECT COUNT(*) FROM dim_product"] = None
    _patch_engine(monkeypatch, responses)

    with pytest.raises(qc.QualityCheckError, match="dim_product"):
        qc.run_quality_check()


def test_run_quality_check_fails_when_key_column_has_nulls(monkeypatch):
    responses = _healthy_responses()
    responses["SELECT COUNT(*) FROM fact_sales WHERE customer_id IS NULL"] = 3
    _patch_engine(monkeypatch, responses)

    with pytest.raises(qc.QualityCheckError, match="customer_id"):
        qc.run_quality_check()
