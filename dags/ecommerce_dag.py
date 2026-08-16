"""
ecommerce_pipeline DAG

    extract_task        원본 CSV 존재 확인 + 이전 배치 결과 아카이빙
    validate_task        Null / Duplicate / Schema / PK / FK 검증 (실패 시 여기서 중단)
    transform_task        Spark로 정제 + fact/dim 조립
    aggregate_task        Spark로 집계 마트 + KPI 계산 (Window Function 포함)
    warehouse_load_task    Postgres Star Schema에 upsert / mart 재적재
    quality_check_task     적재 후 웨어하우스 상태 재검증
    notify_task            성공 알림 (실패는 on_failure_callback으로 별도 처리)

Task별로 재시도/타임아웃을 따로 가져가고, 실패 시 notifier.notify_failure가
default_args의 on_failure_callback으로 공통 호출된다.
"""
import os
import sys
from datetime import datetime, timedelta

dag_path = os.path.dirname(os.path.abspath(__file__))
if dag_path not in sys.path:
    sys.path.append(dag_path)

from airflow import DAG  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402

from config.pipeline_config import DEFAULT_RETRIES, DEFAULT_RETRY_DELAY_MINUTES, TASK_TIMEOUT_MINUTES  # noqa: E402
from scripts.notifier import notify_failure, notify_success  # noqa: E402


# pyspark 등 무거운 의존성은 DAG 파싱 시점이 아니라 태스크 콜러블 내부에서
# import한다. 스케줄러가 DAG 파일을 주기적으로 재파싱하기 때문.
def extract_task_callable():
    from scripts.extract import run_extract
    return run_extract()


def validate_task_callable():
    from scripts.data_validation import run_validation
    return run_validation()


def transform_task_callable():
    from scripts.spark_job import run_transform_job
    return run_transform_job()


def aggregate_task_callable():
    from scripts.spark_job import run_aggregation_job
    return run_aggregation_job()


def warehouse_load_task_callable():
    from scripts.dw_loader import load_to_postgres
    return load_to_postgres()


def quality_check_task_callable():
    from scripts.quality_check import run_quality_check
    return run_quality_check()


def notify_task_callable(**context):
    ti = context["ti"]
    quality_result = ti.xcom_pull(task_ids="quality_check_task")
    notify_success(context=context, extra=quality_result)


default_args = {
    "owner": "chris",
    "start_date": datetime(2026, 5, 1),
    "retries": DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=DEFAULT_RETRY_DELAY_MINUTES),
    "execution_timeout": timedelta(minutes=TASK_TIMEOUT_MINUTES),
    "on_failure_callback": notify_failure,
}

with DAG(
    "ecommerce_pipeline",
    default_args=default_args,
    description="Olist 이커머스 데이터를 Spark로 정제/집계해 Star Schema에 적재하는 배치 파이프라인",
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "olist", "spark", "postgres"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task_callable,
    )

    validate_task = PythonOperator(
        task_id="validate_task",
        python_callable=validate_task_callable,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task_callable,
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_task",
        python_callable=aggregate_task_callable,
    )

    warehouse_load_task = PythonOperator(
        task_id="warehouse_load_task",
        python_callable=warehouse_load_task_callable,
    )

    quality_check_task = PythonOperator(
        task_id="quality_check_task",
        python_callable=quality_check_task_callable,
    )

    notify_task = PythonOperator(
        task_id="notify_task",
        python_callable=notify_task_callable,
    )

    (
        extract_task
        >> validate_task
        >> transform_task
        >> aggregate_task
        >> warehouse_load_task
        >> quality_check_task
        >> notify_task
    )
