"""
파이프라인 완료/실패 알림.

기본은 로그로 남기고, 환경변수 SLACK_WEBHOOK_URL이 설정되어 있으면
Slack으로도 보낸다. 설정 안 하면 그냥 로그만 찍고 넘어간다.
"""
import os

import requests

from scripts.logging_utils import get_logger

logger = get_logger(__name__)


def _send_slack(message: str):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return
    try:
        requests.post(webhook_url, json={"text": message}, timeout=5)
    except Exception:
        logger.warning("Slack 알림 전송 실패 (파이프라인 자체는 계속 진행)", exc_info=True)


def notify_success(context: dict = None, extra: dict = None):
    dag_id = context.get("dag").dag_id if context and context.get("dag") else "ecommerce_pipeline"
    message = f"✅ [{dag_id}] 파이프라인이 정상적으로 완료됐습니다."
    if extra:
        message += f"\n결과 요약: {extra}"
    logger.info(message)
    _send_slack(message)


def notify_failure(context: dict):
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    dag_id = context.get("dag").dag_id if context.get("dag") else "ecommerce_pipeline"
    exception = context.get("exception")
    message = f"❌ [{dag_id}] Task `{task_id}` 실패\n원인: {exception}"
    logger.error(message)
    _send_slack(message)
