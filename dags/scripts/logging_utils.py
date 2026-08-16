"""
공통 로깅 설정.

get_logger()로 모듈별 로거를 통일해서 쓰고, 실행 시간을 재고 싶으면
@log_duration 데코레이터만 붙이면 된다.
"""
import functools
import logging
import os
import time

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_configured_loggers = set()


def get_logger(name: str) -> logging.Logger:
    """모듈별 로거를 가져온다. 최초 호출 시 한 번만 핸들러를 붙인다."""
    logger = logging.getLogger(name)

    if name not in _configured_loggers:
        level_name = os.getenv("LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_LOG_FORMAT))

        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False  # Airflow 기본 핸들러와 중복 출력 방지
        _configured_loggers.add(name)

    return logger


def log_duration(logger: logging.Logger = None):
    """함수 실행 시간을 INFO 레벨로 기록하는 데코레이터.

    사용 예:
        @log_duration()
        def run_spark_job():
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log = logger or get_logger(func.__module__)
            start = time.time()
            log.info("[%s] 시작", func.__name__)
            try:
                result = func(*args, **kwargs)
                elapsed = time.time() - start
                log.info("[%s] 완료 (소요 시간 %.2f초)", func.__name__, elapsed)
                return result
            except Exception:
                elapsed = time.time() - start
                log.error("[%s] 실패 (소요 시간 %.2f초)", func.__name__, elapsed, exc_info=True)
                raise
        return wrapper
    return decorator
