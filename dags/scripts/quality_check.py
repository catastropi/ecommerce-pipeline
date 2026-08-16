"""
적재가 끝난 뒤 웨어하우스 상태를 한번 더 확인하는 단계.

data_validation.py 가 "원본 CSV가 이상 없는지"를 봤다면, 여기서는
"Postgres에 실제로 잘 들어갔는지"를 확인한다. Spark 단계에서는 멀쩡했는데
적재 커넥션이 끊기거나 일부 배치만 반영되는 경우를 잡기 위한 용도다.
"""
from sqlalchemy import create_engine, text

from config.pipeline_config import get_sqlalchemy_url
from scripts.logging_utils import get_logger, log_duration

logger = get_logger(__name__)


class QualityCheckError(Exception):
    pass


CHECKS = [
    ("fact_sales", "SELECT COUNT(*) FROM fact_sales", 1),
    ("dim_customer", "SELECT COUNT(*) FROM dim_customer", 1),
    ("dim_product", "SELECT COUNT(*) FROM dim_product", 1),
    ("mart.daily_sales", "SELECT COUNT(*) FROM mart.daily_sales", 1),
]

NULL_KEY_CHECKS = [
    ("fact_sales", "order_id"),
    ("fact_sales", "product_id"),
    ("fact_sales", "customer_id"),
]


@log_duration(logger)
def run_quality_check():
    engine = create_engine(get_sqlalchemy_url(), pool_pre_ping=True)
    results = {}

    with engine.connect() as conn:
        for label, query, min_rows in CHECKS:
            count = conn.execute(text(query)).scalar()
            results[label] = count
            logger.info("[%s] row count = %s", label, count)
            if count is None or count < min_rows:
                raise QualityCheckError(f"[{label}] 최소 {min_rows}행이 필요한데 {count}행 밖에 없습니다")

        for table, column in NULL_KEY_CHECKS:
            null_count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
            ).scalar()
            if null_count:
                raise QualityCheckError(f"[{table}.{column}] 핵심 키 컬럼에 null이 {null_count}건 있습니다")

    logger.info("품질 검사 통과: %s", results)
    return results


if __name__ == "__main__":
    run_quality_check()
