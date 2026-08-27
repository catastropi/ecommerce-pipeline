"""
DAG의 첫 번째 단계.

Kaggle Olist 데이터셋은 고정된 정적 파일이라 여기서 "추출"은 실질적으로
1) raw 경로에 원본 CSV가 실제로 있는지 확인하고 2) 이전 배치의 curated
결과를 archive/로 옮겨 이력을 남기는 일만 한다.

extract 단계 맨 앞에서 incremental_ingest.merge_staged_sources()를 호출해서
API(`POST /ingest/orders`)로 들어온 주문 이벤트가 있으면 원본 CSV에 먼저
병합한다. 스테이징 파일이 비어있으면 바로 지나가므로 기존 배치 동작에는
영향 없음.
"""
import os
import shutil
from datetime import datetime

from config.pipeline_config import RAW_DATA_PATH, CURATED_DATA_PATH, ARCHIVE_DATA_PATH, SOURCE_FILES
from scripts.incremental_ingest import merge_staged_sources
from scripts.logging_utils import get_logger, log_duration

logger = get_logger(__name__)


class ExtractError(Exception):
    pass


@log_duration(logger)
def run_extract():
    incremental_merged = merge_staged_sources()
    if incremental_merged:
        logger.info("API로 유입된 주문 이벤트를 원본 CSV에 병합했습니다: %s", incremental_merged)

    missing = []
    for table_key, file_name in SOURCE_FILES.items():
        path = os.path.join(RAW_DATA_PATH, file_name)
        if not os.path.exists(path):
            missing.append(path)

    if missing:
        raise ExtractError(
            "다음 원본 CSV 파일을 찾을 수 없습니다. Kaggle에서 받은 Olist 데이터셋을 "
            f"data/raw/olist 아래에 두었는지 확인해주세요: {missing}"
        )

    _archive_previous_batch()
    logger.info("Extract 단계 완료: 원본 CSV %d개 확인", len(SOURCE_FILES))
    return {"source_files_checked": len(SOURCE_FILES), "incremental_merged": incremental_merged}


def _archive_previous_batch(keep_last_n=5):
    marts_dir = os.path.join(CURATED_DATA_PATH, "marts")
    if not os.path.exists(marts_dir) or not os.listdir(marts_dir):
        logger.info("이전 배치 결과가 없어 archive 단계를 건너뜁니다 (최초 실행)")
        return

    batch_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_target = os.path.join(ARCHIVE_DATA_PATH, f"marts_{batch_stamp}")
    os.makedirs(ARCHIVE_DATA_PATH, exist_ok=True)

    shutil.copytree(marts_dir, archive_target)
    logger.info("이전 배치 curated/marts 를 archive로 백업: %s", archive_target)

    try:
        archived_dirs = sorted([
            os.path.join(ARCHIVE_DATA_PATH, d)
            for d in os.listdir(ARCHIVE_DATA_PATH)
            if d.startswith("marts_") and os.path.isdir(os.path.join(ARCHIVE_DATA_PATH, d))
        ])

        if len(archived_dirs) > keep_last_n:
            dirs_to_delete = archived_dirs[:-keep_last_n]
            for old_dir in dirs_to_delete:
                shutil.rmtree(old_dir)
                logger.info("오래된 아카이브 자동 삭제 완료: %s", old_dir)

    except Exception as e:
        # 정리 작업 중 에러가 발생해도 메인 파이프라인(Extract)이 실패하지 않도록 로깅만 처리
        logger.warning("아카이브 정리 중 오류가 발생했습니다: %s", e)


if __name__ == "__main__":
    run_extract()
