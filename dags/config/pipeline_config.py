"""
파이프라인 전역 설정.

루트의 config.yaml(경로/옵션)과 .env(비밀번호 등 민감 정보)를 읽어서
채우고, 둘 다 없으면 기본값으로 동작한다.

이 모듈은 Airflow 컨테이너 안에서든 로컬 파이썬에서든 동일하게 import되므로
외부 의존성은 최소화했다 (PyYAML, python-dotenv 정도).
"""
import os

try:
    import yaml
except ImportError:  # PyYAML 이 없는 환경에서도 기본값으로는 동작하게
    yaml = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# 경로 계산
# ---------------------------------------------------------------------------
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))                       # .../dags/config
BASE_DIR = os.path.dirname(_CURRENT_DIR)                                        # .../dags
PROJECT_ROOT = os.path.dirname(BASE_DIR)                                        # 프로젝트 루트 (/opt/airflow)
CONFIG_YAML_PATH = os.path.join(PROJECT_ROOT, "config.yaml")


def _load_yaml_config():
    if yaml is None or not os.path.exists(CONFIG_YAML_PATH):
        return {}
    with open(CONFIG_YAML_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


_cfg = _load_yaml_config()
_data_lake_cfg = _cfg.get("data_lake", {})
_source_files_cfg = _cfg.get("source_files", {})
_validation_cfg = _cfg.get("validation", {})
_spark_cfg = _cfg.get("spark", {})
_warehouse_cfg = _cfg.get("warehouse", {})
_pipeline_cfg = _cfg.get("pipeline", {})


def _resolve(path_from_config, default_relative):
    rel = path_from_config or default_relative
    # BASE_DIR이 아니라 PROJECT_ROOT 기준으로 잡아서 도커 볼륨 경로(/opt/airflow/data)와 맞춘다.
    return os.path.join(PROJECT_ROOT, rel) if not os.path.isabs(rel) else rel


# ---------------------------------------------------------------------------
# 데이터 레이크 경로 (raw -> processed -> curated -> archive)
# ---------------------------------------------------------------------------
RAW_DATA_PATH = _resolve(_data_lake_cfg.get("raw_dir"), "data/raw/olist")
PROCESSED_DATA_PATH = _resolve(_data_lake_cfg.get("processed_dir"), "data/processed")
CURATED_DATA_PATH = _resolve(_data_lake_cfg.get("curated_dir"), "data/curated")
ARCHIVE_DATA_PATH = _resolve(_data_lake_cfg.get("archive_dir"), "data/archive")

# 개별 원본 CSV 파일명 (Kaggle "Brazilian E-Commerce Public Dataset by Olist")
SOURCE_FILES = {
    "customers": _source_files_cfg.get("customers", "olist_customers_dataset.csv"),
    "orders": _source_files_cfg.get("orders", "olist_orders_dataset.csv"),
    "order_items": _source_files_cfg.get("order_items", "olist_order_items_dataset.csv"),
    "payments": _source_files_cfg.get("payments", "olist_order_payments_dataset.csv"),
    "products": _source_files_cfg.get("products", "olist_products_dataset.csv"),
    "sellers": _source_files_cfg.get("sellers", "olist_sellers_dataset.csv"),
    "reviews": _source_files_cfg.get("reviews", "olist_order_reviews_dataset.csv"),
    "geolocation": _source_files_cfg.get("geolocation", "olist_geolocation_dataset.csv"),
    "category_translation": _source_files_cfg.get(
        "category_translation", "product_category_name_translation.csv"
    ),
}

# ---------------------------------------------------------------------------
# DB 접속 정보. 도커 컴포즈 안에서 도는 컨테이너 기준으로는 host가 "postgres"
# 여야 하고, 로컬 파이썬에서 바로 붙일 때는 "localhost"를 써야 해서 환경변수로 뺐다.
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "dbname": os.getenv("POSTGRES_DB", "ecommerce"),
    "user": os.getenv("POSTGRES_USER", "chris"),
    "password": os.getenv("POSTGRES_PASSWORD", "password"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
}


def get_sqlalchemy_url():
    return (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )


# ---------------------------------------------------------------------------
# 검증 / Spark / Warehouse / 스케줄 관련 옵션
# ---------------------------------------------------------------------------
MAX_NULL_RATIO = float(_validation_cfg.get("max_null_ratio", 0.05))
FAIL_ON_MISSING_COLUMNS = bool(_validation_cfg.get("fail_on_missing_columns", True))

SPARK_APP_NAME = _spark_cfg.get("app_name", "ecommerce_etl")
SPARK_SHUFFLE_PARTITIONS = int(_spark_cfg.get("shuffle_partitions", 8))
PRICE_OUTLIER_IQR_MULTIPLIER = float(_spark_cfg.get("price_outlier_iqr_multiplier", 3.0))
MOVING_AVERAGE_WINDOW_DAYS = int(_spark_cfg.get("moving_average_window_days", 7))

WAREHOUSE_SCHEMA = _warehouse_cfg.get("schema", "public")
MART_SCHEMA = _warehouse_cfg.get("mart_schema", "mart")
LOAD_MODE = _warehouse_cfg.get("load_mode", "upsert")

DEFAULT_RETRIES = int(_pipeline_cfg.get("default_retries", 2))
DEFAULT_RETRY_DELAY_MINUTES = int(_pipeline_cfg.get("default_retry_delay_minutes", 5))
TASK_TIMEOUT_MINUTES = int(_pipeline_cfg.get("task_timeout_minutes", 30))
