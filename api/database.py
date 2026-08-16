"""
API 서버용 DB 커넥션.

Airflow쪽 config(dags/config/pipeline_config.py)를 그대로 가져다 쓸 수도
있지만, FastAPI는 별도 컨테이너로 뜨는 게 자연스러워서 여기서는 필요한
접속 정보만 환경변수로 따로 읽는다. 접속 대상 DB는 동일한 Postgres다.
"""
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv(
    "API_DB_URL",
    "postgresql+psycopg2://chris:password@postgres:5432/ecommerce",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5, max_overflow=10)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """FastAPI Depends() 로 주입할 세션. 요청이 끝나면 항상 닫히도록 try/finally로 감싼다."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
