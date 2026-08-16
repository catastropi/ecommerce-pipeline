"""
tests/ 전체에서 공유하는 pytest fixture.

spark fixture를 test_spark_transformations.py / test_spark_aggregations.py에
각각 따로 정의해뒀었는데, 파일 안에 로컬로 정의한 fixture는 이름과 scope가
같아도 파일별로 별개 취급된다. 그래서 pytest tests/ 로 두 파일을 한 프로세스에서
같이 돌리면, 뒤에 실행되는 파일의 fixture가 SparkSession.builder.getOrCreate()를
호출할 때 이미 앞 파일이 띄워둔 SparkContext를 그대로 재사용하게 되면서(설정은
무시되고 경고만 찍힘) 리소스 충돌/불안정 실행의 원인이 될 수 있다. 파일 하나씩
디버그로 실행할 때는 이 충돌이 애초에 안 생기니까 문제 없이 잘 되는 것처럼 보인다.

conftest.py로 fixture를 하나로 합쳐두면, 몇 개 파일에서 spark를 쓰든 세션 전체에서
SparkSession이 딱 하나만 뜬다.
"""
import os
import sys

import pytest


@pytest.fixture(scope="session")
def spark():
    # Windows(특히 conda 환경)에서 Spark가 액션 실행 시 별도로 띄우는 Python
    # 워커가 PATH의 'python' 별칭(Microsoft Store stub)에 걸려 죽는 문제 방지.
    # 드라이버와 동일한 인터프리터(sys.executable)로 워커를 강제 고정한다.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pipeline_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()
