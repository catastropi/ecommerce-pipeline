FROM apache/airflow:2.7.1
USER root
RUN apt-get update && apt-get install -y default-jdk
USER airflow
RUN pip install pyspark psycopg2-binary pandas pyarrow PyYAML python-dotenv requests "SQLAlchemy>=1.4.0,<2.0.0"