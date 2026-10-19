FROM apache/airflow:3.3.2

USER root

# Install Java required by Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install project dependencies
RUN pip install --no-cache-dir \
    pyspark==4.2.0 \
    dbt-core==1.12.5 \
    dbt-postgres==1.11.0 \
    boto3 \
    requests \
    psycopg2-binary
