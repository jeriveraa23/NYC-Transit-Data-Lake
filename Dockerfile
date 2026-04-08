FROM apache/airflow:2.7.1-python3.10

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir \
    pyspark==3.3.4 \
    boto3 \
    pandas \
    pyarrow \
    fastparquet \
    apache-airflow-providers-amazon \
    requests

COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/

# Ensure scripts have execution permissions
RUN chmod +x /opt/airflow/scripts/*.sh