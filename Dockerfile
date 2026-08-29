FROM apache/airflow:3.3.0

USER root

# Install OpenJDK 17 (required by PySpark to launch JVM gateway)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Copy PySpark and py4j directly from the host .venv (already downloaded — no internet needed)
COPY .venv/Lib/site-packages/pyspark /home/airflow/.local/lib/python3.13/site-packages/pyspark
COPY .venv/Lib/site-packages/py4j /home/airflow/.local/lib/python3.13/site-packages/py4j

# Install remaining lightweight packages (these are small and fast)
RUN pip install --no-cache-dir \
    snowflake-connector-python \
    pymysql \
    requests \
    cryptography \
    python-dotenv \
    apache-airflow-providers-amazon \
    boto3
