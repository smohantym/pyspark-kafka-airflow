# airflow/Dockerfile
FROM apache/airflow:2.9.3-python3.11

# Install Java for PySpark
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so PySpark can find Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Back to airflow user for pip (required by Airflow image)
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
