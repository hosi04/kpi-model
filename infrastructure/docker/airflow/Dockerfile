FROM apache/airflow:3.1.2
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl vim \
    && rm -rf /var/lib/apt/lists/*
# Create required directories
RUN mkdir -p /opt/airflow/logs \
    && chown -R airflow:0 /opt/airflow/logs \
    && chmod -R 777 /opt/airflow/logs
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
# Copy .env file to the container
COPY .env /opt/airflow/.env