FROM apache/airflow:2.8.1-python3.10

USER root

# Install jupyter để chạy .ipynb
USER airflow
RUN pip install --user jupyter pandas kafka-python

# Copy script vào container
COPY scripts/ /opt/airflow/scripts/

USER airflow
