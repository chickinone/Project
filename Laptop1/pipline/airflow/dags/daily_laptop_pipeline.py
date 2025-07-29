from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'phantruong',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_laptop_pipeline',
    default_args=default_args,
    description='Crawl → Clean → Kafka daily pipeline',
    schedule_interval='@daily',  
    start_date=datetime(2025, 7, 12),
    catchup=False,
    tags=['laptop', 'pipeline'],
) as dag:

    crawl_task = BashOperator(
        task_id='crawl_data',
        bash_command='python D:/Laptop1/Data/Crawl.py'
    )

    clean_task = BashOperator(
        task_id='clean_data',
        bash_command='jupyter nbconvert --to notebook --execute D:/Laptop1/Clean_data/data.ipynb --inplace'
    )

    kafka_task = BashOperator(
        task_id='send_to_kafka',
        bash_command='python D:/Laptop1/Kafka/kafka_producer.py'
    )

    # pipeline sequence
    crawl_task >> clean_task >> kafka_task
