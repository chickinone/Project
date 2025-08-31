from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_DIR = "/opt/airflow/dags/.."
RAW_DIR    = os.path.join(PROJECT_DIR, "Data")
CLEAN_DIR  = os.path.join(PROJECT_DIR, "Clean_data")
LG_DATA    = os.path.join(PROJECT_DIR, "log_generator", "data")

def ensure_dirs():
    os.makedirs(LG_DATA, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)

with DAG(
    dag_id="etl_batch",
    description="Crawl -> Clean -> Xuất CSV dim/fact sang log_generator/data",
    default_args=default_args,
    schedule_interval="0 11 * * *",   # chạy 02:00 hàng ngày
    start_date=datetime(2025, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ETL","batch"],
) as dag:

    prep_dirs = PythonOperator(
        task_id="prep_dirs",
        python_callable=ensure_dirs,
    )

    # BẬT CRAWL: chạy script Crawl.py để lấy dữ liệu raw vào thư mục Data/
    crawl_raw = BashOperator(
        task_id="crawl_raw",
        bash_command=f"cd {PROJECT_DIR} && python -u Data/Crawl.py",
        env={"PYTHONUNBUFFERED":"1"},
        do_xcom_push=False,
    )

    # Làm sạch & chuẩn hoá -> tạo dim_*.csv, fact_*.csv trong Clean_data/
    clean_transform = BashOperator(
        task_id="clean_transform",
        bash_command=f"cd {PROJECT_DIR} && python -u Clean_data/clean.py",
        env={"PYTHONUNBUFFERED":"1"},
        do_xcom_push=False,
    )

    # Copy toàn bộ dim_*.csv, fact_*.csv sang log_generator/data để producer dùng
    export_csvs = BashOperator(
        task_id="export_csvs_to_log_generator",
        bash_command=(
            f"mkdir -p {LG_DATA} && "
            f"cp -f {CLEAN_DIR}/dim_*.csv {LG_DATA} || true && "
            f"cp -f {CLEAN_DIR}/fact_*.csv {LG_DATA} || true"
        ),
    )

    # CHUỖI CHẠY: prep -> crawl -> clean -> export
    prep_dirs >> crawl_raw >> clean_transform >> export_csvs
