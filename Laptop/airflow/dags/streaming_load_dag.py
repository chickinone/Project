from datetime import datetime, timedelta
import time
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

PROJECT_DIR = "/opt/airflow/dags/.."
LG_DIR      = f"{PROJECT_DIR}/log_generator"

# (Tuỳ bạn) Topic sẽ do producer.py tự tạo/gửi theo mapping.
# Nếu muốn tách DIM & FACT rõ ràng, bạn có thể truyền tham số cho producer.py (ví dụ --only-dim / --only-fact)
# Ở đây mình chạy 2 lần producer với chế độ DIM trước, FACT sau (giả lập bằng biến môi trường).

def dq_check():
    # Kết nối Postgres của bạn (sửa thông tin kết nối)
    conn = psycopg2.connect(
        dbname="laptopdb",
        user="postgres",
        password="postgres",
        host="localhost",   # nếu Airflow chạy trong container khác, đổi thành hostname service, ví dụ "postgres"
        port=5432,
    )
    cur = conn.cursor()
    # Ví dụ kiểm tra số bản ghi (bạn sửa tên bảng cho khớp init.sql của bạn)
    checks = [
        ("dim_brand", "SELECT COUNT(*) FROM dim_brand"),
        ("dim_product", "SELECT COUNT(*) FROM dim_product"),
        ("fact_order", "SELECT COUNT(*) FROM fact_order"),
        ("fact_order_detail", "SELECT COUNT(*) FROM fact_order_detail"),
    ]
    errors = []
    for name, sql in checks:
        cur.execute(sql); n = cur.fetchone()[0]
        print(f"[DQ] {name}: {n} rows")
        if n == 0:
            errors.append(name)
    cur.close(); conn.close()
    if errors:
        raise ValueError(f"DQ fail, bảng rỗng: {errors}")

with DAG(
    dag_id="streaming_load",
    description="Phát DIM trước, FACT sau vào Kafka; Flink đang chạy; cuối cùng DQ Postgres",
    default_args=default_args,
    schedule_interval=None,   # bạn trigger thủ công (hoặc đặt lịch riêng)
    start_date=datetime(2025, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming","kafka","flink"],
) as dag:

    # Đảm bảo Flink job đã chạy (bạn có thể viết một PythonOperator ping REST API Flink để confirm)
    # Ở đây demo tối giản: chỉ sleep hoặc bỏ qua nếu chắc chắn Flink RUNNING
    wait_flink_up = PythonOperator(
        task_id="wait_flink_up",
        python_callable=lambda: time.sleep(5),
    )

    # Gửi DIM trước (giả sử producer.py đọc LG_DIR/data và gửi theo thứ tự DIM trước)
    produce_dims = BashOperator(
        task_id="produce_dims",
        bash_command=(
            f"cd {LG_DIR} && "
            f"ONLY_DIM=1 python producer.py"
        ),
        env={"PYTHONUNBUFFERED":"1"},
    )

    # Nghỉ 10s để Flink ingest DIM xong (tuỳ chỉnh)
    pause = PythonOperator(
        task_id="pause_10s",
        python_callable=lambda: time.sleep(10),
    )

    # Gửi FACT
    produce_facts = BashOperator(
        task_id="produce_facts",
        bash_command=(
            f"cd {LG_DIR} && "
            f"ONLY_FACT=1 python producer.py"
        ),
        env={"PYTHONUNBUFFERED":"1"},
    )

    dq = PythonOperator(
        task_id="data_quality_check_postgres",
        python_callable=dq_check,
    )

    wait_flink_up >> produce_dims >> pause >> produce_facts >> dq
