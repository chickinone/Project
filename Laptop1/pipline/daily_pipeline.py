import subprocess
import os
from datetime import datetime


CRAWL_SCRIPT = "D:/Laptop1/Data/Crawl.py"
CLEAN_SCRIPT = "D:/Laptop1/Clean_data/data.ipynb"
PRODUCER_SCRIPT = "D:/Laptop1/Kafka/kafka_producer.py"


def log(message):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now}] {message}")

def run_python_file(file_path):
    if not os.path.isfile(file_path):
        log(f"❌ File not found: {file_path}")
        return False
    try:
        log(f"▶️ Running: {file_path}")
        subprocess.run(["python", file_path], check=True)
        log(f"✅ Completed: {file_path}")
        return True
    except subprocess.CalledProcessError as e:
        log(f"❌ Error running {file_path}: {e}")
        return False

def run_notebook(notebook_path):
    if not os.path.isfile(notebook_path):
        log(f"❌ Notebook not found: {notebook_path}")
        return False
    try:
        log(f"▶️ Executing notebook: {notebook_path}")
        subprocess.run([
            "jupyter", "nbconvert",
            "--to", "notebook",
            "--execute", notebook_path,
            "--inplace"
        ], check=True)
        log(f"✅ Notebook completed: {notebook_path}")
        return True
    except subprocess.CalledProcessError as e:
        log(f"❌ Error executing notebook {notebook_path}: {e}")
        return False

if __name__ == "__main__":
    log("🚀 Starting daily data pipeline...")

    if not run_python_file(CRAWL_SCRIPT):
        exit(1)

    if not run_notebook(CLEAN_SCRIPT):
        exit(1)

    if not run_python_file(PRODUCER_SCRIPT):
        exit(1)

    log("🎉 Daily pipeline completed successfully.")
