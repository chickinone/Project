# producer.py
import os
import re
import time
import json
import math
import pandas as pd
from kafka import KafkaProducer

# =======================
# CẤU HÌNH
# =======================
KAFKA_BROKER = "flink-kafka:9092"
CSV_FOLDER = "./data"
SLEEP_TIME = 0                      # để 0 cho nhanh
LOG_EVERY = 100                     # in tiến độ mỗi n bản ghi

ALLOWED_FILES = {
    "dim_date.csv",
    "dim_customer.csv",
    "dim_customer_2.csv",
    "dim_brand.csv",
    "dim_category.csv",
    "dim_employee.csv",
    "dim_store.csv",
    "dim_supplier.csv",
    "dim_payment_method.csv",
    "dim_product.csv",
    "fact_sales.csv",
    "fact_order.csv",
    "fact_order_detail.csv",
    "fact_inventory.csv",
    "fact_return.csv",
    "fact_web_traffic.csv",
}

# =======================
# MAPPING CỘT (header chuẩn hóa về lowercase)
# =======================
COLUMN_MAPS = {
    # -------- DIMENSIONS --------
    "dim_date.csv": {
        "date": "fulldate",
        "day": "dayofmonth",
        "weekday": "isweekday",
        "month": "monthnumber",
        "quarter": "quarternumber",
        "year": "yearnumber",
    },
    "dim_customer.csv": {
        "createdate": "createdat",
    },
    "dim_customer_2.csv": {
        "createdate": "createdat",
    },
    "dim_payment_method.csv": {
        "createdate": "createdat",
    },
    "dim_store.csv": {
        "opendate": "openeddate",
    },
    "dim_employee.csv": {
        "hiredate": "hiredate",
        "isactive": "isactive"
    },
    "dim_product.csv": {
        "ram_value": "ramvalue",
        "ram_unit": "ramunit",
        "ram_max_value": "rammaxvalue",
        "ram_max_unit": "rammaxunit",
        "ram_speed": "ramspeed",
        "ram_type": "ramtype",
        "storage_value": "storagevalue",
        "storage_unit": "storageunit",
        "screen_size": "screensize",
        "screen_resolution": "screenresolution",
        "screen_refreshrate": "screenrefreshrate",
        "screen_refreshrate_unit": "screenrefreshrateunit",
        "screen_tech": "screentech",
        "screen_description": "screendescription",
        "color_coverage": "colorcoverage",
        "sound_tech": "soundtech",
        "keyboard_backlight": "haskeyboardbacklight",
        "webcams": "haswebcam",
        "wireless": "haswireless",
        "os": "operatingsystem",
        "battery": "batteryspec",
        "description": "productdescription",
        "discount": "discountrate",
        "link": "productlink",
        "image": "productimage",
    },

    # -------- FACTS --------
    "fact_sales.csv": {
        "discount": "salesdiscount",
    },
    "fact_order_detail.csv": {
        "discount": "orderdiscount",
    },
    "fact_return.csv": {
        "reason": "returnreason",
    },
}

# =======================
# REQUIRED TARGETS: đảm bảo không mất cột
# =======================
REQUIRED_TARGETS = {
    "dim_date.csv": [
        "datekey","fulldate","dayofmonth","dayname","isweekday","monthnumber","monthname",
        "quarternumber","yearnumber","weekofyear","isholiday","ismonthend","ismonthstart",
        "isquarterstart","isquarterend","isyearend","isyearstart"
    ],
    "dim_customer.csv": [
        "customerkey","customerid","fullname","gender","dateofbirth","age","phonenumber",
        "email","address","city","province","country","customertype","isloyalcustomer",
        "joindate","isactive","createdat"
    ],
    "dim_customer_2.csv": [
        "customerkey","customerid","fullname","gender","dateofbirth","phonenumber","email",
        "address","city","province","country","customertype","isloyalcustomer","joindate",
        "isactive","version","effectivefrom","effectiveto","iscurrent","age"
    ],
    "dim_employee.csv": [
        "employeekey","employeeid","fullname","gender","position","storekey","hiredate","isactive"
    ],
    "dim_product.csv": [
        "productkey","name","model","brandkey","categorykey","cpu","cores","threads","cpuspeedghz","maxspeed",
        "gpu","ramvalue","ramunit","rammaxvalue","rammaxunit","ramspeed","ramtype","storagevalue",
        "storageunit","screensize","screenresolution","screenrefreshrate","screenrefreshrateunit",
        "screentech","screendescription","colorcoverage","soundtech","haskeyboardbacklight",
        "haswebcam","haswireless","ports","operatingsystem","batteryspec","material","features",
        "productdescription","releasedate","isactive","price","currentprice","discountrate",
        "productlink","productimage"
    ],
    "fact_sales.csv": [
        "salesid","datekey","customerkey","productkey","employeekey","storekey","quantity",
        "unitprice","totalamount","salesdiscount","finalamount"
    ],
    "fact_order_detail.csv": [
        "orderdetailid","orderid","productkey","quantity","unitprice","totalprice","orderdiscount","finalprice"
    ],
    "fact_return.csv": [
        "returnid","datekey","customerkey","productkey","quantity","returnreason","refundamount"
    ],
}

# =======================
# KAFKA PRODUCER
# =======================
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=5,
)

# =======================
# HÀM TIỆN ÍCH
# =======================
def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop_cols = [c for c in df.columns if str(c).lower().startswith("unnamed")]
    return df.drop(columns=drop_cols) if drop_cols else df

def normalize_columns_to_lower(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def rename_columns(file_name: str, df: pd.DataFrame) -> pd.DataFrame:
    mapping = COLUMN_MAPS.get(file_name.lower(), {})
    if mapping:
        existing = {old: new for old, new in mapping.items() if old in df.columns}
        if existing:
            df = df.rename(columns=existing)
    return df

def add_surrogate_keys(file_name: str, df: pd.DataFrame) -> pd.DataFrame:
    fn = file_name.lower()
    if fn == "dim_product.csv" and "productkey" not in df.columns:
        df.insert(0, "productkey", range(1, len(df) + 1))
    return df

def normalize_dates_and_bools(file_name: str, df: pd.DataFrame) -> pd.DataFrame:
    date_cols = {
        "dim_employee.csv": ["hiredate"],
        "dim_product.csv": ["releasedate"],
        "dim_customer.csv": ["dateofbirth","joindate","createdat"],
        "dim_customer_2.csv": ["dateofbirth","joindate","effectivefrom","effectiveto"],
        "dim_date.csv": ["fulldate"],
    }
    for c in date_cols.get(file_name.lower(), []):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")

    for c in [col for col in df.columns if col.startswith(("is","has"))]:
        df[c] = df[c].map(
            lambda x: True if str(x).strip().lower() in {"true","1","t","yes","y"}
            else (False if str(x).strip().lower() in {"false","0","f","no","n"} else None)
        )
    return df

# --- Chuẩn hoá số ---
_CURRENCY_RE = re.compile(r"[^\d,.\-]")
def _to_decimal(x):
    if x is None or (isinstance(x, float) and math.isnan(x)): return None
    s = str(x).strip()
    if not s: return None
    s = _CURRENCY_RE.sub("", s)
    if "," in s and "." in s:
        s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def _to_int(x):
    if x is None or (isinstance(x, float) and math.isnan(x)): return None
    s = str(x).strip()
    if not s: return None
    s = _CURRENCY_RE.sub("", s)
    s = s.replace(",", "")
    try:
        return int(float(s))
    except Exception:
        return None

NUM_DEC_COLS = {
    "dim_product.csv": ["price","currentprice","discountrate","cpuspeedghz"],
    "fact_sales.csv": ["unitprice","totalamount","salesdiscount","finalamount"],
    "fact_order.csv": ["totalamount","discounttotal"],
    "fact_order_detail.csv": ["unitprice","totalprice","orderdiscount","finalprice"],
    "fact_return.csv": ["refundamount"],
}
NUM_INT_COLS = {
    "dim_product.csv": ["cores","threads"],
    "fact_sales.csv": ["quantity"],
    "fact_order_detail.csv": ["quantity"],
    "fact_inventory.csv": ["quantityinstock","reorderlevel"],
    "fact_web_traffic.csv": ["timespentseconds"],
}

def normalize_numbers(file_name: str, df: pd.DataFrame) -> pd.DataFrame:
    fn = file_name.lower()
    for col in NUM_DEC_COLS.get(fn, []):
        if col in df.columns:
            df[col] = df[col].map(_to_decimal)
    for col in NUM_INT_COLS.get(fn, []):
        if col in df.columns:
            df[col] = df[col].map(_to_int)
    return df

def warn_missing_required(file_name: str, df: pd.DataFrame):
    req = REQUIRED_TARGETS.get(file_name.lower())
    if not req:
        return
    missing = [c for c in req if c not in df.columns]
    if missing:
        print(f"[WARN] {file_name}: thiếu cột đích sau khi chuẩn hoá: {missing}")

def clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    return df.where(pd.notnull(df), None)

# =======================
# PIPELINE: CSV -> KAFKA
# =======================
def send_csv_to_kafka(file_path: str, topic_name: str, file_name: str) -> int:
    df = pd.read_csv(file_path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    df = drop_unnamed_columns(df)
    df = normalize_columns_to_lower(df)
    df = rename_columns(file_name, df)
    df = add_surrogate_keys(file_name, df)
    df = normalize_dates_and_bools(file_name, df)
    df = normalize_numbers(file_name, df)
    warn_missing_required(file_name, df)
    df = clean_nulls(df)

    sent = 0
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        producer.send(topic_name, value=row.to_dict())
        sent += 1
        if LOG_EVERY and i % LOG_EVERY == 0:
            print(f"[{file_name}] sent {i}/{len(df)}")
        if SLEEP_TIME:
            time.sleep(SLEEP_TIME)

    producer.flush()
    return sent

# =======================
# MAIN
# =======================
if __name__ == "__main__":
    TOPIC_SUFFIX = os.getenv("TOPIC_SUFFIX", "_topic")
    FACT_BARRIER_SEC = float(os.getenv("FACT_BARRIER_SEC", "15"))
    ORDER_DETAIL_BARRIER_SEC = float(os.getenv("ORDER_DETAIL_BARRIER_SEC", "10"))

    def to_topic(fname: str) -> str:
        return fname.replace(".csv", "") + TOPIC_SUFFIX

    if not os.path.isdir(CSV_FOLDER):
        raise FileNotFoundError(f"Không tìm thấy thư mục dữ liệu: {CSV_FOLDER}")

    all_csv = {
        f for f in os.listdir(CSV_FOLDER)
        if f.lower().endswith(".csv") and f.lower() in ALLOWED_FILES
    }

    DIM_ORDER = [
        "dim_brand.csv",
        "dim_category.csv",
        "dim_product.csv",
        "dim_supplier.csv",
        "dim_store.csv",
        "dim_employee.csv",
        "dim_payment_method.csv",
        "dim_date.csv",
        "dim_customer.csv",
        "dim_customer_2.csv",
    ]
    dim_files = [f for f in DIM_ORDER if f in all_csv]

    fact_all = sorted([f for f in all_csv if f.startswith("fact_")])
    fact_order = [f for f in fact_all if f == "fact_order.csv"]
    fact_order_detail = [f for f in fact_all if f == "fact_order_detail.csv"]
    fact_others = [f for f in fact_all if f not in {"fact_order.csv", "fact_order_detail.csv"}]

    total_sent = 0

    print("🚀 BẮT ĐẦU LOAD DIMENSIONS")
    for file in dim_files:
        path, topic = os.path.join(CSV_FOLDER, file), to_topic(file)
        print(f"🔄 Gửi {file} → {topic}")
        total_sent += send_csv_to_kafka(path, topic, file)

    if fact_all:
        print(f"⏳ Chờ {FACT_BARRIER_SEC}s để đảm bảo DIM đã vào DB...")
        time.sleep(FACT_BARRIER_SEC)

    print("🚀 BẮT ĐẦU LOAD FACTS")
    for file in fact_order:
        path, topic = os.path.join(CSV_FOLDER, file), to_topic(file)
        print(f"🔄 Gửi {file} → {topic}")
        total_sent += send_csv_to_kafka(path, topic, file)

    if fact_order and fact_order_detail:
        print(f"⏳ Chờ {ORDER_DETAIL_BARRIER_SEC}s trước khi gửi FACT_ORDER_DETAIL...")
        time.sleep(ORDER_DETAIL_BARRIER_SEC)

    for file in fact_others:
        path, topic = os.path.join(CSV_FOLDER, file), to_topic(file)
        print(f"🔄 Gửi {file} → {topic}")
        total_sent += send_csv_to_kafka(path, topic, file)

    for file in fact_order_detail:
        path, topic = os.path.join(CSV_FOLDER, file), to_topic(file)
        print(f"🔄 Gửi {file} → {topic}")
        total_sent += send_csv_to_kafka(path, topic, file)

    print(f"🎯 Done. Tổng số bản ghi đã gửi: {total_sent}")
