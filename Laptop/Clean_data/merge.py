import pandas as pd
from sqlalchemy import create_engine
import os
from urllib.parse import quote_plus

DB_TYPE = "mysql"
DB_USER = "root"
DB_PASS = quote_plus("Truongdz2004@")  
DB_HOST = "127.0.0.1"
DB_PORT = "3306"
DB_NAME = "LAPTOP_FINAL"
CSV_FOLDER = r"D:/Test/Clean_data"  

csv_table_map = {
    "dim_date.csv": "DIM_DATE",
    "dim_brand.csv": "DIM_BRAND",
    "dim_category.csv": "DIM_CATEGORY",
    "dim_store.csv": "DIM_STORE",
    "dim_payment_method.csv": "DIM_PAYMENT_METHOD",
    "dim_supplier.csv": "DIM_SUPPLIER",
    "dim_product.csv": "DIM_PRODUCT",
    "dim_customer.csv": "DIM_CUSTOMER",
    "dim_customer_2.csv": "DIM_CUSTOMER_2",
    "dim_employee.csv": "DIM_EMPLOYEE",

    "fact_order.csv": "FACT_ORDER",
    "fact_order_detail.csv": "FACT_ORDER_DETAIL",
    "fact_sales.csv": "FACT_SALES",
    "fact_return.csv": "FACT_RETURN",
    "fact_inventory.csv": "FACT_INVENTORY",
    "fact_web_traffic.csv": "FACT_WEB_TRAFFIC"
}


# Kết nối SQLAlchemy
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)
# Đẩy dữ liệu
for filename, tablename in csv_table_map.items():
    try:
        path = os.path.join(CSV_FOLDER, filename)
        print(f"👉 Loading {filename} vào bảng {tablename}...")
        df = pd.read_csv(path)
        df.to_sql(tablename, con=engine, if_exists='append', index=False)
        print(f"✅ Thành công: {tablename}")
    except Exception as e:
        print(f"❌ Lỗi ở {filename} → {tablename}: {e}")


