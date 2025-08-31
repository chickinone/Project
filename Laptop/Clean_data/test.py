import pandas as pd
import glob

# Lấy tất cả file CSV trong thư mục hiện tại
for file in glob.glob("*.csv"):
    try:
        df = pd.read_csv(file, encoding="utf-8-sig")
        print(f"\n=== {file} ===")
        print(",".join(df.columns))   # in header
    except Exception as e:
        print(f"❌ Error đọc {file}: {e}")
