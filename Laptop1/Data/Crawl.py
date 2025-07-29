import random
import time
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from collections import defaultdict

# Cấu hình trình duyệt với Undetected ChromeDriver
options = uc.ChromeOptions()
options.add_argument("--incognito")  # Chạy ẩn danh
options.add_argument("--disable-blink-features=AutomationControlled")  # Ẩn Selenium
options.add_argument("start-maximized")  # Mở full màn hình
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36") 
driver = uc.Chrome(options=options)
driver.get("https://www.thegioididong.com/laptop")

# Chờ tải trang
time.sleep(random.uniform(4, 7))

# Lấy danh sách sản phẩm bằng cách bấm nút "Xem thêm"
while True:
    try:
        button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "see-more-btn"))
        )
        ActionChains(driver).move_to_element(button).perform()
        button.click()
        time.sleep(random.uniform(3, 6))  # Tăng thời gian delay ngẫu nhiên
    except:
        print("Không còn nút 'Xem thêm', đã tải hết sản phẩm.")
        break

data = defaultdict(list)

def scroll_down():
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

def click_and_get_specs(group_ids, max_items=6):
    specs_data = {}
    for group_id in group_ids:
        try:
            btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, f'a[data-group-id="{group_id}"]'))
            )
            ActionChains(driver).move_to_element(btn).perform()
            driver.execute_script("arguments[0].click();", btn)
            WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".text-specifi.active li"))
            )
            time.sleep(1)
        except:
            continue

        specs = driver.find_elements(By.CSS_SELECTOR, ".text-specifi.active li")
        texts = [spec.text for spec in specs]
        specs_data[group_id] = (texts + ["Không có"] * max_items)[:max_items]

    return specs_data

def get_text_by_class(cls):
    try:
        return driver.find_element(By.CLASS_NAME, cls).text
    except:
        return "Không có"

def get_introduction():
    try:
        tab = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#tab-spec .tab-link[data-tab="tab-2"]'))
        )
        driver.execute_script("arguments[0].click();", tab)
        time.sleep(1)
        h3s = driver.find_elements(By.CSS_SELECTOR, ".text-detail h3")
        return "; ".join([h3.text.strip() for h3 in h3s if h3.text.strip()])
    except:
        return "Không có"

def fetch_product_data(link):
    if not link.startswith("http"): return
    try:
        driver.get(link)
        time.sleep(random.uniform(3, 5))
        scroll_down()

        name = driver.find_element(By.TAG_NAME, "h1").text
        model = name.split(" ")[1] if len(name.split(" ")) > 1 else "Không có"

        original_price = get_text_by_class("box-price-old")
        price = get_text_by_class("box-price-present")
        sale = get_text_by_class("box-price-percent")

        specs = click_and_get_specs([32, 34, 52, 53, 56, 62], max_items=6)
        intro = get_introduction()

        data["Tên"].append(name)
        data["Model"].append(model)
        data["Giá gốc"].append(original_price)
        data["Giá hiện tại"].append(price)
        data["Khuyến mãi"].append(sale)

        data["CPU"].append(specs.get(32, ["Không có"])[0])
        data["Số nhân"].append(specs.get(32, ["Không có"])[1])
        data["Số luồng"].append(specs.get(32, ["Không có"])[2])
        data["Tốc độ CPU"].append(specs.get(32, ["Không có"])[3])
        data["Tốc độ tối đa"].append(specs.get(32, ["Không có"])[4])

        data["RAM"].append(specs.get(34, ["Không có"])[0])
        data["Tốc độ RAM"].append(specs.get(34, ["Không có"])[1])
        data["RAM tối đa"].append(specs.get(34, ["Không có"])[2])
        data["Ổ cứng"].append(specs.get(34, ["Không có"])[3])

        data["Màn hình"].append(specs.get(52, ["Không có"])[0])
        data["Độ phân giải"].append(specs.get(52, ["Không có"])[1])
        data["Tần số quét"].append(specs.get(52, ["Không có"])[2])
        data["Độ phủ màu"].append(specs.get(52, ["Không có"])[3])
        data["Công nghệ màn hình"].append(specs.get(52, ["Không có"])[4])

        data["Card đồ họa"].append(specs.get(53, ["Không có"])[0])
        data["Công nghệ âm thanh"].append(specs.get(53, ["Không có"])[1])

        data["Cổng kết nối"].append(specs.get(56, ["Không có"])[0])
        data["Kết nối không dây"].append(specs.get(56, ["Không có"])[1])
        data["Webcam"].append(specs.get(56, ["Không có"])[2])
        data["Tính năng khác"].append(specs.get(56, ["Không có"])[3])
        data["Đèn bàn phím"].append(specs.get(56, ["Không có"])[4])

        data["Kích thước"].append(specs.get(62, ["Không có"])[0])
        data["Chất liệu"].append(specs.get(62, ["Không có"])[1])
        data["Hệ thống pin"].append(specs.get(62, ["Không có"])[2])
        data["Hệ điều hành"].append(specs.get(62, ["Không có"])[3])
        data["Thời gian ra mắt"].append(specs.get(62, ["Không có"])[4])

        data["Mô tả"].append(intro)
        data["Link"].append(link)

        print(f"✔ Đã lấy xong: {name}")
    except Exception as e:
        print(f"❌ Lỗi khi xử lý {link}: {e}")

# Lấy danh sách URL sản phẩm
try:
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "listproduct")))
    product_urls = list(set([
        a.get_attribute("href")
        for a in driver.find_elements(By.CSS_SELECTOR, ".listproduct a[href]")
        if a.get_attribute("href").startswith("http")
    ]))
except:
    print("❌ Không lấy được danh sách sản phẩm")
    driver.quit()
    exit()

# Crawl từng sản phẩm
for link in product_urls[:10]:  # test với 10 sản phẩm đầu
    fetch_product_data(link)

# Chuẩn hóa chiều dài
max_len = max(len(v) for v in data.values())
for key in data:
    while len(data[key]) < max_len:
        data[key].append("Không có")

# Xuất file CSV
df = pd.DataFrame(data)
df.to_csv("merged_laptop_data.csv", index=False, encoding="utf-8-sig")
print("📦 Đã lưu vào laptop_full_data.csv")

driver.quit()