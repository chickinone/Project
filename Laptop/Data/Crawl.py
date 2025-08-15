import os
import random
import time
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from collections import defaultdict

def safe_get(lst, index, default="Không có"):
    try:
        return lst[index]
    except IndexError:
        return default
    
options = uc.ChromeOptions()
options.add_argument("--incognito")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("start-maximized")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
driver = uc.Chrome(options=options)
driver.get("https://www.thegioididong.com/laptop")

time.sleep(random.uniform(4, 7))
while True:
    try:
        button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, "see-more-btn")))
        ActionChains(driver).move_to_element(button).perform()
        button.click()
        time.sleep(random.uniform(3, 6))
    except:
        print("✅ Không còn nút 'Xem thêm', đã tải hết sản phẩm.")
        break

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
        specs_data[group_id] = texts[:max_items] + ["Không có"] * (max_items - len(texts))
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

error_links = []

def fetch_and_save(link, file_path="merged_laptop_data.csv"):
    temp_data = defaultdict(list)
    try:
        if not link.startswith("http"):
            return

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

        temp_data["Tên"].append(name)
        temp_data["Model"].append(model)
        temp_data["Giá gốc"].append(original_price)
        temp_data["Giá hiện tại"].append(price)
        temp_data["Khuyến mãi"].append(sale)

        temp_data["CPU"].append(safe_get(specs.get(32, []), 0))
        temp_data["Số nhân"].append(safe_get(specs.get(32, []), 1))
        temp_data["Số luồng"].append(safe_get(specs.get(32, []), 2))
        temp_data["Tốc độ CPU"].append(safe_get(specs.get(32, []), 3))
        temp_data["Tốc độ tối đa"].append(safe_get(specs.get(32, []), 4))

        temp_data["RAM"].append(safe_get(specs.get(34, []), 0))
        temp_data["Tốc độ RAM"].append(safe_get(specs.get(34, []), 1))
        temp_data["RAM tối đa"].append(safe_get(specs.get(34, []), 2))
        temp_data["Ổ cứng"].append(safe_get(specs.get(34, []), 3))

        temp_data["Màn hình"].append(safe_get(specs.get(52, []), 0))
        temp_data["Độ phân giải"].append(safe_get(specs.get(52, []), 1))
        temp_data["Tần số quét"].append(safe_get(specs.get(52, []), 2))
        temp_data["Độ phủ màu"].append(safe_get(specs.get(52, []), 3))
        temp_data["Công nghệ màn hình"].append(safe_get(specs.get(52, []), 4))

        temp_data["Card đồ họa"].append(safe_get(specs.get(53, []), 0))
        temp_data["Công nghệ âm thanh"].append(safe_get(specs.get(53, []), 1))

        temp_data["Cổng kết nối"].append(safe_get(specs.get(56, []), 0))
        temp_data["Kết nối không dây"].append(safe_get(specs.get(56, []), 1))
        temp_data["Webcam"].append(safe_get(specs.get(56, []), 2))
        temp_data["Tính năng khác"].append(safe_get(specs.get(56, []), 3))
        temp_data["Đèn bàn phím"].append(safe_get(specs.get(56, []), 4))

        temp_data["Kích thước"].append(safe_get(specs.get(62, []), 0))
        temp_data["Chất liệu"].append(safe_get(specs.get(62, []), 1))
        temp_data["Hệ thống pin"].append(safe_get(specs.get(62, []), 2))
        temp_data["Hệ điều hành"].append(safe_get(specs.get(62, []), 3))
        temp_data["Thời gian ra mắt"].append(safe_get(specs.get(62, []), 4))

        temp_data["Mô tả"].append(intro)
        temp_data["Link"].append(link)
        df = pd.DataFrame(temp_data)
        df.to_csv(file_path, mode='a', index=False, header=not os.path.exists(file_path), encoding="utf-8-sig")

        print(f"✔ Đã lấy xong: {name}")
    except Exception as e:
        print(f"❌ Lỗi khi xử lý {link}: {e}")
        error_links.append(link)

try:
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "listproduct")))
    product_urls = list(set([
        a.get_attribute("href")
        for a in driver.find_elements(By.CSS_SELECTOR, ".listproduct a[href]") if a.get_attribute("href").startswith("http")
    ]))
    print(f"Tìm thấy {len(product_urls)} sản phẩm.")
except:
    print("Không lấy được danh sách sản phẩm")
    driver.quit()
    exit()
for link in product_urls:
    fetch_and_save(link)

if error_links:
    print(f" Thử lại {len(error_links)} sản phẩm bị lỗi...")
    for link in error_links:
        fetch_and_save(link)

print("Đã lưu toàn bộ dữ liệu vào merged_laptop_data.csv")
driver.quit()
