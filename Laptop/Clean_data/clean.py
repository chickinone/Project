import pandas as pd
import os

data_folder = "D:/Laptop/Data"


file_list = [
    "laptop_data.csv",
    "cpu_specs.csv",
    "material.csv",
    "Port.csv",
    "Ram.csv",
    "Screen.csv",
    "Sound.csv"
]


full_paths = [os.path.join(data_folder, filename) for filename in file_list]


dataframes = {
    os.path.basename(path): pd.read_csv(path, encoding='utf-8-sig')
    for path in full_paths
}


main_df = dataframes["laptop_data.csv"]
for filename, df in dataframes.items():
    if filename != "laptop_data.csv":
        main_df = pd.merge(main_df, df, on="Link", how="left")
data_output = "D:/Laptop/Clean_data"
output_path = os.path.join(data_output, "merged_laptop_data.csv")
main_df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ Merge completed! Output file saved at: {output_path}")
df = pd.read_csv(output_path, encoding="utf-8-sig")
rename_dict = {
    "name": "name",
    "Model": "model",
    "price": "price",
    "Giá hiện tại": "current_price",
    "Khuyến mãi": "discount",
    "Link": "link",
    "Mô tả": "description",
    "CPU": "cpu",
    "Số nhân": "cores",
    "Số luồng": "threads",
    "Tốc độ CPU": "cpu_speed",
    "Tốc độ tối đa": "max_speed",
    "Kich_thuoc": "size",
    "Chat_lieu": "material",
    "He_thong_pin": "battery",
    "He_dieu_hanh": "os",
    "Thoi_gian_ra_mat": "release_time",
    "Cong_Giao_tiep": "ports",
    "Wireless": "wireless",
    "Webcams": "webcams",
    "Features": "features",
    "Den_ban_phim": "keyboard_backlight",
    "RAM": "ram",
    "Ram Speed": "ram_speed",
    "Ram Max": "ram_max",
    "Hard Drive": "hard_drive",
    "Review": "review",
    "Image": "image",
    "Screen": "screen",
    "Do_phan_giais": "resolution",
    "Tan_so_quets": "refresh_rate",
    "Do_phu_mau": "color_coverage",
    "Screen_Technologys": "screen_tech",
    "Card_Screen": "gpu",
    "Technology_Sound": "sound_tech"
}
df.rename(columns=rename_dict, inplace=True)

df.replace("", 0, inplace=True)
df.fillna(0, inplace=True)

df = df[df['price'].notnull()]
df = df[df['link'].notnull() & (df['link'].astype(str).str.strip() != "")]


cleaned_output_path = os.path.join(data_output, "cleaned_laptop_data.csv")
df.to_csv(cleaned_output_path, index=False, encoding='utf-8-sig')
print(f"Data cleaned! File saved at: {cleaned_output_path}")
