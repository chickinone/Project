import numpy as np
import matplotlib.pyplot as plt
import rasterio
from Algorithm.FCM import FuzzyCMeans
import os
from Algorithm.CFCM import Dcfcm 
from Ultility.test import align_clusters
from Algorithm.SSFCM import Dssfcm
import time

# Bước 1: Đường dẫn đến các ảnh vệ tinh .tif
image_paths_scl = [
    'E:/NCKH/data/LANDSAT/SCL/2024/z200/z200_B2_scl.tif',
    'E:/NCKH/data/LANDSAT/SCL/2024/z200/z200_B3_scl.tif',
    'E:/NCKH/data/LANDSAT/SCL/2024/z200/z200_B4_scl.tif',
    'E:/NCKH/data/LANDSAT/SCL/2024/z200/z200_B5_scl.tif',
]

image_paths_tn = [
    'E:/NCKH/data/LANDSAT/TN/2024/z200/z200_B2_tn.tif',
    'E:/NCKH/data/LANDSAT/TN/2024/z200/z200_B3_tn.tif',
    'E:/NCKH/data/LANDSAT/TN/2024/z200/z200_B4_tn.tif',
    'E:/NCKH/data/LANDSAT/TN/2024/z200/z200_B5_tn.tif',
]

image_paths_hn = [    
    'E:/NCKH/data/LANDSAT/HN/2024/z200/z200_B2_hn.tif',
    'E:/NCKH/data/LANDSAT/HN/2024/z200/z200_B3_hn.tif',
    'E:/NCKH/data/LANDSAT/HN/2024/z200/z200_B4_hn.tif',
    'E:/NCKH/data/LANDSAT/HN/2024/z200/z200_B5_hn.tif',
]
image_paths4 = [
    'E:/NCKH/data/LANDSAT/SCL/z50_B2_scl.tif',
    'E:/NCKH/data/LANDSAT/SCL/z50_B3_scl.tif',
    'E:/NCKH/data/LANDSAT/SCL/z50_B4_scl.tif',
    'E:/NCKH/data/LANDSAT/SCL/z50_B5_scl.tif',
]
image_paths5 = [
    'E:/NCKH/data/LANDSAT/TN/z50_B2_tn.tif',
    'E:/NCKH/data/LANDSAT/TN/z50_B3_tn.tif',
    'E:/NCKH/data/LANDSAT/TN/z50_B4_tn.tif',
    'E:/NCKH/data/LANDSAT/TN/z50_B5_tn.tif',
]
image_paths6 = [
    'E:/NCKH/data/LANDSAT/HN/z50_B2_hn.tif',
    'E:/NCKH/data/LANDSAT/HN/z50_B3_hn.tif',
    'E:/NCKH/data/LANDSAT/HN/z50_B4_hn.tif',
    'E:/NCKH/data/LANDSAT/HN/z50_B5_hn.tif',
]
all_image_paths = [image_paths_scl, image_paths_tn, image_paths_hn]

brands = []



def load_and_prepare(image_paths):
    bands = []
    for path in image_paths:
        with rasterio.open(path) as src:
            band = src.read()  # (1, H, W)
            print(band.shape)
            bands.append(band)
    data = np.concatenate(bands)            # (n_bands, H, W)
    data = np.transpose(data, (1, 2, 0))             # (H, W, n_bands)
    w, h, c = data.shape
    data_2d = data.reshape(w * h, c)                 # (H*W, n_bands)
    return data_2d, w, h ,c 

# Đọc và xử lý từng nhóm ảnh riêng biệt
data1, w1, h1, c1 = load_and_prepare(image_paths_scl)
data2, w2, h2, c2 = load_and_prepare(image_paths_tn)
data3, w3, h3, c3 = load_and_prepare(image_paths_hn)


all_data = [data1, data2, data3]


# Bước 6: Phân cụm bằng Fuzzy C-Means
C = 6
M = 2
EPSILON = 1e-5
MAXITER = 10000

# Bảng màu cho từng lớp
COLORS = np.array([
    [0, 255, 0],        # Class 3: Field, grass Đồng ruộng, cỏ
    [0, 64, 0],         # Class 6: Dense forest, jungle RỪng rậm
    [1, 192, 255],      # Class 4: Sparse forest, low trees  Rừng thưa, cây thấp
    [0, 128, 0],        # Class 5: Perennial Plants cây lâu năm
    [0, 0, 255],        # Class 1: Rivers, lakes, ponds sông, hồ, ao
    [128, 128, 128],    # Class 2: Vacant land, roads đắt trống đường nhà
], dtype=np.uint8)


data4, w4, h4, c4 = load_and_prepare(image_paths4)
data5, w5, h5, c5 = load_and_prepare(image_paths5)
data6, w6, h6, c6 = load_and_prepare(image_paths6)
# fcm = FuzzyCMeans(n_clusters=C, m=M, epsilon=EPSILON, max_iter=MAXITER)
# centroid4,membership4 , step , _= fcm.fit(data=data4)
# print("Đã chạy xong")
# centroid5,membership5 , step , _= fcm.fit(data=data5)
# print("Đã chạy xong")
# centroid6,membership6 , step , _= fcm.fit(data=data6)
# print("Đã chạy xong")

# centroid = [centroid4, centroid5, centroid6]

# labels4 = np.argmax(membership4, axis=1) # Nhãn
# labels5 = np.argmax(membership5, axis=1) # Nhãn
# labels6 = np.argmax(membership6, axis=1) # Nhãn


# labels = [labels4, labels5, labels6]



# labels_image = labels.reshape(( height,width))
# labels_image4 = labels4.reshape(( w4,h4))

# colored_segmented_image = np.zeros(( height,width, 3), dtype=np.uint8)
# colored_segmented_image4 = np.zeros(( w4,h4, 3), dtype=np.uint8)

# for i, color in enumerate(COLORS):
#     colored_segmented_image4[labels_image4 == i] = color  # labels của các đoạn với màu tương ứng

# plt.imshow(colored_segmented_image4)
# plt.title("Z50")
# plt.axis('off')
# plt.show()

dcfcm = Dcfcm(n_clusters=C, m=M, epsilon=EPSILON, max_iter=MAXITER)
dcfcm.phase1(all_data)

# Hàm align_clusters đã import sẵn từ Ultility.test
# def align_clusters(centroids, membership_matrix, standard_centroid):

# Chuẩn bị standard_centroid từ kết quả FCM (centroid4)
# standard_centroid = centroid4

# Áp dụng align_clusters cho từng site trong D-CFCM
for i, site in enumerate(dcfcm.data_site):
    if (i == 0): 
        continue
    aligned_centroids, aligned_membership = align_clusters(site.cluster_centers, site.membership_matrix, dcfcm.data_site[0].cluster_centers)
    # Cập nhật lại trong site
    site.cluster_centers = aligned_centroids
    site.membership_matrix = aligned_membership
start_time = time.time()
dcfcm.phase2(iterations=10000)
phase2_time = time.time() - start_time


def create_segmented_image(membership_matrix, width, height, colors):
    labels = np.argmax(membership_matrix, axis=1)
    labels_image = labels.reshape((width,height))  # Chú ý chiều là (H, W)
    segmented_image = np.zeros((width,height, 3), dtype=np.uint8)
    for i, color in enumerate(colors):
        segmented_image[labels_image == i] = color
    return segmented_image


sizes = [(w1, h1), (w2, h2), (w3,h3)]
titles = ["SCL (Site 1) ", "TN (Site 2) ", "HN (Site 3) "]

plt.figure (figsize=(15, 5))

# Iterate through each site and display the segmented image
for i, site in enumerate (dcfcm.data_site) :
    width, height = sizes[i]
    membership_matrix = site.membership_matrix
    segmented_image = create_segmented_image(membership_matrix, width, height, COLORS)

    plt.subplot (1, 3, i + 1)
    plt.imshow (segmented_image)
    plt.title (f"Segmented Image - {titles [i] } ")
    plt.axis ("off")

plt.tight_layout ()
plt.show ()


from scipy.spatial.distance import cdist

# # đồng bộ hoá z200 vs z50
# def remap_and_color(membership_matrix, width, height, ref_centroids, site_centroids, colors):
#     mapping = np.argmin(cdist(site_centroids, ref_centroids), axis=1)
#     remapped_labels = np.array([mapping[l] for l in np.argmax(membership_matrix, axis=1)])
#     labels_image = remapped_labels.reshape(width, height)
#     colored_img = np.zeros((width, height, 3), dtype=np.uint8)
#     for i, color in enumerate(colors):
#         colored_img[labels_image == i] = color
#     return colored_img

# segmented_images = []
# for i, site in enumerate (dcfcm.data_site) :
#     width, height = sizes[i]
#     colored_image = remap_and_color(dcfcm.data_site[i].membership_matrix,width, height,centroid[i],dcfcm.data_site[i].cluster_centers,COLORS)
#     segmented_images.append(colored_image)
# titles = ["Segmented Image - SCL", "Segmented Image - TN", "Segmented Image - HN"]
# for i in range(3):
#     plt.subplot(1, 3, i + 1)
#     plt.imshow(segmented_images[i])
#     plt.title(titles[i])
#     plt.axis("off")

# plt.tight_layout()
# plt.show()
output_dir = "E:/NCKH/data/LANDSAToutput_segmented_images_2024"
os.makedirs(output_dir, exist_ok=True)

# # Lưu từng ảnh trong segmented_images
site_names = ["SCL_2024", "TN_2024", "HN_2024"]
# for i, image in enumerate(segmented_images):
#     filename = f"{output_dir}/segmented_{site_names[i]}.png"
#     plt.imsave(filename, image)
#     print(f"Đã lưu ảnh: {filename}")


# # Tính diện tích
from collections import Counter

def print_cluster_stats(labels, site_name, output_path):
    pixel_area_m2 = 200 * 200  # 40000 m²
    pixel_area_km2 = pixel_area_m2 / 1e6  # 0.04 km²

    unique, counts = np.unique(labels, return_counts=True)
    cluster_stats = dict(zip(unique, counts))

    total_pixels = np.sum(counts)
    total_area_km2 = total_pixels * pixel_area_km2

    lines = []
    lines.append(f"Thống kê diện tích từng cụm cho {site_name}:\n")
    lines.append(f"Tổng số điểm ảnh: {total_pixels} (~{total_area_km2:.2f} km²)\n")
    lines.append("-" * 45 + "\n")
    for cluster_id, pixel_count in cluster_stats.items():
        area_km2 = pixel_count * pixel_area_km2
        percentage = (pixel_count / total_pixels) * 100
        lines.append(f"Cụm {cluster_id + 1}: {pixel_count} pixel -> {area_km2:.2f} km² ({percentage:.2f}%)\n")

    print(f"Thống kê diện tích từng cụm cho {site_name}:")
    print(f"Tổng số điểm ảnh: {total_pixels} (~{total_area_km2:.2f} km²)")
    print("-" * 45)
    for cluster_id, pixel_count in cluster_stats.items():
        area_km2 = pixel_count * pixel_area_km2
        percentage = (pixel_count / total_pixels) * 100
        print(f"Cụm {cluster_id + 1}: {pixel_count} pixel -> {area_km2:.2f} km² ({percentage:.2f}%)")
    # Ghi vào file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)


# # Gọi cho từng khu vực
# for i, site in enumerate (dcfcm.data_site) :
#     print_cluster_stats(np.argmax(site.membership_matrix, axis=1), f"Z200 - {site_names[i]}")

# Gọi hàm cho từng site và lưu
for i, site in enumerate(dcfcm.data_site):
    labels = np.argmax(site.membership_matrix, axis=1)  # gán nhãn cụm
    site_name = site_names[i]
    stats_path = f"{output_dir}/stats_{site_name}.txt"
    print_cluster_stats(labels, site_name, stats_path)
    print(f"Đã lưu thống kê: {stats_path}")


from Ultility.validity import  davies_bouldin, calinski_harabasz, silhouette, separation, classification_entropy, hypervolume, cs, partition_coefficient
from Ultility.data import  round_float 
SPLIT = '\t'
M = 2
average='weighted'
def wdvl(val: float) -> str:
    return str(round_float(val))
def print_info2(title: str, X: np.ndarray, U: np.ndarray, V: np.ndarray , process_time: float, step: int = 0, split: str = SPLIT) -> str:
        # print("labels_numeric", labels_numeric)
        # print("predicted_labels", np.argmax(U, axis=1))
        kqdg = [
            title,
            str(wdvl(process_time)),
            # str(step),
            wdvl(davies_bouldin(X, np.argmax(U, axis=1))),  # DB
            wdvl(partition_coefficient(U)),  # PC
            wdvl(classification_entropy(U)),  # CE
            wdvl(separation(X, U, V, M)),  # S
            wdvl(calinski_harabasz(X, np.argmax(U, axis=1))),  # CH
            wdvl(silhouette(X, np.argmax(U, axis=1))),  # SI
            wdvl(hypervolume(U, M)),  # FHV
            wdvl(cs(X, U, V, M)),  # CS
            # wdvl(f1_score(labels_numeric, np.argmax(U, axis=1), average)),
            # wdvl(accuracy_score(labels_numeric, np.argmax(U, axis=1)))

        ]
        # print(wdvl(f1_score(labels_numeric, np.argmax(U, axis=1), average)))
        # print(wdvl(accuracy_score(labels_numeric, np.argmax(U, axis=1))))
        return ' & '.join(kqdg) + r'\\'
    
titles = ['Alg', 'Time', 'DB-', 'PC+', 'CE-', 'S-   ' , 'CH+     ', 'SI+', 'FHV+', 'F1+', 'AC+']
print(SPLIT.join(titles))
lines2 = []

for i, site in enumerate(dcfcm.data_site):
    print(print_info2(title=f"Site-{site_names[i]}", X=site.local_data, U=site.membership_matrix, V=site.cluster_centers, process_time=phase2_time, step=dcfcm.steps[i]))
    lines2.append(print_info2(title=f"Site-{site_names[i]}", X=site.local_data, U=site.membership_matrix, V=site.cluster_centers, process_time=phase2_time, step=dcfcm.steps[i]))
output_dir2 = "E:/NCKH/data/index_images_2024"
os.makedirs(output_dir2, exist_ok=True)
stats_path = f"{output_dir2}/stats_{site_name}.txt"
with open(stats_path, 'w', encoding='utf-8') as f:
    f.writelines(lines2)