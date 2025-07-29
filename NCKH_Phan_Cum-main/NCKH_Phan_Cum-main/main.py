#Viết tất cả các thuật toán ra màn hình từ tham số,...

import time
import numpy as np
import pandas as pd 
from Algorithm.CFCM import Dcfcm
from Algorithm.FCM import FuzzyCMeans
from Algorithm.SSFCM import Dssfcm
from Algorithm.K_Means import K_MEANS
from Ultility.validity import dunn, davies_bouldin, calinski_harabasz, silhouette, separation, classification_entropy, hypervolume, cs, partition_coefficient
from Ultility.data import round_float, fetch_data_from_uci
from tabulate import tabulate

def print_table(results):
    headers = ["Alg", "Time", "Step", "DB-", "PC+", "CE-", "S-", "CH+", "SI+", "FHV+", "CS-", "F1+", "AC+"]
    print(tabulate(results, headers=headers, tablefmt="pretty", floatfmt=".4f"))

def print_info(title, X, U, V, process_time, step=0):
    M = 2
    return [
        title,
        round_float(process_time),
        step,
        round_float(davies_bouldin(X, np.argmax(U, axis=1))),
        round_float(partition_coefficient(U)),
        round_float(classification_entropy(U)),
        round_float(separation(X, U, V, M)),
        round_float(calinski_harabasz(X, np.argmax(U, axis=1))),
        round_float(silhouette(X, np.argmax(U, axis=1))),
        round_float(hypervolume(U, M)),
        round_float(cs(X, U, V, M)),
    ]

def print_info1(title, X, labels, V, process_time, step=0):
    M = 2
    return [
        title,
        round_float(process_time),
        step,
        round_float(davies_bouldin(X, labels)),
        round_float(np.nan),
        round_float(np.nan),
        round_float(np.nan),
        round_float(calinski_harabasz(X, labels)),
        round_float(silhouette(X, labels)),
        round_float(np.nan),
        round_float(np.nan),
        round_float(np.nan),
        round_float(np.nan),

    ]

def run_algorithms():
    dataset_id = 602
    data_dict = fetch_data_from_uci(dataset_id)
    data = data_dict['X']
    labels = data_dict['y']
    n_clusters = len(np.unique(labels))
    print(n_clusters)

    results = []

    # KMeans
    start_time = time.time()
    kmeans_model = K_MEANS()
    labels, centroids, steps = kmeans_model.kmeans(data, n_clusters)
    process_time = time.time() - start_time
    results.append(print_info1("KMeans", data, labels, centroids, process_time, steps))

    # FCM
    fcm = FuzzyCMeans(n_clusters=n_clusters)
    start_time = time.time()
    centroids, membership_matrix, steps, _ = fcm.fit(data)
    process_time = time.time() - start_time
    results.append(print_info("FCM", data, membership_matrix, centroids, process_time, steps))

    # SSFCM
    ssfcm = Dssfcm(n_clusters=n_clusters)
    start_time = time.time()
    centroids, membership_matrix, steps = ssfcm.fit(data, labels, supervised_ratio=0.3)
    process_time = time.time() - start_time
    results.append(print_info("SSFCM", data, membership_matrix, centroids, process_time, steps))

    # CFCM
    num_sites = 3
    sub_datasets = np.array_split(data, num_sites)
    dcfcm = Dcfcm(n_clusters=n_clusters, beta=0.5, max_iter=100)
    dcfcm.phase1(sub_datasets)
    start_time = time.time()
    dcfcm.phase2(iterations=5)
    process_time = time.time() - start_time
    for i, site in enumerate(dcfcm.data_site):
        results.append(print_info(f"CFCM-Site-{i+1}", site.local_data, site.membership_matrix, site.cluster_centers, process_time, 5))


    print_table(results)


if __name__ == "__main__":
    
    for i in range(3):
        run_algorithms()

