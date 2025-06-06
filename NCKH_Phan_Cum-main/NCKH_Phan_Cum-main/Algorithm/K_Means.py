import pandas as pd
import numpy as np
import time
from Ultility.validity import dunn, davies_bouldin, calinski_harabasz, silhouette
from Ultility.data import fetch_data_from_uci
class K_MEANS:
    def __init__(self, max_iter=300, tol=1e-4):
        self.max_iter = max_iter
        self.tol = tol
    
    #Khởi tạo tâm cụm
    def initialize_centroids(self, data, k):
        np.random.seed(42)
        centroids = np.random.choice(data.shape[0], k, replace=False)
        return data[centroids, :]
    
    #Khoảng cách
    def distances(self, data, centroids):
        return np.sqrt(((data[:, np.newaxis] - centroids) ** 2).sum(axis=2))
    
    #Gán nhãn
    def update_labels(self, distances):
        return np.argmin(distances, axis=1)
    
    #Cập nhật tâm cụm
    def update_centroids(self, data, labels, C):
        return np.array([data[labels == i].mean(axis=0) for i in range(C)])

    def kmeans(self, data, C):
        centroids = self.initialize_centroids(data, C)
        for i in range(self.max_iter):
            old_centroids = centroids
            distances = self.distances(data, centroids)
            labels = self.update_labels(distances)
            centroids = self.update_centroids(data, labels, C)

            if np.sum((centroids - old_centroids) ** 2) < self.tol:
                break

        return labels, centroids, i + 1

# Load dataset
# file_path = 'E:/NCKH/data/UCI/Iris.csv'
# dataset_id = 602
# data_dict = fetch_data_from_uci(dataset_id)
# data = data_dict['X']

# data = pd.read_csv(file_path, header=None)
# data = data.iloc[:, :-1].values

# C = 3

# start_time = time.time()
# kmeans_model = K_MEANS()
# labels, centroids, num_iterations = kmeans_model.kmeans(data, C)
# end_time = time.time()

# Calculate clustering metrics
# di = dunn(data, labels)
# db = davies_bouldin(data, labels)
# ch = calinski_harabasz(data, labels)
# si = silhouette(data, labels)

# # Print results
# print("Thời gian thực hiện: {:.4f} giây".format(end_time - start_time))
# print("Nhãn (Labels):", labels)
# print("Tâm cụm (Centroids):", centroids)
# print("Số lần lặp (Iterations):", num_iterations)

# # Print clustering metrics
# print("Dunn Index (DI):", di)
# print("Davies-Bouldin (DB):", db)
# print("Calinski-Harabasz (CH):", ch)
# print("Silhouette (SI):", si)
