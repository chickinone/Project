import time
import numpy as np
from CFCM import Dcfcm
from scipy.spatial.distance import cdist
from Algorithm.FCM import FuzzyCMeans
from Algorithm.SSFCM import Dssfcm
from Ultility.test import align_clusters


class SSCFCM(Dcfcm):
    def __init__(self, n_clusters: int, m: float = 2.0, epsilon: float = 1e-5, max_iter: int = 10000):
        super().__init__(n_clusters, m, epsilon)
        self.data_site = [] #Lưu trữ các datasite là các đối tượng trong FCM
        # self.beta = beta #hệ só cộng tác giữa các datasite 
        self.labels_datasite = []
        self.j_ii = []
        self.u_bar = []

    # def init_u_bar(self, labels):
    #     # Khởi tạo mảng số 0 có shape là NxC
    #     u_bar = np.zeros((len(labels), self.n_clusters))
    #     # Xét từng phần tử nếu có nhãn thì gán tương ứng điểm dữ liệu cho tâm cụm = 1
    #     for index, val in enumerate(labels):
    #         if val != -1:
    #             u_bar[index][val] = 1
    #     return u_bar

    #     # Cập nhật tâm cụm (ghi đè hàm từ fcm)

    # CT 18
    def update_centroids(self, data: np.ndarray, membership: np.ndarray, membership_bar: np.ndarray, centroids_fall: np.ndarray, beta):
        # print("shape membership", membership.shape)
        # print("shape membership_bar", membership_bar.shape)

        u_sub_ufall = (membership - membership_bar)**self.m  # N x C
        # part1 = np.dot(u_sub_ufall.T, membership)  # (C, N) x (N, D) = (C, D)
        part1 = np.dot(u_sub_ufall.T, data)  # (C, N) x (N, D) = (C, D)

        # C x D * C x 1 = C X D
        part2 = centroids_fall * np.sum(u_sub_ufall, axis=0)[:, np.newaxis]
        numerator = part1 + part2
        denumerator = (1 + beta) * np.sum(u_sub_ufall, axis=0)
        return numerator/denumerator[:, np.newaxis]

    # CT 17
    def update_membership(self, data: np.ndarray, centroids: np.ndarray, centroids_fall: np.ndarray, beta) -> np.ndarray:
        d_ik = cdist(data, centroids)  # N x C
        v_sub_vfall = np.linalg.norm(centroids - centroids_fall)  # C x 1
        numerator = 1/(d_ik**2 + beta*v_sub_vfall**2)  # N x C
        denumerator = np.sum(numerator**(1/(self.m-1)), axis=1)  # N x 1

        # thêm chiều 
        return numerator/denumerator[:, np.newaxis]

    def calculate_centroids_fall(self):
        # N, _ = self.data_site[0].local_data.shape
        # c = self.data_site[0].cluster_centers.shape[0]
        c, d = self.data_site[0].cluster_centers.shape

        # print("N,c", N,c)
        # v_fall = np.zeros((self.len_datasite, N, c))
        v_fall = np.zeros((self.len_datasite, c, d))

        for i in range(self.len_datasite):
            v_fall[i] = np.mean(np.array([self.data_site[k].cluster_centers for k in range(self.len_datasite) if k != i]), axis=0)
                
        return v_fall
    
    def caculate_j_fall(self, data: np.ndarray, u_fall: np.ndarray, centroids: np.ndarray):
            distances = cdist(data, centroids)
            return np.sum((u_fall ** self.m) * (distances ** 2))
    
    def caculate_beta(self, u_fall):
        # N, _ = self.data_site[0].local_data.shape
        # c = self.data_site[0].cluster_centers.shape[0]
        beta_matrix = np.zeros((self.len_datasite, self.len_datasite))
        for i in range(self.len_datasite):
            data_i = self.data_site[i].local_data
            j_ii = self.j_ii[i]
            for j in range(self.len_datasite):
                if i == j:
                    continue
                centers_j = self.data_site[j].cluster_centers
                j_fall = self.caculate_j_fall(data_i, u_fall[i][j], centers_j)
                beta_matrix[i][j] = min(1, j_ii / j_fall) if j_fall > 0 else 1

        return beta_matrix
    
    def phase1(self, datas: list):
        #Chạy FCM cho các datasite
        self.len_datasite = len(datas)
        for data in datas:
            fcm = FuzzyCMeans(self.n_clusters, self.m, self.epsilon, self.max_iter)
            fcm.fit(data)
            self.data_site.append(fcm)
            self.j_ii.append(fcm.compute_objective_j(data, fcm.membership_matrix, fcm.cluster_centers))

    def phase2(self, iterations: int, u_bar: np.ndarray):
        for _ in range(iterations):
            check = [False] * self.len_datasite
            u_fall = self.caculate_U_fall() # PxPxNxC
            beta_matrix = self.caculate_beta(u_fall)
            v_fall_all = self.calculate_centroids_fall()

            for i in range(self.len_datasite):
                fcm_i = self.data_site[i]
                data_i = fcm_i.local_data
                v_i = fcm_i.cluster_centers
                v_fall = v_fall_all[i]

                mask = np.arange(self.len_datasite) != i
                beta_avg = np.mean(beta_matrix[i, mask], axis=0) 

                for _ in range(iterations):
                    new_membership_matrix = self.update_membership(data_i, v_i, v_fall, beta_avg)
                    # Cập nhật lại trọng tâm cụm
                    new_centers = self.update_centroids(data_i, new_membership_matrix, u_bar[i], v_fall, beta_avg)

                    if (np.linalg.norm(new_membership_matrix - fcm_i.membership_matrix) < self.epsilon):
                        check[i] = True
                        break 
                    fcm_i.membership_matrix = new_membership_matrix
                    fcm_i.cluster_centers = new_centers 
                    
            if all(check):
                break

    def fit(self, data: np.ndarray, labels, num_sites: int, supervised_ratio: float):
        sub_datasets, sub_datasets_labels = divide_data_for_collaborativ(data, labels, num_sites)
        start_time = time.time()
        self.phase1(sub_datasets)

        ssfcm = Dssfcm(n_clusters=self.n_clusters)
        for i in range(self.len_datasite):
            n_points, _ = self.data_site[i].local_data.shape
            u_bar, _ = ssfcm.create_u_bar(n_points, self.n_clusters, labels, supervised_ratio)
            self.u_bar.append(u_bar)
        for i, site in enumerate(self.data_site):
            align_centroids, align_membership = align_clusters(site.cluster_centers, site.membership_matrix, standard_centroid)
            site.cluster_centers = align_centroids
            site.membership_matrix = align_membership 

        self.phase2(iterations=10000, u_bar=self.u_bar)
        phase2_time = time.time() - start_time
        
        return phase2_time

import pandas as pd
from Ultility.validity import dunn, davies_bouldin, calinski_harabasz, silhouette, separation, classification_entropy, hypervolume, cs, partition_coefficient, partition_entropy, f1_score, accuracy_score
from Ultility.data import  round_float , fetch_data_from_uci1
from Ultility.ultility import divide_data_for_collaborativ

if __name__ == "__main__":
    dataset_id = 109
    data_dict = fetch_data_from_uci1(dataset_id)
    data, labels = data_dict['X'], data_dict['y']
    labels_numeric, _ = Dssfcm.convert_labels_to_int(labels)
    standard_centroid = np.array([
        data[labels_numeric == 0].mean(axis=0),
        data[labels_numeric == 1].mean(axis=0),
        data[labels_numeric == 2].mean(axis=0),
    ])

    num_sites = 3
    sub_datasets, sub_datasets_labels = divide_data_for_collaborativ(data, labels, num_sites)
    for i, sub_data in enumerate(sub_datasets):
        print(f"Số lượng điểm trong site {i+1}: {len(sub_data)}")


    sscfcm = SSCFCM(n_clusters=3, m=2, epsilon=1e-5, max_iter=300)
    phase2_time = sscfcm.fit(data, labels, num_sites, supervised_ratio=0.3)
    SPLIT = '\t'
    M = 2
    average='weighted'
    def wdvl(val: float) -> str:
        return str(round_float(val))
    def print_info(title: str, X: np.ndarray, U: np.ndarray, V: np.ndarray , process_time: float, step: int = 0, split: str = SPLIT, predicted_labels: np.ndarray = None) -> str:
        _ , labels_numeric = np.unique(predicted_labels, return_inverse=True)
        # print("labels_numeric", labels_numeric)
        # print("predicted_labels", np.argmax(U, axis=1))
        kqdg = [
            title,
            str(wdvl(process_time)),
            str(step),
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
        result = split.join(kqdg)
        return result
    
    titles = ['Alg', 'Time', 'Step', 'DB-', 'PC+', 'CE-', 'S-   ' , 'CH+     ', 'SI+', 'FHV+', 'F1+', 'AC+']
    print(SPLIT.join(titles))
    for i, site in enumerate(sscfcm.data_site):
        print(print_info(title=f"Site-{i+1}", X=site.local_data, U=site.membership_matrix, V=site.cluster_centers, process_time=phase2_time, step=10000, predicted_labels=sub_datasets_labels[i]))

    def print_info2(title: str, X: np.ndarray, U: np.ndarray, V: np.ndarray , process_time: float, step: int = 0, split: str = SPLIT, predicted_labels: np.ndarray = None) -> str:
        _ , labels_numeric = np.unique(predicted_labels, return_inverse=True)
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
    
    titles = ['Alg', 'Time', 'Step', 'DB-', 'PC+', 'CE-', 'S-   ' , 'CH+     ', 'SI+', 'FHV+', 'F1+', 'AC+']
    print(SPLIT.join(titles))
    for i, site in enumerate(sscfcm.data_site):
        print(print_info2(title=f"Site-{i+1}", X=site.local_data, U=site.membership_matrix, V=site.cluster_centers, process_time=phase2_time, step=10000, predicted_labels=sub_datasets_labels[i]))

