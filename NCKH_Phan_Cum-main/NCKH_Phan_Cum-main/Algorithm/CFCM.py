import time
import numpy as np
from Algorithm.FCM import FuzzyCMeans
import pandas as pd
from Ultility.ultility import divide_data_for_collaborativ

from Algorithm.SSFCM import Dssfcm
from Ultility.test import align_clusters
class Dcfcm():
    def __init__(self, n_clusters: int, m: float = 2.0, beta: float = 0.1, epsilon: float = 1e-5, max_iter: int = 10000):
        self.n_clusters = n_clusters
        self.m = m
        self.epsilon = epsilon
        self.max_iter = max_iter
        self.data_site = [] #Lưu trữ các datasite là các đối tượng trong FCM
        self.beta = beta #hệ só cộng tác giữa các datasite 
        self.labels_datasite = []

    def phase1(self, datas: list):
        #Chạy FCM cho các datasite
        self.len_datasite = len(datas)
        for data in datas:
            print(f"Chạy data thứ {len(self.data_site) + 1}")
            fcm = FuzzyCMeans(self.n_clusters, self.m, self.epsilon, self.max_iter)
            fcm.fit(data)
            self.data_site.append(fcm)
           
            print("Chạy xong")
        print("shape data", self.data_site[0].local_data.shape)
        print("centroids", self.data_site[0].cluster_centers.shape)
  
    def caculate_U_fall(self):
        N, _ = self.data_site[0].local_data.shape
        print( self.data_site[0].cluster_centers.shape)
        c = self.data_site[0].cluster_centers.shape[0]
        # print("N,c", N,c)
        u_fall = np.zeros((self.len_datasite, self.len_datasite, N, c ))
        for i in range(len(self.data_site)):
            # print(self.data_site[i].local_data)
            for j in range(len(self.data_site)):
                if i == j:
                    continue
                # print(f"Shape of data_site[i].local_data: {self.data_site[i].local_data.shape}")
                # print(f"Shape of data_site[j].cluster_centers: {self.data_site[j].cluster_centers.shape}")
                # print("datashape",self.data_site[i].local_data.shape)
                # print("centroids",self.data_site[j].cluster_centers.shape)
                u_fall[i][j] = FuzzyCMeans._update_membership_matrix(self,data=self.data_site[i].local_data, centroids=self.data_site[j].cluster_centers)
                
        return u_fall

    def caculate_U(self, u_fall , fcm_i):
        component1 = fcm_i._update_membership_matrix(fcm_i.local_data, fcm_i.cluster_centers)
        
        component3_1 = self.beta * np.sum(u_fall, axis=0) 
        component3_2 = 1 + self.beta * (self.len_datasite - 1)

        component3 = component3_1 / component3_2

        component2 = 1 - np.sum(component3, axis=1, keepdims=True)

        return (component1 * component2) +  component3

    def caculate_v(self,  U_ii: np.array, u_fall, data: np.array):
        print("U_ii shape:", U_ii.shape)
        print("u_fall shape:", u_fall.shape)
        print("signal shape before fix:", ((U_ii[np.newaxis, :] - u_fall) ** 2).shape)
        print("data shape ",data.shape)

        signal = (U_ii[np.newaxis,:] - u_fall) ** 2 # P x N x C
        # signal = (U_ii - u_fall) ** 2


        denominator1 = np.sum(U_ii ** 2, axis=0) [:,np.newaxis]
        print("denominator1 shape:", denominator1.shape)
        denominator2 = self.beta * np.sum(np.sum(signal, axis=1), axis=0)[:,np.newaxis] # Nhớ bỏ thêm chiều vì đang test D=1
        print("denominator2 shape:", denominator2.shape)
        # numerator1 = np.sum((U_ii ** 2) * data, axis=0)# a hiếu sửa
        numerator1 = np.sum((U_ii ** 2)[:, :, np.newaxis] * data[:, np.newaxis, :], axis=0)

        # numerator1 = np.sum((U_ii ** 2)[:, :, np.newaxis] * data[:, np.newaxis, :], axis=0, keepdims=True)
        numerator1 = np.dot(U_ii.T ** 2, data)
        print("numerator1 shape:", numerator1.shape)
        # numerator2 = self.beta * np.sum(np.sum(signal * data, axis=1), axis=0)[:,np.newaxis] # Nhớ bỏ thêm chiều vì đang test D=1 a hiếu sửa
        numerator2 = self.beta * np.sum(np.sum(signal[:, :, :, np.newaxis] * data[np.newaxis, :, np.newaxis, :], axis=1), axis=0)

        # numerator2 = self.beta * np.sum(np.sum(signal[:,:,:, np.newaxis] * data[np.newaxis, :, np.newaxis,:], axis=1, keepdims=True), axis=0, keepdims=True)
        # numerator2 = self.beta *   np.sum(np.sum(signal * data, axis=1), axis=0, keepdims=True)
        print("numerator2 shape:", numerator2.shape)
        # return (numerator1 + numerator2) / (denominator1 + denominator2)
        return (numerator1 + numerator2) / (denominator1 + denominator2)


    def phase2(self, iterations: int):
        self.steps = [0] * self.len_datasite
        for step in range(iterations):
            check = [False] * self.len_datasite
            print("Step",step)

            u_fall = self.caculate_U_fall()  # Tính ma trận cảm ứng giữa các site
            for i in range(self.len_datasite):
                fcm_i = self.data_site[i]
                # Cập nhật ma trận thành viên dựa trên U_fall
                new_membership_matrix = self.caculate_U(u_fall[i], fcm_i)
        
                # Cập nhật lại trọng tâm cụm
                # fcm_i.cluster_centers = self.caculate_v(new_membership_matrix, u_fall[i], fcm_i.local_data)
                new_centers = self.caculate_v(new_membership_matrix, u_fall[i], fcm_i.local_data)
                print(i,new_centers.shape)


                # if (np.linalg.norm(new_membership_matrix - fcm_i.membership_matrix) < self.epsilon):
                #     check[i] = True
                #     continue
                # else:
                #     self.steps[i] += 1
                delta_u = np.linalg.norm(new_membership_matrix - fcm_i.membership_matrix)
                delta_v = np.linalg.norm(new_centers - fcm_i.cluster_centers)

                if delta_u < self.epsilon and delta_v < self.epsilon:
                    check[i] = True
                    continue
                else:
                    self.steps[i] += 1

                fcm_i.membership_matrix = new_membership_matrix
                print("update",fcm_i.membership_matrix.shape)

                fcm_i.cluster_centers = new_centers # Loại bỏ chiều thừa
                print("update",fcm_i.cluster_centers.shape,new_centers.shape )

            
            if all(check):
                break


import pandas as pd
from Ultility.validity import dunn, davies_bouldin, calinski_harabasz, silhouette, separation, classification_entropy, hypervolume, cs, partition_coefficient, partition_entropy, f1_score, accuracy_score
from Ultility.data import  round_float , fetch_data_from_uci1

if __name__ == "__main__":

    # dataset_id = 602
    # data_dict = fetch_data_from_uci(dataset_id)
    # data, labels = data_dict['X'], data_dict['y']  # X là features, y là labels
    # # data = data[:, 1:]  # Bỏ cột đầu tiên (ID)
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

    # Khởi tạo CFM với 3 cụm (Iris có 3 lớp)
    dcfcm = Dcfcm(n_clusters=4, beta=0.5, max_iter=10000)

    # Chạy giai đoạn 1 (Phân cụm cục bộ tại mỗi site)   
    dcfcm.phase1(sub_datasets)

    # In ra trọng tâm cụm của từng data site
    print("\n Trọng tâm cụm trước khi cộng tác:")
    for i, site in enumerate(dcfcm.data_site):
        print(f"Data site {i+1}:\n", site.cluster_centers)

    for i, site in enumerate(dcfcm.data_site):
        align_centroids, align_membership = align_clusters(site.cluster_centers, site.membership_matrix, standard_centroid)
        site.cluster_centers = align_centroids
        site.membership_matrix = align_membership 

    print("\n Trọng tâm cụm sau khi sắp xếp:")
    for i, site in enumerate(dcfcm.data_site):
        print(f"Data site {i+1}:\n", site.cluster_centers)

    # Giai đoạn 2: Cập nhật theo U_fall
    start_time = time.time()
    dcfcm.phase2(iterations=10000)
    phase2_time = time.time() - start_time

    print("\n Trọng tâm cụm sau khi cộng tác:")
    for i, site in enumerate(dcfcm.data_site):
        print(f"Data site {i+1}:\n", site.cluster_centers)

    print("\n Ma trận thành viên của từng data site:")
    for i, site in enumerate(dcfcm.data_site):
        print(f"Data site {i+1}:\n", site.membership_matrix.shape)


    # u_fall = dcfcm.caculate_U_fall()
    # print("Ma trận u_fall:")
    # print(u_fall)

    # đầu ra là cặp u v cho từng cặp dataasite và chỉ số của từng bộ datasite


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
    for i, site in enumerate(dcfcm.data_site):
        print(print_info(title=f"Site-{i+1}", X=site.local_data, U=site.membership_matrix, V=site.cluster_centers, process_time=phase2_time, step=dcfcm.steps[i], predicted_labels=sub_datasets_labels[i]))


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
    for i, site in enumerate(dcfcm.data_site):
        print(print_info2(title=f"Site-{i+1}", X=site.local_data, U=site.membership_matrix, V=site.cluster_centers, process_time=phase2_time, step=dcfcm.steps[i], predicted_labels=sub_datasets_labels[i]))

