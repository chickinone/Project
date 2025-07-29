import numpy as np
from scipy.spatial.distance import cdist

from Algorithm.FCM import FuzzyCMeans
class Dssfcm(FuzzyCMeans):
    def __init__(self, n_clusters: int, m: float = 2, epsilon: float = 1e-5, max_iter: int = 10000):
        # Khởi tạo các tham số từ lớp cha FuzzyCMeans
        super().__init__(n_clusters=n_clusters, m=m, epsilon=epsilon, max_iter=max_iter)
        self.u_bar = None
        
    @staticmethod   
    def convert_labels_to_int(labels):
        unique_labels, int_labels = np.unique(labels, return_inverse=True)
        label_dict = {label: idx for idx, label in enumerate(unique_labels)}
        return int_labels, label_dict

    def create_u_bar(self, n_points, n_clusters, labels, supervised_ratio: float):
        # Chuyển nhãn từ string sang int
        int_labels, label_dict = self.convert_labels_to_int(labels)
        # Tạo ma trận giám sát với tất cả các giá trị ban đầu là -1
        u_bar = np.full((n_points, n_clusters), 0)
        # Xác định số lượng điểm có nhãn dựa trên tỷ lệ supervised_ratio
        num_supervised = int(n_points * supervised_ratio)
        # Chọn ngẫu nhiên các chỉ số của các điểm được gán nhãn
        np.random.seed(42)
        supervised_indices = np.random.choice(np.arange(n_points), size=num_supervised, replace=False)
        # Gán nhãn vào các điểm có nhãn trong ma trận bán giám sát
        u_bar[supervised_indices, int_labels[supervised_indices]] = 1.0
        self.u_bar = u_bar 
        return u_bar, supervised_indices


    def _compute_cluster_centers(self, data: np.ndarray, membership: np.ndarray):
        # Tính toán sự khác biệt giữa ma trận membership hiện tại và ma trận u_bar (có giám sát)
        u_u_bar = membership - self.u_bar  # Tính hiệu giữa membership và u_bar
        return super()._compute_cluster_centers(data=data, membership=u_u_bar)
        

    
    def _update_membership_matrix(self, data: np.ndarray, centroids: np.ndarray) -> np.ndarray:
        # Cập nhật lại ma trận độ thành viên
        # distance =  cdist(data, centroids)
       

        u = super()._update_membership_matrix(data, centroids)
        u_bar_sum = 1 - np.sum(self.u_bar, axis=1, keepdims=True)
        return self.u_bar + u_bar_sum * u
    
     
    def fit(self, data: np.ndarray, labels, supervised_ratio: float):
        # Khởi tạo u_bar từ nhãn (labels)
        n_points, _ = data.shape
        self.create_u_bar(n_points, self.n_clusters, labels, supervised_ratio)
    
        # Gọi hàm fit từ lớp cha (FuzzyCMeans)
        centroids, membership_matrix, steps, _ = super().fit(data)
        self.steps = steps  # Lưu số bước lặp vào thuộc tính của class
        
        return centroids, membership_matrix, steps

import pandas as pd
from Ultility.validity import dunn, davies_bouldin, calinski_harabasz, silhouette, separation, classification_entropy, hypervolume, cs, partition_coefficient, partition_entropy, f1_score, accuracy_score
from Ultility.data import  round_float , fetch_data_from_uci1, TEST_CASES
import numpy as np

if __name__ == "__main__":
    # dataset_id = 602
    # data_dict= fetch_data_from_uci(dataset_id)
    # data, labels = data_dict['X'], data_dict['y']
    dataset_id = 109
    data_dict = fetch_data_from_uci1(dataset_id)
    data, labels = data_dict['X'], data_dict['y']

    n_clusters = TEST_CASES[dataset_id]['n_cluster']  # Xác định số cụm từ nhãn trong dữ liệu
    print(n_clusters)

    ssfcm = Dssfcm(n_clusters=n_clusters)
    centroids, membership_matrix, steps = ssfcm.fit(data=data, labels=labels,supervised_ratio=0.5)
    int_labels, label_dict = ssfcm.convert_labels_to_int(labels)
    predicted_labels = np.argmax(membership_matrix, axis=1)

    print("u_bar matrix:")
    # print(ssfcm.u_bar)

    print('Ma tran thanh vien:')
    # print(ssfcm.membership_matrix)

    print(np.sum(ssfcm.membership_matrix, axis=1))
    
    print("tam cum:")
    # print(ssfcm.cluster_centers)

    print('Labels:')
    # print(ssfcm.predict())

    SPLIT = '\t'
    M = 2
    average='weighted'
    def wdvl(val: float) -> str:
        return str(round_float(val))

    def print_info(title: str, X: np.ndarray, U: np.ndarray, V: np.ndarray, process_time: float, step: int = 0, split: str = SPLIT) -> str:
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
            # wdvl(f1_score(int_labels, np.argmax(U, axis=1), average)),
            # wdvl(accuracy_score(int_labels, np.argmax(U, axis=1)))
        ]
        result = split.join(kqdg)
        return result
    
    titles = ['Alg', 'Time', 'Step', 'DB-', 'PC+', 'CE-', 'S-       ' , 'CH+        ', 'SI+', 'FHV+', 'CS-', 'F1+', 'AC+']
    print(SPLIT.join(titles))
    print(print_info( title='SSFCM', X=data, U=membership_matrix, V=centroids, process_time=ssfcm.process_time, step=steps))

    def print_info2(title: str, X: np.ndarray, U: np.ndarray, V: np.ndarray, process_time: float, step: int = 0, split: str = SPLIT) -> str:
        # print(np.argmax(U, axis=1))
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
            # wdvl(f1_score(int_labels, np.argmax(U, axis=1), average)),
            # wdvl(accuracy_score(int_labels, np.argmax(U, axis=1)))
        ]
        return ' & '.join(kqdg) + r'\\'
    titles = ['Alg', 'Time', 'Step', 'DB-', 'PC+', 'CE-', 'S-       ' , 'CH+        ', 'SI+', 'FHV+', 'CS-', 'F1+', 'AC+']
    print(SPLIT.join(titles))
    print(print_info2( title='SSFCM', X=data, U=membership_matrix, V=centroids, process_time=ssfcm.process_time, step=steps))
