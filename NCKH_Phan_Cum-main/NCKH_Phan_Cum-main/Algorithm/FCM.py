import numpy as np
import time
from scipy.spatial.distance import cdist

class FuzzyCMeans:
    def __init__(self, n_clusters: int, m: float = 2.0, epsilon: float = 1e-5, max_iter: int = 10000):
        self.n_clusters = n_clusters
        self.m = m
        self.epsilon = epsilon
        self.max_iter = max_iter
        self.cluster_centers = None
        self.membership_matrix = None
        self.process_time = 0 # Tính thời gian xử lý
        self.local_data = None
    def _initialize_membership_matrix(self, n_points: int, seed: int = 0):
        # np.random.seed(42)
        # return np.random.dirichlet(np.ones(self.n_clusters), size=n_points)
        if seed > 0:
            np.random.seed(seed=seed)
        U0 = np.random.rand(n_points, self.n_clusters)
        return U0 / U0.sum(axis=1)[:, None]

    def _compute_cluster_centers(self, data: np.array, membership: np.ndarray = None):
        if membership is None:
            membership = self.membership_matrix
        membership_matrix_power = membership ** self.m
        return np.dot(membership_matrix_power.T, data) / membership_matrix_power.sum(axis=0)[:, None]

    def _update_membership_matrix(self, data: np.array, centroids: np.ndarray):
        # print(f"data shape: {data.shape}")
        # print(f"centroids shape: {centroids.shape}")

        distances =  cdist(data, centroids)

        inv_dists = distances ** (-2 / (self.m - 1))
        return inv_dists / np.sum(inv_dists, axis=1, keepdims=True)
    
    def compute_objective_j(self, data: np.ndarray, U: np.ndarray, V: np.ndarray) -> float:
        _distance = cdist(data, V)
        # return np.sum((self.membership ** self._m) * (_distance ** 2))
        return np.sum((U ** self.m) * (_distance ** 2))

    def fit(self, data: np.array, seed: int = 42):
        n_points, _ = data.shape
        _start_tm = time.time()
        self.local_data = data
        self.membership_matrix = self._initialize_membership_matrix(n_points, seed=seed)
        # self.membership_matrix = self._initialize_membership_matrix(n_points=len(data), seed=seed)

        for iteration in range(self.max_iter):
            self.cluster_centers = self._compute_cluster_centers(data,  self.membership_matrix)

            new_membership_matrix = self._update_membership_matrix(data, centroids=self.cluster_centers)
            
            if np.linalg.norm(new_membership_matrix - self.membership_matrix) < self.epsilon:
                break

            self.membership_matrix = new_membership_matrix
        self.process_time = time.time() - _start_tm
        return self.cluster_centers, self.membership_matrix, iteration + 1, self.local_data

import time
import numpy as np
from Ultility.data import round_float, TEST_CASES, fetch_data_from_uci1
from Algorithm.FCM import FuzzyCMeans
from Ultility.validity import dunn, davies_bouldin, calinski_harabasz, silhouette, separation, classification_entropy, hypervolume, cs, partition_coefficient, f1_score, accuracy_score

if __name__ == "__main__":
    _start_time = time.time()

    # Lấy dữ liệu từ UCI
    dataset_id = 109
    data_dict = fetch_data_from_uci1(dataset_id)
    data, labels = data_dict['X'], data_dict['y']

    C = TEST_CASES[dataset_id]['n_cluster']
    M = 2
    EPSILON = 1e-5
    MAXITER = 10000
    # Khởi tạo FuzzyCMeans và tính toán
    fcm = FuzzyCMeans(n_clusters=C, m=M, epsilon=EPSILON, max_iter=MAXITER)
    centroids, membership_matrix, steps, _ = fcm.fit(data)

    print("Thời gian tính toán tuần tự", round_float(time.time() - _start_time))

    # print("Centroids:\n", centroids)
    print("Số bước lặp:", steps)

    SPLIT = '\t'
    M = 2
    average='weighted'
    _ , labels_numeric = np.unique(labels, return_inverse=True)
    # # np.set_printoptions(threshold=np.inf)
    # print(labels_numeric)

    def wdvl(val: float) -> str:
        return str(round_float(val))
    
    print("Unique values in labels_numeric:", np.unique(labels_numeric))
    print("Unique values in predicted_labels:", np.unique(np.argmax(membership_matrix, axis=1)))

    def print_info(title: str, X: np.ndarray, U: np.ndarray, V: np.ndarray, process_time: float, step: int = 0, split: str = SPLIT) -> str:
        # print(np.argmax(U, axis=1))
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
        result = split.join(kqdg)
        return result
    titles = ['Alg', 'Time', 'Step', 'DB-', 'PC+', 'CE-', 'S-       ' , 'CH+        ', 'SI+', 'FHV+', 'CS-', 'F1+', 'AC+']
    print(SPLIT.join(titles))
    print(print_info( title='FCM', X=data, U=membership_matrix, V=centroids, process_time=fcm.process_time, step=steps))
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
            # wdvl(f1_score(labels_numeric, np.argmax(U, axis=1), average)),
            # wdvl(accuracy_score(labels_numeric, np.argmax(U, axis=1)))
        ]
        return ' & '.join(kqdg) + r'\\'
    titles = ['Alg', 'Time', 'Step', 'DB-', 'PC+', 'CE-', 'S-       ' , 'CH+        ', 'SI+', 'FHV+', 'CS-', 'F1+', 'AC+']
    print(SPLIT.join(titles))
    print(print_info2( title='FCM', X=data, U=membership_matrix, V=centroids, process_time=fcm.process_time, step=steps))