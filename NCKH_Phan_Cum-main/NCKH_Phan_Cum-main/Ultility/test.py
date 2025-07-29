# import numpy as np
# from Ultility.data import fetch_data_from_uci

# # np.random.seed(0)
# # dataset_id = 53
# # data_dict = fetch_data_from_uci(dataset_id)
# # X, y = data_dict['X'], data_dict['y']  # X là features, y là labels
# # X = X[:, 1:]  # Bỏ cột đầu tiên (ID)

# def divide_data_for_collaborative(data, labels, n_data_site):
#     np.random.seed(42)

#     indices = np.arange(len(data))
#     np.random.shuffle(indices)

#     X_shuffled = data[indices]
#     y_shuffled = labels[indices]
#     # print(X_shuffled)
#     # print(y_shuffled)

#     sub_data = np.array_split(X_shuffled, n_data_site, axis=0)
#     sub_data_labels = np.array_split(y_shuffled, n_data_site, axis=0)

#     return sub_data, sub_data_labels
import numpy as np
from scipy.spatial.distance import cdist
from scipy.optimize import linear_sum_assignment
from Algorithm.SSFCM import Dssfcm
from Ultility.data import fetch_data_from_uci
# def align_clusters(centroids, membership_matrix, standard_centroid):
#     dist = cdist(centroids, standard_centroid)
#     _, col_idx = linear_sum_assignment(dist)
#     sort = np.argsort(col_idx)
    
#     return centroids[sort], membership_matrix[:, sort]
def align_clusters(centroids, membership_matrix, standard_centroid):
    dist = cdist(centroids, standard_centroid)
    row_ind, col_ind = linear_sum_assignment(dist)
    sort_order = np.argsort(col_ind)
    
    # sắp xếp centroid theo row_ind rồi theo sort_order
    aligned_centroids = centroids[row_ind][sort_order]
    aligned_membership = membership_matrix[:, row_ind][:, sort_order]
    
    return aligned_centroids, aligned_membership
if __name__ == "__main__":
    
    dataset_id = 53
    data_dict = fetch_data_from_uci(dataset_id)
    data, labels = data_dict['X'], data_dict['y']
    data = data[:, 1:]
    labels_numeric, _ = Dssfcm.convert_labels_to_int(labels)
    print(labels_numeric)

    standard_centroid = np.array([
        data[labels_numeric == 0].mean(axis=0),
        data[labels_numeric == 1].mean(axis=0),
        data[labels_numeric == 2].mean(axis=0),
    ])
    # print(data[labels_numeric == 0])

    # print(standard_centroid)
    